#define NAPI_VERSION 8

#include <assert.h>
#include <napi-macros.h>
#include <node_api.h>

#include <rocksdb/cache.h>
#include <rocksdb/comparator.h>
#include <rocksdb/convenience.h>
#include <rocksdb/db.h>
#include <rocksdb/env.h>
#include <rocksdb/filter_policy.h>
#include <rocksdb/merge_operator.h>
#include <rocksdb/options.h>
#include <rocksdb/slice.h>
#include <rocksdb/slice_transform.h>
#include <rocksdb/status.h>
#include <rocksdb/table.h>
#include <rocksdb/write_batch.h>

#include <memory>
#include <optional>
#include <set>
#include <string>
#include <thread>
#include <vector>

#include "max_rev_operator.h"
#include "util.h"

class NullLogger : public rocksdb::Logger {
 public:
  using rocksdb::Logger::Logv;
  virtual void Logv(const char* format, va_list ap) override {}
  virtual size_t GetLogFileSize() const override { return 0; }
};

struct Database;
struct Iterator;
struct Updates;

struct ColumnFamily {
  napi_ref ref;
  napi_value val;
  rocksdb::ColumnFamilyHandle* handle;
  rocksdb::ColumnFamilyDescriptor descriptor;
};

struct Closable {
  virtual ~Closable() {}
  virtual rocksdb::Status Close() = 0;
};

struct Database final {
  ~Database() { assert(!db); }

  rocksdb::Status Close() {
    if (!db) {
      return rocksdb::Status::OK();
    }

    for (auto closable : closables) {
      closable->Close();
    }
    closables.clear();

    for (auto& [id, column] : columns) {
      db->DestroyColumnFamilyHandle(column.handle);
    }
    columns.clear();

    db->FlushWAL(true);

    auto db2 = std::move(db);
    return db2->Close();
  }

  std::unique_ptr<rocksdb::DB> db;
  std::set<Closable*> closables;
  std::map<int32_t, ColumnFamily> columns;
};

enum BatchOp { Empty, Put, Delete, Merge, Data };

struct BatchEntry {
  BatchOp op = BatchOp::Empty;
  std::optional<std::string> key;
  std::optional<std::string> val;
  std::optional<ColumnFamily> column;
};

struct BatchIterator : public rocksdb::WriteBatch::Handler {
  BatchIterator(Database* database,
                bool keys = true,
                bool values = true,
                bool data = true,
                const rocksdb::ColumnFamilyHandle* column = nullptr,
                bool keyAsBuffer = false,
                bool valueAsBuffer = false)
      : database_(database),
        keys_(keys),
        values_(values),
        data_(data),
        column_(column),
        keyAsBuffer_(keyAsBuffer),
        valueAsBuffer_(valueAsBuffer) {}

  napi_status Iterate(napi_env env, const rocksdb::WriteBatch& batch, napi_value* result) {
    cache_.reserve(batch.Count());

    batch.Iterate(this);  // TODO (fix): Handle error?

    napi_value putStr;
    NAPI_STATUS_RETURN(napi_create_string_utf8(env, "put", NAPI_AUTO_LENGTH, &putStr));

    napi_value delStr;
    NAPI_STATUS_RETURN(napi_create_string_utf8(env, "del", NAPI_AUTO_LENGTH, &delStr));

    napi_value mergeStr;
    NAPI_STATUS_RETURN(napi_create_string_utf8(env, "merge", NAPI_AUTO_LENGTH, &mergeStr));

    napi_value dataStr;
    NAPI_STATUS_RETURN(napi_create_string_utf8(env, "data", NAPI_AUTO_LENGTH, &dataStr));

    napi_value nullVal;
    NAPI_STATUS_RETURN(napi_get_null(env, &nullVal));

    NAPI_STATUS_RETURN(napi_create_array_with_length(env, cache_.size() * 4, result));
    for (size_t n = 0; n < cache_.size(); ++n) {
      napi_value op;
      if (cache_[n].op == BatchOp::Put) {
        op = putStr;
      } else if (cache_[n].op == BatchOp::Delete) {
        op = delStr;
      } else if (cache_[n].op == BatchOp::Merge) {
        op = mergeStr;
      } else if (cache_[n].op == BatchOp::Data) {
        op = dataStr;
      } else {
        continue;
      }

      NAPI_STATUS_RETURN(napi_set_element(env, *result, n * 4 + 0, op));

      napi_value key;
      NAPI_STATUS_RETURN(Convert(env, cache_[n].key, keyAsBuffer_, key));
      NAPI_STATUS_RETURN(napi_set_element(env, *result, n * 4 + 1, key));

      napi_value val;
      NAPI_STATUS_RETURN(Convert(env, cache_[n].val, valueAsBuffer_, val));
      NAPI_STATUS_RETURN(napi_set_element(env, *result, n * 4 + 2, val));

      // TODO (fix)
      // napi_value column = cache_[n].column ? cache_[n].column->val : nullVal;
      NAPI_STATUS_RETURN(napi_set_element(env, *result, n * 4 + 3, nullVal));
    }

    cache_.clear();

    return napi_ok;
  }

  rocksdb::Status PutCF(uint32_t column_family_id, const rocksdb::Slice& key, const rocksdb::Slice& value) override {
    if (column_ && column_->GetID() != column_family_id) {
      return rocksdb::Status::OK();
    }

    BatchEntry entry;

    entry.op = BatchOp::Put;

    if (keys_) {
      entry.key = key.ToStringView();
    }

    if (values_) {
      entry.val = value.ToStringView();
    }

    // if (database_ && database_->columns.find(column_family_id) != database_->columns.end()) {
    //   entry.column = database_->columns[column_family_id];
    // }

    cache_.push_back(entry);

    return rocksdb::Status::OK();
  }

  rocksdb::Status DeleteCF(uint32_t column_family_id, const rocksdb::Slice& key) override {
    if (column_ && column_->GetID() != column_family_id) {
      return rocksdb::Status::OK();
    }

    BatchEntry entry;

    entry.op = BatchOp::Delete;

    if (keys_) {
      entry.key = key.ToStringView();
    }

    // if (database_ && database_->columns.find(column_family_id) != database_->columns.end()) {
    //   entry.column = database_->columns[column_family_id];
    // }

    cache_.push_back(entry);

    return rocksdb::Status::OK();
  }

  rocksdb::Status MergeCF(uint32_t column_family_id, const rocksdb::Slice& key, const rocksdb::Slice& value) override {
    if (column_ && column_->GetID() != column_family_id) {
      return rocksdb::Status::OK();
    }

    BatchEntry entry;

    entry.op = BatchOp::Merge;

    if (keys_) {
      entry.key = key.ToStringView();
    }

    if (values_) {
      entry.val = value.ToStringView();
    }

    // if (database_ && database_->columns.find(column_family_id) != database_->columns.end()) {
    //   entry.column = database_->columns[column_family_id];
    // }

    cache_.push_back(entry);

    return rocksdb::Status::OK();
  }

  void LogData(const rocksdb::Slice& data) override {
    if (!data_) {
      return;
    }

    BatchEntry entry;

    entry.op = BatchOp::Data;

    entry.val = data.ToStringView();

    cache_.push_back(entry);
  }

  bool Continue() override { return true; }

 private:
  Database* database_;
  bool keys_;
  bool values_;
  bool data_;
  const rocksdb::ColumnFamilyHandle* column_;
  bool keyAsBuffer_;
  bool valueAsBuffer_;
  std::vector<BatchEntry> cache_;
};

struct Updates : public BatchIterator, public Closable {
  Updates(Database* database,
          int64_t seqNumber,
          bool keys,
          bool values,
          bool data,
          const rocksdb::ColumnFamilyHandle* column,
          bool keyAsBuffer,
          bool valueAsBuffer)
      : BatchIterator(database, keys, values, data, column, keyAsBuffer, valueAsBuffer),
        database_(database),
        start_(seqNumber) {}

  virtual ~Updates() { assert(!iterator_); }

  rocksdb::Status Close() override {
    iterator_.reset();
    return rocksdb::Status::OK();
  }

  void Attach(napi_env env, napi_value context) {
    napi_create_reference(env, context, 1, &ref_);
    database_->closables.insert(this);
  }

  void Detach(napi_env env) {
    database_->closables.erase(this);
    if (ref_) {
      napi_delete_reference(env, ref_);
    }
  }

  Database* database_;
  int64_t start_;
  std::unique_ptr<rocksdb::TransactionLogIterator> iterator_;

 private:
  napi_ref ref_ = nullptr;
};

struct BaseIterator : public Closable {
  BaseIterator(Database* database,
               rocksdb::ColumnFamilyHandle* column,
               const bool reverse,
               const std::optional<std::string>& lt,
               const std::optional<std::string>& lte,
               const std::optional<std::string>& gt,
               const std::optional<std::string>& gte,
               const int limit,
               const bool fillCache,
               std::shared_ptr<const rocksdb::Snapshot> snapshot)
      : database_(database),
        column_(column),
        snapshot_(snapshot),
        reverse_(reverse),
        limit_(limit),
        fillCache_(fillCache) {
    if (lte) {
      upper_bound_ = rocksdb::PinnableSlice();
      *upper_bound_->GetSelf() = std::move(*lte) + '\0';
      upper_bound_->PinSelf();
    } else if (lt) {
      upper_bound_ = rocksdb::PinnableSlice();
      *upper_bound_->GetSelf() = std::move(*lt);
      upper_bound_->PinSelf();
    }

    if (gte) {
      lower_bound_ = rocksdb::PinnableSlice();
      *lower_bound_->GetSelf() = std::move(*gte);
      lower_bound_->PinSelf();
    } else if (gt) {
      lower_bound_ = rocksdb::PinnableSlice();
      *lower_bound_->GetSelf() = std::move(*gt) + '\0';
      lower_bound_->PinSelf();
    }
  }

  virtual ~BaseIterator() { assert(!iterator_); }

  bool DidSeek() const { return iterator_ != nullptr; }

  void SeekToRange() {
    if (!iterator_) {
      Init();
    }

    if (reverse_) {
      iterator_->SeekToLast();
    } else {
      iterator_->SeekToFirst();
    }
  }

  void Seek(const rocksdb::Slice& target) {
    if (!iterator_) {
      Init();
    }

    if ((upper_bound_ && target.compare(*upper_bound_) >= 0) || (lower_bound_ && target.compare(*lower_bound_) < 0)) {
      // TODO (fix): Why is this required? Seek should handle it?
      // https://github.com/facebook/rocksdb/issues/9904
      iterator_->SeekToLast();
      if (iterator_->Valid()) {
        iterator_->Next();
      }
    } else if (reverse_) {
      iterator_->SeekForPrev(target);
    } else {
      iterator_->Seek(target);
    }
  }

  rocksdb::Status Close() override {
    snapshot_.reset();
    iterator_.reset();
    return rocksdb::Status::OK();
  }

  bool Valid() const {
    assert(iterator_);
    return iterator_->Valid();
  }

  bool Increment() {
    assert(iterator_);
    return limit_ < 0 || ++count_ <= limit_;
  }

  void Next() {
    assert(iterator_);

    if (reverse_)
      iterator_->Prev();
    else
      iterator_->Next();
  }

  rocksdb::Slice CurrentKey() const {
    assert(iterator_);
    return iterator_->key();
  }

  rocksdb::Slice CurrentValue() const {
    assert(iterator_);
    return iterator_->value();
  }

  rocksdb::Status Status() const {
    assert(iterator_);
    return iterator_->status();
  }

  Database* database_;
  rocksdb::ColumnFamilyHandle* column_;
  std::shared_ptr<const rocksdb::Snapshot> snapshot_;

 private:
  void Init() {
    rocksdb::ReadOptions readOptions;
    if (upper_bound_) {
      readOptions.iterate_upper_bound = &*upper_bound_;
    }
    if (lower_bound_) {
      readOptions.iterate_lower_bound = &*lower_bound_;
    }
    readOptions.fill_cache = fillCache_;
    readOptions.snapshot = snapshot_.get();
    readOptions.async_io = true;
    readOptions.adaptive_readahead = true;

    iterator_.reset(database_->db->NewIterator(readOptions, column_));
  }

  int count_ = 0;
  std::optional<rocksdb::PinnableSlice> lower_bound_;
  std::optional<rocksdb::PinnableSlice> upper_bound_;
  std::unique_ptr<rocksdb::Iterator> iterator_;
  const bool reverse_;
  const int limit_;
  const bool fillCache_;
};

struct Iterator final : public BaseIterator {
  Iterator(Database* database,
           rocksdb::ColumnFamilyHandle* column,
           const bool reverse,
           const bool keys,
           const bool values,
           const int limit,
           const std::optional<std::string>& lt,
           const std::optional<std::string>& lte,
           const std::optional<std::string>& gt,
           const std::optional<std::string>& gte,
           const bool fillCache,
           const bool keyAsBuffer,
           const bool valueAsBuffer,
           const size_t highWaterMarkBytes,
           std::shared_ptr<const rocksdb::Snapshot> snapshot)
      : BaseIterator(database, column, reverse, lt, lte, gt, gte, limit, fillCache, snapshot),
        keys_(keys),
        values_(values),
        keyAsBuffer_(keyAsBuffer),
        valueAsBuffer_(valueAsBuffer),
        highWaterMarkBytes_(highWaterMarkBytes) {}

  void Attach(napi_env env, napi_value context) {
    napi_create_reference(env, context, 1, &ref_);
    database_->closables.insert(this);
  }

  void Detach(napi_env env) {
    database_->closables.erase(this);
    if (ref_) {
      napi_delete_reference(env, ref_);
    }
  }

  const bool keys_;
  const bool values_;
  const bool keyAsBuffer_;
  const bool valueAsBuffer_;
  const size_t highWaterMarkBytes_;
  bool first_ = true;

 private:
  napi_ref ref_ = nullptr;
};

static napi_status GetColumnFamily(Database* database,
                                   napi_env env,
                                   napi_value options,
                                   rocksdb::ColumnFamilyHandle** column) {
  bool hasColumn = false;
  NAPI_STATUS_RETURN(napi_has_named_property(env, options, "column", &hasColumn));

  if (hasColumn) {
    napi_value value = nullptr;
    NAPI_STATUS_RETURN(napi_get_named_property(env, options, "column", &value));

    bool nully = false;

    napi_value nullVal;
    NAPI_STATUS_RETURN(napi_get_null(env, &nullVal));
    NAPI_STATUS_RETURN(napi_strict_equals(env, nullVal, value, &nully));
    if (nully) {
      *column = nullptr;
      return napi_ok;
    }

    napi_value undefinedVal;
    NAPI_STATUS_RETURN(napi_get_undefined(env, &undefinedVal));
    NAPI_STATUS_RETURN(napi_strict_equals(env, undefinedVal, value, &nully));
    if (nully) {
      *column = nullptr;
      return napi_ok;
    }

    NAPI_STATUS_RETURN(napi_get_value_external(env, value, reinterpret_cast<void**>(column)));
  } else if (database) {
    *column = database->db->DefaultColumnFamily();
  } else {
    *column = nullptr;
  }

  return napi_ok;
}

static void env_cleanup_hook(void* arg) {
  auto database = reinterpret_cast<Database*>(arg);
  if (database) {
    database->Close();
  }
}

static void FinalizeDatabase(napi_env env, void* data, void* hint) {
  if (data) {
    auto database = reinterpret_cast<Database*>(data);
    database->Close();
    napi_remove_env_cleanup_hook(env, env_cleanup_hook, database);
    for (auto& [id, column] : database->columns) {
      napi_delete_reference(env, column.ref);
    }
    delete database;
  }
}

NAPI_METHOD(db_init) {
  auto database = new Database();
  napi_add_env_cleanup_hook(env, env_cleanup_hook, database);

  napi_value result;
  NAPI_STATUS_THROWS(napi_create_external(env, database, FinalizeDatabase, nullptr, &result));

  return result;
}

template <typename T, typename U>
rocksdb::Status InitOptions(napi_env env, T& columnOptions, const U& options) {
  rocksdb::ConfigOptions configOptions;

  uint32_t memtable_memory_budget = 256 * 1024 * 1024;
  if (Uint32Property(env, options, "memtableMemoryBudget", memtable_memory_budget) != napi_ok) {
    return rocksdb::Status::InvalidArgument("memtableMemoryBudget");
  }

  std::string compaction = "level";
  if (StringProperty(env, options, "compaction", compaction) != napi_ok) {
    return rocksdb::Status::InvalidArgument("compaction");
  }

  if (compaction == "universal") {
    columnOptions.write_buffer_size = static_cast<size_t>(memtable_memory_budget / 4);
    // merge two memtables when flushing to L0
    columnOptions.min_write_buffer_number_to_merge = 2;
    // this means we'll use 50% extra memory in the worst case, but will reduce
    // write stalls.
    columnOptions.max_write_buffer_number = 6;
    // universal style compaction
    columnOptions.compaction_style = rocksdb::kCompactionStyleUniversal;
    columnOptions.compaction_options_universal.compression_size_percent = 80;
  } else {
    // merge two memtables when flushing to L0
    columnOptions.min_write_buffer_number_to_merge = 2;
    // this means we'll use 50% extra memory in the worst case, but will reduce
    // write stalls.
    columnOptions.max_write_buffer_number = 6;
    // start flushing L0->L1 as soon as possible. each file on level0 is
    // (memtable_memory_budget / 2). This will flush level 0 when it's bigger than
    // memtable_memory_budget.
    columnOptions.level0_file_num_compaction_trigger = 2;
    // doesn't really matter much, but we don't want to create too many files
    columnOptions.target_file_size_base = memtable_memory_budget / 8;
    // make Level1 size equal to Level0 size, so that L0->L1 compactions are fast
    columnOptions.max_bytes_for_level_base = memtable_memory_budget;

    // level style compaction
    columnOptions.compaction_style = rocksdb::kCompactionStyleLevel;

    // TODO (perf): only compress levels >= 2
  }

  bool compression = true;
  if (BooleanProperty(env, options, "compression", compression) != napi_ok) {
    return rocksdb::Status::InvalidArgument("compression");
  }

  columnOptions.compression = compression ? rocksdb::kZSTD : rocksdb::kNoCompression;
  if (columnOptions.compression == rocksdb::kZSTD) {
    columnOptions.compression_opts.max_dict_bytes = 16 * 1024;
    columnOptions.compression_opts.zstd_max_train_bytes = 16 * 1024 * 100;
    // TODO (perf): compression_opts.parallel_threads
  }

  std::optional<std::string> prefixExtractorOpt;
  if (StringProperty(env, options, "prefixExtractor", prefixExtractorOpt) != napi_ok) {
    return rocksdb::Status::InvalidArgument("prefixExtractor");
  }
  if (prefixExtractorOpt) {
    ROCKS_STATUS_RETURN(
        rocksdb::SliceTransform::CreateFromString(configOptions, *prefixExtractorOpt, &columnOptions.prefix_extractor));
  }

  std::optional<std::string> comparatorOpt;
  if (StringProperty(env, options, "comparator", comparatorOpt) != napi_ok) {
    return rocksdb::Status::InvalidArgument("comparator");
  }
  if (comparatorOpt) {
    ROCKS_STATUS_RETURN(
        rocksdb::Comparator::CreateFromString(configOptions, *comparatorOpt, &columnOptions.comparator));
  }

  std::optional<std::string> mergeOperatorOpt;
  if (StringProperty(env, options, "mergeOperator", mergeOperatorOpt) != napi_ok) {
    return rocksdb::Status::InvalidArgument("mergeOperator");
  }
  if (mergeOperatorOpt) {
    ROCKS_STATUS_RETURN(
        rocksdb::MergeOperator::CreateFromString(configOptions, *mergeOperatorOpt, &columnOptions.merge_operator));
  }

  uint32_t cacheSize = 8 << 20;
  if (Uint32Property(env, options, "cacheSize", cacheSize) != napi_ok) {
    return rocksdb::Status::InvalidArgument("cacheSize");
  }

  rocksdb::BlockBasedTableOptions tableOptions;

  if (cacheSize) {
    tableOptions.block_cache = rocksdb::NewLRUCache(cacheSize);
    tableOptions.cache_index_and_filter_blocks = true;
    if (BooleanProperty(env, options, "cacheIndexAndFilterBlocks", tableOptions.cache_index_and_filter_blocks) !=
        napi_ok) {
      return rocksdb::Status::InvalidArgument("compression");
    }
  } else {
    tableOptions.no_block_cache = true;
    tableOptions.cache_index_and_filter_blocks = false;
  }

  std::string optimize = "";
  if (StringProperty(env, options, "optimize", optimize) != napi_ok) {
    return rocksdb::Status::InvalidArgument("optimize");
  }

  if (optimize == "point-lookup") {
    tableOptions.data_block_index_type = rocksdb::BlockBasedTableOptions::kDataBlockBinaryAndHash;
    tableOptions.data_block_hash_table_util_ratio = 0.75;
    tableOptions.filter_policy.reset(rocksdb::NewRibbonFilterPolicy(10, 1));

    columnOptions.memtable_prefix_bloom_size_ratio = 0.02;
    columnOptions.memtable_whole_key_filtering = true;
  } else if (optimize == "range-lookup") {
    // TODO?
  } else {
    tableOptions.filter_policy.reset(rocksdb::NewRibbonFilterPolicy(10));
  }

  std::optional<std::string> filterPolicyOpt;
  if (StringProperty(env, options, "filterPolicy", filterPolicyOpt) != napi_ok) {
    return rocksdb::Status::InvalidArgument("filterPolicy");
  }
  if (filterPolicyOpt) {
    ROCKS_STATUS_RETURN(
        rocksdb::FilterPolicy::CreateFromString(configOptions, *filterPolicyOpt, &tableOptions.filter_policy));
  }

  uint32_t blockSize = 4096;
  if (Uint32Property(env, options, "blockSize", blockSize) != napi_ok) {
    return rocksdb::Status::InvalidArgument("blockSize");
  }
  tableOptions.block_size = blockSize;

  uint32_t blockRestartInterval = 16;
  if (Uint32Property(env, options, "blockRestartInterval", blockRestartInterval) != napi_ok) {
    return rocksdb::Status::InvalidArgument("blockRestartInterval");
  }
  tableOptions.block_restart_interval = blockRestartInterval;

  tableOptions.format_version = 5;
  tableOptions.checksum = rocksdb::kXXH3;

  tableOptions.optimize_filters_for_memory = true;
  if (BooleanProperty(env, options, "optimizeFiltersForMemory", tableOptions.optimize_filters_for_memory) != napi_ok) {
    return rocksdb::Status::InvalidArgument("optimizeFiltersForMemory");
  }

  columnOptions.table_factory.reset(rocksdb::NewBlockBasedTableFactory(tableOptions));

  return rocksdb::Status::OK();
}

NAPI_METHOD(db_open) {
  NAPI_ARGV(4);

  Database* database;
  NAPI_STATUS_THROWS(napi_get_value_external(env, argv[0], reinterpret_cast<void**>(&database)));

  std::string location;
  NAPI_STATUS_THROWS(ToString(env, argv[1], location));

  rocksdb::Options dbOptions;

  uint32_t parallelism = std::max<uint32_t>(1, std::thread::hardware_concurrency() / 2);
  NAPI_STATUS_THROWS(Uint32Property(env, argv[2], "parallelism", parallelism));

  dbOptions.IncreaseParallelism(parallelism);

  dbOptions.create_if_missing = true;
  NAPI_STATUS_THROWS(BooleanProperty(env, argv[2], "createIfMissing", dbOptions.create_if_missing));
  dbOptions.error_if_exists = false;
  NAPI_STATUS_THROWS(BooleanProperty(env, argv[2], "errorIfExists", dbOptions.error_if_exists));

  dbOptions.avoid_unnecessary_blocking_io = true;

  dbOptions.write_dbid_to_manifest = true;

  dbOptions.use_adaptive_mutex = true;  // We don't have soo many threads in the libuv thread pool...

  dbOptions.enable_pipelined_write = false;  // We only write in the main thread...

  uint32_t walTTL;
  NAPI_STATUS_THROWS(Uint32Property(env, argv[2], "walTTL", walTTL));
  dbOptions.WAL_ttl_seconds = walTTL / 1e3;

  uint32_t walSizeLimit;
  NAPI_STATUS_THROWS(Uint32Property(env, argv[2], "walSizeLimit", walSizeLimit));
  dbOptions.WAL_size_limit_MB = walSizeLimit / 1e6;

  bool wal_compression = false;
  NAPI_STATUS_THROWS(BooleanProperty(env, argv[2], "walCompression", wal_compression));
  dbOptions.wal_compression =
      wal_compression ? rocksdb::CompressionType::kZSTD : rocksdb::CompressionType::kNoCompression;

  dbOptions.create_missing_column_families = true;

  dbOptions.unordered_write = false;

  NAPI_STATUS_THROWS(BooleanProperty(env, argv[2], "unorderedWrite", dbOptions.unordered_write));
  dbOptions.fail_if_options_file_error = true;

  dbOptions.manual_wal_flush = false;
  NAPI_STATUS_THROWS(BooleanProperty(env, argv[2], "manualWalFlush", dbOptions.manual_wal_flush));

  // TODO (feat): dbOptions.listeners

  std::string infoLogLevel;
  NAPI_STATUS_THROWS(StringProperty(env, argv[2], "infoLogLevel", infoLogLevel));
  if (infoLogLevel.size() > 0) {
    rocksdb::InfoLogLevel lvl = {};

    if (infoLogLevel == "debug")
      lvl = rocksdb::InfoLogLevel::DEBUG_LEVEL;
    else if (infoLogLevel == "info")
      lvl = rocksdb::InfoLogLevel::INFO_LEVEL;
    else if (infoLogLevel == "warn")
      lvl = rocksdb::InfoLogLevel::WARN_LEVEL;
    else if (infoLogLevel == "error")
      lvl = rocksdb::InfoLogLevel::ERROR_LEVEL;
    else if (infoLogLevel == "fatal")
      lvl = rocksdb::InfoLogLevel::FATAL_LEVEL;
    else if (infoLogLevel == "header")
      lvl = rocksdb::InfoLogLevel::HEADER_LEVEL;
    else
      napi_throw_error(env, nullptr, "invalid log level");

    dbOptions.info_log_level = lvl;
  } else {
    // In some places RocksDB checks this option to see if it should prepare
    // debug information (ahead of logging), so set it to the highest level.
    dbOptions.info_log_level = rocksdb::InfoLogLevel::HEADER_LEVEL;
    dbOptions.info_log.reset(new NullLogger());
  }

  ROCKS_STATUS_THROWS(InitOptions(env, dbOptions, argv[2]));

  std::vector<rocksdb::ColumnFamilyDescriptor> descriptors;

  bool hasColumns;
  NAPI_STATUS_THROWS(napi_has_named_property(env, argv[2], "columns", &hasColumns));

  if (hasColumns) {
    napi_value columns;
    NAPI_STATUS_THROWS(napi_get_named_property(env, argv[2], "columns", &columns));

    napi_value keys;
    NAPI_STATUS_THROWS(napi_get_property_names(env, columns, &keys));

    uint32_t len;
    NAPI_STATUS_THROWS(napi_get_array_length(env, keys, &len));

    descriptors.resize(len);
    for (uint32_t n = 0; n < len; ++n) {
      napi_value key;
      NAPI_STATUS_THROWS(napi_get_element(env, keys, n, &key));

      napi_value column;
      NAPI_STATUS_THROWS(napi_get_property(env, columns, key, &column));

      ROCKS_STATUS_THROWS(InitOptions(env, descriptors[n].options, column));

      NAPI_STATUS_THROWS(ToString(env, key, descriptors[n].name));
    }
  }

  auto callback = argv[3];

  runAsync<std::vector<rocksdb::ColumnFamilyHandle*>>(
      "leveldown.open", env, callback,
      [=](auto& handles) {
        rocksdb::DB* db = nullptr;
        const auto status = descriptors.empty() ? rocksdb::DB::Open(dbOptions, location, &db)
                                                : rocksdb::DB::Open(dbOptions, location, descriptors, &handles, &db);
        database->db.reset(db);
        return status;
      },
      [=](auto& handles, auto env, auto& argv) {
        argv.resize(2);

        const auto size = handles.size();
        NAPI_STATUS_RETURN(napi_create_object(env, &argv[1]));

        for (size_t n = 0; n < size; ++n) {
          ColumnFamily column;
          column.handle = handles[n];
          column.descriptor = descriptors[n];
          NAPI_STATUS_RETURN(napi_create_external(env, column.handle, nullptr, nullptr, &column.val));
          NAPI_STATUS_RETURN(napi_create_reference(env, column.val, 1, &column.ref));

          NAPI_STATUS_RETURN(napi_set_named_property(env, argv[1], descriptors[n].name.c_str(), column.val));

          database->columns[column.handle->GetID()] = column;
        }

        return napi_ok;
      });

  return 0;
}

napi_value noop_callback(napi_env env, napi_callback_info info) {
  return 0;
}

NAPI_METHOD(db_close) {
  NAPI_ARGV(2);

  Database* database;
  NAPI_STATUS_THROWS(napi_get_value_external(env, argv[0], reinterpret_cast<void**>(&database)));

  auto callback = argv[1];

  struct State {};
  runAsync<State>(
      "leveldown.close", env, callback, [=](auto& state) { return database->Close(); },
      [](auto& state, auto env, auto& argv) { return napi_ok; });

  return 0;
}

NAPI_METHOD(updates_init) {
  NAPI_ARGV(2);

  Database* database;
  NAPI_STATUS_THROWS(napi_get_value_external(env, argv[0], reinterpret_cast<void**>(&database)));

  napi_value sinceProperty;
  int64_t since;
  NAPI_STATUS_THROWS(napi_get_named_property(env, argv[1], "since", &sinceProperty));
  NAPI_STATUS_THROWS(napi_get_value_int64(env, sinceProperty, &since));

  napi_value keysProperty;
  bool keys;
  NAPI_STATUS_THROWS(napi_get_named_property(env, argv[1], "keys", &keysProperty));
  NAPI_STATUS_THROWS(napi_get_value_bool(env, keysProperty, &keys));

  napi_value valuesProperty;
  bool values;
  NAPI_STATUS_THROWS(napi_get_named_property(env, argv[1], "values", &valuesProperty));
  NAPI_STATUS_THROWS(napi_get_value_bool(env, valuesProperty, &values));

  napi_value dataProperty;
  bool data;
  NAPI_STATUS_THROWS(napi_get_named_property(env, argv[1], "data", &dataProperty));
  NAPI_STATUS_THROWS(napi_get_value_bool(env, dataProperty, &data));

  bool keyAsBuffer = false;
  NAPI_STATUS_THROWS(EncodingIsBuffer(env, argv[1], "keyEncoding", keyAsBuffer));

  bool valueAsBuffer = false;
  NAPI_STATUS_THROWS(EncodingIsBuffer(env, argv[1], "valueEncoding", valueAsBuffer));

  rocksdb::ColumnFamilyHandle* column = nullptr;
  NAPI_STATUS_THROWS(GetColumnFamily(nullptr, env, argv[1], &column));

  auto updates = new Updates(database, since, keys, values, data, column, keyAsBuffer, valueAsBuffer);

  napi_value result;
  NAPI_STATUS_THROWS(napi_create_external(env, updates, Finalize<Updates>, updates, &result));

  // Prevent GC of JS object before the iterator is closed (explicitly or on
  // db close) and keep track of non-closed iterators to end them on db close.
  updates->Attach(env, result);

  return result;
}

NAPI_METHOD(updates_next) {
  NAPI_ARGV(2);

  Updates* updates;
  NAPI_STATUS_THROWS(napi_get_value_external(env, argv[0], reinterpret_cast<void**>(&updates)));

  auto callback = argv[1];

  runAsync<rocksdb::BatchResult>(
      "leveldown.updates.next", env, callback,
      [=](auto& batchResult) {
        if (!updates->iterator_) {
          rocksdb::TransactionLogIterator::ReadOptions options;
          const auto status = updates->database_->db->GetUpdatesSince(updates->start_, &updates->iterator_, options);
          if (!status.ok()) {
            return status;
          }
        } else if (updates->iterator_->Valid()) {
          updates->iterator_->Next();
        }

        if (!updates->iterator_->Valid() || !updates->iterator_->status().ok()) {
          return updates->iterator_->status();
        }

        batchResult = updates->iterator_->GetBatch();

        return rocksdb::Status::OK();
      },
      [=](auto& batchResult, auto env, auto& argv) {
        if (!batchResult.writeBatchPtr) {
          return napi_ok;
        }

        argv.resize(5);
        NAPI_STATUS_RETURN(updates->Iterate(env, *batchResult.writeBatchPtr, &argv[1]));
        NAPI_STATUS_RETURN(napi_create_int64(env, batchResult.sequence, &argv[2]));
        NAPI_STATUS_RETURN(napi_create_int64(env, batchResult.writeBatchPtr->Count(), &argv[3]));
        NAPI_STATUS_RETURN(napi_create_int64(env, updates->start_, &argv[4]));

        return napi_ok;
      });

  return 0;
}

NAPI_METHOD(updates_close) {
  NAPI_ARGV(1);

  Updates* updates;
  NAPI_STATUS_THROWS(napi_get_value_external(env, argv[0], reinterpret_cast<void**>(&updates)));

  updates->Detach(env);
  updates->Close();

  return 0;
}

NAPI_METHOD(db_get_many) {
  NAPI_ARGV(4);

  Database* database;
  NAPI_STATUS_THROWS(napi_get_value_external(env, argv[0], reinterpret_cast<void**>(&database)));

  std::vector<std::string> keys;
  {
    uint32_t length;
    NAPI_STATUS_THROWS(napi_get_array_length(env, argv[1], &length));

    keys.resize(length);
    for (uint32_t n = 0; n < length; n++) {
      napi_value element;
      NAPI_STATUS_THROWS(napi_get_element(env, argv[1], n, &element));
      NAPI_STATUS_THROWS(ToString(env, element, keys[n]));
    }
  }

  bool valueAsBuffer = false;
  NAPI_STATUS_THROWS(EncodingIsBuffer(env, argv[2], "valueEncoding", valueAsBuffer));

  bool fillCache = true;
  NAPI_STATUS_THROWS(BooleanProperty(env, argv[2], "fillCache", fillCache));

  bool ignoreRangeDeletions = false;
  NAPI_STATUS_THROWS(BooleanProperty(env, argv[2], "ignoreRangeDeletions", ignoreRangeDeletions));

  rocksdb::ColumnFamilyHandle* column;
  NAPI_STATUS_THROWS(GetColumnFamily(database, env, argv[2], &column));

  auto callback = argv[3];

  auto snapshot = std::shared_ptr<const rocksdb::Snapshot>(
      database->db->GetSnapshot(), [database](auto ptr) { database->db->ReleaseSnapshot(ptr); });

  runAsync<std::vector<rocksdb::PinnableSlice>>(
      "leveldown.get.many", env, callback,
      [=, keys = std::move(keys), snapshot = std::move(snapshot)](auto& values) {
        rocksdb::ReadOptions readOptions;
        readOptions.fill_cache = fillCache;
        readOptions.snapshot = snapshot.get();
        readOptions.async_io = true;
        readOptions.ignore_range_deletions = ignoreRangeDeletions;

        const auto size = keys.size();

        std::vector<rocksdb::Slice> keys2;
        keys2.reserve(size);
        for (const auto& key : keys) {
          keys2.emplace_back(key);
        }
        std::vector<rocksdb::Status> statuses;

        statuses.resize(size);
        values.resize(size);

        database->db->MultiGet(readOptions, column, size, keys2.data(), values.data(), statuses.data());

        for (size_t idx = 0; idx < size; idx++) {
          if (statuses[idx].IsNotFound()) {
            values[idx] = rocksdb::PinnableSlice(nullptr);
          } else if (!statuses[idx].ok()) {
            return statuses[idx];
          }
        }

        return rocksdb::Status::OK();
      },
      [=](auto& values, auto env, auto& argv) {
        argv.resize(2);

        NAPI_STATUS_RETURN(napi_create_array_with_length(env, values.size(), &argv[1]));

        for (size_t idx = 0; idx < values.size(); idx++) {
          napi_value element;
          if (values[idx].GetSelf()) {
            NAPI_STATUS_RETURN(Convert(env, &values[idx], valueAsBuffer, element));
          } else {
            NAPI_STATUS_RETURN(napi_get_undefined(env, &element));
          }
          NAPI_STATUS_RETURN(napi_set_element(env, argv[1], static_cast<uint32_t>(idx), element));
        }

        return napi_ok;
      });

  return 0;
}

NAPI_METHOD(db_clear) {
  NAPI_ARGV(2);

  Database* database;
  NAPI_STATUS_THROWS(napi_get_value_external(env, argv[0], reinterpret_cast<void**>(&database)));

  bool reverse = false;
  NAPI_STATUS_THROWS(BooleanProperty(env, argv[1], "reverse", reverse));

  int32_t limit = -1;
  NAPI_STATUS_THROWS(Int32Property(env, argv[1], "limit", limit));

  rocksdb::ColumnFamilyHandle* column;
  NAPI_STATUS_THROWS(GetColumnFamily(database, env, argv[1], &column));

  std::optional<std::string> lt;
  NAPI_STATUS_THROWS(StringProperty(env, argv[1], "lt", lt));

  std::optional<std::string> lte;
  NAPI_STATUS_THROWS(StringProperty(env, argv[1], "lte", lte));

  std::optional<std::string> gt;
  NAPI_STATUS_THROWS(StringProperty(env, argv[1], "gt", gt));

  std::optional<std::string> gte;
  NAPI_STATUS_THROWS(StringProperty(env, argv[1], "gte", gte));

  if (limit == -1) {
    rocksdb::PinnableSlice begin;
    if (gte) {
      *begin.GetSelf() = std::move(*gte);
    } else if (gt) {
      *begin.GetSelf() = std::move(*gt) + '\0';
    }
    begin.PinSelf();

    rocksdb::PinnableSlice end;
    if (lte) {
      *end.GetSelf() = std::move(*lte) + '\0';
    } else if (lt) {
      *end.GetSelf() = std::move(*lt);
    } else {
      // HACK: Assume no key that starts with 0xFF is larger than 1MiB.
      end.GetSelf()->resize(1e6);
      memset(end.GetSelf()->data(), 255, end.GetSelf()->size());
    }
    end.PinSelf();

    if (begin.compare(end) < 0) {
      rocksdb::WriteOptions writeOptions;
      ROCKS_STATUS_THROWS(database->db->DeleteRange(writeOptions, column, begin, end));
    }

    return 0;
  } else {
    // TODO (fix): Error handling.
    // TODO (fix): This should be async...

    std::shared_ptr<const rocksdb::Snapshot> snapshot(database->db->GetSnapshot(),
                                                      [=](const auto ptr) { database->db->ReleaseSnapshot(ptr); });
    BaseIterator it(database, column, reverse, lt, lte, gt, gte, limit, false, snapshot);

    it.SeekToRange();

    rocksdb::WriteBatch batch;
    rocksdb::WriteOptions writeOptions;
    rocksdb::Status status;

    while (true) {
      size_t bytesRead = 0;

      while (bytesRead <= 16 * 1024 && it.Valid() && it.Increment()) {
        const auto key = it.CurrentKey();
        batch.Delete(column, key);
        bytesRead += key.size();
        it.Next();
      }

      status = it.Status();
      if (!status.ok() || bytesRead == 0) {
        break;
      }

      status = database->db->Write(writeOptions, &batch);
      if (!status.ok()) {
        break;
      }

      batch.Clear();
    }

    it.Close();

    if (!status.ok()) {
      ROCKS_STATUS_THROWS(status);
    }

    return 0;
  }
}

NAPI_METHOD(db_get_property) {
  NAPI_ARGV(2);

  Database* database;
  NAPI_STATUS_THROWS(napi_get_value_external(env, argv[0], reinterpret_cast<void**>(&database)));

  rocksdb::PinnableSlice property;
  NAPI_STATUS_THROWS(ToString(env, argv[1], property));

  std::string value;
  database->db->GetProperty(property, &value);

  napi_value result;
  NAPI_STATUS_THROWS(napi_create_string_utf8(env, value.data(), value.size(), &result));

  return result;
}

NAPI_METHOD(db_get_latest_sequence) {
  NAPI_ARGV(1);

  Database* database;
  NAPI_STATUS_THROWS(napi_get_value_external(env, argv[0], reinterpret_cast<void**>(&database)));

  const auto seq = database->db->GetLatestSequenceNumber();

  napi_value result;
  NAPI_STATUS_THROWS(napi_create_int64(env, seq, &result));

  return result;
}

NAPI_METHOD(iterator_init) {
  NAPI_ARGV(2);

  Database* database;
  NAPI_STATUS_THROWS(napi_get_value_external(env, argv[0], reinterpret_cast<void**>(&database)));

  bool reverse = false;
  NAPI_STATUS_THROWS(BooleanProperty(env, argv[1], "reverse", reverse));

  bool keys = true;
  NAPI_STATUS_THROWS(BooleanProperty(env, argv[1], "keys", keys));

  bool values = true;
  NAPI_STATUS_THROWS(BooleanProperty(env, argv[1], "values", values));

  bool fillCache = false;
  NAPI_STATUS_THROWS(BooleanProperty(env, argv[1], "fillCache", fillCache));

  bool keyAsBuffer = false;
  NAPI_STATUS_THROWS(EncodingIsBuffer(env, argv[1], "keyEncoding", keyAsBuffer));

  bool valueAsBuffer = false;
  NAPI_STATUS_THROWS(EncodingIsBuffer(env, argv[1], "valueEncoding", valueAsBuffer));

  int32_t limit = -1;
  NAPI_STATUS_THROWS(Int32Property(env, argv[1], "limit", limit));

  int32_t highWaterMarkBytes = 64 * 1024;
  NAPI_STATUS_THROWS(Int32Property(env, argv[1], "highWaterMarkBytes", highWaterMarkBytes));

  std::optional<std::string> lt;
  NAPI_STATUS_THROWS(StringProperty(env, argv[1], "lt", lt));

  std::optional<std::string> lte;
  NAPI_STATUS_THROWS(StringProperty(env, argv[1], "lte", lte));

  std::optional<std::string> gt;
  NAPI_STATUS_THROWS(StringProperty(env, argv[1], "gt", gt));

  std::optional<std::string> gte;
  NAPI_STATUS_THROWS(StringProperty(env, argv[1], "gte", gte));

  rocksdb::ColumnFamilyHandle* column;
  NAPI_STATUS_THROWS(GetColumnFamily(database, env, argv[1], &column));

  std::shared_ptr<const rocksdb::Snapshot> snapshot(database->db->GetSnapshot(),
                                                    [=](const auto ptr) { database->db->ReleaseSnapshot(ptr); });

  auto iterator = new Iterator(database, column, reverse, keys, values, limit, lt, lte, gt, gte, fillCache, keyAsBuffer,
                               valueAsBuffer, highWaterMarkBytes, snapshot);

  napi_value result;
  NAPI_STATUS_THROWS(napi_create_external(env, iterator, Finalize<Iterator>, iterator, &result));

  // Prevent GC of JS object before the iterator is closed (explicitly or on
  // db close) and keep track of non-closed iterators to end them on db close.
  iterator->Attach(env, result);

  return result;
}

NAPI_METHOD(iterator_seek) {
  NAPI_ARGV(2);

  Iterator* iterator;
  NAPI_STATUS_THROWS(napi_get_value_external(env, argv[0], reinterpret_cast<void**>(&iterator)));

  rocksdb::PinnableSlice target;
  NAPI_STATUS_THROWS(ToString(env, argv[1], target));

  iterator->first_ = true;
  iterator->Seek(target);  // TODO: Does seek causing blocking IO?

  return 0;
}

NAPI_METHOD(iterator_close) {
  NAPI_ARGV(1);

  Iterator* iterator;
  NAPI_STATUS_THROWS(napi_get_value_external(env, argv[0], reinterpret_cast<void**>(&iterator)));

  iterator->Detach(env);
  iterator->Close();

  return 0;
}

NAPI_METHOD(iterator_get_sequence) {
  NAPI_ARGV(1);

  Iterator* iterator;
  NAPI_STATUS_THROWS(napi_get_value_external(env, argv[0], reinterpret_cast<void**>(&iterator)));

  const auto seq = iterator->snapshot_->GetSequenceNumber();

  napi_value result;
  NAPI_STATUS_THROWS(napi_create_int64(env, seq, &result));

  return result;
}

NAPI_METHOD(iterator_nextv) {
  NAPI_ARGV(3);

  Iterator* iterator;
  NAPI_STATUS_THROWS(napi_get_value_external(env, argv[0], reinterpret_cast<void**>(&iterator)));

  uint32_t size;
  NAPI_STATUS_THROWS(napi_get_value_uint32(env, argv[1], &size));

  auto callback = argv[2];

  struct State {
    std::vector<std::optional<std::string>> cache;
    bool finished = false;
  };

  runAsync<State>(
      std::string("leveldown.iterator.next"), env, callback,
      [=](auto& state) {
        if (!iterator->DidSeek()) {
          iterator->SeekToRange();
        }

        state.cache.reserve(size * 2);
        size_t bytesRead = 0;

        while (true) {
          if (!iterator->first_)
            iterator->Next();
          else
            iterator->first_ = false;

          if (!iterator->Valid() || !iterator->Increment())
            break;

          if (iterator->keys_ && iterator->values_) {
            auto k = iterator->CurrentKey();
            auto v = iterator->CurrentValue();
            bytesRead += k.size() + v.size();
            state.cache.push_back(k.ToString());
            state.cache.push_back(v.ToString());
          } else if (iterator->keys_) {
            auto k = iterator->CurrentKey();
            bytesRead += k.size();
            state.cache.push_back(k.ToString());
            state.cache.push_back(std::nullopt);
          } else if (iterator->values_) {
            auto v = iterator->CurrentValue();
            bytesRead += v.size();
            state.cache.push_back(std::nullopt);
            state.cache.push_back(v.ToString());
          }

          if (bytesRead > iterator->highWaterMarkBytes_ || state.cache.size() / 2 >= size) {
            state.finished = false;
            return rocksdb::Status::OK();
          }
        }

        state.finished = true;

        return iterator->Status();
      },
      [=](auto& state, auto env, auto& argv) {
        argv.resize(3);

        NAPI_STATUS_RETURN(napi_create_array_with_length(env, state.cache.size(), &argv[1]));

        for (size_t n = 0; n < state.cache.size(); n += 2) {
          napi_value key;
          napi_value val;

          NAPI_STATUS_RETURN(Convert(env, state.cache[n + 0], iterator->keyAsBuffer_, key));
          NAPI_STATUS_RETURN(Convert(env, state.cache[n + 1], iterator->valueAsBuffer_, val));

          NAPI_STATUS_RETURN(napi_set_element(env, argv[1], static_cast<int>(n + 0), key));
          NAPI_STATUS_RETURN(napi_set_element(env, argv[1], static_cast<int>(n + 1), val));
        }

        NAPI_STATUS_RETURN(napi_get_boolean(env, state.finished, &argv[2]));

        return napi_ok;
      });

  return 0;
}

NAPI_METHOD(batch_do) {
  NAPI_ARGV(3);

  Database* database;
  NAPI_STATUS_THROWS(napi_get_value_external(env, argv[0], reinterpret_cast<void**>(&database)));

  rocksdb::WriteBatch batch;

  uint32_t length;
  NAPI_STATUS_THROWS(napi_get_array_length(env, argv[1], &length));

  for (uint32_t i = 0; i < length; i++) {
    std::optional<std::string> type;
    std::optional<std::string> key;
    std::optional<std::string> value;

    napi_value element;
    NAPI_STATUS_THROWS(napi_get_element(env, argv[1], i, &element));
    NAPI_STATUS_THROWS(StringProperty(env, element, "type", type));

    rocksdb::ColumnFamilyHandle* column;
    NAPI_STATUS_THROWS(GetColumnFamily(database, env, element, &column));

    if (!type) {
      ROCKS_STATUS_THROWS(rocksdb::Status::InvalidArgument("type"));
    } else if (*type == "del") {
      NAPI_STATUS_THROWS(StringProperty(env, element, "key", key));
      if (!key) {
        ROCKS_STATUS_THROWS(rocksdb::Status::InvalidArgument("key"));
      }
      ROCKS_STATUS_THROWS(batch.Delete(column, *key));
    } else if (*type == "put") {
      NAPI_STATUS_THROWS(StringProperty(env, element, "key", key));
      if (!key) {
        ROCKS_STATUS_THROWS(rocksdb::Status::InvalidArgument("key"));
      }
      NAPI_STATUS_THROWS(StringProperty(env, element, "value", value));
      if (!value) {
        ROCKS_STATUS_THROWS(rocksdb::Status::InvalidArgument("value"));
      }
      ROCKS_STATUS_THROWS(batch.Put(column, *key, *value));
    } else if (*type == "data") {
      NAPI_STATUS_THROWS(StringProperty(env, element, "value", value));
      if (!value) {
        ROCKS_STATUS_THROWS(rocksdb::Status::InvalidArgument("value"));
      }
      ROCKS_STATUS_THROWS(batch.PutLogData(*value));
    } else if (*type == "merge") {
      NAPI_STATUS_THROWS(StringProperty(env, element, "key", key));
      if (!key) {
        ROCKS_STATUS_THROWS(rocksdb::Status::InvalidArgument("key"));
      }
      NAPI_STATUS_THROWS(StringProperty(env, element, "value", value));
      if (!value) {
        ROCKS_STATUS_THROWS(rocksdb::Status::InvalidArgument("value"));
      }
      ROCKS_STATUS_THROWS(batch.Merge(column, *key, *value));
    } else {
      ROCKS_STATUS_THROWS(rocksdb::Status::InvalidArgument("type"));
    }
  }

  rocksdb::WriteOptions writeOptions;
  ROCKS_STATUS_THROWS(database->db->Write(writeOptions, &batch));

  return 0;
}

NAPI_METHOD(batch_init) {
  auto batch = new rocksdb::WriteBatch();

  napi_value result;
  NAPI_STATUS_THROWS(napi_create_external(env, batch, Finalize<rocksdb::WriteBatch>, batch, &result));

  return result;
}

NAPI_METHOD(batch_put) {
  NAPI_ARGV(4);

  rocksdb::WriteBatch* batch;
  NAPI_STATUS_THROWS(napi_get_value_external(env, argv[0], reinterpret_cast<void**>(&batch)));

  rocksdb::PinnableSlice key;
  NAPI_STATUS_THROWS(ToString(env, argv[1], key));

  rocksdb::PinnableSlice val;
  NAPI_STATUS_THROWS(ToString(env, argv[2], val));

  rocksdb::ColumnFamilyHandle* column;
  NAPI_STATUS_THROWS(GetColumnFamily(nullptr, env, argv[3], &column));

  if (column) {
    ROCKS_STATUS_THROWS(batch->Put(column, key, val));
  } else {
    ROCKS_STATUS_THROWS(batch->Put(key, val));
  }

  return 0;
}

NAPI_METHOD(batch_del) {
  NAPI_ARGV(3);

  rocksdb::WriteBatch* batch;
  NAPI_STATUS_THROWS(napi_get_value_external(env, argv[0], reinterpret_cast<void**>(&batch)));

  rocksdb::PinnableSlice key;
  NAPI_STATUS_THROWS(ToString(env, argv[1], key));

  rocksdb::ColumnFamilyHandle* column;
  NAPI_STATUS_THROWS(GetColumnFamily(nullptr, env, argv[2], &column));

  if (column) {
    ROCKS_STATUS_THROWS(batch->Delete(column, key));
  } else {
    ROCKS_STATUS_THROWS(batch->Delete(key));
  }

  return 0;
}

NAPI_METHOD(batch_merge) {
  NAPI_ARGV(4);

  rocksdb::WriteBatch* batch;
  NAPI_STATUS_THROWS(napi_get_value_external(env, argv[0], (void**)(&batch)));

  rocksdb::PinnableSlice key;
  NAPI_STATUS_THROWS(ToString(env, argv[1], key));

  rocksdb::PinnableSlice val;
  NAPI_STATUS_THROWS(ToString(env, argv[2], val));

  rocksdb::ColumnFamilyHandle* column;
  NAPI_STATUS_THROWS(GetColumnFamily(nullptr, env, argv[3], &column));

  if (column) {
    ROCKS_STATUS_THROWS(batch->Merge(column, key, val));
  } else {
    ROCKS_STATUS_THROWS(batch->Merge(key, val));
  }

  return 0;
}

NAPI_METHOD(batch_clear) {
  NAPI_ARGV(1);

  rocksdb::WriteBatch* batch;
  NAPI_STATUS_THROWS(napi_get_value_external(env, argv[0], reinterpret_cast<void**>(&batch)));

  batch->Clear();

  return 0;
}

NAPI_METHOD(batch_write) {
  NAPI_ARGV(3);

  Database* database;
  NAPI_STATUS_THROWS(napi_get_value_external(env, argv[0], reinterpret_cast<void**>(&database)));

  rocksdb::WriteBatch* batch;
  NAPI_STATUS_THROWS(napi_get_value_external(env, argv[1], reinterpret_cast<void**>(&batch)));

  rocksdb::WriteOptions writeOptions;
  ROCKS_STATUS_THROWS(database->db->Write(writeOptions, batch));

  return 0;
}

NAPI_METHOD(batch_put_log_data) {
  NAPI_ARGV(3);

  rocksdb::WriteBatch* batch;
  NAPI_STATUS_THROWS(napi_get_value_external(env, argv[0], reinterpret_cast<void**>(&batch)));

  rocksdb::PinnableSlice logData;
  NAPI_STATUS_THROWS(ToString(env, argv[1], logData));

  ROCKS_STATUS_THROWS(batch->PutLogData(logData));

  return 0;
}

NAPI_METHOD(batch_count) {
  NAPI_ARGV(1);

  rocksdb::WriteBatch* batch;
  NAPI_STATUS_THROWS(napi_get_value_external(env, argv[0], reinterpret_cast<void**>(&batch)));

  napi_value result;
  NAPI_STATUS_THROWS(napi_create_int64(env, batch->Count(), &result));

  return result;
}

NAPI_METHOD(batch_iterate) {
  NAPI_ARGV(3);

  Database* database;
  NAPI_STATUS_THROWS(napi_get_value_external(env, argv[0], reinterpret_cast<void**>(&database)));

  rocksdb::WriteBatch* batch;
  NAPI_STATUS_THROWS(napi_get_value_external(env, argv[1], reinterpret_cast<void**>(&batch)));

  bool keys = true;
  NAPI_STATUS_THROWS(BooleanProperty(env, argv[2], "keys", keys));

  bool values = true;
  NAPI_STATUS_THROWS(BooleanProperty(env, argv[2], "values", values));

  bool data = true;
  NAPI_STATUS_THROWS(BooleanProperty(env, argv[2], "data", data));

  bool keyAsBuffer = false;
  NAPI_STATUS_THROWS(EncodingIsBuffer(env, argv[2], "keyEncoding", keyAsBuffer));

  bool valueAsBuffer = false;
  NAPI_STATUS_THROWS(EncodingIsBuffer(env, argv[2], "valueEncoding", valueAsBuffer));

  rocksdb::ColumnFamilyHandle* column = nullptr;
  NAPI_STATUS_THROWS(GetColumnFamily(nullptr, env, argv[2], &column));

  BatchIterator iterator(nullptr, keys, values, data, column, keyAsBuffer, valueAsBuffer);

  napi_value result;
  NAPI_STATUS_THROWS(iterator.Iterate(env, *batch, &result));

  return result;
}

NAPI_INIT() {
  NAPI_EXPORT_FUNCTION(db_init);
  NAPI_EXPORT_FUNCTION(db_open);
  NAPI_EXPORT_FUNCTION(db_close);
  NAPI_EXPORT_FUNCTION(db_get_many);
  NAPI_EXPORT_FUNCTION(db_clear);
  NAPI_EXPORT_FUNCTION(db_get_property);
  NAPI_EXPORT_FUNCTION(db_get_latest_sequence);

  NAPI_EXPORT_FUNCTION(iterator_init);
  NAPI_EXPORT_FUNCTION(iterator_seek);
  NAPI_EXPORT_FUNCTION(iterator_close);
  NAPI_EXPORT_FUNCTION(iterator_nextv);
  NAPI_EXPORT_FUNCTION(iterator_get_sequence);

  NAPI_EXPORT_FUNCTION(updates_init);
  NAPI_EXPORT_FUNCTION(updates_close);
  NAPI_EXPORT_FUNCTION(updates_next);

  NAPI_EXPORT_FUNCTION(batch_do);
  NAPI_EXPORT_FUNCTION(batch_init);
  NAPI_EXPORT_FUNCTION(batch_put);
  NAPI_EXPORT_FUNCTION(batch_del);
  NAPI_EXPORT_FUNCTION(batch_clear);
  NAPI_EXPORT_FUNCTION(batch_write);
  NAPI_EXPORT_FUNCTION(batch_put_log_data);
  NAPI_EXPORT_FUNCTION(batch_merge);
  NAPI_EXPORT_FUNCTION(batch_count);
  NAPI_EXPORT_FUNCTION(batch_iterate);
}
