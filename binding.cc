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
#include <rocksdb/options.h>
#include <rocksdb/slice_transform.h>
#include <rocksdb/table.h>
#include <rocksdb/write_batch.h>

#include <array>
#include <memory>
#include <optional>
#include <set>
#include <string>
#include <thread>
#include <vector>

class NullLogger : public rocksdb::Logger {
 public:
  using rocksdb::Logger::Logv;
  virtual void Logv(const char* format, va_list ap) override {}
  virtual size_t GetLogFileSize() const override { return 0; }
};

struct Database;
struct Iterator;
struct Updates;

#define NAPI_STATUS_RETURN(call) \
  {                              \
    auto _status = (call);       \
    if (_status != napi_ok) {    \
      return _status;            \
    }                            \
  }

#define ROCKS_STATUS_RETURN(call) \
  {                               \
    auto _status = (call);        \
    if (!_status.ok()) {          \
      return _status;             \
    }                             \
  }

#define ROCKS_STATUS_THROWS(call)             \
  {                                           \
    auto _status = (call);                    \
    if (!_status.ok()) {                      \
      napi_throw(env, ToError(env, _status)); \
      return NULL;                            \
    }                                         \
  }

static napi_value CreateError(napi_env env, const std::optional<std::string_view>& code, const std::string_view& msg) {
  napi_value codeValue = nullptr;
  if (code) {
    NAPI_STATUS_THROWS(napi_create_string_utf8(env, code->data(), code->size(), &codeValue));
  }
  napi_value msgValue;
  NAPI_STATUS_THROWS(napi_create_string_utf8(env, msg.data(), msg.size(), &msgValue));
  napi_value error;
  NAPI_STATUS_THROWS(napi_create_error(env, codeValue, msgValue, &error));
  return error;
}

static bool HasProperty(napi_env env, napi_value obj, const std::string_view& key) {
  bool has = false;
  napi_has_named_property(env, obj, key.data(), &has);
  return has;
}

static napi_value GetProperty(napi_env env, napi_value obj, const std::string_view& key) {
  napi_value value = nullptr;
  napi_get_named_property(env, obj, key.data(), &value);
  return value;
}

static std::optional<bool> BooleanProperty(napi_env env, napi_value obj, const std::string_view& key) {
  if (HasProperty(env, obj, key.data())) {
    const auto value = GetProperty(env, obj, key.data());
    bool result;
    napi_get_value_bool(env, value, &result);
    return result;
  }

  return {};
}

static bool EncodingIsBuffer(napi_env env, napi_value obj, const std::string_view& option) {
  napi_value value;
  size_t size;

  if (napi_get_named_property(env, obj, option.data(), &value) == napi_ok &&
      napi_get_value_string_utf8(env, value, nullptr, 0, &size) == napi_ok) {
    // Value is either "buffer" or "utf8" so we can tell them apart just by size
    return size == 6;
  }

  return false;
}

static std::optional<uint32_t> Uint32Property(napi_env env, napi_value obj, const std::string_view& key) {
  if (HasProperty(env, obj, key.data())) {
    const auto value = GetProperty(env, obj, key.data());
    uint32_t result;
    napi_get_value_uint32(env, value, &result);
    return result;
  }

  return {};
}

static std::optional<int> Int32Property(napi_env env, napi_value obj, const std::string_view& key) {
  if (HasProperty(env, obj, key.data())) {
    const auto value = GetProperty(env, obj, key.data());
    int result;
    napi_get_value_int32(env, value, &result);
    return result;
  }

  return {};
}

static std::optional<int64_t> Int64Property(napi_env env, napi_value obj, const std::string_view& key) {
  if (HasProperty(env, obj, key.data())) {
    const auto value = GetProperty(env, obj, key.data());
    int64_t result;
    bool lossless;
    napi_get_value_bigint_int64(env, value, &result, &lossless);
    return result;
  }

  return {};
}

static napi_status ToString(napi_env env, napi_value from, std::string& to) {
  napi_valuetype type;
  NAPI_STATUS_RETURN(napi_typeof(env, from, &type));

  if (type == napi_string) {
    size_t length = 0;
    NAPI_STATUS_RETURN(napi_get_value_string_utf8(env, from, nullptr, 0, &length));
    to.resize(length, '\0');
    NAPI_STATUS_RETURN(napi_get_value_string_utf8(env, from, &to[0], length + 1, &length));
  } else {
    bool isBuffer;
    NAPI_STATUS_RETURN(napi_is_buffer(env, from, &isBuffer));

    if (isBuffer) {
      char* buf = nullptr;
      size_t length = 0;
      NAPI_STATUS_RETURN(napi_get_buffer_info(env, from, reinterpret_cast<void**>(&buf), &length));
      to.assign(buf, length);
    } else {
      return napi_invalid_arg;
    }
  }

  return napi_ok;
}

static std::optional<std::string> StringProperty(napi_env env, napi_value opts, const std::string_view& name) {
  if (HasProperty(env, opts, name)) {
    const auto property = GetProperty(env, opts, name);
    std::string value;
    ToString(env, property, value);
    return value;
  }
  return {};
}

static napi_status CallFunction(napi_env env, napi_value callback, const int argc, napi_value* argv) {
  napi_value global;

  NAPI_STATUS_RETURN(napi_get_global(env, &global));
  NAPI_STATUS_RETURN(napi_call_function(env, global, callback, argc, argv, nullptr));

  return napi_ok;
}

static napi_value ToError(napi_env env, const rocksdb::Status& status) {
  if (status.ok()) {
    return 0;
  }

  const auto msg = status.ToString();

  if (status.IsNotFound()) {
    return CreateError(env, "LEVEL_NOT_FOUND", msg);
  } else if (status.IsCorruption()) {
    return CreateError(env, "LEVEL_CORRUPTION", msg);
  } else if (status.IsIOError()) {
    if (msg.find("IO error: lock ") != std::string::npos) {  // env_posix.cc
      return CreateError(env, "LEVEL_LOCKED", msg);
    } else if (msg.find("IO error: LockFile ") != std::string::npos) {  // env_win.cc
      return CreateError(env, "LEVEL_LOCKED", msg);
    } else if (msg.find("IO error: While lock file") != std::string::npos) {  // env_mac.cc
      return CreateError(env, "LEVEL_LOCKED", msg);
    } else {
      return CreateError(env, "LEVEL_IO_ERROR", msg);
    }
  }

  return CreateError(env, {}, msg);
}

template <typename T>
static void Finalize(napi_env env, void* data, void* hint) {
  if (hint) {
    delete reinterpret_cast<T*>(hint);
  }
}

template <typename T>
napi_status Convert(napi_env env, T&& s, bool asBuffer, napi_value& result) {
  if (!s) {
    return napi_get_null(env, &result);
  } else if (asBuffer) {
    return napi_create_buffer_copy(env, s->size(), s->data(), NULL, &result);
  } else {
    return napi_create_string_utf8(env, s->data(), s->size(), &result);
  }
}

/**
 * Base worker class. Handles the async work. Derived classes can override the
 * following virtual methods (listed in the order in which they're called):
 *
 * - Execute (abstract, worker pool thread): main work
 * - OnOk (main thread): call JS callback on success
 * - OnError (main thread): call JS callback on error
 * - Destroy (main thread): do cleanup regardless of success
 */
struct Worker {
  Worker(napi_env env, Database* database, napi_value callback, const std::string& resourceName) : database_(database) {
    NAPI_STATUS_THROWS_VOID(napi_create_reference(env, callback, 1, &callbackRef_));
    napi_value asyncResourceName;
    NAPI_STATUS_THROWS_VOID(napi_create_string_utf8(env, resourceName.data(), resourceName.size(), &asyncResourceName));
    NAPI_STATUS_THROWS_VOID(
        napi_create_async_work(env, callback, asyncResourceName, Worker::Execute, Worker::Complete, this, &asyncWork_));
  }

  virtual ~Worker() {}

  static void Execute(napi_env env, void* data) {
    auto self = reinterpret_cast<Worker*>(data);
    self->status_ = self->Execute(*self->database_);
  }

  static void Complete(napi_env env, napi_status status, void* data) {
    auto self = reinterpret_cast<Worker*>(data);

    // TODO (fix): napi status handling...

    napi_value callback;
    napi_get_reference_value(env, self->callbackRef_, &callback);

    if (self->status_.ok()) {
      self->OnOk(env, callback);
    } else {
      self->OnError(env, callback, ToError(env, self->status_));
    }

    self->Destroy(env);

    napi_delete_reference(env, self->callbackRef_);
    napi_delete_async_work(env, self->asyncWork_);

    delete self;
  }

  virtual rocksdb::Status Execute(Database& database) = 0;

  virtual napi_status OnOk(napi_env env, napi_value callback) {
    napi_value argv[1];
    NAPI_STATUS_RETURN(napi_get_null(env, &argv[0]));

    return CallFunction(env, callback, 1, &argv[0]);
  }

  virtual napi_status OnError(napi_env env, napi_value callback, napi_value err) {
    return CallFunction(env, callback, 1, &err);
  }

  virtual void Destroy(napi_env env) {}

  void Queue(napi_env env) { napi_queue_async_work(env, asyncWork_); }

  Database* database_;

 private:
  napi_ref callbackRef_;
  napi_async_work asyncWork_;
  rocksdb::Status status_;
};

struct Database {
  void AttachIterator(napi_env env, Iterator* iterator) {
    iterators_.insert(iterator);
    IncrementPriorityWork(env);
  }

  void DetachIterator(napi_env env, Iterator* iterator) {
    iterators_.erase(iterator);
    DecrementPriorityWork(env);
  }

  void AttachUpdates(napi_env env, Updates* update) {
    updates_.insert(update);
    IncrementPriorityWork(env);
  }

  void DetachUpdates(napi_env env, Updates* update) {
    updates_.erase(update);
    DecrementPriorityWork(env);
  }

  void IncrementPriorityWork(napi_env env) { napi_reference_ref(env, priorityRef_, &priorityWork_); }

  void DecrementPriorityWork(napi_env env) {
    napi_reference_unref(env, priorityRef_, &priorityWork_);

    if (priorityWork_ == 0 && pendingCloseWorker_) {
      pendingCloseWorker_->Queue(env);
      pendingCloseWorker_ = nullptr;
    }
  }

  bool HasPriorityWork() const { return priorityWork_ > 0; }

  std::unique_ptr<rocksdb::DB> db_;
  Worker* pendingCloseWorker_;
  std::set<Iterator*> iterators_;
  std::set<Updates*> updates_;
  std::vector<rocksdb::ColumnFamilyHandle*> columns_;
  napi_ref priorityRef_;

 private:
  uint32_t priorityWork_ = 0;
};

struct BaseIterator {
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

  void Close() {
    snapshot_.reset();
    iterator_.reset();
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

    iterator_.reset(database_->db_->NewIterator(readOptions, column_));
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
    database_->AttachIterator(env, this);
  }

  void Detach(napi_env env) {
    database_->DetachIterator(env, this);
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

struct Updates {
  Updates(Database* database, bool values, bool keyAsBuffer, bool valueAsBuffer, int64_t seqNumber)
      : database_(database),
        values_(values),
        keyAsBuffer_(keyAsBuffer),
        valueAsBuffer_(valueAsBuffer),
        seqNumber_(seqNumber) {}

  void Close() { iterator_.reset(); }

  void Attach(napi_env env, napi_value context) {
    napi_create_reference(env, context, 1, &ref_);
    database_->AttachUpdates(env, this);
  }

  void Detach(napi_env env) {
    database_->DetachUpdates(env, this);
    if (ref_) {
      napi_delete_reference(env, ref_);
    }
  }

  Database* database_;
  const bool values_;
  const bool keyAsBuffer_;
  const bool valueAsBuffer_;
  int64_t seqNumber_;
  std::unique_ptr<rocksdb::TransactionLogIterator> iterator_;

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
    NAPI_STATUS_RETURN(napi_get_value_external(env, value, reinterpret_cast<void**>(column)));
  } else {
    *column = database->db_->DefaultColumnFamily();
  }

  return napi_ok;
}

/**
 * Hook for when the environment exits. This hook will be called after
 * already-scheduled napi_async_work items have finished, which gives us
 * the guarantee that no db operations will be in-flight at this time.
 */
static void env_cleanup_hook(void* arg) {
  auto database = reinterpret_cast<Database*>(arg);

  // Do everything that db_close() does but synchronously. We're expecting that
  // GC did not (yet) collect the database because that would be a user mistake
  // (not closing their db) made during the lifetime of the environment. That's
  // different from an environment being torn down (like the main process or a
  // worker thread) where it's our responsibility to clean up. Note also, the
  // following code must be a safe noop if called before db_open() or after
  // db_close().
  if (database && database->db_) {
    for (auto it : database->iterators_) {
      // TODO: does not do `napi_delete_reference`. Problem?
      it->Close();
    }

    for (auto it : database->updates_) {
      // TODO: does not do `napi_delete_reference`. Problem?
      it->Close();
    }

    for (auto it : database->columns_) {
      database->db_->DestroyColumnFamilyHandle(it);
    }

    // Having closed the iterators (and released snapshots) we can safely close.
    database->db_->Close();
  }
}

static void FinalizeDatabase(napi_env env, void* data, void* hint) {
  if (data) {
    auto database = reinterpret_cast<Database*>(data);
    napi_remove_env_cleanup_hook(env, env_cleanup_hook, database);
    if (database->priorityRef_)
      napi_delete_reference(env, database->priorityRef_);
    delete database;
  }
}

NAPI_METHOD(db_init) {
  auto database = new Database();
  napi_add_env_cleanup_hook(env, env_cleanup_hook, database);

  napi_value result;
  NAPI_STATUS_THROWS(napi_create_external(env, database, FinalizeDatabase, nullptr, &result));

  NAPI_STATUS_THROWS(napi_create_reference(env, result, 0, &database->priorityRef_));

  return result;
}

struct OpenWorker final : public Worker {
  OpenWorker(napi_env env,
             Database* database,
             napi_value callback,
             const std::string& location,
             const rocksdb::Options& options,
             std::vector<rocksdb::ColumnFamilyDescriptor> column_families)
      : Worker(env, database, callback, "leveldown.db.open"),
        options_(options),
        location_(location),
        column_families_(std::move(column_families)) {}

  rocksdb::Status Execute(Database& database) override {
    rocksdb::DB* db = nullptr;
    const auto status = column_families_.empty()
                            ? rocksdb::DB::Open(options_, location_, &db)
                            : rocksdb::DB::Open(options_, location_, column_families_, &database.columns_, &db);
    database.db_.reset(db);
    return status;
  }

  napi_status OnOk(napi_env env, napi_value callback) override {
    napi_value argv[2];
    NAPI_STATUS_RETURN(napi_get_null(env, &argv[0]));

    const auto size = database_->columns_.size();
    NAPI_STATUS_RETURN(napi_create_object(env, &argv[1]));

    for (size_t n = 0; n < size; ++n) {
      napi_value column;
      NAPI_STATUS_RETURN(napi_create_external(env, database_->columns_[n], nullptr, nullptr, &column));
      NAPI_STATUS_RETURN(napi_set_named_property(env, argv[1], column_families_[n].name.c_str(), column));
    }

    return CallFunction(env, callback, 2, argv);
  }

  rocksdb::Options options_;
  const std::string location_;
  std::vector<rocksdb::ColumnFamilyDescriptor> column_families_;
};

template <typename T, typename U>
rocksdb::Status InitOptions(napi_env env, T& columnOptions, const U& options) {
  const auto memtable_memory_budget = Uint32Property(env, options, "memtableMemoryBudget").value_or(256 * 1024 * 1024);

  const auto compaction = StringProperty(env, options, "compaction").value_or("level");

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

  columnOptions.compression =
      BooleanProperty(env, options, "compression").value_or((true)) ? rocksdb::kZSTD : rocksdb::kNoCompression;
  if (columnOptions.compression == rocksdb::kZSTD) {
    columnOptions.compression_opts.max_dict_bytes = 16 * 1024;
    columnOptions.compression_opts.zstd_max_train_bytes = 16 * 1024 * 100;
    // TODO (perf): compression_opts.parallel_threads
  }

  const auto prefixExtractorOpt = StringProperty(env, options, "prefixExtractor");
  if (prefixExtractorOpt) {
    rocksdb::ConfigOptions configOptions;
    ROCKS_STATUS_RETURN(
        rocksdb::SliceTransform::CreateFromString(configOptions, *prefixExtractorOpt, &columnOptions.prefix_extractor));
  }

  const auto comparatorOpt = StringProperty(env, options, "comparator");
  if (comparatorOpt) {
    rocksdb::ConfigOptions configOptions;
    ROCKS_STATUS_RETURN(
        rocksdb::Comparator::CreateFromString(configOptions, *comparatorOpt, &columnOptions.comparator));
  }

  const auto cacheSize = Uint32Property(env, options, "cacheSize").value_or(8 << 20);

  rocksdb::BlockBasedTableOptions tableOptions;

  if (cacheSize) {
    tableOptions.block_cache = rocksdb::NewLRUCache(cacheSize);
    tableOptions.cache_index_and_filter_blocks =
        BooleanProperty(env, options, "cacheIndexAndFilterBlocks").value_or(true);
  } else {
    tableOptions.no_block_cache = true;
    tableOptions.cache_index_and_filter_blocks = false;
  }

  const auto optimize = StringProperty(env, options, "optimize").value_or("");

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

  tableOptions.block_size = Uint32Property(env, options, "blockSize").value_or(4096);
  tableOptions.block_restart_interval = Uint32Property(env, options, "blockRestartInterval").value_or(16);
  tableOptions.format_version = 5;
  tableOptions.checksum = rocksdb::kXXH3;
  tableOptions.optimize_filters_for_memory = BooleanProperty(env, options, "optimizeFiltersForMemory").value_or(true);

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

  dbOptions.IncreaseParallelism(Uint32Property(env, argv[2], "parallelism")
                                    .value_or(std::max<uint32_t>(1, std::thread::hardware_concurrency() / 2)));

  dbOptions.create_if_missing = BooleanProperty(env, argv[2], "createIfMissing").value_or(true);
  dbOptions.error_if_exists = BooleanProperty(env, argv[2], "errorIfExists").value_or(false);
  dbOptions.avoid_unnecessary_blocking_io = true;
  dbOptions.write_dbid_to_manifest = true;
  dbOptions.use_adaptive_mutex = true;       // We don't have soo many threads in the libuv thread pool...
  dbOptions.enable_pipelined_write = false;  // We only write in the main thread...
  dbOptions.max_background_jobs = Uint32Property(env, argv[2], "maxBackgroundJobs")
                                      .value_or(std::max<uint32_t>(2, std::thread::hardware_concurrency() / 8));
  dbOptions.WAL_ttl_seconds = Uint32Property(env, argv[2], "walTTL").value_or(0) / 1e3;
  dbOptions.WAL_size_limit_MB = Uint32Property(env, argv[2], "walSizeLimit").value_or(0) / 1e6;
  dbOptions.create_missing_column_families = true;
  dbOptions.unordered_write = BooleanProperty(env, argv[2], "unorderedWrite").value_or(false);
  dbOptions.fail_if_options_file_error = true;
  dbOptions.wal_compression = BooleanProperty(env, argv[2], "walCompression").value_or(false)
                                  ? rocksdb::CompressionType::kZSTD
                                  : rocksdb::CompressionType::kNoCompression;

  const auto infoLogLevel = StringProperty(env, argv[2], "infoLogLevel").value_or("");
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

  std::vector<rocksdb::ColumnFamilyDescriptor> columnsFamilies;

  bool hasColumns;
  NAPI_STATUS_THROWS(napi_has_named_property(env, argv[2], "columns", &hasColumns));

  if (hasColumns) {
    napi_value columns;
    NAPI_STATUS_THROWS(napi_get_named_property(env, argv[2], "columns", &columns));

    napi_value keys;
    NAPI_STATUS_THROWS(napi_get_property_names(env, columns, &keys));

    uint32_t len;
    NAPI_STATUS_THROWS(napi_get_array_length(env, keys, &len));

    columnsFamilies.resize(len);
    for (uint32_t n = 0; n < len; ++n) {
      napi_value key;
      NAPI_STATUS_THROWS(napi_get_element(env, keys, n, &key));

      napi_value column;
      NAPI_STATUS_THROWS(napi_get_property(env, columns, key, &column));

      ROCKS_STATUS_THROWS(InitOptions(env, columnsFamilies[n].options, column));

      NAPI_STATUS_THROWS(ToString(env, key, columnsFamilies[n].name));
    }
  }

  auto worker = new OpenWorker(env, database, argv[3], location, dbOptions, columnsFamilies);
  worker->Queue(env);

  return 0;
}

struct CloseWorker final : public Worker {
  CloseWorker(napi_env env, Database* database, napi_value callback)
      : Worker(env, database, callback, "leveldown.db.close") {}

  rocksdb::Status Execute(Database& database) override {
    for (auto it : database.columns_) {
      database.db_->DestroyColumnFamilyHandle(it);
    }

    return database.db_->Close();
  }
};

napi_value noop_callback(napi_env env, napi_callback_info info) {
  return 0;
}

NAPI_METHOD(db_close) {
  NAPI_ARGV(2);

  Database* database;
  NAPI_STATUS_THROWS(napi_get_value_external(env, argv[0], reinterpret_cast<void**>(&database)));

  auto worker = new CloseWorker(env, database, argv[1]);

  if (!database->HasPriorityWork()) {
    worker->Queue(env);
  } else {
    database->pendingCloseWorker_ = worker;
  }

  return 0;
}

struct UpdatesNextWorker final : public rocksdb::WriteBatch::Handler, public Worker {
  UpdatesNextWorker(napi_env env, Updates* updates, napi_value callback)
      : Worker(env, updates->database_, callback, "rocks_level.db.get"), updates_(updates) {
    database_->IncrementPriorityWork(env);
  }

  rocksdb::Status Execute(Database& database) override {
    rocksdb::TransactionLogIterator::ReadOptions options;

    if (!updates_->iterator_) {
      const auto status = database_->db_->GetUpdatesSince(updates_->seqNumber_, &updates_->iterator_, options);
      if (!status.ok()) {
        return status;
      }
    } else {
      updates_->iterator_->Next();
    }

    if (!updates_->iterator_->Valid()) {
      return updates_->iterator_->status();
    }

    auto batch = updates_->iterator_->GetBatch();

    updates_->seqNumber_ = batch.sequence;

    cache_.reserve(batch.writeBatchPtr->Count() * 2);

    return batch.writeBatchPtr->Iterate(this);
  }

  napi_status OnOk(napi_env env, napi_value callback) override {
    napi_value argv[3];
    NAPI_STATUS_RETURN(napi_get_null(env, &argv[0]));

    if (cache_.empty()) {
      return CallFunction(env, callback, 1, argv);
    }

    NAPI_STATUS_RETURN(napi_create_array_with_length(env, cache_.size(), &argv[1]));

    for (size_t idx = 0; idx < cache_.size(); idx += 2) {
      napi_value key;
      NAPI_STATUS_RETURN(Convert(env, cache_[idx + 0], updates_->keyAsBuffer_, key));
      NAPI_STATUS_RETURN(napi_set_element(env, argv[1], static_cast<int>(idx + 0), key));

      napi_value val;
      NAPI_STATUS_RETURN(Convert(env, cache_[idx + 1], updates_->valueAsBuffer_, val));
      NAPI_STATUS_RETURN(napi_set_element(env, argv[1], static_cast<int>(idx + 1), val));
    }

    NAPI_STATUS_RETURN(napi_create_bigint_int64(env, updates_->seqNumber_, &argv[2]));

    return CallFunction(env, callback, 3, argv);
  }

  void Destroy(napi_env env) override {
    database_->DecrementPriorityWork(env);
    Worker::Destroy(env);
  }

  void Put(const rocksdb::Slice& key, const rocksdb::Slice& value) override {
    cache_.emplace_back(key.ToString());
    if (updates_->values_) {
      cache_.emplace_back(value.ToString());
    } else {
      cache_.emplace_back(std::nullopt);
    }
  }

  void Delete(const rocksdb::Slice& key) override {
    cache_.emplace_back(key.ToString());
    cache_.emplace_back(std::nullopt);
  }

  bool Continue() override { return true; }

 private:
  std::vector<std::optional<std::string>> cache_;
  Updates* updates_;
};

NAPI_METHOD(updates_init) {
  NAPI_ARGV(2);

  Database* database;
  NAPI_STATUS_THROWS(napi_get_value_external(env, argv[0], reinterpret_cast<void**>(&database)));

  const auto values = BooleanProperty(env, argv[1], "values").value_or(true);
  const bool keyAsBuffer = EncodingIsBuffer(env, argv[1], "keyEncoding");
  const bool valueAsBuffer = EncodingIsBuffer(env, argv[1], "valueEncoding");
  const auto seqNumber = Int64Property(env, argv[1], "since").value_or(database->db_->GetLatestSequenceNumber());

  auto updates = std::make_unique<Updates>(database, values, keyAsBuffer, valueAsBuffer, seqNumber);

  napi_value result;
  NAPI_STATUS_THROWS(napi_create_external(env, updates.get(), Finalize<Updates>, updates.get(), &result));

  // Prevent GC of JS object before the iterator is closed (explicitly or on
  // db close) and keep track of non-closed iterators to end them on db close.
  updates.release()->Attach(env, result);

  return result;
}

NAPI_METHOD(updates_next) {
  NAPI_ARGV(2);

  Updates* updates;
  NAPI_STATUS_THROWS(napi_get_value_external(env, argv[0], (void**)&updates));

  auto worker = new UpdatesNextWorker(env, updates, argv[1]);
  worker->Queue(env);

  return 0;
}

NAPI_METHOD(updates_close) {
  NAPI_ARGV(1);

  Updates* updates;
  NAPI_STATUS_THROWS(napi_get_value_external(env, argv[0], (void**)&updates));

  updates->Detach(env);
  updates->Close();

  return 0;
}

NAPI_METHOD(db_put) {
  NAPI_ARGV(4);

  Database* database;
  NAPI_STATUS_THROWS(napi_get_value_external(env, argv[0], reinterpret_cast<void**>(&database)));

  std::string key;
  NAPI_STATUS_THROWS(ToString(env, argv[1], key));

  std::string val;
  NAPI_STATUS_THROWS(ToString(env, argv[2], val));

  rocksdb::WriteOptions writeOptions;
  ROCKS_STATUS_THROWS(database->db_->Put(writeOptions, key, val));

  return 0;
}

struct GetWorker final : public Worker {
  GetWorker(napi_env env,
            Database* database,
            rocksdb::ColumnFamilyHandle* column,
            napi_value callback,
            const std::string& key,
            const bool asBuffer,
            const bool fillCache)
      : Worker(env, database, callback, "rocks_level.db.get"),
        column_(column),
        key_(key),
        asBuffer_(asBuffer),
        fillCache_(fillCache),
        snapshot_(database_->db_->GetSnapshot(),
                  [this](const rocksdb::Snapshot* ptr) { database_->db_->ReleaseSnapshot(ptr); }) {
    database_->IncrementPriorityWork(env);
  }

  rocksdb::Status Execute(Database& database) override {
    rocksdb::ReadOptions readOptions;
    readOptions.fill_cache = fillCache_;
    readOptions.snapshot = snapshot_.get();

    auto status = database.db_->Get(readOptions, column_, key_, &value_);

    key_.clear();
    snapshot_ = nullptr;

    return status;
  }

  napi_status OnOk(napi_env env, napi_value callback) override {
    napi_value argv[2];
    NAPI_STATUS_RETURN(napi_get_null(env, &argv[0]));

    NAPI_STATUS_RETURN(Convert(env, &value_, asBuffer_, argv[1]));

    return CallFunction(env, callback, 2, argv);
  }

  void Destroy(napi_env env) override {
    database_->DecrementPriorityWork(env);
    Worker::Destroy(env);
  }

 private:
  rocksdb::ColumnFamilyHandle* column_;
  std::string key_;
  rocksdb::PinnableSlice value_;
  const bool asBuffer_;
  const bool fillCache_;
  std::shared_ptr<const rocksdb::Snapshot> snapshot_;
};

NAPI_METHOD(db_get) {
  NAPI_ARGV(4);

  Database* database;
  NAPI_STATUS_THROWS(napi_get_value_external(env, argv[0], reinterpret_cast<void**>(&database)));

  std::string key;
  NAPI_STATUS_THROWS(ToString(env, argv[1], key));

  const auto asBuffer = EncodingIsBuffer(env, argv[2], "valueEncoding");
  const auto fillCache = BooleanProperty(env, argv[2], "fillCache").value_or(true);

  rocksdb::ColumnFamilyHandle* column;
  NAPI_STATUS_THROWS(GetColumnFamily(database, env, argv[2], &column));

  auto worker = new GetWorker(env, database, column, argv[3], key, asBuffer, fillCache);
  worker->Queue(env);

  return 0;
}

struct GetManyWorker final : public Worker {
  GetManyWorker(napi_env env,
                Database* database,
                rocksdb::ColumnFamilyHandle* column,
                std::vector<std::string> keys,
                napi_value callback,
                const bool valueAsBuffer,
                const bool fillCache,
                const bool ignoreRangeDeletions)
      : Worker(env, database, callback, "leveldown.get.many"),
        column_(column),
        keys_(std::move(keys)),
        valueAsBuffer_(valueAsBuffer),
        fillCache_(fillCache),
        ignoreRangeDeletions_(ignoreRangeDeletions),
        snapshot_(database_->db_->GetSnapshot()) {
    database_->IncrementPriorityWork(env);
  }

  ~GetManyWorker() { database_->db_->ReleaseSnapshot(snapshot_); }

  rocksdb::Status Execute(Database& database) override {
    rocksdb::ReadOptions readOptions;
    readOptions.fill_cache = fillCache_;
    readOptions.snapshot = snapshot_;
    readOptions.async_io = true;
    readOptions.ignore_range_deletions = ignoreRangeDeletions_;

    std::vector<rocksdb::Slice> keys;
    keys.reserve(keys_.size());
    for (const auto& key : keys_) {
      keys.emplace_back(key);
    }

    statuses_.resize(keys.size());
    values_.resize(keys.size());

    database.db_->MultiGet(readOptions, column_, keys.size(), keys.data(), values_.data(), statuses_.data());

    for (const auto& status : statuses_) {
      if (!status.ok() && !status.IsNotFound()) {
        return status;
      }
    }

    return rocksdb::Status::OK();
  }

  napi_status OnOk(napi_env env, napi_value callback) override {
    napi_value argv[2];
    NAPI_STATUS_RETURN(napi_get_null(env, &argv[0]));

    const auto size = values_.size();

    NAPI_STATUS_RETURN(napi_create_array_with_length(env, size, &argv[1]));

    for (size_t idx = 0; idx < size; idx++) {
      napi_value element;
      if (statuses_[idx].ok()) {
        NAPI_STATUS_RETURN(Convert(env, &values_[idx], valueAsBuffer_, element));
      } else {
        NAPI_STATUS_RETURN(napi_get_undefined(env, &element));
      }
      NAPI_STATUS_RETURN(napi_set_element(env, argv[1], static_cast<uint32_t>(idx), element));
    }

    return CallFunction(env, callback, 2, argv);
  }

  void Destroy(napi_env env) override {
    database_->DecrementPriorityWork(env);
    Worker::Destroy(env);
  }

 private:
  rocksdb::ColumnFamilyHandle* column_;
  std::vector<std::string> keys_;
  std::vector<rocksdb::PinnableSlice> values_;
  std::vector<rocksdb::Status> statuses_;
  const bool valueAsBuffer_;
  const bool fillCache_;
  const bool ignoreRangeDeletions_;
  const rocksdb::Snapshot* snapshot_;
};

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

  const bool asBuffer = EncodingIsBuffer(env, argv[2], "valueEncoding");
  const bool fillCache = BooleanProperty(env, argv[2], "fillCache").value_or(true);
  const bool ignoreRangeDeletions = BooleanProperty(env, argv[2], "ignoreRangeDeletions").value_or(false);

  rocksdb::ColumnFamilyHandle* column;
  NAPI_STATUS_THROWS(GetColumnFamily(database, env, argv[2], &column));

  auto worker =
      new GetManyWorker(env, database, column, std::move(keys), argv[3], asBuffer, fillCache, ignoreRangeDeletions);
  worker->Queue(env);

  return 0;
}

NAPI_METHOD(db_del) {
  NAPI_ARGV(3);

  Database* database;
  NAPI_STATUS_THROWS(napi_get_value_external(env, argv[0], reinterpret_cast<void**>(&database)));

  std::string key;
  NAPI_STATUS_THROWS(ToString(env, argv[1], key));

  rocksdb::ColumnFamilyHandle* column;
  NAPI_STATUS_THROWS(GetColumnFamily(database, env, argv[2], &column));

  rocksdb::WriteOptions writeOptions;
  ROCKS_STATUS_THROWS(database->db_->Delete(writeOptions, column, key));

  return 0;
}

NAPI_METHOD(db_clear) {
  NAPI_ARGV(2);

  Database* database;
  NAPI_STATUS_THROWS(napi_get_value_external(env, argv[0], reinterpret_cast<void**>(&database)));

  const auto reverse = BooleanProperty(env, argv[1], "reverse").value_or(false);
  const auto limit = Int32Property(env, argv[1], "limit").value_or(-1);

  rocksdb::ColumnFamilyHandle* column;
  NAPI_STATUS_THROWS(GetColumnFamily(database, env, argv[1], &column));

  auto lt = StringProperty(env, argv[1], "lt");
  auto lte = StringProperty(env, argv[1], "lte");
  auto gt = StringProperty(env, argv[1], "gt");
  auto gte = StringProperty(env, argv[1], "gte");

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
      ROCKS_STATUS_THROWS(database->db_->DeleteRange(writeOptions, column, begin, end));
    }

    return 0;
  } else {
    // TODO (fix): Error handling.
    // TODO (fix): This should be async...

    std::shared_ptr<const rocksdb::Snapshot> snapshot(database->db_->GetSnapshot(),
                                                      [=](const auto ptr) { database->db_->ReleaseSnapshot(ptr); });
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

      status = database->db_->Write(writeOptions, &batch);
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

  std::string property;
  NAPI_STATUS_THROWS(ToString(env, argv[1], property));

  std::string value;
  database->db_->GetProperty(property, &value);

  napi_value result;
  NAPI_STATUS_THROWS(napi_create_string_utf8(env, value.data(), value.size(), &result));

  return result;
}

NAPI_METHOD(iterator_init) {
  NAPI_ARGV(2);

  Database* database;
  NAPI_STATUS_THROWS(napi_get_value_external(env, argv[0], reinterpret_cast<void**>(&database)));

  const auto options = argv[1];
  const auto reverse = BooleanProperty(env, options, "reverse").value_or(false);
  const auto keys = BooleanProperty(env, options, "keys").value_or(true);
  const auto values = BooleanProperty(env, options, "values").value_or(true);
  const auto fillCache = BooleanProperty(env, options, "fillCache").value_or(false);
  const bool keyAsBuffer = EncodingIsBuffer(env, options, "keyEncoding");
  const bool valueAsBuffer = EncodingIsBuffer(env, options, "valueEncoding");
  const auto limit = Int32Property(env, options, "limit").value_or(-1);
  const auto highWaterMarkBytes = Uint32Property(env, options, "highWaterMarkBytes").value_or(64 * 1024);

  const auto lt = StringProperty(env, options, "lt");
  const auto lte = StringProperty(env, options, "lte");
  const auto gt = StringProperty(env, options, "gt");
  const auto gte = StringProperty(env, options, "gte");

  rocksdb::ColumnFamilyHandle* column;
  NAPI_STATUS_THROWS(GetColumnFamily(database, env, options, &column));

  std::shared_ptr<const rocksdb::Snapshot> snapshot(database->db_->GetSnapshot(),
                                                    [=](const auto ptr) { database->db_->ReleaseSnapshot(ptr); });

  auto iterator = std::make_unique<Iterator>(database, column, reverse, keys, values, limit, lt, lte, gt, gte,
                                             fillCache, keyAsBuffer, valueAsBuffer, highWaterMarkBytes, snapshot);

  napi_value result;
  NAPI_STATUS_THROWS(napi_create_external(env, iterator.get(), Finalize<Iterator>, iterator.get(), &result));

  // Prevent GC of JS object before the iterator is closed (explicitly or on
  // db close) and keep track of non-closed iterators to end them on db close.
  iterator.release()->Attach(env, result);

  return result;
}

NAPI_METHOD(iterator_seek) {
  NAPI_ARGV(2);

  Iterator* iterator;
  NAPI_STATUS_THROWS(napi_get_value_external(env, argv[0], reinterpret_cast<void**>(&iterator)));

  std::string target;
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
  NAPI_STATUS_THROWS(napi_create_bigint_int64(env, seq, &result));

  return 0;
}

struct NextWorker final : public Worker {
  NextWorker(napi_env env, Iterator* iterator, uint32_t size, napi_value callback)
      : Worker(env, iterator->database_, callback, "leveldown.iterator.next"), iterator_(iterator), size_(size) {}

  rocksdb::Status Execute(Database& database) override {
    if (!iterator_->DidSeek()) {
      iterator_->SeekToRange();
    }

    cache_.reserve(size_ * 2);
    size_t bytesRead = 0;

    while (true) {
      if (!iterator_->first_)
        iterator_->Next();
      else
        iterator_->first_ = false;

      if (!iterator_->Valid() || !iterator_->Increment())
        break;

      if (iterator_->keys_ && iterator_->values_) {
        auto k = iterator_->CurrentKey();
        auto v = iterator_->CurrentValue();
        bytesRead += k.size() + v.size();
        cache_.push_back(k.ToString());
        cache_.push_back(v.ToString());
      } else if (iterator_->keys_) {
        auto k = iterator_->CurrentKey();
        bytesRead += k.size();
        cache_.push_back(k.ToString());
        cache_.push_back(std::nullopt);
      } else if (iterator_->values_) {
        auto v = iterator_->CurrentValue();
        bytesRead += v.size();
        cache_.push_back(std::nullopt);
        cache_.push_back(v.ToString());
      }

      if (bytesRead > iterator_->highWaterMarkBytes_ || cache_.size() / 2 >= size_) {
        finished_ = false;
        return rocksdb::Status::OK();
      }
    }

    finished_ = true;

    return iterator_->Status();
  }

  napi_status OnOk(napi_env env, napi_value callback) override {
    napi_value argv[3];
    NAPI_STATUS_RETURN(napi_get_null(env, &argv[0]));

    NAPI_STATUS_RETURN(napi_create_array_with_length(env, cache_.size(), &argv[1]));

    for (size_t n = 0; n < cache_.size(); n += 2) {
      napi_value key;
      napi_value val;

      NAPI_STATUS_RETURN(Convert(env, cache_[n + 0], iterator_->keyAsBuffer_, key));
      NAPI_STATUS_RETURN(Convert(env, cache_[n + 1], iterator_->valueAsBuffer_, val));

      NAPI_STATUS_RETURN(napi_set_element(env, argv[1], static_cast<int>(n + 0), key));
      NAPI_STATUS_RETURN(napi_set_element(env, argv[1], static_cast<int>(n + 1), val));
    }

    NAPI_STATUS_RETURN(napi_get_boolean(env, finished_, &argv[2]));

    return CallFunction(env, callback, 3, argv);
  }

 private:
  std::vector<std::optional<std::string>> cache_;
  Iterator* iterator_ = nullptr;
  uint32_t size_ = 0;
  bool finished_ = true;
};

NAPI_METHOD(iterator_nextv) {
  NAPI_ARGV(3);

  Iterator* iterator;
  NAPI_STATUS_THROWS(napi_get_value_external(env, argv[0], reinterpret_cast<void**>(&iterator)));

  uint32_t size;
  NAPI_STATUS_THROWS(napi_get_value_uint32(env, argv[1], &size));

  auto worker = new NextWorker(env, iterator, size, argv[2]);
  worker->Queue(env);

  return 0;
}

NAPI_METHOD(batch_do) {
  NAPI_ARGV(3);

  Database* database;
  NAPI_STATUS_THROWS(napi_get_value_external(env, argv[0], reinterpret_cast<void**>(&database)));

  rocksdb::WriteBatch batch;

  std::string type;
  std::string key;
  std::string value;

  uint32_t length;
  NAPI_STATUS_THROWS(napi_get_array_length(env, argv[1], &length));

  for (uint32_t i = 0; i < length; i++) {
    napi_value element;
    NAPI_STATUS_THROWS(napi_get_element(env, argv[1], i, &element));

    napi_value typeProperty;
    NAPI_STATUS_THROWS(napi_get_named_property(env, element, "type", &typeProperty));
    NAPI_STATUS_THROWS(ToString(env, typeProperty, type));

    rocksdb::ColumnFamilyHandle* column;
    NAPI_STATUS_THROWS(GetColumnFamily(database, env, element, &column));

    if (type == "del") {
      napi_value keyProperty;
      NAPI_STATUS_THROWS(napi_get_named_property(env, element, "key", &keyProperty));
      NAPI_STATUS_THROWS(ToString(env, keyProperty, key));

      ROCKS_STATUS_THROWS(batch.Delete(column, key));
    } else if (type == "put") {
      napi_value keyProperty;
      NAPI_STATUS_THROWS(napi_get_named_property(env, element, "key", &keyProperty));
      NAPI_STATUS_THROWS(ToString(env, keyProperty, key));

      napi_value valueProperty;
      NAPI_STATUS_THROWS(napi_get_named_property(env, element, "value", &valueProperty));
      NAPI_STATUS_THROWS(ToString(env, valueProperty, value));

      ROCKS_STATUS_THROWS(batch.Put(column, key, value));
    } else {
      ROCKS_STATUS_THROWS(rocksdb::Status::InvalidArgument());
    }
  }

  rocksdb::WriteOptions writeOptions;
  ROCKS_STATUS_THROWS(database->db_->Write(writeOptions, &batch));

  return 0;
}

NAPI_METHOD(batch_init) {
  NAPI_ARGV(1);

  Database* database;
  NAPI_STATUS_THROWS(napi_get_value_external(env, argv[0], reinterpret_cast<void**>(&database)));

  auto batch = new rocksdb::WriteBatch();

  napi_value result;
  NAPI_STATUS_THROWS(napi_create_external(env, batch, Finalize<rocksdb::WriteBatch>, batch, &result));

  return result;
}

NAPI_METHOD(batch_put) {
  NAPI_ARGV(5);

  Database* database;
  NAPI_STATUS_THROWS(napi_get_value_external(env, argv[0], reinterpret_cast<void**>(&database)));

  rocksdb::WriteBatch* batch;
  NAPI_STATUS_THROWS(napi_get_value_external(env, argv[1], (void**)(&batch)));

  std::string key;
  NAPI_STATUS_THROWS(ToString(env, argv[2], key));

  std::string val;
  NAPI_STATUS_THROWS(ToString(env, argv[3], val));

  rocksdb::ColumnFamilyHandle* column;
  NAPI_STATUS_THROWS(GetColumnFamily(database, env, argv[4], &column));

  ROCKS_STATUS_THROWS(batch->Put(column, key, val));

  return 0;
}

NAPI_METHOD(batch_del) {
  NAPI_ARGV(4);

  Database* database;
  NAPI_STATUS_THROWS(napi_get_value_external(env, argv[0], reinterpret_cast<void**>(&database)));

  rocksdb::WriteBatch* batch;
  NAPI_STATUS_THROWS(napi_get_value_external(env, argv[1], reinterpret_cast<void**>(&batch)));

  std::string key;
  NAPI_STATUS_THROWS(ToString(env, argv[2], key));

  rocksdb::ColumnFamilyHandle* column;
  NAPI_STATUS_THROWS(GetColumnFamily(database, env, argv[3], &column));

  ROCKS_STATUS_THROWS(batch->Delete(column, key));

  return 0;
}

NAPI_METHOD(batch_clear) {
  NAPI_ARGV(2);

  rocksdb::WriteBatch* batch;
  NAPI_STATUS_THROWS(napi_get_value_external(env, argv[1], reinterpret_cast<void**>(&batch)));

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
  ROCKS_STATUS_THROWS(database->db_->Write(writeOptions, batch));

  return 0;
}

NAPI_INIT() {
  NAPI_EXPORT_FUNCTION(db_init);
  NAPI_EXPORT_FUNCTION(db_open);
  NAPI_EXPORT_FUNCTION(db_close);
  NAPI_EXPORT_FUNCTION(db_put);
  NAPI_EXPORT_FUNCTION(db_get);
  NAPI_EXPORT_FUNCTION(db_get_many);
  NAPI_EXPORT_FUNCTION(db_del);
  NAPI_EXPORT_FUNCTION(db_clear);
  NAPI_EXPORT_FUNCTION(db_get_property);

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
}
