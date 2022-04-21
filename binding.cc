#define NAPI_VERSION 8

#include <napi-macros.h>
#include <node_api.h>
#include <assert.h>

#include <rocksdb/db.h>
#include <rocksdb/write_batch.h>
#include <rocksdb/cache.h>
#include <rocksdb/filter_policy.h>
#include <rocksdb/cache.h>
#include <rocksdb/comparator.h>
#include <rocksdb/env.h>
#include <rocksdb/options.h>
#include <rocksdb/table.h>

namespace leveldb = rocksdb;

#include <set>
#include <memory>
#include <string>
#include <vector>

class NullLogger : public rocksdb::Logger {
public:
  using rocksdb::Logger::Logv;
  virtual void Logv(const char* format, va_list ap) override {}
  virtual size_t GetLogFileSize() const override { return 0; }
};

struct Database;
struct Iterator;
static void iterator_do_close (napi_env env, Iterator* iterator, napi_value cb);

#define NAPI_DB_CONTEXT() \
  Database* database = nullptr; \
  NAPI_STATUS_THROWS(napi_get_value_external(env, argv[0], (void**)&database));

#define NAPI_ITERATOR_CONTEXT() \
  Iterator* iterator = nullptr; \
  NAPI_STATUS_THROWS(napi_get_value_external(env, argv[0], (void**)&iterator));

#define NAPI_BATCH_CONTEXT() \
  leveldb::WriteBatch* batch = nullptr; \
  NAPI_STATUS_THROWS(napi_get_value_external(env, argv[0], (void**)&batch));

static bool IsString (napi_env env, napi_value value) {
  napi_valuetype type;
  napi_typeof(env, value, &type);
  return type == napi_string;
}

static bool IsBuffer (napi_env env, napi_value value) {
  bool isBuffer;
  napi_is_buffer(env, value, &isBuffer);
  return isBuffer;
}

static bool IsObject (napi_env env, napi_value value) {
  napi_valuetype type;
  napi_typeof(env, value, &type);
  return type == napi_object;
}

static napi_value CreateError (napi_env env, const std::string& str) {
  napi_value msg;
  napi_create_string_utf8(env, str.data(), str.size(), &msg);
  napi_value error;
  napi_create_error(env, nullptr, msg, &error);
  return error;
}

static napi_value CreateCodeError (napi_env env, const std::string& code, const std::string& msg) {
  napi_value codeValue;
  napi_create_string_utf8(env, code.data(), code.size(), &codeValue);
  napi_value msgValue;
  napi_create_string_utf8(env, msg.data(), msg.size(), &msgValue);
  napi_value error;
  napi_create_error(env, codeValue, msgValue, &error);
  return error;
}

static bool HasProperty (napi_env env, napi_value obj, const std::string& key) {
  bool has = false;
  napi_has_named_property(env, obj, key.data(), &has);
  return has;
}

static napi_value GetProperty (napi_env env, napi_value obj, const std::string& key) {
  napi_value value;
  napi_get_named_property(env, obj, key.data(), &value);
  return value;
}

static bool BooleanProperty (napi_env env, napi_value obj, const std::string& key,
                             bool defaultValue) {
  if (HasProperty(env, obj, key.data())) {
    const auto value = GetProperty(env, obj, key.data());
    bool result;
    napi_get_value_bool(env, value, &result);
    return result;
  }

  return defaultValue;
}

static bool EncodingIsBuffer (napi_env env, napi_value obj, const std::string& option) {
  napi_value value;
  size_t size;

  if (napi_get_named_property(env, obj, option.data(), &value) == napi_ok &&
    napi_get_value_string_utf8(env, value, nullptr, 0, &size) == napi_ok) {
    // Value is either "buffer" or "utf8" so we can tell them apart just by size
    return size == 6;
  }

  return false;
}

static uint32_t Uint32Property (napi_env env, napi_value obj, const std::string& key,
                                uint32_t defaultValue) {
  if (HasProperty(env, obj, key.data())) {
    const auto value = GetProperty(env, obj, key.data());
    uint32_t result;
    napi_get_value_uint32(env, value, &result);
    return result;
  }

  return defaultValue;
}

static int Int32Property (napi_env env, napi_value obj, const std::string& key,
                          int defaultValue) {
  if (HasProperty(env, obj, key.data())) {
    const auto value = GetProperty(env, obj, key.data());
    int result;
    napi_get_value_int32(env, value, &result);
    return result;
  }

  return defaultValue;
}

static std::string ToString (napi_env env, napi_value from, const std::string& defaultValue = "") {
  if (IsString(env, from)) {
    size_t length = 0;
    napi_get_value_string_utf8(env, from, nullptr, 0, &length);
    std::string value(length, '\0');
    napi_get_value_string_utf8( env, from, &value[0], value.length() + 1, &length);
    return value;
  } else if (IsBuffer(env, from)) {
    char* buf = nullptr;
    size_t length = 0;
    napi_get_buffer_info(env, from, reinterpret_cast<void **>(&buf), &length);
    return std::string(buf, length);
  }

  return defaultValue;
}

static std::string StringProperty (napi_env env, napi_value obj, const std::string& key, const std::string& defaultValue = "") {
  if (HasProperty(env, obj, key)) {
    napi_value value = GetProperty(env, obj, key);
    if (IsString(env, value)) {
      return ToString(env, value);
    }
  }

  return defaultValue;
}

static size_t StringOrBufferLength (napi_env env, napi_value value) {
  size_t size = 0;

  if (IsString(env, value)) {
    napi_get_value_string_utf8(env, value, nullptr, 0, &size);
  } else if (IsBuffer(env, value)) {
    char* buf = nullptr;
    napi_get_buffer_info(env, value, (void **)&buf, &size);
  }

  return size;
}

static std::string* RangeOption (napi_env env, napi_value opts, const std::string& name) {
  if (HasProperty(env, opts, name)) {
    const auto value = GetProperty(env, opts, name);
    return new std::string(ToString(env, value));
  }

  return nullptr;
}

static std::vector<std::string> KeyArray (napi_env env, napi_value arr) {
  uint32_t length;
  std::vector<std::string> result;

  if (napi_get_array_length(env, arr, &length) == napi_ok) {
    result.reserve(length);

    for (uint32_t i = 0; i < length; i++) {
      napi_value element;

      if (napi_get_element(env, arr, i, &element) == napi_ok &&
          StringOrBufferLength(env, element) > 0) {
        result.push_back(ToString(env, element));
      }
    }
  }

  return result;
}

static napi_status CallFunction (napi_env env,
                                 napi_value callback,
                                 const int argc,
                                 napi_value* argv) {
  napi_value global;
  napi_get_global(env, &global);
  return napi_call_function(env, global, callback, argc, argv, nullptr);
}

template <typename T>
void Convert (napi_env env, const T& s, bool asBuffer, napi_value& result) {
  if (asBuffer) {
    napi_create_buffer_copy(env, s.size(), s.data(), nullptr, &result);
  } else {
    napi_create_string_utf8(env, s.data(), s.size(), &result);
  }
}

/**
 * Base worker class. Handles the async work. Derived classes can override the
 * following virtual methods (listed in the order in which they're called):
 *
 * - DoExecute (abstract, worker pool thread): main work
 * - HandleOKCallback (main thread): call JS callback on success
 * - HandleErrorCallback (main thread): call JS callback on error
 * - DoFinally (main thread): do cleanup regardless of success
 */
struct BaseWorker {
  // Note: storing env is discouraged as we'd end up using it in unsafe places.
  BaseWorker (napi_env env,
              Database* database,
              napi_value callback,
              const char* resourceName)
    : database_(database) {
    NAPI_STATUS_THROWS_VOID(napi_create_reference(env, callback, 1, &callbackRef_));
    napi_value asyncResourceName;
    NAPI_STATUS_THROWS_VOID(napi_create_string_utf8(env, resourceName,
                                               NAPI_AUTO_LENGTH,
                                               &asyncResourceName));
    NAPI_STATUS_THROWS_VOID(napi_create_async_work(env, callback,
                                              asyncResourceName,
                                              BaseWorker::Execute,
                                              BaseWorker::Complete,
                                              this, &asyncWork_));
  }

  virtual ~BaseWorker () {}

  static void Execute (napi_env env, void* data) {
    auto self = reinterpret_cast<BaseWorker*>(data);

    // Don't pass env to DoExecute() because use of Node-API
    // methods should generally be avoided in async work.
    self->DoExecute();
  }

  bool SetStatus (const leveldb::Status& status) {
    status_ = status;
    return status.ok();
  }

  virtual void DoExecute () = 0;

  static void Complete (napi_env env, napi_status status, void* data) {
    auto self = reinterpret_cast<BaseWorker*>(data);

    self->DoComplete(env);
    self->DoFinally(env);
  }

  void DoComplete (napi_env env) {
    napi_value callback;
    napi_get_reference_value(env, callbackRef_, &callback);

    if (status_.ok()) {
      HandleOKCallback(env, callback);
    } else {
      HandleErrorCallback(env, callback);
    }
  }

  virtual void HandleOKCallback (napi_env env, napi_value callback) {
    napi_value argv;
    napi_get_null(env, &argv);
    CallFunction(env, callback, 1, &argv);
  }

  virtual void HandleErrorCallback (napi_env env, napi_value callback) {
    napi_value argv;

    const auto msg = status_.ToString();

    if (status_.IsNotFound()) {
      argv = CreateCodeError(env, "LEVEL_NOT_FOUND", msg);
    } else if (status_.IsCorruption()) {
      argv = CreateCodeError(env, "LEVEL_CORRUPTION", msg);
    } else if (status_.IsIOError()) {
      if (msg.find("IO error: lock ") != std::string::npos) { // env_posix.cc
        argv = CreateCodeError(env, "LEVEL_LOCKED", msg);
      } else if (msg.find("IO error: LockFile ") != std::string::npos) { // env_win.cc
        argv = CreateCodeError(env, "LEVEL_LOCKED", msg);
      } else if (msg.find("IO error: While lock file") != std::string::npos) { // env_mac.cc
        argv = CreateCodeError(env, "LEVEL_LOCKED", msg);
      } else {
        argv = CreateCodeError(env, "LEVEL_IO_ERROR", msg);
      }
    } else {
      argv = CreateError(env, msg);
    }

    CallFunction(env, callback, 1, &argv);
  }

  virtual void DoFinally (napi_env env) {
    napi_delete_reference(env, callbackRef_);
    napi_delete_async_work(env, asyncWork_);

    delete this;
  }

  void Queue (napi_env env) {
    napi_queue_async_work(env, asyncWork_);
  }

  Database* database_;

private:
  napi_ref callbackRef_;
  napi_async_work asyncWork_;
  leveldb::Status status_;
};

/**
 * Owns the LevelDB storage, cache, filter policy and iterators.
 */
struct Database {
  Database ()
    : pendingCloseWorker_(nullptr),
      ref_(nullptr),
      priorityWork_(0) {}

  leveldb::Status Open (const leveldb::Options& options,
                        const bool readOnly,
                        const char* location) {
    if (readOnly) {
      leveldb::DB* db = nullptr;
      const auto status = rocksdb::DB::OpenForReadOnly(options, location, &db);
      db_.reset(db);
      return status;
    } else {
      leveldb::DB* db = nullptr;
      const auto status = leveldb::DB::Open(options, location, &db);
      db_.reset(db);
      return status;
    }
  }

  void CloseDatabase () {
    db_.reset();
  }

  leveldb::Status Put (const leveldb::WriteOptions& options,
                       const std::string& key,
                       const std::string& value) {
    return db_->Put(options, db_->DefaultColumnFamily(), key, value);
  }

  leveldb::Status Get (const leveldb::ReadOptions& options,
                       const std::string& key,
                       rocksdb::PinnableSlice& value) {
    return db_->Get(options, db_->DefaultColumnFamily(), key, &value);
  }

  leveldb::Status Del (const leveldb::WriteOptions& options,
                       const std::string& key) {
    return db_->Delete(options, db_->DefaultColumnFamily(), key);
  }

  leveldb::Status WriteBatch (const leveldb::WriteOptions& options,
                              leveldb::WriteBatch* batch) {
    return db_->Write(options, batch);
  }

  uint64_t ApproximateSize (const leveldb::Range* range) {
    uint64_t size = 0;
    db_->GetApproximateSizes(range, 1, &size);
    return size;
  }

  void CompactRange (const leveldb::Slice* start,
                     const leveldb::Slice* end) {
    rocksdb::CompactRangeOptions options;
    db_->CompactRange(options, start, end);
  }

  void GetProperty (const std::string& property, std::string& value) {
    db_->GetProperty(property, &value);
  }

  const leveldb::Snapshot* NewSnapshot () {
    return db_->GetSnapshot();
  }

  leveldb::Iterator* NewIterator (const leveldb::ReadOptions& options) {
    return db_->NewIterator(options);
  }

  void ReleaseSnapshot (const leveldb::Snapshot* snapshot) {
    return db_->ReleaseSnapshot(snapshot);
  }

  void AttachIterator (napi_env env, Iterator* iterator) {
    iterators_.insert(iterator);
    IncrementPriorityWork(env);
  }

  void DetachIterator (napi_env env, Iterator* iterator) {
    iterators_.erase(iterator);
    DecrementPriorityWork(env);
  }

  void IncrementPriorityWork (napi_env env) {
    napi_reference_ref(env, ref_, &priorityWork_);
  }

  void DecrementPriorityWork (napi_env env) {
    napi_reference_unref(env, ref_, &priorityWork_);

    if (priorityWork_ == 0 && pendingCloseWorker_) {
      pendingCloseWorker_->Queue(env);
      pendingCloseWorker_ = nullptr;
    }
  }

  bool HasPriorityWork () const {
    return priorityWork_ > 0;
  }

  std::unique_ptr<leveldb::DB> db_;
  BaseWorker* pendingCloseWorker_;
  std::set<Iterator*> iterators_;
  napi_ref ref_;

private:
  uint32_t priorityWork_;
};

/**
 * Base worker class for doing async work that defers closing the database.
 */
struct PriorityWorker : public BaseWorker {
  PriorityWorker (napi_env env, Database* database, napi_value callback, const char* resourceName)
    : BaseWorker(env, database, callback, resourceName) {
      database_->IncrementPriorityWork(env);
  }

  virtual ~PriorityWorker () {}

  void DoFinally (napi_env env) override {
    database_->DecrementPriorityWork(env);
    BaseWorker::DoFinally(env);
  }
};

struct BaseIterator {
  BaseIterator(Database* database,
               const bool reverse,
               const std::string* lt,
               const std::string* lte,
               const std::string* gt,
               const std::string* gte,
               const int limit,
               const bool fillCache)
    : database_(database),
      snapshot_(database->NewSnapshot()),
      dbIterator_(database->NewIterator([&]{
        leveldb::ReadOptions options;
        options.fill_cache = fillCache;
        options.verify_checksums = false;
        options.snapshot = snapshot_;
        return options;
      }())),
      didSeek_(false),
      reverse_(reverse),
      lt_(lt),
      lte_(lte),
      gt_(gt),
      gte_(gte),
      limit_(limit),
      count_(0) {
  }

  virtual ~BaseIterator () {
    assert(!dbIterator_);

    delete lt_;
    delete lte_;
    delete gt_;
    delete gte_;
  }

  bool DidSeek () const {
    return didSeek_;
  }

  /**
   * Seek to the first relevant key based on range options.
   */
  void SeekToRange () {
    didSeek_ = true;

    if (!reverse_ && gte_) {
      dbIterator_->Seek(*gte_);
    } else if (!reverse_ && gt_) {
      dbIterator_->Seek(*gt_);

      if (dbIterator_->Valid() && dbIterator_->key().compare(*gt_) == 0) {
        dbIterator_->Next();
      }
    } else if (reverse_ && lte_) {
      dbIterator_->Seek(*lte_);

      if (!dbIterator_->Valid()) {
        dbIterator_->SeekToLast();
      } else if (dbIterator_->key().compare(*lte_) > 0) {
        dbIterator_->Prev();
      }
    } else if (reverse_ && lt_) {
      dbIterator_->Seek(*lt_);

      if (!dbIterator_->Valid()) {
        dbIterator_->SeekToLast();
      } else if (dbIterator_->key().compare(*lt_) >= 0) {
        dbIterator_->Prev();
      }
    } else if (reverse_) {
      dbIterator_->SeekToLast();
    } else {
      dbIterator_->SeekToFirst();
    }
  }

  /**
   * Seek manually (during iteration).
   */
  void Seek (const std::string& target) {
    didSeek_ = true;

    if (OutOfRange(target)) {
      return SeekToEnd();
    }

    dbIterator_->Seek(target);

    if (dbIterator_->Valid()) {
      const auto cmp = dbIterator_->key().compare(target);
      if (reverse_ ? cmp > 0 : cmp < 0) {
        Next();
      }
    } else {
      SeekToFirst();
      if (dbIterator_->Valid()) {
        const auto cmp = dbIterator_->key().compare(target);
        if (reverse_ ? cmp > 0 : cmp < 0) {
          SeekToEnd();
        }
      }
    }
  }

  void Close () {
    if (dbIterator_) {
      delete dbIterator_;
      dbIterator_ = nullptr;
      database_->ReleaseSnapshot(snapshot_);
    }
  }

  bool Valid () const {
    return dbIterator_->Valid() && !OutOfRange(dbIterator_->key());
  }

  bool Increment () {
    return limit_ < 0 || ++count_ <= limit_;
  }

  void Next () {
    if (reverse_) dbIterator_->Prev();
    else dbIterator_->Next();
  }

  void SeekToFirst () {
    if (reverse_) dbIterator_->SeekToLast();
    else dbIterator_->SeekToFirst();
  }

  void SeekToLast () {
    if (reverse_) dbIterator_->SeekToFirst();
    else dbIterator_->SeekToLast();
  }

  void SeekToEnd () {
    SeekToLast();
    Next();
  }

  leveldb::Slice CurrentKey () const {
    return dbIterator_->key();
  }

  leveldb::Slice CurrentValue () const {
    return dbIterator_->value();
  }

  leveldb::Status Status () const {
    return dbIterator_->status();
  }

  bool OutOfRange (const leveldb::Slice& target) const {
    if (lte_) {
      if (target.compare(*lte_) > 0) return true;
    } else if (lt_) {
      if (target.compare(*lt_) >= 0) return true;
    }

    if (gte_) {
      if (target.compare(*gte_) < 0) return true;
    } else if (gt_) {
      if (target.compare(*gt_) <= 0) return true;
    }

    return false;
  }

  Database* database_;

private:
  const leveldb::Snapshot* snapshot_;
  leveldb::Iterator* dbIterator_;
  bool didSeek_;
  const bool reverse_;
  const std::string* lt_;
  const std::string* lte_;
  const std::string* gt_;
  const std::string* gte_;
  const int limit_;
  int count_;
};

struct Iterator final : public BaseIterator {
  Iterator (Database* database,
            const bool reverse,
            const bool keys,
            const bool values,
            const int limit,
            const std::string* lt,
            const std::string* lte,
            const std::string* gt,
            const std::string* gte,
            const bool fillCache,
            const bool keyAsBuffer,
            const bool valueAsBuffer,
            const uint32_t highWaterMarkBytes)
    : BaseIterator(database, reverse, lt, lte, gt, gte, limit, fillCache),
      keys_(keys),
      values_(values),
      keyAsBuffer_(keyAsBuffer),
      valueAsBuffer_(valueAsBuffer),
      highWaterMarkBytes_(highWaterMarkBytes),
      first_(true),
      ref_(nullptr) {
  }

  void Attach (napi_env env, napi_value context) {
    napi_create_reference(env, context, 1, &ref_);
    database_->AttachIterator(env, this);
  }

  void Detach (napi_env env) {
    database_->DetachIterator(env, this);
    if (ref_) napi_delete_reference(env, ref_);
  }

  bool ReadMany (uint32_t size) {
    cache_.clear();
    cache_.reserve(size * 2);
    size_t bytesRead = 0;

    while (true) {
      if (!first_) Next();
      else first_ = false;

      if (!Valid() || !Increment()) break;
    
      if (keys_ && values_) {
        auto k = CurrentKey();
        auto v = CurrentValue();
        cache_.emplace_back(k.data(), k.size());
        cache_.emplace_back(v.data(), v.size());
        bytesRead += k.size() + v.size();
      } else if (keys_) {
        auto k = CurrentKey();
        cache_.emplace_back(k.data(), k.size());
        cache_.push_back({});
        bytesRead += k.size();
      } else if (values_) {
        auto v = CurrentValue();
        cache_.push_back({});
        cache_.emplace_back(v.data(), v.size());
        bytesRead += v.size();
      }

      if (bytesRead > highWaterMarkBytes_ || cache_.size() / 2 >= size) {
        return true;
      }
    }

    return false;
  }

  const bool keys_;
  const bool values_;
  const bool keyAsBuffer_;
  const bool valueAsBuffer_;
  const uint32_t highWaterMarkBytes_;
  bool first_;
  std::vector<std::string> cache_;

private:
  napi_ref ref_;
};

/**
 * Hook for when the environment exits. This hook will be called after
 * already-scheduled napi_async_work items have finished, which gives us
 * the guarantee that no db operations will be in-flight at this time.
 */
static void env_cleanup_hook (void* arg) {
  auto database = reinterpret_cast<Database*>(arg);

  // Do everything that db_close() does but synchronously. We're expecting that GC
  // did not (yet) collect the database because that would be a user mistake (not
  // closing their db) made during the lifetime of the environment. That's different
  // from an environment being torn down (like the main process or a worker thread)
  // where it's our responsibility to clean up. Note also, the following code must
  // be a safe noop if called before db_open() or after db_close().
  if (database && database->db_) {
    // TODO: does not do `napi_delete_reference(env, iterator->ref_)`. Problem?
    for (auto it : database->iterators_) {
      it->Close();
    }

    // Having closed the iterators (and released snapshots) we can safely close.
    database->CloseDatabase();
  }
}

static void FinalizeDatabase (napi_env env, void* data, void* hint) {
  if (data) {
    auto database = reinterpret_cast<Database*>(data);
    napi_remove_env_cleanup_hook(env, env_cleanup_hook, database);
    if (database->ref_) napi_delete_reference(env, database->ref_);
    delete database;
  }
}

NAPI_METHOD(db_init) {
  auto database = new Database();
  napi_add_env_cleanup_hook(env, env_cleanup_hook, database);

  napi_value result;
  NAPI_STATUS_THROWS(napi_create_external(env, database,
                                          FinalizeDatabase,
                                          nullptr, &result));

  // Reference counter to prevent GC of database while priority workers are active
  NAPI_STATUS_THROWS(napi_create_reference(env, result, 0, &database->ref_));

  return result;
}

/**
 * Worker class for opening a database.
 * TODO: shouldn't this be a PriorityWorker?
 */
struct OpenWorker final : public BaseWorker {
  OpenWorker (napi_env env,
              Database* database,
              napi_value callback,
              const std::string& location,
              const bool createIfMissing,
              const bool errorIfExists,
              const bool compression,
              const uint32_t writeBufferSize,
              const uint32_t blockSize,
              const uint32_t maxOpenFiles,
              const uint32_t blockRestartInterval,
              const uint32_t maxFileSize,
              const uint32_t cacheSize,
              const std::string& infoLogLevel,
              const bool readOnly)
    : BaseWorker(env, database, callback, "leveldown.db.open"),
      readOnly_(readOnly),
      location_(location) {
    options_.create_if_missing = createIfMissing;
    options_.error_if_exists = errorIfExists;
    options_.compression = compression
      ? leveldb::kSnappyCompression
      : leveldb::kNoCompression;
    options_.write_buffer_size = writeBufferSize;
    options_.max_open_files = maxOpenFiles;
    options_.max_log_file_size = maxFileSize;
    options_.paranoid_checks = false;

    if (infoLogLevel.size() > 0) {
      rocksdb::InfoLogLevel lvl = {};

      if (infoLogLevel == "debug") lvl = rocksdb::InfoLogLevel::DEBUG_LEVEL;
      else if (infoLogLevel == "info") lvl = rocksdb::InfoLogLevel::INFO_LEVEL;
      else if (infoLogLevel == "warn") lvl = rocksdb::InfoLogLevel::WARN_LEVEL;
      else if (infoLogLevel == "error") lvl = rocksdb::InfoLogLevel::ERROR_LEVEL;
      else if (infoLogLevel == "fatal") lvl = rocksdb::InfoLogLevel::FATAL_LEVEL;
      else if (infoLogLevel == "header") lvl = rocksdb::InfoLogLevel::HEADER_LEVEL;
      else napi_throw_error(env, nullptr, "invalid log level");

      options_.info_log_level = lvl;
    } else {
      // In some places RocksDB checks this option to see if it should prepare
      // debug information (ahead of logging), so set it to the highest level.
      options_.info_log_level = rocksdb::InfoLogLevel::HEADER_LEVEL;
      options_.info_log.reset(new NullLogger());
    }

    rocksdb::BlockBasedTableOptions tableOptions;

    if (cacheSize) {
      tableOptions.block_cache = rocksdb::NewLRUCache(cacheSize);
    } else {
      tableOptions.no_block_cache = true;
    }

    tableOptions.block_size = blockSize;
    tableOptions.block_restart_interval = blockRestartInterval;
    tableOptions.filter_policy.reset(rocksdb::NewBloomFilterPolicy(10));
    tableOptions.format_version = 5;
    tableOptions.checksum = rocksdb::kxxHash64;

    options_.table_factory.reset(
      rocksdb::NewBlockBasedTableFactory(tableOptions)
    );
  }

  void DoExecute () override {
    SetStatus(database_->Open(options_, readOnly_, location_.c_str()));
  }

  leveldb::Options options_;
  const bool readOnly_;
  const std::string location_;
};

NAPI_METHOD(db_open) {
  NAPI_ARGV(4);
  NAPI_DB_CONTEXT();

  const auto location = ToString(env, argv[1]);
  const auto options = argv[2];
  const auto createIfMissing = BooleanProperty(env, options, "createIfMissing", true);
  const auto errorIfExists = BooleanProperty(env, options, "errorIfExists", false);
  const auto compression = BooleanProperty(env, options, "compression", true);
  const auto readOnly = BooleanProperty(env, options, "readOnly", false);

  const auto infoLogLevel = StringProperty(env, options, "infoLogLevel");

  const auto cacheSize = Uint32Property(env, options, "cacheSize", 8 << 20);
  const auto writeBufferSize = Uint32Property(env, options , "writeBufferSize" , 4 << 20);
  const auto blockSize = Uint32Property(env, options, "blockSize", 4096);
  const auto maxOpenFiles = Uint32Property(env, options, "maxOpenFiles", 1000);
  const auto blockRestartInterval = Uint32Property(env, options,
                                                 "blockRestartInterval", 16);
  const auto maxFileSize = Uint32Property(env, options, "maxFileSize", 2 << 20);

  const auto callback = argv[3];

  auto worker = new OpenWorker(env, database, callback, location,
                               createIfMissing, errorIfExists,
                               compression, writeBufferSize, blockSize,
                               maxOpenFiles, blockRestartInterval,
                               maxFileSize, cacheSize,
                               infoLogLevel, readOnly);
  worker->Queue(env);

  return 0;
}

struct CloseWorker final : public BaseWorker {
  CloseWorker (napi_env env,
               Database* database,
               napi_value callback)
    : BaseWorker(env, database, callback, "leveldown.db.close") {}

  void DoExecute () override {
    database_->CloseDatabase();
  }
};

napi_value noop_callback (napi_env env, napi_callback_info info) {
  return 0;
}

NAPI_METHOD(db_close) {
  NAPI_ARGV(2);
  NAPI_DB_CONTEXT();

  const auto callback = argv[1];

  auto worker = new CloseWorker(env, database, callback);

  if (!database->HasPriorityWork()) {
    worker->Queue(env);
  } else {
    database->pendingCloseWorker_ = worker;
  }

  return 0;
}

struct PutWorker final : public PriorityWorker {
  PutWorker (napi_env env,
             Database* database,
             napi_value callback,
             const std::string& key,
             const std::string& value,
             bool sync)
    : PriorityWorker(env, database, callback, "rocks_level.db.put"),
      key_(key), value_(value), sync_(sync) {
  }

  void DoExecute () override {
    leveldb::WriteOptions options;
    options.sync = sync_;
    SetStatus(database_->Put(options, key_, value_));
  }

  const std::string key_;
  const std::string value_;
  const bool sync_;
};

NAPI_METHOD(db_put) {
  NAPI_ARGV(5);
  NAPI_DB_CONTEXT();

  const auto key = ToString(env, argv[1]);
  const auto value = ToString(env, argv[2]);
  const auto sync = BooleanProperty(env, argv[3], "sync", false);
  const auto callback = argv[4];

  auto worker = new PutWorker(env, database, callback, key, value, sync);
  worker->Queue(env);

  return 0;
}

struct GetWorker final : public PriorityWorker {
  GetWorker (napi_env env,
             Database* database,
             napi_value callback,
             const std::string& key,
             const bool asBuffer,
             const bool fillCache)
    : PriorityWorker(env, database, callback, "rocks_level.db.get"),
      key_(key), asBuffer_(asBuffer), fillCache_(fillCache) {
  }

  void DoExecute () override {
    leveldb::ReadOptions options;
    options.fill_cache = fillCache_;
    SetStatus(database_->Get(options, key_, value_));
  }

  void HandleOKCallback (napi_env env, napi_value callback) override {
    napi_value argv[2];
    napi_get_null(env, &argv[0]);
    Convert(env, std::move(value_), asBuffer_, argv[1]);
    CallFunction(env, callback, 2, argv);
  }

private:
  const std::string key_;
  rocksdb::PinnableSlice value_;
  const bool asBuffer_;
  const bool fillCache_;
};

NAPI_METHOD(db_get) {
  NAPI_ARGV(4);
  NAPI_DB_CONTEXT();

  const auto key = ToString(env, argv[1]);
  const auto options = argv[2];
  const auto asBuffer = EncodingIsBuffer(env, options, "valueEncoding");
  const auto fillCache = BooleanProperty(env, options, "fillCache", true);
  const auto callback = argv[3];

  auto worker = new GetWorker(env, database, callback, key, asBuffer, fillCache);
  worker->Queue(env);

  return 0;
}

struct GetManyWorker final : public PriorityWorker {
  GetManyWorker (napi_env env,
                 Database* database,
                 const std::vector<std::string>& keys,
                 napi_value callback,
                 const bool valueAsBuffer,
                 const bool fillCache)
    : PriorityWorker(env, database, callback, "leveldown.get.many"),
      keys_(keys), valueAsBuffer_(valueAsBuffer), fillCache_(fillCache),
      snapshot_(database->NewSnapshot()) {
  }

  ~GetManyWorker () {
    if (snapshot_) {
      database_->ReleaseSnapshot(snapshot_);
      snapshot_ = nullptr;
    }
  }

  void DoExecute () override {
    cache_.reserve(keys_.size());

    leveldb::ReadOptions options;
    options.snapshot = snapshot_;
    options.fill_cache = fillCache_;
    
    rocksdb::PinnableSlice value;

    for (const auto& key: keys_) {
      const auto status = database_->Get(options, key, value);

      if (status.ok()) {
        cache_.emplace_back(std::move(value));
      } else if (status.IsNotFound()) {
        cache_.emplace_back(nullptr);
      } else {
        SetStatus(status);
        break;
      }

      value.Reset();
    }

    database_->ReleaseSnapshot(snapshot_);
    snapshot_ = nullptr;
  }

  void HandleOKCallback (napi_env env, napi_value callback) override {
    const auto size = cache_.size();

    napi_value array;
    napi_create_array_with_length(env, size, &array);

    for (size_t idx = 0; idx < size; idx++) {
      napi_value element;
      if (cache_[idx].GetSelf() != nullptr) {
        Convert(env, cache_[idx], valueAsBuffer_, element);
      } else {
        napi_get_undefined(env, &element);
      }
      napi_set_element(env, array, static_cast<uint32_t>(idx), element);
    }

    napi_value argv[2];
    napi_get_null(env, &argv[0]);
    argv[1] = array;
    CallFunction(env, callback, 2, argv);
  }

private:
  const std::vector<std::string> keys_;
  const bool valueAsBuffer_;
  std::vector<rocksdb::PinnableSlice> cache_;
  const bool fillCache_;
  const leveldb::Snapshot* snapshot_;
};

NAPI_METHOD(db_get_many) {
  NAPI_ARGV(4);
  NAPI_DB_CONTEXT();

  const auto keys = KeyArray(env, argv[1]);
  const auto options = argv[2];
  const bool asBuffer = EncodingIsBuffer(env, options, "valueEncoding");
  const bool fillCache = BooleanProperty(env, options, "fillCache", true);
  const auto callback = argv[3];

  auto worker = new GetManyWorker(env, database, keys, callback, asBuffer, fillCache);
  worker->Queue(env);

  return 0;
}

struct DelWorker final : public PriorityWorker {
  DelWorker (napi_env env,
             Database* database,
             napi_value callback,
             const std::string& key,
             bool sync)
    : PriorityWorker(env, database, callback, "rocks_level.db.del"),
      key_(key), sync_(sync) {
  }

  void DoExecute () override {
    leveldb::WriteOptions options;
    options.sync = sync_;
    SetStatus(database_->Del(options, key_));
  }

  const std::string key_;
  const bool sync_;
};

NAPI_METHOD(db_del) {
  NAPI_ARGV(4);
  NAPI_DB_CONTEXT();

  const auto key = ToString(env, argv[1]);
  const auto sync = BooleanProperty(env, argv[2], "sync", false);
  const auto callback = argv[3];

  auto worker = new DelWorker(env, database, callback, key, sync);
  worker->Queue(env);

  return 0;
}

struct ClearWorker final : public PriorityWorker {
  ClearWorker (napi_env env,
               Database* database,
               napi_value callback,
               const bool reverse,
               const int limit,
               const std::string* lt,
               const std::string* lte,
               const std::string* gt,
               const std::string* gte)
    : PriorityWorker(env, database, callback, "rocks_level.db.clear"),
      iterator_(database, reverse, lt, lte, gt, gte, limit, false) {
  }

  void DoExecute () override {
    iterator_.SeekToRange();

    // TODO: add option
    const uint32_t hwm = 16 * 1024;

    leveldb::WriteBatch batch;
    leveldb::WriteOptions options;

    while (true) {
      size_t bytesRead = 0;

      while (bytesRead <= hwm && iterator_.Valid() && iterator_.Increment()) {
        const auto key = iterator_.CurrentKey();
        batch.Delete(key);
        bytesRead += key.size();
        iterator_.Next();
      }

      if (!SetStatus(iterator_.Status()) || bytesRead == 0) {
        break;
      }

      if (!SetStatus(database_->WriteBatch(options, &batch))) {
        break;
      }

      batch.Clear();
    }

    iterator_.Close();
  }

private:
  BaseIterator iterator_;
};

NAPI_METHOD(db_clear) {
  NAPI_ARGV(3);
  NAPI_DB_CONTEXT();

  napi_value options = argv[1];
  napi_value callback = argv[2];

  const auto reverse = BooleanProperty(env, options, "reverse", false);
  const auto limit = Int32Property(env, options, "limit", -1);

  const auto lt = RangeOption(env, options, "lt");
  const auto lte = RangeOption(env, options, "lte");
  const auto gt = RangeOption(env, options, "gt");
  const auto gte = RangeOption(env, options, "gte");

  auto worker = new ClearWorker(env, database, callback, reverse, limit, lt, lte, gt, gte);
  worker->Queue(env);

  return 0;
}

struct ApproximateSizeWorker final : public PriorityWorker {
  ApproximateSizeWorker (napi_env env,
                         Database* database,
                         napi_value callback,
                         const std::string& start,
                         const std::string& end)
    : PriorityWorker(env, database, callback, "rocks_level.db.approximate_size"),
      start_(start), end_(end) {}

  void DoExecute () override {
    leveldb::Range range(start_, end_);
    size_ = database_->ApproximateSize(&range);
  }

  void HandleOKCallback (napi_env env, napi_value callback) override {
    napi_value argv[2];
    napi_get_null(env, &argv[0]);
    napi_create_int64(env, size_, &argv[1]);
    CallFunction(env, callback, 2, argv);
  }

  std::string start_;
  std::string end_;
  uint64_t size_;
};

NAPI_METHOD(db_approximate_size) {
  NAPI_ARGV(4);
  NAPI_DB_CONTEXT();

  const auto start = ToString(env, argv[1]);
  const auto end = ToString(env, argv[2]);
  const auto callback = argv[3];

  auto worker  = new ApproximateSizeWorker(env, database, callback, start, end);
  worker->Queue(env);

  return 0;
}

struct CompactRangeWorker final : public PriorityWorker {
  CompactRangeWorker (napi_env env,
                      Database* database,
                      napi_value callback,
                      const std::string& start,
                      const std::string& end)
    : PriorityWorker(env, database, callback, "rocks_level.db.compact_range"),
      start_(start), end_(end) {}

  void DoExecute () override {
    leveldb::Slice start = start_;
    leveldb::Slice end = end_;
    database_->CompactRange(&start, &end);
  }

  const std::string start_;
  const std::string end_;
};

NAPI_METHOD(db_compact_range) {
  NAPI_ARGV(4);
  NAPI_DB_CONTEXT();

  const auto start = ToString(env, argv[1]);
  const auto end = ToString(env, argv[2]);
  const auto callback = argv[3];

  auto worker  = new CompactRangeWorker(env, database, callback, start, end);
  worker->Queue(env);

  return 0;
}

NAPI_METHOD(db_get_property) {
  NAPI_ARGV(2);
  NAPI_DB_CONTEXT();

  const auto property = ToString(env, argv[1]);

  std::string value;
  database->GetProperty(property, value);

  napi_value result;
  napi_create_string_utf8(env, value.data(), value.size(), &result);

  return result;
}

struct DestroyWorker final : public BaseWorker {
  DestroyWorker (napi_env env,
                 const std::string& location,
                 napi_value callback)
    : BaseWorker(env, nullptr, callback, "rocks_level.destroy_db"),
      location_(location) {}

  ~DestroyWorker () {}

  void DoExecute () override {
    leveldb::Options options;
    SetStatus(leveldb::DestroyDB(location_, options));
  }

  const std::string location_;
};

NAPI_METHOD(destroy_db) {
  NAPI_ARGV(2);

  const auto location = ToString(env, argv[0]);
  const auto callback = argv[1];

  auto worker = new DestroyWorker(env, location, callback);
  worker->Queue(env);

  return 0;
}

struct RepairWorker final : public BaseWorker {
  RepairWorker (napi_env env,
                const std::string& location,
                napi_value callback)
    : BaseWorker(env, nullptr, callback, "rocks_level.repair_db"),
      location_(location) {}

  void DoExecute () override {
    leveldb::Options options;
    SetStatus(leveldb::RepairDB(location_, options));
  }

  const std::string location_;
};

NAPI_METHOD(repair_db) {
  NAPI_ARGV(2);

  const auto location = ToString(env, argv[1]);
  const auto callback = argv[1];

  auto worker = new RepairWorker(env, location, callback);
  worker->Queue(env);

  return 0;
}

static void FinalizeIterator (napi_env env, void* data, void* hint) {
  if (data) {
    delete reinterpret_cast<Iterator*>(data);
  }
}

NAPI_METHOD(iterator_init) {
  NAPI_ARGV(2);
  NAPI_DB_CONTEXT();

  const auto options = argv[1];
  const auto reverse = BooleanProperty(env, options, "reverse", false);
  const auto keys = BooleanProperty(env, options, "keys", true);
  const auto values = BooleanProperty(env, options, "values", true);
  const auto fillCache = BooleanProperty(env, options, "fillCache", false);
  const bool keyAsBuffer = EncodingIsBuffer(env, options, "keyEncoding");
  const bool valueAsBuffer = EncodingIsBuffer(env, options, "valueEncoding");
  const auto limit = Int32Property(env, options, "limit", -1);
  const auto highWaterMarkBytes = Uint32Property(env, options, "highWaterMarkBytes", 16 * 1024);

  const auto lt = RangeOption(env, options, "lt");
  const auto lte = RangeOption(env, options, "lte");
  const auto gt = RangeOption(env, options, "gt");
  const auto gte = RangeOption(env, options, "gte");

  auto iterator = new Iterator(database, reverse, keys,
                               values, limit, lt, lte, gt, gte, fillCache,
                               keyAsBuffer, valueAsBuffer, highWaterMarkBytes);
  napi_value result;

  NAPI_STATUS_THROWS(napi_create_external(env, iterator,
                                          FinalizeIterator,
                                          nullptr, &result));

  // Prevent GC of JS object before the iterator is closed (explicitly or on
  // db close) and keep track of non-closed iterators to end them on db close.
  iterator->Attach(env, result);

  return result;
}

NAPI_METHOD(iterator_seek) {
  NAPI_ARGV(2);
  NAPI_ITERATOR_CONTEXT();

  const auto target = ToString(env, argv[1]);
  iterator->first_ = true;
  iterator->Seek(target);

  return 0;
}

struct CloseIteratorWorker final : public BaseWorker {
  CloseIteratorWorker (napi_env env,
                       Iterator* iterator,
                       napi_value callback)
    : BaseWorker(env, iterator->database_, callback, "leveldown.iterator.end"),
      iterator_(iterator) {}

  void DoExecute () override {
    iterator_->Close();
  }

  void DoFinally (napi_env env) override {
    iterator_->Detach(env);
    BaseWorker::DoFinally(env);
  }

private:
  Iterator* iterator_;
};

NAPI_METHOD(iterator_close) {
  NAPI_ARGV(2);
  NAPI_ITERATOR_CONTEXT();

  const auto callback = argv[1];

  auto worker = new CloseIteratorWorker(env, iterator, callback);
  worker->Queue(env);

  return 0;
}

struct NextWorker final : public BaseWorker {
  NextWorker (napi_env env,
              Iterator* iterator,
              uint32_t size,
              napi_value callback)
    : BaseWorker(env, iterator->database_, callback,
                 "leveldown.iterator.next"),
      iterator_(iterator), size_(size), ok_() {}

  void DoExecute () override {
    if (!iterator_->DidSeek()) {
      iterator_->SeekToRange();
    }

    // Limit the size of the cache to prevent starving the event loop
    // in JS-land while we're recursively calling process.nextTick().
    ok_ = iterator_->ReadMany(size_);

    if (!ok_) {
      SetStatus(iterator_->Status());
    }
  }

  void HandleOKCallback (napi_env env, napi_value callback) override {
    const auto size = iterator_->cache_.size();
    napi_value result;
    napi_create_array_with_length(env, size, &result);

    for (size_t idx = 0; idx < iterator_->cache_.size(); idx += 2) {
      napi_value key;
      napi_value val;

      Convert(env, iterator_->cache_[idx + 0], iterator_->keyAsBuffer_, key);
      Convert(env, iterator_->cache_[idx + 1], iterator_->valueAsBuffer_, val);

      napi_set_element(env, result, static_cast<int>(idx + 0), key);
      napi_set_element(env, result, static_cast<int>(idx + 1), val);
    }

    iterator_->cache_.clear();

    napi_value argv[3];
    napi_get_null(env, &argv[0]);
    argv[1] = result;
    napi_get_boolean(env, !ok_, &argv[2]);
    CallFunction(env, callback, 3, argv);
  }

  void DoFinally (napi_env env) override {
    BaseWorker::DoFinally(env);
  }

private:
  Iterator* iterator_;
  uint32_t size_;
  bool ok_;
};

NAPI_METHOD(iterator_nextv) {
  NAPI_ARGV(3);
  NAPI_ITERATOR_CONTEXT();

  uint32_t size;
  NAPI_STATUS_THROWS(napi_get_value_uint32(env, argv[1], &size));
  if (size == 0) size = 1;

  const auto callback = argv[2];

  auto worker = new NextWorker(env, iterator, size, callback);
  worker->Queue(env);

  return 0;
}

/**
 * Worker class for batch write operation.
 */
struct BatchWorker final : public PriorityWorker {
  BatchWorker (napi_env env,
               Database* database,
               napi_value callback,
               leveldb::WriteBatch* batch,
               const bool sync,
               const bool hasData)
    : PriorityWorker(env, database, callback, "rocks_level.batch.do"),
      batch_(batch), hasData_(hasData) {
    options_.sync = sync;
  }

  ~BatchWorker () {
    delete batch_;
  }

  void DoExecute () override {
    if (hasData_) {
      SetStatus(database_->WriteBatch(options_, batch_));
    }
  }

private:
  leveldb::WriteOptions options_;
  leveldb::WriteBatch* batch_;
  const bool hasData_;
};

NAPI_METHOD(batch_do) {
  NAPI_ARGV(4);
  NAPI_DB_CONTEXT();

  const auto array = argv[1];
  const auto sync = BooleanProperty(env, argv[2], "sync", false);
  const auto callback = argv[3];

  uint32_t length;
  napi_get_array_length(env, array, &length);

  leveldb::WriteBatch* batch = new leveldb::WriteBatch();
  bool hasData = false;

  for (uint32_t i = 0; i < length; i++) {
    napi_value element;
    napi_get_element(env, array, i, &element);

    if (!IsObject(env, element)) continue;

    std::string type = StringProperty(env, element, "type");

    if (type == "del") {
      if (!HasProperty(env, element, "key")) continue;
      const auto key = ToString(env, GetProperty(env, element, "key"));

      batch->Delete(key);
      if (!hasData) hasData = true;
    } else if (type == "put") {
      if (!HasProperty(env, element, "key")) continue;
      if (!HasProperty(env, element, "value")) continue;

      const auto key = ToString(env, GetProperty(env, element, "key"));
      const auto value = ToString(env, GetProperty(env, element, "value"));

      batch->Put(key, value);
      if (!hasData) hasData = true;
    }
  }

  auto worker = new BatchWorker(env, database, callback, batch, sync, hasData);
  worker->Queue(env);

  return 0;
}

static void FinalizeBatch (napi_env env, void* data, void* hint) {
  if (data) {
    delete reinterpret_cast<leveldb::WriteBatch*>(data);
  }
}

NAPI_METHOD(batch_init) {
  NAPI_ARGV(1);
  NAPI_DB_CONTEXT();

  auto batch = new leveldb::WriteBatch();

  napi_value result;
  NAPI_STATUS_THROWS(napi_create_external(env, batch,
                                          FinalizeBatch,
                                          nullptr, &result));
  return result;
}

NAPI_METHOD(batch_put) {
  NAPI_ARGV(3);
  NAPI_BATCH_CONTEXT();

  const auto key = ToString(env, argv[1]);
  const auto value = ToString(env, argv[2]);

  batch->Put(key, value);

  return 0;
}

NAPI_METHOD(batch_del) {
  NAPI_ARGV(2);
  NAPI_BATCH_CONTEXT();

  const auto key = ToString(env, argv[1]);

  batch->Delete(key);

  return 0;
}

NAPI_METHOD(batch_clear) {
  NAPI_ARGV(1);
  NAPI_BATCH_CONTEXT();

  batch->Clear();

  return 0;
}

struct BatchWriteWorker final : public PriorityWorker {
  BatchWriteWorker (napi_env env,
                    Database* database,
                    napi_value batch,
                    napi_value callback,
                    const bool sync)
    : PriorityWorker(env, database, callback, "leveldown.batch.write"),
      sync_(sync) {

    NAPI_STATUS_THROWS_VOID(napi_get_value_external(env, batch, reinterpret_cast<void**>(&batch_)));

    // Prevent GC of batch object before we execute
    NAPI_STATUS_THROWS_VOID(napi_create_reference(env, batch, 1, &batchRef_));
  }

  void DoExecute () override {
    leveldb::WriteOptions options;
    options.sync = sync_;
    SetStatus(database_->WriteBatch(options, batch_));
  }

  void DoFinally (napi_env env) override {
    napi_delete_reference(env, batchRef_);
    PriorityWorker::DoFinally(env);
  }

private:
  leveldb::WriteBatch* batch_;
  const bool sync_;
  napi_ref batchRef_;
};

NAPI_METHOD(batch_write) {
  NAPI_ARGV(4);
  NAPI_DB_CONTEXT();

  const auto batch = argv[1];
  const auto options = argv[2];
  const auto sync = BooleanProperty(env, options, "sync", false);
  const auto callback = argv[3];

  auto worker = new BatchWriteWorker(env, database, batch, callback, sync);
  worker->Queue(env);

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
  NAPI_EXPORT_FUNCTION(db_approximate_size);
  NAPI_EXPORT_FUNCTION(db_compact_range);
  NAPI_EXPORT_FUNCTION(db_get_property);

  NAPI_EXPORT_FUNCTION(destroy_db);
  NAPI_EXPORT_FUNCTION(repair_db);

  NAPI_EXPORT_FUNCTION(iterator_init);
  NAPI_EXPORT_FUNCTION(iterator_seek);
  NAPI_EXPORT_FUNCTION(iterator_close);
  NAPI_EXPORT_FUNCTION(iterator_nextv);

  NAPI_EXPORT_FUNCTION(batch_do);
  NAPI_EXPORT_FUNCTION(batch_init);
  NAPI_EXPORT_FUNCTION(batch_put);
  NAPI_EXPORT_FUNCTION(batch_del);
  NAPI_EXPORT_FUNCTION(batch_clear);
  NAPI_EXPORT_FUNCTION(batch_write);
}
