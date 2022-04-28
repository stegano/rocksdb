#define NAPI_VERSION 8

#include <assert.h>
#include <napi-macros.h>
#include <node_api.h>

#include <rocksdb/cache.h>
#include <rocksdb/comparator.h>
#include <rocksdb/db.h>
#include <rocksdb/env.h>
#include <rocksdb/filter_policy.h>
#include <rocksdb/options.h>
#include <rocksdb/table.h>
#include <rocksdb/write_batch.h>

#include <array>
#include <memory>
#include <optional>
#include <set>
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

#define NAPI_STATUS_RETURN(call) \
  {                              \
    const auto status = (call);  \
    if (status != napi_ok) {     \
      return status;             \
    }                            \
  }

#define NAPI_PENDING_EXCEPTION()                                 \
  {                                                              \
    bool result;                                                 \
    NAPI_STATUS_THROWS(napi_is_exception_pending(env, &result)); \
    if (result) {                                                \
      return nullptr;                                            \
    }                                                            \
  }

#define NAPI_DB_CONTEXT()       \
  Database* database = nullptr; \
  NAPI_STATUS_THROWS(napi_get_value_external(env, argv[0], (void**)&database));

#define NAPI_ITERATOR_CONTEXT() \
  Iterator* iterator = nullptr; \
  NAPI_STATUS_THROWS(napi_get_value_external(env, argv[0], (void**)&iterator));

#define NAPI_BATCH_CONTEXT()            \
  rocksdb::WriteBatch* batch = nullptr; \
  NAPI_STATUS_THROWS(napi_get_value_external(env, argv[0], (void**)&batch));

static bool IsString(napi_env env, napi_value value) {
  napi_valuetype type;
  napi_typeof(env, value, &type);
  return type == napi_string;
}

static bool IsBuffer(napi_env env, napi_value value) {
  bool isBuffer;
  napi_is_buffer(env, value, &isBuffer);
  return isBuffer;
}

static bool IsObject(napi_env env, napi_value value) {
  napi_valuetype type;
  napi_typeof(env, value, &type);
  return type == napi_object;
}

static napi_value CreateError(napi_env env, const std::string_view& str) {
  napi_value msg;
  napi_create_string_utf8(env, str.data(), str.size(), &msg);
  napi_value error;
  napi_create_error(env, nullptr, msg, &error);
  return error;
}

static napi_value CreateCodeError(napi_env env, const std::string_view& code, const std::string_view& msg) {
  napi_value codeValue;
  napi_create_string_utf8(env, code.data(), code.size(), &codeValue);
  napi_value msgValue;
  napi_create_string_utf8(env, msg.data(), msg.size(), &msgValue);
  napi_value error;
  napi_create_error(env, codeValue, msgValue, &error);
  return error;
}

static bool HasProperty(napi_env env, napi_value obj, const std::string_view& key) {
  bool has = false;
  napi_has_named_property(env, obj, key.data(), &has);
  return has;
}

static napi_value GetProperty(napi_env env, napi_value obj, const std::string_view& key) {
  napi_value value;
  napi_get_named_property(env, obj, key.data(), &value);
  return value;
}

static bool BooleanProperty(napi_env env, napi_value obj, const std::string_view& key, bool defaultValue) {
  if (HasProperty(env, obj, key.data())) {
    const auto value = GetProperty(env, obj, key.data());
    bool result;
    napi_get_value_bool(env, value, &result);
    return result;
  }

  return defaultValue;
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

static uint32_t Uint32Property(napi_env env, napi_value obj, const std::string_view& key, uint32_t defaultValue) {
  if (HasProperty(env, obj, key.data())) {
    const auto value = GetProperty(env, obj, key.data());
    uint32_t result;
    napi_get_value_uint32(env, value, &result);
    return result;
  }

  return defaultValue;
}

static int Int32Property(napi_env env, napi_value obj, const std::string_view& key, int defaultValue) {
  if (HasProperty(env, obj, key.data())) {
    const auto value = GetProperty(env, obj, key.data());
    int result;
    napi_get_value_int32(env, value, &result);
    return result;
  }

  return defaultValue;
}

static std::string ToString(napi_env env, napi_value from, const std::string& defaultValue = "") {
  if (IsString(env, from)) {
    size_t length = 0;
    napi_get_value_string_utf8(env, from, nullptr, 0, &length);
    std::string value(length, '\0');
    napi_get_value_string_utf8(env, from, &value[0], value.length() + 1, &length);
    return value;
  } else if (IsBuffer(env, from)) {
    char* buf = nullptr;
    size_t length = 0;
    napi_get_buffer_info(env, from, reinterpret_cast<void**>(&buf), &length);
    return std::string(buf, length);
  }

  return defaultValue;
}

static std::string StringProperty(napi_env env,
                                  napi_value obj,
                                  const std::string_view& key,
                                  const std::string& defaultValue = "") {
  if (HasProperty(env, obj, key)) {
    napi_value value = GetProperty(env, obj, key);
    if (IsString(env, value)) {
      return ToString(env, value);
    }
  }

  return defaultValue;
}

static std::optional<std::string> RangeOption(napi_env env, napi_value opts, const std::string_view& name) {
  if (HasProperty(env, opts, name)) {
    const auto value = GetProperty(env, opts, name);
    return ToString(env, value);
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
    return CreateCodeError(env, "LEVEL_NOT_FOUND", msg);
  } else if (status.IsCorruption()) {
    return CreateCodeError(env, "LEVEL_CORRUPTION", msg);
  } else if (status.IsIOError()) {
    if (msg.find("IO error: lock ") != std::string::npos) {  // env_posix.cc
      return CreateCodeError(env, "LEVEL_LOCKED", msg);
    } else if (msg.find("IO error: LockFile ") != std::string::npos) {  // env_win.cc
      return CreateCodeError(env, "LEVEL_LOCKED", msg);
    } else if (msg.find("IO error: While lock file") != std::string::npos) {  // env_mac.cc
      return CreateCodeError(env, "LEVEL_LOCKED", msg);
    } else {
      return CreateCodeError(env, "LEVEL_IO_ERROR", msg);
    }
  }

  return CreateError(env, msg);
}

template <typename T>
napi_status Convert(napi_env env, const T& s, bool asBuffer, napi_value& result) {
  if (asBuffer) {
    return napi_create_buffer_copy(env, s.size(), s.data(), nullptr, &result);
  } else {
    return napi_create_string_utf8(env, s.data(), s.size(), &result);
  }
}

struct NapiSlice : public rocksdb::Slice {
  NapiSlice(napi_env env, napi_value from) {
    if (IsString(env, from)) {
      NAPI_STATUS_THROWS_VOID(napi_get_value_string_utf8(env, from, nullptr, 0, &size_));
      char* data;
      if (size_ + 1 < stack_.size()) {
        data = stack_.data();
      } else {
        heap_.reset(new char[size_ + 1]);
        data = heap_.get();
      }
      data[size_] = 0;
      NAPI_STATUS_THROWS_VOID(napi_get_value_string_utf8(env, from, data, size_ + 1, &size_));
      data_ = data;
    } else if (IsBuffer(env, from)) {
      void* data;
      NAPI_STATUS_THROWS_VOID(napi_get_buffer_info(env, from, &data, &size_));
      data_ = static_cast<char*>(data);
    }
  }

  std::unique_ptr<char[]> heap_;
  std::array<char, 8192> stack_;
};

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
    napi_value argv;
    NAPI_STATUS_RETURN(napi_get_null(env, &argv));
    return CallFunction(env, callback, 1, &argv);
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
  rocksdb::Status Open(const rocksdb::Options& options, const bool readOnly, const char* location) {
    if (readOnly) {
      rocksdb::DB* db = nullptr;
      const auto status = rocksdb::DB::OpenForReadOnly(options, location, &db);
      db_.reset(db);
      return status;
    } else {
      rocksdb::DB* db = nullptr;
      const auto status = rocksdb::DB::Open(options, location, &db);
      db_.reset(db);
      return status;
    }
  }

  void AttachIterator(napi_env env, Iterator* iterator) {
    iterators_.insert(iterator);
    IncrementPriorityWork(env);
  }

  void DetachIterator(napi_env env, Iterator* iterator) {
    iterators_.erase(iterator);
    DecrementPriorityWork(env);
  }

  void IncrementPriorityWork(napi_env env) { napi_reference_ref(env, prioritRef_, &priorityWork_); }

  void DecrementPriorityWork(napi_env env) {
    napi_reference_unref(env, prioritRef_, &priorityWork_);

    if (priorityWork_ == 0 && pendingCloseWorker_) {
      pendingCloseWorker_->Queue(env);
      pendingCloseWorker_ = nullptr;
    }
  }

  bool HasPriorityWork() const { return priorityWork_ > 0; }

  std::unique_ptr<rocksdb::DB> db_;
  Worker* pendingCloseWorker_;
  std::set<Iterator*> iterators_;
  napi_ref prioritRef_;

 private:
  uint32_t priorityWork_ = 0;
};

struct BaseIterator {
  BaseIterator(Database* database,
               const bool reverse,
               const std::optional<std::string>& lt,
               const std::optional<std::string>& lte,
               const std::optional<std::string>& gt,
               const std::optional<std::string>& gte,
               const int limit,
               const bool fillCache)
      : database_(database),
        snapshot_(database_->db_->GetSnapshot(),
                  [this](const rocksdb::Snapshot* ptr) { database_->db_->ReleaseSnapshot(ptr); }),
        reverse_(reverse),
        limit_(limit),
        fillCache_(fillCache) {
    if (lte) {
      upper_bound_ = std::make_unique<rocksdb::PinnableSlice>();
      upper_bound_->PinSelf(*lte + '\0');
    } else if (lt) {
      upper_bound_ = std::make_unique<rocksdb::PinnableSlice>();
      upper_bound_->PinSelf(*lt);
    }

    if (gte) {
      lower_bound_ = std::make_unique<rocksdb::PinnableSlice>();
      lower_bound_->PinSelf(*gte);
    } else if (gt) {
      lower_bound_ = std::make_unique<rocksdb::PinnableSlice>();
      lower_bound_->PinSelf(*gt + '\0');
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

  bool Valid() const { return iterator_->Valid(); }

  bool Increment() { return limit_ < 0 || ++count_ <= limit_; }

  void Next() {
    if (reverse_)
      iterator_->Prev();
    else
      iterator_->Next();
  }

  rocksdb::Slice CurrentKey() const { return iterator_->key(); }

  rocksdb::Slice CurrentValue() const { return iterator_->value(); }

  rocksdb::Status Status() const { return iterator_->status(); }

  Database* database_;

 private:
  void Init() {
    rocksdb::ReadOptions options;
    if (upper_bound_) {
      options.iterate_upper_bound = &*upper_bound_;
    }
    if (lower_bound_) {
      options.iterate_lower_bound = &*lower_bound_;
    }
    options.fill_cache = fillCache_;
    options.snapshot = snapshot_.get();

    iterator_.reset(database_->db_->NewIterator(options));
  }

  std::unique_ptr<rocksdb::PinnableSlice> lower_bound_;
  std::unique_ptr<rocksdb::PinnableSlice> upper_bound_;
  std::shared_ptr<const rocksdb::Snapshot> snapshot_;
  std::unique_ptr<rocksdb::Iterator> iterator_;
  const bool reverse_;
  const int limit_;
  int count_ = 0;
  const bool fillCache_;
};

struct Iterator final : public BaseIterator {
  Iterator(Database* database,
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
           const uint32_t highWaterMarkBytes)
      : BaseIterator(database, reverse, lt, lte, gt, gte, limit, fillCache),
        keys_(keys),
        values_(values),
        keyAsBuffer_(keyAsBuffer),
        valueAsBuffer_(valueAsBuffer),
        highWaterMarkBytes_(highWaterMarkBytes),
        first_(true),
        ref_(nullptr) {}

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
  const uint32_t highWaterMarkBytes_;
  bool first_;

 private:
  napi_ref ref_;
};

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
    // TODO: does not do `napi_delete_reference(env, iterator->ref_)`. Problem?
    for (auto it : database->iterators_) {
      it->Close();
    }

    // Having closed the iterators (and released snapshots) we can safely close.
    database->db_->Close();
  }
}

static void FinalizeDatabase(napi_env env, void* data, void* hint) {
  if (data) {
    auto database = reinterpret_cast<Database*>(data);
    napi_remove_env_cleanup_hook(env, env_cleanup_hook, database);
    if (database->prioritRef_)
      napi_delete_reference(env, database->prioritRef_);
    delete database;
  }
}

NAPI_METHOD(db_init) {
  auto database = new Database();
  napi_add_env_cleanup_hook(env, env_cleanup_hook, database);

  napi_value result;
  NAPI_STATUS_THROWS(napi_create_external(env, database, FinalizeDatabase, nullptr, &result));

  NAPI_STATUS_THROWS(napi_create_reference(env, result, 0, &database->prioritRef_));

  return result;
}

struct OpenWorker final : public Worker {
  OpenWorker(napi_env env,
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
      : Worker(env, database, callback, "leveldown.db.open"), readOnly_(readOnly), location_(location) {
    options_.create_if_missing = createIfMissing;
    options_.error_if_exists = errorIfExists;
    options_.compression = compression ? rocksdb::kSnappyCompression : rocksdb::kNoCompression;
    options_.write_buffer_size = writeBufferSize;
    options_.max_open_files = maxOpenFiles;
    options_.max_log_file_size = maxFileSize;
    options_.use_adaptive_mutex = true;

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

    options_.table_factory.reset(rocksdb::NewBlockBasedTableFactory(tableOptions));
  }

  rocksdb::Status Execute(Database& database) override { return database.Open(options_, readOnly_, location_.c_str()); }

  rocksdb::Options options_;
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
  const auto writeBufferSize = Uint32Property(env, options, "writeBufferSize", 4 << 20);
  const auto blockSize = Uint32Property(env, options, "blockSize", 4096);
  const auto maxOpenFiles = Uint32Property(env, options, "maxOpenFiles", 1000);
  const auto blockRestartInterval = Uint32Property(env, options, "blockRestartInterval", 16);
  const auto maxFileSize = Uint32Property(env, options, "maxFileSize", 2 << 20);

  const auto callback = argv[3];

  auto worker =
      new OpenWorker(env, database, callback, location, createIfMissing, errorIfExists, compression, writeBufferSize,
                     blockSize, maxOpenFiles, blockRestartInterval, maxFileSize, cacheSize, infoLogLevel, readOnly);
  worker->Queue(env);

  return 0;
}

struct CloseWorker final : public Worker {
  CloseWorker(napi_env env, Database* database, napi_value callback)
      : Worker(env, database, callback, "leveldown.db.close") {}

  rocksdb::Status Execute(Database& database) override { return database.db_->Close(); }
};

napi_value noop_callback(napi_env env, napi_callback_info info) {
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

NAPI_METHOD(db_put) {
  NAPI_ARGV(4);
  NAPI_DB_CONTEXT();

  const auto key = NapiSlice(env, argv[1]);
  const auto val = NapiSlice(env, argv[2]);

  NAPI_PENDING_EXCEPTION();

  rocksdb::WriteOptions options;
  return ToError(env, database->db_->Put(options, key, val));
}

struct GetWorker final : public Worker {
  GetWorker(napi_env env,
            Database* database,
            napi_value callback,
            const std::string& key,
            const bool asBuffer,
            const bool fillCache)
      : Worker(env, database, callback, "rocks_level.db.get"),
        key_(key),
        asBuffer_(asBuffer),
        fillCache_(fillCache),
        snapshot_(database_->db_->GetSnapshot(),
                  [this](const rocksdb::Snapshot* ptr) { database_->db_->ReleaseSnapshot(ptr); }) {
    database_->IncrementPriorityWork(env);
  }

  rocksdb::Status Execute(Database& database) override {
    rocksdb::ReadOptions options;
    options.fill_cache = fillCache_;
    options.snapshot = snapshot_.get();

    auto status = database.db_->Get(options, database.db_->DefaultColumnFamily(), key_, &value_);

    key_.clear();
    snapshot_ = nullptr;

    return status;
  }

  napi_status OnOk(napi_env env, napi_value callback) override {
    napi_value argv[2];
    NAPI_STATUS_RETURN(napi_get_null(env, &argv[0]));

    NAPI_STATUS_RETURN(Convert(env, value_, asBuffer_, argv[1]));

    value_.Reset();

    return CallFunction(env, callback, 2, argv);
  }

  void Destroy(napi_env env) override {
    database_->DecrementPriorityWork(env);
    Worker::Destroy(env);
  }

 private:
  std::string key_;
  rocksdb::PinnableSlice value_;
  const bool asBuffer_;
  const bool fillCache_;
  std::shared_ptr<const rocksdb::Snapshot> snapshot_;
};

NAPI_METHOD(db_get) {
  NAPI_ARGV(4);
  NAPI_DB_CONTEXT();

  const auto key = ToString(env, argv[1]);
  const auto options = argv[2];
  const auto asBuffer = EncodingIsBuffer(env, options, "valueEncoding");
  const auto fillCache = BooleanProperty(env, options, "fillCache", true);
  const auto callback = argv[3];

  NAPI_PENDING_EXCEPTION();

  auto worker = new GetWorker(env, database, callback, key, asBuffer, fillCache);
  worker->Queue(env);

  return 0;
}

struct GetManyWorker final : public Worker {
  GetManyWorker(napi_env env,
                Database* database,
                const std::vector<std::string>& keys,
                napi_value callback,
                const bool valueAsBuffer,
                const bool fillCache)
      : Worker(env, database, callback, "leveldown.get.many"),
        keys_(keys),
        valueAsBuffer_(valueAsBuffer),
        fillCache_(fillCache),
        snapshot_(database_->db_->GetSnapshot(),
                  [this](const rocksdb::Snapshot* ptr) { database_->db_->ReleaseSnapshot(ptr); }) {
    database_->IncrementPriorityWork(env);
  }

  rocksdb::Status Execute(Database& database) override {
    rocksdb::ReadOptions options;
    options.fill_cache = fillCache_;
    options.snapshot = snapshot_.get();

    status_ = database.db_->MultiGet(options, std::vector<rocksdb::Slice>(keys_.begin(), keys_.end()), &values_);

    keys_.clear();
    snapshot_ = nullptr;

    for (auto status : status_) {
      if (!status.ok() && !status.IsNotFound()) {
        return status;
      }
    }

    return rocksdb::Status::OK();
  }

  napi_status OnOk(napi_env env, napi_value callback) override {
    const auto size = values_.size();

    napi_value array;
    NAPI_STATUS_RETURN(napi_create_array_with_length(env, size, &array));

    for (size_t idx = 0; idx < size; idx++) {
      napi_value element;
      if (status_[idx].ok()) {
        NAPI_STATUS_RETURN(Convert(env, values_[idx], valueAsBuffer_, element));
      } else {
        NAPI_STATUS_RETURN(napi_get_undefined(env, &element));
      }
      NAPI_STATUS_RETURN(napi_set_element(env, array, static_cast<uint32_t>(idx), element));
    }

    values_.clear();
    status_.clear();

    napi_value argv[2];
    NAPI_STATUS_RETURN(napi_get_null(env, &argv[0]));
    argv[1] = array;
    return CallFunction(env, callback, 2, argv);
  }

  void Destroy(napi_env env) override {
    database_->DecrementPriorityWork(env);
    Worker::Destroy(env);
  }

 private:
  std::vector<std::string> keys_;
  std::vector<std::string> values_;
  std::vector<rocksdb::Status> status_;
  const bool valueAsBuffer_;
  const bool fillCache_;
  std::shared_ptr<const rocksdb::Snapshot> snapshot_;
};

NAPI_METHOD(db_get_many) {
  NAPI_ARGV(4);
  NAPI_DB_CONTEXT();

  std::vector<std::string> keys;
  {
    uint32_t length;
    NAPI_STATUS_THROWS(napi_get_array_length(env, argv[1], &length));

    keys.reserve(length);

    for (uint32_t i = 0; i < length; i++) {
      napi_value element;

      NAPI_STATUS_THROWS(napi_get_element(env, argv[1], i, &element));
      keys.push_back(ToString(env, element));
    }
  }

  const auto options = argv[2];
  const bool asBuffer = EncodingIsBuffer(env, options, "valueEncoding");
  const bool fillCache = BooleanProperty(env, options, "fillCache", true);
  const auto callback = argv[3];

  NAPI_PENDING_EXCEPTION();

  auto worker = new GetManyWorker(env, database, keys, callback, asBuffer, fillCache);
  worker->Queue(env);

  return 0;
}

NAPI_METHOD(db_del) {
  NAPI_ARGV(3);
  NAPI_DB_CONTEXT();

  rocksdb::WriteOptions options;
  return ToError(env, database->db_->Delete(options, NapiSlice(env, argv[1])));
}

NAPI_METHOD(db_clear) {
  NAPI_ARGV(2);
  NAPI_DB_CONTEXT();

  const auto reverse = BooleanProperty(env, argv[1], "reverse", false);
  const auto limit = Int32Property(env, argv[1], "limit", -1);

  const auto lt = RangeOption(env, argv[1], "lt");
  const auto lte = RangeOption(env, argv[1], "lte");
  const auto gt = RangeOption(env, argv[1], "gt");
  const auto gte = RangeOption(env, argv[1], "gte");

  // TODO (perf): Use DeleteRange.

  BaseIterator it(database, reverse, lt, lte, gt, gte, limit, false);

  it.SeekToRange();

  // TODO: add option
  const uint32_t hwm = 16 * 1024;

  rocksdb::WriteBatch batch;
  rocksdb::WriteOptions options;
  rocksdb::Status status;

  while (true) {
    size_t bytesRead = 0;

    while (bytesRead <= hwm && it.Valid() && it.Increment()) {
      const auto key = it.CurrentKey();
      batch.Delete(key);
      bytesRead += key.size();
      it.Next();
    }

    status = it.Status();
    if (!status.ok() || bytesRead == 0) {
      break;
    }

    status = database->db_->Write(options, &batch);
    if (!status.ok()) {
      break;
    }

    batch.Clear();
  }

  it.Close();

  return ToError(env, status);
}

NAPI_METHOD(db_get_property) {
  NAPI_ARGV(2);
  NAPI_DB_CONTEXT();

  const auto property = NapiSlice(env, argv[1]);

  NAPI_PENDING_EXCEPTION();

  std::string value;
  // TODO (fix): Handle return value?
  database->db_->GetProperty(property, &value);

  napi_value result;
  NAPI_STATUS_THROWS(napi_create_string_utf8(env, value.data(), value.size(), &result));

  return result;
}

static void FinalizeIterator(napi_env env, void* data, void* hint) {
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

  auto iterator = new Iterator(database, reverse, keys, values, limit, lt, lte, gt, gte, fillCache, keyAsBuffer,
                               valueAsBuffer, highWaterMarkBytes);
  napi_value result;

  NAPI_STATUS_THROWS(napi_create_external(env, iterator, FinalizeIterator, nullptr, &result));

  // Prevent GC of JS object before the iterator is closed (explicitly or on
  // db close) and keep track of non-closed iterators to end them on db close.
  iterator->Attach(env, result);

  return result;
}

NAPI_METHOD(iterator_seek) {
  NAPI_ARGV(2);
  NAPI_ITERATOR_CONTEXT();

  const auto target = NapiSlice(env, argv[1]);

  NAPI_PENDING_EXCEPTION();

  iterator->first_ = true;
  iterator->Seek(target);

  return 0;
}

NAPI_METHOD(iterator_close) {
  NAPI_ARGV(1);
  NAPI_ITERATOR_CONTEXT();

  iterator->Detach(env);
  iterator->Close();

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
        cache_.emplace_back(k.data(), k.size());
        cache_.emplace_back(v.data(), v.size());
        bytesRead += k.size() + v.size();
      } else if (iterator_->keys_) {
        auto k = iterator_->CurrentKey();
        cache_.emplace_back(k.data(), k.size());
        cache_.push_back({});
        bytesRead += k.size();
      } else if (iterator_->values_) {
        auto v = iterator_->CurrentValue();
        cache_.push_back({});
        cache_.emplace_back(v.data(), v.size());
        bytesRead += v.size();
      }

      if (bytesRead > iterator_->highWaterMarkBytes_ || cache_.size() / 2 >= size_) {
        finished_ = true;
        return rocksdb::Status::OK();
      }
    }

    return iterator_->Status();
  }

  napi_status OnOk(napi_env env, napi_value callback) override {
    const auto size = cache_.size();
    napi_value result;
    NAPI_STATUS_RETURN(napi_create_array_with_length(env, size, &result));

    for (size_t idx = 0; idx < cache_.size(); idx += 2) {
      napi_value key;
      napi_value val;

      NAPI_STATUS_RETURN(Convert(env, cache_[idx + 0], iterator_->keyAsBuffer_, key));
      NAPI_STATUS_RETURN(Convert(env, cache_[idx + 1], iterator_->valueAsBuffer_, val));

      NAPI_STATUS_RETURN(napi_set_element(env, result, static_cast<int>(idx + 0), key));
      NAPI_STATUS_RETURN(napi_set_element(env, result, static_cast<int>(idx + 1), val));
    }

    cache_.clear();

    napi_value argv[3];
    NAPI_STATUS_RETURN(napi_get_null(env, &argv[0]));
    argv[1] = result;
    NAPI_STATUS_RETURN(napi_get_boolean(env, !finished_, &argv[2]));
    return CallFunction(env, callback, 3, argv);
  }

 private:
  std::vector<std::string> cache_;
  Iterator* iterator_ = nullptr;
  uint32_t size_ = 0;
  bool finished_ = false;
};

NAPI_METHOD(iterator_nextv) {
  NAPI_ARGV(3);
  NAPI_ITERATOR_CONTEXT();

  uint32_t size;
  NAPI_STATUS_THROWS(napi_get_value_uint32(env, argv[1], &size));

  const auto callback = argv[2];

  auto worker = new NextWorker(env, iterator, size, callback);
  worker->Queue(env);

  return 0;
}

NAPI_METHOD(batch_do) {
  NAPI_ARGV(3);
  NAPI_DB_CONTEXT();

  const auto operations = argv[1];

  rocksdb::WriteBatch batch;

  uint32_t length;
  NAPI_STATUS_THROWS(napi_get_array_length(env, operations, &length));

  for (uint32_t i = 0; i < length; i++) {
    napi_value element;
    NAPI_STATUS_THROWS(napi_get_element(env, operations, i, &element));

    if (!IsObject(env, element))
      continue;

    const auto type = StringProperty(env, element, "type");

    if (type == "del") {
      if (!HasProperty(env, element, "key"))
        continue;

      const auto key = NapiSlice(env, GetProperty(env, element, "key"));

      batch.Delete(key);
    } else if (type == "put") {
      if (!HasProperty(env, element, "key"))
        continue;
      if (!HasProperty(env, element, "value"))
        continue;

      const auto key = NapiSlice(env, GetProperty(env, element, "key"));
      const auto value = NapiSlice(env, GetProperty(env, element, "value"));

      batch.Put(key, value);
    }
  }

  NAPI_PENDING_EXCEPTION();

  rocksdb::WriteOptions options;
  return ToError(env, database->db_->Write(options, &batch));
}

static void FinalizeBatch(napi_env env, void* data, void* hint) {
  if (data) {
    delete reinterpret_cast<rocksdb::WriteBatch*>(data);
  }
}

NAPI_METHOD(batch_init) {
  NAPI_ARGV(1);
  NAPI_DB_CONTEXT();

  auto batch = new rocksdb::WriteBatch();

  napi_value result;
  NAPI_STATUS_THROWS(napi_create_external(env, batch, FinalizeBatch, nullptr, &result));
  return result;
}

NAPI_METHOD(batch_put) {
  NAPI_ARGV(3);
  NAPI_BATCH_CONTEXT();

  const auto key = NapiSlice(env, argv[1]);
  const auto val = NapiSlice(env, argv[2]);

  NAPI_PENDING_EXCEPTION();

  batch->Put(key, val);

  return 0;
}

NAPI_METHOD(batch_del) {
  NAPI_ARGV(2);
  NAPI_BATCH_CONTEXT();

  const auto key = NapiSlice(env, argv[1]);

  NAPI_PENDING_EXCEPTION();

  batch->Delete(key);

  return 0;
}

NAPI_METHOD(batch_clear) {
  NAPI_ARGV(1);
  NAPI_BATCH_CONTEXT();

  batch->Clear();

  return 0;
}

NAPI_METHOD(batch_write) {
  NAPI_ARGV(3);
  NAPI_DB_CONTEXT();

  rocksdb::WriteBatch* batch;
  NAPI_STATUS_THROWS(napi_get_value_external(env, argv[1], reinterpret_cast<void**>(&batch)));

  rocksdb::WriteOptions options;
  return ToError(env, database->db_->Write(options, batch));
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

  NAPI_EXPORT_FUNCTION(batch_do);
  NAPI_EXPORT_FUNCTION(batch_init);
  NAPI_EXPORT_FUNCTION(batch_put);
  NAPI_EXPORT_FUNCTION(batch_del);
  NAPI_EXPORT_FUNCTION(batch_clear);
  NAPI_EXPORT_FUNCTION(batch_write);
}
