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

#include "max_rev_operator.h"

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

void noop(void* arg1, void* arg2) {}

// TODO (fix): Should use rocksdb::Slice since "to" cannot outlive "from".
static napi_status ToString(napi_env env, napi_value from, rocksdb::PinnableSlice& to) {
  napi_valuetype type;
  NAPI_STATUS_RETURN(napi_typeof(env, from, &type));

  if (type == napi_string) {
    size_t length = 0;
    NAPI_STATUS_RETURN(napi_get_value_string_utf8(env, from, nullptr, 0, &length));
    to.GetSelf()->resize(length, '\0');
    NAPI_STATUS_RETURN(napi_get_value_string_utf8(env, from, &(*to.GetSelf())[0], length + 1, &length));
    to.PinSelf();
  } else {
    bool isBuffer;
    NAPI_STATUS_RETURN(napi_is_buffer(env, from, &isBuffer));

    if (isBuffer) {
      char* buf = nullptr;
      size_t length = 0;
      NAPI_STATUS_RETURN(napi_get_buffer_info(env, from, reinterpret_cast<void**>(&buf), &length));

      // TODO (fix): Should extend life of "from". Or "to" should be a non-pinnable slice.
      to.PinSlice(rocksdb::Slice(buf, length), noop, nullptr, nullptr);
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
  } else if (status.IsTryAgain()) {
    return CreateError(env, "LEVEL_TRYAGAIN", msg);
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
    using Y = typename std::decay<decltype(*s)>::type;
    auto ptr = new Y(std::move(*s));
    return napi_create_external_buffer(env, ptr->size(), const_cast<char*>(ptr->data()), Finalize<Y>, ptr, &result);
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

struct ColumnFamily {
  napi_ref ref;
  napi_value val;
  rocksdb::ColumnFamilyHandle* handle;
  rocksdb::ColumnFamilyDescriptor descriptor;
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
  std::map<int32_t, ColumnFamily> columns_;
  napi_ref priorityRef_;

 private:
  uint32_t priorityWork_ = 0;
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
    batch.Iterate(this);  // TODO (fix): Error?

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

    // if (database_ && database_->columns_.find(column_family_id) != database_->columns_.end()) {
    //   entry.column = database_->columns_[column_family_id];
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

    // if (database_ && database_->columns_.find(column_family_id) != database_->columns_.end()) {
    //   entry.column = database_->columns_[column_family_id];
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

    // if (database_ && database_->columns_.find(column_family_id) != database_->columns_.end()) {
    //   entry.column = database_->columns_[column_family_id];
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

struct Updates : public BatchIterator {
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
  int64_t start_;
  std::unique_ptr<rocksdb::TransactionLogIterator> iterator_;

 private:
  napi_ref ref_ = nullptr;
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
    *column = database->db_->DefaultColumnFamily();
  } else {
    *column = nullptr;
  }

  return napi_ok;
}

template <typename State,
          typename ExecuteType = std::function<rocksdb::Status(State& state, Database& database)>,
          typename OnOkType = std::function<napi_status(State& state, napi_env env, napi_value callback)>>
struct GenericWorker final : public Worker {
  template <typename T1, typename T2>
  GenericWorker(const std::string& name,
                napi_env env,
                napi_value callback,
                Database* database,
                bool priority,
                T1&& execute,
                T2&& onOK)
      : Worker(env, database, callback, name),
        priority_(priority),
        execute_(std::forward<T1>(execute)),
        onOk_(std::forward<T2>(onOK)) {
    if (priority_) {
      database_->IncrementPriorityWork(env);
    }
  }

  rocksdb::Status Execute(Database& database) override { return execute_(state_, database); }

  napi_status OnOk(napi_env env, napi_value callback) override { return onOk_(state_, env, callback); }

  void Destroy(napi_env env) override {
    if (priority_) {
      database_->DecrementPriorityWork(env);
    }
    Worker::Destroy(env);
  }

 private:
  State state_;
  bool priority_ = false;
  ExecuteType execute_;
  OnOkType onOk_;
};

template <typename State, typename T1, typename T2>
Worker* createWorker(const std::string& name,
                     napi_env env,
                     napi_value callback,
                     Database* database,
                     bool priority,
                     T1&& execute,
                     T2&& onOk) {
  return new GenericWorker<State, typename std::decay<T1>::type, typename std::decay<T2>::type>(
      name, env, callback, database, priority, std::forward<T1>(execute), std::forward<T2>(onOk));
}

template <typename State, typename T1, typename T2>
void runWorker(const std::string& name,
               napi_env env,
               napi_value callback,
               Database* database,
               bool priority,
               T1&& execute,
               T2&& onOk) {
  auto worker = new GenericWorker<State, typename std::decay<T1>::type, typename std::decay<T2>::type>(
      name, env, callback, database, priority, std::forward<T1>(execute), std::forward<T2>(onOk));
  worker->Queue(env);
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
    for (auto& it : database->iterators_) {
      // TODO: does not do `napi_delete_reference`. Problem?
      it->Close();
    }

    for (auto& it : database->updates_) {
      // TODO: does not do `napi_delete_reference`. Problem?
      it->Close();
    }

    for (auto& it : database->columns_) {
      database->db_->DestroyColumnFamilyHandle(it.second.handle);
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
    for (auto& it : database->columns_) {
      napi_delete_reference(env, it.second.ref);
    }
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

template <typename T, typename U>
rocksdb::Status InitOptions(napi_env env, T& columnOptions, const U& options) {
  rocksdb::ConfigOptions configOptions;

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
    ROCKS_STATUS_RETURN(
        rocksdb::SliceTransform::CreateFromString(configOptions, *prefixExtractorOpt, &columnOptions.prefix_extractor));
  }

  const auto comparatorOpt = StringProperty(env, options, "comparator");
  if (comparatorOpt) {
    ROCKS_STATUS_RETURN(
        rocksdb::Comparator::CreateFromString(configOptions, *comparatorOpt, &columnOptions.comparator));
  }

  const auto mergeOperatorOpt = StringProperty(env, options, "mergeOperator");
  if (mergeOperatorOpt) {
    if (*mergeOperatorOpt == "maxRev") {
      columnOptions.merge_operator = std::make_shared<MaxRevOperator>();
    } else {
      ROCKS_STATUS_RETURN(
          rocksdb::MergeOperator::CreateFromString(configOptions, *mergeOperatorOpt, &columnOptions.merge_operator));
    }
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

  const auto filterPolicyOpt = StringProperty(env, options, "filterPolicy");
  if (filterPolicyOpt) {
    rocksdb::ConfigOptions configOptions;
    ROCKS_STATUS_RETURN(
        rocksdb::FilterPolicy::CreateFromString(configOptions, *filterPolicyOpt, &tableOptions.filter_policy));
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
  dbOptions.WAL_ttl_seconds = Uint32Property(env, argv[2], "walTTL").value_or(0) / 1e3;
  dbOptions.WAL_size_limit_MB = Uint32Property(env, argv[2], "walSizeLimit").value_or(0) / 1e6;
  dbOptions.wal_compression = BooleanProperty(env, argv[2], "walCompression").value_or(false)
                                  ? rocksdb::CompressionType::kZSTD
                                  : rocksdb::CompressionType::kNoCompression;
  dbOptions.create_missing_column_families = true;
  dbOptions.unordered_write = BooleanProperty(env, argv[2], "unorderedWrite").value_or(false);
  dbOptions.fail_if_options_file_error = true;
  dbOptions.manual_wal_flush = BooleanProperty(env, argv[2], "manualWalFlush").value_or(false);

  // TODO (feat): dbOptions.listeners

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

  runWorker<std::vector<rocksdb::ColumnFamilyHandle*>>(
      "leveldown.open", env, callback, database, false,
      [=](auto& handles, auto& database) -> rocksdb::Status {
        rocksdb::DB* db = nullptr;
        const auto status = descriptors.empty()
                                ? rocksdb::DB::Open(dbOptions, location, &db)
                                : rocksdb::DB::Open(dbOptions, location, descriptors, &handles, &db);
        database.db_.reset(db);
        return status;
      },
      [=](auto& handles, napi_env env, napi_value callback) -> napi_status {
        napi_value argv[2];
        NAPI_STATUS_RETURN(napi_get_null(env, &argv[0]));

        const auto size = handles.size();
        NAPI_STATUS_RETURN(napi_create_object(env, &argv[1]));

        for (size_t n = 0; n < size; ++n) {
          ColumnFamily column;
          column.handle = handles[n];
          column.descriptor = descriptors[n];
          NAPI_STATUS_RETURN(napi_create_external(env, column.handle, nullptr, nullptr, &column.val));
          NAPI_STATUS_RETURN(napi_create_reference(env, column.val, 1, &column.ref));

          NAPI_STATUS_RETURN(napi_set_named_property(env, argv[1], descriptors[n].name.c_str(), column.val));

          database->columns_[column.handle->GetID()] = column;
        }

        return CallFunction(env, callback, 2, argv);
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
  auto worker = createWorker<State>(
      "leveldown.close", env, callback, database, false,
      [=](auto& state, auto& database) -> rocksdb::Status {
        for (auto& it : database.columns_) {
          database.db_->DestroyColumnFamilyHandle(it.second.handle);
        }
        database.columns_.clear();

        return database.db_->Close();
      },
      [](auto& state, napi_env env, napi_value callback) -> napi_status {
        napi_value argv[1];
        NAPI_STATUS_RETURN(napi_get_null(env, &argv[0]));

        return CallFunction(env, callback, 1, &argv[0]);
      });

  if (!database->HasPriorityWork()) {
    worker->Queue(env);
  } else {
    database->pendingCloseWorker_ = worker;
  }

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

  const bool keyAsBuffer = EncodingIsBuffer(env, argv[1], "keyEncoding");
  const bool valueAsBuffer = EncodingIsBuffer(env, argv[1], "valueEncoding");

  rocksdb::ColumnFamilyHandle* column = nullptr;
  NAPI_STATUS_THROWS(GetColumnFamily(nullptr, env, argv[1], &column));

  auto updates = std::make_unique<Updates>(database, since, keys, values, data, column, keyAsBuffer, valueAsBuffer);

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
  NAPI_STATUS_THROWS(napi_get_value_external(env, argv[0], reinterpret_cast<void**>(&updates)));

  auto callback = argv[1];

  runWorker<rocksdb::BatchResult>(
      "leveldown.updates.next", env, callback, updates->database_, true,
      [=](auto& batchResult, auto& database) -> rocksdb::Status {
        if (!updates->iterator_) {
          rocksdb::TransactionLogIterator::ReadOptions options;
          const auto status = database.db_->GetUpdatesSince(updates->start_, &updates->iterator_, options);
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
      [=](auto& batchResult, napi_env env, napi_value callback) -> napi_status {
        napi_value argv[5];

        NAPI_STATUS_RETURN(napi_get_null(env, &argv[0]));

        if (!batchResult.writeBatchPtr) {
          return CallFunction(env, callback, 1, argv);
        }

        NAPI_STATUS_RETURN(updates->Iterate(env, *batchResult.writeBatchPtr, &argv[1]));

        NAPI_STATUS_RETURN(napi_create_int64(env, batchResult.sequence, &argv[2]));
        NAPI_STATUS_RETURN(napi_create_int64(env, batchResult.writeBatchPtr->Count(), &argv[3]));
        NAPI_STATUS_RETURN(napi_create_int64(env, updates->start_, &argv[4]));

        return CallFunction(env, callback, 5, argv);
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

  const bool valueAsBuffer = EncodingIsBuffer(env, argv[2], "valueEncoding");
  const bool fillCache = BooleanProperty(env, argv[2], "fillCache").value_or(true);
  const bool ignoreRangeDeletions = BooleanProperty(env, argv[2], "ignoreRangeDeletions").value_or(false);

  rocksdb::ColumnFamilyHandle* column;
  NAPI_STATUS_THROWS(GetColumnFamily(database, env, argv[2], &column));

  auto callback = argv[3];

  auto snapshot = std::shared_ptr<const rocksdb::Snapshot>(
      database->db_->GetSnapshot(), [database](auto ptr) { database->db_->ReleaseSnapshot(ptr); });

  runWorker<std::vector<rocksdb::PinnableSlice>>(
      "leveldown.get.many", env, callback, database, true,
      [=, keys = std::move(keys)](auto& values, Database& db) -> rocksdb::Status {
        rocksdb::ReadOptions readOptions;
        readOptions.fill_cache = fillCache;
        readOptions.snapshot = snapshot.get();
        readOptions.async_io = true;
        readOptions.ignore_range_deletions = ignoreRangeDeletions;

        std::vector<rocksdb::Slice> keys2;
        keys2.reserve(keys.size());
        for (const auto& key : keys) {
          keys2.emplace_back(key);
        }
        std::vector<rocksdb::Status> statuses;
        
        statuses.resize(keys2.size());
        values.resize(keys2.size());

        db.db_->MultiGet(readOptions, column, keys2.size(), keys2.data(), values.data(), statuses.data());

        for (size_t idx = 0; idx < keys2.size(); idx++) {
          if (statuses[idx].IsNotFound()) {
            values[idx] = rocksdb::PinnableSlice(nullptr);
          } else if (!statuses[idx].ok()) {
            return statuses[idx];
          }
        }

        return rocksdb::Status::OK();
      },
      [=](auto& values, napi_env env, napi_value callback) -> napi_status {
        napi_value argv[2];
        NAPI_STATUS_RETURN(napi_get_null(env, &argv[0]));

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

        return CallFunction(env, callback, 2, argv);
      });

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

  rocksdb::PinnableSlice property;
  NAPI_STATUS_THROWS(ToString(env, argv[1], property));

  std::string value;
  database->db_->GetProperty(property, &value);

  napi_value result;
  NAPI_STATUS_THROWS(napi_create_string_utf8(env, value.data(), value.size(), &result));

  return result;
}

NAPI_METHOD(db_get_latest_sequence) {
  NAPI_ARGV(1);

  Database* database;
  NAPI_STATUS_THROWS(napi_get_value_external(env, argv[0], reinterpret_cast<void**>(&database)));

  const auto seq = database->db_->GetLatestSequenceNumber();

  napi_value result;
  NAPI_STATUS_THROWS(napi_create_int64(env, seq, &result));

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

  runWorker<State>(
      std::string("leveldown.iterator.next"), env, callback, iterator->database_, true,
      [=](auto& state, Database& db) -> rocksdb::Status {
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
      [=](auto& state, napi_env env, napi_value callback) -> napi_status {
        napi_value argv[3];
        NAPI_STATUS_RETURN(napi_get_null(env, &argv[0]));

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

        return CallFunction(env, callback, 3, argv);
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
    rocksdb::PinnableSlice type;
    rocksdb::PinnableSlice key;
    rocksdb::PinnableSlice value;

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
    } else if (type == "data") {
      napi_value valueProperty;
      NAPI_STATUS_THROWS(napi_get_named_property(env, element, "value", &valueProperty));
      NAPI_STATUS_THROWS(ToString(env, valueProperty, value));

      ROCKS_STATUS_THROWS(batch.PutLogData(value));
    } else if (type == "merge") {
      napi_value keyProperty;
      NAPI_STATUS_THROWS(napi_get_named_property(env, element, "key", &keyProperty));
      NAPI_STATUS_THROWS(ToString(env, keyProperty, key));

      napi_value valueProperty;
      NAPI_STATUS_THROWS(napi_get_named_property(env, element, "value", &valueProperty));
      NAPI_STATUS_THROWS(ToString(env, valueProperty, value));

      ROCKS_STATUS_THROWS(batch.Merge(column, key, value));
    } else {
      ROCKS_STATUS_THROWS(rocksdb::Status::InvalidArgument());
    }
  }

  rocksdb::WriteOptions writeOptions;
  ROCKS_STATUS_THROWS(database->db_->Write(writeOptions, &batch));

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
  NAPI_STATUS_THROWS(napi_get_value_external(env, argv[0], (void**)(&batch)));

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
  ROCKS_STATUS_THROWS(database->db_->Write(writeOptions, batch));

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

  napi_value keysProperty;
  bool keys;
  NAPI_STATUS_THROWS(napi_get_named_property(env, argv[2], "keys", &keysProperty));
  NAPI_STATUS_THROWS(napi_get_value_bool(env, keysProperty, &keys));

  napi_value valuesProperty;
  bool values;
  NAPI_STATUS_THROWS(napi_get_named_property(env, argv[2], "values", &valuesProperty));
  NAPI_STATUS_THROWS(napi_get_value_bool(env, valuesProperty, &values));

  napi_value dataProperty;
  bool data;
  NAPI_STATUS_THROWS(napi_get_named_property(env, argv[2], "data", &dataProperty));
  NAPI_STATUS_THROWS(napi_get_value_bool(env, dataProperty, &data));

  const bool keyAsBuffer = EncodingIsBuffer(env, argv[1], "keyEncoding");
  const bool valueAsBuffer = EncodingIsBuffer(env, argv[1], "valueEncoding");

  rocksdb::ColumnFamilyHandle* column = nullptr;
  NAPI_STATUS_THROWS(GetColumnFamily(nullptr, env, argv[2], &column));

  napi_value result;
  BatchIterator iterator(nullptr, keys, values, data, column, keyAsBuffer, valueAsBuffer);

  NAPI_STATUS_THROWS(iterator.Iterate(env, *batch, &result));

  return result;
}

NAPI_METHOD(db_flush_wal) {
  NAPI_ARGV(2);

  Database* database;
  NAPI_STATUS_THROWS(napi_get_value_external(env, argv[0], reinterpret_cast<void**>(&database)));

  const auto flush = BooleanProperty(env, argv[1], "flush").value_or(false);

  ROCKS_STATUS_THROWS(database->db_->FlushWAL(flush));

  return 0;
}

template <typename T>
napi_status FromLogFile(napi_env env, const T& file, napi_value* obj) {
  NAPI_STATUS_RETURN(napi_create_object(env, obj));

  napi_value pathName;
  NAPI_STATUS_RETURN(napi_create_string_utf8(env, file->PathName().c_str(), NAPI_AUTO_LENGTH, &pathName));
  NAPI_STATUS_RETURN(napi_set_named_property(env, *obj, "pathName", pathName));

  napi_value logNumber;
  NAPI_STATUS_RETURN(napi_create_int64(env, file->LogNumber(), &logNumber));
  NAPI_STATUS_RETURN(napi_set_named_property(env, *obj, "logNumber", logNumber));

  napi_value type;
  NAPI_STATUS_RETURN(napi_create_int64(env, file->Type(), &type));
  NAPI_STATUS_RETURN(napi_set_named_property(env, *obj, "type", type));

  napi_value startSequence;
  NAPI_STATUS_RETURN(napi_create_int64(env, file->StartSequence(), &startSequence));
  NAPI_STATUS_RETURN(napi_set_named_property(env, *obj, "startSequence", startSequence));

  napi_value sizeFileBytes;
  NAPI_STATUS_RETURN(napi_create_int64(env, file->SizeFileBytes(), &sizeFileBytes));
  NAPI_STATUS_RETURN(napi_set_named_property(env, *obj, "sizeFileBytes", sizeFileBytes));

  return napi_ok;
}

NAPI_METHOD(db_get_sorted_wal_files) {
  NAPI_ARGV(1);

  Database* database;
  NAPI_STATUS_THROWS(napi_get_value_external(env, argv[0], reinterpret_cast<void**>(&database)));

  rocksdb::VectorLogPtr files;
  ROCKS_STATUS_THROWS(database->db_->GetSortedWalFiles(files));

  napi_value ret;
  NAPI_STATUS_THROWS(napi_create_array_with_length(env, files.size(), &ret));

  for (size_t n = 0; n < files.size(); ++n) {
    napi_value obj;
    NAPI_STATUS_THROWS(FromLogFile(env, files[n], &obj));
    NAPI_STATUS_THROWS(napi_set_element(env, ret, n, obj));
  }

  return ret;
}

NAPI_METHOD(db_get_current_wal_file) {
  NAPI_ARGV(1);

  Database* database;
  NAPI_STATUS_THROWS(napi_get_value_external(env, argv[0], reinterpret_cast<void**>(&database)));

  std::unique_ptr<rocksdb::LogFile> file;
  ROCKS_STATUS_THROWS(database->db_->GetCurrentWalFile(&file));

  napi_value ret;
  NAPI_STATUS_THROWS(FromLogFile(env, file, &ret));

  return ret;
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

  NAPI_EXPORT_FUNCTION(db_flush_wal);
  NAPI_EXPORT_FUNCTION(db_get_sorted_wal_files);
  NAPI_EXPORT_FUNCTION(db_get_current_wal_file);

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
