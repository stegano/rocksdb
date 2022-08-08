#pragma once

#include <assert.h>
#include <napi-macros.h>
#include <node_api.h>

#include <rocksdb/slice.h>
#include <rocksdb/status.h>

#include <array>
#include <optional>
#include <string>

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

static napi_status BooleanProperty(napi_env env, napi_value obj, const std::string_view& key, bool& result) {
  bool has = false;
  NAPI_STATUS_RETURN(napi_has_named_property(env, obj, key.data(), &has));

  if (has) {
    napi_value value;
    NAPI_STATUS_RETURN(napi_get_named_property(env, obj, key.data(), &value));
    NAPI_STATUS_RETURN(napi_get_value_bool(env, value, &result));
  }

  return napi_ok;
}

static napi_status EncodingIsBuffer(napi_env env, napi_value obj, const std::string_view& key, bool& result) {
  bool has = false;
  NAPI_STATUS_RETURN(napi_has_named_property(env, obj, key.data(), &has));

  if (has) {
    napi_value value;
    NAPI_STATUS_RETURN(napi_get_named_property(env, obj, key.data(), &value));

    size_t size;
    NAPI_STATUS_RETURN(napi_get_value_string_utf8(env, value, nullptr, 0, &size));

    // Value is either "buffer" or "utf8" so we can tell them apart just by size
    result = size == 6;
  }

  return napi_ok;
}

static napi_status Uint32Property(napi_env env, napi_value obj, const std::string_view& key, uint32_t& result) {
  bool has = false;
  NAPI_STATUS_RETURN(napi_has_named_property(env, obj, key.data(), &has));

  if (has) {
    napi_value value;
    NAPI_STATUS_RETURN(napi_get_named_property(env, obj, key.data(), &value));
    NAPI_STATUS_RETURN(napi_get_value_uint32(env, value, &result));
  }

  return napi_ok;
}

static napi_status Int32Property(napi_env env, napi_value obj, const std::string_view& key, int32_t& result) {
  bool has = false;
  NAPI_STATUS_RETURN(napi_has_named_property(env, obj, key.data(), &has));

  if (has) {
    napi_value value;
    NAPI_STATUS_RETURN(napi_get_named_property(env, obj, key.data(), &value));
    NAPI_STATUS_RETURN(napi_get_value_int32(env, value, &result));
  }

  return napi_ok;
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

void DestroyReference(void* arg1, void* arg2) {
  auto env = reinterpret_cast<napi_env>(arg1);
  auto ref = reinterpret_cast<napi_ref>(arg2);
  napi_delete_reference(env, ref);
}

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

      napi_ref ref;
      NAPI_STATUS_RETURN(napi_create_reference(env, from, 1, &ref));
      to.PinSlice(rocksdb::Slice(buf, length), DestroyReference, env, ref);
    } else {
      return napi_invalid_arg;
    }
  }

  return napi_ok;
}

template <typename T>
static napi_status StringProperty(napi_env env, napi_value obj, const std::string_view& key, T& result) {
  bool has = false;
  NAPI_STATUS_RETURN(napi_has_named_property(env, obj, key.data(), &has));

  if (has) {
    napi_value value;
    NAPI_STATUS_RETURN(napi_get_named_property(env, obj, key.data(), &value));

    std::string to;
    NAPI_STATUS_RETURN(ToString(env, value, to));

    result = std::move(to);
  }

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
    // napi_create_external_buffer would be nice but is unsafe since node
    // buffers are not read-only.
    return napi_create_buffer_copy(env, s->size(), s->data(), nullptr, &result);
  } else {
    return napi_create_string_utf8(env, s->data(), s->size(), &result);
  }
}

template <typename State, typename T1, typename T2>
napi_status runAsync(const std::string& name, napi_env env, napi_value callback, T1&& execute, T2&& then) {
  struct Worker final {
    static void Execute(napi_env env, void* data) {
      auto worker = reinterpret_cast<Worker*>(data);
      worker->status = worker->execute(worker->state);
    }

    static void Complete(napi_env env, napi_status status, void* data) {
      auto worker = std::unique_ptr<Worker>(reinterpret_cast<Worker*>(data));

      napi_value callback;
      NAPI_STATUS_THROWS_VOID(napi_get_reference_value(env, worker->callbackRef, &callback));

      napi_value global;
      NAPI_STATUS_THROWS_VOID(napi_get_global(env, &global));

      if (worker->status.ok()) {
        std::vector<napi_value> argv;

        argv.resize(1);
        NAPI_STATUS_THROWS_VOID(napi_get_null(env, &argv[0]));

        const auto ret = worker->then(worker->state, env, argv);

        if (ret == napi_ok) {
          NAPI_STATUS_THROWS_VOID(napi_call_function(env, global, callback, argv.size(), argv.data(), nullptr));
        } else {
          const napi_extended_error_info* errInfo = nullptr;
          NAPI_STATUS_THROWS_VOID(napi_get_last_error_info(env, &errInfo));
          auto err = CreateError(env, std::nullopt,
                                 !errInfo || !errInfo->error_message ? "empty error message" : errInfo->error_message);
          NAPI_STATUS_THROWS_VOID(napi_call_function(env, global, callback, 1, &err, nullptr));
        }
      } else {
        auto err = ToError(env, worker->status);
        NAPI_STATUS_THROWS_VOID(napi_call_function(env, global, callback, 1, &err, nullptr));
      }
    }

    ~Worker() {
      if (callbackRef) {
        napi_delete_reference(env, callbackRef);
        callbackRef = nullptr;
      }
      if (asyncWork) {
        napi_delete_async_work(env, asyncWork);
        asyncWork = nullptr;
      }
    }

    napi_env env = nullptr;

    typename std::decay<T1>::type execute;
    typename std::decay<T2>::type then;

    napi_ref callbackRef = nullptr;
    napi_async_work asyncWork = nullptr;
    rocksdb::Status status = rocksdb::Status::OK();
    State state = State();
  };

  auto worker = std::unique_ptr<Worker>(new Worker{env, std::forward<T1>(execute), std::forward<T2>(then)});

  NAPI_STATUS_RETURN(napi_create_reference(env, callback, 1, &worker->callbackRef));
  napi_value asyncResourceName;
  NAPI_STATUS_RETURN(napi_create_string_utf8(env, name.data(), name.size(), &asyncResourceName));
  NAPI_STATUS_RETURN(napi_create_async_work(env, callback, asyncResourceName, Worker::Execute, Worker::Complete,
                                            worker.get(), &worker->asyncWork));

  NAPI_STATUS_RETURN(napi_queue_async_work(env, worker->asyncWork));

  worker.release();

  return napi_ok;
}