#pragma once

#include <assert.h>
#include <napi-macros.h>

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

template <typename T>
static napi_status StringProperty(napi_env env, napi_value obj, const std::string_view& key, T& result) {
  bool has = false;
  NAPI_STATUS_RETURN(napi_has_named_property(env, obj, key.data(), &has));

  if (has) {
    napi_value value;
    NAPI_STATUS_RETURN(napi_get_named_property(env, obj, key.data(), &value));

    std::string str;
    NAPI_STATUS_RETURN(ToString(env, value, str));

    result = std::move(str);
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
    using Y = typename std::decay<decltype(*s)>::type;
    auto ptr = new Y(std::move(*s));
    return napi_create_external_buffer(env, ptr->size(), const_cast<char*>(ptr->data()), Finalize<Y>, ptr, &result);
  } else {
    return napi_create_string_utf8(env, s->data(), s->size(), &result);
  }
}
