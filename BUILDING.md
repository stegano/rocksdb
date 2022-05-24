# LINUX

- Run `./configure` in `deps/liburing`.
- Update package lib, `apt-get update`.
- Update gcc, `apt-get upgrade gcc`.
- Clone and build zstd with `CFLAGS="-O3 -fPIC" make -C lib libzstd.a`
  - Copy `libzstd.a` to `/usr/lib/x86_64-linux-gnu`.
  - Copy headers to `/usr/lib/x86_64-linux-gnu/include`.
- Clone and build folly with `python3 ./build/fbcode_builder/getdeps.py build --no-tests --extra-cmake-defines='{"CMAKE_CXX_FLAGS": "-fPIC"}'`
  - Copy `libfolly.a` to `/usr/lib/x86_64-linux-gnu`.
  - Copy headers to `/usr/lib/x86_64-linux-gnu/include`.
  - Copy boost headers from folly scratchpad to `/usr/lib/x86_64-linux-gnu/include`.
- `npx prebuildify -t -t 17.8.0 --napi --strip --arch x64`
# OSX

- Run `./configure` in `deps/liburing`.
- `brew install zstd`
- `brew install boost`
- `brew install folly`
- `npx prebuildify -t -t 17.8.0 --napi --strip --arch x64`