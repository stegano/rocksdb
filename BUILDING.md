# LINUX

- Run `./configure` in `deps/liburing`.
- Clone and build zstd with `CFLAGS="-O3 -fPIC" make -C lib libzstd.a` and copy `libzstd.a` to `/usr/lib/x86_64-linux-gnu`.
- Put zstd headers at `/usr/lib/x86_64-linux-gnu/include`.

# OSX

- Run `./configure` in `deps/liburing`.
- `brew install zstd`
- `brew install boost`
- `brew install folly`