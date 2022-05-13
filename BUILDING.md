# LINUX

- Run `./configure` in `deps/liburing`.
- Install and build zstd with `CFLAGS="-O3 -fPIC" make -C lib libzstd.a` and copy to `/usr/lib/x86_64-linux-gnu/libzstd_pic.a`.
- Put headers at `/usr/lib/x86_64-linux-gnu/include`.

# OSX

- Run `./configure` in `deps/liburing`.
- `brew install zstd@1.5.2`