FROM node:22.9.0

ENV CMAKE_BUILD_PARALLEL_LEVEL=16 MAKEFLAGS=-j16 JOBS=16 DEBUG_LEVEL=0

# liburing-dev
RUN apt update && apt install liburing-dev cmake -y

# Clone and build folly
RUN apt update && apt install sudo -y
RUN mkdir -p /opt/folly && cd /opt/folly && \
  git clone --depth 1 --branch v2024.11.25.00 https://github.com/facebook/folly . && \
  ./build/fbcode_builder/getdeps.py install-system-deps --recursive && \
  ./build/fbcode_builder/getdeps.py build --no-tests --extra-cmake-defines='{"CMAKE_CXX_FLAGS": "-fPIC"}'

# Copy folly (lib + headers + boost) into system folder
RUN cd `cd /opt/folly && ./build/fbcode_builder/getdeps.py show-inst-dir folly` && \
  cp lib/libfolly.a /usr/lib/x86_64-linux-gnu/ && \
  cp -rv include/ /usr/lib/x86_64-linux-gnu && \
  cp -rv ../boost*/include/ /usr/lib/x86_64-linux-gnu

RUN git clone https://github.com/fmtlib/fmt.git && cd fmt && \
  cmake -DCMAKE_POSITION_INDEPENDENT_CODE=TRUE . && \
  make && \
  cp -rv include/ /usr/lib/x86_64-linux-gnu && \
  cp libfmt.a /usr/lib/x86_64-linux-gnu/

# Copy source
WORKDIR /rocks-level
COPY . .

# Build libzstd using makefile in rocksdb
RUN cd deps/rocksdb/rocksdb && make libzstd.a && \
  cp libzstd.a /usr/lib/x86_64-linux-gnu/

# This will build rocksdb (deps/rocksdb/rocksdb.gyp) and then the rocks-level bindings (binding.gyp)
RUN yarn && npx prebuildify -t 22.9.0 --napi --strip --arch x64
