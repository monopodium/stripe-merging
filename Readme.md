## How to install protobuf
```
git clone -b 3.10.x https://github.com/protocolbuffers/protobuf.git
cd protobuf
git submodule update --init --recursive
./autogen.sh
cd cmake
mkdir build
cd build/
cmake -G Ninja -DCMAKE_INSTALL_PREFIX=/home/ms/Documents/peper2022/protobuf -DCMAKE_POSITION_INDEPENDENT_CODE=ON -DCMAKE_BUILD_TYPE=Release -Dprotobuf_BUILD_TESTS=OFF ..
ninja
sudo ninja install
sudo apt install ninja-build 
```
## How to install grpc

```
git clone https://gitee.com/duduqp/dudugrpc.git
cd grpc
git submodule update --init
mkdir -p cmake/build
cd cmake/build

#set(CMAKE_INSTALL_PREFIX "/home/ms/Documents/peper2022/grpc")

cmake \
  -DCMAKE_BUILD_TYPE=Release \
  -DgRPC_INSTALL=ON \
  -DgRPC_BUILD_TESTS=OFF \
  -DgRPC_SSL_PROVIDER=package \
  ../..

make install -j8 DESTDIR=/home/ms/Documents/peper2022/grpc
```