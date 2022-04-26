export MY_INSTALL_DIR=/home/ms/Documents/my_code
echo MY_INSTALL_DIR
export PATH="$MY_INSTALL_DIR/bin:$PATH"
mkdir -p cmake/build
cd cmake/build
cmake -DCMAKE_PREFIX_PATH=$MY_INSTALL_DIR ../..
make -j

./greeter_server 0.0.0.0:50053
./greeter_client

wondershaper -a eth0 -u 10240
iperf -s
cd /home/ms/Documents/my_code/ms_code/prototype
./build/datanode 64 0.0.0.0:10001

cd /home/ms/Documents/my_code/ms_code/prototype
./build/datanode 64 0.0.0.0:10002

cd /home/ms/Documents/my_code/ms_code/prototype
./build/datanode 64 0.0.0.0:10003

cd /home/ms/Documents/my_code/ms_code/prototype
./build/datanode 64 0.0.0.0:10004

cd /home/ms/Documents/my_code/ms_code/prototype
./build/datanode 64 0.0.0.0:10005

cd /home/ms/Documents/my_code/ms_code/prototype
./build/datanode 64 0.0.0.0:10006

 ./build/coordinator
 ./build/client
gcc main.c -o main.out