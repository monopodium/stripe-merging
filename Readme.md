This repo is forked from https://github.com/infocom22-lrc-stripe-merging/code (@copyright QingPeng Du). We have made changes to the code in the source repository.

# Overview

This repository is created for "Optimal Data Placement for Stripe Merging in Locally Repairable Codes" , including the system implementation(lrc-stripe-merging-Extensions/ms_code/ directory). Our contribution is to propose a merging-style redundancy transition for LRC(Locally-Repairable-Code)-coded and PC(product code) data.We Design a novel stripe placement for modern data center architecture which achieves excellent transition performance.We find that Merging-style transition is better than existed approaches in terms of generation 
of wide stripe and transition performance. 

For LRC:
Overall,our stripe-merging transition consists of two parts:Coding technique and Pre-designed Placement,they play a major role for different redundancy requirements.Specifically, We are interested in two special case:1)x (k-l-g) lrc stripes merging to a (x * k,x * l,g) lrc stripe.2)x (k-l-g) stripes merging to a (x * k,x * l,x * g) lrc stripe.Also,since cross cluster traffic proves to be dominant,we use cross cluster traffic blocks as transition performance metric.For more details,We recommend referring to our paper.

For PC:
Overall,our stripe-merging transition consists of two parts:Coding technique and Pre-designed Placement,they play a major role for different redundancy requirements.Specifically, We are interested in two special case:1)x (h,v) pc stripes merging to a (xh,v) pc stripe.2)x (h,v) stripes merging to a (h,xv) pc stripe.

# Simulation

We perform simulations to show advantages of our design and explore how much can we gain with different configurations.We consider different $x$ and different parameters $(h,v)$.
We set the number of stripes as 100, and the number of clusters (i.e., $c$)
as $xv+1$, i.e., the number of clusters the DIS placement
spans. We calculate the average transitioning cost for merging every $x$
small-size stripes.


# System Implementation

We implement an erasure coding tiny dfs of about 4000 sloc as testbed and further perform system evalution on it.The architecture follows master-worker style,like many state-of-art distributed file storage suchas HDFS,Ceph.Three major components are client,coordinator,datanode.

Currently,we do not care availabilty issue and just deploy single coordinator,we encourage anyone to perfect the system,for example,you can bring it more availability by adding more CNs and keep them in consistent state by consensus protocol like raft or existed coordinate service like Zookeeper.

Transition process logically looks similar like this.Briefly Speaking,the client sends a rpc to CN which tells CN to start transition.Then,CN generates a transition plan and guides picked DNs to transfer data blocks or complete coding task parallelly.DN actually perform overall blocks sending and receiving.Once DNs complete the task,they send an ack back to CN,so that CN can further modify corresponding meta data and return transition statistics(we use transition duration) to client.We currently focus on two special cases:x=1(g same) and x=2(g double) in experiments. 

User is required to input coding parameter,block size,stripe number and placement policy when start a client to upload stripes.When transition,it asks for transition policy and transition cases(i.e. g same or g double),and finally gets transition duration as result from CN.

Note:We use a physical node to simulate a cluster in productive environment.Similarly,cross cluster bandwidth and inner cluster bandwidth correspond to cross node bandwidth(we deploy system in a LAN) and disk io(SSD in our experiments).


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
