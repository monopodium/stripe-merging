#Minimum CMake required
cmake_minimum_required(VERSION 3.13)

# Project
project(lrc C CXX)
set(CMAKE_CXX_STANDARD 20)
set(CMAKE_VERBOSE_MAKEFILE ON)
# Include
include_directories("${CMAKE_CURRENT_SOURCE_DIR}/include")
list(APPEND CMAKE_PREFIX_PATH "/home/ms/Documents/my_code")
set(spdlog_DIR ${CMAKE_CURRENT_SOURCE_DIR}/cmake)

# Protobuf
set(protobuf_MODULE_COMPATIBLE TRUE)
find_package(protobuf CONFIG REQUIRED)
message(STATUS "Using protobuf ${protobuf_VERSION}")

set(_PROTOBUF_LIBPROTOBUF protobuf::libprotobuf)


# Protobuf-compiler
set(_PROTOBUF_PROTOC $<TARGET_FILE:protobuf::protoc>)

# gRPC
find_package(gRPC CONFIG REQUIRED)
message(STATUS "Using gRPC ${gRPC_VERSION}")
message(STATUS "gRPC_DIR = ${gRPC_DIR}")
message(STATUS "gRPC_INCLUDE_DIRS = ${gRPC_INCLUDE_DIRS}")
message(STATUS "gRPC_LIBS = ${gRPC_LIBS}")

set(_GRPC_CPP_PLUGIN_EXECUTABLE $<TARGET_FILE:gRPC::grpc_cpp_plugin>)
set(_GRPC_GRPCPP_UNSECURE gRPC::grpc++_unsecure)
set(_REFLECTION gRPC::grpc++_reflection)
set(_GRPC_GRPCPP grpc++)
# Proto file
get_filename_component(cn_proto "${CMAKE_CURRENT_SOURCE_DIR}/src/proto/coordinator.proto" ABSOLUTE)
get_filename_component(dn_proto "${CMAKE_CURRENT_SOURCE_DIR}/src/proto/datanode.proto" ABSOLUTE)
get_filename_component(cn_proto_path "${cn_proto}" PATH)
message("${cn_proto}")
message("${cn_proto_path}")


# Generated sources

set(cn_proto_srcs "${CMAKE_CURRENT_SOURCE_DIR}/src/proto/coordinator.pb.cc")
set(cn_proto_hdrs "${CMAKE_CURRENT_SOURCE_DIR}/src/proto/coordinator.pb.h")
set(cn_grpc_srcs "${CMAKE_CURRENT_SOURCE_DIR}/src/proto/coordinator.grpc.pb.cc")
set(cn_grpc_hdrs "${CMAKE_CURRENT_SOURCE_DIR}/src/proto/coordinator.grpc.pb.h")
set(dn_proto_srcs "${CMAKE_CURRENT_SOURCE_DIR}/src/proto/datanode.pb.cc")
set(dn_proto_hdrs "${CMAKE_CURRENT_SOURCE_DIR}/src/proto/datanode.pb.h")
set(dn_grpc_srcs "${CMAKE_CURRENT_SOURCE_DIR}/src/proto/datanode.grpc.pb.cc")
set(dn_grpc_hdrs "${CMAKE_CURRENT_SOURCE_DIR}/src/proto/datanode.grpc.pb.h")
add_custom_command(
        OUTPUT ${cn_proto_srcs} ${cn_proto_hdrs} ${cn_grpc_srcs} ${cn_grpc_hdrs} ${dn_proto_srcs} ${dn_proto_hdrs} ${dn_grpc_srcs} ${dn_grpc_hdrs}
        COMMAND ${_PROTOBUF_PROTOC}
        ARGS --grpc_out "${CMAKE_CURRENT_SOURCE_DIR}/src/proto"
        --cpp_out "${CMAKE_CURRENT_SOURCE_DIR}/src/proto"
        -I "${CMAKE_CURRENT_SOURCE_DIR}/src/proto"
        --plugin=protoc-gen-grpc="${_GRPC_CPP_PLUGIN_EXECUTABLE}"
        "${cn_proto}" "${dn_proto}"
        DEPENDS "${cn_proto}" "${dn_proto}")
# Include generated *.pb.h files
include_directories("${CMAKE_CURRENT_SOURCE_DIR}/src/proto")

link_directories(${CMAKE_CURRENT_SOURCE_DIR}/lib)

set(Boost_USE_STATIC_LIBS ON)
#find_package(Boost COMPONENTS thread REQUIRED)

set(testsrc ${CMAKE_CURRENT_SOURCE_DIR}/test/test.cpp)
add_executable(unittest ${testsrc})
target_link_libraries(unittest libJerasure.a libgf_complete.a)

# Targets (client|server)
foreach (_target
        client coordinator datanode)
    add_executable(${_target}
            ${cn_proto_srcs}
            ${cn_grpc_srcs}
            ${dn_proto_srcs}
            ${dn_grpc_srcs}
            src/FileSystemClient.cpp
            src/FileSystemCoordinator.cpp
            src/FileSystemDataNode.cpp
            ${_target}.cpp
            src/ToolBox.cpp)
    target_link_libraries(${_target}
            ${_REFLECTION}
            ${_GRPC_GRPCPP}
            ${Boost_LIBRARIES}
	    ${_GRPC_GRPCPP_UNSECURE}
            ${_PROTOBUF_LIBPROTOBUF}
            -lJerasure -lgf_complete -lboost_thread
            )
endforeach ()
#CMAKE_INSTALL_PREFIX