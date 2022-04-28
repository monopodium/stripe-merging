#ifndef LRC_FILESYSTEMCN_H
#define LRC_FILESYSTEMCN_H

#include "coordinator.grpc.pb.h"
#include "datanode.grpc.pb.h"
#include "MetaInfo.h"
#include <grpcpp/grpcpp.h>
#include <grpcpp/ext/proto_server_reflection_plugin.h>
#include <spdlog/logger.h>
#include "devcommon.h"

#include <spdlog/sinks/basic_file_sink.h>
#include <grpc++/create_channel.h>
#define DEBUG_INFO(format, ...) printf("File:%s, Line:%d, Function:%s, %s", \
	__FILE__, __LINE__ , __FUNCTION__, ##__VA_ARGS__);
namespace Product 
{
    class FileSystemCoordinator 
    {

        public:
            FileSystemCoordinator(const std::string &mConfPath = "./conf/configuration.xml",
                     const std::string &mMetaPath = "./meta/fsimage.xml",
                     const std::string &mLogPath = "./log/logFile.txt",
                     const std::string &mClusterPath = "./conf/cluster.xml");
            

            /*********class CoordinatorImpl***************/
            class CoordinatorImpl final :public coordinator::CoordinatorService::Service
            {
                private:
                
                    enum PLACE{
                        RANDOM = 0,
                        DIS = 1,
                        AGG = 2
                    };
                    bool m_initialized{false};
                    std::map<std::string, std::unique_ptr<datanode::FromCoodinator::Stub>> m_dn_ptrs;
                    std::string m_conf_path;
                    std::string m_meta_path;
                    std::string m_log_path;
                    std::string m_cluster_path;
                    std::string m_coordinator_IP;
                    //stub
                    std::map<std::string, std::unique_ptr<datanode::FromCoodinator::Stub>> m_datanode_ptrs;

                    //cluster
                    std::unordered_map<int,ClusterInfo> m_cluster_info;
                    std::unordered_map<std::string, DataNodeInfo> m_dn_info;//cluster.xml

                    std::shared_ptr<spdlog::logger> m_cn_logger;
                    std::atomic<int> m_fs_nextstripeid{0};

                    typedef std::unordered_map<int,std::string> one_column_loc;
                    struct stripe_locaion
                    {
                        int stripe_id = 0;
                        std::unordered_map<int,one_column_loc> colums_locations;
                        one_column_loc G_location;

                    };
                    std::unordered_map<int,stripe_locaion> all_stripe_location;
                    PLACE m_placementpolicy{PLACE::RANDOM};
                    int m_dis_place_count = 0;
                    typedef std::vector<std::string> StripeLocations;
                    std::unordered_map<int,StripeLocations> stripe_in_updating;
                public:                    

                    CoordinatorImpl()=default;

                    CoordinatorImpl(const std::string &mConfPath = "./conf/configuration.xml",
                           const std::string &mMetaPath = "./meta/fsimage.xml",
                           const std::string &mLogPath = "./log/logFile.txt",
                           const std::string &mClusterPath = "./conf/cluster.xml");

                    grpc::Status setplacementpolicy(::grpc::ServerContext *context, 
                                            const::coordinator::SetPlacementCommand *request,
                                            ::coordinator::RequestResult *response) override;

                    grpc::Status uploadStripe(::grpc::ServerContext *context, const ::coordinator::StripeInfo *request,
                                      ::coordinator::StripeDetail *response) override;

                    grpc::Status uploadCheck(::grpc::ServerContext *context, const::coordinator::StripeInfo *request,
                                     ::coordinator::RequestResult *response) override;

                    grpc::Status deleteStripe(::grpc::ServerContext *context, const::coordinator::StripeId *request,
                                      ::coordinator::RequestResult *response) override;

                    bool askDNhandling(const std::string & dnuri,int stripeid,bool isupload=true,bool ispart=false);
                    const std::shared_ptr<spdlog::logger> &getMCnLogger() const;
                    const std::string &getMFsUri() const;
                    const std::unordered_map<std::string, DataNodeInfo> &getMDnInfo() const;
                    
                    
                    bool initialize();
                    bool initcluster() ;
                    bool clearexistedstripes();
                    bool isMInitialized() const;

                    ~CoordinatorImpl();
                    

            };

            /**********CooNodeFromDNImpl**********/
            class CooNodeFromDNImpl final :public coordinator::FromDataNode::Service 
            {
                std::shared_ptr<CoordinatorImpl> m_coordinatorImpl_ptr;
                public:
                    
                    
                    const std::shared_ptr<CoordinatorImpl> &getCoordinatorImplPtr() const;
                    
                    void setCoordinatorImplPtr(const std::shared_ptr<CoordinatorImpl> &mFsimplPtr);

                    CooNodeFromDNImpl();

                    CooNodeFromDNImpl(const std::shared_ptr<CoordinatorImpl> &mFsimplPtr) = delete;
            };


            void Run() 
            {
                //need a builder
                std::string coordinatorImpl_rpc_IP = m_coordinatorImpl.getMFsUri();
                std::cout << "coordinatorImpl_rpc_IP"<<coordinatorImpl_rpc_IP <<std::endl;
                //grpc::EnableDefaultHealthCheckService(true);
                grpc::reflection::InitProtoReflectionServerBuilderPlugin();
                grpc::ServerBuilder builder;
                // Listen on the given address without any authentication mechanism.
                builder.AddListeningPort(coordinatorImpl_rpc_IP, grpc::InsecureServerCredentials());
                // Register "service" as the instance through which we'll communicate with
                // clients. In this case it corresponds to an *synchronous* service.
                builder.RegisterService(&m_coordinatorImpl);
                // Finally assemble the server.

                std::unique_ptr<grpc::Server> server(builder.BuildAndStart());

                std::cout << "coordinatorImpl_rpc_IP"<<coordinatorImpl_rpc_IP <<std::endl;

                m_cn_logger->info("Server listening on {}", coordinatorImpl_rpc_IP);
                std::cout<<"Server listening on {}"<<coordinatorImpl_rpc_IP<<std::endl;

                std::cout << m_coordinatorImpl.getMDnInfo().size() <<std::endl;

                // Wait for the server to shutdown. Note that some other thread must be
                // responsible for shutting down the server for this call to ever return.
                server->Wait();
            }

            bool isInitialzed() const
            {
                return m_initialized;
            }

        private:
            
            bool m_initialized{false};
            CoordinatorImpl m_coordinatorImpl;
            CooNodeFromDNImpl m_CooNodeFromDNImpl;

            std::shared_ptr<spdlog::logger> m_cn_logger;


    };


}
#endif