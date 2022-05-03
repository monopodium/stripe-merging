#ifndef LRC_FILESYSTEMDN_H
#define LRC_FILESYSTEMDN_H


#include <grpc++/health_check_service_interface.h>
#include <grpcpp/server_builder.h>
#include <grpcpp/ext/proto_server_reflection_plugin.h>
#include <asio/io_service.hpp>
#include <asio/ip/tcp.hpp>
#include <spdlog/logger.h>
#include "coordinator.grpc.pb.h"
#include "datanode.grpc.pb.h"
#include "devcommon.h"
#include <spdlog/spdlog.h>
#include "ToolBox.h"

#include <grpc++/create_channel.h>
#include <spdlog/sinks/basic_file_sink.h>

#define DEBUG_INFO(format, ...) printf("File:%s, Line:%d, Function:%s, %s", \
	__FILE__, __LINE__ , __FUNCTION__, ##__VA_ARGS__);
namespace productcode
{
    class FileSystemDataNode 
    {
        FileSystemDataNode(const FileSystemDataNode &) = delete;

        FileSystemDataNode(FileSystemDataNode &&) = delete;

        FileSystemDataNode &operator=(const FileSystemDataNode &) = delete;

        FileSystemDataNode &operator=(FileSystemDataNode &&) = delete;

        std::string m_conf_path;
        std::string m_data_path{"./data/"};
        //std::string m_datanodeupload_port;//socket listenning
        //std::string m_datanodedownload_port;//socket listenning
        std::string m_log_path;

        std::shared_ptr<spdlog::logger> m_dn_logger;

        std::string m_server_address = "0.0.0.0:50051";
        int m_Defaultblocksize = 64;

        int m_datanodeupload_port = 10241;//socket listenning
        int m_datanodedownload_port = 10242;//socket listenning

        public:
            FileSystemDataNode(int mDefaultblocksize = 64,
                        int datanodeupload_port = 10241,
                        int datanodedownload_port = 10242,
                        const std::string server_address = "0.0.0.0:50051",
                        const std::string mConfPath = "./conf/configuration.xml", 
                        const std::string mLogPath = "./log/logFile.txt",
                        const std::string mDataPath = "./data/") 
                        : m_Defaultblocksize(mDefaultblocksize),
                        m_datanodeupload_port(datanodeupload_port),
                        m_datanodedownload_port(datanodedownload_port),
                        m_server_address(server_address),
                        m_conf_path(mConfPath),
                        m_log_path(mLogPath),
                        m_data_path(mDataPath),
                        m_dn_fromcnimpl_ptr(
                            FileSystemDataNode::FromCoordinatorImpl::getptr(datanodeupload_port,datanodedownload_port,server_address,mConfPath, mDataPath)
                            ) 
            {
                //m_dn_fromcnimpl_ptr->setMDefaultblocksize(mDefaultblocksize);
                m_dn_logger = spdlog::basic_logger_mt("datanode_logger", mLogPath, false);
                //m_dn_fromcnimpl_ptr->SetAddress(m_server_address);
                // int datanodedownload_offset_upload = 100;
                // auto ipaddr = Product::uritoipaddr(m_server_address);
                // auto port = std::stoi(m_server_address.substr(m_server_address.find(':') + 1));
                // m_datanodeupload_port = port + datanodedownload_offset_upload;
                // m_datanodedownload_port = port + 
                //m_datanodeupload_port = m_dn_fromcnimpl_ptr->getMDatanodeUploadPort();
                //m_datanodedownload_port = m_dn_fromcnimpl_ptr->getMDatanodeDownloadPort();
            }
            
            ~FileSystemDataNode();

            void Run() 
            {
                if (!m_dn_fromcnimpl_ptr->isInitialized()) 
                {
                    m_dn_logger->error("dnfromcnimpl is still not initialzed!");
                    std::cout<<"dnfromcnimpl is still not initialzed!"<<std::endl;
                    return;
                }

                //need a builder

                //std::string dn_fromcnimpl_rpc_uri = m_dn_fromcnimpl_ptr->getMDnfromcnUri();

                //auto blksz = m_dn_fromcnimpl_ptr->getMDefaultblocksize();

                std::cout << "default blksz: TO be added"<<std::endl;

                //grpc::EnableDefaultHealthCheckService(true);
                //DEBUG_INFO("%s", "hello world");

                grpc::reflection::InitProtoReflectionServerBuilderPlugin();
                DEBUG_INFO("%s", "hello world");

                grpc::ServerBuilder builder;
                // Listen on the given address without any authentication mechanism.
                DEBUG_INFO("%s", "hello world");
                builder.AddListeningPort(m_server_address, grpc::InsecureServerCredentials());
                // Register "service" as the instance through which we'll communicate with
                // clients. In this case it corresponds to an *synchronous* service.
                DEBUG_INFO("%s", "hello world");
                builder.RegisterService(m_dn_fromcnimpl_ptr.get());
                // Finally assemble the server.
                DEBUG_INFO("%s", "hello world");
                std::unique_ptr<grpc::Server> server(builder.BuildAndStart());
                m_dn_logger->info("DataNode Server listening on {}", m_server_address);
                std::cout<<"DataNode Server listening on {}"<<m_server_address<<std::endl;

                // Wait for the server to shutdown. Note that some other thread must be
                // responsible for shutting down the server for this call to ever return.
                server->Wait();
            }

            class FromCoordinatorImpl: 
                public std::enable_shared_from_this<productcode::FileSystemDataNode::FromCoordinatorImpl>,
                public datanode::FromCoodinator::Service 
            {
                private:
                    std::string m_coordinator_IP;
                    std::string m_server_address = "0.0.0.0:50051";
                    int m_datanodeupload_port;
                    int m_datanodedownload_port;
                    asio::io_context m_ioservice;
                    std::string m_confpath;
                    std::string m_datapath;//to read write clear ...
                                            //optional a buffer of a packet size ?
                    std::shared_ptr<spdlog::logger> m_dnfromcnimpl_logger;
                    std::shared_ptr<coordinator::CoordinatorService::Stub> m_fs_stub;
                    std::shared_ptr<coordinator::FromDataNode::Stub> m_cnfromdn_stub;

                    bool m_initialized{false};
                    
                    bool initialize() 
                    {
                        //parse /conf/configuration.xml
                        //parse xml
                        pugi::xml_document xdoc;
                        xdoc.load_file(m_confpath.c_str(), pugi::parse_default, pugi::encoding_utf8);
                        auto propertynode = xdoc.child("properties").child("property");

                        for (auto propattr = propertynode.first_attribute(); propattr; propattr = propattr.next_attribute()) {
                            auto propname = propattr.name();
                            auto propvalue = propattr.value();
                            
                            if (std::string{"coordinator_IP"} == propname) 
                            {
                                m_coordinator_IP = propvalue;
                            }
                            // if (std::string{"datanodeupload_port"} == propname) 
                            // {
                            //     m_datanodeupload_port = propvalue;
                            // }
                            // if (std::string{"datanodedownload_port"} == propname) 
                            // {
                            //     m_datanodedownload_port = propvalue;
                            // }
                            // if (std::string{"datanode_uri"} == propname) 
                            // {
                            //     m_dnfromcn_uri = propvalue;
                            // }
                        }
                        return true;
                    }
                    bool initstub() 
                    {
                        m_fs_stub = std::move(coordinator::CoordinatorService::NewStub(grpc::CreateChannel(
                        m_coordinator_IP, grpc::InsecureChannelCredentials())));
                        return true;
                    }
                    FromCoordinatorImpl(const int datanodeupload_port = 10241,
                                        const int datanodedownload_port = 10242,
                                        const std::string server_address = "0.0.0.0:50051",
                                        const std::string &mConfPath = "./log/logFile.txt",
                                        const std::string &mDatapath = "./data/"
                                        )
                                        : m_datanodeupload_port(datanodeupload_port),
                                        m_datanodedownload_port(datanodedownload_port),
                                        m_server_address(server_address),
                                        m_confpath(mConfPath),
                                        m_datapath(mDatapath)
                                       
                    {
                        std::cout<<"m_datanodeupload_port"<<m_datanodeupload_port<<std::endl;
                        std::cout<<"m_datanodedownload_port"<<m_datanodedownload_port<<std::endl;
                        m_dnfromcnimpl_logger = spdlog::basic_logger_mt("dncnimpl", "./log/logFile2.txt", true);
                        if (!std::filesystem::exists(std::filesystem::path(m_datapath))) 
                        {
                            std::filesystem::create_directory(std::filesystem::path(m_datapath));
                        }
                        auto res = initialize();
                        if (!res) 
                        {
                            m_dnfromcnimpl_logger->error("datanode fromcnimpl initialize failed!");
                            std::cout<<"datanode fromcnimpl initialize failed!"<<std::endl;
                        return;
                        }
                        res = initstub();
                        if (!res) 
                        {
                            m_dnfromcnimpl_logger->error("datanode cnstub initialize failed!");
                            std::cout<<"datanode cnstub initialize failed!"<<std::endl;
                            return;
                        }
                        m_dnfromcnimpl_logger->info("datanode cnstub initialize success!");
                        std::cout<<"datanode cnstub initialize success!"<<std::endl;
                        m_initialized = true;
                    }
                    FromCoordinatorImpl();
                    static std::shared_ptr<asio::ip::tcp::acceptor> prepareacceptor(
                        FileSystemDataNode::FromCoordinatorImpl & dnimpl,short port) 
                    {
                        using namespace asio;
                        static std::unordered_map<int,std::shared_ptr<asio::ip::tcp::acceptor>> mappings;
                        if(mappings.contains(port)) 
                        {
                            return mappings[port];
                        }
                        auto acptptr = std::make_shared<ip::tcp::acceptor>(dnimpl.m_ioservice,
                                            ip::tcp::endpoint(asio::ip::tcp::v4(), port),true);
                        mappings.insert({port,acptptr});

                        return acptptr;//auto move
                    }
                
                    
                public:
                    template<typename ...T >
                    static std::shared_ptr<productcode::FileSystemDataNode::FromCoordinatorImpl> getptr(T&& ...t)
                    {
                        return std::shared_ptr<productcode::FileSystemDataNode::FromCoordinatorImpl>(new FromCoordinatorImpl(std::forward<T>(t)...));
                    }
                    virtual ~FromCoordinatorImpl();

                    grpc::Status clearallstripe(::grpc::ServerContext *context, const ::datanode::ClearallstripeCMD *request,
                                        ::datanode::RequestResult *response) override;

                    grpc::Status checkalive(::grpc::ServerContext *context, const ::datanode::CheckaliveCMD *request,
                                    ::datanode::RequestResult *response) override;

                    grpc::Status handleupload(::grpc::ServerContext *context, const ::datanode::UploadCMD *request,
                                             ::datanode::RequestResult *response) override;
                    std::shared_ptr<productcode::FileSystemDataNode::FromCoordinatorImpl> get_sharedholder();
                    
                    grpc::Status clearstripe(::grpc::ServerContext *context, const ::datanode::StripeId *request,
                                     ::datanode::RequestResult *response) override;
                    bool isMInitialized() const;
                    bool isInitialized() const;
                    void setInitialized(bool initialized);
                    
                    const std::string &getMDatapath() const;
                    void SetAddress(std::string server_address) {
                        m_server_address = server_address;
                    }
                    

            };
            class FromDataNodeImpl : public datanode::FromDataNode::Service 
            {
                public:
            };

            std::shared_ptr<FromCoordinatorImpl> m_dn_fromcnimpl_ptr;
            

    };
}
#endif //LRC_FILESYSTEMDN_H