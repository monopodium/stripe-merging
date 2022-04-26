#ifndef LRC_FILESYSTEMCLIENT_H
#define LRC_FILESYSTEMCLIENT_H
#include "devcommon.h"
#include "asio/thread_pool.hpp"
#include "pugixml.hpp"
#include <grpcpp/grpcpp.h>
#include <asio/io_context.hpp>

#include "erasurecoding/LRCCoder.h"

#include "MetaInfo.h"
#include "coordinator.grpc.pb.h"

#include <spdlog/logger.h>
#include <spdlog/sinks/basic_file_sink.h>

namespace Product{
    class FileSystemClient{
        //for validation
        std::string m_meta_path ;
        std::string m_conf_path ;

        std::string m_coordinator_IP ;
        std::string m_log_path ;
        public:
            FileSystemClient(const std::string & p_conf_path="./conf/configuration.xml",
                        const std::string & p_fsimage_path="./meta/fsimage.xml")
                        :m_conf_path(p_conf_path)
            {
                //parse config file
                pugi::xml_document doc;
                doc.load_file(p_conf_path.data(),pugi::parse_default,pugi::encoding_utf8);
                pugi::xml_node properties_node = doc.child("properties");
                auto property_node = properties_node.child("property");
                for(auto attr = property_node.first_attribute();attr;attr=attr.next_attribute())
                {
                    auto prop_name = attr.name();
                    auto prop_value = attr.value();

                    if(std::string{"coordinator_IP"} == prop_name)
                    {
                        m_coordinator_IP = prop_value;
                    }

                    if(std::string{"log_path"} == prop_name)
                    {
                        m_log_path = prop_value;
                    }
                }

                auto channel = grpc::CreateChannel(m_coordinator_IP,grpc::InsecureChannelCredentials());
                m_coordinator_ptr = coordinator::CoordinatorService::NewStub(channel);

                m_client_logger = spdlog::basic_logger_mt("client_logger",m_log_path.append(".client"),true);
            }

            enum PLACE{
                RANDOM = 0,
                DIS = 1,
                AGG = 2
            };
            ~FileSystemClient() = default;
            bool SetPlacementPolicy(PLACE p);
            bool UploadStripe(const std::string &srcpath, int stripeid,const ECSchema &ecschema, bool trivial);
        private:
            std::unique_ptr<coordinator::CoordinatorService::Stub> m_coordinator_ptr;

            std::shared_ptr<spdlog::logger> m_client_logger;

            FileSystemClient(const FileSystemClient &) =  delete;

            FileSystemClient & operator=(const FileSystemClient &) =delete ;

            FileSystemClient(FileSystemClient &&) = delete ;

            FileSystemClient & operator=(FileSystemClient &&) = delete;
        


    };


}
#endif //LRC_FILESYSTEMCLIENT_H
