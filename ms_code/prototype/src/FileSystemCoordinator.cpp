#include <spdlog/sinks/basic_file_sink.h>
#include "ToolBox.h"
#include "FileSystemCoordinator.h"
#include "coordinator.grpc.pb.h"
#include "combination_generator.h"

namespace Product 
{
    FileSystemCoordinator::FileSystemCoordinator(const std::string &mConfPath,
                                const std::string &mMetaPath,
                                const std::string &mLogPath,
                                const std::string &mClusterPath)
                                    : m_coordinatorImpl(mConfPath, mMetaPath, mLogPath,mClusterPath) 
    {
        m_cn_logger = m_coordinatorImpl.getMCnLogger(); //must wait until impl initialized first!
        
        if (!m_coordinatorImpl.isMInitialized()) 
        {
            m_cn_logger->error("initialize FileSystemImpl failed!");
            return;
        }

        m_initialized = true;
    }
    FileSystemCoordinator::CoordinatorImpl::CoordinatorImpl(const std::string &mConfPath, 
                                    const std::string &mMetaPath,
                                    const std::string &mLogPath,
                                    const std::string &mClusterPath)
                                    : m_conf_path(mConfPath), 
                                    m_meta_path(mMetaPath),
                                     m_log_path(mLogPath), 
                                     m_cluster_path(mClusterPath) 
    {
        m_cn_logger = spdlog::basic_logger_mt("cn_logger", mLogPath, true);

        if (!std::filesystem::exists(std::filesystem::path{m_conf_path})) 
        {
            m_cn_logger->error("configure file not exist!");
            std::cout<<"configure file not exist!"<<std::endl;
            return;
        }

        auto init_res = initialize();

        if (!init_res) 
        {
            m_cn_logger->error("configuration file error!");
            std::cout<<"configuration file error!"<<std::endl;
            return;
        }

        auto cluster_res = initcluster();

        if (!cluster_res) {
            m_cn_logger->error("cluster file error!");
            std::cout<<"cluster file error!"<<std::endl;
            return;
        }

        auto m_cluster_info_backup = m_cluster_info;

        /* init all stubs to dn */

        for (int i = 0; i < m_cluster_info_backup.size(); ++i) 
        {
            auto dn_alive = std::vector<std::string>();
            for (auto p:m_cluster_info_backup[i].datanodesuri) 
            {
                //filter out offline DNs
                //by call stubs checkalive
                auto _stub = datanode::FromCoodinator::NewStub(
                        grpc::CreateChannel(p, grpc::InsecureChannelCredentials()));

                int retry = 3;//default redetect 3 times

                while (0 != retry) 
                {
                    grpc::ClientContext clientContext;
                    datanode::CheckaliveCMD Cmd;
                    datanode::RequestResult result;
                    grpc::Status status;

                    status = _stub->checkalive(&clientContext, Cmd, &result);

                    if (status.ok()) 
                    {
                        m_cn_logger->info("{} is living !", p);
                        std::cout<<p<<"{} is living !"<<std::endl;

                        if (!result.trueorfalse()) 
                        {
                            m_cn_logger->warn("but not initialized!");
                            std::cout<<"but not initialized!"<<std::endl;
                            // 3 * 10s is deadline
                            std::this_thread::sleep_for(std::chrono::milliseconds(10));
                            retry--;
                        } 
                        else
                        {
                            dn_alive.push_back(p);
                            m_dn_ptrs.insert(std::make_pair(p, std::move(_stub)));
                            retry = 0;
                        }
                    } else 
                    {
                        m_dn_info.erase(p);
                        std::this_thread::sleep_for(std::chrono::milliseconds(10));
                        retry--;
                    }

                }
            }
            if (!dn_alive.empty()) 
            {   
                m_cluster_info[i].datanodesuri = dn_alive;
            }
            else {
                //whole cluster offline
                m_cluster_info.erase(i);
            }
            //timeout,this node is unreachable ...
        }

        if (!std::filesystem::exists(std::filesystem::path{mMetaPath})) 
        {
            auto clear_res = clearexistedstripes();
            if (!clear_res) {
                std::cout<<"Metapath {} does not exists , clear file system failed!"<<std::endl;
                m_cn_logger->error("Metapath {} does not exists , clear file system failed!", mMetaPath);
                return;
            }
            //create mMetaPath
            std::filesystem::create_directory(std::filesystem::path{m_meta_path}.parent_path());

        } else 
        {
            //            loadhistory(); //todo
            clearexistedstripes();
        }
        m_cn_logger->info("cn initialize success!");
        std::cout<<"cn initialize success!"<<std::endl;

        m_initialized = true;
    }

    bool FileSystemCoordinator::CoordinatorImpl::initialize() 
    {
        /* set default ecschema initialize all DN stubs , load DN info Cluster info */
        //parse /conf/configuration.xml
        //parse xml
        pugi::xml_document xdoc;
        xdoc.load_file(m_conf_path.c_str(), pugi::parse_default, pugi::encoding_utf8);
        auto propertynode = xdoc.child("properties").child("property");
        for (auto propattr = propertynode.first_attribute(); 
            propattr; 
            propattr = propattr.next_attribute()) 
        {
            auto propname = propattr.name();
            auto propvalue = propattr.value();
            if (std::string{"coordinator_IP"} == propname) {
                m_coordinator_IP = propvalue;
                std::cout << "my coordinator uri :" << propvalue << std::endl;
            }
        }

        return true;
    }
    bool FileSystemCoordinator::CoordinatorImpl::initcluster() 
    {
        //parse  cluster.xml
        try 
        {
            pugi::xml_document xdoc;
            xdoc.load_file(m_cluster_path.c_str());
            auto clustersnode = xdoc.child("clusters");

            for (auto clusternode = clustersnode.child("cluster"); 
                clusternode; 
                clusternode = clusternode.next_sibling()) 
            {

                auto id = clusternode.attribute("id").value();
                auto gatewayuri = clusternode.attribute("gateway").value();
                auto datanodes = clusternode.child("nodes");
                int cluster_id = std::stoi(id);

                DataNodeInfo dninfo;
                dninfo.clusterid = cluster_id;
                dninfo.crosscluster_routeruri = gatewayuri;

                std::vector<std::string> dns;

                for (auto eachdn = datanodes.first_child(); 
                    eachdn; 
                    eachdn = eachdn.next_sibling()) 
                {
                    m_dn_info[eachdn.attribute("uri").value()] = dninfo;

                    dns.push_back(eachdn.attribute("uri").value());
                }
                ClusterInfo clusterInfo{dns, gatewayuri, cluster_id, 0};
                m_cluster_info[cluster_id] = clusterInfo;

            }
        } catch (...) 
        {
            return false;
        }
        return true;
    }
    bool FileSystemCoordinator::CoordinatorImpl::clearexistedstripes() 
    {
        //call stubs to invoke DNs clear operation
        datanode::ClearallstripeCMD clearallstripeCmd;
        grpc::Status status;
        for (auto &stub:m_dn_ptrs) 
        {
            grpc::ClientContext clientContext;
            datanode::RequestResult result;
            status = stub.second->clearallstripe(&clientContext, clearallstripeCmd, &result);

            if (status.ok()) 
            {
                if (!result.trueorfalse()) 
                {
                    m_cn_logger->error("{} clear all stripe failed!", stub.first);
                    return false;
                }
            }else 
            {
                m_cn_logger->error("{} rpc error!", stub.first);
            }
        }
        return true;
    }
    FileSystemCoordinator::CoordinatorImpl::~CoordinatorImpl()
    {
        m_cn_logger->info("cn im-memory image flush back to metapath!");
        //flushhistory();
        std::cout<<"to be finished"<<std::endl;
    }
    bool FileSystemCoordinator::CoordinatorImpl::isMInitialized() const 
    {
        return m_initialized;
    }
    const std::shared_ptr<spdlog::logger> &FileSystemCoordinator::CoordinatorImpl::getMCnLogger() const 
    {
        return m_cn_logger;
    }

    const std::string &FileSystemCoordinator::CoordinatorImpl::getMFsUri() const 
    {
        return m_coordinator_IP;
    }
    const std::unordered_map<std::string, DataNodeInfo> &FileSystemCoordinator::CoordinatorImpl::getMDnInfo() const 
    {
        return m_dn_info;
    }




    // FileSystemCoordinator::CoordinatorImpl::~CoordinatorImpl() 
    // {
    //     m_cn_logger->info("cn im-memory image flush back to metapath!");
    //     //flushhistory();
    //     std::cout<<"to be finished"<<std::endl;
    // }

    FileSystemCoordinator::CooNodeFromDNImpl::CooNodeFromDNImpl() 
    {

    }

    const std::shared_ptr<FileSystemCoordinator::CoordinatorImpl> &FileSystemCoordinator::CooNodeFromDNImpl::getCoordinatorImplPtr() const 
    {
        return m_coordinatorImpl_ptr;
    }

    void FileSystemCoordinator::CooNodeFromDNImpl::setCoordinatorImplPtr(const std::shared_ptr<CoordinatorImpl> &mFsimplPtr) 
    {
        m_coordinatorImpl_ptr = mFsimplPtr;
    }

    grpc::Status FileSystemCoordinator::CoordinatorImpl::setplacementpolicy(::grpc::ServerContext *context,
                                                     const ::coordinator::SetPlacementCommand *request,
                                                     ::coordinator::RequestResult *response) {
        if (coordinator::SetPlacementCommand_PLACE_RANDOM == request->place()) {
            m_placementpolicy = PLACE::RANDOM;
        }
        if (coordinator::SetPlacementCommand_PLACE_DIS == request->place()) {
            m_placementpolicy = PLACE::DIS;
        } 
        if (coordinator::SetPlacementCommand_PLACE_AGG == request->place())
        {
            m_placementpolicy = PLACE::AGG;
        }
        response->set_trueorfalse(true);
        return grpc::Status::OK;
    }

}