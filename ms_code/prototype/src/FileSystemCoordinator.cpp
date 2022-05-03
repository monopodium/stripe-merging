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
            std::cout<<"configure file not exist!"<<std::endl<<std::flush;
            return;
        }

        auto init_res = initialize();

        if (!init_res) 
        {
            m_cn_logger->error("configuration file error!");
            std::cout<<"configuration file error!"<<std::endl<<std::flush;
            return;
        }

        auto cluster_res = initcluster();
        std::cout<<"m_cluster_info"<<m_cluster_info.size()<<std::endl<<std::flush;

        if (!cluster_res) {
            m_cn_logger->error("cluster file error!");
            std::cout<<"cluster file error!"<<std::endl<<std::flush;
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
                        std::cout<<p<<"{} is living !"<<std::endl<<std::flush;

                        if (!result.trueorfalse()) 
                        {
                            m_cn_logger->warn("but not initialized!");
                            std::cout<<"but not initialized!"<<std::endl<<std::flush;
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
                        std::cout<<p<<"{} is not living !"<<std::endl<<std::flush;
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
                std::cout<<"m_cluster_info.erase(i)"<<std::endl<<std::flush;
                m_cluster_info.erase(i);
            }
            //timeout,this node is unreachable ...
        }

        if (!std::filesystem::exists(std::filesystem::path{mMetaPath})) 
        {
            auto clear_res = clearexistedstripes();
            if (!clear_res) {
                std::cout<<"Metapath {} does not exists , clear file system failed!"<<std::endl<<std::flush;
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
        std::cout<<"cn initialize success!"<<std::endl<<std::flush;

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
                std::cout << "my coordinator uri :" << propvalue << std::endl<<std::flush;
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
    grpc::Status FileSystemCoordinator::CoordinatorImpl::uploadStripe(::grpc::ServerContext *context, const ::coordinator::StripeInfo *request,
                                     ::coordinator::StripeDetail *response)
                        
    {
        std::scoped_lock slk(m_stripeupdatingcount_mtx);
        int stripeid = request->stripeid();
        int blocksize_KB = request->blksize();
        auto retstripeloc = response->mutable_stripelocation();
        int r = request->stripe_r();
        int c = request->stripe_c();
        int sz = request->blksize();
        auto stripeId = response->mutable_stripeid();
        stripeId->set_stripeid(stripeid);
        //int totalcluster = m_cluster_info.size();
        std::cout<<"stripeid"<<stripeid<<"blocksize_KB"<<blocksize_KB<<std::endl<<std::flush;
        

        int c_number = m_cluster_info.size();
        std::vector<int> totalcluster(c_number, 0);
        std::iota(totalcluster.begin(), totalcluster.end(), 0);
        std::cout<<"m_placementpolicy"<<m_placementpolicy<<std::endl<<std::flush;
        std::cout<<"PLACE::RANDOM"<<PLACE::RANDOM<<std::endl<<std::flush;
        std::cout<<"PLACE::AGG"<<PLACE::AGG<<std::endl<<std::flush;
        std::cout<<"PLACE::DIS"<<PLACE::DIS<<std::endl<<std::flush;
        std::cout<<"clusters"<<c_number<<std::endl<<std::flush;

        stripe_locaion stripe_locaion_new;
        stripe_locaion_new.stripe_id = stripeid;
        stripe_locaion_new.r = r;
        stripe_locaion_new.c = c;
        if (m_placementpolicy == PLACE::RANDOM) 
        {
            srand (unsigned (time(0))+stripeid);
            std::vector<int> clusters(totalcluster.begin(), totalcluster.end());
            std::random_shuffle(clusters.begin(), clusters.end());
            
            for(int i = 0; i < c+1;i++)
            {
                
                auto column_new = retstripeloc->mutable_last_r_g();
                if( i != c)
                {
                    column_new = retstripeloc->add_columnsloc();
                }
                int flag = 0;
                int nodes_number = m_cluster_info[clusters[i]].datanodesuri.size();
                auto nodes_uri = m_cluster_info[clusters[i]].datanodesuri;

                std::vector<int> totalnodes(nodes_number, 0);
                std::iota(totalnodes.begin(), totalnodes.end(), 0);

                std::random_shuffle(totalnodes.begin(), totalnodes.end());

                for(const auto &node : totalnodes)
                {
                    flag++;
                    if(flag == r+1){
                        if(i!=c)
                        {
                            stripe_locaion_new.colums_locations[i][r] = nodes_uri[node];
                        }else
                        {
                            stripe_locaion_new.G_location[r] = nodes_uri[node];
                        }
                        
                        column_new->set_last_c(nodes_uri[node]);
                        break;
                    }
                    if(i!=c)
                    {
                        stripe_locaion_new.colums_locations[i][flag-1] = nodes_uri[node];
                    }else
                    {
                        stripe_locaion_new.G_location[flag-1] = nodes_uri[node];
                    }    
                    column_new->add_dataloc(nodes_uri[node]);
                }
                
            }
            
        }
        else if(m_placementpolicy == PLACE::AGG)
        {
            for(int i = 0; i < c+1;i++)
            {
                auto column_new = retstripeloc->mutable_last_r_g();
                if( i != c)
                {
                    column_new = retstripeloc->add_columnsloc();
                }
                auto nodes_uri = m_cluster_info[i].datanodesuri;
                for(int j = 0; j < r+1;j++)
                {
                    if(j == r)
                    {
                        if(i!=c)
                        {
                            stripe_locaion_new.colums_locations[i][r] = nodes_uri[j];
                        }else
                        {
                            stripe_locaion_new.G_location[r] = nodes_uri[j];
                        }
                        
                        column_new->set_last_c(nodes_uri[j]);
                        break;
                    }

                    if(i!=c)
                    {
                        stripe_locaion_new.colums_locations[i][j] = nodes_uri[j];
                    }else
                    {
                        stripe_locaion_new.G_location[j] = nodes_uri[j];
                    } 

                    column_new->add_dataloc(nodes_uri[j]);
                }
            }
        }
        else if(m_placementpolicy == PLACE::DIS)
        {
            
            if((m_dis_place_count+1)*c + 1 > c_number)
            {
                return grpc::Status::CANCELLED;
            }
            /** always place the last column into frist cluster**/
            {
                auto column_new = retstripeloc->mutable_last_r_g();
                auto nodes_uri = m_cluster_info[0].datanodesuri;
                for(int j = 0; j < r+1;j++)
                {
                    stripe_locaion_new.G_location[j] = nodes_uri[j];
                    if(j == r)
                    {
                        column_new->set_last_c(nodes_uri[j]);
                    }else
                    {
                        column_new->add_dataloc(nodes_uri[j]);
                    }

                }
            }

            for(int i = 0; i < c;i++)
            {
                auto column_new = retstripeloc->add_columnsloc();
                auto nodes_uri = m_cluster_info[1+c*m_dis_place_count+i].datanodesuri;
                for(int j = 0; j < r+1;j++)
                {
                    if(j == r)
                    {     
                        stripe_locaion_new.colums_locations[i][r] = nodes_uri[j];
                        column_new->set_last_c(nodes_uri[j]);
                        break;
                    }
                    stripe_locaion_new.colums_locations[i][j] = nodes_uri[j];
                    column_new->add_dataloc(nodes_uri[j]);
                }
            }
            m_dis_place_count++;
        }
        else
        {
            return grpc::Status::CANCELLED;
        }
        all_stripe_location[stripeid] = stripe_locaion_new;
        int stripe_number = all_stripe_location.size();
        /*print to check begin*/
        for(const auto &stripe_item : all_stripe_location)
        {
            
            std::cout<<"stripe_id:"<<stripe_item.first<<std::endl<<std::flush;
            std::cout<<"stripe_id:"<<stripe_item.second.stripe_id<<std::endl<<std::flush;
            for(const auto &column_item : stripe_item.second.colums_locations)
            {
                std::cout<<"column_item:"<<column_item.first<<" ";
                for(const auto &block_item : column_item.second)
                {
                    std::cout<<" "<<block_item.first<<" ";
                    std::cout<<" "<<block_item.second<<std::endl<<std::flush;
                    stripe_in_updating[stripeid].push_back(block_item.second);
                }
            }
            for(const auto &column_item : stripe_item.second.G_location)
            {
                std::cout<<"column_item_last:"<<column_item.first<<" ";
                std::cout<<" "<<column_item.second<<std::endl<<std::flush;
                stripe_in_updating[stripeid].push_back(column_item.second);
            }
        }
        for(const auto &node_uri : stripe_in_updating[stripeid])
        {
            if (!askDNhandling(node_uri, stripeid,blocksize_KB)) return grpc::Status::CANCELLED;
        }
        /*print to check end*/
        // auto[cand_dn, cand_lp, cand_gp] = placement_resolve(
        //         {k, l, g, sz}, m_placementpolicy);
        return grpc::Status::OK;
    }

    bool FileSystemCoordinator::CoordinatorImpl::askDNhandling(
        const std::string &dnuri, int stripeid, int blocksize_KB,  bool isupload, bool ispart) 
    {
        m_cn_logger->info("ask {} to wait for client", dnuri);
        std::cout<<"ask {"<<dnuri<<"} to wait for client"<<std::endl<<std::flush;
        grpc::ClientContext handlectx;
        datanode::RequestResult handlereqres;
        grpc::Status status;
        if (isupload) {
            datanode::UploadCMD uploadCmd;
            if (ispart) uploadCmd.set_aspart(ispart);
            uploadCmd.set_blksize_kb(blocksize_KB);
            status = m_dn_ptrs[dnuri]->handleupload(&handlectx, uploadCmd, &handlereqres);
        } else {
            datanode::DownloadCMD downloadCmd;
            if (ispart) downloadCmd.set_aspart(ispart);
            //uploadCmd.set_blksize_kb(blocksize_KB);
            status = m_dn_ptrs[dnuri]->handledownload(&handlectx, downloadCmd, &handlereqres);
        }
        if (status.ok()) {
             std::cout << "rpc askDNhandlestripe success!" << dnuri << std::endl<<std::flush;
            return handlereqres.trueorfalse();
        } else {
            std::cout << "rpc askDNhandlestripe error!" << dnuri << std::endl<<std::flush;
            m_cn_logger->error("rpc askDNhandlestripe error!");
            return false;
        }
    }

    grpc::Status FileSystemCoordinator::CoordinatorImpl::deleteStripe
    (::grpc::ServerContext *context, const ::coordinator::StripeId *request,::coordinator::RequestResult *response) 
    {

        std::cout << "delete stripe" << request->stripeid() << std::endl<<std::flush;
        std::scoped_lock slk(m_stripeupdatingcount_mtx);
        for (auto dnuri:stripe_in_updating[request->stripeid()]) {
            grpc::ClientContext deletestripectx;
            datanode::StripeId stripeId;
            stripeId.set_stripeid(request->stripeid());
            datanode::RequestResult deleteres;
            m_dn_ptrs[dnuri]->clearstripe(&deletestripectx, stripeId, &deleteres);
            //m_dn_info[dnuri.first].stored_stripeid.erase(request->stripeid());
        }
        //delete
        stripe_in_updating.erase(request->stripeid());
        all_stripe_location.erase(request->stripeid());
        //stripe_in_updatingcounter.erase(request->stripeid());
        //m_fs_image.erase(request->stripeid());

        return grpc::Status::OK;
    }
    grpc::Status FileSystemCoordinator::CoordinatorImpl::uploadCheck(::grpc::ServerContext *context, const ::coordinator::StripeInfo *request,
                                              ::coordinator::RequestResult *response) {
        //handle client upload check
        //check if stripeid success or not
        std::unique_lock uniqueLock(m_stripeupdatingcount_mtx);
        //60s deadline
        int stripeid = request->stripeid();
        // auto res = m_updatingcond.wait_for(uniqueLock, std::chrono::seconds(60), [&]() {
        //     return !stripe_in_updatingcounter.contains(request->stripeid());
        // });
        std::cout<<"uploadCheck2"<<stripeid<<std::endl<<std::flush;
        auto res = m_updatingcond.wait_for(uniqueLock, std::chrono::seconds(60), [&]() {
            return all_stripe_location.find(stripeid)->second.issave;
        });
        // if(all_stripe_location.find(stripeid)==all_stripe_location.end())
        // {
        //     if(all_stripe_location.find(stripeid)->second.issave)
        //     {
        //         std::cout<<"uploadCheck1"<<stripeid<<std::endl<<std::flush;
        //         response->set_trueorfalse(true);
        //         return grpc::Status::OK;
        //     }
        //     response->set_trueorfalse(false);
        //     std::cout<<"uploadCheck2"<<stripeid<<std::endl<<std::flush;
        //     return grpc::Status::OK;
            
        // }
        // std::cout<<"uploadCheck3"<<stripeid<<std::endl<<std::flush;
        // return grpc::Status::CANCELLED;
        std::cout<<"uploadCheck3"<<stripeid<<std::endl<<std::flush;
        response->set_trueorfalse(res);
        //if (res) flushhistory();
        return grpc::Status::OK;
        
    }

    grpc::Status FileSystemCoordinator::CoordinatorImpl::reportblockupload(::grpc::ServerContext *context,
                                                    const ::coordinator::StripeId *request,
                                                    ::coordinator::RequestResult *response) 
    {
        std::cout << "datanode " << context->peer() << " receive block of stripe " << request->stripeid()
                  << " from client successfully!\n";
        m_cn_logger->info("datanode {} receive block of stripe {} from client successfully!", context->peer(),
                          request->stripeid());
//        std::scoped_lock lockGuard(m_stripeupdatingcount_mtx);
        //updatestripeupdatingcounter(request->stripeid(), context->peer());
        std::scoped_lock lockGuard(m_stripeupdatingcount_mtx);
        all_stripe_location[request->stripeid()].issave = true;
        response->set_trueorfalse(true);
        return grpc::Status::OK;
    }

    FileSystemCoordinator::CoordinatorImpl::~CoordinatorImpl()
    {
        m_cn_logger->info("cn im-memory image flush back to metapath!");
        //flushhistory();
        std::cout<<"to be finished"<<std::endl<<std::flush;
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
    //     std::cout<<"to be finished"<<std::endl<<std::flush;
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
            std::cout<<"m_placementpolicy = PLACE::RANDOM"<<std::endl<<std::flush;
            
        }
        if (coordinator::SetPlacementCommand_PLACE_DIS == request->place()) {
            m_placementpolicy = PLACE::DIS;
            std::cout<<"m_placementpolicy = PLACE::DIS"<<std::endl<<std::flush;
        } 
        if (coordinator::SetPlacementCommand_PLACE_AGG == request->place())
        {
            m_placementpolicy = PLACE::AGG;
            std::cout<<"m_placementpolicy = PLACE::AGG"<<std::endl<<std::flush;
        }
        response->set_trueorfalse(true);
        return grpc::Status::OK;
    }

    grpc::Status FileSystemCoordinator::CoordinatorImpl::listAllStripes(::grpc::ServerContext *context,
                                                              const ::coordinator::ListAllStripeCMD *request,
                                                              ::coordinator::StripeDetails *response) 
    {
        
        std::scoped_lock slk(m_stripeupdatingcount_mtx);
        //std::scoped_lock lockGuard(m_stripeupdatingcount_mtx);
        for(const auto &stripe_item : all_stripe_location)
        {
            std::cout<<"listAllStripes1 "<<all_stripe_location.size()<<std::endl<<std::flush;
            //coordinator::StripeDetail Stripedetail;
            auto Stripedetail = response->add_stripedetail();
            auto stripeId = Stripedetail->mutable_stripeid();
            stripeId->set_stripeid(stripe_item.first);
            std::cout<<"listAllStripes2 "<<stripe_item.second.r<<std::endl<<std::flush;
            auto retstripeloc = Stripedetail->mutable_stripelocation();

            std::cout<<"stripe_id:"<<stripe_item.first<<std::endl<<std::flush;
            std::cout<<"stripe_id:"<<stripe_item.second.stripe_id<<std::endl<<std::flush;
            for(const auto &column_item : stripe_item.second.colums_locations)
            {
                auto column_new = retstripeloc->add_columnsloc();
                std::cout<<"column_item:"<<column_item.first<<" ";
                for(const auto &block_item : column_item.second)
                {
                    std::cout<<" "<<block_item.first<<" ";
                    std::cout<<" "<<block_item.second<<std::endl<<std::flush;
                    if(block_item.first!=stripe_item.second.r){
                        column_new->add_dataloc(block_item.second);
                    }else{
                        column_new->set_last_c(block_item.second);
                    }
                    
                    
                }
            }
            auto column_new = retstripeloc->mutable_last_r_g();
            for(const auto &column_item : stripe_item.second.G_location)
            {
                
                std::cout<<"column_item_last:"<<column_item.first<<" ";
                std::cout<<" "<<column_item.second<<std::endl<<std::flush;
                if(column_item.first!=stripe_item.second.r){
                        column_new->add_dataloc(column_item.second);
                    }else{
                        column_new->set_last_c(column_item.second);
                    }
                
            }
        }
        return grpc::Status::OK;
    }

    grpc::Status FileSystemCoordinator::CoordinatorImpl::downloadStripe
    (::grpc::ServerContext *context, const ::coordinator::StripeId *request,
    ::coordinator::StripeDetail *response) 
    {

        std::scoped_lock slk(m_stripeupdatingcount_mtx);
        auto retstripeloc = response->mutable_stripelocation();
        int stripeid = request->stripeid();
        std::vector<std::string> datauris;//extractdatablklocation(request->stripeid());
        std::vector<std::string> lpuris;//extractlpblklocation(request->stripeid());
        std::vector<std::string> gpuris;//extractgpblklocation(request->stripeid());
        goto serverequest;

        serverequest:
        int start = 0;
        while (start < m_fs_image[stripeid].size() &&
               m_fs_image[stripeid][start] != "d") {
            datauris.push_back(m_fs_image[stripeid][start]);
            start++;
        }
        //serve download
        bool res = askDNservepull(std::unordered_set<std::string>(datauris.cbegin(), datauris.cend()), "", stripeid);
        if (!res) {
            m_cn_logger->info("datanodes can not serve client download request!");
            return grpc::Status::CANCELLED;
        }

        for (int i = 0; i < datauris.size(); ++i) {
            retstripeloc->add_dataloc(datauris[i]);
        }

        std::cout << "returned locations!\n";
        return grpc::Status::OK;
    }

}