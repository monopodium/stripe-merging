#include "FileSystemClient.h"
#include "coordinator.grpc.pb.h"
#include <boost/thread/barrier.hpp>
#include <boost/thread/thread.hpp>
#include <asio/ip/tcp.hpp>
#include <asio/connect.hpp>
#include <asio/write.hpp>
#include <asio/read.hpp>
#include "ToolBox.h"
#include "Jerasure/include/cauchy.h"
#include "Jerasure/include/jerasure.h"
#include "Jerasure/include/reed_sol.h"
#include "Jerasure/include/galois.h"

#define DEBUG_INFO(format, ...) printf("File:%s, Line:%d, Function:%s, %s", \
	__FILE__, __LINE__ , __FUNCTION__, ##__VA_ARGS__);

namespace Product{
    static void calculate_parity_ptrs_rs(char **data_ptrs, uint32_t k, char **coding_ptrs, uint32_t m, size_t block_size) 
    {
        int *matrix = reed_sol_vandermonde_coding_matrix(k, m, 8);
        jerasure_matrix_encode(k, m, 8, matrix, data_ptrs, coding_ptrs, block_size);
        free(matrix);
    }

    static void encode(char **data_words,
                    char **target_column_parities,char **target_row_parities, char **target_global_parities,
                    int cellsize /*to set 64 KB*/,int r, int c)
    {
        DEBUG_INFO("%s", "hello world\n");
        if(target_column_parities)
        {
            if(r==1)
            {
                for(int i = 0; i < c; i++)
                {
                    target_column_parities[i] = data_words[i];
                }          
            }else
            {
                char **input_ptrs  = new char*[r];
                for(int i = 0; i < c;i++){
                    for(int j = 0; j < r ;j++)
                    {
                        input_ptrs[j] = data_words[j*c+i];
                        //std::cout<<"j "<<j<<" j*c+i "<<j*c+i<<std::endl<<std::flush;
                    }
                    calculate_parity_ptrs_rs(input_ptrs, r, &target_column_parities[i], 1, cellsize);
                }
            }
            
        }
        if(target_row_parities)
        {
            if(c == 1)
            {
                for(int i = 0; i < r; i++)
                {
                    target_row_parities[i] = data_words[i];
                } 
            }
            else
            {
                char **input_ptrs = new char*[c];
                for(int i = 0;i < r; i++)
                {
                    for(int j = 0; j < c;j++)
                    {
                        input_ptrs[j] = data_words[i*c+j];
                        //std::cout<<"j "<<j<<" i*c+j "<<i*c+j<<std::endl<<std::flush;
                    }
                    calculate_parity_ptrs_rs(input_ptrs, c, &target_row_parities[i], 1, cellsize);
                }
            }
            
        }
        if(target_row_parities)
        {
            if(r==1)
            {
                target_global_parities[0] = target_row_parities[0];
            }
            else
            {
                calculate_parity_ptrs_rs(target_row_parities, r, target_global_parities, 1, cellsize);
            }
        }
    }

    bool FileSystemClient::SetPlacementPolicy(PLACE p) 
    {
        grpc::ClientContext setpolicyctx;

        
        coordinator::SetPlacementCommand SetPlacementCMD;

        if(p==PLACE::RANDOM)
        {
            SetPlacementCMD.set_place(coordinator::SetPlacementCommand_PLACE_RANDOM);
        }

        if(p==PLACE::DIS)
        {
            SetPlacementCMD.set_place(coordinator::SetPlacementCommand_PLACE_DIS);
        }

        if(p==PLACE::AGG)
        {
            SetPlacementCMD.set_place(coordinator::SetPlacementCommand_PLACE_AGG);
        }

        coordinator::RequestResult setpolicyres;
        grpc::Status status =m_coordinator_ptr->setplacementpolicy(&setpolicyctx, SetPlacementCMD, &setpolicyres);
        if(status.ok())
        {
            std::cout<<"Set placement policy successfully:"<<p<<std::endl<<std::flush;
        }
        else
        {
            std::cout<<"Fail to set placement policy"<<p<<std::endl<<std::flush;
        }
        return true;
    }
    
    static void socket_data(std::string uri, char *dstbuffer, int chunklen, int stripe_id)
    {
        int _stripeid = stripe_id;
        asio::io_context _ioc;
        asio::error_code ec;
        //int m_datanodeupload_offset = 100;
        
        auto ipaddr = uri.substr(0, uri.find(':'));
        short port = std::stoi(uri.substr(uri.find(':') + 1));
        short _datatransferport = port + m_datanodeupload_offset;
        asio::ip::tcp::endpoint ep(asio::ip::address::from_string(ipaddr), _datatransferport);
        std::cout<<"ipaddr"<<ipaddr<<"_datatransferport"<<_datatransferport<<std::endl<<std::flush;
        asio::ip::tcp::resolver resolver(_ioc);
        //auto endpoints = resolver.resolve(asio::ip::tcp::v4(), ipaddr, _datatransferport);
        asio::ip::tcp::socket sock_data(_ioc);
        sock_data.connect(ep);
        //sock_data.connect(endpoints);
        auto writen = asio::write(sock_data, asio::buffer(&_stripeid, sizeof(_stripeid)), ec);
        auto writen1 = asio::write(sock_data, asio::buffer(dstbuffer, chunklen), ec);
        //std::cout<<"ec"<<ec<<std::endl<<std::flush;

    }
    bool FileSystemClient::PrintStripeDetail(const coordinator::StripeDetail stripeDetail)
    {
        /*** print location to check **/
            auto stripeLocation = stripeDetail.stripelocation();
            std::cout << "stripe ID: "<<stripeDetail.stripeid().stripeid()<<std::endl<<std::flush;
            for (const auto &d : stripeLocation.columnsloc()) 
            {
                for(const auto &dp : d.dataloc())
                {
                    
                    std::cout<< dp <<std::endl<<std::flush;
                    
                }
                std::cout << d.last_c() <<std::endl<<std::flush;
            }
            //std::cout << "\n";
            std::cout << "LastColumn:"<<std::endl<<std::flush;
            for (const auto &d : stripeLocation.last_r_g().dataloc()) {
                std::cout << d <<std::endl<<std::flush;
            }
            std::cout << stripeLocation.last_r_g().last_c() << " "<<std::endl<<std::flush;
            /**** print location to check **/
            //#endif
    }
    bool FileSystemClient::UploadStripe(const std::string &srcpath, int stripeid,const ECSchema &ecschema, bool trivial) 
    {
        grpc::ClientContext getlocationclientContext;
        coordinator::StripeInfo stripeInfo;
        stripeInfo.set_stripeid(stripeid);
        stripeInfo.set_stripe_r(ecschema.r);
        stripeInfo.set_stripe_c(ecschema.c);
        stripeInfo.set_blksize(ecschema.blksize);
        coordinator::StripeDetail stripeDetail;
        auto res = m_coordinator_ptr->uploadStripe(&getlocationclientContext, stripeInfo, &stripeDetail);
        if (!res.ok()) 
        {
            //m_client_logger->error("upload stripe failed!");
            std::cout<<"upload stripe failed!"<<std::endl<<std::flush;
            return false;
        }
        else 
        {   
            /***check****/
            const auto& stripeLocation = stripeDetail.stripelocation();
            if (ecschema.c != stripeLocation.columnsloc_size()) 
            {
                //m_client_logger->error("cluster configuration error!");
                std::cout<<"cluster configuration error!"<<std::endl<<std::flush;
                return false;
            }
            for (const auto &d : stripeLocation.columnsloc()) 
            {
                if(d.dataloc_size()!=ecschema.r)
                {
                    //m_client_logger->error("cluster configuration error!");
                    std::cout<<"cluster configuration error!"<<std::endl<<std::flush;
                    return false;
                }
            }
            const auto& last_r_g = stripeLocation.last_r_g();
            if(last_r_g.dataloc_size()!=ecschema.r)
            {
                //m_client_logger->error("cluster configuration error!");
                std::cout<<"cluster configuration error!"<<std::endl<<std::flush;
                return false;
            }
            /***checkend****/
            //#ifdef DEBUG
            
            /***read data file**/
            int srcfd = open(srcpath.c_str(), O_RDONLY);
            std::cout<<srcpath.c_str()<<std::endl<<std::flush;
            if (srcfd < 0) 
            {
                perror("open src file error!");
                return false;
            }
//            std::cout << "open file success !\n";
            int defaultcellsize = ecschema.blksize*1024 ;
            /**r*c c r 1**/
            char *total_workspace = new char[((ecschema.r+1)*(ecschema.c+1)) *
                                             (defaultcellsize+1)];
            char **datablks = new char *[ecschema.c*ecschema.r];
            char **columnblks = new char *[ecschema.c];
            char **rowblks = new char *[ecschema.r];
            char **Gblks = new char *[1];

            //DEBUG_INFO("%s", "hello world\n");
            int k = 0;
            for (int i = 0; i < ecschema.r*ecschema.c; ++i, ++k) 
            {
                
                datablks[i] = &total_workspace[k * (defaultcellsize+1)];
            }
            for (int i = 0; i < ecschema.c; ++i, ++k) 
            {
                columnblks[i] = &total_workspace[k * (defaultcellsize+1)];
            }
            for (int i = 0; i < ecschema.r; ++i, ++k) 
            {
                rowblks[i] = &total_workspace[k * (defaultcellsize+1)];
            }
            std::cout<<"k "<<k<<std::endl<<std::flush;
            Gblks[0] = &total_workspace[k * (defaultcellsize+1)];

            //int chunklen = defaultcellsize;
            //int phase = 1024;
            for(int i = 0; i < ecschema.r*ecschema.c; ++i)
            {
                int readn = pread(srcfd, datablks[i], defaultcellsize,
                i * (defaultcellsize));
                //std::cout<<datablks[i]<<std::endl<<std::flush;
            }
            DEBUG_INFO("%s", "hello world\n");
            encode(datablks,
                    columnblks, rowblks, Gblks,
                    defaultcellsize /*to set 64 KB*/, ecschema.r, ecschema.c);
            DEBUG_INFO("%s", "hello world\n");

            /**print stripe to check**/
            // for(int i = 0 ; i < ecschema.c ;i++)
            // {
            //     std::cout<<"columnblks[i]"<<i<<std::endl<<std::flush;
            //     std::cout<<columnblks[i]<<std::endl<<std::flush;
            // }
            // for(int i = 0 ; i < ecschema.r ;i++)
            // {
            //     std::cout<<"rowblks[i]"<<i<<std::endl<<std::flush;
            //     std::cout<<rowblks[i]<<std::endl<<std::flush;
            // }
            // std::cout<<"Gblks[0]"<<std::endl<<std::flush;
            // std::cout<<Gblks[0]<<std::endl<<std::flush;
            /**print stripe to check**/

            
            /***save data**/
            std::cout << "stripe : "<<stripeDetail.stripeid().stripeid()<<" datanode : \n";
            for(int i = 0;i < ecschema.c; i++)
            {
                for(int j = 0;j < ecschema.r; j++)
                {
                    auto uri = stripeLocation.columnsloc()[i].dataloc()[j];
                    std::cout<<" "<< uri << " "<<std::endl<<std::flush;
                    socket_data(uri, datablks[j*ecschema.c+i], defaultcellsize,stripeid);
                    
                }
                auto uri = stripeLocation.columnsloc()[i].last_c();
                std::cout<<"last uri"<<uri<< " "<<std::endl<<std::flush;
                socket_data(uri, columnblks[i], defaultcellsize,stripeid);      
            }
            std::cout<<" "<< "Last_column:" << " "<<std::endl<<std::flush;
            for(int j = 0;j < ecschema.r; j++)
            {
                auto uri = stripeLocation.last_r_g().dataloc()[j];
                std::cout<<" "<< uri << " "<<std::endl<<std::flush;
                socket_data(uri, rowblks[j], defaultcellsize,stripeid);
            }
            
            auto uri = stripeLocation.last_r_g().last_c();
            std::cout<<"last uri"<<uri<< " "<<std::endl<<std::flush;
            socket_data(uri, Gblks[0], defaultcellsize,stripeid);
            
            // grpc::ClientContext checkresultclientContext;
            // coordinator::RequestResult checkres;
            // stripeInfo.set_stripeid(stripeDetail.stripeid().stripeid());
            // auto checkstatus = m_coordinator_ptr->uploadCheck(&checkresultclientContext, stripeInfo, &checkres);
            // if (checkstatus.ok() && checkres.trueorfalse()) {
            //     std::cout << "upload stripe success!"<<std::endl<<std::flush;
            //     m_client_logger->info("upload stripe success!");
            //     return true;
            // }
            
            // std::cout << "upload stripe failed,please retry!"<<std::endl<<std::flush;
            // m_client_logger->error("upload stripe failed,please retry!");
            // grpc::ClientContext rollbackctx;
            // coordinator::StripeId stripeId;
            // stripeId.set_stripeid(stripeid);
            // coordinator::RequestResult rollbackres;
            // m_coordinator_ptr->deleteStripe(&rollbackctx, stripeId, &rollbackres);
            // return false;
        }

        return true;

    }

    std::vector<StripeInfo> FileSystemClient::ListStripes() const 
    {
        std::vector<StripeInfo> ret;
        grpc::ClientContext lsctx;
        coordinator::ListAllStripeCMD cmd;
        coordinator::StripeDetails stripeDetailptrs;
        auto re = m_coordinator_ptr->listAllStripes(&lsctx, cmd,&stripeDetailptrs);
        //std::unique_ptr<coordinator::StripeDetail> stripeDetailptr = std::make_unique<coordinator::StripeDetail>();
        int idx = 0;
       for(const auto &stripeDetailptr:stripeDetailptrs.stripedetail())
        {
            StripeInfo stripeInfo;
            auto stripelocation = stripeDetailptr.stripelocation();
            std::cout<<"stripeInfo"<<std::endl<<std::flush;
            for (int i = 0 ; i < stripelocation.columnsloc_size(); i++)
            {
                
                for (int j = 0 ; j < stripelocation.columnsloc(i).dataloc_size(); j++)
                {
                    stripeInfo.colums_locations[i][j] = std::make_tuple(0,stripelocation.columnsloc(i).dataloc(j));
                }
                stripeInfo.colums_locations[i][stripelocation.columnsloc(i).dataloc_size()] = std::make_tuple(0,stripelocation.columnsloc(i).last_c());
                
            }
            for (int j = 0 ; j < stripelocation.last_r_g().dataloc_size(); j++)
            {
                stripeInfo.G_location[j] = std::make_tuple(0,stripelocation.last_r_g().dataloc(j));
            }
            stripeInfo.G_location[stripelocation.last_r_g().dataloc_size()] = std::make_tuple(0,stripelocation.last_r_g().last_c());
            stripeInfo.stripeid = stripeDetailptr.stripeid().stripeid();
            ret.push_back(stripeInfo);
        }
        // while (reader->Read(stripeLocptr.get())) {
        //     StripeInfo stripeInfo;
        //     for (int i = 0; i < stripeLocptr->dataloc_size(); ++i) {
        //         stripeInfo.blklocation.push_back(stripeLocptr->dataloc(i));
        //     }
        //     stripeInfo.blklocation.emplace_back("\n");
        //     for (int i = 0; i < stripeLocptr->localparityloc_size(); ++i) {
        //         stripeInfo.blklocation.push_back(stripeLocptr->localparityloc(i));
        //     }
        //     stripeInfo.blklocation.emplace_back("\n");
        //     for (int i = 0; i < stripeLocptr->globalparityloc_size(); ++i) {
        //         stripeInfo.blklocation.push_back(stripeLocptr->globalparityloc(i));
        //     }
        //     stripeInfo.blklocation.emplace_back("\n");
        //     stripeInfo.stripeid = idx;
        //     idx++;
        //     ret.push_back(stripeInfo);
        // }

        return ret;
    }

    bool FileSystemClient::DownLoadStripe(const std::string &srcpath, 
    const std::string &dstpath, int stripe_id,int blk_size ) 
    {
        grpc::ClientContext downloadctx;
        coordinator::StripeId stripeId;
        stripeId.set_stripeid(stripe_id);
        coordinator::StripeDetail stripeDetail;
        auto status = m_coordinator_ptr->downloadStripe(&downloadctx, stripeId, &stripeDetail);

        if (!status.ok()) {
            m_client_logger->warn("download stripe {} failed!Please retry!", stripe_id);

            return false;
        }

        PrintStripeDetail(stripeDetail);

        auto stripeLocation = stripeDetail.stripelocation();
        boost::thread_group tp;
        int c = stripeLocation.columnsloc_size();
        int r = stripeLocation.columnsloc(0).dataloc_size();
        int data_block_number = c*r;
        char **dataptrs = new char *[data_block_number];
        std::vector<std::string>datalocations(data_block_number);
        for (int i = 0; i < c; ++i) 
        {
            for(int j = 0; j < r;++j)
            {
                datalocations[j*c+i] = stripeLocation.columnsloc(i).dataloc(j);
            }
            
        }
        
        
        asio::io_context ioc;
        auto readertask = [&](int idx) 
        {

            //int datanodedownload_offset = 32221 - 10001;
            asio::ip::tcp::socket sock(ioc);

            const auto &uri = datalocations[idx];
            auto ipaddr = uritoipaddr(uri);
            auto port = std::stoi(uri.substr(uri.find(':') + 1));
            asio::ip::tcp::endpoint ep(asio::ip::address::from_string(ipaddr), port + m_datanodeupload_offset);

            //local
            if(sock.local_endpoint().address()==ep.address())
            {
                asio::write(sock,asio::buffer(&stripe_id,sizeof(stripe_id)));
                //prepare abs path
                int pathlen = 0;
                asio::read(sock, asio::buffer(&pathlen,sizeof(pathlen)));
                std::string localreadpath(pathlen,'\0');
                asio::read(sock, asio::buffer(localreadpath.data(),pathlen));
                std::ifstream ifs(localreadpath);
                if(ifs.good())
                {
                    ifs.read(dataptrs[idx], blk_size  * 1024);
                    std::cout << "a local block read success!"<<std::endl;
                }else{
                    std::cout << "a local read failed!"<<std::endl;
                }
            }else {
                sock.connect(ep);
                asio::read(sock, asio::buffer(dataptrs[idx], blk_size  * 1024));
    //            std::cout << "thread read a block!" << std::endl;
            }
        };

        for (int i = 0; i < data_block_number; ++i) 
        {
            tp.create_thread([readertask, i] { return readertask(i); });
        }

        tp.join_all();

        if (!std::filesystem::exists("./download/")) {
            std::filesystem::create_directory("./download/");
        }

        std::ofstream ofs("./download/" + std::to_string(stripe_id),
                            std::ios::binary | std::ios::trunc | std::ios::out);

        for (int i = 0; i < data_block_number; ++i) 
        {
            ofs.write(dataptrs[i], blk_size*1024);
            ofs.flush();
        }
        std::cout << ofs.tellp() << "bytes successfully downloaded!" << std::endl;

        //otherwise ... try with hint
        return true;
    }

}