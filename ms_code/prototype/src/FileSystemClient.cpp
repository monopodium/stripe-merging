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
                        //std::cout<<"j "<<j<<" j*c+i "<<j*c+i<<std::endl;
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
                        //std::cout<<"j "<<j<<" i*c+j "<<i*c+j<<std::endl;
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
            std::cout<<"Set placement policy successfully:"<<p<<std::endl;
        }
        else
        {
            std::cout<<"Fail to set placement policy"<<p<<std::endl;
        }
        return true;
    }
    static bool socket_data(std::string uri, char *dstbuffer, int chunklen, int stripe_id)
    {
        int _stripeid = stripe_id;
        asio::io_context _ioc;
        asio::error_code ec;
        int _datanodeupload_offset = 100;
        asio::ip::tcp::socket sock_data(_ioc);
        auto ipaddr = uri.substr(0, uri.find(':'));
        short port = std::stoi(uri.substr(uri.find(':') + 1));
        short _datatransferport = port + _datanodeupload_offset;
        asio::ip::tcp::endpoint ep(asio::ip::address::from_string(ipaddr), _datatransferport);
        sock_data.connect(ep);
        auto writen = asio::write(sock_data, asio::buffer(&_stripeid, sizeof(_stripeid)), ec);
        auto writen = asio::write(sock_data, asio::buffer(dstbuffer, chunklen), ec);

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
            std::cout<<"upload stripe failed!"<<std::endl;
            return false;
        }
        else 
        {   
            /***check****/
            const auto& stripeLocation = stripeDetail.stripelocation();
            if (ecschema.c != stripeLocation.columnsloc_size()) 
            {
                //m_client_logger->error("cluster configuration error!");
                std::cout<<"cluster configuration error!"<<std::endl;
                return false;
            }
            for (const auto &d : stripeLocation.columnsloc()) 
            {
                if(d.dataloc_size()!=ecschema.r)
                {
                    //m_client_logger->error("cluster configuration error!");
                    std::cout<<"cluster configuration error!"<<std::endl;
                    return false;
                }
            }
            const auto& last_r_g = stripeLocation.last_r_g();
            if(last_r_g.dataloc_size()!=ecschema.r)
            {
                //m_client_logger->error("cluster configuration error!");
                std::cout<<"cluster configuration error!"<<std::endl;
                return false;
            }
            /***checkend****/
            //#ifdef DEBUG
            
            /***read data file**/
            int srcfd = open(srcpath.c_str(), O_RDONLY);
            std::cout<<srcpath.c_str()<<std::endl;
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
            std::cout<<"k "<<k<<std::endl;
            Gblks[0] = &total_workspace[k * (defaultcellsize+1)];

            //int chunklen = defaultcellsize;
            //int phase = 1024;
            for(int i = 0; i < ecschema.r*ecschema.c; ++i)
            {
                int readn = pread(srcfd, datablks[i], defaultcellsize,
                i * (defaultcellsize));
                //std::cout<<datablks[i]<<std::endl;
            }
            DEBUG_INFO("%s", "hello world\n");
            encode(datablks,
                    columnblks, rowblks, Gblks,
                    defaultcellsize /*to set 64 KB*/, ecschema.r, ecschema.c);
            DEBUG_INFO("%s", "hello world\n");

            /**print stripe to check**/
            for(int i = 0 ; i < ecschema.c ;i++)
            {
                std::cout<<"columnblks[i]"<<i<<std::endl;
                std::cout<<columnblks[i]<<std::endl;
            }
            for(int i = 0 ; i < ecschema.r ;i++)
            {
                std::cout<<"rowblks[i]"<<i<<std::endl;
                std::cout<<rowblks[i]<<std::endl;
            }
            std::cout<<"Gblks[0]"<<std::endl;
            std::cout<<Gblks[0]<<std::endl;
            /**print stripe to check**/

            /*** print location to check **/
            // std::cout << "stripe : "<<stripeDetail.stripeid().stripeid()<<" datanode : \n";
            // for (const auto &d : stripeLocation.columnsloc()) 
            // {
            //     for(const auto &dp : d.dataloc())
            //     {
                    
            //         std::cout<<" "<< dp << " ";
                    
            //     }
            //     std::cout <<" d.last_c()"<< d.last_c() <<" \n";
            // }
            // //std::cout << "\n";
            // std::cout << "LastColumn : \n";
            // for (const auto &d : stripeLocation.last_r_g().dataloc()) {
            //     std::cout << d << " ";
            // }
            // std::cout << stripeLocation.last_r_g().last_c() << " "<<std::endl;
            /**** print location to check **/
            //#endif
            /***save data**/
            std::cout << "stripe : "<<stripeDetail.stripeid().stripeid()<<" datanode : \n";
            for(int i = 0;i < ecschema.c; i++)
            {
                for(int j = 0;j < ecschema.r; j++)
                {
                    auto uri = stripeLocation.columnsloc()[i].dataloc()[j];
                    std::cout<<" "<< uri << " ";
                    socket_data(uri, datablks[j*ecschema.c+i], defaultcellsize,stripeid);
                    
                }
                auto uri = stripeLocation.columnsloc()[ecschema.r].last_c();
                std::cout<<"last uri"<<uri<< " "<<std::endl;
                socket_data(uri, columnblks[i], defaultcellsize,stripeid);      
            }
            for(int j = 0;j < ecschema.r; j++)
            {
                auto uri = stripeLocation.last_r_g().dataloc()[j];
                std::cout<<" "<< uri << " ";
                socket_data(uri, rowblks[j], defaultcellsize,stripeid);
            }
            
            auto uri = stripeLocation.last_r_g().last_c();
            std::cout<<"last uri"<<uri<< " "<<std::endl;
            socket_data(uri, Gblks[0], defaultcellsize,stripeid);
            
            grpc::ClientContext checkresultclientContext;
            coordinator::RequestResult checkres;
            stripeInfo.set_stripeid(stripeDetail.stripeid().stripeid());
            auto checkstatus = m_coordinator_ptr->uploadCheck(&checkresultclientContext, stripeInfo, &checkres);
            if (checkstatus.ok() && checkres.trueorfalse()) {
                std::cout << "upload stripe success!"<<std::endl;
                m_client_logger->info("upload stripe success!");
                return true;
            }
            std::cout << "upload stripe failed,please retry!"<<std::endl;
            m_client_logger->error("upload stripe failed,please retry!");
            grpc::ClientContext rollbackctx;
            coordinator::StripeId stripeId;
            stripeId.set_stripeid(stripeid);
            coordinator::RequestResult rollbackres;
            m_coordinator_ptr->deleteStripe(&rollbackctx, stripeId, &rollbackres);
            return false;
        }

        return true;

    }



}