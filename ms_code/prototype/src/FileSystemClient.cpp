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
        
        if(target_column_parities)
        {
            char **input_ptrs  = new char*[c];
            for(int i; i < c;i++){
                for(int j; j < r ;j++)
                {
                    input_ptrs[j] = data_words[j*c+i];
                }
                calculate_parity_ptrs_rs(input_ptrs, r, &target_row_parities[i], 1, cellsize);
            }
            
        }
        if(target_row_parities)
        {
            char **input_ptrs = new char*[r];
            for(int i;i < r; i++){
                for(int j; j < c;j++)
                {
                    input_ptrs[j] = data_words[i*c+j];
                }
                calculate_parity_ptrs_rs(input_ptrs, c, &target_row_parities[i], 1, cellsize);
            }
            
        }
        if(target_row_parities)
        {
            calculate_parity_ptrs_rs(target_row_parities, r, target_global_parities, 1, cellsize);
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

    bool FileSystemClient::UploadStripe(const std::string &srcpath, int stripeid,const ECSchema &ecschema, bool trivial) 
    {
        // grpc::ClientContext getlocationclientContext;
        // coordinator::StripeInfo stripeInfo;
        // stripeInfo.set_stripeid(stripeid);
        // stripeInfo.set_stripe_r(ecschema.r);
        // stripeInfo.set_stripe_c(ecschema.c);
        // stripeInfo.set_blksize(ecschema.blksize);
        // coordinator::StripeDetail stripeDetail;
        // auto res = m_fileSystem_ptr->uploadStripe(&getlocationclientContext, stripeInfo, &stripeDetail);
        if (0/*!res.ok()*/) 
        {
            //m_client_logger->error("upload stripe failed!");
            std::cout<<"upload stripe failed!"<<std::endl;
            return false;
        }
        else 
        {   
            /***check****/
            // const auto& stripeLocation = stripeDetail.stripelocation();
            // if (ecschema.c != stripeLocation.columnsLoc_size()
            //     ||1 != stripeLocation.Last_R_G_size()) 
            // {
            //     //m_client_logger->error("cluster configuration error!");
            //     std::cout<<"cluster configuration error!"<<std::endl;
            //     return false;
            // }
            // for (const auto &d : stripeLocation.columnsLoc()) 
            // {
            //     if(d.dataLoc_size()!=ecschema.r||d.Last_c_size()!=1)
            //     {
            //         //m_client_logger->error("cluster configuration error!");
            //         std::cout<<"cluster configuration error!"<<std::endl;
            //         return false;
            //     }
            // }
            // const auto& Last_R_G = stripeLocation.Last_R_G();
            // if(Last_R_G.dataLoc_size()!=ecschema.r||Last_R_G.Last_c_size()!=1)
            // {
            //     //m_client_logger->error("cluster configuration error!");
            //     std::cout<<"cluster configuration error!"<<std::endl;
            //     return false;
            // }
            /***checkend****/
            //#ifdef DEBUG
            // std::cout << "stripe : "<<stripeDetail.stripeid().stripeid()<<" datanode : \n";
            // for (const auto &d : stripeLocation.columnsLoc()) 
            // {
            //     const auto& columnsLocs = stripeLocation.columnsLoc();
            //     for(const auto &dp : columnsLocs.dataLoc())
            //     {
            //         std::cout << dp << " ";
            //     }
            //     std::cout << columnsLocs.Last_c() << " ";
            // }
            // std::cout << "\n";
            // std::cout << "LastColumn : \n";
            // for (const auto &d : stripeLocation.Last_R_G().dataLoc()) {
            //     std::cout << d << " ";
            // }
            // std::cout << stripeLocation.Last_R_G().Last_c() << " "<<std::endl;
            //#endif

            /***read data file**/
            int srcfd = open(srcpath.c_str(), O_RDONLY);
            if (srcfd < 0) 
            {
                perror("open src file error!");
                return false;
            }
//            std::cout << "open file success !\n";
            int defaultcellsize = ecschema.blksize ;
            /**r*c c r 1**/
            char *total_workspace = new char[((ecschema.r+1)*(ecschema.c+1)) *
                                             defaultcellsize];
            char **datablks = new char *[ecschema.c*ecschema.r];
            char **columnblks = new char *[ecschema.c];
            char **rowblks = new char *[ecschema.r];
            char **Gblks = new char *[1];

            int k = 0;
            for (int i = 0; i < ecschema.r*ecschema.c; ++i, ++k) 
            {
                datablks[i] = &total_workspace[k * defaultcellsize];
            }
            for (int i = 0; i < ecschema.c; ++i, ++k) 
            {
                columnblks[i] = &total_workspace[k * defaultcellsize];
            }
            for (int i = 0; i < ecschema.r; ++i, ++k) 
            {
                rowblks[i] = &total_workspace[k * defaultcellsize];
            }
            Gblks[0] = &total_workspace[k * defaultcellsize];

            int chunklen = defaultcellsize;
            int phase = 1024;
            for(int i = 0; i < ecschema.r*ecschema.c; ++i)
            {
                int readn = pread(srcfd, datablks[i], chunklen*phase,
                i * (chunklen * phase));
                std::cout<<datablks[i]<<std::endl;
            }
            encode(datablks,
                    columnblks, rowblks, Gblks,
                    chunklen * phase /*to set 64 KB*/, ecschema.r, ecschema.c);
            for(int i = 0 ; i < ecschema.c ;i++)
            {
                std::cout<<columnblks[i]<<std::endl;
            }
            for(int i = 0 ; i < ecschema.r ;i++)
            {
                std::cout<<rowblks[i]<<std::endl;
            }
            std::cout<<Gblks[0]<<std::endl;
            
        }
        return true;

    }



}