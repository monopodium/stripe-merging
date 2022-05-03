#ifndef LRC_METAINFO_H
#define LRC_METAINFO_H
#include "devcommon.h"

namespace Product{
    /*
     * this file provides general filesystem meta infomation such as StripeInfo , ClusterInfo etc.
    */
    struct ClusterInfo
    {
        std::vector<std::string> datanodesuri;
        std::string gatewayuri;
        int clusterid;
        int stripeload;//stripes number
        ClusterInfo(const std::vector<std::string> &datanodesuri, 
                    const std::string &gatewayuri, int clusterid,
                    int stripeload) : datanodesuri(datanodesuri), 
                    gatewayuri(gatewayuri), clusterid(clusterid),
                    stripeload(stripeload) {}
        ClusterInfo()=default;
    };
    struct DataNodeInfo
    {
        std::unordered_set<int> stored_stripeid;//{stripeid,block_type(0:data 1:localp 2:globalp)}
        std::string crosscluster_routeruri;
        int clusterid;
        int consumed_space;//count in MB because blk size count in MB too
        int port;
    };
    struct ECSchema{
        ECSchema() =default;

        ECSchema(int r, int c, int blksize):r{r},c{c},blksize{blksize}{}
        int r;
        int c;
        int blksize; // count Bytes
    };
    typedef std::unordered_map<int,std::tuple<int,std::string>> one_column_loc;
    enum ErasureCodingPolicy{
        LRC = 0 // default
    };
    struct StripeInfo{
        /*
         * describe a stripe[same as a file in this project] , consists of stripe width , local parity
         * and global parity blocks , currently our own LRC ECSchema
        */

        //location : an uri vector
        //ie : [{datablk :}ip:port1,ip:port2,...|{local_parity_blk :}ip:port k+1,...|{global_parityblk : }... ]
        //std::vector<std::string> blklocation;

        //ie : [cluster1 , cluster1 ,cluster2,] means blklocation[i] coming from clusterdistribution[i]
        std::vector<int> clusterdistribution;
        int stripe_id = 0;
        std::unordered_map<int,one_column_loc> colums_locations;
        one_column_loc G_location;
        
        std::string dir;
        int stripeid;
        ECSchema ecschema;
        ErasureCodingPolicy ecpolicy;


        bool operator<(const StripeInfo &rhs) const {
            return stripeid < rhs.stripeid;
        }

    };
    
}

#endif //LRC_METAINFO_H
