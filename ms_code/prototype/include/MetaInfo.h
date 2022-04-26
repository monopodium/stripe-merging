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
    
}

#endif //LRC_METAINFO_H
