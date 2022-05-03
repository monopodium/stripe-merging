#include "FileSystemClient.h"
#include "ToolBox.h"
#include <iomanip>
#include <bitset>
//#include <iostream>
//#include <stdio.h>
//using namespace std;

int main(int argc, char** argv){
    if(argc != 6) {
        std::cout<<"./client r c placement block_size[KB] stripe_number !"<<std::endl<<std::flush;
        std::cout<<"r"<<std::endl<<std::flush;
        std::cout<<"c"<<std::endl<<std::flush;
        std::cout<<"placement"<<std::endl<<std::flush; //1:Random 2:DIS 3:AGG
        std::cout<<"block_size[KB]"<<std::endl<<std::flush;
        std::cout<<"stripe_number"<<std::endl<<std::flush;
        exit(1);
    }
    int block_size = 64;
    int stripe_number = 100;
    int placement = -1;
    int r,c;

    r = atoi(argv[1]);
    c = atoi(argv[2]);
    placement = atoi(argv[3]);
    block_size = atoi(argv[4]);
    stripe_number = atoi(argv[5]);

    std::cout << "parameters: r "<<r
    <<" c "<<c
    <<" placement "<<placement
    <<" block_size[KB] "<<block_size
    <<" stripe_number "<<stripe_number<<std::endl<<std::flush;

    Product::FileSystemClient fileSystemClient;

    //0:Random 1:DIS 2:AGG
    if(placement==0)
    {
        fileSystemClient.SetPlacementPolicy(Product::FileSystemClient::PLACE::RANDOM);
    }
    if(placement==1)
    {
        fileSystemClient.SetPlacementPolicy(Product::FileSystemClient::PLACE::DIS);
    }
    if(placement==2)
    {
        fileSystemClient.SetPlacementPolicy(Product::FileSystemClient::PLACE::AGG);
    }

    for (int i = 0; i < stripe_number; ++i) 
    {
        Product::RandomStripeGenerator("./teststripes/teststripe" + std::to_string(i) + ".txt", r*c, block_size * 1024);
        fileSystemClient.UploadStripe("./teststripes/teststripe" + std::to_string(i) + ".txt", i, {r, c, block_size}, true);
    }
    //sleep(5*1000);
    std::cout << "ListStripes " << std::endl<<std::flush;
    auto stripeInfos = fileSystemClient.ListStripes();
    for (const auto &stripeDetail : stripeInfos)
    {
        std::cout << "stripeid: " << stripeDetail.stripeid << std::endl<<std::flush;
        for(const auto &datacolums:stripeDetail.colums_locations)
        {
            std::cout<<"column id "<<datacolums.first<<std::endl<<std::flush;
            for(const auto &node:datacolums.second){
                std::cout<< std::get<1>(node.second)<<std::endl<<std::flush;
            }
        }
        std::cout<<"Last column "<<std::endl<<std::flush;
        for(const auto &node:stripeDetail.G_location)
        {
            std::cout<< std::get<1>(node.second)<<std::endl<<std::flush;
        }
        //fileSystemClient.DownLoadStripe("", "", 0);
    }

        // for (const auto &stripe : stripelocs) {
        //     std::cout << "stripeid: " << stripe.stripeid << std::endl<<std::flush;
        //     for (const auto &node : stripe.blklocation) {
        //         std::cout << node << ("\n" == node ? "" : "\t");
        //     }
        // }
    std::cout << std::endl<<std::flush;
    return 0;
}