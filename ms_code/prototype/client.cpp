#include "FileSystemClient.h"
#include "ToolBox.h"
#include <iomanip>
#include <bitset>
//#include <iostream>
//#include <stdio.h>
//using namespace std;

int main(int argc, char** argv){
    if(argc != 6) {
        std::cout<<"./client r c placement block_size[KB] stripe_number !"<<std::endl;
        std::cout<<"r"<<std::endl;
        std::cout<<"c"<<std::endl;
        std::cout<<"placement"<<std::endl; //1:Random 2:DIS 3:AGG
        std::cout<<"block_size[KB]"<<std::endl;
        std::cout<<"stripe_number"<<std::endl;
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
    <<" stripe_number "<<stripe_number<<std::endl;

    Product::FileSystemClient fileSystemClient;

    //1:Random 2:DIS 3:AGG
    switch (placement)
    {
        case 1:
            {
            fileSystemClient.SetPlacementPolicy(Product::FileSystemClient::PLACE::RANDOM);
            }
        case 2:
            {
            fileSystemClient.SetPlacementPolicy(Product::FileSystemClient::PLACE::DIS);
            }
        case 3:
            {
            fileSystemClient.SetPlacementPolicy(Product::FileSystemClient::PLACE::AGG);
            }
        default:
            {
            fileSystemClient.SetPlacementPolicy(Product::FileSystemClient::PLACE::RANDOM);
            }
    }

    for (int i = 0; i < stripe_number; ++i) 
    {
        Product::RandomStripeGenerator("teststripe" + std::to_string(i) + ".txt", r*c, block_size * 1024);
        fileSystemClient.UploadStripe("teststripe" + std::to_string(i) + ".txt", i, {r, c, block_size}, true);
    }

    return 0;
}