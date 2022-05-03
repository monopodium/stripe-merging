#include "spdlog/spdlog.h"
#include "spdlog/sinks/basic_file_sink.h" // support for basic file logging

#include <FileSystemDataNode.h>

int main(int argc, char ** argv)
{
    

    int datanodeupload_port = std::stoi(argv[1]);
    int datanodedownload_port = std::stoi(argv[2]);
    int blksz = std::stoi(argv[3]);
    std::string server_address(argv[4], argv[4] + strlen(argv[4]));
    std::cout << server_address << std::endl;
    //std::string server_address = "0.0.0.0:10001";
    productcode::FileSystemDataNode fileSystemDataNode(blksz,datanodeupload_port,datanodedownload_port, server_address);
    fileSystemDataNode.Run();
    return 0;
}