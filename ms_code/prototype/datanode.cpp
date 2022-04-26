#include "spdlog/spdlog.h"
#include "spdlog/sinks/basic_file_sink.h" // support for basic file logging

#include <FileSystemDataNode.h>

int main(int argc, char ** argv)
{
    int blksz = std::stoi(argv[1]);

    std::string server_address(argv[2], argv[2] + strlen(argv[2]));
    std::cout << server_address << std::endl;
    //std::string server_address = "0.0.0.0:10001";
    productcode::FileSystemDataNode fileSystemDataNode(blksz, server_address);
    fileSystemDataNode.Run();
    return 0;
}