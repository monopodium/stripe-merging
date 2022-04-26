#include "spdlog/spdlog.h"
#include "spdlog/sinks/basic_file_sink.h" // support for basic file logging
#include "FileSystemCoordinator.h"
int main()
{

    Product::FileSystemCoordinator cn;
    cn.Run();
    return 0;
}