#include <datanode.grpc.pb.h>
#include <FileSystemDataNode.h>
#include <asio.hpp>
#include <grpc++/create_channel.h>
#include <spdlog/sinks/basic_file_sink.h>
#include <boost/thread/thread.hpp>
#include <Jerasure/include/jerasure.h>
#include <erasurecoding/LRCCoder.h>
#include <boost/thread/barrier.hpp>
//#include "ToolBox.h"

namespace productcode {
    grpc::Status productcode::FileSystemDataNode::FromCoordinatorImpl::clearallstripe(::grpc::ServerContext *context,
                                                                        const ::datanode::ClearallstripeCMD *request,
                                                                        ::datanode::RequestResult *response) 
    {
        //clear dir mdatapth
        m_dnfromcnimpl_logger->info("clear data directory!");
        std::cout<<"clear data directory!"<<std::endl;
        std::filesystem::remove_all(m_datapath);
        response->set_trueorfalse(true);
        return grpc::Status::OK;
    }
    const std::string &FileSystemDataNode::FromCoordinatorImpl::getMDatapath() const {
        return m_datapath;
    }
    grpc::Status FileSystemDataNode::FromCoordinatorImpl::checkalive(::grpc::ServerContext *context,
                                                  const ::datanode::CheckaliveCMD *request,
                                                  ::datanode::RequestResult *response) 
    {
        if (m_initialized) 
        {
            response->set_trueorfalse(true);
        } else 
        {
            response->set_trueorfalse(false);
        }
        return grpc::Status::OK;
    }

    bool FileSystemDataNode::FromCoordinatorImpl::isInitialized() const {
        return m_initialized;
    }

    void FileSystemDataNode::FromCoordinatorImpl::setInitialized(bool initialized) {
        FromCoordinatorImpl::m_initialized = initialized;
    }
    FileSystemDataNode::FromCoordinatorImpl::FromCoordinatorImpl() 
    {

    }
    FileSystemDataNode::FromCoordinatorImpl::~FromCoordinatorImpl() 
    {

    }
    FileSystemDataNode::~FileSystemDataNode() {
    }





}