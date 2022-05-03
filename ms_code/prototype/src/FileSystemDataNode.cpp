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
    grpc::Status productcode::FileSystemDataNode::FromCoordinatorImpl::handleupload(::grpc::ServerContext *context,
                                                                      const ::datanode::UploadCMD *request,

                                                                      ::datanode::RequestResult *response) {


        int stripeid = request->stripeid();
        int blocksize_KB = request->blksize_kb();

        std::cout << "try prepare acceptor" << std::endl;
        std::shared_ptr<productcode::FileSystemDataNode::FromCoordinatorImpl> holdme = get_sharedholder();
        
        //auto acptptr = prepareacceptor(*this, m_datanodeupload_port);

        // if (acptptr) 
        // {
        //     std::cout << "prepared acceptor on port " << m_datanodeupload_port << std::endl;
        // }
        // else 
        // {
        //     std::cout << "prepared acceptor failed!" << std::endl;
        // }
        //auto sockptr = std::make_unique<asio::ip::tcp::socket>(m_ioservice);
        int blocksize = blocksize_KB * 1024+2;

        std::cout << "default blk size:" << blocksize << std::endl;
        
        
        //auto _sockptr = std::move(sockptr);
        //auto ptr = std::move(acptptr);
        std::string datadir = m_datapath;
        int total_blksize = blocksize;
        //std::shared_ptr<productcode::FileSystemDataNode::FromCoordinatorImpl> _this = holdme;
        bool aspart = request->aspart();

        auto handler = [/*_sockptr = std::move(
                sockptr), ptr = std::move(acptptr), */ aspart = aspart](
                std::string datadir, int total_blksize,
                std::shared_ptr<productcode::FileSystemDataNode::FromCoordinatorImpl> _this) mutable {
            int stpid = -1;
            int blkid = -1;
            try{
                
        
                std::cout << "handle recv thread start " <<"total_blksize"<<total_blksize<< std::endl;
                
                asio::io_context ioc;
                asio::ip::tcp::acceptor acceptor(ioc, asio::ip::tcp::endpoint(asio::ip::tcp::v4(), _this->m_datanodeupload_port));
                asio::ip::tcp::socket socket = acceptor.accept();

                asio::error_code ec;

                //boost::array<char, boost::numeric_cast<size_t>(total_blksize)> data;
                std::vector<char> buf(total_blksize);
                //boost::asio::write(socket, boost::asio::buffer(data, length));
                std::size_t length = socket.read_some(asio::buffer(&stpid, sizeof(stpid)), ec);
                length = socket.read_some(asio::buffer(buf,total_blksize), ec);
                // for(auto &char_new:buf)
                // {
                //     std::cout<<char_new;
                // }
                // std::cout<<std::endl;
                //length = socket.read_some(asio::buffer(data), ec);
                // if (ec == boost::asio::error::eof) {
                //     std::cout << "连接被 client 妥善的关闭了" << std::endl;
                //     //break;
                // } else if (ec) {
                //     // 其他错误
                //     throw boost::system::system_error(ec);
                // }
                
                //ptr->accept(*_sockptr);
                //std::vector<char> buf(total_blksize);
                //std::cout << "accept client connection" << std::endl;

               // asio::read(*_sockptr, asio::buffer(&stpid, sizeof(stpid)));
                //std::cout<<"ec"<<ec<<std::endl;
                if (aspart) 
                {
                   //asio::read(*_sockptr, asio::buffer(&blkid, sizeof(blkid)));
                    std::cout << "this connection will handle partial coded block " 
                    << stpid << "-" << blkid << std::endl;
                } else 
                {
                    std::cout << "this connection will handle block " << stpid << std::endl;
                }
                //asio::read(*_sockptr, asio::buffer(buf,total_blksize));

                if (aspart) 
                {
                    std::cout << "receive partial coded block " << stpid << "-" << blkid << std::endl;
                }else 
                {
                    std::cout << "receive block" << stpid << std::endl;
                }
                auto targetdir = aspart ? datadir + "part/" : datadir;
                auto writepath = aspart ? targetdir + std::to_string(stpid) + "-" + std::to_string(blkid) : targetdir +
                                                                                                                std::to_string(
                                                                                                                        stpid)+std::to_string(_this->m_datanodeupload_port);
                if (!std::filesystem::exists(std::filesystem::path{targetdir}))
                {
                    std::filesystem::create_directory(targetdir);
                }
                std::ofstream ofs(writepath, std::ios::binary | std::ios::out | std::ios::trunc);
                ofs.write(&buf[0], total_blksize);
                std::cout << "successfully write : " << ofs.tellp() << "bytes" << std::endl;
                ofs.flush();
                        
                                                                                                        
            }catch (std::exception &e){
                std::cout << "exception" << std::endl;
                std::cout << e.what() << std::endl;
            }
            if(stpid!=-1)
            {
                grpc::ClientContext reportctx;
                coordinator::StripeId stripeId;
                stripeId.set_stripeid(stpid);
                coordinator::RequestResult reportres;
                std::cout << "datanode call reportblktransfer to cn!\n";

                const auto &status = _this->m_fs_stub->reportblockupload(&reportctx, stripeId, &reportres);
                
                std::cout << "datanode call reportblktransfer to cn success!\n";
                if (status.ok() && reportres.trueorfalse()) 
                {
                    std::cout << "report stripe transfer reach cn! " << std::endl;
                    _this->m_dnfromcnimpl_logger->info("report stripe transfer reach cn!");

                } else 
                {
                    std::cout << "report stripe transfer not reach cn! " << std::endl;
                    _this->m_dnfromcnimpl_logger->error("report stripe transfer not reach cn!");
                }
            }
            
        };

        
        

        try 
        {
            std::thread h(std::move(handler), m_datapath, blocksize, holdme);
            std::cout << "receive askDNhandling rpc!\n";
            response->set_trueorfalse(true);

            h.detach();
            
            return grpc::Status::OK;
        } catch (std::exception &e) {
            std::cout << "exception" << std::endl;
            std::cout << e.what() << std::endl;
            return grpc::Status::OK;
        }
        
        
    }

    std::shared_ptr<productcode::FileSystemDataNode::FromCoordinatorImpl> FileSystemDataNode::FromCoordinatorImpl::get_sharedholder() {
        return shared_from_this();
    }

    grpc::Status productcode::FileSystemDataNode::FromCoordinatorImpl::clearstripe(::grpc::ServerContext *context, const ::datanode::StripeId *request,
                                                   ::datanode::RequestResult *response) {

        m_dnfromcnimpl_logger->info("clear stripe {}", request->stripeid());
        if (request->stripeid() == -1) 
        {
            std::filesystem::remove_all(m_datapath + "part/");
        }
        else std::filesystem::remove(m_datapath + (std::to_string(request->stripeid())));
        response->set_trueorfalse(true);
        return grpc::Status::OK;
    }

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
        //std::cout<<"m_initialized"<<m_initialized<<std::endl;
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