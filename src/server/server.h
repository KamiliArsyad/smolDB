#ifndef SMOLDB_SERVER_H
#define SMOLDB_SERVER_H

#include <grpcpp/grpcpp.h>

#include <memory>
#include <string>

#include "agrpc/asio_grpc.hpp"
#include "smoldb.grpc.pb.h"
#include "smoldb.h"

// Forward declare to keep header clean
namespace smoldb
{
class ProcedureManager;
}

class GrpcServer
{
 public:
  GrpcServer(smoldb::ProcedureManager* proc_mgr,
             boost::asio::any_io_executor executor);

  void run(const std::string& server_address);
  void stop();

 private:
  boost::asio::awaitable<void> handle_execute_procedure(
      agrpc::ServerRPC<
          &smoldb::rpc::SmolDBService::AsyncService::RequestExecuteProcedure>&
          rpc,
      smoldb::rpc::ExecuteProcedureRequest& request);

  smoldb::ProcedureManager* proc_mgr_;
  boost::asio::any_io_executor executor_;
  std::unique_ptr<grpc::Server> server_;
  std::unique_ptr<agrpc::GrpcContext> grpc_context_;
};

#endif  // SMOLDB_SERVER_H