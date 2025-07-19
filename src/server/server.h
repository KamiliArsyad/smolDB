#ifndef SMOLDB_SERVER_H
#define SMOLDB_SERVER_H

#include <grpcpp/grpcpp.h>

#include <memory>
#include <string>

#include "smoldb.h"         // For ProcedureManager
#include "smoldb.grpc.pb.h"

// To be replaced with a native coroutine version in the future.
#define SMOLDB_USE_CALLBACK_API 1

#if SMOLDB_USE_CALLBACK_API
class GrpcCallbackService final : public smoldb::rpc::SmolDBService::CallbackService
{
 public:
  explicit GrpcCallbackService(smoldb::ProcedureManager* proc_mgr);

  grpc::ServerUnaryReactor* ExecuteProcedure(
      grpc::CallbackServerContext* context,
      const smoldb::rpc::ExecuteProcedureRequest* request,
      smoldb::rpc::ExecuteProcedureResponse* response) override;

 private:
  smoldb::ProcedureManager* proc_mgr_;  // Not owned
};
#endif  // SMOLDB_USE_CALLBACK_API

#endif  // SMOLDB_SERVER_H