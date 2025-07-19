#include <grpcpp/ext/proto_server_reflection_plugin.h>
#include <grpcpp/health_check_service_interface.h>

#include <iostream>

#include "server.h"
#include "smoldb.h"

// A dummy procedure for testing the server directly.
class EchoProcedure : public smoldb::TransactionProcedure
{
 public:
  std::string get_name() const override { return "echo"; }

  smoldb::ProcedureStatus execute(smoldb::TransactionContext& ctx,
                                  const smoldb::ProcedureParams& params,
                                  smoldb::ProcedureResult& result) override
  {
    result = params;  // Echo back all input parameters
    return smoldb::ProcedureStatus::SUCCESS;
  }
};

void RunServer()
{
  std::string server_address("0.0.0.0:50051");

  smoldb::DBConfig db_config;
  smoldb::SmolDB db_engine(db_config);
  db_engine.startup();
  db_engine.get_procedure_manager()->register_procedure(
      std::make_unique<EchoProcedure>());

#if SMOLDB_USE_CALLBACK_API
  GrpcCallbackService service(db_engine.get_procedure_manager());
#else
#error "No gRPC service implementation selected."
#endif

  grpc::EnableDefaultHealthCheckService(true);
  grpc::reflection::InitProtoReflectionServerBuilderPlugin();

  grpc::ServerBuilder builder;
  builder.AddListeningPort(server_address, grpc::InsecureServerCredentials());

  builder.RegisterService(&service);

  std::unique_ptr<grpc::Server> server(builder.BuildAndStart());
  std::cout << "Server listening on " << server_address << std::endl;

  server->Wait();
  db_engine.shutdown();
}

int main(int argc, char** argv)
{
  RunServer();
  return 0;
}