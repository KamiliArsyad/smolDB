#include <grpcpp/ext/proto_server_reflection_plugin.h>
#include <grpcpp/health_check_service_interface.h>

#include <boost/asio/executor_work_guard.hpp>
#include <boost/asio/io_context.hpp>
#include <iostream>
#include <thread>
#include <vector>

#include "server.h"
#include "smoldb.h"

namespace asio = boost::asio;

// A dummy procedure for testing the server directly.
class EchoProcedure : public smoldb::TransactionProcedure
{
 public:
  std::string get_name() const override { return "echo"; }

  asio::awaitable<smoldb::ProcedureStatus> execute(
      smoldb::TransactionContext& ctx, const smoldb::ProcedureParams& params,
      smoldb::ProcedureResult& result) override
  {
    result = params;  // Echo back all input parameters
    co_return smoldb::ProcedureStatus::SUCCESS;
  }
};

void RunServer(smoldb::DBConfig& db_config, asio::any_io_executor executor)
{
  std::string server_address(db_config.listen_address);
  smoldb::SmolDB db_engine(db_config, executor);
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

  std::unique_ptr server(builder.BuildAndStart());
  std::cout << "Server listening on " << server_address << std::endl;

  server->Wait();
  db_engine.shutdown();
}

int main(int argc, char** argv)
{
  smoldb::DBConfig config;

  // Create the application-wide Asio context.
  asio::io_context io_context;

  // Create a thread pool to run the io_context.
  const int num_threads = std::thread::hardware_concurrency();
  std::vector<std::thread> thread_pool;
  thread_pool.reserve(num_threads);

  // Use a work_guard to keep the io_context running even when there's no
  // work.
  auto work_guard = asio::make_work_guard(io_context.get_executor());

  // 4. Populate the thread pool.
  for (int i = 0; i < num_threads; ++i)
  {
    thread_pool.emplace_back([&io_context]() { io_context.run(); });
  }

  // Run the server, passing the executor to it.
  RunServer(config, io_context.get_executor());

  // Clean shutdown.
  work_guard.reset();  // Allow the io_context to stop.
  io_context.stop();
  for (auto& t : thread_pool)
  {
    t.join();
  }

  return 0;
}