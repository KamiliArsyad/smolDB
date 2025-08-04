#include <boost/asio/executor_work_guard.hpp>
#include <boost/asio/io_context.hpp>
#include <iostream>
#include <thread>
#include <vector>

#include "smoldb.h"
#include "server.h"

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
  smoldb::SmolDB db_engine(db_config, executor);
  db_engine.startup();
  db_engine.get_procedure_manager()->register_procedure(
      std::make_unique<EchoProcedure>());

  GrpcServer server(db_engine.get_procedure_manager(), executor);
  server.run(db_config.get_full_listen_address());

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

  // Populate the thread pool.
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