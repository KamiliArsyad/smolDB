#include "server.h"

#include <google/protobuf/util/time_util.h>

#include <boost/lexical_cast.hpp>

#include "lock_excepts.h"
#include "proc/proc_mgr.h"

namespace smoldb
{
class TransactionAbortedException;
}
namespace
{
// --- Marshalling Utilities ---
// Convert from Protobuf `smoldb::rpc::Value` to the backend `smoldb::Value`
smoldb::Value to_backend_value(const smoldb::rpc::Value& proto_val)
{
  switch (proto_val.value_oneof_case())
  {
    case smoldb::rpc::Value::kIntValue:
      return proto_val.int_value();
    case smoldb::rpc::Value::kFloatValue:
      return proto_val.float_value();
    case smoldb::rpc::Value::kStringValue:
      return proto_val.string_value();
    case smoldb::rpc::Value::kTimestampValue:
    {
      auto ms = google::protobuf::util::TimeUtil::TimestampToMilliseconds(
          proto_val.timestamp_value());
      return smoldb::DATETIME_TYPE(std::chrono::milliseconds(ms));
    }
    default:
      throw std::invalid_argument("Unknown protobuf value type");
  }
}

// Convert from our backend `smoldb::Value` to Protobuf `smoldb::rpc::Value`
void from_backend_value(const smoldb::Value& backend_val,
                        smoldb::rpc::Value* proto_val)
{
  if (const auto* val = boost::get<int32_t>(&backend_val))
  {
    proto_val->set_int_value(*val);
  }
  else if (const auto* val = boost::get<float>(&backend_val))
  {
    proto_val->set_float_value(*val);
  }
  else if (const auto* val = boost::get<std::string>(&backend_val))
  {
    proto_val->set_string_value(*val);
  }
  else if (const auto* val = boost::get<smoldb::DATETIME_TYPE>(&backend_val))
  {
    auto ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                  val->time_since_epoch())
                  .count();
    *proto_val->mutable_timestamp_value() =
        google::protobuf::util::TimeUtil::MillisecondsToTimestamp(ms);
  }
}

smoldb::ProcedureParams to_backend_params(
    const google::protobuf::Map<std::string, smoldb::rpc::Value>& proto_params)
{
  smoldb::ProcedureParams backend_params;
  for (const auto& [key, val] : proto_params)
  {
    backend_params[key] = to_backend_value(val);
  }
  return backend_params;
}

void from_backend_result(
    const smoldb::ProcedureResult& backend_result,
    google::protobuf::Map<std::string, smoldb::rpc::Value>* proto_results)
{
  for (const auto& [key, val] : backend_result)
  {
    from_backend_value(val, &(*proto_results)[key]);
  }
}

}  // anonymous namespace

GrpcServer::GrpcServer(smoldb::ProcedureManager* proc_mgr,
                       boost::asio::any_io_executor executor)
    : proc_mgr_(proc_mgr), executor_(std::move(executor))
{
  assert(proc_mgr_ != nullptr);
}

boost::asio::awaitable<void> GrpcServer::handle_execute_procedure(
    agrpc::ServerRPC<
        &smoldb::rpc::SmolDBService::AsyncService::RequestExecuteProcedure>&
        rpc,
    smoldb::rpc::ExecuteProcedureRequest& request)
{
  smoldb::rpc::ExecuteProcedureResponse response;
  grpc::Status status = grpc::Status::OK;

  try
  {
    smoldb::ProcedureParams params = to_backend_params(request.params());

    auto [proc_status, result] = co_await proc_mgr_->async_execute_procedure(
        request.procedure_name(), params);

    if (proc_status == smoldb::ProcedureStatus::ABORT)
    {
      response.set_status(smoldb::rpc::ExecuteProcedureResponse::ABORT);
    }
    else
    {
      response.set_status(smoldb::rpc::ExecuteProcedureResponse::SUCCESS);
    }
    from_backend_result(result, response.mutable_results());
  }
  catch (const smoldb::TransactionAbortedException& e)
  {
    // Handle retryable backend errors (deadlocks, timeouts)
    status = grpc::Status(grpc::StatusCode::ABORTED, e.what());
  }
  catch (const std::exception& e)
  {
    // Handle unexpected errors
    status = grpc::Status(grpc::StatusCode::INTERNAL, e.what());
  }

  co_await rpc.finish(response, status, boost::asio::use_awaitable);
}

void GrpcServer::run(const std::string& server_address)
{
  grpc::ServerBuilder builder;
  builder.AddListeningPort(server_address, grpc::InsecureServerCredentials());

  smoldb::rpc::SmolDBService::AsyncService service;
  builder.RegisterService(&service);

  // The GrpcContext must be created before the server is built.
  grpc_context_ =
      std::make_unique<agrpc::GrpcContext>(builder.AddCompletionQueue());

  server_ = builder.BuildAndStart();  // Assign to member
  if (!server_)
  {
    throw std::runtime_error("Failed to start gRPC server");
  }

  agrpc::register_awaitable_rpc_handler<agrpc::ServerRPC<
      &smoldb::rpc::SmolDBService::AsyncService::RequestExecuteProcedure>>(
      *grpc_context_, service,
      [this](auto& rpc, auto& request) -> boost::asio::awaitable<void>
      { return this->handle_execute_procedure(rpc, request); },
      agrpc::detail::RethrowFirstArg{});

  std::cout << "Server listening on " << server_address << std::endl;
  grpc_context_->run();  // This will block until stop() is called.
}

void GrpcServer::stop()
{
  if (server_)
  {
    server_->Shutdown();
  }
  if (grpc_context_)
  {
    grpc_context_->stop();
  }
}