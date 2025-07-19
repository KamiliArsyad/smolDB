#include "server.h"

#include <google/protobuf/util/time_util.h>

#include "lock_excepts.h"

#if SMOLDB_USE_CALLBACK_API

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
      return smoldb::datetime(std::chrono::milliseconds(ms));
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
  else if (const auto* val = boost::get<smoldb::datetime>(&backend_val))
  {
    auto ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                  val->time_since_epoch())
                  .count();
    *proto_val->mutable_timestamp_value() =
        google::protobuf::util::TimeUtil::MillisecondsToTimestamp(ms);
  }
}

}  // anonymous namespace

GrpcCallbackService::GrpcCallbackService(smoldb::ProcedureManager* proc_mgr)
    : proc_mgr_(proc_mgr)
{
  assert(proc_mgr_ != nullptr);
}

grpc::ServerUnaryReactor* GrpcCallbackService::ExecuteProcedure(
    grpc::CallbackServerContext* context,
    const smoldb::rpc::ExecuteProcedureRequest* request,
    smoldb::rpc::ExecuteProcedureResponse* response)
{
  auto* reactor = context->DefaultReactor();
  try
  {
    // 1. Marshall request from Protobuf to C++ types
    smoldb::ProcedureParams params;
    for (const auto& [key, val] : request->params())
    {
      params[key] = to_backend_value(val);
    }

    // 2. Execute the procedure
    smoldb::ProcedureResult result =
        proc_mgr_->execute_procedure(request->procedure_name(), params);

    // 3. Marshall response from C++ to Protobuf types
    response->set_status(smoldb::rpc::ExecuteProcedureResponse::SUCCESS);
    for (const auto& [key, val] : result)
    {
      from_backend_value(val, &(*response->mutable_results())[key]);
    }
    reactor->Finish(grpc::Status::OK);
  }
  catch (const smoldb::TransactionAbortedException& e)
  {
    response->set_status(smoldb::rpc::ExecuteProcedureResponse::ABORT);
    (*response->mutable_results())["reason"].set_string_value(e.what());
    reactor->Finish(grpc::Status::OK);
  }
  catch (const std::exception& e)
  {
    reactor->Finish(grpc::Status(grpc::StatusCode::INTERNAL, e.what()));
  }
  return reactor;
}
#endif  // SMOLDB_USE_CALLBACK_API