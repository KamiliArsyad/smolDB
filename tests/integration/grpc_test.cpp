#include <grpcpp/grpcpp.h>
#include <gtest/gtest.h>

#include <thread>

#include "backend/smoldb.h"
#include "proc_ctx.h"
#include "server/server.h"
#include "smoldb.grpc.pb.h"

class TransferPointsTestProc : public smoldb::TransactionProcedure
{
 public:
  std::string get_name() const override { return "transfer_points"; }
  smoldb::ProcedureStatus execute(smoldb::TransactionContext& ctx,
                                  const smoldb::ProcedureParams& params,
                                  smoldb::ProcedureResult& result) override
  {
    const auto from_id = boost::get<int32_t>(params.at("from_user"));
    const auto to_id = boost::get<int32_t>(params.at("to_user"));
    const auto amount = boost::get<int32_t>(params.at("amount"));
    if (amount <= 0) return smoldb::ProcedureStatus::ABORT;

    auto* table = ctx.get_table("user_points");
    smoldb::Schema schema = table->get_schema();
    smoldb::RID from_rid, to_rid;
    smoldb::Row from_row(schema), to_row(schema);
    if (!table->get_rid_from_index(ctx.get_txn_id(), from_id, from_rid) ||
        !table->get_rid_from_index(ctx.get_txn_id(), to_id, to_rid))
    {
      return smoldb::ProcedureStatus::ABORT;
    }
    table->get_row(ctx.get_txn_id(), from_rid, from_row);
    table->get_row(ctx.get_txn_id(), to_rid, to_row);
    int32_t from_balance = boost::get<int32_t>(from_row.get_value("points"));
    if (from_balance < amount)
    {
      result["reason"] = "Insufficient points";
      return smoldb::ProcedureStatus::ABORT;
    }
    int32_t to_balance = boost::get<int32_t>(to_row.get_value("points"));
    from_row.set_value("points", from_balance - amount);
    to_row.set_value("points", to_balance + amount);
    table->update_row(ctx.get_txn_id(), from_rid, from_row);
    table->update_row(ctx.get_txn_id(), to_rid, to_row);
    result["new_from_balance"] = from_balance - amount;
    result["new_to_balance"] = to_balance + amount;
    return smoldb::ProcedureStatus::SUCCESS;
  }
};

class GrpcTest : public ::testing::Test
{
 protected:
  void SetUp() override
  {
    test_dir_ = std::filesystem::temp_directory_path() / "grpc_test";
    std::filesystem::remove_all(test_dir_);
    std::filesystem::create_directories(test_dir_);

    smoldb::DBConfig db_config{test_dir_};
    db_ = std::make_unique<smoldb::SmolDB>(db_config);
    db_->startup();
    db_->get_procedure_manager()->register_procedure(
        std::make_unique<TransferPointsTestProc>());

    smoldb::Schema schema = {{0, "user_id", smoldb::Col_type::INT, false, {}},
                             {1, "points", smoldb::Col_type::INT, false, {}}};
    db_->create_table(1, "user_points", schema);
    db_->create_index(1, 0, "pk_user_points");
    auto txn_id = db_->begin_transaction();
    auto* table = db_->get_table("user_points");
    smoldb::Row r1(schema), r2(schema);
    r1.set_value("user_id", 101);
    r1.set_value("points", 100);
    r2.set_value("user_id", 202);
    r2.set_value("points", 50);
    table->insert_row(txn_id, r1);
    table->insert_row(txn_id, r2);
    db_->commit_transaction(txn_id);

    server_address_ = db_config.listen_address;
    service_ =
        std::make_unique<GrpcCallbackService>(db_->get_procedure_manager());

    grpc::ServerBuilder builder;
    builder.AddListeningPort(server_address_,
                             grpc::InsecureServerCredentials());
    builder.RegisterService(service_.get());
    server_ = builder.BuildAndStart();
    server_thread_ = std::thread([this]() { server_->Wait(); });

    auto channel = grpc::CreateChannel(server_address_,
                                       grpc::InsecureChannelCredentials());
    stub_ = smoldb::rpc::SmolDBService::NewStub(channel);
  }

  void TearDown() override
  {
    server_->Shutdown();
    server_thread_.join();
    db_->shutdown();
  }

  std::filesystem::path test_dir_;
  std::string server_address_;
  std::unique_ptr<smoldb::SmolDB> db_;
  std::unique_ptr<GrpcCallbackService> service_;
  std::unique_ptr<grpc::Server> server_;
  std::thread server_thread_;
  std::unique_ptr<smoldb::rpc::SmolDBService::Stub> stub_;
};

TEST_F(GrpcTest, SuccessfulProcedureCall)
{
  smoldb::rpc::ExecuteProcedureRequest request;
  request.set_procedure_name("transfer_points");
  (*request.mutable_params())["from_user"].set_int_value(101);
  (*request.mutable_params())["to_user"].set_int_value(202);
  (*request.mutable_params())["amount"].set_int_value(10);

  smoldb::rpc::ExecuteProcedureResponse response;
  grpc::ClientContext context;
  grpc::Status status = stub_->ExecuteProcedure(&context, request, &response);

  ASSERT_TRUE(status.ok());
  ASSERT_EQ(response.status(), smoldb::rpc::ExecuteProcedureResponse::SUCCESS);
  ASSERT_EQ(response.results().size(), 2);
  EXPECT_EQ(response.results().at("new_from_balance").int_value(), 90);
  EXPECT_EQ(response.results().at("new_to_balance").int_value(), 60);
}

TEST_F(GrpcTest, AbortedProcedureCall)
{
  smoldb::rpc::ExecuteProcedureRequest request;
  request.set_procedure_name("transfer_points");
  (*request.mutable_params())["from_user"].set_int_value(101);
  (*request.mutable_params())["to_user"].set_int_value(202);
  (*request.mutable_params())["amount"].set_int_value(200);  // Insufficient

  smoldb::rpc::ExecuteProcedureResponse response;
  grpc::ClientContext context;
  grpc::Status status = stub_->ExecuteProcedure(&context, request, &response);

  ASSERT_TRUE(status.ok());
  ASSERT_EQ(response.status(), smoldb::rpc::ExecuteProcedureResponse::ABORT);
  ASSERT_EQ(response.results().size(), 1);
  EXPECT_EQ(response.results().at("reason").string_value(),
            "Insufficient points");
}