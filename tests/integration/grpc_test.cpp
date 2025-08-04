#include <grpcpp/grpcpp.h>
#include <gtest/gtest.h>

#include <boost/asio/co_spawn.hpp>
#include <boost/asio/detached.hpp>
#include <boost/asio/executor_work_guard.hpp>
#include <boost/asio/io_context.hpp>
#include <future>
#include <thread>

#include "backend/proc/proc_ctx.h"
#include "backend/smoldb.h"
#include "server/server.h"
#include "smoldb.grpc.pb.h"

using namespace smoldb;
namespace asio = boost::asio;

struct RpcResult
{
  grpc::Status status;
  smoldb::rpc::ExecuteProcedureResponse response;
};

class TransferPointsTestProc : public TransactionProcedure
{
 public:
  std::string get_name() const override { return "transfer_points"; }
  asio::awaitable<ProcedureStatus> execute(TransactionContext& ctx,
                                           const ProcedureParams& params,
                                           ProcedureResult& result) override
  {
    const auto from_id = boost::get<int32_t>(params.at("from_user"));
    const auto to_id = boost::get<int32_t>(params.at("to_user"));
    const auto amount = boost::get<int32_t>(params.at("amount"));
    if (amount <= 0) co_return ProcedureStatus::ABORT;

    auto* table = ctx.get_table("user_points");
    Schema schema = table->get_schema();
    RID from_rid, to_rid;
    Row from_row(schema), to_row(schema);
    if (!table->get_rid_from_index(ctx.get_txn_id(), from_id, from_rid) ||
        !table->get_rid_from_index(ctx.get_txn_id(), to_id, to_rid))
    {
      co_return ProcedureStatus::ABORT;
    }
    table->get_row(ctx.get_txn_id(), from_rid, from_row);
    table->get_row(ctx.get_txn_id(), to_rid, to_row);
    int32_t from_balance = boost::get<int32_t>(from_row.get_value("points"));
    if (from_balance < amount)
    {
      result["reason"] = "Insufficient points";
      co_return ProcedureStatus::ABORT;
    }
    int32_t to_balance = boost::get<int32_t>(to_row.get_value("points"));
    from_row.set_value("points", from_balance - amount);
    to_row.set_value("points", to_balance + amount);
    table->update_row(ctx.get_txn_id(), from_rid, from_row);
    table->update_row(ctx.get_txn_id(), to_rid, to_row);
    result["new_from_balance"] = from_balance - amount;
    result["new_to_balance"] = to_balance + amount;
    co_return ProcedureStatus::SUCCESS;
  }
};

// New procedure for verifying state in tests
class CheckBalanceProcedure : public TransactionProcedure
{
 public:
  std::string get_name() const override { return "check_balance"; }
  asio::awaitable<ProcedureStatus> execute(TransactionContext& ctx,
                                           const ProcedureParams& params,
                                           ProcedureResult& result) override
  {
    const auto user_id = boost::get<int32_t>(params.at("user_id"));
    auto* table = ctx.get_table("user_points");
    RID rid;
    Row row(table->get_schema());
    if (!table->get_rid_from_index(ctx.get_txn_id(), user_id, rid) ||
        !table->get_row(ctx.get_txn_id(), rid, row))
    {
      result["reason"] = "User not found";
      co_return ProcedureStatus::ABORT;
    }
    result["points"] = row.get_value("points");
    co_return ProcedureStatus::SUCCESS;
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

    DBConfig db_config{test_dir_};
    server_address_ = db_config.get_full_listen_address();

    db_ = std::make_unique<SmolDB>(db_config, io_context_.get_executor());
    db_->startup();
    db_->get_procedure_manager()->register_procedure(
        std::make_unique<TransferPointsTestProc>());
    db_->get_procedure_manager()->register_procedure(
        std::make_unique<CheckBalanceProcedure>());  // Register new procedure

    Schema schema = {{0, "user_id", Col_type::INT, false, {}},
                     {1, "points", Col_type::INT, false, {}}};
    db_->create_table(1, "user_points", schema);
    db_->create_index(1, 0, "pk_user_points");

    asio::co_spawn(
        io_context_,
        [&]() -> asio::awaitable<void>
        {
          auto txn_id = db_->begin_transaction();
          auto* table = db_->get_table("user_points");
          Row r1(schema), r2(schema), r3(schema);
          r1.set_value("user_id", 101);
          r1.set_value("points", 100);
          r2.set_value("user_id", 202);
          r2.set_value("points", 50);
          r3.set_value("user_id", 303);
          r3.set_value("points", 500);
          table->insert_row(txn_id, r1);
          table->insert_row(txn_id, r2);
          table->insert_row(txn_id, r3);
          co_await db_->async_commit_transaction(txn_id);
        },
        asio::detached);
    io_context_.run();
    io_context_.restart();

    server_obj_ = std::make_unique<GrpcServer>(db_->get_procedure_manager(),
                                               io_context_.get_executor());

    work_guard_ = std::make_unique<
        asio::executor_work_guard<asio::io_context::executor_type>>(
        io_context_.get_executor());
    executor_thread_ = std::thread([this]() { io_context_.run(); });

    server_thread_ =
        std::thread([this]() { server_obj_->run(server_address_); });

    auto channel = grpc::CreateChannel(server_address_,
                                       grpc::InsecureChannelCredentials());
    stub_ = rpc::SmolDBService::NewStub(channel);
  }

  void TearDown() override
  {
    server_obj_->stop();

    if (server_thread_.joinable())
    {
      server_thread_.join();
    }

    work_guard_.reset();
    io_context_.stop();
    if (executor_thread_.joinable())
    {
      executor_thread_.join();
    }

    db_->shutdown();
  }

  std::filesystem::path test_dir_;
  std::string server_address_;
  std::unique_ptr<SmolDB> db_;
  std::unique_ptr<GrpcServer> server_obj_;
  std::thread server_thread_;
  std::unique_ptr<rpc::SmolDBService::Stub> stub_;

  asio::io_context io_context_;
  std::unique_ptr<asio::executor_work_guard<asio::io_context::executor_type>>
      work_guard_;
  std::thread executor_thread_;
};

TEST_F(GrpcTest, SuccessfulProcedureCall)
{
  rpc::ExecuteProcedureRequest request;
  request.set_procedure_name("transfer_points");
  (*request.mutable_params())["from_user"].set_int_value(101);
  (*request.mutable_params())["to_user"].set_int_value(202);
  (*request.mutable_params())["amount"].set_int_value(10);

  rpc::ExecuteProcedureResponse response;
  grpc::ClientContext context;
  grpc::Status status = stub_->ExecuteProcedure(&context, request, &response);

  ASSERT_TRUE(status.ok());
  ASSERT_EQ(response.status(), rpc::ExecuteProcedureResponse::SUCCESS);
  ASSERT_EQ(response.results().size(), 2);
  EXPECT_EQ(response.results().at("new_from_balance").int_value(), 90);
  EXPECT_EQ(response.results().at("new_to_balance").int_value(), 60);
}

TEST_F(GrpcTest, AbortedProcedureCall)
{
  rpc::ExecuteProcedureRequest request;
  request.set_procedure_name("transfer_points");
  (*request.mutable_params())["from_user"].set_int_value(101);
  (*request.mutable_params())["to_user"].set_int_value(202);
  (*request.mutable_params())["amount"].set_int_value(200);  // Insufficient

  rpc::ExecuteProcedureResponse response;
  grpc::ClientContext context;
  grpc::Status status = stub_->ExecuteProcedure(&context, request, &response);

  ASSERT_TRUE(status.ok());
  ASSERT_EQ(response.status(), rpc::ExecuteProcedureResponse::ABORT);
  ASSERT_EQ(response.results().size(), 1);
  EXPECT_EQ(response.results().at("reason").string_value(),
            "Insufficient points");
}

TEST_F(GrpcTest, InvalidProcedureName)
{
  rpc::ExecuteProcedureRequest request;
  request.set_procedure_name("non_existent_procedure");

  rpc::ExecuteProcedureResponse response;
  grpc::ClientContext context;
  grpc::Status status = stub_->ExecuteProcedure(&context, request, &response);

  ASSERT_FALSE(status.ok());
  EXPECT_EQ(status.error_code(), grpc::StatusCode::INTERNAL);
  EXPECT_NE(status.error_message().find("not found"), std::string::npos);
}

TEST_F(GrpcTest, MissingParameter)
{
  rpc::ExecuteProcedureRequest request;
  request.set_procedure_name("transfer_points");
  (*request.mutable_params())["from_user"].set_int_value(101);
  // "amount" and "to_user" are missing

  rpc::ExecuteProcedureResponse response;
  grpc::ClientContext context;
  grpc::Status status = stub_->ExecuteProcedure(&context, request, &response);

  ASSERT_FALSE(status.ok());
  EXPECT_EQ(status.error_code(), grpc::StatusCode::INTERNAL);
}

TEST_F(GrpcTest, ConcurrentConflictingUpdatesAreAtomic)
{
  auto make_call = [this](int from, int to, int amount) -> RpcResult
  {
    rpc::ExecuteProcedureRequest request;
    request.set_procedure_name("transfer_points");
    (*request.mutable_params())["from_user"].set_int_value(from);
    (*request.mutable_params())["to_user"].set_int_value(to);
    (*request.mutable_params())["amount"].set_int_value(amount);

    RpcResult result;
    grpc::ClientContext context;
    context.set_deadline(std::chrono::system_clock::now() +
                         std::chrono::seconds(2));
    result.status =
        stub_->ExecuteProcedure(&context, request, &result.response);
    return result;
  };

  // User 101 has 100 points. Two clients try to withdraw 70 points each.
  // One must succeed, one must fail due to insufficient funds (a logical
  // abort). The final balance must be 30.
  auto future1 = std::async(std::launch::async, make_call, 101, 202, 70);
  auto future2 = std::async(std::launch::async, make_call, 101, 303, 70);

  RpcResult r1 = future1.get();
  RpcResult r2 = future2.get();

  // We expect both RPCs to return grpc::Status::OK, but one payload
  // will indicate SUCCESS and the other ABORT.
  ASSERT_TRUE(r1.status.ok());
  ASSERT_TRUE(r2.status.ok());

  bool one_succeeded =
      r1.response.status() == rpc::ExecuteProcedureResponse::SUCCESS;
  bool two_succeeded =
      r2.response.status() == rpc::ExecuteProcedureResponse::SUCCESS;

  ASSERT_TRUE(one_succeeded ^ two_succeeded)
      << "Expected exactly one transaction to succeed and one to logically "
         "abort.";

  // Verify the final balance.
  rpc::ExecuteProcedureRequest check_req;
  check_req.set_procedure_name("check_balance");
  (*check_req.mutable_params())["user_id"].set_int_value(101);

  RpcResult check_result;
  grpc::ClientContext context;
  check_result.status =
      stub_->ExecuteProcedure(&context, check_req, &check_result.response);

  ASSERT_TRUE(check_result.status.ok());
  ASSERT_EQ(check_result.response.status(),
            rpc::ExecuteProcedureResponse::SUCCESS);
  EXPECT_EQ(check_result.response.results().at("points").int_value(), 30)
      << "Final balance should be 100 - 70 = 30.";
}