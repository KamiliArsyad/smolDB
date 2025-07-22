#include <gtest/gtest.h>

#include <boost/asio.hpp>
#include <boost/asio/co_spawn.hpp>
#include <boost/asio/detached.hpp>
#include <boost/asio/executor_work_guard.hpp>
#include <boost/asio/use_awaitable.hpp>
#include <boost/system/error_code.hpp>

#include "storage/wal_mgr.h"

using namespace smoldb;
namespace asio = boost::asio;

class WAL_AsyncTest : public ::testing::Test
{
 protected:
  void SetUp() override
  {
    test_dir = std::filesystem::temp_directory_path() / "wal_async_test";
    std::filesystem::remove_all(test_dir);
    std::filesystem::create_directories(test_dir);
    wal_path = test_dir / "test.wal";

    executor = io_context.get_executor();
    wal_mgr = std::make_unique<WAL_mgr>(wal_path, executor);

    // **THE FIX**: Create a work guard to keep the io_context running.
    work_guard = std::make_unique<
        asio::executor_work_guard<asio::io_context::executor_type>>(
        io_context.get_executor());

    // The io_context will now block in run() until stop() is called.
    runner_thread = std::thread([this] { io_context.run(); });
  }

  void TearDown() override
  {
    wal_mgr.reset();  // Stop the WAL writer thread first.

    // **THE FIX**: Reset the guard to allow the io_context to stop, then stop
    // it.
    work_guard.reset();
    io_context.stop();

    if (runner_thread.joinable())
    {
      runner_thread.join();
    }
    std::filesystem::remove_all(test_dir);
  }

  std::filesystem::path test_dir;
  std::filesystem::path wal_path;
  asio::io_context io_context;
  asio::any_io_executor executor;
  std::unique_ptr<asio::executor_work_guard<asio::io_context::executor_type>>
      work_guard;
  std::unique_ptr<WAL_mgr> wal_mgr;
  std::thread runner_thread;
};

TEST_F(WAL_AsyncTest, CoroutineWaitsAndResumes)
{
  std::promise<bool> test_completed_promise;
  auto test_completed_future = test_completed_promise.get_future();

  asio::co_spawn(
      executor,
      [&]() -> asio::awaitable<void>
      {
        LogRecordHeader hdr{};
        hdr.type = BEGIN;
        hdr.txn_id = 1;
        hdr.lr_length = sizeof(LogRecordHeader);

        LSN lsn = wal_mgr->append_record_async(hdr);
        EXPECT_FALSE(wal_mgr->is_lsn_flushed(lsn));

        boost::system::error_code ec;
        co_await wal_mgr->async_wait_for_flush(
            lsn, asio::redirect_error(asio::use_awaitable, ec));

        EXPECT_FALSE(ec);
        EXPECT_TRUE(wal_mgr->is_lsn_flushed(lsn));
        test_completed_promise.set_value(true);
        co_return;
      },
      asio::detached);

  ASSERT_EQ(test_completed_future.wait_for(std::chrono::seconds(2)),
            std::future_status::ready);
  EXPECT_TRUE(test_completed_future.get());
}

TEST_F(WAL_AsyncTest, AwaiterDoesNotBlockIfLSNAlreadyFlushed)
{
  std::promise<bool> test_completed_promise;
  auto test_completed_future = test_completed_promise.get_future();

  LogRecordHeader hdr{};
  hdr.type = BEGIN;
  hdr.txn_id = 1;
  hdr.lr_length = sizeof(LogRecordHeader);
  LSN lsn = wal_mgr->append_record(hdr);

  ASSERT_TRUE(wal_mgr->is_lsn_flushed(lsn));

  asio::co_spawn(
      executor,
      [&]() -> asio::awaitable<void>
      {
        boost::system::error_code ec;
        co_await wal_mgr->async_wait_for_flush(
            lsn, asio::redirect_error(asio::use_awaitable, ec));
        EXPECT_FALSE(ec);
        test_completed_promise.set_value(true);
        co_return;
      },
      asio::detached);

  ASSERT_EQ(test_completed_future.wait_for(std::chrono::seconds(2)),
            std::future_status::ready);
  EXPECT_TRUE(test_completed_future.get());
}

TEST_F(WAL_AsyncTest, MultipleCoroutinesWaitOnSameBatch)
{
  std::promise<void> promise1, promise2, promise3;
  auto future1 = promise1.get_future();
  auto future2 = promise2.get_future();
  auto future3 = promise3.get_future();

  LogRecordHeader hdr{};
  hdr.type = BEGIN;
  hdr.lr_length = sizeof(LogRecordHeader);

  hdr.txn_id = 1;
  LSN lsn1 = wal_mgr->append_record_async(hdr);
  hdr.txn_id = 2;
  LSN lsn2 = wal_mgr->append_record_async(hdr);

  asio::co_spawn(
      executor,
      [&]() -> asio::awaitable<void>
      {
        co_await wal_mgr->async_wait_for_flush(lsn2, asio::use_awaitable);
        EXPECT_TRUE(wal_mgr->is_lsn_flushed(lsn1));
        EXPECT_TRUE(wal_mgr->is_lsn_flushed(lsn2));
        promise1.set_value();
        co_return;
      },
      asio::detached);

  asio::co_spawn(
      executor,
      [&]() -> asio::awaitable<void>
      {
        co_await wal_mgr->async_wait_for_flush(lsn2, asio::use_awaitable);
        EXPECT_TRUE(wal_mgr->is_lsn_flushed(lsn2));
        promise2.set_value();
        co_return;
      },
      asio::detached);

  asio::co_spawn(
      executor,
      [&]() -> asio::awaitable<void>
      {
        co_await wal_mgr->async_wait_for_flush(lsn1, asio::use_awaitable);
        EXPECT_TRUE(wal_mgr->is_lsn_flushed(lsn1));
        promise3.set_value();
        co_return;
      },
      asio::detached);

  ASSERT_EQ(future1.wait_for(std::chrono::seconds(2)),
            std::future_status::ready);
  ASSERT_EQ(future2.wait_for(std::chrono::seconds(2)),
            std::future_status::ready);
  ASSERT_EQ(future3.wait_for(std::chrono::seconds(2)),
            std::future_status::ready);
}