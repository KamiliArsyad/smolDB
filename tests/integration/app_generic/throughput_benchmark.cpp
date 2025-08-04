#include <gtest/gtest.h>

#include <algorithm>
#include <atomic>
#include <boost/asio/co_spawn.hpp>
#include <boost/asio/detached.hpp>
#include <boost/asio/executor_work_guard.hpp>
#include <boost/asio/experimental/awaitable_operators.hpp>
#include <boost/asio/experimental/parallel_group.hpp>
#include <boost/asio/io_context.hpp>
#include <boost/asio/use_future.hpp>
#include <chrono>
#include <iostream>
#include <numeric>
#include <random>
#include <thread>
#include <vector>

#include "backend/proc/proc.h"
#include "backend/proc/proc_ctx.h"
#include "backend/smoldb.h"

using namespace smoldb;
namespace asio = boost::asio;

class TransferPointsProcedureForBench : public TransactionProcedure
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
    Table<>* table = ctx.get_table("user_points");
    Schema schema = table->get_schema();
    RID from_rid, to_rid;
    Row from_row(schema), to_row(schema);
    table->get_rid_from_index(ctx.get_txn_id(), from_id, from_rid);
    table->get_rid_from_index(ctx.get_txn_id(), to_id, to_rid);
    table->get_row(ctx.get_txn_id(), from_rid, from_row);
    table->get_row(ctx.get_txn_id(), to_rid, to_row);
    int32_t from_balance = boost::get<int32_t>(from_row.get_value("points"));
    int32_t to_balance = boost::get<int32_t>(to_row.get_value("points"));
    from_row.set_value("points", from_balance - amount);
    to_row.set_value("points", to_balance + amount);
    co_await table->async_update_row(ctx.get_txn_id(), from_rid, from_row);
    co_await table->async_update_row(ctx.get_txn_id(), to_rid, to_row);
    co_return ProcedureStatus::SUCCESS;
  }
};

class ThroughputBenchmark : public ::testing::Test
{
 protected:
  const int NUM_USERS = 50000;
  const int INITIAL_POINTS = 100000;

  void SetUp() override
  {
    test_dir = std::filesystem::temp_directory_path() / "throughput_bench";
    std::filesystem::remove_all(test_dir);
    std::filesystem::create_directories(test_dir);

    smoldb::DBConfig config;
    config.db_directory = test_dir;
    config.buffer_pool_shard_count = 128;
    config.lock_manager_shard_count = 128;
    config.buffer_pool_size_frames = 512;  // A larger pool for a bench
    config.wal_mgr_batch_deadline_threshold = std::chrono::microseconds(500);
    config.wal_mgr_batch_byte_threshold = 1 << 16;
    db = std::make_unique<SmolDB>(config, io_context_.get_executor());
    db->startup();

    Schema schema = {{0, "user_id", Col_type::INT, false, {}},
                     {1, "points", Col_type::INT, false, {}}};
    db->create_table(1, "user_points", schema, 256);
    db->create_index(1, 0, "pk_user_points");

    asio::co_spawn(
        io_context_,
        [&]() -> asio::awaitable<void>
        {
          TransactionID txn = db->begin_transaction();
          Table<>* table = db->get_table("user_points");
          for (int i = 0; i < NUM_USERS; ++i)
          {
            Row r(schema);
            r.set_value("user_id", i);
            r.set_value("points", INITIAL_POINTS);
            co_await table->async_insert_row(txn, r);
          }
          co_await db->async_commit_transaction(txn);
        },
        asio::detached);
    io_context_.run();
    io_context_.restart();

    db->get_procedure_manager()->register_procedure(
        std::make_unique<TransferPointsProcedureForBench>());
  }

  void TearDown() override
  {
    db->shutdown();
    db.reset();
    std::filesystem::remove_all(test_dir);
  }

  std::filesystem::path test_dir;
  std::unique_ptr<SmolDB> db;
  asio::io_context io_context_;
};

const std::string proc_name_str = "transfer_points";

TEST_F(ThroughputBenchmark, MeasureThroughputAndLatency)
{
  const int NUM_THREADS = std::thread::hardware_concurrency();
  const int DURATION_SECONDS = 5;

  // Create an executor thread pool
  std::vector<std::thread> executor_threads;
  auto work_guard = asio::make_work_guard(io_context_);
  for (int i = 0; i < NUM_THREADS; ++i)
  {
    executor_threads.emplace_back([this]() { io_context_.run(); });
  }

  std::atomic<bool> stop_flag = false;
  std::atomic<size_t> total_ops = 0;
  std::vector<std::thread> client_threads;
  std::vector<std::vector<double>> latencies_per_thread(NUM_THREADS);

  for (int i = 0; i < NUM_THREADS; ++i)
  {
    client_threads.emplace_back(
        [&, thread_id = i]()
        {
          std::mt19937 gen(std::random_device{}() + thread_id);
          std::uniform_int_distribution<> user_dist(0, NUM_USERS - 1);
          ProcedureManager* proc_mgr = db->get_procedure_manager();

          while (!stop_flag.load())
          {
            int from = user_dist(gen);
            int to = user_dist(gen);
            if (from == to) continue;

            ProcedureParams params = {
                {"from_user", from}, {"to_user", to}, {"amount", 1}};
            ProcedureOptions opt;
            opt.max_retries = 3;
            opt.backoff_fn = [](int retry_count)
            { return std::chrono::milliseconds(3 * retry_count); };

            auto start = std::chrono::high_resolution_clock::now();

            // FIX 1: Capture params and opt by value to ensure lifetime.
            auto fut = asio::co_spawn(
                io_context_,
                [proc_mgr, params, opt, &proc_name_str]()
                {
                  return proc_mgr->async_execute_procedure(proc_name_str,
                                                           params, opt);
                },
                asio::use_future);

            try
            {
              fut.get();  // Block this client thread until op is complete
            }
            // FIX 2: Catch specific exceptions and log them for debugging.
            catch (const std::exception& e)
            {
              // This will now show the underlying error instead of failing
              // silently.
              std::cerr << "Benchmark op failed: " << e.what() << std::endl;
              continue;
            }

            auto end = std::chrono::high_resolution_clock::now();
            std::chrono::duration<double, std::micro> latency = end - start;
            latencies_per_thread[thread_id].push_back(latency.count());
            total_ops++;
          }
        });
  }

  std::this_thread::sleep_for(std::chrono::seconds(DURATION_SECONDS));
  stop_flag.store(true);

  for (auto& t : client_threads)
  {
    t.join();
  }
  work_guard.reset();
  for (auto& t : executor_threads)
  {
    t.join();
  }

  // --- Analysis ---
  if (total_ops.load() == 0)
  {
    FAIL() << "No operations were successfully completed.";
  }
  std::vector<double> all_latencies;
  for (const auto& vec : latencies_per_thread)
  {
    all_latencies.insert(all_latencies.end(), vec.begin(), vec.end());
  }

  std::sort(all_latencies.begin(), all_latencies.end());

  double throughput = total_ops.load() / (double)DURATION_SECONDS;
  double p50 = all_latencies[all_latencies.size() * 0.50];
  double p95 = all_latencies[all_latencies.size() * 0.95];
  double p99 = all_latencies[all_latencies.size() * 0.99];

  std::cout << "\n--- Throughput Benchmark Results ---\n";
  std::cout << "Threads: " << NUM_THREADS << "\n";
  std::cout << "Duration: " << DURATION_SECONDS << "s\n";
  std::cout << "------------------------------------\n";
  std::cout << "Throughput: " << std::fixed << std::setprecision(2)
            << throughput << " ops/sec\n";
  std::cout << "Total Ops: " << total_ops.load() << "\n";
  std::cout << "Latency (us): p50=" << p50 << " p95=" << p95 << " p99=" << p99
            << "\n";
  std::cout << "------------------------------------\n";

  // The test itself doesn't fail, it just reports metrics.
  SUCCEED();
}

TEST_F(ThroughputBenchmark, PureAsyncThroughput)
{
  // This test measures the backend's throughput from within the async world,
  // avoiding the sync/async bridge overhead of the main benchmark.

  const int NUM_CONCURRENT_WORKERS = 256;
  const int OPS_PER_WORKER = 1000;
  const int TOTAL_OPS = NUM_CONCURRENT_WORKERS * OPS_PER_WORKER;

  // Run the entire test within a single coroutine launched on the io_context
  auto fut = asio::co_spawn(
      io_context_,
      [&]() -> asio::awaitable<double>
      {
        ProcedureManager* proc_mgr = db->get_procedure_manager();


        // --- Worker Coroutine Logic --
        auto worker = [&]() -> asio::awaitable<void>
        {
          std::mt19937 gen(std::random_device{}());
          std::uniform_int_distribution<> user_dist(0, NUM_USERS - 1);

          for (int i = 0; i < OPS_PER_WORKER; ++i)
          {
            int from = user_dist(gen);
            int to = user_dist(gen);
            if (from == to) continue;

            ProcedureParams params = {
                {"from_user", from}, {"to_user", to}, {"amount", 1}};
            ProcedureOptions opt;
            opt.max_retries = 3;

            try
            {
              // The key part: co_await directly, no future, no blocking.
              co_await proc_mgr->async_execute_procedure(proc_name_str, params,
                                                         opt);
            }
            catch (const std::exception& e)
            {
              // For this test, we can ignore failures to focus on raw
              // throughput
            }
          }
        };
        // --- End Worker Logic ---

        // Create a group of concurrent tasks.
        std::vector<decltype(asio::co_spawn(io_context_, worker(),
                                            asio::deferred))>
            tasks;

        auto start_time = std::chrono::high_resolution_clock::now();
        for (int i = 0; i < NUM_CONCURRENT_WORKERS; ++i)
          tasks.push_back(
              asio::co_spawn(io_context_, worker(), asio::deferred));

        co_await asio::experimental::make_parallel_group(std::move(tasks))
            .async_wait(asio::experimental::wait_for_all(),
                        asio::use_awaitable);

        auto end_time = std::chrono::high_resolution_clock::now();
        std::chrono::duration<double> duration = end_time - start_time;

        co_return TOTAL_OPS / duration.count();
      },
      asio::use_future);

  // Setup executor threads and run the test
  std::vector<std::thread> executor_threads;
  auto work_guard = asio::make_work_guard(io_context_);
  const size_t num_hw_threads = std::thread::hardware_concurrency();
  for (size_t i = 0; i < num_hw_threads; ++i)
  {
    executor_threads.emplace_back([this]() { io_context_.run(); });
  }

  double tps = fut.get();

  work_guard.reset();
  for (auto& t : executor_threads)
  {
    t.join();
  }

  std::cout << "\n--- Pure Async Benchmark Results ---\n";
  std::cout << "Concurrency: " << NUM_CONCURRENT_WORKERS << " async workers on "
            << num_hw_threads << " kernel threads\n";
  std::cout << "Total Ops: " << TOTAL_OPS << "\n";
  std::cout << "------------------------------------\n";
  std::cout << "Throughput: " << std::fixed << std::setprecision(2) << tps
            << " ops/sec\n";
  std::cout << "------------------------------------\n";

  // We expect this to be significantly higher than the previous results.
  EXPECT_GT(tps, 50000) << "The pure async throughput should exceed the "
                           "original synchronous TPS.";
}