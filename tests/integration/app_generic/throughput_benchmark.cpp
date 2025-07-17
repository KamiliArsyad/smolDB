#include <gtest/gtest.h>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <iostream>
#include <numeric>
#include <random>
#include <thread>
#include <vector>

#include "backend/proc/proc.h"
#include "backend/proc/proc_ctx.h"
#include "backend/smoldb.h"

// We can reuse the same procedure from the correctness test
class TransferPointsProcedureForBench : public TransactionProcedure
{
 public:
  std::string get_name() const override { return "transfer_points"; }
  ProcedureStatus execute(TransactionContext& ctx,
                          const ProcedureParams& params, ProcedureResult& result) override
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
    table->update_row(ctx.get_txn_id(), from_rid, from_row);
    table->update_row(ctx.get_txn_id(), to_rid, to_row);
    return ProcedureStatus::SUCCESS;
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
    config.buffer_pool_size_frames = 512;  // A larger pool for a bench
    db = std::make_unique<SmolDB>(config);
    db->startup();

    Schema schema = {{0, "user_id", Col_type::INT, false, {}},
                     {1, "points", Col_type::INT, false, {}}};
    db->create_table(1, "user_points", schema, 256);
    db->create_index(1, 0, "pk_user_points");

    TransactionID txn = db->begin_transaction();
    Table<>* table = db->get_table("user_points");
    for (int i = 0; i < NUM_USERS; ++i)
    {
      Row r(schema);
      r.set_value("user_id", i);
      r.set_value("points", INITIAL_POINTS);
      table->insert_row(txn, r);
    }
    db->commit_transaction(txn);

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
};

TEST_F(ThroughputBenchmark, MeasureThroughputAndLatency)
{
  const int NUM_THREADS = std::thread::hardware_concurrency();
  const int DURATION_SECONDS = 5;

  std::atomic<bool> stop_flag = false;
  std::atomic<size_t> total_ops = 0;
  std::vector<std::thread> threads;
  std::vector<std::vector<double>> latencies_per_thread(NUM_THREADS);

  for (int i = 0; i < NUM_THREADS; ++i)
  {
    threads.emplace_back(
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

            auto start = std::chrono::high_resolution_clock::now();
            ProcedureOptions opt;
            opt.max_retries = 3;
            opt.backoff_fn = [](int retry_count) { return std::chrono::milliseconds(3 * retry_count); };

            proc_mgr->execute_procedure("transfer_points", params, opt);
            auto end = std::chrono::high_resolution_clock::now();

            std::chrono::duration<double, std::micro> latency = end - start;
            latencies_per_thread[thread_id].push_back(latency.count());
            total_ops++;
          }
        });
  }

  std::this_thread::sleep_for(std::chrono::seconds(DURATION_SECONDS));
  stop_flag.store(true);

  for (auto& t : threads)
  {
    t.join();
  }

  // --- Analysis ---
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