#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <random>
#include <thread>
#include <vector>

#define private public
#include "backend/smoldb.h"
#undef private

// A simple thread-safe logger for debugging concurrency issues.
static std::mutex log_mutex;
void log_thread_activity(const std::string& msg)
{
  std::scoped_lock lock(log_mutex);
  std::cout << "[Thread " << std::this_thread::get_id() << "] " << msg
            << std::endl;
}

/**
 * @test ConcurrencyFuzzTest.HammerTheBufferPool
 * @brief A non-deterministic stress test for the BufferPool.
 *
 * This test's primary purpose is to create a high-contention environment to
 * reveal subtle race conditions, livelocks, or deadlocks in the BufferPool's
 * sharded implementation. It is best run under thread-sanitizer (TSan) or
 * on a multi-core machine for extended periods.
 *
 * It works by spawning a number of threads equal to the hardware concurrency
 * and having each thread repeatedly and randomly request pages from a fixed
 * set. A small buffer pool size is intentionally chosen to force frequent
 * evictions, maximizing pressure on the eviction and page-loading logic.
 */
TEST(ConcurrencyFuzzTest, HammerTheBufferPool)
{
  const size_t num_threads = 3 * std::thread::hardware_concurrency();
  const int num_pages_to_hammer = 1000;
  const std::chrono::seconds test_duration = std::chrono::seconds(3);

  auto test_dir = std::filesystem::temp_directory_path() / "fuzz_test";
  std::filesystem::remove_all(test_dir);
  std::filesystem::create_directories(test_dir);

  SmolDB db(test_dir, 32);
  db.startup();

  std::atomic<bool> stop_flag = false;
  std::vector<std::thread> threads;

  for (size_t i = 0; i < num_threads; ++i)
  {
    threads.emplace_back(
        [&]()
        {
          std::random_device rd;
          std::mt19937 gen(rd());
          std::uniform_int_distribution<> distrib(0, num_pages_to_hammer - 1);

          while (!stop_flag.load())
          {
            PageID pid = distrib(gen);
            // log_thread_activity("Attempting to fetch page " +
            //                     std::to_string(pid));
            ASSERT_NO_THROW({
              auto guard = db.buffer_pool_->fetch_page(pid);
              // log_thread_activity("Fetched page " + std::to_string(pid) +
              //                     ",marking dirty.");
              guard->data()[0] = std::byte{static_cast<uint8_t>(pid)};
              guard.mark_dirty();
            });
          }
        });
  }

  std::this_thread::sleep_for(test_duration);
  stop_flag.store(true);

  for (auto& t : threads)
  {
    t.join();
  }

  ASSERT_NO_THROW(db.shutdown());

  std::filesystem::remove_all(test_dir);
}