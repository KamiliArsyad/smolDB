#include "wal_mgr.h"

#include <chrono>
#include <cstring>
#include <iostream>
#include <numeric>
#include <vector>

#include "bfrpl.h"

using namespace smoldb;

namespace
{
struct FlushStat
{
  size_t bytes = 0;
  uint64_t usec = 0;
};
thread_local std::vector<FlushStat> g_stats;
}  // namespace

namespace
{
constexpr int kMaxIovPerCall = IOV_MAX;
constexpr size_t kMaxBytesPerCall = 1 << 20;

static inline void advance_iov(std::vector<iovec>& iov, size_t& i,
                               size_t& off_inc, size_t nbytes)
{
  while (nbytes)
  {
    auto& v = iov[i];
    size_t take = std::min(nbytes, v.iov_len);
    v.iov_base = static_cast<char*>(v.iov_base) + take;
    v.iov_len -= take;
    off_inc += take;
    nbytes -= take;
    if (v.iov_len == 0) ++i;
  }
}
}  // namespace

WAL_mgr::WAL_mgr(const std::filesystem::path& path,
                 boost::asio::any_io_executor executor,
                 size_t batch_byte_thresh,
                 std::chrono::microseconds batch_deadline_thresh)
    : batch_bytes_(batch_byte_thresh),
      batch_time_(batch_deadline_thresh),
      path_(path),
      executor_(std::move(executor))
{
  std::scoped_lock lock(general_mtx_);

  LSN max_lsn = 0;
  std::ifstream in(path_, std::ios::binary);
  if (in.is_open())
  {
    std::streamoff curr_offset = 0;
    while (in.peek() != EOF)
    {
      curr_offset = in.tellg();
      LogRecordHeader hdr{};
      in.read(reinterpret_cast<char*>(&hdr), sizeof(hdr));
      if (in.gcount() != sizeof(hdr)) break;
      if (hdr.lsn == 0) continue;
      lsn_to_offset_idx_[hdr.lsn] = curr_offset;
      max_lsn = std::max(max_lsn, hdr.lsn);
      if (hdr.lr_length > sizeof(LogRecordHeader))
      {
        in.seekg(hdr.lr_length - sizeof(LogRecordHeader), std::ios::cur);
      }
    }
    in.close();
  }

  next_lsn_.store(max_lsn + 1);
  flushed_lsn_.store(max_lsn);

  // I'm hardcoding this for now as I don't see why any other config is needed.
  wal_fd_ = open(path_.c_str(), O_CREAT | O_WRONLY | O_CLOEXEC, 0644);
  if (wal_fd_ < 0)
    throw std::runtime_error("open WAL failed: " + path_.string());

  file_offset_ = lseek(wal_fd_, 0, SEEK_END);
  if (file_offset_ < 0) throw std::runtime_error("lseek WAL failed");

  flush_deadline_ = std::chrono::steady_clock::time_point::max();
  queue_was_empty_ = true;

  writer_thread_ = std::thread(&WAL_mgr::writer_thread_main, this);
}

WAL_mgr::~WAL_mgr()
{
  if (writer_thread_.joinable())
  {
    stop_writer_.store(true);
    cv_.notify_one();
    writer_thread_.join();
  }
  if (wal_fd_ >= 0) ::close(wal_fd_);

}

void WAL_mgr::writer_thread_main()
{
  pthread_setname_np(pthread_self(), "wal-writer");
  using clock = std::chrono::steady_clock;
  std::list<std::unique_ptr<LogRecordBatch>> local_queue;

  while (true)
  {
    // Wait until condition to flush is met
    {
      std::unique_lock lock(mtx_);

      auto predicate = [&]
      {
        if (stop_writer_.load() && write_queue_.empty()) return true;
        if (write_queue_.empty()) return false;
        // flush if B reached or T expired
        bool size_ready = (pending_bytes_ >= batch_bytes_);
        bool time_ready = (clock::now() >= flush_deadline_);
        return size_ready || time_ready;
      };

      // If queue empty, just wait (no deadline)
      while (!predicate())
      {
        if (write_queue_.empty())
        {
          cv_.wait(lock, [&]
                   { return !write_queue_.empty() || stop_writer_.load(); });
          if (stop_writer_.load() && write_queue_.empty())
          {
            /* For stats collection
             * TODO: Set up a cmake flag for turning this on/off
            if (!g_stats.empty())
            {
              std::vector<uint64_t> lat;
              lat.reserve(g_stats.size());
              std::vector<size_t> sz;
              sz.reserve(g_stats.size());
              for (auto& s : g_stats)
              {
                lat.push_back(s.usec);
                sz.push_back(s.bytes);
              }
              std::sort(lat.begin(), lat.end());
              size_t p90 = lat[lat.size() * 90 / 100];

              auto avg_sz = std::accumulate(sz.begin(), sz.end(), 0ULL) / sz.size();
              std::cout << "[WAL] flush p90 = " << p90 << " µs, "
                        << "avg bytes/flush = " << avg_sz << '\n';
            }
            */

            return;
          }
          // queue became non-empty: ensure deadline set by producer
          continue;
        }
        // Queue non-empty: wait until deadline or signal (size threshold)
        cv_.wait_until(lock, flush_deadline_, predicate);
      }

      if (stop_writer_.load() && write_queue_.empty()) return;

      local_queue.splice(local_queue.end(), write_queue_);
      pending_bytes_ = 0;
      flush_deadline_ = clock::time_point::max();
      queue_was_empty_ = true;
    }

    if (local_queue.empty()) continue;

    std::vector<iovec> iov;
    iov.reserve(local_queue.size());

    LSN max_lsn_in_batch = 0;
    off_t batch_start_off = 0;
    size_t total_bytes = 0;

    {
      std::scoped_lock lock(general_mtx_);
      batch_start_off = file_offset_;
      off_t cur = batch_start_off;

      for (const auto& batch : local_queue)
      {
        const auto* hdr =
            reinterpret_cast<const LogRecordHeader*>(batch->data.data());
        lsn_to_offset_idx_[hdr->lsn] = cur;
        max_lsn_in_batch = std::max(max_lsn_in_batch, hdr->lsn);

        iov.push_back(
            iovec{.iov_base = const_cast<char*>(
                      reinterpret_cast<const char*>(batch->data.data())),
                  .iov_len = batch->data.size()});
        cur += batch->data.size();
        total_bytes += batch->data.size();
      }
    }

    off_t off = batch_start_off;
    size_t i = 0;
    size_t wrote_total = 0;
    auto flush_start = std::chrono::steady_clock::now();
    while (i < iov.size())
    {
      // pack a chunk limited by both iov count and bytes
      int cnt = 0;
      size_t bytes = 0;
      for (size_t j = i; j < iov.size() && cnt < kMaxIovPerCall; ++j)
      {
        if (bytes + iov[j].iov_len > kMaxBytesPerCall) break;
        bytes += iov[j].iov_len;
        ++cnt;
      }
      if (cnt == 0)
      {  // a single entry too large; write it alone
        cnt = 1;
        bytes = std::min(iov[i].iov_len, kMaxBytesPerCall);
      }

      ssize_t w = pwritev2(wal_fd_, &iov[i], cnt, off, RWF_DSYNC);
      if (w < 0)
      {
        int e = errno;
        throw std::runtime_error(std::string("pwritev failed: ") +
                                 std::strerror(e));
      }

      size_t off_inc = 0;
      advance_iov(iov, i, off_inc, static_cast<size_t>(w));
      off += off_inc;
      wrote_total += static_cast<size_t>(w);
    }
    auto flush_end = std::chrono::steady_clock::now();
    auto dur_us = std::chrono::duration_cast<std::chrono::microseconds>(
                      flush_end - flush_start)
                      .count();

    g_stats.push_back(
        FlushStat{.bytes = total_bytes, .usec = static_cast<uint64_t>(dur_us)});
    if (wrote_total != total_bytes)
      throw std::runtime_error("short pwritev to WAL");

    // // IMPORTANT: DURABILITY (upd: no longer needed wit pwritev2 + RWF flag.
    // if (fdatasync(wal_fd_) != 0)
    // {
    //   int e = errno;
    //   throw std::runtime_error(std::string("fdatasync failed: ") +
    //                            std::strerror(e));
    // }

    {
      // Avoid waiting for batches arranged in tandem
      std::lock_guard lock(mtx_);
      if (!write_queue_.empty()) flush_deadline_ = clock::now();
    }

    {
      std::scoped_lock lock(general_mtx_);
      file_offset_ = batch_start_off + static_cast<off_t>(wrote_total);
    }

    flushed_lsn_.store(max_lsn_in_batch, std::memory_order_release);

    std::vector<std::function<void()>> handlers_to_run;
    {
      std::lock_guard lock(waiters_mutex_);
      auto range_end = waiters_.upper_bound(max_lsn_in_batch);
      for (auto it = waiters_.begin(); it != range_end; ++it)
      {
        handlers_to_run.push_back(std::move(it->second));
      }
      waiters_.erase(waiters_.begin(), range_end);
    }

    for (auto& handler : handlers_to_run)
    {
      boost::asio::post(executor_, std::move(handler));
    }

    {
      std::lock_guard lock(flush_mutex_);
      flush_cv_.notify_all();
    }

    local_queue.clear();
  }
}

LSN WAL_mgr::append_record_async(std::unique_ptr<LogRecordBatch>&& batch)
{
  auto* hdr = reinterpret_cast<LogRecordHeader*>(batch->data.data());
  LSN reserved_lsn = next_lsn_.fetch_add(1);
  hdr->lsn = reserved_lsn;

  {
    std::scoped_lock<std::mutex> lock(mtx_);
    bool was_empty = write_queue_.empty();
    write_queue_.push_back(std::move(batch));
    pending_bytes_ += write_queue_.back()->data.size();

    if (was_empty)
    {
      queue_was_empty_ = false;
      flush_deadline_ = std::chrono::steady_clock::now() + batch_time_;
      cv_.notify_one();
    }
    else if (pending_bytes_ >= batch_bytes_)
    {
      cv_.notify_one();
    }
  }

  return reserved_lsn;
}

LSN WAL_mgr::append_record(LogRecordHeader& hdr, const void* payload)
{
  auto batch = std::make_unique<LogRecordBatch>();
  const std::size_t payload_sz =
      hdr.lr_length - sizeof(LogRecordHeader);  // may be 0
  batch->data.resize(sizeof(LogRecordHeader) + payload_sz);

  // copy header; further mutation happens inside append_record_async
  std::memcpy(batch->data.data(), &hdr, sizeof(LogRecordHeader));

  if (payload_sz && payload)
    std::memcpy(batch->data.data() + sizeof(LogRecordHeader), payload,
                payload_sz);

  LSN lsn = append_record_async(std::move(batch));

  std::unique_lock lk(flush_mutex_);
  flush_cv_.wait(lk, [&] { return is_lsn_flushed(lsn); });

  hdr.lsn = lsn;
  return lsn;
}

void WAL_mgr::recover(BufferPool& bfr_manager,
                      const std::filesystem::path& path)
{
  std::ifstream in(path, std::ios::binary);
  if (!in.is_open())
  {
    throw std::runtime_error("Unable to open WAL file: " + path.string());
  }

  while (in.peek() != EOF)
  {
    LogRecordHeader hdr;
    in.read(reinterpret_cast<char*>(&hdr), sizeof(hdr));
    if (in.gcount() != sizeof(hdr)) break;

    uint32_t payload_len = hdr.lr_length - sizeof(LogRecordHeader);
    std::vector<char> buf(payload_len);
    in.read(buf.data(), payload_len);
    if (static_cast<uint32_t>(in.gcount()) != payload_len)
    {
      throw std::runtime_error("Corrupt WAL: unexpected payload size");
    }

    switch (hdr.type)
    {
      case UPDATE:
      {
        auto* upd = reinterpret_cast<const UpdatePagePayload*>(buf.data());
        auto page = bfr_manager.fetch_page(upd->page_id);
        auto pw = page.write();
        std::memcpy(pw->data() + upd->offset, upd->aft(), upd->length);
        pw->hdr.page_lsn = hdr.lsn;
        page.mark_dirty();
        break;
      }
      case COMMIT:
      case ABORT:
        break;
      default:
        break;
    }
  }

  in.close();
}

std::map<LSN, std::pair<LogRecordHeader, std::vector<char>>>
WAL_mgr::read_all_records()
{
  std::scoped_lock lock(general_mtx_);
  std::map<LSN, std::pair<LogRecordHeader, std::vector<char>>> records;

  std::ifstream in(path_, std::ios::binary);
  if (!in.is_open())
  {
    return records;
  }

  while (in.peek() != EOF)
  {
    LogRecordHeader hdr;
    in.read(reinterpret_cast<char*>(&hdr), sizeof(hdr));
    if (in.gcount() != sizeof(hdr)) break;
    if (hdr.lr_length < sizeof(LogRecordHeader)) break;

    uint32_t payload_len = hdr.lr_length - sizeof(LogRecordHeader);
    std::vector<char> buf(payload_len);
    if (payload_len > 0)
    {
      in.read(buf.data(), payload_len);
      if ((uint32_t)in.gcount() != payload_len) break;
    }
    records[hdr.lsn] = {hdr, std::move(buf)};
  }
  return records;
}

bool WAL_mgr::get_record(LSN lsn, LogRecordHeader& header_out,
                         std::vector<char>& payload_out)
{
  std::scoped_lock lock(general_mtx_);
  if (!lsn_to_offset_idx_.contains(lsn)) return false;
  auto offset = lsn_to_offset_idx_[lsn];

  std::ifstream in(path_, std::ios::binary);
  if (!in.is_open())
  {
    return false;
  }

  in.seekg(offset);
  if (!in) return false;

  in.read(reinterpret_cast<char*>(&header_out), sizeof(LogRecordHeader));
  if (in.gcount() != sizeof(LogRecordHeader)) return false;

  if (header_out.lsn != lsn)
  {
    throw std::runtime_error("LSN mismatch: index corruption.");
  }

  uint32_t payload_size = header_out.lr_length - sizeof(LogRecordHeader);
  if (payload_size > 0)
  {
    payload_out.resize(payload_size);
    in.read(payload_out.data(), payload_size);
    if (static_cast<uint32_t>(in.gcount()) != payload_size) return false;
  }
  else
  {
    payload_out.clear();
  }

  return true;
}

WAL_mgr::BatchBuilder WAL_mgr::make_batch(LR_TYPE type, uint64_t txn_id,
                                          LSN prev_lsn, std::size_t payload_sz,
                                          std::size_t extra_sz)
{
  auto batch = std::make_unique<LogRecordBatch>();
  batch->data.resize(sizeof(LogRecordHeader) + payload_sz + extra_sz);

  auto* hdr = reinterpret_cast<LogRecordHeader*>(batch->data.data());
  hdr->lsn = 0;  // filled when appended
  hdr->type = type;
  hdr->txn_id = txn_id;
  hdr->prev_lsn = prev_lsn;
  hdr->lr_length = static_cast<uint32_t>(batch->data.size());

  return BatchBuilder(std::move(batch), hdr);
}
