#ifndef RECOVERY_MANAGER_H
#define RECOVERY_MANAGER_H

#include <map>

#include "bfrpl.h"
#include "trx.h"
#include "wal_mgr.h"

enum class RecoveryState
{
  IN_FLIGHT,
  COMMITTED
};

struct ATT_Entry
{
  RecoveryState state;
  LSN last_lsn;
};

// For testing: define specific, deterministic crash points.
enum class RecoveryCrashPoint
{
  DISABLED,
  AFTER_ANALYSIS,
  DURING_REDO,
  DURING_UNDO
};

class RecoveryManager
{
 public:
  explicit RecoveryManager(BufferPool* buffer_pool, WAL_mgr* wal_mgr);

  void recover();

#ifndef NDEBUG
  void set_crash_point(RecoveryCrashPoint point) { crash_point_ = point; }
#endif

 private:
  void analysis_phase();
  void redo_phase();
  void undo_phase();

  void apply_change(LSN lsn, uint32_t page_id, uint16_t offset, uint16_t length,
                    const std::byte* data);

#ifndef NDEBUG
  void check_for_crash(RecoveryCrashPoint current_point);
  RecoveryCrashPoint crash_point_ = RecoveryCrashPoint::DISABLED;
#endif

  BufferPool* buffer_pool_;
  WAL_mgr* wal_mgr_;

  std::map<LSN, std::pair<LogRecordHeader, std::vector<char>>> log_records_;
  std::map<TransactionID, ATT_Entry> active_txn_table_;
  std::map<PageID, LSN> dirty_page_table_;
};

#endif  // RECOVERY_MANAGER_H