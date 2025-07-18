#ifndef SMOLDB_H
#define SMOLDB_H

#include <filesystem>
#include <memory>

#include "access/access.h"
#include "config.h"
#include "executor/trx.h"
#include "executor/trx_mgr.h"
#include "proc_mgr.h"
#include "recovery_manager.h"  // For RecoveryCrashPoint
#include "storage/bfrpl.h"
#include "storage/dsk_mgr.h"
#include "storage/heapfile.h"
#include "storage/wal_mgr.h"

namespace smoldb
{

class Transaction;

const size_t BUFFER_SIZE_FOR_TEST = std::thread::hardware_concurrency();

class SmolDB
{
 public:
  explicit SmolDB(const smoldb::DBConfig& config);
  ~SmolDB();

  // No copy/move
  SmolDB(const SmolDB&) = delete;
  SmolDB& operator=(const SmolDB&) = delete;

  /**
   * @brief Starts the database system.
   * Performs recovery from WAL and loads the catalog.
   */
  void startup();

#ifndef NDEBUG
  void startup_with_crash_point(RecoveryCrashPoint crash_point);
#endif

  /**
   * @brief Shuts down the database system.
   * Flushes all dirty pages to disk.
   */
  void shutdown();

  /**
   * @brief Creates a new table.
   * @param table_id The unique ID for the table.
   * @param table_name The name of the table.
   * @param schema The schema of the table.
   * @param max_tuple_size The maximum size of a tuple in bytes.
   */
  void create_table(uint8_t table_id, const std::string& table_name,
                    const Schema& schema, size_t max_tuple_size = 256);

  /**
   * @brief Creates a new index on a table.
   * @param table_id The ID of the table to index.
   * @param key_column_id The ID of the column to use as the index key.
   * @param index_name The name of the index (for future use).
   */
  void create_index(uint8_t table_id, uint8_t key_column_id,
                    const std::string& index_name = {});

  /**
   * @brief Retrieves a pointer to a table by its name.
   * @param table_name The name of the table.
   * @return A pointer to the table, or nullptr if not found.
   */
  Table<>* get_table(const std::string& table_name);

  /**
   * @brief Retrieves a pointer to a table by its ID.
   * @param table_id The ID of the table.
   * @return A pointer to the table, or nullptr if not found.
   */
  Table<>* get_table(uint8_t table_id);

  /**
   * @brief Retrieves the procedure manager to register or execute procedures.
   */
  [[nodiscard]] ProcedureManager* get_procedure_manager() const;

  /**
   * @brief Begins a new transaction.
   * @return The ID of the transaction.
   */
  TransactionID begin_transaction();

  /**
   * @brief Commits an existing transaction.
   */
  void commit_transaction(TransactionID txn_id);

  /**
   * @brief Aborts an existing transaction.
   */
  void abort_transaction(TransactionID txn_id);

 private:
  friend class HeapFileTest;  // Allow test to access internals
  friend class AriesTest;

  std::filesystem::path db_directory_;
  std::filesystem::path db_file_path_;
  std::filesystem::path wal_file_path_;
  std::filesystem::path catalog_file_path_;

  bool is_shutdown_ = false;

  std::unique_ptr<Disk_mgr> disk_mgr_;
  std::unique_ptr<WAL_mgr> wal_mgr_;
  std::unique_ptr<BufferPool> buffer_pool_;
  std::unique_ptr<LockManager> lock_manager_;
  std::unique_ptr<TransactionManager> txn_manager_;
  std::unique_ptr<Catalog> catalog_;
  std::unique_ptr<ProcedureManager> proc_manager_;
};

}  // namespace smoldb
#endif  // SMOLDB_H