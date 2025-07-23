#include "smoldb.h"

#include <iostream>

#include "executor/lock_mgr.h"
#include "index/idx.h"
#include "recovery_manager.h"

using namespace smoldb;

SmolDB::SmolDB(const smoldb::DBConfig& config,
               boost::asio::any_io_executor executor)
    : db_directory_(config.db_directory), executor_(std::move(executor))
{
  if (!std::filesystem::exists(db_directory_))
  {
    std::filesystem::create_directories(db_directory_);
  }

  db_file_path_ = db_directory_ / "smoldb.db";
  wal_file_path_ = db_directory_ / "smoldb.wal";
  catalog_file_path_ = db_directory_ / "smoldb.cat";

  // All managers are created here, and only here. This is critical for tests.
  disk_mgr_ = std::make_unique<Disk_mgr>(db_file_path_);
  wal_mgr_ = std::make_unique<WAL_mgr>(wal_file_path_, executor_);
  buffer_pool_ = std::make_unique<BufferPool>(config.buffer_pool_size_frames,
                                              disk_mgr_.get(), wal_mgr_.get(),
                                              config.buffer_pool_shard_count);
  lock_manager_ =
      std::make_unique<LockManager>(config.lock_manager_shard_count);
  txn_manager_ = std::make_unique<TransactionManager>(
      lock_manager_.get(), wal_mgr_.get(), buffer_pool_.get());
  catalog_ = std::make_unique<Catalog>();
  proc_manager_ = std::make_unique<ProcedureManager>(txn_manager_.get(),
                                                     catalog_.get(), executor_);
}

SmolDB::~SmolDB()
{
  // If shutdown was not called, the system "crashed". unique_ptrs will clean
  // up memory, but durability guarantees are lost.
  if (!is_shutdown_)
  {
    // In a real crash, this destructor wouldn't even run. In our tests,
    // it stops the WAL thread, mimicking a process exit.
  }
}

void SmolDB::startup()
{
  // The WAL is already running, which is fine. Recovery must serialize
  // against it by using its own read-only view of the file.
  RecoveryManager recovery_mgr(buffer_pool_.get(), wal_mgr_.get());
  recovery_mgr.recover();

  catalog_->load(catalog_file_path_);
  catalog_->set_storage_managers(buffer_pool_.get(), wal_mgr_.get());
  catalog_->set_transaction_managers(lock_manager_.get(), txn_manager_.get());
  catalog_->reinit_tables();
  catalog_->build_all_indexes();
}

#ifndef NDEBUG
void SmolDB::startup_with_crash_point(RecoveryCrashPoint crash_point)
{
  RecoveryManager recovery_mgr(buffer_pool_.get(), wal_mgr_.get());
  recovery_mgr.set_crash_point(crash_point);

  // As the test expects, this will throw an exception partway through.
  recovery_mgr.recover();
}
#endif

void SmolDB::shutdown()
{
  if (is_shutdown_)
  {
    return;
  }

  // Persist the catalog *before* flushing pages, in case the catalog
  // itself has dirty pages.
  catalog_->dump(catalog_file_path_);

  // Flush all data to disk.
  buffer_pool_->flush_all();

  // The WAL_mgr destructor will be called after this, stopping the thread.
  is_shutdown_ = true;
}

void SmolDB::create_table(uint8_t table_id, const std::string& table_name,
                          const Schema& schema, size_t max_tuple_size)
{
  catalog_->create_table(table_id, table_name, schema, max_tuple_size);
  catalog_->dump(catalog_file_path_);
}

void SmolDB::create_index(uint8_t table_id, uint8_t key_column_id,
                          const std::string& index_name)
{
  catalog_->create_index(table_id, key_column_id, index_name);
  catalog_->dump(catalog_file_path_);
}

Table<>* SmolDB::get_table(const std::string& table_name)
{
  return catalog_->get_table(table_name);
}

Table<>* SmolDB::get_table(uint8_t table_id)
{
  return catalog_->get_table(table_id);
}

ProcedureManager* SmolDB::get_procedure_manager() const
{
  return proc_manager_.get();
}

TransactionID SmolDB::begin_transaction() { return txn_manager_->begin(); }

void SmolDB::commit_transaction(TransactionID txn_id)
{
  txn_manager_->commit(txn_id);
}

void SmolDB::abort_transaction(TransactionID txn_id)
{
  txn_manager_->abort(txn_id);
}

boost::asio::awaitable<void> SmolDB::async_commit_transaction(
    TransactionID txn_id)
{
  co_await txn_manager_->async_commit(txn_id);
}

boost::asio::awaitable<void> SmolDB::async_abort_transaction(
    TransactionID txn_id)
{
  co_await txn_manager_->async_abort(txn_id);
}