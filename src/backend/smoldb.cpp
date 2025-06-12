#include "smoldb.h"

#include <iostream>

#include "executor/lock_mgr.h"

SmolDB::SmolDB(const std::filesystem::path& db_directory,
               size_t buffer_pool_size)
    : db_directory_(db_directory)
{
  if (!std::filesystem::exists(db_directory_))
  {
    std::filesystem::create_directories(db_directory_);
  }

  db_file_path_ = db_directory_ / "smoldb.db";
  wal_file_path_ = db_directory_ / "smoldb.wal";
  catalog_file_path_ = db_directory_ / "smoldb.cat";

  // Initialization order is critical here
  disk_mgr_ = std::make_unique<Disk_mgr>(db_file_path_);
  wal_mgr_ = std::make_unique<WAL_mgr>(wal_file_path_);
  buffer_pool_ = std::make_unique<BufferPool>(buffer_pool_size, disk_mgr_.get(),
                                              wal_mgr_.get());
  lock_manager_ = std::make_unique<LockManager>();
  txn_manager_ = std::make_unique<TransactionManager>(
      lock_manager_.get(), wal_mgr_.get(), buffer_pool_.get());
  catalog_ = std::make_unique<Catalog>();
  catalog_->set_storage_managers(buffer_pool_.get(), wal_mgr_.get());
  catalog_->set_transaction_managers(lock_manager_.get(), txn_manager_.get());
}

SmolDB::~SmolDB()
{
  if (!is_shutdown_)
  {
    // Ensure shutdown is called if user forgets, useful for RAII
    shutdown();
  }
}

void SmolDB::startup()
{
  // 1. Load catalog from disk. If it doesn't exist, this is a no-op.
  catalog_->load(catalog_file_path_);

  // 2. Perform recovery from WAL. This will replay any changes not yet
  // persisted to the db file.
  wal_mgr_->recover(*buffer_pool_, wal_file_path_);

  // 3. Re-initialize table objects based on loaded catalog metadata.
  catalog_->reinit_tables();
}

void SmolDB::shutdown()
{
  if (is_shutdown_)
  {
    return;
  }

  // 1. Persist the current state of the catalog to disk.
  catalog_->dump(catalog_file_path_);

  // 2. Flush all dirty pages from buffer pool to disk to ensure durability.
  buffer_pool_->flush_all();

  is_shutdown_ = true;
}

void SmolDB::create_table(uint8_t table_id, const std::string& table_name,
                          const Schema& schema, size_t max_tuple_size)
{
  catalog_->create_table(table_id, table_name, schema, max_tuple_size);
}

Table<>* SmolDB::get_table(const std::string& table_name)
{
  return catalog_->get_table(table_name);
}

Table<>* SmolDB::get_table(uint8_t table_id)
{
  return catalog_->get_table(table_id);
}

TransactionID SmolDB::begin_transaction()
{
    return txn_manager_->begin();
}

void SmolDB::commit_transaction(TransactionID txn_id)
{
    txn_manager_->commit(txn_id);
}

void SmolDB::abort_transaction(TransactionID txn_id)
{
    txn_manager_->abort(txn_id);
}
