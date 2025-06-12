#ifndef SMOLDB_H
#define SMOLDB_H

#include <filesystem>
#include <memory>

#include "access/access.h"
#include "storage/heapfile.h"
#include "storage/bfrpl.h"
#include "storage/dsk_mgr.h"
#include "storage/wal_mgr.h"

class SmolDB
{
 public:
  explicit SmolDB(const std::filesystem::path& db_directory,
                  size_t buffer_pool_size = 128);
  ~SmolDB();

  // No copy/move
  SmolDB(const SmolDB&) = delete;
  SmolDB& operator=(const SmolDB&) = delete;

  /**
   * @brief Starts the database system.
   * Performs recovery from WAL and loads the catalog.
   */
  void startup();

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

 private:
  friend class HeapFileTest;  // Allow test to access internals

  std::filesystem::path db_directory_;
  std::filesystem::path db_file_path_;
  std::filesystem::path wal_file_path_;
  std::filesystem::path catalog_file_path_;

  bool is_shutdown_ = false;

  std::unique_ptr<Disk_mgr> disk_mgr_;
  std::unique_ptr<WAL_mgr> wal_mgr_;
  std::unique_ptr<BufferPool> buffer_pool_;
  std::unique_ptr<Catalog> catalog_;
};

#endif  // SMOLDB_H