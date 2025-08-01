#ifndef SMOLDB_CONFIG_H
#define SMOLDB_CONFIG_H

#include <cstddef>
#include <filesystem>

namespace smoldb
{

/**
 * @brief Centralized configuration for a SmolDB instance.
 *
 * This struct holds all engine-level parameters. It is designed to be
 * programmatically configured, with safe, hardcoded defaults.
 */
struct DBConfig
{
  std::filesystem::path db_directory = "./smoldb_data";
  size_t buffer_pool_size_frames = 128;
  size_t buffer_pool_shard_count = std::thread::hardware_concurrency();
  size_t lock_manager_shard_count = std::thread::hardware_concurrency();
  int trx_lock_timeout = 100;

  std::string listen_address = "0.0.0.0";
  std::string listen_port = "50051";

  const std::string get_full_listen_address()
  {
    return listen_address + ":" + listen_port;
  }
};

}  // namespace smoldb

#endif  // SMOLDB_CONFIG_H