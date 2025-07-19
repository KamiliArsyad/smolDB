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
  std::string listen_address = "0.0.0.0:50051";
};

}  // namespace smoldb

#endif  // SMOLDB_CONFIG_H