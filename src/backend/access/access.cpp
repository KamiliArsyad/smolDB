#include "access.h"

#include <boost/archive/binary_iarchive.hpp>
#include <boost/archive/binary_oarchive.hpp>
#include <cassert>
#include <cstring>
#include <fstream>
#include <sstream>

// Row implementation
std::vector<std::byte> Row::to_bytes() const
{
  std::ostringstream oss;
  boost::archive::binary_oarchive oa(oss);
  oa << *this;

  std::string str = oss.str();
  std::vector<std::byte> bytes(str.size());
  std::memcpy(bytes.data(), str.data(), str.size());
  return bytes;
}

Row Row::from_bytes(const std::vector<std::byte>& data, const Schema& schema)
{
  std::string str(reinterpret_cast<const char*>(data.data()), data.size());
  std::istringstream iss(str);
  boost::archive::binary_iarchive ia(iss);

  Row row;
  ia >> row;

  // Ensure the schema matches (in case of schema evolution)
  if (row.schema_ != schema)
  {
    // For now, just update the schema. In the future, we might want schema
    // migration logic
    row.schema_ = schema;
  }

  return row;
}

// Catalog implementation
void Catalog::dump(const std::filesystem::path& path) const
{
  std::ofstream ofs{path, std::ios::binary};
  boost::archive::binary_oarchive oa{ofs};
  oa << *this;
}

void Catalog::load(const std::filesystem::path& path)
{
  std::ifstream ifs{path, std::ios::binary};
  if (!ifs.is_open())
  {
    // It's not an error if the catalog file doesn't exist yet
    return;
  }
  boost::archive::binary_iarchive ia{ifs};
  ia >> *this;
}

void Catalog::create_table(uint8_t table_id, const std::string& table_name,
                           const Schema& schema, size_t max_tuple_size)
{
  if (m_schemas_.count(table_id))
  {
    throw std::invalid_argument("Table with ID " + std::to_string(table_id) +
                                " already exists");
  }
  for (const auto& [id, meta] : m_schemas_)
  {
    if (meta.table_name == table_name)
    {
      throw std::invalid_argument("Table with name '" + table_name +
                                  "' already exists");
    }
  }

  assert(buffer_pool_ && "BufferPool not set in Catalog");
  assert(wal_mgr_ && "WAL_mgr not set in Catalog");
  assert(lock_manager_ && "LockManager not set in Catalog");
  assert(txn_manager_ && "TransactionManager not set in Catalog");

  PageID first_page_id = buffer_pool_->allocate_page();
  m_schemas_.emplace(table_id, TableMetadata{schema, first_page_id, table_name,
                                             max_tuple_size});

  auto heap_file = std::make_unique<HeapFile>(buffer_pool_, wal_mgr_,
                                              first_page_id, max_tuple_size);
  tables_[table_id] =
      std::make_unique<Table<>>(std::move(heap_file), table_id, table_name,
                                schema, lock_manager_, txn_manager_);
}

Table<>* Catalog::get_table(uint8_t table_id)
{
  auto it = tables_.find(table_id);
  if (it != tables_.end())
  {
    return it->second.get();
  }

  // Table not loaded, try to load from metadata
  auto meta_it = m_schemas_.find(table_id);
  if (meta_it != m_schemas_.end())
  {
    assert(buffer_pool_ && "BufferPool not set in Catalog");
    assert(wal_mgr_ && "WAL_mgr not set in Catalog");
    assert(lock_manager_ && "LockManager not set in Catalog");
    assert(txn_manager_ && "TransactionManager not set in Catalog");

    const auto& meta = meta_it->second;
    auto heap_file = std::make_unique<HeapFile>(
        buffer_pool_, wal_mgr_, meta.first_page_id, meta.max_tuple_size);
    tables_[table_id] = std::make_unique<Table<>>(
        std::move(heap_file), table_id, meta.table_name, meta.schema,
        lock_manager_, txn_manager_);
    return tables_[table_id].get();
  }

  return nullptr;
}

Table<>* Catalog::get_table(const std::string& table_name)
{
  // Check loaded tables first
  for (auto& [id, table] : tables_)
  {
    if (table && table->get_table_name() == table_name)
    {
      return table.get();
    }
  }

  // Table not loaded, search metadata
  for (const auto& [id, meta] : m_schemas_)
  {
    if (meta.table_name == table_name)
    {
      return get_table(id);  // Will load if not already loaded
    }
  }
  return nullptr;
}

std::vector<uint8_t> Catalog::list_table_ids() const
{
  std::vector<uint8_t> ids;
  ids.reserve(m_schemas_.size());
  for (const auto& [id, schema] : m_schemas_)
  {
    ids.push_back(id);
  }
  return ids;
}

void Catalog::reinit_tables()
{
  assert(buffer_pool_ && "BufferPool not set in Catalog to re-init tables");
  assert(wal_mgr_ && "WAL_mgr not set in Catalog to re-init tables");
  assert(lock_manager_ && "LockManager not set in Catalog to re-init tables");
  assert(txn_manager_ &&
         "TransactionManager not set in Catalog to re-init tables");

  tables_.clear();  // Clear old table objects if any
  for (const auto& [table_id, meta] : m_schemas_)
  {
    auto heap_file = std::make_unique<HeapFile>(
        buffer_pool_, wal_mgr_, meta.first_page_id, meta.max_tuple_size);
    tables_[table_id] = std::make_unique<Table<>>(
        std::move(heap_file), table_id, meta.table_name, meta.schema,
        lock_manager_, txn_manager_);
  }
}