#include "access.h"

#include <boost/archive/binary_iarchive.hpp>
#include <boost/archive/binary_oarchive.hpp>
#include <cassert>
#include <cstring>
#include <fstream>
#include <sstream>

#include "../executor/trx_mgr.h"
#include "../index/h_idx.h"
#include "../index/idx.h"
#include "../storage/mock_heapfile.h"

using namespace smoldb;

// Row implementation
std::vector<std::byte> Row::to_bytes() const
{
  size_t required_size = calculate_packed_size();
  std::vector<std::byte> buffer(required_size);
  // Zero-initialize the buffer. This is crucial for ensuring that char arrays
  // for strings are null-padded, making deserialization safer.
  std::memset(buffer.data(), 0, required_size);

  auto* current_pos = buffer.data();

  for (size_t i = 0; i < schema_.size(); ++i)
  {
    const auto& col = schema_[i];
    const auto& val = values_[i];

    switch (col.type)
    {
      case Col_type::INT:
      {
        auto v = boost::get<int32_t>(val);
        std::memcpy(current_pos, &v, sizeof(v));
        current_pos += sizeof(v);
        break;
      }
      case Col_type::FLOAT:
      {
        auto v = boost::get<float>(val);
        std::memcpy(current_pos, &v, sizeof(v));
        current_pos += sizeof(v);
        break;
      }
      case Col_type::DATETIME:
      {
        auto v = boost::get<DATETIME_TYPE>(val);
        std::memcpy(current_pos, &v, sizeof(v));
        current_pos += sizeof(v);
        break;
      }
      case Col_type::STRING:
      {
        const auto& v = boost::get<std::string>(val);
        // The buffer is already zeroed, so we don't need to worry about the
        // null terminator.
        std::memcpy(current_pos, v.c_str(), v.length());
        current_pos += col.size;
        break;
      }
    }
  }
  return buffer;
}

Row Row::from_bytes(const std::vector<std::byte>& data, const Schema& schema)
{
  Row row(schema);  // Constructs the row with a correctly sized values_ vector
  const auto* current_pos = data.data();

  for (size_t i = 0; i < schema.size(); ++i)
  {
    const auto& col = schema[i];
    Value v;

    switch (col.type)
    {
      case Col_type::INT:
      {
        int32_t val;
        std::memcpy(&val, current_pos, sizeof(val));
        v = val;
        current_pos += sizeof(val);
        break;
      }
      case Col_type::FLOAT:
      {
        float val;
        std::memcpy(&val, current_pos, sizeof(val));
        v = val;
        current_pos += sizeof(val);
        break;
      }
      case Col_type::DATETIME:
      {
        DATETIME_TYPE val;
        std::memcpy(&val, current_pos, sizeof(val));
        v = val;
        current_pos += sizeof(val);
        break;
      }
      case Col_type::STRING:
      {
        // Construct the string directly from the pointer. The underlying
        // buffer was zeroed, so this is safe even if the original string
        // was shorter than col.size.
        v = std::string(reinterpret_cast<const char*>(current_pos));
        current_pos += col.size;
        break;
      }
    }
    row.values_[i] = v;
  }

  return row;
}

size_t Row::calculate_packed_size() const
{
  size_t total_size = 0;
  for (const auto& col : schema_)
  {
    switch (col.type)
    {
      case Col_type::INT:
        total_size += sizeof(int32_t);
        break;
      case Col_type::FLOAT:
        total_size += sizeof(float);
        break;
      case Col_type::DATETIME:
        total_size += sizeof(DATETIME_TYPE);
        break;
      case Col_type::STRING:
        if (col.size == 0)
        {
          throw std::invalid_argument("STRING column '" + col.name +
                                      "' must have a non-zero size.");
        }
        total_size += col.size;
        break;
    }
  }
  return total_size;
}

/**
 * @brief Calculates the row / tuple data size of a schema.
 * @param schema Schema to calculate the row size of
 * @return The size of the row.
 */
static size_t calculate_row_size(const Schema& schema)
{
  size_t size = 0;
  for (const auto& col : schema)
  {
    if (col.type != Col_type::STRING)
      size += type_size(col.type);
    else if (col.size == 0)
      throw std::invalid_argument("String size is zero");
    else
      size += col.size;
  }

  return size;
}

template <typename HeapFileT>
void Table<HeapFileT>::Iterator::find_next()
{
  std::vector<std::byte> tuple_data;

  // Call the new HeapFile method. It will advance current_rid_ to the next
  // valid record.
  if (table_->heap_file_->get_next_tuple(current_rid_, tuple_data))
  {
    current_val_.first = current_rid_;
    current_val_.second = Row::from_bytes(tuple_data, table_->get_schema());

    // Prepare the RID for the *next* search, which should start
    // at the slot immediately following the one we just found.
    current_rid_.slot++;
  }
  else
  {
    // No more tuples were found. Mark this iterator as the end.
    current_rid_ = {table_->heap_file_->last_page_id() + 1, 0};
  }
}

template <typename HeapFileT>
Table<HeapFileT>::~Table() = default;

template <typename HeapFileT>
typename Table<HeapFileT>::Iterator Table<HeapFileT>::begin()
{
  RID start_rid = {heap_file_->first_page_id(), 0};
  Iterator it(this, start_rid);
  it.find_next();  // Prime the iterator by finding the first valid record
  return it;
}

template <typename HeapFileT>
typename Table<HeapFileT>::Iterator Table<HeapFileT>::end()
{
  // The end iterator points to a conceptual slot just after the last page
  RID end_rid = {heap_file_->last_page_id() + 1, 0};
  return Iterator(this, end_rid);
}

// Insert a row into the table
template <typename HeapFileT>
RID Table<HeapFileT>::insert_row(TransactionID txn_id, const Row& row)
{
  if (!txn_manager_)
  {
    throw std::runtime_error("TransactionManager not available in Table");
  }
  Transaction* txn = txn_manager_->get_transaction(txn_id);
  if (!txn)
  {
    throw std::runtime_error("Transaction not active");
  }

  if (!heap_file_)
  {
    throw std::runtime_error("Table not properly initialized");
  }

  // Validate row schema matches table schema
  if (row.get_schema().size() != schema_.size())
  {
    throw std::invalid_argument("Row schema doesn't match table schema");
  }

  // Convert row to bytes and store
  auto row_bytes = row.to_bytes();

  // With S2PL, we must acquire an exclusive lock on the RID *before* making
  // the modification. For an insert, the RID doesn't exist yet, so we don't
  // lock here, but the append operation itself is atomic and will generate a
  // new RID which the transaction implicitly holds an X-lock on.
  RID new_rid = heap_file_->append(txn, row_bytes);
  lock_manager_->acquire_exclusive(txn, new_rid);

  if (index_)
  {
    index_->insert_entry(row, new_rid);

    // Add the compensating action to the transaction's private undo log.
    txn->add_index_undo(
        {IndexUndoType::REVERSE_INSERT, index_.get(), row, new_rid});
  }

  txn->cache_row(new_rid, row);
  return new_rid;
}

// Explicitly instantiate the template methods for the default HeapFile type
template <typename HeapFileT>
bool Table<HeapFileT>::update_row(TransactionID txn_id, RID rid,
                                  const Row& new_row)
{
  if (!txn_manager_)
  {
    throw std::runtime_error("TransactionManager not available in Table");
  }
  Transaction* txn = txn_manager_->get_transaction(txn_id);
  if (!txn)
  {
    throw std::runtime_error("Transaction not active");
  }

  if (!heap_file_)
  {
    throw std::runtime_error("Table not properly initialized");
  }

  if (new_row.get_schema().size() != schema_.size())
  {
    throw std::invalid_argument("Row schema doesn't match table schema");
  }

  auto row_bytes = new_row.to_bytes();

  // Acquire exclusive lock before any modification
  if (!lock_manager_->acquire_exclusive(txn, rid))
  {
    // TODO: Handle lock acquisition failure (e.g., timeout)
    return false;
  }

  // Handle index update BEFORE heap file update ---
  if (index_)
  {
    // To handle a key update, we need both the old key and the new key.
    // Fetch the row state as it exists *before* this update.
    Row old_row;
    std::vector<std::byte> old_bytes;

    if (!txn->get_cached_row(rid, old_row))
    {
      if (!heap_file_->get(txn, rid, old_bytes))
      {
        return false;  // Row doesn't exist or is a tombstone.
      }
      old_row = Row::from_bytes(old_bytes, schema_);
    }

    if (index_->update_entry(old_row, new_row, rid))
    {
      // Operation success;
      txn->add_index_undo(
          {IndexUndoType::REVERSE_DELETE, index_.get(), old_row, rid});
      txn->add_index_undo(
          {IndexUndoType::REVERSE_INSERT, index_.get(), new_row, rid});
    }
  }

  txn->cache_row(rid, new_row);
  return heap_file_->update(txn, rid, row_bytes);
}

template <typename HeapFileT>
bool Table<HeapFileT>::delete_row(TransactionID txn_id, RID rid)
{
  if (!txn_manager_)
  {
    throw std::runtime_error("TransactionManager not available in Table");
  }
  Transaction* txn = txn_manager_->get_transaction(txn_id);
  if (!txn)
  {
    throw std::runtime_error("Transaction not active");
  }

  if (!heap_file_)
  {
    throw std::runtime_error("Table not properly initialized");
  }

  txn->remove_cache(rid);

  // Acquire exclusive lock before any modification
  if (!lock_manager_->acquire_exclusive(txn, rid))
  {
    // TODO: Handle lock acquisition failure (e.g., timeout)
    return false;
  }

  if (index_)
  {
    // To get the key for the index, we MUST fetch the row before it's deleted.
    std::vector<std::byte> old_bytes;
    if (!heap_file_->get(txn, rid, old_bytes))
    {
      // The row doesn't exist or is already a tombstone, so nothing to do.
      return false;
    }

    Row old_row = Row::from_bytes(old_bytes, schema_);
    index_->delete_entry(old_row);

    // Add the compensating action to the transaction's private undo log.
    txn->add_index_undo(
        {IndexUndoType::REVERSE_DELETE, index_.get(), old_row, rid});
  }

  return heap_file_->delete_row(txn, rid);
}

template <typename HeapFileT>
bool Table<HeapFileT>::get_rid_from_index(TransactionID txn_id,
                                          const Value& key, RID& out_rid) const
{
  if (!index_)
  {
    return false;
  }
  // TODO: Validate correctness;
  // The index is responsible for its own concurrency control (e.g., shared
  // lock on its internal map). From the transaction's perspective, this is a
  // read operation. The lock on the actual RID will be acquired later when the
  // caller uses this RID to call get_row or update_row.
  return index_->get(key, out_rid);
}

template <typename HeapFileT>
bool Table<HeapFileT>::get_row(TransactionID txn_id, RID rid,
                               Row& out_row) const
{
  if (!txn_manager_)
  {
    throw std::runtime_error("TransactionManager not available in Table");
  }
  Transaction* txn = txn_manager_->get_transaction(txn_id);
  if (!txn)
  {
    throw std::runtime_error("Transaction not active");
  }

  if (!heap_file_)
  {
    throw std::runtime_error("Table not properly initialized");
  }

  // If a row was previously read, this txn definitely still have the lock.
  if (txn->get_cached_row(rid, out_row)) return true;

  // Acquire lock before reading
  lock_manager_->acquire_shared(txn, rid);

  std::vector<std::byte> row_bytes;
  if (!heap_file_->get(txn, rid, row_bytes))
  {
    return false;
  }
  out_row = Row::from_bytes(row_bytes, schema_);
  txn->cache_row(rid, out_row);
  return true;
}

template <typename HeapFileT>
IndexMetadata Table<HeapFileT>::create_index(const std::string& idx_name,
                                             uint8_t key_col_id)
{
  assert(key_col_id < schema_.size() && "Column does not exist");

  index_ = std::make_unique<InMemoryHashIndex>(key_col_id);

  // This code will only be compiled if HeapFileT is the real HeapFile.
  // It will be completely removed for the MockHeapFile instantiation.
  if constexpr (std::is_same_v<HeapFileT, HeapFile>)
  {
    index_->build(this);
  }

  return {idx_name, key_col_id};
}
template <typename HeapFileT>
Index* Table<HeapFileT>::get_index() const
{
  return index_.get();
}

// Force instantiation for the default HeapFile type
template class Table<HeapFile>;
template class Table<MockHeapFile>;

// Catalog implementation
Catalog::~Catalog() = default;

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

  const size_t required_size = calculate_row_size(schema);
  if (required_size > max_tuple_size)
    throw std::invalid_argument("The required size of the row (" +
                                std::to_string(required_size) +
                                ") is greater than the maximum tuple size (" +
                                std::to_string(max_tuple_size) + ")");

  assert(buffer_pool_ && "BufferPool not set in Catalog");
  assert(wal_mgr_ && "WAL_mgr not set in Catalog");
  assert(lock_manager_ && "LockManager not set in Catalog");
  assert(txn_manager_ && "TransactionManager not set in Catalog");

  PageID first_page_id = buffer_pool_->allocate_page();
  m_schemas_.emplace(table_id, TableMetadata{schema, first_page_id, table_name,
                                             required_size});

  auto heap_file = std::make_unique<HeapFile>(buffer_pool_, wal_mgr_,
                                              first_page_id, required_size);
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

void Catalog::create_index(uint8_t table_id, uint8_t key_column_id,
                           const std::string& index_name)
{
  if (m_schemas_.find(table_id) == m_schemas_.end())
  {
    throw std::invalid_argument(
        "Cannot create index for non-existent table ID " +
        std::to_string(table_id));
  }
  if (indexes_.contains(table_id))
  {
    throw std::invalid_argument("Index for table ID " +
                                std::to_string(table_id) + " already exists.");
  }

  // For now, we only support one type of index.
  indexes_[table_id] =
      tables_[table_id]->create_index(index_name, key_column_id);
}

void Catalog::build_all_indexes()
{
  for (auto& [table_id, meta] : indexes_)
  {
    get_table(table_id)->create_index(meta.name, meta.key_col);
  }
}
