#ifndef ACCESS_H
#define ACCESS_H
#include <boost/serialization/access.hpp>
#include <boost/serialization/binary_object.hpp>
#include <boost/serialization/unordered_map.hpp>
#include <boost/serialization/variant.hpp>
#include <boost/serialization/vector.hpp>
#include <chrono>
#include <cstdint>
#include <filesystem>
#include <memory>
#include <stdexcept>
#include <string>
#include <variant>

#include "../storage/heapfile.h"
#include "../storage/storage.h"

// ---------------------------------------------------------------------
// std::chrono <-> Boost.Serialization
// ---------------------------------------------------------------------
#include <boost/serialization/split_free.hpp>

namespace boost::serialization
{

/*** duration ***/
template <class Ar, class Rep, class Period>
void save(Ar& ar, const std::chrono::duration<Rep, Period>& d,
          const unsigned /*version*/)
{
  Rep cnt = d.count();
  ar & cnt;
}

template <class Ar, class Rep, class Period>
void load(Ar& ar, std::chrono::duration<Rep, Period>& d,
          const unsigned /*version*/)
{
  Rep cnt;
  ar & cnt;
  d = std::chrono::duration<Rep, Period>(cnt);
}

template <class Ar, class Rep, class Period>
inline void serialize(Ar& ar, std::chrono::duration<Rep, Period>& d,
                      const unsigned v)
{
  split_free(ar, d, v);
}

/*** time_point ***/
template <class Ar, class Clock, class Duration>
void save(Ar& ar, const std::chrono::time_point<Clock, Duration>& tp,
          const unsigned /*version*/)
{
  Duration since_epoch = tp.time_since_epoch();
  ar & since_epoch;
}

template <class Ar, class Clock, class Duration>
void load(Ar& ar, std::chrono::time_point<Clock, Duration>& tp,
          const unsigned /*version*/)
{
  Duration since_epoch;
  ar & since_epoch;
  tp = std::chrono::time_point<Clock, Duration>(since_epoch);
}

template <class Ar, class Clock, class Duration>
inline void serialize(Ar& ar, std::chrono::time_point<Clock, Duration>& tp,
                      const unsigned v)
{
  split_free(ar, tp, v);
}

}  // namespace boost::serialization

using datetime = std::chrono::system_clock::time_point;

enum class Col_type
{
  INT,
  FLOAT,
  STRING,
  DATETIME
};

struct Column
{
  uint8_t id;
  std::string name;
  Col_type type;
  bool nullable;
  std::vector<std::byte> default_bytes;

  bool operator==(Column const& obj) const { return id == obj.id; }

 private:
  friend class boost::serialization::access;
  template <class Ar>
  void serialize(Ar& ar, unsigned)
  {
    ar & id & name & type & nullable & default_bytes;
  }
};

using Value = boost::variant<int32_t, float, std::string, datetime>;

/**
 * @brief A vector of column definition.
 */
using Schema = std::vector<Column>;

/**
 * @brief Represents a row of data in a table
 */
class Row
{
 private:
  std::vector<Value> values_;
  Schema schema_;

  friend class boost::serialization::access;
  template <class Ar>
  void serialize(Ar& ar, unsigned)
  {
    ar & values_ & schema_;
  }

 public:
  Row() = default;
  explicit Row(Schema schema) : schema_(std::move(schema))
  {
    values_.resize(schema_.size());
  }

  Row(Schema schema, std::vector<Value> values)
      : values_(std::move(values)), schema_(std::move(schema))
  {
    if (values_.size() != schema_.size())
    {
      throw std::invalid_argument("Value count doesn't match schema");
    }
  }

  // Set value by column index
  void set_value(size_t col_idx, const Value& value)
  {
    if (col_idx >= values_.size())
    {
      throw std::out_of_range("Column index out of range");
    }

    // Type checking
    const Column& col = schema_[col_idx];
    if (!is_value_compatible(value, col.type))
    {
      throw std::invalid_argument("Value type doesn't match column type");
    }

    values_[col_idx] = value;
  }

  // Set value by column name
  void set_value(const std::string& col_name, const Value& value)
  {
    auto it = std::find_if(schema_.begin(), schema_.end(),
                           [&col_name](const Column& c)
                           { return c.name == col_name; });
    if (it == schema_.end())
    {
      throw std::invalid_argument("Column not found: " + col_name);
    }

    size_t idx = std::distance(schema_.begin(), it);
    set_value(idx, value);
  }

  // Get value by column index
  const Value& get_value(size_t col_idx) const
  {
    if (col_idx >= values_.size())
    {
      throw std::out_of_range("Column index out of range");
    }
    return values_[col_idx];
  }

  // Get value by column name
  const Value& get_value(const std::string& col_name) const
  {
    auto it = std::find_if(schema_.begin(), schema_.end(),
                           [&col_name](const Column& c)
                           { return c.name == col_name; });
    if (it == schema_.end())
    {
      throw std::invalid_argument("Column not found: " + col_name);
    }

    size_t idx = std::distance(schema_.begin(), it);
    return get_value(idx);
  }

  const Schema& get_schema() const { return schema_; }
  size_t column_count() const { return values_.size(); }

  // Convert to binary format for storage
  std::vector<std::byte> to_bytes() const;

  // Create from binary format
  static Row from_bytes(const std::vector<std::byte>& data,
                        const Schema& schema);

 private:
  bool is_value_compatible(const Value& value, Col_type type) const
  {
    switch (type)
    {
      case Col_type::INT:
        return boost::get<int32_t>(&value);
      case Col_type::FLOAT:
        return boost::get<float>(&value);
      case Col_type::STRING:
        return boost::get<std::string>(&value) != nullptr;
      case Col_type::DATETIME:
        return boost::get<std::chrono::system_clock::time_point>(&value) !=
               nullptr;
      default:
        return false;
    }
  }
};

struct TableMetadata
{
  Schema schema;
  PageID first_page_id;
  std::string table_name;
  size_t max_tuple_size;

  // Default constructor for serialization
  TableMetadata() = default;

  TableMetadata(Schema s, PageID p, std::string n, size_t mts)
      : schema(std::move(s)),
        first_page_id(p),
        table_name(std::move(n)),
        max_tuple_size(mts)
  {
  }

  template <class Ar>
  void serialize(Ar& ar, unsigned)
  {
    ar & schema & first_page_id & table_name & max_tuple_size;
  }
};

/**
 * @brief Represents a database table with schema and data storage
 */
template <typename HeapFileT = HeapFile>
class Table
{
 private:
  uint8_t table_id_;
  std::string table_name_;
  Schema schema_;
  std::unique_ptr<HeapFileT> heap_file_;

  friend class boost::serialization::access;
  template <class Ar>
  void serialize(Ar& ar, unsigned)
  {
    ar & table_id_ & table_name_ & schema_;
    // Note: heap_file_ is not serialized, it's reconstructed when needed
  }

 public:
  Table() = default;

  /**
   * @brief Main ctor: injects a real or mock HeapFile
   */
  explicit Table(std::unique_ptr<HeapFileT> heap_file, uint8_t table_id = 0,
                 std::string table_name = {}, Schema schema = {})
      : table_id_(table_id),
        table_name_(std::move(table_name)),
        schema_(std::move(schema)),
        heap_file_(std::move(heap_file))
  {
  }

  // Insert a row into the table
  RID insert_row(const Row& row)
  {
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
    return heap_file_->append(row_bytes);
  }

  // Get a single row by its RID
  bool get_row(RID rid, Row& out_row) const
  {
    if (!heap_file_)
    {
      throw std::runtime_error("Table not properly initialized");
    }
    std::vector<std::byte> row_bytes;
    if (!heap_file_->get(rid, row_bytes))
    {
      return false;
    }
    out_row = Row::from_bytes(row_bytes, schema_);
    return true;
  }

  // Get all rows (full table scan)
  std::vector<Row> scan_all() const
  {
    if (!heap_file_)
    {
      throw std::runtime_error("Table not properly initialized");
    }

    auto byte_rows = heap_file_->full_scan();
    std::vector<Row> rows;
    rows.reserve(byte_rows.size());

    for (const auto& byte_row : byte_rows)
    {
      rows.push_back(Row::from_bytes(byte_row, schema_));
    }

    return rows;
  }

  // Getters
  uint8_t get_table_id() const { return table_id_; }
  const std::string& get_table_name() const { return table_name_; }
  const Schema& get_schema() const { return schema_; }
};

class Catalog
{
 private:
  std::unordered_map<uint8_t, TableMetadata> m_schemas_;
  std::unordered_map<uint8_t, std::unique_ptr<Table<>>> tables_;

  // These are not persisted; they are set at runtime by a DB engine object.
  BufferPool* buffer_pool_ = nullptr;
  WAL_mgr* wal_mgr_ = nullptr;

  friend class boost::serialization::access;
  template <class Ar>
  void serialize(Ar& ar, unsigned)
  {
    ar & m_schemas_;
  }

 public:
  Catalog() = default;

  void set_managers(BufferPool* bp, WAL_mgr* wm)
  {
    buffer_pool_ = bp;
    wal_mgr_ = wm;
  }

  // Create a new table
  void create_table(uint8_t table_id, const std::string& table_name,
                    const Schema& schema, size_t max_tuple_size = 256);

  // Get table by ID
  Table<>* get_table(uint8_t table_id);

  // Get table by name
  Table<>* get_table(const std::string& table_name);

  // List all table IDs
  std::vector<uint8_t> list_table_ids() const;

  void dump(const std::filesystem::path& path) const;
  void load(const std::filesystem::path& path);

  // Reinitialize all tables after loading from disk
  void reinit_tables();
};

#endif  // ACCESS_H