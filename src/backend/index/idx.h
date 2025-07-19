#ifndef SMOLDB_IDX_H
#define SMOLDB_IDX_H

#include "../access/value.h"
#include "../executor/trx.h"
#include "../storage/heapfile.h"

namespace smoldb
{

class Row;
class HeapFile;
struct RID;
class Transaction;

// Forward declare with no default
template <typename HeapFileT>
class Table;

// The key for an index will be one of the supported Value types.
using IndexKey = Value;

/**
 * @brief A pure virtual base class for all index implementations.
 */
class Index
{
 public:
  virtual ~Index() = default;

  /**
   * @brief Inserts a new entry into the index.
   * @param row The row to be indexed.
   * @param rid The RID of the new entry.
   */
  virtual void insert_entry(const Row& row, const RID& rid) = 0;

  /**
   * @brief Deletes an entry from the index.
   * @param row The row to delete from the index.
   */
  virtual void delete_entry(const Row& row) = 0;

  /**
   * @brief Retrieves the RID for a given key.
   * @param key The key to look up.
   * @param out_rid The RID is written to this parameter if found.
   * @return true if the key was found, false otherwise.
   */
  virtual bool get(const IndexKey& key, RID& out_rid) const = 0;

  /**
   * @brief Updates an index entry.
   * @param old_row State of the row prior to update.
   * @param new_row State of the row after the update.
   * @param rid The RID of the updated row.
   * @return true iff an update operation was done.
   */
  virtual bool update_entry(const Row& old_row, const Row& new_row,
                            const RID& rid) = 0;

  /**
   * @brief Populates the index from scratch by scanning a source table.
   * This is a non-transactional, system-level operation.
   * @param source_table The table to build the index from.
   */
  virtual void build(Table<>* source_table) = 0;
};

}  // namespace smoldb
#endif  // SMOLDB_IDX_H