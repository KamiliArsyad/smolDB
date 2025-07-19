#ifndef SMOLDB_PROC_CTX_H
#define SMOLDB_PROC_CTX_H

#include <string>

#include "../executor/trx_types.h"

namespace smoldb
{

// Forward declarations to minimize header dependencies.
class Catalog;
class Transaction;
class ProcedureManager;
class HeapFile;

template <typename HeapFileT>
class Table;

/**
 * @brief A safe, transaction-aware handle passed to a running procedure.
 *
 * This class acts as a "guardrail," ensuring that all database operations
 * performed by a procedure are correctly associated with the enclosing
 * transaction. It deliberately hides direct commit/abort functionality, as that
 * lifecycle is the sole responsibility of the ProcedureManager.
 */
class TransactionContext
{
 public:
  /**
   * @brief Provides safe, read-only access to the transaction's ID.
   */
  TransactionID get_txn_id() const;

  /**
   * @brief Retrieves a handle to a table for transactional DML operations.
   */
  Table<HeapFile>* get_table(const std::string& name);

 private:
  // Only the ProcedureManager can create a TransactionContext.
  friend class ProcedureManager;

  TransactionContext(Transaction* txn, Catalog* catalog);

  Transaction* txn_;
  Catalog* catalog_;
};

}  // namespace smoldb

#endif  // SMOLDB_PROC_CTX_H