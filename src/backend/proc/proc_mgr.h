#ifndef SMOLDB_PROC_MGR_H
#define SMOLDB_PROC_MGR_H

#include <memory>
#include <stdexcept>
#include <string>
#include <unordered_map>

#include "proc.h"

namespace smoldb
{
// Forward declarations
class TransactionManager;
class Catalog;

/**
 * @brief Manages the registration and transactional execution of procedures.
 *
 * This is the primary engine for running user-defined logic. It is responsible
 * for the entire transactional lifecycle (BEGIN, EXECUTE, COMMIT/ABORT),
 * ensuring atomicity for every procedure call.
 */
class ProcedureManager
{
 public:
  ProcedureManager(TransactionManager* txn_manager, Catalog* catalog);

  /**
   * @brief Registers a procedure with the engine, taking ownership of it.
   * Throws if a procedure with the same name already exists.
   */
  void register_procedure(std::unique_ptr<TransactionProcedure> proc);

  /**
   * @brief Executes a registered procedure by name within a new transaction.
   *
   * This is the main entry point for application-level database interaction.
   * It handles all transactional boilerplate, guaranteeing rollback on failure.
   *
   * @param proc_name The name of the procedure to execute.
   * @param params The parameters to pass to the procedure.
   * @param options User-configurable option e.g., for handling failures.
   * @return The named value returned by the procedure.
   */
  std::pair<ProcedureStatus, ProcedureResult> execute_procedure(
      const std::string& proc_name, const ProcedureParams& params,
      const ProcedureOptions& options = {});

 private:
  TransactionManager* txn_manager_;
  Catalog* catalog_;
  std::unordered_map<std::string, std::unique_ptr<TransactionProcedure>>
      procedures_;
};
}  // namespace smoldb

#endif  // SMOLDB_PROC_MGR_H