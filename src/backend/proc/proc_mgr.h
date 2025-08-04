#ifndef SMOLDB_PROC_MGR_H
#define SMOLDB_PROC_MGR_H

#include <boost/asio/any_io_executor.hpp>
#include <boost/asio/awaitable.hpp>
#include <boost/asio/steady_timer.hpp>
#include <memory>
#include <stdexcept>
#include <string>
#include <string_view>
#include <unordered_map>

#include "proc.h"

struct SvHash
{
  using is_transparent = void;
  size_t operator()(std::string_view sv) const noexcept
  {
    return std::hash<std::string_view>{}(sv);
  }
  size_t operator()(const std::string& s) const noexcept
  {
    return std::hash<std::string_view>{}(s);
  }
};

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
  ProcedureManager(TransactionManager* txn_manager, Catalog* catalog,
                   boost::asio::any_io_executor executor);

  /**
   * @brief Registers a procedure with the engine, taking ownership of it.
   * Throws if a procedure with the same name already exists.
   */
  void register_procedure(std::unique_ptr<TransactionProcedure> proc);

  /**
   * @brief Asynchronously executes a registered procedure by name within a new
   * transaction.
   *
   * This is the main entry point for application-level database interaction.
   * It handles all transactional boilerplate, guaranteeing rollback on failure.
   *
   * @param proc_name The name of the procedure to execute.
   * @param params The parameters to pass to the procedure.
   * @param options User-configurable option e.g., for handling failures.
   * @return An awaitable that resolves to the procedure's status and result.
   */
  boost::asio::awaitable<std::pair<ProcedureStatus, ProcedureResult>>
  async_execute_procedure(std::string_view proc_name,
                          const ProcedureParams& params,
                          const ProcedureOptions& options = {});

  /**
   * @brief Returns the executor this manager uses to run coroutines.
   */
  boost::asio::any_io_executor get_executor() const { return executor_; }

 private:
  TransactionManager* txn_manager_;
  Catalog* catalog_;
  boost::asio::any_io_executor executor_;
  std::unordered_map<std::string, std::unique_ptr<TransactionProcedure>, SvHash,
                     std::equal_to<>>
      procedures_;
};
}  // namespace smoldb

#endif  // SMOLDB_PROC_MGR_H