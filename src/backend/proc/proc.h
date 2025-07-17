#ifndef SMOLDB_PROC_H
#define SMOLDB_PROC_H

#include <map>
#include <string>

#include "../access/value.h"

// Forward declaration to avoid circular include with proc_ctx.h
class TransactionContext;

/**
 * @brief A type-safe map for named procedure parameters.
 */
using ProcedureParams = std::map<std::string, Value>;

/**
 * @brief A type-safe map to store procedure result.
 */
using ProcedureResult = std::map<std::string, Value>;

/**
 * @brief Defines the desired outcome of a procedure's execution.
 * The ProcedureManager will honor this status to either commit or abort.
 */
enum class ProcedureStatus
{
  SUCCESS,
  ABORT
};

namespace smoldb
{
namespace backoff
{
inline std::chrono::milliseconds constant(int retry_count)
{
  return std::chrono::milliseconds(2);
}
inline std::chrono::milliseconds linear(int retry_count)
{
  return std::chrono::milliseconds(retry_count * 2);
}
// Add exponential, etc. later if needed.
}  // namespace backoff
}  // namespace smoldb

struct ProcedureOptions
{
  int max_retries = 0;
  std::function<std::chrono::milliseconds(int)> backoff_fn =
      smoldb::backoff::constant;
};

/**
 * @brief The pure virtual base class for all custom transaction logic.
 *
 * Users of SmolDB will inherit from this class to implement their specific
 * business transactions (e.g., MakeTransfer, ProcessOrder). This keeps the
 * application logic separate from the database engine's transactional core.
 */
class TransactionProcedure
{
 public:
  virtual ~TransactionProcedure() = default;

  /**
   * @brief Returns the unique, registered name of the procedure.
   */
  virtual std::string get_name() const = 0;

  /**
   * @brief The main execution logic for the business transaction.
   *
   * @param ctx A transaction-aware context providing safe access to the DB.
   * @param params A map of input parameters for the procedure.
   * @param result An output parameter to return a value to the caller.
   * @return A status indicating whether the transaction should be committed or
   * aborted.
   */
  virtual ProcedureStatus execute(TransactionContext& ctx,
                                  const ProcedureParams& params,
                                  ProcedureResult& result) = 0;
};

#endif  // SMOLDB_PROC_H