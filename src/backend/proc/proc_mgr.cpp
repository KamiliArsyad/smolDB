#include "proc_mgr.h"

#include "../executor/lock_excepts.h"
#include "../executor/trx_mgr.h"
#include "proc_ctx.h"

using namespace smoldb;

ProcedureManager::ProcedureManager(TransactionManager* txn_manager,
                                   Catalog* catalog)
    : txn_manager_(txn_manager), catalog_(catalog)
{
  assert(txn_manager_ != nullptr);
  assert(catalog_ != nullptr);
}

void ProcedureManager::register_procedure(
    std::unique_ptr<TransactionProcedure> proc)
{
  const auto& name = proc->get_name();
  if (procedures_.count(name))
  {
    throw std::invalid_argument("Procedure with name '" + name +
                                "' is already registered.");
  }
  procedures_[name] = std::move(proc);
}

ProcedureResult ProcedureManager::execute_procedure(const std::string& proc_name,
                                          const ProcedureParams& params,
                                          const ProcedureOptions& options)
{
  auto it = procedures_.find(proc_name);
  if (it == procedures_.end())
  {
    throw std::invalid_argument("Procedure '" + proc_name + "' not found.");
  }
  TransactionProcedure* proc = it->second.get();

  int attempts = options.max_retries + 1;
  int retry_count = 0;
  while (true)
  {
    TransactionID txn_id = txn_manager_->begin();
    std::map<std::string, Value> result;
    bool should_abort_logically = false;

    try
    {
      Transaction* txn = txn_manager_->get_transaction(txn_id);
      TransactionContext ctx(txn, catalog_);
      ProcedureStatus status = proc->execute(ctx, params, result);
      if (status == ProcedureStatus::ABORT)
      {
        should_abort_logically = true;
      }
    }
    catch (const TransactionAbortedException& e)
    {
      // Catch all retryable exceptions
      txn_manager_->abort(txn_id);
      if (++retry_count >= attempts) throw;  // Out of retries
      std::this_thread::sleep_for(options.backoff_fn(retry_count));
      continue;  // Go to next loop iteration to retry
    }
    catch (...)
    {
      // Non-retryable error
      txn_manager_->abort(txn_id);
      throw;
    }

    // If we get here, the procedure executed without a system error.
    // Honor the logical outcome.
    if (should_abort_logically)
    {
      txn_manager_->abort(txn_id);
    }
    else
    {
      txn_manager_->commit(txn_id);
    }
    return result;  // Success, exit the loop.
  }
}