#include "proc_mgr.h"

#include "../executor/trx_mgr.h"
#include "proc_ctx.h"

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

Value ProcedureManager::execute_procedure(const std::string& proc_name,
                                          const ProcedureParams& params)
{
  auto it = procedures_.find(proc_name);
  if (it == procedures_.end())
  {
    throw std::invalid_argument("Procedure '" + proc_name + "' not found.");
  }
  TransactionProcedure* proc = it->second.get();

  TransactionID txn_id = txn_manager_->begin();
  Value result;
  bool should_abort = false;

  try
  {
    Transaction* txn = txn_manager_->get_transaction(txn_id);
    TransactionContext ctx(txn, catalog_);
    ProcedureStatus status = proc->execute(ctx, params, result);
    if (status == ProcedureStatus::ABORT)
    {
      should_abort = true;
    }
  }
  catch (...)
  {
    // If the procedure throws any exception, we must abort and re-throw.
    txn_manager_->abort(txn_id);
    throw;
  }

  // Commit or abort based on the procedure's returned status.
  if (should_abort)
  {
    txn_manager_->abort(txn_id);
  }
  else
  {
    txn_manager_->commit(txn_id);
  }

  return result;
}