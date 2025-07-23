#include "proc_mgr.h"

#include <boost/asio/use_awaitable.hpp>

#include "../executor/lock_excepts.h"
#include "../executor/trx_mgr.h"
#include "proc_ctx.h"

using namespace smoldb;
namespace asio = boost::asio;

ProcedureManager::ProcedureManager(TransactionManager* txn_manager,
                                   Catalog* catalog,
                                   boost::asio::any_io_executor executor)
    : txn_manager_(txn_manager),
      catalog_(catalog),
      executor_(std::move(executor))
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

asio::awaitable<std::pair<ProcedureStatus, ProcedureResult>>
ProcedureManager::async_execute_procedure(const std::string_view proc_name,
                                          const ProcedureParams& params,
                                          const ProcedureOptions& options)
{
  auto it = procedures_.find(proc_name);
  if (it == procedures_.end())
  {
    throw std::invalid_argument("Procedure '" + std::string(proc_name) + "' not found.");
  }
  TransactionProcedure* proc = it->second.get();
  ProcedureStatus status;

  int attempts = options.max_retries + 1;
  int retry_count = 0;

  while (true)
  {
    TransactionID txn_id = txn_manager_->begin();
    ProcedureResult result;
    bool should_abort_logically = false;
    bool retryable_system_abort = false;
    std::exception_ptr pending_exc;  // capture to re‑throw later

    try
    {
      Transaction* txn = txn_manager_->get_transaction(txn_id);
      TransactionContext ctx(txn, catalog_);
      status = co_await proc->execute(ctx, params, result);

      if (status == ProcedureStatus::ABORT)
      {
        should_abort_logically = true;
      }
    }
    catch (const TransactionAbortedException&)
    {
      retryable_system_abort = true;
      pending_exc = std::current_exception();
    }
    catch (...)
    {
      pending_exc = std::current_exception();  // non‑retryable error
    }

    if (pending_exc)
    {
      co_await txn_manager_->async_abort(txn_id);

      if (retryable_system_abort)
      {
        if (++retry_count >= attempts)
          std::rethrow_exception(pending_exc);  // Out of retries

        // Asynchronous backoff
        asio::steady_timer timer(executor_);
        timer.expires_after(options.backoff_fn(retry_count));
        co_await timer.async_wait(asio::use_awaitable);

        continue;  // Go to next loop iteration to retry
      }

      // Non‑retryable error
      std::rethrow_exception(pending_exc);
    }

    // If we get here, the procedure executed without a system error.
    // Honor the logical outcome.
    if (should_abort_logically)
    {
      co_await txn_manager_->async_abort(txn_id);
    }
    else
    {
      co_await txn_manager_->async_commit(txn_id);
    }

    co_return std::pair{status, result};
  }
}