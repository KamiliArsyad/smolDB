#include "proc_ctx.h"

#include "../access/access.h"
#include "../executor/trx.h"

TransactionContext::TransactionContext(Transaction* txn, Catalog* catalog)
    : txn_(txn), catalog_(catalog)
{
  assert(txn_ != nullptr);
  assert(catalog_ != nullptr);
}

TransactionID TransactionContext::get_txn_id() const { return txn_->get_id(); }

Table<>* TransactionContext::get_table(const std::string& name)
{
  return catalog_->get_table(name);
}