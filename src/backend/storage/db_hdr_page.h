#ifndef SMOLDB_DB_HEADER_PAGE_H
#define SMOLDB_DB_HEADER_PAGE_H

#include "../executor/trx.h"
#include "storage.h"

// The first page of the database file is reserved for metadata.
constexpr PageID DB_HEADER_PAGE_ID = 0;

struct DBHeaderPage
{
  uint64_t next_transaction_id_;

  // Other future metadata can be added here, e.g.:
  // uint64_t next_table_id_;
  // uint64_t catalog_root_page_id_;
};

static_assert(sizeof(DBHeaderPage) <= PAGE_SIZE - sizeof(PageHeader),
              "DBHeaderPage content is too large for a single page.");
#endif //SMOLDB_DB_HEADER_PAGE_H
