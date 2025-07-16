#ifndef SMOLDB_IDX_UNDO_H
#define SMOLDB_IDX_UNDO_H

#include "../access/access.h"
#include "../storage/heapfile.h"

class Index;

enum class IndexUndoType
{
  REVERSE_INSERT,  // An insert was performed; on abort, we must delete.
  REVERSE_DELETE   // A delete was performed; on abort, we must re-insert.
};

struct IndexUndoAction
{
  const IndexUndoType type;
  Index* index;  // Pointer to the index that was modified.
  const Row row;
  const RID rid;  // Required for re-inserting on a delete-abort.
};

#endif  // SMOLDB_IDX_UNDO_H