#ifndef FDBAM_H
#define FDBAM_H

#include "access/htup.h"
#include "access/memtup.h"
#include "access/relscan.h"
#include "access/sdir.h"
#include "access/tableam.h"
#include "access/tupmacs.h"
#include "access/xlogutils.h"
#include "access/xlog.h"
#include "access/appendonly_visimap.h"
#include "executor/tuptable.h"
#include "nodes/primnodes.h"
#include "nodes/bitmapset.h"
#include "storage/block.h"
#include "storage/lmgr.h"
#include "utils/rel.h"
#include "utils/snapshot.h"

typedef struct FDBHeapInsertDescData
{
	Relation		rel;
	FDBDatabase    *db;
	uint64 next_sequence;
	uint64 max_sequence;
} FDBHeapInsertDescData;

typedef FDBHeapInsertDescData *FDBHeapInsertDesc;

void fdb_dml_init(Relation relation, CmdType operation);
void fdb_dml_finish(Relation relation, CmdType operation);

bool is_customer_table(Relation rel);

extern void fdb_heap_insert(Relation relation, HeapTuple tup, CommandId cid,
							int options, BulkInsertState bistate);

#endif /* FDBAM_H */
