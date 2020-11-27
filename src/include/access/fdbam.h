#ifndef FDBAM_H
#define FDBAM_H

#include "access/heapam.h"
#include "access/htup.h"
#include "access/relscan.h"
#include "access/sdir.h"
#include "access/tableam.h"
#include "access/tupmacs.h"
#include "access/xlogutils.h"
#include "access/xlog.h"
#include "executor/tuptable.h"
#include "nodes/primnodes.h"
#include "nodes/bitmapset.h"
#include "storage/block.h"
#include "storage/fdbaccess.h"
#include "storage/lmgr.h"
#include "utils/rel.h"
#include "utils/snapshot.h"

#define FDB_KEY_LEN 20

typedef struct FDBScanDescData
{
	TableScanDescData rs_base;

	FDBDatabase *db;
	FDBTransaction *tr;
	FDBFuture *current_future;
	FDBKeyValue const *out_kv;
	int nkv;
	int next_kv;
	bool out_more;
	HeapTupleData tuple;
} FDBScanDescData;

typedef FDBScanDescData *FDBScanDesc;

typedef struct FDBInsertDescData
{
	Relation		rel;
	FDBDatabase    *db;
	uint64 next_sequence;
	uint64 max_sequence;
} FDBInsertDescData;

typedef FDBInsertDescData *FDBInsertDesc;

void fdb_dml_init(Relation relation, CmdType operation);
void fdb_dml_finish(Relation relation, CmdType operation);

bool is_customer_table(Relation rel);

extern char* fdb_heap_make_key(Relation relation, uint16 folk_num,
							   ItemPointerData tid);
extern void fdb_heap_insert(Relation relation, HeapTuple tup, CommandId cid,
							int options, BulkInsertState bistate);
extern TableScanDesc fdb_beginscan(Relation relation, Snapshot snapshot,
			  int nkeys, ScanKey key,
			  ParallelTableScanDesc parallel_scan,
			  uint32 flags);
extern void fdb_endscan(TableScanDesc sscan);
extern bool fdb_getnextslot(TableScanDesc sscan, ScanDirection direction,
							TupleTableSlot *slot);
extern HeapTuple fdb_getnext(TableScanDesc sscan, ScanDirection direction);

/* FDB visitility */
extern bool FDBTupleSatisfiesVisibility(HeapTuple tup, Snapshot snapshot,
										FDBScanDesc scan);


#endif /* FDBAM_H */
