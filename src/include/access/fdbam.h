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

typedef struct FDBDatabaseDescData
{
	FDBDatabase    *db;
	FDBTransaction *tr;
} FDBDatabaseDescData;

typedef FDBDatabaseDescData *FDBDatabaseDesc;

typedef struct FDBScanDescData
{
	TableScanDescData rs_base;

	FDBDatabaseDescData fdb_database;
	FDBFuture *current_future;
	FDBKeyValue const *out_kv;
	int nkv;
	int next_kv;
	bool out_more;
	HeapTupleData tuple;
} FDBScanDescData;

typedef FDBScanDescData *FDBScanDesc;

typedef struct FDBAccessDescData
{
	Relation		rel;
	FDBDatabaseDescData fdb_database;
	uint64 next_sequence;
	uint64 max_sequence;
} FDBAccessDescData;

typedef FDBAccessDescData *FDBAccessDesc;

typedef struct FDBInsertDescData
{
	Relation		rel;
	FDBDatabaseDescData fdb_database;
	uint64 next_sequence;
	uint64 max_sequence;
} FDBInsertDescData;

typedef FDBInsertDescData *FDBInsertDesc;

typedef struct FDBDeleteDescData
{
	Relation		rel;
	FDBDatabaseDescData fdb_database;
} FDBDeleteDescData;

typedef FDBDeleteDescData *FDBDeleteDesc;


typedef FDBInsertDescData *FDBUpdateDesc;

extern void fdb_dml_init(Relation relation, CmdType operation);
extern void fdb_dml_finish(Relation relation, CmdType operation);
extern void fdb_init_connect();
extern void fdb_destroy_connect();

extern bool is_customer_table(Relation rel);

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
extern TM_Result fdb_delete(Relation relation, ItemPointer tid,
							CommandId cid, Snapshot crosscheck, bool wait,
							TM_FailureData *tmfd, bool changingPart);
extern TM_Result fdb_update(Relation relation, ItemPointer otid, HeapTuple newtup,
		   CommandId cid, Snapshot crosscheck, bool wait,
		   TM_FailureData *tmfd, LockTupleMode *lockmode);
/* FDB visitility */
void FDBTupleSetHintBits(HeapTupleHeader tuple, uint32 tuple_len, Relation rel,
						 FDBDatabaseDesc fdb_database, uint16 infomask,
						 TransactionId xid);
extern bool FDBTupleSatisfiesVisibility(HeapTuple tup, Snapshot snapshot,
										FDBScanDesc scan);

extern TM_Result FDBTupleSatisfiesUpdate(HeapTuple htup, CommandId curcid,
										 FDBDeleteDesc scan);
#endif /* FDBAM_H */
