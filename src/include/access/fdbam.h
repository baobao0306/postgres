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

extern char *cluster_file;


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

typedef struct FDBIndexInsertDescData
{
	Relation index;
	FDBDatabaseDescData fdb_database;
} FDBIndexInsertDescData;

typedef FDBIndexInsertDescData *FDBIndexInsertDesc;


typedef struct FDBDmlState
{
	Oid relationOid;
	FDBInsertDesc insertDesc;
	FDBDeleteDesc deleteDesc;
	FDBUpdateDesc updateDesc;
	FDBIndexInsertDesc indexInsertDesc;
} FDBDmlState;


typedef struct FDBLocal
{
	FDBDmlState			   *last_used_state;
	HTAB				   *dmlDescriptorTab;

	MemoryContext			stateCxt;
	MemoryContextCallback	cb;
} FDBLocal;

extern FDBLocal fdbLocal;

typedef struct IndexFetchFDBHeapData
{
	IndexFetchTableData xs_base;
	FDBDatabaseDescData fdb_database;
} IndexFetchFDBHeapData;


extern FDBDmlState * find_dml_state(const Oid relationOid);

extern void fdb_dml_init(Relation relation, RelationPtr indexRelations,
						 int indexNum);
extern void fdb_dml_finish(Relation relation);
extern void fdb_init_connect();
extern void fdb_destroy_connect();

extern bool is_customer_table(Relation rel);

extern char* fdb_heap_make_key(RelFileNode rd_node, uint16 folk_num,
							   ItemPointerData tid);
extern ItemPointerData fdb_key_get_tid(char *key);

extern void fdb_heap_insert(Relation relation, HeapTuple tup, CommandId cid,
							int options, BulkInsertState bistate);
extern void fdb_multi_insert(Relation relation, TupleTableSlot **slots,
							 int ntuples, CommandId cid, int options,
							 BulkInsertState bistate);
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
extern FDBIndexInsertDesc fdbindex_insert_init(Relation index);
extern void fdbindex_insert_finish(FDBIndexInsertDesc desc);
extern IndexFetchTableData* fdb_index_fetch_begin(Relation rel);
extern void fdb_index_fetch_reset(IndexFetchTableData *scan);
extern void fdb_index_fetch_end(IndexFetchTableData *scan);
extern bool fdb_index_fetch_tuple(struct IndexFetchTableData *scan,
					  ItemPointer tid,
					  Snapshot snapshot,
					  TupleTableSlot *slot,
					  bool *call_again, bool *all_dead);

/* FDB visitility */
void FDBTupleSetHintBits(HeapTuple tuple, uint32 tuple_len, Relation rel,
						 FDBDatabaseDesc fdb_database, uint16 infomask,
						 TransactionId xid);
extern bool FDBTupleSatisfiesVisibility(HeapTuple tup, Snapshot snapshot,
										FDBScanDesc scan);

extern TM_Result FDBTupleSatisfiesUpdate(HeapTuple htup, CommandId curcid,
										 FDBDeleteDesc scan);
extern void fdb_clear_table(RelFileNode rd_node);
#endif /* FDBAM_H */
