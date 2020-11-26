#include <pthread.h>
#include "postgres.h"

#include "access/bufmask.h"
#include "access/genam.h"
#include "access/heapam.h"
#include "access/heapam_xlog.h"
#include "access/hio.h"
#include "access/multixact.h"
#include "access/parallel.h"
#include "access/relscan.h"
#include "access/sysattr.h"
#include "access/tableam.h"
#include "access/transam.h"
#include "access/tuptoaster.h"
#include "access/valid.h"
#include "access/visibilitymap.h"
#include "access/xact.h"
#include "access/xlog.h"
#include "access/xloginsert.h"
#include "access/xlogutils.h"
#include "catalog/catalog.h"
#include "miscadmin.h"
#include "pgstat.h"
#include "port/atomics.h"
#include "storage/bufmgr.h"
#include "storage/fdbaccess.h"
#include "access/fdbam.h"
#include "storage/freespace.h"
#include "storage/lmgr.h"
#include "storage/predicate.h"
#include "storage/procarray.h"
#include "storage/smgr.h"
#include "storage/spin.h"
#include "storage/standby.h"
#include "utils/datum.h"
#include "utils/inval.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/relcache.h"
#include "utils/snapmgr.h"
#include "utils/spccache.h"

#define FDB_MAX_SEQ 0x0000FFFFFFFFFFFF;

char *cluster_file = "fdb.cluster";


typedef struct FDBDmlState
{
	Oid relationOid;
	FDBInsertDesc insertDesc;
} FDBDmlState;

static void reset_state_cb(void *arg);

static struct FDBLocal
{
	FDBDmlState			   *last_used_state;
	HTAB				   *dmlDescriptorTab;

	MemoryContext			stateCxt;
	MemoryContextCallback	cb;
} fdbLocal	  = {
		.last_used_state  = NULL,
		.dmlDescriptorTab = NULL,

		.stateCxt		  = NULL,
		.cb				  = {
				.func	= reset_state_cb,
				.arg	= NULL
		},
};
static void init_dml_local_state(void);
static inline FDBDmlState * enter_dml_state(const Oid relationOid);
static inline FDBDmlState * find_dml_state(const Oid relationOid);
static inline FDBDmlState * remove_dml_state(const Oid relationOid);


ItemPointerData fdb_sequence_to_tid(uint64 seq);

void fdb_increase_max_sequence(FDBInsertDesc desc);
FDBInsertDesc fdb_insert_init(Relation rel);
static FDBInsertDesc get_insert_descriptor(const Relation relation);
void fdb_insert_finish(FDBInsertDesc aoInsertDesc);
uint64 fdb_get_new_sequence(FDBInsertDesc desc);
ItemPointerData fdb_get_new_tid(FDBInsertDesc desc);
void fdb_init_scan(FDBScanDesc scan, ScanKey key);

bool is_customer_table(Relation rel)
{
	Oid relid = RelationGetRelid(rel);
	Form_pg_class reltuple = rel->rd_rel;

	return reltuple->relkind == RELKIND_RELATION &&
		   !IsCatalogRelationOid(relid) &&
		   relid >= FirstNormalObjectId;
}

static void
init_dml_local_state(void)
{
	HASHCTL hash_ctl;

	if (!fdbLocal.dmlDescriptorTab)
	{
		Assert(fdbLocal.stateCxt == NULL);
		fdbLocal.stateCxt = AllocSetContextCreate(
				CurrentMemoryContext,
				"AppendOnly DML State Context",
				ALLOCSET_SMALL_SIZES);
		MemoryContextRegisterResetCallback(
				fdbLocal.stateCxt,
				&fdbLocal.cb);

		memset(&hash_ctl, 0, sizeof(hash_ctl));
		hash_ctl.keysize = sizeof(Oid);
		hash_ctl.entrysize = sizeof(FDBDmlState);
		hash_ctl.hcxt = fdbLocal.stateCxt;
		fdbLocal.dmlDescriptorTab =
				hash_create("AppendOnly DML state", 128, &hash_ctl,
							HASH_CONTEXT | HASH_ELEM | HASH_BLOBS);
	}
}

static inline FDBDmlState *
enter_dml_state(const Oid relationOid)
{
	FDBDmlState *state;
	bool				found;

	Assert(fdbLocal.dmlDescriptorTab);

	state = (FDBDmlState *) hash_search(
			fdbLocal.dmlDescriptorTab,
			&relationOid,
			HASH_ENTER,
			&found);

	Assert(!found);

	state->insertDesc = NULL;

	fdbLocal.last_used_state = state;
	return state;
}

static inline FDBDmlState *
find_dml_state(const Oid relationOid)
{
	FDBDmlState *state;
	Assert(fdbLocal.dmlDescriptorTab);

	if (fdbLocal.last_used_state &&
		fdbLocal.last_used_state->relationOid == relationOid)
		return fdbLocal.last_used_state;

	state = (FDBDmlState *) hash_search(
			fdbLocal.dmlDescriptorTab,
			&relationOid,
			HASH_FIND,
			NULL);

	Assert(state);

	fdbLocal.last_used_state = state;
	return state;
}

static inline FDBDmlState *
remove_dml_state(const Oid relationOid)
{
	FDBDmlState *state;
	Assert(fdbLocal.dmlDescriptorTab);

	state = (FDBDmlState *) hash_search(
			fdbLocal.dmlDescriptorTab,
			&relationOid,
			HASH_REMOVE,
			NULL);

	Assert(state);

	if (fdbLocal.last_used_state &&
		fdbLocal.last_used_state->relationOid == relationOid)
		fdbLocal.last_used_state = NULL;

	return state;
}

void
fdb_dml_init(Relation relation, CmdType operation)
{
	init_dml_local_state();
	(void) enter_dml_state(RelationGetRelid(relation));
}

void
fdb_dml_finish(Relation relation, CmdType operation)
{
	FDBDmlState *state;

	state = remove_dml_state(RelationGetRelid(relation));

	if (state->insertDesc)
	{
		Assert(state->insertDesc->aoi_rel == relation);
		fdb_insert_finish(state->insertDesc);
		state->insertDesc = NULL;
	}
}

static void
reset_state_cb(void *arg)
{
	fdbLocal.dmlDescriptorTab = NULL;
	fdbLocal.last_used_state = NULL;
	fdbLocal.stateCxt = NULL;
}

static FDBInsertDesc
get_insert_descriptor(const Relation relation)
{
	struct FDBDmlState *state;

	state = find_dml_state(RelationGetRelid(relation));

	if (state->insertDesc == NULL)
	{
		MemoryContext oldcxt;

		oldcxt = MemoryContextSwitchTo(fdbLocal.stateCxt);
		state->insertDesc = fdb_insert_init(relation);
		MemoryContextSwitchTo(oldcxt);
	}

	return state->insertDesc;
}


char* fdb_heap_make_key(Relation relation, uint16 folk_num, ItemPointerData tid)
{
	char *key = palloc0(20);
	memcpy(key, &htonl(relation->rd_node.spcNode), 4);
	memcpy(key + 4, &htonl(relation->rd_node.dbNode), 4);
	memcpy(key + 8, &htonl(relation->rd_node.relNode), 4);

	memcpy(key + 12, &htons(folk_num), 2);

	memcpy(key + 14, &htons(tid.ip_blkid.bi_hi), 4);
	memcpy(key + 16, &htons(tid.ip_blkid.bi_lo), 4);
	memcpy(key + 18, &htons(tid.ip_posid), 4);

	return key;
}

ItemPointerData fdb_sequence_to_tid(uint64 seq)
{
	ItemPointerData tid;
	tid.ip_blkid.bi_hi = (uint16) ((INT64CONST(0x0000FFFF00000000) & seq) >> 32);
	tid.ip_blkid.bi_lo = (uint16) ((INT64CONST(0x00000000FFFF0000) & seq) >> 16);
	tid.ip_posid = (uint16) ((INT64CONST(0x000000000000FFFF) & seq));

	return tid;
}

void fdb_increase_max_sequence(FDBInsertDesc desc)
{
	int value_size;
	char *sequence_value;
	uint64 next_sequence;
	uint64 max_sequence;
	bool success = false;

	ItemPointerData zero_tid;
	memset(&zero_tid, 0, sizeof(ItemPointerData));

	char *sequence_key = fdb_heap_make_key(desc->rel, 1, zero_tid);
	FDBTransaction *tr = fdb_tr_create(desc->db);

	for (int i = 0; i < MaxRetry; ++i)
	{
		sequence_value = fdb_tr_get(tr, sequence_key, FDB_KEY_LEN, &value_size);
		Assert(value_size == 8);
		memcpy(&next_sequence, sequence_value, sizeof(next_sequence));
		pfree(sequence_value);

		max_sequence = next_sequence + 100;

		fdb_tr_set(tr, sequence_key, FDB_KEY_LEN, &max_sequence, sizeof(max_sequence));
		if (fdb_tr_commit(tr))
		{
			success = true;
			desc->next_sequence = next_sequence;
			desc->max_sequence = max_sequence;
			break;
		}
	}
	pfree(sequence_key);
	fdb_tr_destroy(tr);
	if (!success)
		elog(ERROR, "Fdb update max sequence retry over %d times", MaxRetry);
}

FDBInsertDesc
fdb_insert_init(Relation rel)
{
	FDBInsertDesc desc = palloc(sizeof(struct FDBInsertDescData));
	checkError(fdb_select_api_version(FDB_API_VERSION));
	checkError(fdb_setup_network());
	pthread_t netThread;

	pthread_create(&netThread, NULL, (void *)runNetwork, NULL);
	FDBDatabase *db;

	checkError(fdb_create_database(cluster_file, &db));
	desc->db = db;
	desc->rel = rel;

	fdb_increase_max_sequence(desc);
}

void
fdb_insert_finish(FDBInsertDesc desc)
{
	checkError(fdb_stop_network());
	fdb_database_destroy(desc->db);
	pthread_exit(NULL);
	pfree(desc);
}

uint64
fdb_get_new_sequence(FDBInsertDesc desc)
{
	uint64 result_seq;
	Assert(desc->next_sequence <= desc->max_sequence);
	if (desc->next_sequence == desc->max_sequence)
		fdb_increase_max_sequence(desc);

	result_seq = desc->next_sequence;
	desc->next_sequence++;
	return result_seq;
}

ItemPointerData
fdb_get_new_tid(FDBInsertDesc desc)
{
	uint64 seq;
	ItemPointerData tid;

	seq = fdb_get_new_sequence(desc);
	tid = fdb_sequence_to_tid(seq);
	if (!ItemPointerIsValid(&tid))
	{
		seq = fdb_get_new_sequence(desc);
		tid = fdb_sequence_to_tid(seq);
	}
	return tid;
}

void fdb_heap_insert(Relation relation, HeapTuple tup, CommandId cid,
					 int options, BulkInsertState bistate)
{
	TransactionId xid = GetCurrentTransactionId();
	HeapTuple			heaptup;
	FDBInsertDesc	desc;
	bool				all_visible_cleared = false;
	char			   *key;
	uint64				seq;

	heaptup = heap_prepare_insert(relation, tup, xid, cid, options);

	desc = get_insert_descriptor(relation);


	heaptup->t_self = fdb_get_new_tid(desc);
	heaptup->t_data->t_ctid = heaptup->t_self;

	key = fdb_heap_make_key(relation, 0, heaptup->t_self);

	fdb_simple_insert(desc->db, key, FDB_KEY_LEN, (char *) heaptup->t_data,
				      heaptup->t_len);
	pfree(key);

	CacheInvalidateHeapTuple(relation, heaptup, NULL);

	pgstat_count_heap_insert(relation, 1);

	if (heaptup != tup)
	{
		tup->t_self = heaptup->t_self;
		heap_freetuple(heaptup);
	}
}

void
fdb_init_scan(FDBScanDesc scan, ScanKey key)
{
	char *start_key;
	char *end_key;
	checkError(fdb_create_database(cluster_file, &scan->db));
	scan.tr = fdb_tr_create(scan->db);

	start_key = fdb_heap_make_key(scan->rs_base.rs_rd, FDB_MAIN_FORKNUM,
							   fdb_sequence_to_tid(1));
	end_key = fdb_heap_make_key(scan->rs_base.rs_rd, FDB_MAIN_FORKNUM,
							 fdb_sequence_to_tid(FDB_MAX_SEQ));
	fdb_tr_get_kv(scan.tr, start_key, FDB_KEY_LEN, true,
				  end_key, FDB_KEY_LEN, scan->current_future,
				  &scan->out_kv, &scan->nkv);
	scan->next_kv = 0;

	if (key != NULL)
		memcpy(scan->rs_base.rs_key, key, scan->rs_base.rs_nkeys * sizeof(ScanKeyData));
}

TableScanDesc
fdb_beginscan(Relation relation, Snapshot snapshot,
			  int nkeys, ScanKey key,
			  ParallelTableScanDesc parallel_scan,
			  uint32 flags)
{
	FDBScanDesc scan;

	RelationIncrementReferenceCount(relation);

	scan = (FDBScanDesc) palloc(sizeof(struct FDBScanDescData));

	scan->rs_base.rs_rd = relation;
	scan->rs_base.rs_snapshot = snapshot;
	scan->rs_base.rs_nkeys = nkeys;
	scan->rs_base.rs_flags = flags;
	scan->rs_base.rs_parallel = parallel_scan;

	if (!(snapshot && IsMVCCSnapshot(snapshot)))
		scan->rs_base.rs_flags &= ~SO_ALLOW_PAGEMODE;

	if (scan->rs_base.rs_flags & (SO_TYPE_SEQSCAN | SO_TYPE_SAMPLESCAN))
	{
		/*
		 * Ensure a missing snapshot is noticed reliably, even if the
		 * isolation mode means predicate locking isn't performed (and
		 * therefore the snapshot isn't used here).
		 */
		Assert(snapshot);
		PredicateLockRelation(relation, snapshot);
	}

	/* we only need to set this up once */
	scan->tuple.t_tableOid = RelationGetRelid(relation);

	/*
	 * we do this here instead of in initscan() because heap_rescan also calls
	 * initscan() and we don't want to allocate memory again
	 */
	if (nkeys > 0)
		scan->rs_base.rs_key = (ScanKey) palloc(sizeof(ScanKeyData) * nkeys);
	else
		scan->rs_base.rs_key = NULL;


	fdb_init_scan(scan, key);

	return (TableScanDesc) scan;
}

void fdb_endscan(TableScanDesc sscan)
{
	FDBScanDesc scan = (FDBScanDesc) sscan;

	RelationDecrementReferenceCount(scan->rs_base.rs_rd);

	if (scan->rs_base.rs_flags & SO_TEMP_SNAPSHOT)
		UnregisterSnapshot(scan->rs_base.rs_snapshot);

	if (scan->tr)
		fdb_tr_destroy(scan->tr);
	if (scan->db)
		fdb_database_destroy(scan->db);

	pfree(scan);
}

void fdb_get_tuple(FDBScanDesc scan)
{
	Snapshot	snapshot = scan->rs_base.rs_snapshot;

	Assert(sscan->next_kv <= sscan->nkv);

	if (scan->next_kv == scan->nkv && !scan->out_more)
		return;

	if (scan->next_kv == scan->nkv && scan->out_more)
	{
		char *end_key;
		end_key = fdb_heap_make_key(
				scan->rs_base.rs_rd, 0,
				fdb_sequence_to_tid(FDB_MAX_SEQ));
		fdb_tr_get_kv(scan.tr, (char *) scan->out_kv[scan->nkv - 1].key,
					  scan->out_kv[scan->nkv - 1].key_length, false,
					  end_key, FDB_KEY_LEN, scan->current_future,
					  &scan->out_kv, &scan->nkv);

		pfree(end_key);
		scan->next_kv = 0;
	}

	if (scan->next_kv == scan->nkv)
		return;

	scan->tuple.t_data = (HeapTupleHeader) scan->out_kv[scan->next_kv].value;
	scan->tuple.t_self = scan->tuple->t_data->t_ctid;
	scan->tuple.t_tableOid = RelationGetRelid(scan->rs_base.rs_rd);
}

void fdb_get_next_tuple(FDBScanDesc scan)
{
	HeapTuple	tuple = &(scan->tuple);
	Snapshot	snapshot = scan->rs_base.rs_snapshot;
	ScanKey		key = scan->rs_base.rs_key;
	int			nkeys = scan->rs_base.rs_nkeys;
	bool		valid;

	while (true)
	{
		fdb_get_tuple(FDBScanDesc scan);

		if (tuple->t_data == NULL)
			return;

		valid = FDBTupleSatisfiesVisibility(tuple,
											snapshot,
											scan);

		if (valid && key != NULL)
			HeapKeyTest(tuple, RelationGetDescr(scan->rs_base.rs_rd),
						nkeys, key, valid);

		if (valid)
			return;
	}
}

bool fdb_getnextslot(TableScanDesc sscan, ScanDirection direction, TupleTableSlot *slot)
{
	FDBScanDesc scan = (FDBScanDesc) sscan;
	HeapTupleTableSlot *hslot = (HeapTupleTableSlot*) slot;

	fdb_get_next_tuple(scan);

	if (scan->tuple.t_data == NULL)
	{
		ExecClearTuple(slot);
		return false;
	}


	slot->tts_flags &= ~TTS_FLAG_EMPTY;
	slot->tts_nvalid = 0;
	hslot.tuple = scan->tuple;
	hslot.off = 0;
	slot->tts_tid = scan->tuple->t_self;

	return true;
}

HeapTuple fdb_getnext(TableScanDesc sscan, ScanDirection direction)
{
	FDBScanDesc scan = (FDBScanDesc) sscan;
	fdb_get_next_tuple(scan);
	if (scan->tuple.t_data == NULL)
		return NULL;

	return &scan->tuple;
}
