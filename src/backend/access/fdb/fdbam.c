#include <pthread.h>
#include <unistd.h>
#include "postgres.h"

#include "access/bufmask.h"
#include "access/fdbam.h"
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

#define FDB_MAX_SEQ 0x0000FFFFFFFFFFFF

char *cluster_file = "/etc/foundationdb/fdb.cluster";
bool connect_on = false;


typedef struct FDBDmlState
{
	Oid relationOid;
	FDBInsertDesc insertDesc;
	FDBDeleteDesc deleteDesc;
	FDBUpdateDesc updateDesc;
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

pthread_t netThread;

ItemPointerData fdb_sequence_to_tid(uint64 seq);

void fdb_increase_max_sequence(FDBInsertDesc desc);
FDBInsertDesc fdb_insert_init(Relation rel);
static FDBInsertDesc get_insert_descriptor(const Relation relation);
void fdb_insert_finish(FDBInsertDesc aoInsertDesc);
uint64 fdb_get_new_sequence(FDBInsertDesc desc);
ItemPointerData fdb_get_new_tid(FDBInsertDesc desc);
FDBDeleteDesc fdb_delete_init(Relation rel);
void fdb_delete_finish(FDBDeleteDesc desc);
static FDBDeleteDesc get_delete_descriptor(const Relation relation);

static FDBUpdateDesc get_update_descriptor(const Relation relation);
FDBUpdateDesc fdb_update_init(Relation rel);
void fdb_update_finish(FDBUpdateDesc desc);
void fdb_init_scan(FDBScanDesc scan, ScanKey key);
void fdb_get_tuple(FDBScanDesc scan);
void fdb_get_next_tuple(FDBScanDesc scan);

static const struct
{
	LOCKMODE	hwlock;
	int			lockstatus;
	int			updstatus;
}

		tupleLockExtraInfo[MaxLockTupleMode + 1] =
{
		{							/* LockTupleKeyShare */
				AccessShareLock,
				MultiXactStatusForKeyShare,
				-1						/* KeyShare does not allow updating tuples */
		},
		{							/* LockTupleShare */
				RowShareLock,
				MultiXactStatusForShare,
				-1						/* Share does not allow updating tuples */
		},
		{							/* LockTupleNoKeyExclusive */
				ExclusiveLock,
				MultiXactStatusForNoKeyUpdate,
				MultiXactStatusNoKeyUpdate
		},
		{							/* LockTupleExclusive */
				AccessExclusiveLock,
				MultiXactStatusForUpdate,
				MultiXactStatusUpdate
		}
};

/*
 * Acquire heavyweight locks on tuples, using a LockTupleMode strength value.
 * This is more readable than having every caller translate it to lock.h's
 * LOCKMODE.
 */
#define LockTupleTuplock(rel, tup, mode) \
	LockTuple((rel), (tup), tupleLockExtraInfo[mode].hwlock)
#define UnlockTupleTuplock(rel, tup, mode) \
	UnlockTuple((rel), (tup), tupleLockExtraInfo[mode].hwlock)
#define ConditionalLockTupleTuplock(rel, tup, mode) \
	ConditionalLockTuple((rel), (tup), tupleLockExtraInfo[mode].hwlock)

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
				"FDB DML State Context",
				ALLOCSET_SMALL_SIZES);
		MemoryContextRegisterResetCallback(
				fdbLocal.stateCxt,
				&fdbLocal.cb);

		memset(&hash_ctl, 0, sizeof(hash_ctl));
		hash_ctl.keysize = sizeof(Oid);
		hash_ctl.entrysize = sizeof(FDBDmlState);
		hash_ctl.hcxt = fdbLocal.stateCxt;
		fdbLocal.dmlDescriptorTab =
				hash_create("FDB DML state", 128, &hash_ctl,
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
	state->deleteDesc = NULL;
	state->updateDesc = NULL;

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
		Assert(state->insertDesc->rel == relation);
		fdb_insert_finish(state->insertDesc);
		state->insertDesc = NULL;
	}
	if (state->deleteDesc)
	{
		Assert(state->deleteDesc->rel == relation);
		fdb_delete_finish(state->deleteDesc);
		state->deleteDesc = NULL;
	}
	if (state->updateDesc)
	{
		Assert(state->updateDesc->rel == relation);
		fdb_update_finish(state->updateDesc);
		state->updateDesc = NULL;
	}
}

static void
reset_state_cb(void *arg)
{
	fdbLocal.dmlDescriptorTab = NULL;
	fdbLocal.last_used_state = NULL;
	fdbLocal.stateCxt = NULL;
}

void fdb_init_connect()
{
	if (!connect_on)
	{
		checkError(fdb_select_api_version(FDB_API_VERSION));
		checkError(fdb_setup_network());

		pthread_create(&netThread, NULL, (void *)runNetwork, NULL);
		connect_on = true;
	}

}

void fdb_destroy_connect()
{
	if (connect_on)
	{
		checkError(fdb_stop_network());
		pthread_join(netThread, NULL);
		connect_on = false;
	}

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
	unsigned int id_net;
	uint16 short_net;

	id_net = htonl(relation->rd_node.spcNode);
	memcpy(key, &id_net, 4);
	id_net = htonl(relation->rd_node.dbNode);
	memcpy(key + 4, &id_net, 4);
	id_net = htonl(relation->rd_node.relNode);
	memcpy(key + 8, &id_net, 4);

	short_net = htons(folk_num);
	memcpy(key + 12, &short_net, 2);

	short_net = htons(tid.ip_blkid.bi_hi);
	memcpy(key + 14, &short_net, 2);
	short_net = htons(tid.ip_blkid.bi_lo);
	memcpy(key + 16, &short_net, 2);
	short_net = htons(tid.ip_posid);
	memcpy(key + 18, &short_net, 2);

	return key;
}

ItemPointerData fdb_key_get_tid(char *key)
{
	ItemPointerData tid;
	uint16 short_net;
	memcpy(&short_net, key + 14, 2);
	tid.ip_blkid.bi_hi = ntohs(short_net);
	memcpy(&short_net, key + 16, 2);
	tid.ip_blkid.bi_lo = ntohs(short_net);
	memcpy(&short_net, key + 18, 2);
	tid.ip_posid = ntohs(short_net);
	return tid;
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
	uint32 value_size;
	char *sequence_value;
	uint64 next_sequence;
	uint64 max_sequence;
	bool success = false;
	char *sequence_key;
	FDBTransaction *tr;

	ItemPointerData zero_tid;
	memset(&zero_tid, 0, sizeof(ItemPointerData));

	sequence_key = fdb_heap_make_key(desc->rel, 1, zero_tid);
	tr = fdb_tr_create(desc->fdb_database.db);

	for (int i = 0; i < MaxRetry; ++i)
	{
		sequence_value = fdb_tr_get(tr, sequence_key, FDB_KEY_LEN, &value_size);
		if (sequence_value == NULL)
			next_sequence = 1;
		else
		{
			Assert(value_size == 8);
			memcpy(&next_sequence, sequence_value, sizeof(next_sequence));
			pfree(sequence_value);
		}
		max_sequence = next_sequence + 100;

		fdb_tr_set(tr, sequence_key, FDB_KEY_LEN, (char *) &max_sequence,
			 sizeof(max_sequence));
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
	FDBDatabase *db;

	FDBInsertDesc desc = palloc(sizeof(struct FDBInsertDescData));

	checkError(fdb_create_database(cluster_file, &db));
	desc->fdb_database.db = db;
	desc->rel = rel;

	fdb_increase_max_sequence(desc);

	return desc;
}

void
fdb_insert_finish(FDBInsertDesc desc)
{
	fdb_database_destroy(desc->fdb_database.db);
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
	char			   *key;

	heaptup = heap_prepare_insert(relation, tup, xid, cid, options);

	desc = get_insert_descriptor(relation);


	heaptup->t_self = fdb_get_new_tid(desc);
	heaptup->t_data->t_ctid = heaptup->t_self;

	key = fdb_heap_make_key(relation, 0, heaptup->t_self);

	fdb_simple_insert(desc->fdb_database.db, key, FDB_KEY_LEN, (char *) heaptup->t_data,
					  heaptup->t_len );
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

	checkError(fdb_create_database(cluster_file, &scan->fdb_database.db));
	scan->fdb_database.tr = fdb_tr_create(scan->fdb_database.db);
	scan->current_future = NULL;

	start_key = fdb_heap_make_key(scan->rs_base.rs_rd, FDB_MAIN_FORKNUM,
							   fdb_sequence_to_tid(1));
	end_key = fdb_heap_make_key(scan->rs_base.rs_rd, FDB_MAIN_FORKNUM,
							 fdb_sequence_to_tid(FDB_MAX_SEQ));
	fdb_tr_get_kv(scan->fdb_database.tr, start_key, FDB_KEY_LEN, true,
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

	if (scan->current_future)
		fdb_future_destroy(scan->current_future);

	if (scan->fdb_database.tr)
		fdb_tr_destroy(scan->fdb_database.tr);
	if (scan->fdb_database.db)
		fdb_database_destroy(scan->fdb_database.db);


	pfree(scan);
}

void fdb_get_tuple(FDBScanDesc scan)
{
	Assert(sscan->next_kv <= sscan->nkv);

	scan->tuple.t_data = NULL;

	if (scan->next_kv == scan->nkv && !scan->out_more)
		return;

	if (scan->next_kv == scan->nkv && scan->out_more)
	{
		char *end_key;
		end_key = fdb_heap_make_key(
				scan->rs_base.rs_rd, 0,
				fdb_sequence_to_tid(FDB_MAX_SEQ));
		fdb_tr_get_kv(scan->fdb_database.tr, (char *) scan->out_kv[scan->nkv - 1].key,
					  scan->out_kv[scan->nkv - 1].key_length, false,
					  end_key, FDB_KEY_LEN, scan->current_future,
					  &scan->out_kv, &scan->nkv);

		pfree(end_key);
		scan->next_kv = 0;
	}

	if (scan->next_kv == scan->nkv)
		return;

	scan->tuple.t_len = scan->out_kv[scan->next_kv].value_length;
	scan->tuple.t_data = (HeapTupleHeader) scan->out_kv[scan->next_kv].value;
	scan->tuple.t_self = fdb_key_get_tid((char *) scan->out_kv[scan->next_kv].key);
	scan->tuple.t_tableOid = RelationGetRelid(scan->rs_base.rs_rd);
	scan->next_kv++;
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
		fdb_get_tuple(scan);

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
	hslot->tuple = &scan->tuple;
	hslot->off = 0;
	slot->tts_tid = scan->tuple.t_self;

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

static FDBDeleteDesc
get_delete_descriptor(const Relation relation)
{
	struct FDBDmlState *state;

	state = find_dml_state(RelationGetRelid(relation));

	if (state->deleteDesc == NULL)
	{
		MemoryContext oldcxt;

		oldcxt = MemoryContextSwitchTo(fdbLocal.stateCxt);
		state->deleteDesc = fdb_delete_init(relation);
		MemoryContextSwitchTo(oldcxt);
	}

	return state->deleteDesc;
}

FDBDeleteDesc
fdb_delete_init(Relation rel)
{
	FDBDatabase *db;

	FDBDeleteDesc desc = palloc(sizeof(struct FDBDeleteDescData));

	checkError(fdb_create_database(cluster_file, &db));
	desc->fdb_database.db = db;
	desc->rel = rel;
	desc->fdb_database.tr = fdb_tr_create(db);

	return desc;
}

void
fdb_delete_finish(FDBDeleteDesc desc)
{
	fdb_tr_destroy(desc->fdb_database.tr);
	fdb_database_destroy(desc->fdb_database.db);
	pfree(desc);
}

static void
UpdateXmaxHintBits(HeapTuple tuple, uint32 tuple_len, Relation rel,
				   FDBDatabaseDesc fdb_database, TransactionId xid)
{
	Assert(TransactionIdEquals(HeapTupleHeaderGetRawXmax(tuple->t_data), xid));
	Assert(!(tuple->t_data->t_infomask & HEAP_XMAX_IS_MULTI));

	if (!(tuple->t_data->t_infomask & (HEAP_XMAX_COMMITTED | HEAP_XMAX_INVALID)))
	{
		if (!HEAP_XMAX_IS_LOCKED_ONLY(tuple->t_data->t_infomask) &&
			TransactionIdDidCommit(xid))
			FDBTupleSetHintBits(tuple, tuple_len, rel, fdb_database,
					   HEAP_XMAX_COMMITTED, xid);
		else
			FDBTupleSetHintBits(tuple, tuple_len, rel, fdb_database,
					   HEAP_XMAX_INVALID, InvalidTransactionId);
	}
}

TM_Result
fdb_delete(Relation relation, ItemPointer tid,
		   CommandId cid, Snapshot crosscheck, bool wait,
		   TM_FailureData *tmfd, bool changingPart)
{
	TM_Result	result;
	TransactionId xid = GetCurrentTransactionId();
	HeapTupleData tp;
	TransactionId new_xmax;
	uint16		new_infomask,
			new_infomask2;
	bool		have_tuple_lock = false;
	bool		iscombo;
	HeapTuple	old_key_tuple = NULL;	/* replica identity of the tuple */
	bool		old_key_copied = false;
	FDBDeleteDesc desc;
	char *key;

	Assert(ItemPointerIsValid(tid));
	if (IsInParallelMode())
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_TRANSACTION_STATE),
						errmsg("cannot delete tuples during a parallel operation")));

	desc = get_delete_descriptor(relation);
	key = fdb_heap_make_key(relation, 0, *tid);
	tp.t_data = (HeapTupleHeader) fdb_tr_get(desc->fdb_database.tr, key, FDB_KEY_LEN,
										  &tp.t_len);

	tp.t_tableOid = RelationGetRelid(relation);
	tp.t_self = *tid;

l1:
	result = FDBTupleSatisfiesUpdate(&tp, cid, desc);
	if (result == TM_Invisible)
	{
		pfree(key);
		pfree(tp.t_data);
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
						errmsg("attempted to delete invisible tuple")));
	}
	else if (result == TM_BeingModified && wait)
	{
		TransactionId xwait;
		uint16		infomask;

		/* must copy state data before unlocking buffer */
		xwait = HeapTupleHeaderGetRawXmax(tp.t_data);
		infomask = tp.t_data->t_infomask;
		if (infomask & HEAP_XMAX_IS_MULTI)
		{
			bool		current_is_member = false;

			if (DoesMultiXactIdConflict((MultiXactId) xwait, infomask,
										LockTupleExclusive, &current_is_member))
			{
				/*
				 * Acquire the lock, if necessary (but skip it when we're
				 * requesting a lock and already have one; avoids deadlock).
				 */
				if (!current_is_member)
					heap_acquire_tuplock(relation, &(tp.t_self), LockTupleExclusive,
										 LockWaitBlock, &have_tuple_lock);

				/* wait for multixact */
				MultiXactIdWait((MultiXactId) xwait, MultiXactStatusUpdate, infomask,
								relation, &(tp.t_self), XLTW_Delete,
								NULL);

				/*
				 * If xwait had just locked the tuple then some other xact
				 * could update this tuple before we get to this point.  Check
				 * for xmax change, and start over if so.
				 */
				if (xmax_infomask_changed(tp.t_data->t_infomask, infomask) ||
					!TransactionIdEquals(HeapTupleHeaderGetRawXmax(tp.t_data),
										 xwait))
					goto l1;
			}

			/*
			 * You might think the multixact is necessarily done here, but not
			 * so: it could have surviving members, namely our own xact or
			 * other subxacts of this backend.  It is legal for us to delete
			 * the tuple in either case, however (the latter case is
			 * essentially a situation of upgrading our former shared lock to
			 * exclusive).  We don't bother changing the on-disk hint bits
			 * since we are about to overwrite the xmax altogether.
			 */
		}
		else if (!TransactionIdIsCurrentTransactionId(xwait))
		{
			/*
			 * Wait for regular transaction to end; but first, acquire tuple
			 * lock.
			 */
			heap_acquire_tuplock(relation, &(tp.t_self), LockTupleExclusive,
								 LockWaitBlock, &have_tuple_lock);
			XactLockTableWait(xwait, relation, &(tp.t_self), XLTW_Delete);

			/*
			 * xwait is done, but if xwait had just locked the tuple then some
			 * other xact could update this tuple before we get to this point.
			 * Check for xmax change, and start over if so.
			 */
			if (xmax_infomask_changed(tp.t_data->t_infomask, infomask) ||
				!TransactionIdEquals(HeapTupleHeaderGetRawXmax(tp.t_data),
									 xwait))
				goto l1;

			/* Otherwise check if it committed or aborted */
			UpdateXmaxHintBits(&tp, tp.t_len, relation,
					  &desc->fdb_database, xwait);
		}

		/*
		 * We may overwrite if previous xmax aborted, or if it committed but
		 * only locked the tuple without updating it.
		 */
		if ((tp.t_data->t_infomask & HEAP_XMAX_INVALID) ||
			HEAP_XMAX_IS_LOCKED_ONLY(tp.t_data->t_infomask) ||
			HeapTupleHeaderIsOnlyLocked(tp.t_data))
			result = TM_Ok;
		else if (!ItemPointerEquals(&tp.t_self, &tp.t_data->t_ctid) ||
				 HeapTupleHeaderIndicatesMovedPartitions(tp.t_data))
			result = TM_Updated;
		else
			result = TM_Deleted;
	}

	if (result != TM_Ok)
	{
		Assert(result == TM_SelfModified ||
			   result == TM_Updated ||
			   result == TM_Deleted ||
			   result == TM_BeingModified);
		Assert(!(tp.t_data->t_infomask & HEAP_XMAX_INVALID));
		Assert(result != TM_Updated ||
			   !ItemPointerEquals(&tp.t_self, &tp.t_data->t_ctid));
		tmfd->ctid = tp.t_data->t_ctid;
		tmfd->xmax = HeapTupleHeaderGetUpdateXid(tp.t_data);
		if (result == TM_SelfModified)
			tmfd->cmax = HeapTupleHeaderGetCmax(tp.t_data);
		else
			tmfd->cmax = InvalidCommandId;
		if (have_tuple_lock)
			UnlockTupleTuplock(relation, &(tp.t_self), LockTupleExclusive);
		pfree(key);
		return result;
	}

	HeapTupleHeaderAdjustCmax(tp.t_data, &cid, &iscombo);

	MultiXactIdSetOldestMember();

	compute_new_xmax_infomask(HeapTupleHeaderGetRawXmax(tp.t_data),
							  tp.t_data->t_infomask, tp.t_data->t_infomask2,
							  xid, LockTupleExclusive, true,
							  &new_xmax, &new_infomask, &new_infomask2);

	START_CRIT_SECTION();
/* store transaction information of xact deleting the tuple */
	tp.t_data->t_infomask &= ~(HEAP_XMAX_BITS | HEAP_MOVED);
	tp.t_data->t_infomask2 &= ~HEAP_KEYS_UPDATED;
	tp.t_data->t_infomask |= new_infomask;
	tp.t_data->t_infomask2 |= new_infomask2;
	HeapTupleHeaderClearHotUpdated(tp.t_data);
	HeapTupleHeaderSetXmax(tp.t_data, new_xmax);
	HeapTupleHeaderSetCmax(tp.t_data, cid, iscombo);
	/* Make sure there is no forward chain link in t_ctid */
	tp.t_data->t_ctid = tp.t_self;

	fdb_simple_insert(desc->fdb_database.db, key, FDB_KEY_LEN, (char *) tp.t_data,
				   tp.t_len);
	pfree(key);

	/* Signal that this is actually a move into another partition */
	if (changingPart)
		HeapTupleHeaderSetMovedPartitions(tp.t_data);

	END_CRIT_SECTION();

	CacheInvalidateHeapTuple(relation, &tp, NULL);

	/*
	 * Release the lmgr tuple lock, if we had it.
	 */
	if (have_tuple_lock)
		UnlockTupleTuplock(relation, &(tp.t_self), LockTupleExclusive);

	pgstat_count_heap_delete(relation);

	if (old_key_tuple != NULL && old_key_copied)
		heap_freetuple(old_key_tuple);

	return TM_Ok;
}


static FDBUpdateDesc
get_update_descriptor(const Relation relation)
{
	struct FDBDmlState *state;

	state = find_dml_state(RelationGetRelid(relation));

	if (state->updateDesc == NULL)
	{
		MemoryContext oldcxt;

		oldcxt = MemoryContextSwitchTo(fdbLocal.stateCxt);
		state->updateDesc = fdb_update_init(relation);
		MemoryContextSwitchTo(oldcxt);
	}

	return state->updateDesc;
}

FDBUpdateDesc
fdb_update_init(Relation rel)
{
	FDBDatabase *db;

	FDBUpdateDesc desc = palloc(sizeof(struct FDBInsertDescData));

	checkError(fdb_create_database(cluster_file, &db));
	desc->fdb_database.db = db;
	desc->rel = rel;
	desc->fdb_database.tr = fdb_tr_create(db);

	fdb_increase_max_sequence(desc);

	return desc;
}

void
fdb_update_finish(FDBUpdateDesc desc)
{
	fdb_tr_destroy(desc->fdb_database.tr);
	fdb_database_destroy(desc->fdb_database.db);
	pfree(desc);
}


TM_Result
fdb_update(Relation relation, ItemPointer otid, HeapTuple newtup,
		   CommandId cid, Snapshot crosscheck, bool wait,
		   TM_FailureData *tmfd, LockTupleMode *lockmode)
{
	TM_Result	result;
	TransactionId xid = GetCurrentTransactionId();
	HeapTupleData oldtup;
	HeapTuple	heaptup;
	MultiXactStatus mxact_status;
	bool		have_tuple_lock = false;
	bool		iscombo;
	bool		use_hot_update = false;
	bool		key_intact;
	bool		checked_lockers;
	bool		locker_remains;
	TransactionId xmax_new_tuple,
			xmax_old_tuple;
	uint16		infomask_old_tuple,
			infomask2_old_tuple,
			infomask_new_tuple,
			infomask2_new_tuple;
	FDBUpdateDesc desc;
	char *old_key;
	char *new_key;

	Assert(ItemPointerIsValid(otid));

	if (IsInParallelMode())
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_TRANSACTION_STATE),
						errmsg("cannot update tuples during a parallel operation")));


	desc = get_update_descriptor(relation);

	old_key = fdb_heap_make_key(relation, 0, *otid);
	oldtup.t_data = (HeapTupleHeader) fdb_tr_get(desc->fdb_database.tr, old_key, FDB_KEY_LEN,
												 &oldtup.t_len);

	oldtup.t_tableOid = RelationGetRelid(relation);
	oldtup.t_self = *otid;

	newtup->t_tableOid = RelationGetRelid(relation);

	*lockmode = LockTupleExclusive;
	mxact_status = MultiXactStatusUpdate;
	key_intact = false;

l2:
	checked_lockers = false;
	locker_remains = false;
	result = FDBTupleSatisfiesUpdate(&oldtup, cid, desc);

	if (result == TM_Invisible)
	{
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
						errmsg("attempted to update invisible tuple")));
	}
	else if (result == TM_BeingModified && wait)
	{
		TransactionId xwait;
		uint16		infomask;
		bool		can_continue = false;

		/*
		 * XXX note that we don't consider the "no wait" case here.  This
		 * isn't a problem currently because no caller uses that case, but it
		 * should be fixed if such a caller is introduced.  It wasn't a
		 * problem previously because this code would always wait, but now
		 * that some tuple locks do not conflict with one of the lock modes we
		 * use, it is possible that this case is interesting to handle
		 * specially.
		 *
		 * This may cause failures with third-party code that calls
		 * heap_update directly.
		 */

		/* must copy state data before unlocking buffer */
		xwait = HeapTupleHeaderGetRawXmax(oldtup.t_data);
		infomask = oldtup.t_data->t_infomask;

		/*
		 * Now we have to do something about the existing locker.  If it's a
		 * multi, sleep on it; we might be awakened before it is completely
		 * gone (or even not sleep at all in some cases); we need to preserve
		 * it as locker, unless it is gone completely.
		 *
		 * If it's not a multi, we need to check for sleeping conditions
		 * before actually going to sleep.  If the update doesn't conflict
		 * with the locks, we just continue without sleeping (but making sure
		 * it is preserved).
		 *
		 * Before sleeping, we need to acquire tuple lock to establish our
		 * priority for the tuple (see heap_lock_tuple).  LockTuple will
		 * release us when we are next-in-line for the tuple.  Note we must
		 * not acquire the tuple lock until we're sure we're going to sleep;
		 * otherwise we're open for race conditions with other transactions
		 * holding the tuple lock which sleep on us.
		 *
		 * If we are forced to "start over" below, we keep the tuple lock;
		 * this arranges that we stay at the head of the line while rechecking
		 * tuple state.
		 */
		if (infomask & HEAP_XMAX_IS_MULTI)
		{
			TransactionId update_xact;
			int			remain;
			bool		current_is_member = false;

			if (DoesMultiXactIdConflict((MultiXactId) xwait, infomask,
										*lockmode, &current_is_member))
			{
				/*
				 * Acquire the lock, if necessary (but skip it when we're
				 * requesting a lock and already have one; avoids deadlock).
				 */
				if (!current_is_member)
					heap_acquire_tuplock(relation, &(oldtup.t_self), *lockmode,
										 LockWaitBlock, &have_tuple_lock);

				/* wait for multixact */
				MultiXactIdWait((MultiXactId) xwait, mxact_status, infomask,
								relation, &oldtup.t_self, XLTW_Update,
								&remain);
				checked_lockers = true;
				locker_remains = remain != 0;

				/*
				 * If xwait had just locked the tuple then some other xact
				 * could update this tuple before we get to this point.  Check
				 * for xmax change, and start over if so.
				 */
				if (xmax_infomask_changed(oldtup.t_data->t_infomask,
										  infomask) ||
					!TransactionIdEquals(HeapTupleHeaderGetRawXmax(oldtup.t_data),
										 xwait))
					goto l2;
			}

			/*
			 * Note that the multixact may not be done by now.  It could have
			 * surviving members; our own xact or other subxacts of this
			 * backend, and also any other concurrent transaction that locked
			 * the tuple with LockTupleKeyShare if we only got
			 * LockTupleNoKeyExclusive.  If this is the case, we have to be
			 * careful to mark the updated tuple with the surviving members in
			 * Xmax.
			 *
			 * Note that there could have been another update in the
			 * MultiXact. In that case, we need to check whether it committed
			 * or aborted. If it aborted we are safe to update it again;
			 * otherwise there is an update conflict, and we have to return
			 * TableTuple{Deleted, Updated} below.
			 *
			 * In the LockTupleExclusive case, we still need to preserve the
			 * surviving members: those would include the tuple locks we had
			 * before this one, which are important to keep in case this
			 * subxact aborts.
			 */
			if (!HEAP_XMAX_IS_LOCKED_ONLY(oldtup.t_data->t_infomask))
				update_xact = HeapTupleGetUpdateXid(oldtup.t_data);
			else
				update_xact = InvalidTransactionId;

			/*
			 * There was no UPDATE in the MultiXact; or it aborted. No
			 * TransactionIdIsInProgress() call needed here, since we called
			 * MultiXactIdWait() above.
			 */
			if (!TransactionIdIsValid(update_xact) ||
				TransactionIdDidAbort(update_xact))
				can_continue = true;
		}
		else if (TransactionIdIsCurrentTransactionId(xwait))
		{
			/*
			 * The only locker is ourselves; we can avoid grabbing the tuple
			 * lock here, but must preserve our locking information.
			 */
			checked_lockers = true;
			locker_remains = true;
			can_continue = true;
		}
		else if (HEAP_XMAX_IS_KEYSHR_LOCKED(infomask) && key_intact)
		{
			/*
			 * If it's just a key-share locker, and we're not changing the key
			 * columns, we don't need to wait for it to end; but we need to
			 * preserve it as locker.
			 */
			checked_lockers = true;
			locker_remains = true;
			can_continue = true;
		}
		else
		{
			/*
			 * Wait for regular transaction to end; but first, acquire tuple
			 * lock.
			 */
			heap_acquire_tuplock(relation, &(oldtup.t_self), *lockmode,
								 LockWaitBlock, &have_tuple_lock);
			XactLockTableWait(xwait, relation, &oldtup.t_self,
							  XLTW_Update);
			checked_lockers = true;

			/*
			 * xwait is done, but if xwait had just locked the tuple then some
			 * other xact could update this tuple before we get to this point.
			 * Check for xmax change, and start over if so.
			 */
			if (xmax_infomask_changed(oldtup.t_data->t_infomask, infomask) ||
				!TransactionIdEquals(xwait,
									 HeapTupleHeaderGetRawXmax(oldtup.t_data)))
				goto l2;

			/* Otherwise check if it committed or aborted */
			UpdateXmaxHintBits(&oldtup, oldtup.t_len, relation,
					  &desc->fdb_database, xwait);
			if (oldtup.t_data->t_infomask & HEAP_XMAX_INVALID)
				can_continue = true;
		}

		if (can_continue)
			result = TM_Ok;
		else if (!ItemPointerEquals(&oldtup.t_self, &oldtup.t_data->t_ctid) ||
				 HeapTupleHeaderIndicatesMovedPartitions(oldtup.t_data))
			result = TM_Updated;
		else
			result = TM_Deleted;
	}

	if (result != TM_Ok)
	{
		Assert(result == TM_SelfModified ||
			   result == TM_Updated ||
			   result == TM_Deleted ||
			   result == TM_BeingModified);
		Assert(!(oldtup.t_data->t_infomask & HEAP_XMAX_INVALID));
		Assert(result != TM_Updated ||
			   !ItemPointerEquals(&oldtup.t_self, &oldtup.t_data->t_ctid));
		tmfd->ctid = oldtup.t_data->t_ctid;
		tmfd->xmax = HeapTupleHeaderGetUpdateXid(oldtup.t_data);
		if (result == TM_SelfModified)
			tmfd->cmax = HeapTupleHeaderGetCmax(oldtup.t_data);
		else
			tmfd->cmax = InvalidCommandId;
		if (have_tuple_lock)
			UnlockTupleTuplock(relation, &(oldtup.t_self), *lockmode);
		return result;
	}

	/* Fill in transaction status data */

	/*
	 * If the tuple we're updating is locked, we need to preserve the locking
	 * info in the old tuple's Xmax.  Prepare a new Xmax value for this.
	 */
	compute_new_xmax_infomask(HeapTupleHeaderGetRawXmax(oldtup.t_data),
							  oldtup.t_data->t_infomask,
							  oldtup.t_data->t_infomask2,
							  xid, *lockmode, true,
							  &xmax_old_tuple, &infomask_old_tuple,
							  &infomask2_old_tuple);



	/*
	 * And also prepare an Xmax value for the new copy of the tuple.  If there
	 * was no xmax previously, or there was one but all lockers are now gone,
	 * then use InvalidXid; otherwise, get the xmax from the old tuple.  (In
	 * rare cases that might also be InvalidXid and yet not have the
	 * HEAP_XMAX_INVALID bit set; that's fine.)
	 */
	if ((oldtup.t_data->t_infomask & HEAP_XMAX_INVALID) ||
		HEAP_LOCKED_UPGRADED(oldtup.t_data->t_infomask) ||
		(checked_lockers && !locker_remains))
		xmax_new_tuple = InvalidTransactionId;
	else
		xmax_new_tuple = HeapTupleHeaderGetRawXmax(oldtup.t_data);

	if (!TransactionIdIsValid(xmax_new_tuple))
	{
		infomask_new_tuple = HEAP_XMAX_INVALID;
		infomask2_new_tuple = 0;
	}
	else
	{
		/*
		 * If we found a valid Xmax for the new tuple, then the infomask bits
		 * to use on the new tuple depend on what was there on the old one.
		 * Note that since we're doing an update, the only possibility is that
		 * the lockers had FOR KEY SHARE lock.
		 */
		if (oldtup.t_data->t_infomask & HEAP_XMAX_IS_MULTI)
		{
			GetMultiXactIdHintBits(xmax_new_tuple, &infomask_new_tuple,
								   &infomask2_new_tuple);
		}
		else
		{
			infomask_new_tuple = HEAP_XMAX_KEYSHR_LOCK | HEAP_XMAX_LOCK_ONLY;
			infomask2_new_tuple = 0;
		}
	}

	/*
	 * Prepare the new tuple with the appropriate initial values of Xmin and
	 * Xmax, as well as initial infomask bits as computed above.
	 */
	newtup->t_data->t_infomask &= ~(HEAP_XACT_MASK);
	newtup->t_data->t_infomask2 &= ~(HEAP2_XACT_MASK);
	HeapTupleHeaderSetXmin(newtup->t_data, xid);
	HeapTupleHeaderSetCmin(newtup->t_data, cid);
	newtup->t_data->t_infomask |= HEAP_UPDATED | infomask_new_tuple;
	newtup->t_data->t_infomask2 |= infomask2_new_tuple;
	HeapTupleHeaderSetXmax(newtup->t_data, xmax_new_tuple);

	/*
	 * Replace cid with a combo cid if necessary.  Note that we already put
	 * the plain cid into the new tuple.
	 */
	HeapTupleHeaderAdjustCmax(oldtup.t_data, &cid, &iscombo);
	heaptup = newtup;

	/* NO EREPORT(ERROR) from here till changes are logged */
	START_CRIT_SECTION();

	heaptup->t_self = fdb_get_new_tid(desc);
	heaptup->t_data->t_ctid = heaptup->t_self;

	new_key = fdb_heap_make_key(relation, 0, heaptup->t_self);

	fdb_simple_insert(desc->fdb_database.db, new_key, FDB_KEY_LEN,
				   (char *) heaptup->t_data, heaptup->t_len );
	pfree(new_key);

	/* Clear obsolete visibility flags, possibly set by ourselves above... */
	oldtup.t_data->t_infomask &= ~(HEAP_XMAX_BITS | HEAP_MOVED);
	oldtup.t_data->t_infomask2 &= ~HEAP_KEYS_UPDATED;
	/* ... and store info about transaction updating this tuple */
	Assert(TransactionIdIsValid(xmax_old_tuple));
	HeapTupleHeaderSetXmax(oldtup.t_data, xmax_old_tuple);
	oldtup.t_data->t_infomask |= infomask_old_tuple;
	oldtup.t_data->t_infomask2 |= infomask2_old_tuple;
	HeapTupleHeaderSetCmax(oldtup.t_data, cid, iscombo);

	/* record address of new tuple in t_ctid of old one */
	oldtup.t_data->t_ctid = heaptup->t_self;

	fdb_simple_insert(desc->fdb_database.db, old_key, FDB_KEY_LEN,
				   (char *) oldtup.t_data, oldtup.t_len);
	pfree(old_key);

	CacheInvalidateHeapTuple(relation, &oldtup, heaptup);

	if (have_tuple_lock)
		UnlockTupleTuplock(relation, &(oldtup.t_self), *lockmode);
	pgstat_count_heap_update(relation, use_hot_update);

	if (heaptup != newtup)
	{
		newtup->t_self = heaptup->t_self;
		heap_freetuple(heaptup);
	}

	return TM_Ok;
}
