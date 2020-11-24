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
#include "storage/fdbam.h"
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

#define FDB_KEY_LEN 20

char *cluster_file = "fdb.cluster";

typedef struct FDBDmlState
{
	Oid relationOid;
	FDBHeapInsertDesc insertDesc;
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


char* fdb_heap_get_key(Relation relation, uint16 folk_num, ItemPointerData tid);
ItemPointerData fdb_sequence_to_tid(uint64 seq);

void fdb_increase_max_sequence(FDBHeapInsertDesc desc);
FDBHeapInsertDesc fdb_insert_init(Relation rel);
static FDBHeapInsertDesc get_insert_descriptor(const Relation relation);
void fdb_insert_finish(FDBHeapInsertDesc aoInsertDesc);
uint64 fdb_get_new_sequence(FDBHeapInsertDesc desc);

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

static FDBHeapInsertDesc
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


char* fdb_heap_get_key(Relation relation, uint16 folk_num, ItemPointerData tid)
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

void fdb_increase_max_sequence(FDBHeapInsertDesc desc)
{
	int value_size;
	char *sequence_value;
	uint64 next_sequence;
	uint64 max_sequence;
	bool success = false;

	ItemPointerData zero_tid;
	memset(&zero_tid, 0, sizeof(ItemPointerData));

	char *sequence_key = fdb_heap_get_key(desc->rel, 1, zero_tid);
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

	fdb_tr_destroy(tr);
	if (!success)
		elog(ERROR, "Fdb update max sequence retry over %d times", MaxRetry);
}

FDBHeapInsertDesc
fdb_insert_init(Relation rel)
{
	FDBHeapInsertDesc desc = palloc(sizeof(struct FDBHeapInsertDescData));
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
fdb_insert_finish(FDBHeapInsertDesc desc)
{
	checkError(fdb_stop_network());
	fdb_database_destroy(desc->db);
	pthread_exit(NULL);
	pfree(desc);
}

uint64
fdb_get_new_sequence(FDBHeapInsertDesc desc)
{
	uint64 result_seq;
	Assert(desc->next_sequence <= desc->max_sequence);
	if (desc->next_sequence == desc->max_sequence)
		fdb_increase_max_sequence(desc);

	result_seq = desc->next_sequence;
	desc->next_sequence++;
	return result_seq;
}

void fdb_heap_insert(Relation relation, HeapTuple tup, CommandId cid,
					 int options, BulkInsertState bistate)
{
	TransactionId xid = GetCurrentTransactionId();
	HeapTuple			heaptup;
	FDBHeapInsertDesc	desc;
	bool				all_visible_cleared = false;
	char			   *key;
	uint64				seq;

	heaptup = heap_prepare_insert(relation, tup, xid, cid, options);

	desc = get_insert_descriptor(relation);

	key = fdb_heap_get_key(relation, 0, fdb_sequence_to_tid(desc->next_sequence));

	seq = fdb_get_new_sequence(desc);

	heaptup->t_self = fdb_sequence_to_tid(seq);
	heaptup->t_data->t_ctid = heaptup->t_self;

	fdb_simple_insert(desc->db, key, FDB_KEY_LEN, (char *) heaptup->t_data,
				      heaptup->t_len);

	CacheInvalidateHeapTuple(relation, heaptup, NULL);

	pgstat_count_heap_insert(relation, 1);

	if (heaptup != tup)
	{
		tup->t_self = heaptup->t_self;
		heap_freetuple(heaptup);
	}
}

