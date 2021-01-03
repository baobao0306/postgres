#include "postgres.h"

#include "access/fdbam.h"
#include "access/fdbindex.h"
#include "access/genam.h"
#include "access/tableam.h"
#include "nodes/execnodes.h"



static void fdbindex_build_callback(Relation index,
									HeapTuple htup,
									Datum *values,
									bool *isnull,
									bool tupleIsAlive,
									void *state);

static FDBIndexInsertDesc get_fdbindex_insert_descriptor(const Relation index);


IndexBuildResult *
fdbindexbuild(Relation heap, Relation index, IndexInfo *indexInfo)
{
	IndexBuildResult *result;
	FDBIndexBuildState *buildstate;
	double reltuples;

	buildstate = fdbindex_build_init(heap, index);

	reltuples = fdbindex_heapscan(heap, index, buildstate, indexInfo);

	fdbindex_build_finish(buildstate);

	result = (IndexBuildResult *) palloc(sizeof(IndexBuildResult));

	result->heap_tuples = reltuples;

	return result;
}

double
fdbindex_heapscan(Relation heap, Relation index, FDBIndexBuildState *buildstate,
				  IndexInfo *indexInfo)
{
	double reltuples;

	if (indexInfo->ii_Unique)
	{
		/* TODO */
	}
	reltuples = table_index_build_scan(heap, index, indexInfo, true, true,
									   fdbindex_build_callback,
									   (void *) buildstate,
									   NULL);

	return reltuples;
}

char *
fdbindex_make_key(RelFileNode rd_node, char *tuple_key, int tuple_key_len)
{
	char *key = palloc(12 + tuple_key_len);

	unsigned int id_net;
	id_net = htonl(rd_node.spcNode);
	memcpy(key, &id_net, 4);
	id_net = htonl(rd_node.dbNode);
	memcpy(key + 4, &id_net, 4);
	id_net = htonl(rd_node.relNode);
	memcpy(key + 8, &id_net, 4);

	memcpy(key + 12, tuple_key, tuple_key_len);

	return key;
}

static void
fdbindex_build_callback(Relation index,
						HeapTuple htup,
						Datum *values,
						bool *isnull,
						bool tupleIsAlive,
						void *state)
{
	FDBIndexBuildState *buildstate = (FDBIndexBuildState *) state;
	char *tuple_key;
	char *fdb_key;
	unsigned int id_net;

	if (index->rd_att->natts != 1)
		elog(ERROR, "FDB index do not support multi column index.");
	if (index->rd_att->natts != 1 || index->rd_att->attrs[0].atttypid != 23)
		elog(ERROR, "FDB index now only support int4.");

	id_net = htonl(values[0]);
	tuple_key = palloc(4);
	memcpy(tuple_key, &id_net, 4);

	fdb_key = fdbindex_make_key(index->rd_node, tuple_key, 4);
	pfree(tuple_key);

	fdb_simple_insert(buildstate->fdb_database.db, fdb_key, 12 + 4,
					  (char *) &htup->t_self, 6);
	pfree(fdb_key);
}

FDBIndexBuildState *
fdbindex_build_init(Relation heap, Relation index)
{
	FDBIndexBuildState *state =  palloc(sizeof(struct FDBDeleteDescData));
	checkError(fdb_create_database(cluster_file, &state->fdb_database.db));

	state->heap = heap;

	return state;
}

void
fdbindex_build_finish(FDBIndexBuildState *state)
{
	fdb_database_destroy(state->fdb_database.db);
	pfree(state);
}

FDBIndexInsertDesc
fdbindex_insert_init(Relation index)
{
	FDBDatabase *db;

	FDBIndexInsertDesc desc = palloc(sizeof(FDBIndexInsertDescData));

	checkError(fdb_create_database(cluster_file, &db));
	desc->fdb_database.db = db;
	desc->index = index;

	return desc;
}

void fdbindex_insert_finish(FDBIndexInsertDesc desc)
{
	fdb_database_destroy(desc->fdb_database.db);

	pfree(desc);
}

static FDBIndexInsertDesc
get_fdbindex_insert_descriptor(const Relation index)
{
	struct FDBDmlState *state;

	state = find_dml_state(RelationGetRelid(index));

	if (state->indexInsertDesc == NULL)
	{
		MemoryContext oldcxt;

		oldcxt = MemoryContextSwitchTo(fdbLocal.stateCxt);

		state->indexInsertDesc = fdbindex_insert_init(index);

		MemoryContextSwitchTo(oldcxt);
	}

	return state->indexInsertDesc;
}

bool
fdbindexinsert(Relation rel, Datum *values, bool *isnull,
			   ItemPointer ht_ctid, Relation heapRel,
			   IndexUniqueCheck checkUnique,
			   IndexInfo *indexInfo)
{
	FDBIndexInsertDesc desc;
	unsigned int id_net;
	char *tuple_key;
	char *fdb_key;

	desc = get_fdbindex_insert_descriptor(rel);



	if (rel->rd_att->natts != 1)
		elog(ERROR, "FDB index do not support multi column index.");
	if (rel->rd_att->natts != 1 || rel->rd_att->attrs[0].atttypid != 23)
		elog(ERROR, "FDB index now only support int4.");

	id_net = htonl(values[0]);
	tuple_key = palloc(4);
	memcpy(tuple_key, &id_net, 4);

	fdb_key = fdbindex_make_key(rel->rd_node, tuple_key, 4);
	pfree(tuple_key);

	fdb_simple_insert(desc->fdb_database.db, fdb_key, 12 + 4,
					  (char *) ht_ctid, 6);
	pfree(fdb_key);
	return true;
}



