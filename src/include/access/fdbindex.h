#ifndef FDBINDEX_H
#define FDBINDEX_H

#include "access/genam.h"

struct IndexInfo;
typedef struct FDBIndexBuildState
{
	Relation		heap;
	Relation		index;
	FDBDatabaseDescData fdb_database;
} FDBIndexBuildState;

typedef struct FDBScanOpaqueData
{
	bool		qual_ok;		/* false if qual can never be satisfied */
	int			numberOfKeys;	/* number of preprocessed scan keys */
	ScanKey		keyData;		/* array of preprocessed scan keys */

	/* workspace for SK_SEARCHARRAY support */
	ScanKey		arrayKeyData;	/* modified copy of scan->keyData */
	int			numArrayKeys;	/* number of equality-type array keys (-1 if
								 * there are any unsatisfiable array keys) */
	int			arrayKeyCount;	/* count indicating number of array scan keys
								 * processed */
	struct BTArrayKeyInfo *arrayKeys;	/* info about each equality-type array key */
	MemoryContext arrayContext; /* scan-lifespan context for array data */

	FDBDatabaseDescData fdb_database;
	FDBFuture *current_future;
	FDBKeyValue const *out_kv;
	int nkv;
	int next_kv;
	bool out_more;
	HeapTupleData tuple;
} FDBScanOpaqueData;

typedef FDBScanOpaqueData *FDBScanOpaque;

extern IndexBuildResult * fdbindexbuild(Relation heap, Relation index,
										struct IndexInfo *indexInfo);
extern double fdbindex_heapscan(Relation heap,
								Relation index,
								FDBIndexBuildState *buildstate,
								struct IndexInfo *indexInfo);

extern FDBIndexBuildState * fdbindex_build_init(Relation heap, Relation index);
extern void fdbindex_build_finish(FDBIndexBuildState *state);
extern bool fdbindexinsert(Relation rel, Datum *values, bool *isnull,
						   ItemPointer ht_ctid, Relation heapRel,
						   IndexUniqueCheck checkUnique,
						   struct IndexInfo *indexInfo);
extern char * fdbindex_make_key(RelFileNode rd_node, TupleDesc tupleDesc,
								Datum *values, bool *isnull, Size *key_len);
extern IndexScanDesc fdbindexbeginscan(Relation rel, int nkeys, int norderbys);
extern void fdbindexendscan(IndexScanDesc scan);
extern bool fdbindexgettuple(IndexScanDesc scan, ScanDirection dir);
extern bool fdbindex_first(IndexScanDesc scan, ScanDirection dir);
extern bool fdbindex_next(IndexScanDesc scan, ScanDirection dir);
extern void fdbindexrescan(IndexScanDesc scan, ScanKey scankey, int nscankeys,
						   ScanKey orderbys, int norderbys);
#endif /* FDBINDEX_H */
