#include "postgres.h"

#include "access/fdbam.h"
#include "access/fdbindex.h"
#include "access/genam.h"
#include "access/nbtree.h"
#include "access/tableam.h"
#include "nodes/execnodes.h"
#include "pgstat.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"



static void fdbindex_build_callback(Relation index,
									HeapTuple htup,
									Datum *values,
									bool *isnull,
									bool tupleIsAlive,
									void *state);

static FDBIndexInsertDesc get_fdbindex_insert_descriptor(const Relation index);
bool fdb_end_point(IndexScanDesc scan, ScanDirection dir);


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

IndexScanDesc fdbindexbeginscan(Relation rel, int nkeys, int norderbys)
{
	IndexScanDesc scan;
	FDBScanOpaque so;

	/* no order by operators allowed */
	Assert(norderbys == 0);

	/* get the scan */
	scan = RelationGetIndexScan(rel, nkeys, norderbys);

	/* allocate private workspace */
	so = (FDBScanOpaque) palloc(sizeof(FDBScanOpaqueData));

	if (scan->numberOfKeys > 0)
		so->keyData = (ScanKey) palloc(scan->numberOfKeys * sizeof(ScanKeyData));
	else
		so->keyData = NULL;

	checkError(fdb_create_database(cluster_file, &so->fdb_database.db));
	so->fdb_database.tr = fdb_tr_create(so->fdb_database.db);
	so->current_future = NULL;
	so->out_kv = NULL;
	so->nkv = 0;
	so->next_kv = 0;
	so->out_more = false;
	scan->opaque = so;

	scan->xs_itupdesc = RelationGetDescr(rel);

	return (IndexScanDesc) scan;
}

void
fdbindexendscan(IndexScanDesc scan)
{
	FDBScanOpaque so = (FDBScanOpaque) scan->opaque;

	/* so->markTuples should not be pfree'd, see btrescan */
	if (so->current_future)
		fdb_future_destroy(so->current_future);

	if (so->fdb_database.tr)
		fdb_tr_destroy(so->fdb_database.tr);
	if (so->fdb_database.db)
		fdb_database_destroy(so->fdb_database.db);
	pfree(so);
}

bool
fdbindexgettuple(IndexScanDesc scan, ScanDirection dir)
{
	bool		res;
	BTScanOpaque so = (BTScanOpaque) scan.opaque;

	if (so->numArrayKeys && !BTScanPosIsValid(so->currPos))
	{
		/* TODO */
		elog(ERROR, "fdb index do not support array keys now.");
	}
	/* This loop handles advancing to the next array elements, if any */
	do
	{
		/*
		 * If we've already initialized this scan, we can just advance it in
		 * the appropriate direction.  If we haven't done so yet, we call
		 * _bt_first() to get the first item in the scan.
		 */
		if (!BTScanPosIsValid(so->currPos))
			res = _bt_first(scan, dir);
		else
		{
			/*
			 * Check to see if we should kill the previously-fetched tuple.
			 */
			if (scan->kill_prior_tuple)
			{
				/*
				 * Yes, remember it for later. (We'll deal with all such
				 * tuples at once right before leaving the index page.)  The
				 * test for numKilled overrun is not just paranoia: if the
				 * caller reverses direction in the indexscan then the same
				 * item might get entered multiple times. It's not worth
				 * trying to optimize that, so we don't detect it, but instead
				 * just forget any excess entries.
				 */
				if (so->killedItems == NULL)
					so->killedItems = (int *)
							palloc(MaxIndexTuplesPerPage * sizeof(int));
				if (so->numKilled < MaxIndexTuplesPerPage)
					so->killedItems[so->numKilled++] = so->currPos.itemIndex;
			}

			/*
			 * Now continue the scan.
			 */
			res = _bt_next(scan, dir);
		}

		/* If we have a tuple, return it ... */
		if (res)
			break;
		/* ... otherwise see if we have more array keys to deal with */
	} while (so->numArrayKeys && _bt_advance_array_keys(scan, dir));
}

bool
fdbindex_first(IndexScanDesc scan, ScanDirection dir)
{
	Relation	rel = scan->indexRelation;
	FDBScanOpaque so = (FDBScanOpaque) scan->opaque;
	Buffer		buf;
	BTStack		stack;
	OffsetNumber offnum;
	StrategyNumber strat;
	bool		nextkey;
	bool		goback;
	BTScanInsertData inskey;
	ScanKey		startKeys[INDEX_MAX_KEYS];
	ScanKeyData notnullkeys[INDEX_MAX_KEYS];
	int			keysCount = 0;
	int			i;
	bool		status = true;
	StrategyNumber strat_total;
	BTScanPosItem *currItem;
	BlockNumber blkno;
	char 	   *fdb_start_key;
	char 	   *start_tuple_key;
	char 	   *fdb_end_key;
	char 	   *end_tuple_key;
	unsigned int id_net;

	Assert(!BTScanPosIsValid(so->currPos));

	pgstat_count_index_scan(rel);

	/*
	 * Examine the scan keys and eliminate any redundant keys; also mark the
	 * keys that must be matched to continue the scan.
	 */
	_bt_preprocess_keys(scan);

	/*
	 * Quit now if _bt_preprocess_keys() discovered that the scan keys can
	 * never be satisfied (eg, x == 1 AND x > 2).
	 */
	if (!so->qual_ok)
	{
		/* Notify any other workers that we're done with this scan key. */
		_bt_parallel_done(scan);
		return false;
	}

	/*
	 * For parallel scans, get the starting page from shared state. If the
	 * scan has not started, proceed to find out first leaf page in the usual
	 * way while keeping other participating processes waiting.  If the scan
	 * has already begun, use the page number from the shared structure.
	 */
	if (scan->parallel_scan != NULL)
	{
		elog(ERROR, "fdb do not support parallel scan now.");
	}

	/*----------
	 * Examine the scan keys to discover where we need to start the scan.
	 *
	 * We want to identify the keys that can be used as starting boundaries;
	 * these are =, >, or >= keys for a forward scan or =, <, <= keys for
	 * a backwards scan.  We can use keys for multiple attributes so long as
	 * the prior attributes had only =, >= (resp. =, <=) keys.  Once we accept
	 * a > or < boundary or find an attribute with no boundary (which can be
	 * thought of as the same as "> -infinity"), we can't use keys for any
	 * attributes to its right, because it would break our simplistic notion
	 * of what initial positioning strategy to use.
	 *
	 * When the scan keys include cross-type operators, _bt_preprocess_keys
	 * may not be able to eliminate redundant keys; in such cases we will
	 * arbitrarily pick a usable one for each attribute.  This is correct
	 * but possibly not optimal behavior.  (For example, with keys like
	 * "x >= 4 AND x >= 5" we would elect to scan starting at x=4 when
	 * x=5 would be more efficient.)  Since the situation only arises given
	 * a poorly-worded query plus an incomplete opfamily, live with it.
	 *
	 * When both equality and inequality keys appear for a single attribute
	 * (again, only possible when cross-type operators appear), we *must*
	 * select one of the equality keys for the starting point, because
	 * _bt_checkkeys() will stop the scan as soon as an equality qual fails.
	 * For example, if we have keys like "x >= 4 AND x = 10" and we elect to
	 * start at x=4, we will fail and stop before reaching x=10.  If multiple
	 * equality quals survive preprocessing, however, it doesn't matter which
	 * one we use --- by definition, they are either redundant or
	 * contradictory.
	 *
	 * Any regular (not SK_SEARCHNULL) key implies a NOT NULL qualifier.
	 * If the index stores nulls at the end of the index we'll be starting
	 * from, and we have no boundary key for the column (which means the key
	 * we deduced NOT NULL from is an inequality key that constrains the other
	 * end of the index), then we cons up an explicit SK_SEARCHNOTNULL key to
	 * use as a boundary key.  If we didn't do this, we might find ourselves
	 * traversing a lot of null entries at the start of the scan.
	 *
	 * In this loop, row-comparison keys are treated the same as keys on their
	 * first (leftmost) columns.  We'll add on lower-order columns of the row
	 * comparison below, if possible.
	 *
	 * The selected scan keys (at most one per index column) are remembered by
	 * storing their addresses into the local startKeys[] array.
	 *----------
	 */
	strat_total = BTEqualStrategyNumber;
	if (so->numberOfKeys > 0)
	{
		AttrNumber	curattr;
		ScanKey		chosen;
		ScanKey		impliesNN;
		ScanKey		cur;

		/*
		 * chosen is the so-far-chosen key for the current attribute, if any.
		 * We don't cast the decision in stone until we reach keys for the
		 * next attribute.
		 */
		curattr = 1;
		chosen = NULL;
		/* Also remember any scankey that implies a NOT NULL constraint */
		impliesNN = NULL;

		/*
		 * Loop iterates from 0 to numberOfKeys inclusive; we use the last
		 * pass to handle after-last-key processing.  Actual exit from the
		 * loop is at one of the "break" statements below.
		 */
		for (cur = so->keyData, i = 0;; cur++, i++)
		{
			if (i >= so->numberOfKeys || cur->sk_attno != curattr)
			{
				/*
				 * Done looking at keys for curattr.  If we didn't find a
				 * usable boundary key, see if we can deduce a NOT NULL key.
				 */
				if (chosen == NULL && impliesNN != NULL &&
					((impliesNN->sk_flags & SK_BT_NULLS_FIRST) ?
					 ScanDirectionIsForward(dir) :
					 ScanDirectionIsBackward(dir)))
				{
					/* Yes, so build the key in notnullkeys[keysCount] */
					chosen = &notnullkeys[keysCount];
					ScanKeyEntryInitialize(chosen,
										   (SK_SEARCHNOTNULL | SK_ISNULL |
											(impliesNN->sk_flags &
											 (SK_BT_DESC | SK_BT_NULLS_FIRST))),
										   curattr,
										   ((impliesNN->sk_flags & SK_BT_NULLS_FIRST) ?
											BTGreaterStrategyNumber :
											BTLessStrategyNumber),
										   InvalidOid,
										   InvalidOid,
										   InvalidOid,
										   (Datum) 0);
				}

				/*
				 * If we still didn't find a usable boundary key, quit; else
				 * save the boundary key pointer in startKeys.
				 */
				if (chosen == NULL)
					break;
				startKeys[keysCount++] = chosen;

				/*
				 * Adjust strat_total, and quit if we have stored a > or <
				 * key.
				 */
				strat = chosen->sk_strategy;
				if (strat != BTEqualStrategyNumber)
				{
					strat_total = strat;
					if (strat == BTGreaterStrategyNumber ||
						strat == BTLessStrategyNumber)
						break;
				}

				/*
				 * Done if that was the last attribute, or if next key is not
				 * in sequence (implying no boundary key is available for the
				 * next attribute).
				 */
				if (i >= so->numberOfKeys ||
					cur->sk_attno != curattr + 1)
					break;

				/*
				 * Reset for next attr.
				 */
				curattr = cur->sk_attno;
				chosen = NULL;
				impliesNN = NULL;
			}

			/*
			 * Can we use this key as a starting boundary for this attr?
			 *
			 * If not, does it imply a NOT NULL constraint?  (Because
			 * SK_SEARCHNULL keys are always assigned BTEqualStrategyNumber,
			 * *any* inequality key works for that; we need not test.)
			 */
			switch (cur->sk_strategy)
			{
				case BTLessStrategyNumber:
				case BTLessEqualStrategyNumber:
					if (chosen == NULL)
					{
						if (ScanDirectionIsBackward(dir))
							chosen = cur;
						else
							impliesNN = cur;
					}
					break;
				case BTEqualStrategyNumber:
					/* override any non-equality choice */
					chosen = cur;
					break;
				case BTGreaterEqualStrategyNumber:
				case BTGreaterStrategyNumber:
					if (chosen == NULL)
					{
						if (ScanDirectionIsForward(dir))
							chosen = cur;
						else
							impliesNN = cur;
					}
					break;
			}
		}
	}

	/*
	 * If we found no usable boundary keys, we have to start from one end of
	 * the tree.  Walk down that edge to the first or last key, and scan from
	 * there.
	 */
	if (keysCount == 0)
	{
		bool		match;

		match = fdb_end_point(scan, dir);

		return match;
	}

	/*
	 * We want to start the scan somewhere within the index.  Set up an
	 * insertion scankey we can use to search for the boundary point we
	 * identified above.  The insertion scankey is built using the keys
	 * identified by startKeys[].  (Remaining insertion scankey fields are
	 * initialized after initial-positioning strategy is finalized.)
	 */
	Assert(keysCount <= INDEX_MAX_KEYS);
	for (i = 0; i < keysCount; i++)
	{
		ScanKey		cur = startKeys[i];

		Assert(cur->sk_attno == i + 1);

		if (cur->sk_flags & SK_ROW_HEADER)
		{
			/*
			 * Row comparison header: look to the first row member instead.
			 *
			 * The member scankeys are already in insertion format (ie, they
			 * have sk_func = 3-way-comparison function), but we have to watch
			 * out for nulls, which _bt_preprocess_keys didn't check. A null
			 * in the first row member makes the condition unmatchable, just
			 * like qual_ok = false.
			 */
			ScanKey		subkey = (ScanKey) DatumGetPointer(cur->sk_argument);

			Assert(subkey->sk_flags & SK_ROW_MEMBER);
			if (subkey->sk_flags & SK_ISNULL)
			{
				_bt_parallel_done(scan);
				return false;
			}
			memcpy(inskey.scankeys + i, subkey, sizeof(ScanKeyData));

			/*
			 * If the row comparison is the last positioning key we accepted,
			 * try to add additional keys from the lower-order row members.
			 * (If we accepted independent conditions on additional index
			 * columns, we use those instead --- doesn't seem worth trying to
			 * determine which is more restrictive.)  Note that this is OK
			 * even if the row comparison is of ">" or "<" type, because the
			 * condition applied to all but the last row member is effectively
			 * ">=" or "<=", and so the extra keys don't break the positioning
			 * scheme.  But, by the same token, if we aren't able to use all
			 * the row members, then the part of the row comparison that we
			 * did use has to be treated as just a ">=" or "<=" condition, and
			 * so we'd better adjust strat_total accordingly.
			 */
			if (i == keysCount - 1)
			{
				bool		used_all_subkeys = false;

				Assert(!(subkey->sk_flags & SK_ROW_END));
				for (;;)
				{
					subkey++;
					Assert(subkey->sk_flags & SK_ROW_MEMBER);
					if (subkey->sk_attno != keysCount + 1)
						break;	/* out-of-sequence, can't use it */
					if (subkey->sk_strategy != cur->sk_strategy)
						break;	/* wrong direction, can't use it */
					if (subkey->sk_flags & SK_ISNULL)
						break;	/* can't use null keys */
					Assert(keysCount < INDEX_MAX_KEYS);
					memcpy(inskey.scankeys + keysCount, subkey,
						   sizeof(ScanKeyData));
					keysCount++;
					if (subkey->sk_flags & SK_ROW_END)
					{
						used_all_subkeys = true;
						break;
					}
				}
				if (!used_all_subkeys)
				{
					switch (strat_total)
					{
						case BTLessStrategyNumber:
							strat_total = BTLessEqualStrategyNumber;
							break;
						case BTGreaterStrategyNumber:
							strat_total = BTGreaterEqualStrategyNumber;
							break;
					}
				}
				break;			/* done with outer loop */
			}
		}
		else
		{
			/*
			 * Ordinary comparison key.  Transform the search-style scan key
			 * to an insertion scan key by replacing the sk_func with the
			 * appropriate btree comparison function.
			 *
			 * If scankey operator is not a cross-type comparison, we can use
			 * the cached comparison function; otherwise gotta look it up in
			 * the catalogs.  (That can't lead to infinite recursion, since no
			 * indexscan initiated by syscache lookup will use cross-data-type
			 * operators.)
			 *
			 * We support the convention that sk_subtype == InvalidOid means
			 * the opclass input type; this is a hack to simplify life for
			 * ScanKeyInit().
			 */
			if (cur->sk_subtype == rel->rd_opcintype[i] ||
				cur->sk_subtype == InvalidOid)
			{
				FmgrInfo   *procinfo;

				procinfo = index_getprocinfo(rel, cur->sk_attno, BTORDER_PROC);
				ScanKeyEntryInitializeWithInfo(inskey.scankeys + i,
											   cur->sk_flags,
											   cur->sk_attno,
											   InvalidStrategy,
											   cur->sk_subtype,
											   cur->sk_collation,
											   procinfo,
											   cur->sk_argument);
			}
			else
			{
				RegProcedure cmp_proc;

				cmp_proc = get_opfamily_proc(rel->rd_opfamily[i],
											 rel->rd_opcintype[i],
											 cur->sk_subtype,
											 BTORDER_PROC);
				if (!RegProcedureIsValid(cmp_proc))
					elog(ERROR, "missing support function %d(%u,%u) for attribute %d of index \"%s\"",
						 BTORDER_PROC, rel->rd_opcintype[i], cur->sk_subtype,
						 cur->sk_attno, RelationGetRelationName(rel));
				ScanKeyEntryInitialize(inskey.scankeys + i,
									   cur->sk_flags,
									   cur->sk_attno,
									   InvalidStrategy,
									   cur->sk_subtype,
									   cur->sk_collation,
									   cmp_proc,
									   cur->sk_argument);
			}
		}
	}

	/*----------
	 * Examine the selected initial-positioning strategy to determine exactly
	 * where we need to start the scan, and set flag variables to control the
	 * code below.
	 *
	 * If nextkey = false, _bt_search and _bt_binsrch will locate the first
	 * item >= scan key.  If nextkey = true, they will locate the first
	 * item > scan key.
	 *
	 * If goback = true, we will then step back one item, while if
	 * goback = false, we will start the scan on the located item.
	 *----------
	 */
	switch (strat_total)
	{
		case BTLessStrategyNumber:

			/*
			 * Find first item >= scankey, then back up one to arrive at last
			 * item < scankey.  (Note: this positioning strategy is only used
			 * for a backward scan, so that is always the correct starting
			 * position.)
			 */
			nextkey = false;
			goback = true;
			break;

		case BTLessEqualStrategyNumber:

			/*
			 * Find first item > scankey, then back up one to arrive at last
			 * item <= scankey.  (Note: this positioning strategy is only used
			 * for a backward scan, so that is always the correct starting
			 * position.)
			 */
			nextkey = true;
			goback = true;
			break;

		case BTEqualStrategyNumber:

			/*
			 * If a backward scan was specified, need to start with last equal
			 * item not first one.
			 */
			if (ScanDirectionIsBackward(dir))
			{
				/*
				 * This is the same as the <= strategy.  We will check at the
				 * end whether the found item is actually =.
				 */
				nextkey = true;
				goback = true;
			}
			else
			{
				/*
				 * This is the same as the >= strategy.  We will check at the
				 * end whether the found item is actually =.
				 */
				nextkey = false;
				goback = false;
			}
			break;

		case BTGreaterEqualStrategyNumber:

			/*
			 * Find first item >= scankey.  (This is only used for forward
			 * scans.)
			 */
			nextkey = false;
			goback = false;
			break;

		case BTGreaterStrategyNumber:

			/*
			 * Find first item > scankey.  (This is only used for forward
			 * scans.)
			 */
			nextkey = true;
			goback = false;
			break;

		default:
			/* can't get here, but keep compiler quiet */
			elog(ERROR, "unrecognized strat_total: %d", (int) strat_total);
			return false;
	}

	/* Initialize remaining insertion scan key fields */
	inskey.heapkeyspace = _bt_heapkeyspace(rel);
	inskey.anynullkeys = false; /* unused */
	inskey.nextkey = nextkey;
	inskey.pivotsearch = false;
	inskey.scantid = NULL;
	inskey.keysz = keysCount;

	if (inskey.keysz != 0)
		elog(ERROR, "FDB index do not support multi keys now");
	if (inskey.keysz != 1 || inskey.scankeys[0].sk_subtype != 23)
		elog(ERROR, "FDB index now only support int4.");


	id_net = htonl(inskey.scankeys[0].sk_argument);

	fdb_start_key = fdbindex_make_key(rel->rd_node, &id_net, 4);
	pfree(start_tuple_key);

	id_net = 0xffffffff;
	id_net = htonl(id_net);
	fdb_end_key = fdbindex_make_key(rel->rd_node, (char *) &id_net, 4);

	so->out_more = fdb_tr_get_kv(so->fdb_database.tr, fdb_start_key, FDB_KEY_LEN, true,
							  fdb_end_key, FDB_KEY_LEN, so->current_future,
							  &so->out_kv, &so->nkv);
	so->next_kv = 0;

	if (so->nkv == 0)
		return false;

	memcpy(&scan->xs_heaptid, so->out_kv[so->next_kv++].value, 6);
	return true;
}

bool
fdb_end_point(IndexScanDesc scan, ScanDirection dir)
{
	unsigned int id_net;
	char *fdb_start_key;
	char *fdb_end_key;
	Relation rel = scan->indexRelation;
	FDBScanOpaque so = (FDBScanOpaque) scan->opaque;

	id_net = 0;
	fdb_start_key = fdbindex_make_key(rel->rd_node, (char *) &id_net, 4);
	id_net = 0xffffffff;

	id_net = htonl(id_net);
	fdb_end_key = fdbindex_make_key(rel->rd_node, (char *) &id_net, 4);

	so->current_future = fdb_tr_get_kv(so->fdb_database.tr, fdb_start_key,
							  FDB_KEY_LEN, true,
							  fdb_end_key, FDB_KEY_LEN, so->current_future,
							  &so->out_kv, &so->nkv, &so->out_more);
	pfree(fdb_end_key);
	so->next_kv = 0;
	if (so->nkv == 0)
		return false;

	memcpy(&scan->xs_heaptid, so->out_kv[so->next_kv++].value, 6);

	return true;
}

bool
fdb_next(IndexScanDesc scan, ScanDirection dir)
{
	FDBScanOpaque so = (FDBScanOpaque) scan->opaque;
	Relation rel = scan->indexRelation;

	if (so->next_kv == so->nkv && !so->out_more)
		return false;

	if (so->next_kv == so->nkv && so->out_more)
	{
		unsigned int id_net;
		char *fdb_end_key;
		id_net = 0xffffffff;
		id_net = htonl(id_net);

		fdb_end_key = fdbindex_make_key(rel->rd_node, (char *) &id_net, 4);

		so->current_future = fdb_tr_get_kv(so->fdb_database.tr,
									   (char *) so->out_kv[so->nkv - 1].key,
									   so->out_kv[so->nkv - 1].key_length, false,
									   fdb_end_key, FDB_KEY_LEN, so->current_future,
									   &so->out_kv, &so->nkv, &so->out_more);

		pfree(fdb_end_key);
		so->next_kv = 0;
	}

	if (so->next_kv == so->nkv)
		return false;

	memcpy(&scan->xs_heaptid, so->out_kv[so->next_kv].value, 6);
	return true;
}
