#include "postgres.h"

#include "access/fdbam.h"
#include "access/multixact.h"
#include "access/xact.h"
#include "utils/builtins.h"
#include "storage/procarray.h"


static inline void
SetHintBits(HeapTuple tuple, uint32 tuple_len, Relation rel,
			FDBDatabaseDesc fdb_database, uint16 infomask, TransactionId xid)
{
	char *key;

	tuple->t_data->t_infomask |= infomask;

	key = fdb_heap_make_key(rel, FDB_MAIN_FORKNUM, tuple->t_self);

	fdb_simple_insert(fdb_database->db, key, FDB_KEY_LEN,
				   (char *) tuple->t_data, tuple_len);

	pfree(key);
}

void
FDBTupleSetHintBits(HeapTuple tuple, uint32 tuple_len, Relation rel,
					FDBDatabaseDesc fdb_database, uint16 infomask,
					TransactionId xid)
{
	SetHintBits(tuple, tuple_len, rel, fdb_database, infomask, xid);
}

static bool
FDBTupleSatisfiesMVCC(HeapTuple htup, Snapshot snapshot, FDBScanDesc scan)
{
	HeapTupleHeader tuple = htup->t_data;

	Assert(ItemPointerIsValid(&htup->t_self));
	Assert(htup->t_tableOid != InvalidOid);

	if (!HeapTupleHeaderXminCommitted(tuple))
	{
		if (HeapTupleHeaderXminInvalid(tuple))
			return false;

		/* Used by pre-9.0 binary upgrades */
		if (tuple->t_infomask & HEAP_MOVED_OFF)
		{
			TransactionId xvac = HeapTupleHeaderGetXvac(tuple);

			if (TransactionIdIsCurrentTransactionId(xvac))
				return false;
			if (!XidInMVCCSnapshot(xvac, snapshot))
			{
				if (TransactionIdDidCommit(xvac))
				{
					SetHintBits(htup, htup->t_len, scan->rs_base.rs_rd, &scan->fdb_database, HEAP_XMIN_INVALID,
								InvalidTransactionId);
					return false;
				}
				SetHintBits(htup, htup->t_len, scan->rs_base.rs_rd, &scan->fdb_database, HEAP_XMIN_COMMITTED,
							InvalidTransactionId);
			}
		}
			/* Used by pre-9.0 binary upgrades */
		else if (tuple->t_infomask & HEAP_MOVED_IN)
		{
			TransactionId xvac = HeapTupleHeaderGetXvac(tuple);

			if (!TransactionIdIsCurrentTransactionId(xvac))
			{
				if (XidInMVCCSnapshot(xvac, snapshot))
					return false;
				if (TransactionIdDidCommit(xvac))
					SetHintBits(htup, htup->t_len, scan->rs_base.rs_rd, &scan->fdb_database, HEAP_XMIN_COMMITTED,
								InvalidTransactionId);
				else
				{
					SetHintBits(htup, htup->t_len, scan->rs_base.rs_rd, &scan->fdb_database, HEAP_XMIN_INVALID,
								InvalidTransactionId);
					return false;
				}
			}
		}
		else if (TransactionIdIsCurrentTransactionId(HeapTupleHeaderGetRawXmin(tuple)))
		{
			if (HeapTupleHeaderGetCmin(tuple) >= snapshot->curcid)
				return false;	/* inserted after scan started */

			if (tuple->t_infomask & HEAP_XMAX_INVALID)	/* xid invalid */
				return true;

			if (HEAP_XMAX_IS_LOCKED_ONLY(tuple->t_infomask))	/* not deleter */
				return true;

			if (tuple->t_infomask & HEAP_XMAX_IS_MULTI)
			{
				TransactionId xmax;

				xmax = HeapTupleGetUpdateXid(tuple);

				/* not LOCKED_ONLY, so it has to have an xmax */
				Assert(TransactionIdIsValid(xmax));

				/* updating subtransaction must have aborted */
				if (!TransactionIdIsCurrentTransactionId(xmax))
					return true;
				else if (HeapTupleHeaderGetCmax(tuple) >= snapshot->curcid)
					return true;	/* updated after scan started */
				else
					return false;	/* updated before scan started */
			}

			if (!TransactionIdIsCurrentTransactionId(HeapTupleHeaderGetRawXmax(tuple)))
			{
				/* deleting subtransaction must have aborted */
				SetHintBits(htup, htup->t_len, scan->rs_base.rs_rd, &scan->fdb_database, HEAP_XMAX_INVALID,
							InvalidTransactionId);
				return true;
			}

			if (HeapTupleHeaderGetCmax(tuple) >= snapshot->curcid)
				return true;	/* deleted after scan started */
			else
				return false;	/* deleted before scan started */
		}
		else if (XidInMVCCSnapshot(HeapTupleHeaderGetRawXmin(tuple), snapshot))
			return false;
		else if (TransactionIdDidCommit(HeapTupleHeaderGetRawXmin(tuple)))
			SetHintBits(htup, htup->t_len, scan->rs_base.rs_rd, &scan->fdb_database, HEAP_XMIN_COMMITTED,
						HeapTupleHeaderGetRawXmin(tuple));
		else
		{
			/* it must have aborted or crashed */
			SetHintBits(htup, htup->t_len, scan->rs_base.rs_rd, &scan->fdb_database, HEAP_XMIN_INVALID,
						InvalidTransactionId);
			return false;
		}
	}
	else
	{
		/* xmin is committed, but maybe not according to our snapshot */
		if (!HeapTupleHeaderXminFrozen(tuple) &&
			XidInMVCCSnapshot(HeapTupleHeaderGetRawXmin(tuple), snapshot))
			return false;		/* treat as still in progress */
	}

	/* by here, the inserting transaction has committed */

	if (tuple->t_infomask & HEAP_XMAX_INVALID)	/* xid invalid or aborted */
		return true;

	if (HEAP_XMAX_IS_LOCKED_ONLY(tuple->t_infomask))
		return true;

	if (tuple->t_infomask & HEAP_XMAX_IS_MULTI)
	{
		TransactionId xmax;

		/* already checked above */
		Assert(!HEAP_XMAX_IS_LOCKED_ONLY(tuple->t_infomask));

		xmax = HeapTupleGetUpdateXid(tuple);

		/* not LOCKED_ONLY, so it has to have an xmax */
		Assert(TransactionIdIsValid(xmax));

		if (TransactionIdIsCurrentTransactionId(xmax))
		{
			if (HeapTupleHeaderGetCmax(tuple) >= snapshot->curcid)
				return true;	/* deleted after scan started */
			else
				return false;	/* deleted before scan started */
		}
		if (XidInMVCCSnapshot(xmax, snapshot))
			return true;
		if (TransactionIdDidCommit(xmax))
			return false;		/* updating transaction committed */
		/* it must have aborted or crashed */
		return true;
	}

	if (!(tuple->t_infomask & HEAP_XMAX_COMMITTED))
	{
		if (TransactionIdIsCurrentTransactionId(HeapTupleHeaderGetRawXmax(tuple)))
		{
			if (HeapTupleHeaderGetCmax(tuple) >= snapshot->curcid)
				return true;	/* deleted after scan started */
			else
				return false;	/* deleted before scan started */
		}

		if (XidInMVCCSnapshot(HeapTupleHeaderGetRawXmax(tuple), snapshot))
			return true;

		if (!TransactionIdDidCommit(HeapTupleHeaderGetRawXmax(tuple)))
		{
			/* it must have aborted or crashed */
			SetHintBits(htup, htup->t_len, scan->rs_base.rs_rd, &scan->fdb_database, HEAP_XMAX_INVALID,
						InvalidTransactionId);
			return true;
		}

		/* xmax transaction committed */
		SetHintBits(htup, htup->t_len, scan->rs_base.rs_rd, &scan->fdb_database, HEAP_XMAX_COMMITTED,
					HeapTupleHeaderGetRawXmax(tuple));
	}
	else
	{
		/* xmax is committed, but maybe not according to our snapshot */
		if (XidInMVCCSnapshot(HeapTupleHeaderGetRawXmax(tuple), snapshot))
			return true;		/* treat as still in progress */
	}

	/* xmax transaction committed */

	return false;

}


static bool
FDBTupleSatisfiesSelf(HeapTuple htup, Snapshot snapshot, FDBScanDesc scan)
{
	HeapTupleHeader tuple = htup->t_data;

	Assert(ItemPointerIsValid(&htup->t_self));
	Assert(htup->t_tableOid != InvalidOid);

	if (!HeapTupleHeaderXminCommitted(tuple))
	{
		if (HeapTupleHeaderXminInvalid(tuple))
			return false;

		/* Used by pre-9.0 binary upgrades */
		if (tuple->t_infomask & HEAP_MOVED_OFF)
		{
			TransactionId xvac = HeapTupleHeaderGetXvac(tuple);

			if (TransactionIdIsCurrentTransactionId(xvac))
				return false;
			if (!TransactionIdIsInProgress(xvac))
			{
				if (TransactionIdDidCommit(xvac))
				{
					SetHintBits(htup, htup->t_len,  scan->rs_base.rs_rd,
										  &scan->fdb_database, HEAP_XMIN_INVALID,
										  InvalidTransactionId);
					return false;
				}
				SetHintBits(htup, htup->t_len,  scan->rs_base.rs_rd,
										&scan->fdb_database, HEAP_XMIN_COMMITTED,
										InvalidTransactionId);
			}
		}
			/* Used by pre-9.0 binary upgrades */
		else if (tuple->t_infomask & HEAP_MOVED_IN)
		{
			TransactionId xvac = HeapTupleHeaderGetXvac(tuple);

			if (!TransactionIdIsCurrentTransactionId(xvac))
			{
				if (TransactionIdIsInProgress(xvac))
					return false;
				if (TransactionIdDidCommit(xvac))
					SetHintBits(htup, htup->t_len, scan->rs_base.rs_rd,
										  &scan->fdb_database, HEAP_XMIN_COMMITTED,
											InvalidTransactionId);
				else
				{
					SetHintBits(htup, htup->t_len, scan->rs_base.rs_rd,
											&scan->fdb_database, HEAP_XMIN_INVALID,
											InvalidTransactionId);
					return false;
				}
			}
		}
		else if (TransactionIdIsCurrentTransactionId(HeapTupleHeaderGetRawXmin(tuple)))
		{
			if (tuple->t_infomask & HEAP_XMAX_INVALID)	/* xid invalid */
				return true;

			if (HEAP_XMAX_IS_LOCKED_ONLY(tuple->t_infomask))	/* not deleter */
				return true;

			if (tuple->t_infomask & HEAP_XMAX_IS_MULTI)
			{
				TransactionId xmax;

				xmax = HeapTupleGetUpdateXid(tuple);

				/* not LOCKED_ONLY, so it has to have an xmax */
				Assert(TransactionIdIsValid(xmax));

				/* updating subtransaction must have aborted */
				if (!TransactionIdIsCurrentTransactionId(xmax))
					return true;
				else
					return false;
			}

			if (!TransactionIdIsCurrentTransactionId(HeapTupleHeaderGetRawXmax(tuple)))
			{
				/* deleting subtransaction must have aborted */
				SetHintBits(htup, htup->t_len, scan->rs_base.rs_rd,
										&scan->fdb_database, HEAP_XMAX_INVALID,
										InvalidTransactionId);
				return true;
			}

			return false;
		}
		else if (TransactionIdIsInProgress(HeapTupleHeaderGetRawXmin(tuple)))
			return false;
		else if (TransactionIdDidCommit(HeapTupleHeaderGetRawXmin(tuple)))
			SetHintBits(htup, htup->t_len, scan->rs_base.rs_rd,
									&scan->fdb_database, HEAP_XMIN_COMMITTED,
									HeapTupleHeaderGetRawXmin(tuple));
		else
		{
			/* it must have aborted or crashed */
			SetHintBits(htup, htup->t_len, scan->rs_base.rs_rd,
									&scan->fdb_database, HEAP_XMIN_INVALID,
									InvalidTransactionId);
			return false;
		}
	}

	/* by here, the inserting transaction has committed */

	if (tuple->t_infomask & HEAP_XMAX_INVALID)	/* xid invalid or aborted */
		return true;

	if (tuple->t_infomask & HEAP_XMAX_COMMITTED)
	{
		if (HEAP_XMAX_IS_LOCKED_ONLY(tuple->t_infomask))
			return true;
		return false;			/* updated by other */
	}

	if (tuple->t_infomask & HEAP_XMAX_IS_MULTI)
	{
		TransactionId xmax;

		if (HEAP_XMAX_IS_LOCKED_ONLY(tuple->t_infomask))
			return true;

		xmax = HeapTupleGetUpdateXid(tuple);

		/* not LOCKED_ONLY, so it has to have an xmax */
		Assert(TransactionIdIsValid(xmax));

		if (TransactionIdIsCurrentTransactionId(xmax))
			return false;
		if (TransactionIdIsInProgress(xmax))
			return true;
		if (TransactionIdDidCommit(xmax))
			return false;
		/* it must have aborted or crashed */
		return true;
	}

	if (TransactionIdIsCurrentTransactionId(HeapTupleHeaderGetRawXmax(tuple)))
	{
		if (HEAP_XMAX_IS_LOCKED_ONLY(tuple->t_infomask))
			return true;
		return false;
	}

	if (TransactionIdIsInProgress(HeapTupleHeaderGetRawXmax(tuple)))
		return true;

	if (!TransactionIdDidCommit(HeapTupleHeaderGetRawXmax(tuple)))
	{
		/* it must have aborted or crashed */
		SetHintBits(htup, htup->t_len, scan->rs_base.rs_rd,
								&scan->fdb_database, HEAP_XMAX_INVALID,
								InvalidTransactionId);
		return true;
	}

	/* xmax transaction committed */

	if (HEAP_XMAX_IS_LOCKED_ONLY(tuple->t_infomask))
	{
		SetHintBits(htup, htup->t_len, scan->rs_base.rs_rd,
								&scan->fdb_database, HEAP_XMAX_INVALID,
								InvalidTransactionId);
		return true;
	}

	SetHintBits(htup, htup->t_len, scan->rs_base.rs_rd,
							&scan->fdb_database, HEAP_XMAX_COMMITTED,
							HeapTupleHeaderGetRawXmax(tuple));
	return false;
}

static bool
FDBTupleSatisfiesAny(HeapTuple htup, Snapshot snapshot, FDBScanDesc scan)
{
	return true;
}

static bool
FDBTupleSatisfiesToast(HeapTuple htup, Snapshot snapshot, FDBScanDesc scan)
{
	HeapTupleHeader tuple = htup->t_data;

	Assert(ItemPointerIsValid(&htup->t_self));
	Assert(htup->t_tableOid != InvalidOid);

	if (!HeapTupleHeaderXminCommitted(tuple))
	{
		if (HeapTupleHeaderXminInvalid(tuple))
			return false;

		/* Used by pre-9.0 binary upgrades */
		if (tuple->t_infomask & HEAP_MOVED_OFF)
		{
			TransactionId xvac = HeapTupleHeaderGetXvac(tuple);

			if (TransactionIdIsCurrentTransactionId(xvac))
				return false;
			if (!TransactionIdIsInProgress(xvac))
			{
				if (TransactionIdDidCommit(xvac))
				{
					SetHintBits(htup, htup->t_len, scan->rs_base.rs_rd,
											&scan->fdb_database, HEAP_XMIN_INVALID,
											InvalidTransactionId);
					return false;
				}
				SetHintBits(htup, htup->t_len, scan->rs_base.rs_rd,
										&scan->fdb_database, HEAP_XMIN_COMMITTED,
										InvalidTransactionId);
			}
		}
			/* Used by pre-9.0 binary upgrades */
		else if (tuple->t_infomask & HEAP_MOVED_IN)
		{
			TransactionId xvac = HeapTupleHeaderGetXvac(tuple);

			if (!TransactionIdIsCurrentTransactionId(xvac))
			{
				if (TransactionIdIsInProgress(xvac))
					return false;
				if (TransactionIdDidCommit(xvac))
					SetHintBits(htup, htup->t_len, scan->rs_base.rs_rd,
											&scan->fdb_database, HEAP_XMIN_COMMITTED,
											InvalidTransactionId);
				else
				{
					SetHintBits(htup, htup->t_len, scan->rs_base.rs_rd,
											&scan->fdb_database, HEAP_XMIN_INVALID,
											InvalidTransactionId);
					return false;
				}
			}
		}

			/*
			 * An invalid Xmin can be left behind by a speculative insertion that
			 * is canceled by super-deleting the tuple.  This also applies to
			 * TOAST tuples created during speculative insertion.
			 */
		else if (!TransactionIdIsValid(HeapTupleHeaderGetXmin(tuple)))
			return false;
	}

	/* otherwise assume the tuple is valid for TOAST. */
	return true;
}

static bool
FDBTupleSatisfiesDirty(HeapTuple htup, Snapshot snapshot, FDBScanDesc scan)
{
	HeapTupleHeader tuple = htup->t_data;

	Assert(ItemPointerIsValid(&htup->t_self));
	Assert(htup->t_tableOid != InvalidOid);

	snapshot->xmin = snapshot->xmax = InvalidTransactionId;
	snapshot->speculativeToken = 0;

	if (!HeapTupleHeaderXminCommitted(tuple))
	{
		if (HeapTupleHeaderXminInvalid(tuple))
			return false;

		/* Used by pre-9.0 binary upgrades */
		if (tuple->t_infomask & HEAP_MOVED_OFF)
		{
			TransactionId xvac = HeapTupleHeaderGetXvac(tuple);

			if (TransactionIdIsCurrentTransactionId(xvac))
				return false;
			if (!TransactionIdIsInProgress(xvac))
			{
				if (TransactionIdDidCommit(xvac))
				{
					SetHintBits(htup, htup->t_len, scan->rs_base.rs_rd,
											&scan->fdb_database, HEAP_XMIN_INVALID,
											InvalidTransactionId);
					return false;
				}
				SetHintBits(htup, htup->t_len, scan->rs_base.rs_rd,
										&scan->fdb_database, HEAP_XMIN_COMMITTED,
										InvalidTransactionId);
			}
		}
			/* Used by pre-9.0 binary upgrades */
		else if (tuple->t_infomask & HEAP_MOVED_IN)
		{
			TransactionId xvac = HeapTupleHeaderGetXvac(tuple);

			if (!TransactionIdIsCurrentTransactionId(xvac))
			{
				if (TransactionIdIsInProgress(xvac))
					return false;
				if (TransactionIdDidCommit(xvac))
					SetHintBits(htup, htup->t_len, scan->rs_base.rs_rd,
											&scan->fdb_database, HEAP_XMIN_COMMITTED,
											InvalidTransactionId);
				else
				{
					SetHintBits(htup, htup->t_len, scan->rs_base.rs_rd,
											&scan->fdb_database, HEAP_XMIN_INVALID,
											InvalidTransactionId);
					return false;
				}
			}
		}
		else if (TransactionIdIsCurrentTransactionId(HeapTupleHeaderGetRawXmin(tuple)))
		{
			if (tuple->t_infomask & HEAP_XMAX_INVALID)	/* xid invalid */
				return true;

			if (HEAP_XMAX_IS_LOCKED_ONLY(tuple->t_infomask))	/* not deleter */
				return true;

			if (tuple->t_infomask & HEAP_XMAX_IS_MULTI)
			{
				TransactionId xmax;

				xmax = HeapTupleGetUpdateXid(tuple);

				/* not LOCKED_ONLY, so it has to have an xmax */
				Assert(TransactionIdIsValid(xmax));

				/* updating subtransaction must have aborted */
				if (!TransactionIdIsCurrentTransactionId(xmax))
					return true;
				else
					return false;
			}

			if (!TransactionIdIsCurrentTransactionId(HeapTupleHeaderGetRawXmax(tuple)))
			{
				/* deleting subtransaction must have aborted */
				SetHintBits(htup, htup->t_len, scan->rs_base.rs_rd,
										&scan->fdb_database, HEAP_XMAX_INVALID,
										InvalidTransactionId);
				return true;
			}

			return false;
		}
		else if (TransactionIdIsInProgress(HeapTupleHeaderGetRawXmin(tuple)))
		{
			/*
			 * Return the speculative token to caller.  Caller can worry about
			 * xmax, since it requires a conclusively locked row version, and
			 * a concurrent update to this tuple is a conflict of its
			 * purposes.
			 */
			if (HeapTupleHeaderIsSpeculative(tuple))
			{
				snapshot->speculativeToken =
								HeapTupleHeaderGetSpeculativeToken(tuple);

				Assert(snapshot->speculativeToken != 0);
			}

			snapshot->xmin = HeapTupleHeaderGetRawXmin(tuple);
			/* XXX shouldn't we fall through to look at xmax? */
			return true;		/* in insertion by other */
		}
		else if (TransactionIdDidCommit(HeapTupleHeaderGetRawXmin(tuple)))
			SetHintBits(htup, htup->t_len, scan->rs_base.rs_rd,
									&scan->fdb_database, HEAP_XMIN_COMMITTED,
									HeapTupleHeaderGetRawXmin(tuple));
		else
		{
			/* it must have aborted or crashed */
			SetHintBits(htup, htup->t_len, scan->rs_base.rs_rd,
									&scan->fdb_database, HEAP_XMIN_INVALID,
									InvalidTransactionId);
			return false;
		}
	}

	/* by here, the inserting transaction has committed */

	if (tuple->t_infomask & HEAP_XMAX_INVALID)	/* xid invalid or aborted */
		return true;

	if (tuple->t_infomask & HEAP_XMAX_COMMITTED)
	{
		if (HEAP_XMAX_IS_LOCKED_ONLY(tuple->t_infomask))
			return true;
		return false;			/* updated by other */
	}

	if (tuple->t_infomask & HEAP_XMAX_IS_MULTI)
	{
		TransactionId xmax;

		if (HEAP_XMAX_IS_LOCKED_ONLY(tuple->t_infomask))
			return true;

		xmax = HeapTupleGetUpdateXid(tuple);

		/* not LOCKED_ONLY, so it has to have an xmax */
		Assert(TransactionIdIsValid(xmax));

		if (TransactionIdIsCurrentTransactionId(xmax))
			return false;
		if (TransactionIdIsInProgress(xmax))
		{
			snapshot->xmax = xmax;
			return true;
		}
		if (TransactionIdDidCommit(xmax))
			return false;
		/* it must have aborted or crashed */
		return true;
	}

	if (TransactionIdIsCurrentTransactionId(HeapTupleHeaderGetRawXmax(tuple)))
	{
		if (HEAP_XMAX_IS_LOCKED_ONLY(tuple->t_infomask))
			return true;
		return false;
	}

	if (TransactionIdIsInProgress(HeapTupleHeaderGetRawXmax(tuple)))
	{
		if (!HEAP_XMAX_IS_LOCKED_ONLY(tuple->t_infomask))
			snapshot->xmax = HeapTupleHeaderGetRawXmax(tuple);
		return true;
	}

	if (!TransactionIdDidCommit(HeapTupleHeaderGetRawXmax(tuple)))
	{
		/* it must have aborted or crashed */
		SetHintBits(htup, htup->t_len, scan->rs_base.rs_rd,
								&scan->fdb_database, HEAP_XMAX_INVALID,
								InvalidTransactionId);
		return true;
	}

	/* xmax transaction committed */

	if (HEAP_XMAX_IS_LOCKED_ONLY(tuple->t_infomask))
	{
		SetHintBits(htup, htup->t_len, scan->rs_base.rs_rd,
								&scan->fdb_database, HEAP_XMAX_INVALID,
								InvalidTransactionId);
		return true;
	}

	SetHintBits(htup, htup->t_len, scan->rs_base.rs_rd,
							&scan->fdb_database, HEAP_XMAX_COMMITTED,
							HeapTupleHeaderGetRawXmax(tuple));
	return false;				/* updated by other */
}

static bool
TransactionIdInArray(TransactionId xid, TransactionId *xip, Size num)
{
	return bsearch(&xid, xip, num,
								 sizeof(TransactionId), xidComparator) != NULL;
}

static bool
FDBTupleSatisfiesHistoricMVCC(HeapTuple htup, Snapshot snapshot,
							  FDBScanDesc scan)
{
	HeapTupleHeader tuple = htup->t_data;
	TransactionId xmin = HeapTupleHeaderGetXmin(tuple);
	TransactionId xmax = HeapTupleHeaderGetRawXmax(tuple);

	Assert(ItemPointerIsValid(&htup->t_self));
	Assert(htup->t_tableOid != InvalidOid);

	/* inserting transaction aborted */
	if (HeapTupleHeaderXminInvalid(tuple))
	{
		Assert(!TransactionIdDidCommit(xmin));
		return false;
	}
		/* check if it's one of our txids, toplevel is also in there */
	else if (TransactionIdInArray(xmin, snapshot->subxip, snapshot->subxcnt))
	{
		bool		resolved;
		CommandId	cmin = HeapTupleHeaderGetRawCommandId(tuple);
		CommandId	cmax = InvalidCommandId;

		/*
		 * another transaction might have (tried to) delete this tuple or
		 * cmin/cmax was stored in a combocid. So we need to lookup the actual
		 * values externally.
		 */
		/* TODO */
//		resolved = ResolveCminCmaxDuringDecoding(HistoricSnapshotGetTupleCids(), snapshot,
//																						 htup, buffer,
//																						 &cmin, &cmax);

		resolved = true;

		if (!resolved)
			elog(ERROR, "could not resolve cmin/cmax of catalog tuple");

		Assert(cmin != InvalidCommandId);

		if (cmin >= snapshot->curcid)
			return false;		/* inserted after scan started */
		/* fall through */
	}
		/* committed before our xmin horizon. Do a normal visibility check. */
	else if (TransactionIdPrecedes(xmin, snapshot->xmin))
	{
		Assert(!(HeapTupleHeaderXminCommitted(tuple) &&
						 !TransactionIdDidCommit(xmin)));

		/* check for hint bit first, consult clog afterwards */
		if (!HeapTupleHeaderXminCommitted(tuple) &&
				!TransactionIdDidCommit(xmin))
			return false;
		/* fall through */
	}
		/* beyond our xmax horizon, i.e. invisible */
	else if (TransactionIdFollowsOrEquals(xmin, snapshot->xmax))
	{
		return false;
	}
		/* check if it's a committed transaction in [xmin, xmax) */
	else if (TransactionIdInArray(xmin, snapshot->xip, snapshot->xcnt))
	{
		/* fall through */
	}

		/*
		 * none of the above, i.e. between [xmin, xmax) but hasn't committed. I.e.
		 * invisible.
		 */
	else
	{
		return false;
	}

	/* at this point we know xmin is visible, go on to check xmax */

	/* xid invalid or aborted */
	if (tuple->t_infomask & HEAP_XMAX_INVALID)
		return true;
		/* locked tuples are always visible */
	else if (HEAP_XMAX_IS_LOCKED_ONLY(tuple->t_infomask))
		return true;

		/*
		 * We can see multis here if we're looking at user tables or if somebody
		 * SELECT ... FOR SHARE/UPDATE a system table.
		 */
	else if (tuple->t_infomask & HEAP_XMAX_IS_MULTI)
	{
		xmax = HeapTupleGetUpdateXid(tuple);
	}

	/* check if it's one of our txids, toplevel is also in there */
	if (TransactionIdInArray(xmax, snapshot->subxip, snapshot->subxcnt))
	{
		bool		resolved;
		CommandId	cmin;
		CommandId	cmax = HeapTupleHeaderGetRawCommandId(tuple);

		/* Lookup actual cmin/cmax values */
//		resolved = ResolveCminCmaxDuringDecoding(HistoricSnapshotGetTupleCids(), snapshot,
//																						 htup, buffer,
//																						 &cmin, &cmax);
		resolved = true;

		if (!resolved)
			elog(ERROR, "could not resolve combocid to cmax");

		Assert(cmax != InvalidCommandId);

		if (cmax >= snapshot->curcid)
			return true;		/* deleted after scan started */
		else
			return false;		/* deleted before scan started */
	}
		/* below xmin horizon, normal transaction state is valid */
	else if (TransactionIdPrecedes(xmax, snapshot->xmin))
	{
		Assert(!(tuple->t_infomask & HEAP_XMAX_COMMITTED &&
						 !TransactionIdDidCommit(xmax)));

		/* check hint bit first */
		if (tuple->t_infomask & HEAP_XMAX_COMMITTED)
			return false;

		/* check clog */
		return !TransactionIdDidCommit(xmax);
	}
		/* above xmax horizon, we cannot possibly see the deleting transaction */
	else if (TransactionIdFollowsOrEquals(xmax, snapshot->xmax))
		return true;
		/* xmax is between [xmin, xmax), check known committed array */
	else if (TransactionIdInArray(xmax, snapshot->xip, snapshot->xcnt))
		return false;
		/* xmax is between [xmin, xmax), but known not to have committed yet */
	else
		return true;
}

static bool
FDBTupleSatisfiesNonVacuumable(HeapTuple htup, Snapshot snapshot,
							   FDBScanDesc scan)
{
	// TODO
//	return HeapTupleSatisfiesVacuum(htup, snapshot->xmin, buffer)
//				 != HEAPTUPLE_DEAD;
	return true;
}

bool
FDBTupleSatisfiesVisibility(HeapTuple tup, Snapshot snapshot, FDBScanDesc scan)
{
	switch (snapshot->snapshot_type)
	{
		case SNAPSHOT_MVCC:
			return FDBTupleSatisfiesMVCC(tup, snapshot, scan);
			break;
		case SNAPSHOT_SELF:
			return FDBTupleSatisfiesSelf(tup, snapshot, scan);
			break;
		case SNAPSHOT_ANY:
			return FDBTupleSatisfiesAny(tup, snapshot, scan);
			break;
		case SNAPSHOT_TOAST:
			return FDBTupleSatisfiesToast(tup, snapshot, scan);
			break;
		case SNAPSHOT_DIRTY:
			return FDBTupleSatisfiesDirty(tup, snapshot, scan);
			break;
		case SNAPSHOT_HISTORIC_MVCC:
			return FDBTupleSatisfiesHistoricMVCC(tup, snapshot, scan);
			break;
		case SNAPSHOT_NON_VACUUMABLE:
			return FDBTupleSatisfiesNonVacuumable(tup, snapshot, scan);
			break;
	}

	return false;				/* keep compiler quiet */
}

TM_Result
FDBTupleSatisfiesUpdate(HeapTuple htup, CommandId curcid,
						FDBDeleteDesc desc)
{
	HeapTupleHeader tuple = htup->t_data;

	Assert(ItemPointerIsValid(&htup->t_self));
	Assert(htup->t_tableOid != InvalidOid);

	if (!HeapTupleHeaderXminCommitted(tuple))
	{
		if (HeapTupleHeaderXminInvalid(tuple))
			return TM_Invisible;

		/* Used by pre-9.0 binary upgrades */
		if (tuple->t_infomask & HEAP_MOVED_OFF)
		{
			TransactionId xvac = HeapTupleHeaderGetXvac(tuple);

			if (TransactionIdIsCurrentTransactionId(xvac))
				return TM_Invisible;
			if (!TransactionIdIsInProgress(xvac))
			{
				if (TransactionIdDidCommit(xvac))
				{
					SetHintBits(htup, htup->t_len, desc->rel, &desc->fdb_database, HEAP_XMIN_INVALID,
								InvalidTransactionId);
					return TM_Invisible;
				}
				SetHintBits(htup, htup->t_len, desc->rel, &desc->fdb_database, HEAP_XMIN_COMMITTED,
							InvalidTransactionId);
			}
		}
			/* Used by pre-9.0 binary upgrades */
		else if (tuple->t_infomask & HEAP_MOVED_IN)
		{
			TransactionId xvac = HeapTupleHeaderGetXvac(tuple);

			if (!TransactionIdIsCurrentTransactionId(xvac))
			{
				if (TransactionIdIsInProgress(xvac))
					return TM_Invisible;
				if (TransactionIdDidCommit(xvac))
					SetHintBits(htup, htup->t_len, desc->rel, &desc->fdb_database, HEAP_XMIN_COMMITTED,
								InvalidTransactionId);
				else
				{
					SetHintBits(htup, htup->t_len, desc->rel, &desc->fdb_database, HEAP_XMIN_INVALID,
								InvalidTransactionId);
					return TM_Invisible;
				}
			}
		}
		else if (TransactionIdIsCurrentTransactionId(HeapTupleHeaderGetRawXmin(tuple)))
		{
			if (HeapTupleHeaderGetCmin(tuple) >= curcid)
				return TM_Invisible;	/* inserted after scan started */

			if (tuple->t_infomask & HEAP_XMAX_INVALID)	/* xid invalid */
				return TM_Ok;

			if (HEAP_XMAX_IS_LOCKED_ONLY(tuple->t_infomask))
			{
				TransactionId xmax;

				xmax = HeapTupleHeaderGetRawXmax(tuple);

				/*
				 * Careful here: even though this tuple was created by our own
				 * transaction, it might be locked by other transactions, if
				 * the original version was key-share locked when we updated
				 * it.
				 */

				if (tuple->t_infomask & HEAP_XMAX_IS_MULTI)
				{
					if (MultiXactIdIsRunning(xmax, true))
						return TM_BeingModified;
					else
						return TM_Ok;
				}

				/*
				 * If the locker is gone, then there is nothing of interest
				 * left in this Xmax; otherwise, report the tuple as
				 * locked/updated.
				 */
				if (!TransactionIdIsInProgress(xmax))
					return TM_Ok;
				return TM_BeingModified;
			}

			if (tuple->t_infomask & HEAP_XMAX_IS_MULTI)
			{
				TransactionId xmax;

				xmax = HeapTupleGetUpdateXid(tuple);

				/* not LOCKED_ONLY, so it has to have an xmax */
				Assert(TransactionIdIsValid(xmax));

				/* deleting subtransaction must have aborted */
				if (!TransactionIdIsCurrentTransactionId(xmax))
				{
					if (MultiXactIdIsRunning(HeapTupleHeaderGetRawXmax(tuple),
											 false))
						return TM_BeingModified;
					return TM_Ok;
				}
				else
				{
					if (HeapTupleHeaderGetCmax(tuple) >= curcid)
						return TM_SelfModified; /* updated after scan started */
					else
						return TM_Invisible;	/* updated before scan started */
				}
			}

			if (!TransactionIdIsCurrentTransactionId(HeapTupleHeaderGetRawXmax(tuple)))
			{
				/* deleting subtransaction must have aborted */
				SetHintBits(htup, htup->t_len, desc->rel, &desc->fdb_database, HEAP_XMAX_INVALID,
							InvalidTransactionId);
				return TM_Ok;
			}

			if (HeapTupleHeaderGetCmax(tuple) >= curcid)
				return TM_SelfModified; /* updated after scan started */
			else
				return TM_Invisible;	/* updated before scan started */
		}
		else if (TransactionIdIsInProgress(HeapTupleHeaderGetRawXmin(tuple)))
			return TM_Invisible;
		else if (TransactionIdDidCommit(HeapTupleHeaderGetRawXmin(tuple)))
			SetHintBits(htup, htup->t_len, desc->rel, &desc->fdb_database, HEAP_XMIN_COMMITTED,
						HeapTupleHeaderGetRawXmin(tuple));
		else
		{
			/* it must have aborted or crashed */
			SetHintBits(htup, htup->t_len, desc->rel, &desc->fdb_database, HEAP_XMIN_INVALID,
						InvalidTransactionId);
			return TM_Invisible;
		}
	}

	/* by here, the inserting transaction has committed */

	if (tuple->t_infomask & HEAP_XMAX_INVALID)	/* xid invalid or aborted */
		return TM_Ok;

	if (tuple->t_infomask & HEAP_XMAX_COMMITTED)
	{
		if (HEAP_XMAX_IS_LOCKED_ONLY(tuple->t_infomask))
			return TM_Ok;
		if (!ItemPointerEquals(&htup->t_self, &tuple->t_ctid) ||
			HeapTupleHeaderIndicatesMovedPartitions(tuple))
			return TM_Updated;	/* updated by other */
		else
			return TM_Deleted;	/* deleted by other */
	}

	if (tuple->t_infomask & HEAP_XMAX_IS_MULTI)
	{
		TransactionId xmax;

		if (HEAP_LOCKED_UPGRADED(tuple->t_infomask))
			return TM_Ok;

		if (HEAP_XMAX_IS_LOCKED_ONLY(tuple->t_infomask))
		{
			if (MultiXactIdIsRunning(HeapTupleHeaderGetRawXmax(tuple), true))
				return TM_BeingModified;

			SetHintBits(htup, htup->t_len, desc->rel, &desc->fdb_database, HEAP_XMAX_INVALID, InvalidTransactionId);
			return TM_Ok;
		}

		xmax = HeapTupleGetUpdateXid(tuple);
		if (!TransactionIdIsValid(xmax))
		{
			if (MultiXactIdIsRunning(HeapTupleHeaderGetRawXmax(tuple), false))
				return TM_BeingModified;
		}

		/* not LOCKED_ONLY, so it has to have an xmax */
		Assert(TransactionIdIsValid(xmax));

		if (TransactionIdIsCurrentTransactionId(xmax))
		{
			if (HeapTupleHeaderGetCmax(tuple) >= curcid)
				return TM_SelfModified; /* updated after scan started */
			else
				return TM_Invisible;	/* updated before scan started */
		}

		if (MultiXactIdIsRunning(HeapTupleHeaderGetRawXmax(tuple), false))
			return TM_BeingModified;

		if (TransactionIdDidCommit(xmax))
		{
			if (!ItemPointerEquals(&htup->t_self, &tuple->t_ctid) ||
				HeapTupleHeaderIndicatesMovedPartitions(tuple))
				return TM_Updated;
			else
				return TM_Deleted;
		}

		/*
		 * By here, the update in the Xmax is either aborted or crashed, but
		 * what about the other members?
		 */

		if (!MultiXactIdIsRunning(HeapTupleHeaderGetRawXmax(tuple), false))
		{
			/*
			 * There's no member, even just a locker, alive anymore, so we can
			 * mark the Xmax as invalid.
			 */
			SetHintBits(htup, htup->t_len, desc->rel, &desc->fdb_database, HEAP_XMAX_INVALID,
						InvalidTransactionId);
			return TM_Ok;
		}
		else
		{
			/* There are lockers running */
			return TM_BeingModified;
		}
	}

	if (TransactionIdIsCurrentTransactionId(HeapTupleHeaderGetRawXmax(tuple)))
	{
		if (HEAP_XMAX_IS_LOCKED_ONLY(tuple->t_infomask))
			return TM_BeingModified;
		if (HeapTupleHeaderGetCmax(tuple) >= curcid)
			return TM_SelfModified; /* updated after scan started */
		else
			return TM_Invisible;	/* updated before scan started */
	}

	if (TransactionIdIsInProgress(HeapTupleHeaderGetRawXmax(tuple)))
		return TM_BeingModified;

	if (!TransactionIdDidCommit(HeapTupleHeaderGetRawXmax(tuple)))
	{
		/* it must have aborted or crashed */
		SetHintBits(htup, htup->t_len, desc->rel, &desc->fdb_database, HEAP_XMAX_INVALID,
					InvalidTransactionId);
		return TM_Ok;
	}

	/* xmax transaction committed */

	if (HEAP_XMAX_IS_LOCKED_ONLY(tuple->t_infomask))
	{
		SetHintBits(htup, htup->t_len, desc->rel, &desc->fdb_database, HEAP_XMAX_INVALID,
					InvalidTransactionId);
		return TM_Ok;
	}

	SetHintBits(htup, htup->t_len, desc->rel, &desc->fdb_database, HEAP_XMAX_COMMITTED,
				HeapTupleHeaderGetRawXmax(tuple));
	if (!ItemPointerEquals(&htup->t_self, &tuple->t_ctid) ||
		HeapTupleHeaderIndicatesMovedPartitions(tuple))
		return TM_Updated;		/* updated by other */
	else
		return TM_Deleted;		/* deleted by other */
}


