#include "postgres.h"

#include "access/fdbam.h"
#include "access/multixact.h"
#include "access/xact.h"
#include "storage/procarray.h"


static inline void
SetHintBits(HeapTupleHeader tuple, uint32 tuple_len, FDBScanDesc scan,
			uint16 infomask, TransactionId xid)
{
	char *key;

	tuple->t_infomask |= infomask;

	key = fdb_heap_make_key(scan->rs_base.rs_rd, FDB_MAIN_FORKNUM, tuple->t_ctid);

	fdb_simple_insert(scan->db, key, FDB_KEY_LEN, (char *) tuple, tuple_len);

	pfree(key);
}

void
FDBTupleSetHintBits(HeapTupleHeader tuple, uint32 tuple_len, FDBScanDesc scan,
					uint16 infomask, TransactionId xid)
{
	SetHintBits(tuple, tuple_len, scan, infomask, xid);
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
					SetHintBits(tuple, htup->t_len, scan, HEAP_XMIN_INVALID,
								InvalidTransactionId);
					return false;
				}
				SetHintBits(tuple, htup->t_len, scan, HEAP_XMIN_COMMITTED,
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
					SetHintBits(tuple, htup->t_len, scan, HEAP_XMIN_COMMITTED,
								InvalidTransactionId);
				else
				{
					SetHintBits(tuple, htup->t_len, scan, HEAP_XMIN_INVALID,
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
				SetHintBits(tuple, htup->t_len, scan, HEAP_XMAX_INVALID,
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
			SetHintBits(tuple, htup->t_len, scan, HEAP_XMIN_COMMITTED,
						HeapTupleHeaderGetRawXmin(tuple));
		else
		{
			/* it must have aborted or crashed */
			SetHintBits(tuple, htup->t_len, scan, HEAP_XMIN_INVALID,
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
			SetHintBits(tuple, htup->t_len, scan, HEAP_XMAX_INVALID,
						InvalidTransactionId);
			return true;
		}

		/* xmax transaction committed */
		SetHintBits(tuple, htup->t_len, scan, HEAP_XMAX_COMMITTED,
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

bool
FDBTupleSatisfiesVisibility(HeapTuple tup, Snapshot snapshot, FDBScanDesc scan)
{
	switch (snapshot->snapshot_type)
	{
		case SNAPSHOT_MVCC:
			return FDBTupleSatisfiesMVCC(tup, snapshot, scan);
			break;
		case SNAPSHOT_SELF:
			return true;
			break;
		case SNAPSHOT_ANY:
			return true;
			break;
		case SNAPSHOT_TOAST:
			return true;
			break;
		case SNAPSHOT_DIRTY:
			return true;
			break;
		case SNAPSHOT_HISTORIC_MVCC:
			return true;
			break;
		case SNAPSHOT_NON_VACUUMABLE:
			return true;
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
					SetHintBits(tuple, htup->t_len, desc, HEAP_XMIN_INVALID,
								InvalidTransactionId);
					return TM_Invisible;
				}
				SetHintBits(tuple, htup->t_len, desc, HEAP_XMIN_COMMITTED,
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
					SetHintBits(tuple, htup->t_len, desc, HEAP_XMIN_COMMITTED,
								InvalidTransactionId);
				else
				{
					SetHintBits(tuple, htup->t_len, desc, HEAP_XMIN_INVALID,
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
				SetHintBits(tuple, htup->t_len, desc, HEAP_XMAX_INVALID,
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
			SetHintBits(tuple, htup->t_len, desc, HEAP_XMIN_COMMITTED,
						HeapTupleHeaderGetRawXmin(tuple));
		else
		{
			/* it must have aborted or crashed */
			SetHintBits(tuple, htup->t_len, desc, HEAP_XMIN_INVALID,
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

			SetHintBits(tuple, htup->t_len, desc, HEAP_XMAX_INVALID, InvalidTransactionId);
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
			SetHintBits(tuple, htup->t_len, desc, HEAP_XMAX_INVALID,
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
		SetHintBits(tuple, htup->t_len, desc, HEAP_XMAX_INVALID,
					InvalidTransactionId);
		return TM_Ok;
	}

	/* xmax transaction committed */

	if (HEAP_XMAX_IS_LOCKED_ONLY(tuple->t_infomask))
	{
		SetHintBits(tuple, htup->t_len, desc, HEAP_XMAX_INVALID,
					InvalidTransactionId);
		return TM_Ok;
	}

	SetHintBits(tuple, htup->t_len, desc, HEAP_XMAX_COMMITTED,
				HeapTupleHeaderGetRawXmax(tuple));
	if (!ItemPointerEquals(&htup->t_self, &tuple->t_ctid) ||
		HeapTupleHeaderIndicatesMovedPartitions(tuple))
		return TM_Updated;		/* updated by other */
	else
		return TM_Deleted;		/* deleted by other */
}


