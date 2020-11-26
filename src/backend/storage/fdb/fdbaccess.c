#include <stdlib.h>
#include <string.h>
#include "storage/fdbaccess.h"
#include "utils/elog.h"


void checkError(fdb_error_t errorNum)
{
	if(errorNum)
		elog(ERROR, "Fdb error %d : %s", errorNum, fdb_get_error(errorNum));
}

void waitAndCheckError(FDBFuture *future)
{
	checkError(fdb_future_block_until_ready(future));
	if(fdb_future_get_error(future) != 0)
	{
		checkError(fdb_future_get_error(future));
	}
}

void runNetwork()
{
	checkError(fdb_run_network());
}


FDBTransaction *fdb_tr_create(FDBDatabase *db)
{
	FDBTransaction *tr;
	checkError(fdb_database_create_transaction(db, &tr));
	return tr;
}

bool fdb_tr_commit(FDBTransaction *tr)
{
	FDBFuture *commitFuture = fdb_transaction_commit(tr);
	checkError(fdb_future_block_until_ready(commitFuture));
	if(fdb_future_get_error(commitFuture) != 0)
	{
		waitAndCheckError(fdb_transaction_on_error(tr, fdb_future_get_error(commitFuture)));
		return false;
	}
	else
		return true;
}

void fdb_tr_set(FDBTransaction *tr, char* key, int key_size, char *value,
				int value_size)
{
	fdb_transaction_set(tr, (uint8_t *) key, key_size, (uint8_t *) value,
						value_size);
}

void fdb_tr_delete(FDBTransaction *tr, char *key, int key_size)
{
	fdb_transaction_clear(tr, (uint8_t *) key, key_size);
}


void fdb_tr_destroy(FDBTransaction *tr)
{
	fdb_transaction_destroy(tr);
}

void fdb_simple_insert(FDBDatabase *db,  char* key, int key_size, char *value,
					   int value_size)
{
	bool success = false;
	FDBTransaction *tr = fdb_tr_create(db);

	for (int i = 0; i < MaxRetry; ++i)
	{
		fdb_tr_set(tr, key, key_size, value, value_size);
		if (fdb_tr_commit(tr))
		{
			success = true;
			break;
		}
	}
	if (!success)
	{
		elog(ERROR, "Fdb insert retry over %d times", MaxRetry);
	}

	fdb_tr_destroy(tr);
}

char* fdb_tr_get(FDBTransaction *tr, char *key, int key_size, int *value_size)
{
	fdb_bool_t valuePresent;
	const uint8_t *value;
	char *out_value = NULL;
	int valueLength;

	FDBFuture *getFuture = fdb_transaction_get(tr ,(uint8_t *) key, key_size, 0);
	waitAndCheckError(getFuture);

	checkError(fdb_future_get_value(getFuture, &valuePresent, &value, &valueLength));

	if (valuePresent == 0)
		return NULL;

	out_value = palloc(valueLength);
	memcpy(out_value, value, valueLength);
	*value_size = valueLength;
	fdb_future_destroy(getFuture);

	return out_value;
}

void fdb_tr_get_range(FDBTransaction *tr,
					 char *start_key, int start_key_size,
					 char *end_key, int end_key_size)
{
	fdb_bool_t outMore = 1;
	const FDBKeyValue *outKv;
	int outCount;
	int totalOut = 0;

	FDBFuture *f = fdb_transaction_get_range(tr,
											 FDB_KEYSEL_FIRST_GREATER_OR_EQUAL((uint8 *) start_key, start_key_size),
											 FDB_KEYSEL_LAST_LESS_THAN((uint8 *) end_key, end_key_size) + 1,
											 0, 0,
											 FDB_STREAMING_MODE_WANT_ALL, 0, 0, 0);

	while(outMore) {
		waitAndCheckError(f);

		checkError(fdb_future_get_keyvalue_array(f, &outKv, &outCount, &outMore));

		totalOut += outCount;

		printf("Read range key-values\n");
		for (int i = 0; i < outCount; i++)
		{
			printf("key : %.*s, value : '%.*s' \n", outKv[i].key_length,
				   outKv[i].key, outKv[i].value_length, outKv[i].value);
		}

		if(outMore) {
			FDBFuture *f2 = fdb_transaction_get_range(
					tr,
					FDB_KEYSEL_FIRST_GREATER_OR_EQUAL(outKv[outCount - 1].key, outKv[outCount - 1].key_length) + 1,
					FDB_KEYSEL_LAST_LESS_THAN(end_key, end_key_size) + 1,
					0, 0, FDB_STREAMING_MODE_WANT_ALL, 0, 0, 0);
			fdb_future_destroy(f);
			f = f2;
		}
	}
}

bool fdb_tr_get_kv(FDBTransaction *tr,
				   char *start_key, int start_key_size, bool include_start,
				   char *end_key, int end_key_size,
				   FDBFuture *f, FDBKeyValue const**out_kv, int *outCount)
{
	fdb_bool_t outMore = 1;

	if (include_start)
		f = fdb_transaction_get_range(
				tr, FDB_KEYSEL_FIRST_GREATER_OR_EQUAL((uint8 *) start_key, start_key_size),
				FDB_KEYSEL_LAST_LESS_THAN((uint8 *) end_key, end_key_size) + 1,
				0, 0,
				FDB_STREAMING_MODE_WANT_ALL, 0, 0, 0);
	else
		f = fdb_transaction_get_range(
				tr, FDB_KEYSEL_FIRST_GREATER_OR_EQUAL((uint8 *) start_key, start_key_size) + 1,
				FDB_KEYSEL_LAST_LESS_THAN((uint8 *) end_key, end_key_size) + 1,
				0, 0,
				FDB_STREAMING_MODE_WANT_ALL, 0, 0, 0);
	waitAndCheckError(f);
	checkError(fdb_future_get_keyvalue_array(f, out_kv, outCount, &outMore));

	return outMore;
}
