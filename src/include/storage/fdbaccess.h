#ifndef FDBACCESS_H
#define FDBACCESS_H

#define FDB_API_VERSION 620

#include "c.h"
#include "fdb_c.h"

#define MaxRetry 10
typedef enum FDBForkNumber
{
	FDB_InvalidForkNumber = -1,
	FDB_MAIN_FORKNUM = 0,
	FDB_SEQUENCE_FORKNUM = 1
} FDBForkNumber;

extern void checkError(fdb_error_t errorNum);

extern void waitAndCheckError(FDBFuture *future);

extern void runNetwork();

extern FDBTransaction *fdb_tr_create(FDBDatabase *db);

extern bool fdb_tr_commit(FDBTransaction *tr);

extern void fdb_tr_set(FDBTransaction *tr, char* key, int key_size, char *value,
					   int value_size);

extern void fdb_tr_destroy(FDBTransaction *tr);

extern void fdb_simple_insert(FDBDatabase *db,  char* key, int key_size,
							  char *value, int value_size);

extern char* fdb_tr_get(FDBTransaction *tr, char *key, int key_size,
						uint32 *value_size);
extern void fdb_tr_delete(FDBTransaction *tr, char *key, int key_size);

extern bool fdb_tr_get_kv(FDBTransaction *tr,
				   char *start_key, int start_key_size, bool include_start,
				   char *end_key, int end_key_size,
				   FDBFuture *f, FDBKeyValue const**out_kv, int *outCount);


#endif							/* FDBACCESS_H */
