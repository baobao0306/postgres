#define FDB_API_VERSION 620

#include "c.h"
#include "storage/fdb_c.h"


#define palloc malloc
#define pfree free

#define MaxRetry 10

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
						int *value_size);
void fdb_tr_delete(FDBTransaction *tr, char *key, int key_size);

