#ifndef FDBINDEX_H
#define FDBINDEX_H

extern double fdbindex_heapscan(Relation heap,
								Relation index,
								FDBIndexBuildState *buildstate,
								IndexInfo *indexInfo);

extern FDBIndexBuildState * fdbindex_build_init(Relation heap, Relation index);
extern void fdbindex_build_finish(FDBIndexBuildState *state);
extern char * fdbindex_make_key(RelFileNode rd_node, char *tuple_key,
								int tuple_key_len);
#endif /* FDBINDEX_H */
