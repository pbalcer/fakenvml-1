#include "unittest.h"
#include "libpmem.h"
#include <assert.h>

#define PMEM(x) PMEMoid
#define LEAFS_PER_NODE 127

typedef struct node_s node_t;
typedef struct iterator_s iterator_t;

struct node_s {
	PMEM(node_t) leafs[127];
	int value;
};

struct base {
	PMEM(node_t) root;
};

void kvs_set(PMEMobjpool *pool, const char *key, int value) {
	struct base *bp = pmemobj_root_direct(pool, sizeof (*bp));

	pmemobj_tx_begin(pool, NULL);
	int i;
	PMEM(node_t) *node = &(bp->root);
	node_t *dnode = NULL;
	for (i = 0; i < strlen(key); ++i) {
		dnode = pmemobj_direct(*node);
		if (dnode == NULL) {
			PMEM(node_t) nnode = pmemobj_zalloc(sizeof(node_t));
			PMEMOBJ_SET(*node, nnode);
			dnode = pmemobj_direct(*node);
		}
		node = &(dnode->leafs[(int)key[i]]);
	}
	pmemobj_memcpy(&dnode->value, &value, sizeof(int));

	pmemobj_tx_commit();
}

void kvs_delete(PMEMobjpool *pool, const char *key) {
	kvs_set(pool, key, 0);
}

int kvs_get(PMEMobjpool *pool, const char *key) {
	struct base *bp = pmemobj_root_direct(pool, sizeof (*bp));
	int i;
	PMEM(node_t) node = bp->root;
	node_t *dnode;
	for (i = 0; i < strlen(key); ++i) {
		dnode = pmemobj_direct(node);
		if (dnode == NULL) {
			break;
		}
		node = dnode->leafs[(int)key[i]];
	}

	return dnode != NULL ? dnode->value : 0;
}

#define TEST_PAIRS_COUNT 7

struct test_pairs {
	const char *key;
	int value;
} pairs[TEST_PAIRS_COUNT] = {
	{"AaAaA", 1},
	{"aAaAa", 2},
	{"ABCDE", 3},
	{"abcde", 4},
	{"123456", 5},
	{"1234567", 6},
	{"12345", 7}
};

int main(int argc, char** argv)
{
	START(argc, argv, "obj_kvstore");

	if (argc < 2)
		FATAL("usage: %s file", argv[0]);

	PMEMobjpool *pop = pmemobj_pool_open(argv[1]);

	int i;
	for (i = 0; i < TEST_PAIRS_COUNT; ++i) {
		kvs_set(pop, pairs[i].key, pairs[i].value);
	}
	for (i = 0; i < TEST_PAIRS_COUNT; ++i) {
		assert(kvs_get(pop, pairs[i].key) == pairs[i].value);
	}

	/* all done */
	pmemobj_pool_close(pop);

	DONE(NULL);
}

