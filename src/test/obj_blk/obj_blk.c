#include "unittest.h"
#include "libpmem.h"
#include <assert.h>
#include <sys/stat.h>

#define EXAMPLE_BLK_SIZE 4

struct base {
	PMEMoid ptr;
	size_t bsize;
	size_t fsize;
};

void objblk_map(PMEMobjpool *pool, size_t bsize, size_t fsize) {
	struct base *bp = pmemobj_root_direct(pool, sizeof (*bp));
	pmemobj_tx_begin(pool, NULL);
	bp->fsize = fsize;
	bp->bsize = bsize;
	size_t pool_size = (fsize / 10) * 9;
	PMEMoid ptr = pmemobj_zalloc(pool_size);
	PMEMOBJ_SET(bp->ptr, ptr);
	pmemobj_tx_commit();
}

int objblk_read(PMEMobjpool *pool, void *buf, off_t blockno) {
	struct base *bp = pmemobj_root_direct(pool, sizeof (*bp));
	size_t pos = blockno * bp->bsize;
	void *dptr = pmemobj_direct(bp->ptr);
	dptr = (void *)((uintptr_t)dptr + pos);
	memcpy(buf, dptr, bp->bsize);
	return 0;
}

int objblk_write(PMEMobjpool *pool, const void *buf, off_t blockno) {
	struct base *bp = pmemobj_root_direct(pool, sizeof (*bp));
	pmemobj_tx_begin(pool, NULL);
	size_t pos = blockno * bp->bsize;
	void *dptr = pmemobj_direct(bp->ptr);
	dptr = (void *)((uintptr_t)dptr + pos);
	pmemobj_memcpy(dptr, buf, bp->bsize);
	pmemobj_tx_commit();
	return 0;
}
#define TEST_PAIRS_COUNT 7
struct test_pairs {
	const char *key;
	int value;
} pairs[TEST_PAIRS_COUNT] = {
	{"ABC", 1},
	{"DEF", 2},
	{"GHI", 3},
	{"JKL", 4},
	{"MNO", 5},
	{"PQR", 6},
	{"STU", 7}
};

int main(int argc, char** argv)
{
	START(argc, argv, "obj_blk");

	if (argc < 2)
		FATAL("usage: %s file", argv[0]);

	PMEMobjpool *pop = pmemobj_pool_open(argv[1]);


	struct stat st;
	stat(argv[1], &st);
	objblk_map(pop, EXAMPLE_BLK_SIZE, st.st_size);

	int i;
	for (i = 0; i < TEST_PAIRS_COUNT; ++i) {
		objblk_write(pop, pairs[i].key, pairs[i].value);
	}
	for (i = 0; i < TEST_PAIRS_COUNT; ++i) {
		char test[EXAMPLE_BLK_SIZE] = {0, };
		objblk_read(pop, test, pairs[i].value);
		assert(strcmp(pairs[i].key, test) == 0);
	}

	/* all done */
	pmemobj_pool_close(pop);

	DONE(NULL);
}

