#include <sys/param.h>
#include <sys/systm.h>
#include <sys/lock.h>
#include <sys/malloc.h>
#include <sys/mutex.h>
#include <sys/queue.h>
#include <sys/sbuf.h>

#include <vm/uma.h>

#include "sls_internal.h"
#include "sls_kv.h"

#define SLSKV_ZONEWARM (256)

uma_zone_t slskv_zone = NULL;

int slskv_count = 0;

static int
slskv_zone_ctor(void *mem, int size, void *args __unused, int flags __unused)
{
	struct slskv_table *table;

	atomic_add_int(&slskv_count, 1);

	table = (struct slskv_table *)mem;

	table->firstfree = 0;
	table->data = NULL;

	return (0);
}

static void
slskv_zone_dtor(void *mem, int size, void *args __unused)
{
	struct slskv_table *table;

	table = (struct slskv_table *)mem;

	KASSERT(table->firstfree == 0, ("Non-empty table\n"));

	atomic_add_int(&slskv_count, -1);
}

int
slskv_init(void)
{
	slskv_zone = uma_zcreate("slstable", sizeof(struct slskv_table),
	    slskv_zone_ctor, slskv_zone_dtor, NULL, NULL,
	    UMA_ALIGNOF(struct slskv_table), 0);
	if (slskv_zone == NULL)
		return (ENOMEM);

	uma_prealloc(slskv_zone, SLSKV_ZONEWARM);

	return (0);
}

void
slskv_fini(void)
{
	if (slskv_zone != NULL)
		uma_zdestroy(slskv_zone);
}

int
slskv_create(struct slskv_table **tablep)
{
	*tablep = uma_zalloc(slskv_zone, M_NOWAIT);
	if (*tablep == NULL)
		return (ENOMEM);

	return (0);
}

void
slskv_destroy(struct slskv_table *table)
{
	uma_zfree(slskv_zone, table);
}

int
slskv_find(struct slskv_table *table, uint64_t key, uintptr_t *value)
{
	struct slskv_pair slot;
	int i;

	for (i = 0; i < table->firstfree; i++) {
		slot = table->slots[i];
		if (slot.key == key) {
			*value = slot.value;
			return (0);
		}
	}

	return (EINVAL);
}

int
slskv_add(struct slskv_table *table, uint64_t key, uintptr_t value)
{
	uintptr_t existing;

	/*
	 * XXX Check whether we can relax the
	 * call's semantics to avoid the check.
	 */
	if (slskv_find(table, key, &existing) == 0)
		return (EINVAL);

	/* XXX Double in size. */
	if (table->firstfree == SLSKV_MAXSLOTS)
		panic("Overflow");

	table->slots[table->firstfree].key = key;
	table->slots[table->firstfree].value = value;
	table->firstfree += 1;

	return (0);
}

void
slskv_del(struct slskv_table *table, uint64_t key)
{
	struct slskv_pair slot;
	int i;

	/* Get the bucket for the key. */
	for (i = 0; i < table->firstfree; i++) {
		slot = table->slots[i];
		if (slot.key == key)
			break;
	}

	if (i == table->firstfree)
		return;

	if (table->firstfree == 1) {
		table->firstfree -= 1;
		return;
	}

	table->slots[i] = table->slots[table->firstfree - 1];
	table->firstfree -= 1;
}

/*
 * Randomly grab an element from the table, remove it and
 * return it. If the table is empty, return an error.
 */
int
slskv_pop(struct slskv_table *table, uint64_t *key, uintptr_t *value)
{
	if (table->firstfree == 0)
		return (EINVAL);

	*key = table->slots[table->firstfree - 1].key;
	*value = table->slots[table->firstfree - 1].value;
	table->firstfree -= 1;

	return (0);
}

/*
 * Return an iterator for the given key-value table. Note that we cannot
 * iterate recursively or serialize, and any operations on the table
 * should be done unlocked (finds always make sense, additions sometimes,
 * deletions are dangerous and are not available).
 */
struct slskv_iter
slskv_iterstart(struct slskv_table *table)
{
	struct slskv_iter iter;

	KASSERT(table != NULL, ("iterating on NULL table\n"));

	iter.table = table;
	iter.slot = 0;

	return (iter);
}

/*
 * Return the next pair of the key-value table.
 * Signal to the caller if the iteration has ended.
 */
int
slskv_itercont(struct slskv_iter *iter, uint64_t *key, uintptr_t *value)
{
	struct slskv_table *table = iter->table;
	struct slskv_pair slot;

	if (iter->slot == table->firstfree)
		return (SLSKV_ITERDONE);

	slot = table->slots[iter->slot];
	iter->slot += 1;

	/* Export the found pair to the caller. */
	*key = slot.key;
	*value = slot.value;

	return (0);
}

int
slsset_find(slsset *table, uint64_t key)
{
	uintptr_t nothing;

	return (slskv_find(table, key, &nothing));
}

int
slsset_add(slsset *table, uint64_t key)
{
	return (slskv_add(table, key, (uintptr_t)key));
}

int
slsset_pop(slsset *table, uint64_t *key)
{
	return (slskv_pop(table, key, (uintptr_t *)key));
}
