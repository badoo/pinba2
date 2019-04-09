
	/* (c) 2007-2012 Andrei Nigmatulin, modified slightly for pinba2 by Anton Povarov */

#ifndef PINBA__MISC_ARRAY_H
#define PINBA__MISC_ARRAY_H 1

#include <assert.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>

#ifdef __cplusplus
#define __builtin_types_compatible_p(a,b) 1 /* this builtin is unavailable in C++ mode */
#endif

void unexpected_at_compile_time()
#if defined(__GNUC__) && !defined(__clang__ /* almost a gnuc but some bits still missing */)
	 __attribute__((error("unexpected at compile time")))
#endif
;

struct array_s {
	void *data;
	size_t sz;
	unsigned long used;
	unsigned long allocated;
};

typedef struct array_s array_t;

#define array_init array_init0

#define array_v(a, type) ({ \
	if (!__builtin_types_compatible_p(__typeof__(a), struct array_s *) && \
		!__builtin_types_compatible_p(__typeof__(a), const struct array_s *)) { \
		unexpected_at_compile_time(); \
	} \
	((type *) (a)->data); })

#define array_init_static(a) (struct array_s) { .data = a, .sz = sizeof(a[0]), .allocated = sizeof(a) / sizeof(a[0]) }

#define array_push_v(a, type) ({ \
	if (!__builtin_types_compatible_p(__typeof__(a), struct array_s *)) { \
		unexpected_at_compile_time(); \
	} \
	((type *) array_push(a)); })

static inline int array_shrink_with_ratio(struct array_s *a, float ratio);
static inline int array_reserve_with_ratio(struct array_s *a, unsigned long count, float ratio);

static inline struct array_s *array_init0(struct array_s *a, size_t sz, unsigned long initial_num)
{
	void *allocated = 0;

	if (!a) {
		a = (struct array_s *) malloc(sizeof(struct array_s));

		if (!a) {
			return 0;
		}

		allocated = a;
	}

	a->sz = sz;

	if (initial_num) {
		/*
		don't calloc, there is no need to zerofill

		a->data = calloc(sz, initial_num);
		*/

		a->data = malloc(sz * initial_num);

		if (!a->data) {
			free(allocated);
			return 0;
		}
	}
	else {
		a->data = 0;
	}

	a->allocated = initial_num;
	a->used = 0;

	return a;
}

static inline void *array_item(struct array_s *a, unsigned long n)
{
	void *ret;

	ret = (char *) a->data + a->sz * n;

	return ret;
}

static inline size_t array_mem_used(const struct array_s *a)
{
	return a->sz * a->allocated;
}

static inline void *array_item_last(struct array_s *a)
{
	return array_item(a, a->used - 1);
}

static inline unsigned long array_item_remove(struct array_s *a, unsigned long n)
{
	/* XXX: it's broken for size_t */
	size_t ret = ~ (size_t) 0;

	if (n < a->used - 1) {
		void *last = array_item(a, a->used - 1);
		void *to_remove = array_item(a, n);

		memcpy(to_remove, last, a->sz);

		ret = n;
	}

	--a->used;

	return ret;
}

static inline unsigned long array_item_remove_with_shrink(struct array_s *a, unsigned long n, float ratio)
{
	unsigned long ret = array_item_remove(a, n);
	array_shrink_with_ratio(a, ratio);
	return ret;
}

static inline void array_item_remove_with_shift(struct array_s *a, unsigned long n)
{
	if (n >= a->used) {
		assert(!"n >= a->used");
		return;
	}

	memmove(array_item(a, n), array_item(a, n + 1), a->sz * (a->used - n - 1));
	a->used --;
}

static inline void array_item_remove_with_shift_with_shrink(struct array_s *a, unsigned long n, float ratio)
{
	array_item_remove_with_shift(a, n);
	array_shrink_with_ratio(a, ratio);
}

static inline unsigned long array_item_no(struct array_s *a, void *item)
{
	return ((uintptr_t) item - (uintptr_t) a->data) / a->sz;
}

static inline unsigned long array_item_remove_ptr(struct array_s *a, void *item)
{
	return array_item_remove(a, array_item_no(a, item));
}

static inline int array_item_in(struct array_s *a, void *item)
{
	if (item < a->data || (char *) item >= (char *) a->data + a->sz * a->used) {
		return 0;
	}

	return 1;
}

static inline void *array_push(struct array_s *a)
{
	void *ret;

	if (-1 == array_reserve_with_ratio(a, 1, 2.0)) {
		return 0;
	}

	ret = array_item(a, a->used);

	++a->used;

	return ret;
}

static inline int array_enlarge(struct array_s *a, unsigned long new_sz, int clean)
{
	void *new_ptr = realloc(a->data, a->sz * new_sz);

	if (!new_ptr) {
		return -1;
	}

	if (clean && new_sz > a->allocated) {
		memset((char *) new_ptr + a->sz * a->allocated, 0, a->sz * (new_sz - a->allocated));
	}

	a->data = new_ptr;
	a->allocated = new_sz;

	return 0;
}

static inline int array_shrink_with_ratio(struct array_s *a, float ratio)
{
	if (ratio < 1.0) {
		assert(!"ratio < 1.0");
	}

	if (a->used == a->allocated) {
		return 0;
	}

	unsigned long new_allocated = a->used * ratio;
	size_t new_size = a->sz * new_allocated;

	if (new_allocated >= a->allocated) {
		return 0;
	}

	if (new_allocated == 0) {
		free(a->data);
		a->data = NULL;
	} else {
		void *new_ptr = realloc(a->data, new_size);
		if (!new_ptr) {
			return -1;
		}
		a->data = new_ptr;
	}

	a->allocated = new_allocated;

	return 0;
}

static inline int array_shrink(struct array_s *a)
{
	return array_shrink_with_ratio(a, 1.0);
}

static inline int array___reserve_with_ratio(struct array_s *a, unsigned long count, float ratio, int clean)
{
	if (a->used + count <= a->allocated) {
		return 0;
	}

	unsigned long new_sz = (a->used + count) * ratio;

	return array_enlarge(a, new_sz, clean);
}

static inline int array_reserve_with_ratio_and_clean(struct array_s *a, unsigned long count, float ratio)
{
	return array___reserve_with_ratio(a, count, ratio, 1);
}

static inline int array_reserve_with_ratio(struct array_s *a, unsigned long count, float ratio)
{
	return array___reserve_with_ratio(a, count, ratio, 0);
}

static inline int array_reserve_and_clean(struct array_s *a, unsigned long count)
{
	return array_reserve_with_ratio_and_clean(a, count, 2.0);
}

static inline int array_reserve(struct array_s *a, unsigned long count)
{
	return array_reserve_with_ratio(a, count, 2.0);
}

static inline void array_free(struct array_s *a)
{
	free(a->data);
	a->data = 0;
	a->sz = 0;
	a->used = a->allocated = 0;
}

static inline int array_copy(struct array_s *d, const struct array_s *s)
{
	if (0 == array_init0(d, s->sz, s->allocated)) {
		return -1;
	}

	memcpy(d->data, s->data, s->sz * s->used);
	d->used = s->used;

	return 0;
}

static inline int array_append(struct array_s *d, const struct array_s *s)
{
	if (d->sz != s->sz) {
		assert(!"d->sz != s->sz");
		return -1;
	}

	if (0 > array_reserve(d, s->used)) {
		return -1;
	}

	memcpy(array_item(d, d->used), s->data, s->sz * s->used);

	d->used += s->used;

	return 0;
}

#endif /* PINBA__MISC_ARRAY_H */
