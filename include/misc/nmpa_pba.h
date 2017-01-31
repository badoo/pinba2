
	/* (c) 2013 Andrei Nigmatulin */

	/* Naive memory pool allocator support for protobuf-c */

#ifndef PINBA__MISC_NMPA_PBA_H
#define PINBA__MISC_NMPA_PBA_H 1

#include "misc/nmpa.h"

static inline void *nmpa___pba_alloc(void *v, size_t sz)
{
	struct nmpa_s *nmpa = (__typeof__(nmpa))v;
	return nmpa_alloc(nmpa, sz);
}


static inline void nmpa___pba_free(void *v __attribute__ ((unused)), void *ptr __attribute__ ((unused)))
{
}


#define nmpa_pba(nmpa) (& (ProtobufCAllocator) { \
	.alloc = nmpa___pba_alloc, \
	.free = nmpa___pba_free, \
	.allocator_data = (nmpa) \
	} )


#endif /* PINBA__MISC_NMPA_PBA_H */
