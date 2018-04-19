#ifndef PINBA__LIMITS_H_
#define PINBA__LIMITS_H_

#include <limits>

////////////////////////////////////////////////////////////////////////////////////////////////

// TUNABLE limits

// max number of key parts we support for reports
#ifndef PINBA_LIMIT___MAX_KEY_PARTS
#define PINBA_LIMIT___MAX_KEY_PARTS 15
#endif

// max histogram size
// here mostly for sanity check, no idea who'd want this kind of precision honestly
#ifndef PINBA_LIMIT___MAX_HISTOGRAM_SIZE
#define PINBA_LIMIT___MAX_HISTOGRAM_SIZE (100 * 1000 * 1000)
#endif


// INTERNAL limits
// don't change these unless you REALLY know what you're doing
#define PINBA_INTERNAL___UINT32_MAX         (std::numeric_limits<uint32_t>::max())
#define PINBA_INTERNAL___EMPTY_KEY_PART     PINBA_INTERNAL___UINT32_MAX
#define PINBA_INTERNAL___EMPTY_HV_BUCKET_ID PINBA_INTERNAL___UINT32_MAX
#define PINBA_INTERNAL___STATUS_MAX         PINBA_INTERNAL___UINT32_MAX


//
static_assert(PINBA_LIMIT___MAX_KEY_PARTS      < PINBA_INTERNAL___UINT32_MAX,         "oh come on!");
static_assert(PINBA_LIMIT___MAX_HISTOGRAM_SIZE < PINBA_INTERNAL___EMPTY_HV_BUCKET_ID, "oh come on!");

#endif // PINBA__LIMITS_H_
