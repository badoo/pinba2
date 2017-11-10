#ifndef PINBA__REPORT_KEY_H_
#define PINBA__REPORT_KEY_H_

#include <cstdint>
#include <meow/chunk.hpp>
#include <meow/str_ref.hpp>

#include "pinba/limits.h"

////////////////////////////////////////////////////////////////////////////////////////////////

template<size_t N>
using report_key_base_t = meow::chunk<uint32_t, N, uint32_t>;

template<size_t N>
using report_key_str_base_t = meow::chunk<meow::str_ref, N, uint32_t>;

using report_key_t     = report_key_base_t<PINBA_LIMIT___MAX_KEY_PARTS>;
using report_key_str_t = report_key_str_base_t<PINBA_LIMIT___MAX_KEY_PARTS>;

////////////////////////////////////////////////////////////////////////////////////////////////

#endif // PINBA__REPORT_KEY_H_