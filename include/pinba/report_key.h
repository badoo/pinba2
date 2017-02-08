#ifndef PINBA__REPORT_KEY_H_
#define PINBA__REPORT_KEY_H_

#include <cstdint>
#include <meow/chunk.hpp>
#include <meow/str_ref.hpp>

using meow::str_ref;

////////////////////////////////////////////////////////////////////////////////////////////////

// max number of key parts we support for reports
#define REPORT_MAX_KEY_PARTS 7

template<size_t N>
using report_key_base_t = meow::chunk<uint32_t, N, uint32_t>;

template<size_t N>
using report_key_str_base_t = meow::chunk<str_ref, N, uint32_t>;

using report_key_t     = report_key_base_t<REPORT_MAX_KEY_PARTS>;
using report_key_str_t = report_key_str_base_t<REPORT_MAX_KEY_PARTS>;

namespace detail {
	template<size_t N>
	struct report_key___padding_checker
	{
		report_key___padding_checker()
		{
			static_assert(sizeof(report_key_base_t<N>) == ((N + 1) * sizeof(uint32_t)), "ensure no padding within report_key_base_t");
		}
	};
	report_key___padding_checker<1> const report_key___padding_checker__1__;
	report_key___padding_checker<2> const report_key___padding_checker__2__;
	report_key___padding_checker<3> const report_key___padding_checker__3__;
	report_key___padding_checker<4> const report_key___padding_checker__4__;
	report_key___padding_checker<5> const report_key___padding_checker__5__;
	report_key___padding_checker<6> const report_key___padding_checker__6__;
	report_key___padding_checker<7> const report_key___padding_checker__7__;
} // namespace detail {

////////////////////////////////////////////////////////////////////////////////////////////////

#endif // PINBA__REPORT_KEY_H_