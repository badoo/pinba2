#ifndef PINBA__BLOOM_H_
#define PINBA__BLOOM_H_

#include <memory>
#include <bitset>
#include <type_traits>

#include <boost/noncopyable.hpp>

#include <meow/utility/static_math.hpp>

#include "pinba/hash.h"

////////////////////////////////////////////////////////////////////////////////////////////////
namespace pinba {
////////////////////////////////////////////////////////////////////////////////////////////////

	template<size_t N>
	struct fixlen_bloom_t : private boost::noncopyable
	{
		// TODO: relax this restriction
		static_assert(meow::static_is_pow<N, 2>::value == true, "N must be a power of 2");

		using self_t   = fixlen_bloom_t<N>;
		using bits_t   = std::bitset<N>;

		static constexpr hash_result_t mask  = N - 1;
		static constexpr hash_result_t shift = meow::static_log2<N>::value;

		static constexpr uint32_t min_probes  = 1;
		static constexpr uint32_t max_probes  = (sizeof(hash_result_t) * 8) / shift;
		static constexpr uint32_t def_probes  = 3;

	private:
		bits_t           bits_;

	public:

		constexpr fixlen_bloom_t()
		{
			static_assert(std::is_standard_layout<self_t>::value, "don't mess with fixlen_bloom_t<>");
		}

		void add(uint32_t value, uint32_t n_probes = def_probes)
		{
			this->add_hashed(hash_number(value), def_probes);
		}

		void add_hashed(uint64_t hashed_value, uint32_t n_probes = def_probes)
		{
			// assert((n_probes >= min_probes) && (n_probes < max_probes));

			for (size_t i = 0; i < n_probes; ++i) // hope this one gets unrolled
			{
				bits_.set((hashed_value >> (shift * i)) & mask);
			}
		}

		void reset()
		{
			bits_.reset();
		}

		bool contains(self_t const& other) const
		{
			return (bits_ & other.bits_) == other.bits_;
		}

		std::string to_string() const
		{
			return bits_.to_string();
		}
	};

	// simple tests for different power of 2 sizes
	static_assert(fixlen_bloom_t<64>::mask  == 0x3f, "");
	static_assert(fixlen_bloom_t<64>::shift == 6, "");

	static_assert(fixlen_bloom_t<128>::mask  == 0x7f, "");
	static_assert(fixlen_bloom_t<128>::shift == 7, "");

	static_assert(fixlen_bloom_t<256>::mask  == 0xff, "");
	static_assert(fixlen_bloom_t<256>::shift == 8, "");

////////////////////////////////////////////////////////////////////////////////////////////////
} // namespace pinba {
////////////////////////////////////////////////////////////////////////////////////////////////

// bloom with all timer tag names from a packet
struct timertag_bloom_t : public pinba::fixlen_bloom_t<128> {};

// bloom with all timer tag names from a timer
struct timer_bloom_t : public pinba::fixlen_bloom_t<64> {};

////////////////////////////////////////////////////////////////////////////////////////////////

#endif // PINBA__BLOOM_H_
