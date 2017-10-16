#ifndef PINBA__BLOOM_H_
#define PINBA__BLOOM_H_

#include <memory>
#include <bitset>
#include <type_traits>

#include <boost/noncopyable.hpp>

#include "t1ha/t1ha.h"

////////////////////////////////////////////////////////////////////////////////////////////////
namespace pinba {
////////////////////////////////////////////////////////////////////////////////////////////////

	template<size_t N>
	struct fixlen_bloom_t : private boost::noncopyable
	{
		using self_t = fixlen_bloom_t<N>;
		using bits_t = std::bitset<N>;

	private:
		bits_t           bits_;

	public:

		constexpr fixlen_bloom_t()
		{
			static_assert(std::is_standard_layout<self_t>::value, "don't mess with fixlen_bloom_t<>");
		}

		void add(uint32_t value)
		{
			// TODO: could just take log2(N) bits from uint64_t word here, to get rid of extra hashing
			//       for 128 bit bloom, need 7 bits for position, so could take 3 times 7 bits
			//       from diff parts of the 64bit hash result
			//
			//       this does not work with all values of N, but can work around that
			bits_.set(bloom___hash(value) % bits_.size());
			bits_.set(bloom___hash(value ^ bloom___rot32(value, 13)) % bits_.size());
			bits_.set(bloom___hash(value ^ bloom___rot32(value, 25)) % bits_.size());
		}

		void clear()
		{
			bits_.clear();
		}

		bool contains(self_t const& other) const
		{
			return (bits_ & other.bits_) == other.bits_;
		}

		std::string to_string() const
		{
			return bits_.to_string();
		}

	private:

		static constexpr inline uint32_t bloom___rot32(uint32_t v, unsigned s)
		{
			return (v >> s) | (v << (32 - s));
		}

		static inline uint64_t bloom___hash(uint32_t key)
		{
			return t1ha0(&key, sizeof(key), key);
		}
	};

////////////////////////////////////////////////////////////////////////////////////////////////
} // namespace pinba {
////////////////////////////////////////////////////////////////////////////////////////////////

// TODO: experiment with diff bloom sizes
struct timertag_bloom_t : public pinba::fixlen_bloom_t<128> {};

////////////////////////////////////////////////////////////////////////////////////////////////

#endif // PINBA__BLOOM_H_
