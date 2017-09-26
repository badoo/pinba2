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

struct timertag_bloom_t : public pinba::fixlen_bloom_t<128> {};

////////////////////////////////////////////////////////////////////////////////////////////////

#endif // PINBA__BLOOM_H_
