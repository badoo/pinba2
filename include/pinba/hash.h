#ifndef PINBA__HASH_H_
#define PINBA__HASH_H_

#include <cstdint>
#include <type_traits>

#include <meow/str_ref.hpp>

#include <t1ha/t1ha.h>

////////////////////////////////////////////////////////////////////////////////////////////////
namespace pinba {
////////////////////////////////////////////////////////////////////////////////////////////////

	inline uint64_t hash_string(meow::str_ref key)
	{
		return t1ha0(key.data(), key.size(), 0);
	}

	struct string_hasher_t
	{
		using result_type   = uint64_t;
		using argument_type = meow::str_ref;

		inline result_type operator()(argument_type key) const
		{
			return hash_string(key);
		}
	};

////////////////////////////////////////////////////////////////////////////////////////////////

	template<class T>
	inline
	typename std::enable_if<std::is_integral<uint64_t>::value, uint64_t>::type
	hash_number(T key)
	{
		return t1ha0(&key, sizeof(key), key);
	}

	template<class T>
	struct number_hasher_t
	{
		using result_type   = uint64_t;
		using argument_type = T;

		inline result_type operator()(argument_type key) const
		{
			return hash_number(key);
		}
	};

////////////////////////////////////////////////////////////////////////////////////////////////
} // namespace pinba {
////////////////////////////////////////////////////////////////////////////////////////////////

#endif // PINBA__HASH_H_
