#ifndef PINBA__MULTI_MERGE_H_
#define PINBA__MULTI_MERGE_H_

#include <algorithm>

// #include "binheap/binary_heap.c" // FIXME

////////////////////////////////////////////////////////////////////////////////////////////////
namespace pinba {
////////////////////////////////////////////////////////////////////////////////////////////////

	namespace { namespace detail {
		template<class Seq, class I>
		struct merge_heap_item_t
		{
			Seq *seq;   // source sequence
			I    iter;  // current value iterator
		};

		template<class Iterator>
		inline size_t maybe_calculate_size_impl(Iterator begin, Iterator end, std::random_access_iterator_tag)
		{
			return std::distance(begin, end);
		}

		template<class Iterator, class Tag>
		inline size_t maybe_calculate_size_impl(Iterator begin, Iterator end, Tag)
		{
			return 0;
		}

		template<class Iterator>
		inline size_t maybe_calculate_size(Iterator begin, Iterator end)
		{
			return maybe_calculate_size_impl(begin, end, typename std::iterator_traits<Iterator>::iterator_category());
		}

	}} // namespace { namespace detail {


	// merge a range (defined by 'begin' and 'end') of pointers to *sorted* sequences into 'result'
	// 'result' will should support
	//   - reserve(size_t)
	//   - push_back(value const&)
	//   - compare(value, value) - should return <0 for less, ==0 for equal, >0 for greater
	template<class Merger, class Iterator>
	inline void multi_merge__stdlib(Merger *result, Iterator begin, Iterator end)
	{
		using SequencePtr = typename std::iterator_traits<Iterator>::value_type;
		static_assert(std::is_pointer<SequencePtr>::value, "expected a range of pointers to sequences");

		using SequenceT    = typename std::remove_pointer<SequencePtr>::type;
		using SequenceIter = typename SequenceT::const_iterator;

		using merge_item_t = detail::merge_heap_item_t<SequenceT, SequenceIter>;

		// FIXME: benchmark labmda vs free template function
		auto const item_greater = [&result](merge_item_t const& l, merge_item_t const& r)
		{
			// return result->compare(*l.iter, *r.iter) > 0;
			return !result->compare(*l.iter, *r.iter);
		};

		size_t       result_length = 0;
		size_t const input_size = std::distance(begin, end);

		merge_item_t *heap = (merge_item_t*)alloca(input_size * sizeof(merge_item_t));
		merge_item_t *heap_end = heap + input_size;

		{
			size_t offset = 0;
			for (auto i = begin; i != end; i = std::next(i))
			{
				auto *sequence = *i;

				auto const curr_b = std::begin(*sequence);
				auto const curr_e = std::end(*sequence);

				if (curr_b == curr_e)
					continue;

				result_length += detail::maybe_calculate_size(curr_b, curr_e);
				// heap[offset++] = { .seq = sequence, .iter = curr_b };
				heap[offset].seq = sequence;
				heap[offset].iter = curr_b; // XXX: potentially fucks up the iterator in operator=(), since it has not been initialized
				offset++;
			}

			heap_end = heap + offset;
		}

		if (result_length > 0)
			result->reserve(result_length);

		std::make_heap(heap, heap_end, item_greater);

		while (heap != heap_end)
		{
			// ff::fmt(stdout, "heap: {{ ");
			// for (merge_item_t *i = heap; i != heap_end; i++)
			// {
			// 	ff::fmt(stdout, "{0}:{1} ", i->iter->bucket_id, i->iter->value);
			// }
			// ff::fmt(stdout, "}\n");

			std::pop_heap(heap, heap_end, item_greater);
			merge_item_t *last = heap_end - 1;

			result->push_back(last->seq, *last->iter);

			// advance to next item if exists
			auto const next_it = std::next(last->iter);
			if (next_it != std::end(*last->seq))
			{
				last->iter = next_it;
				std::push_heap(heap, heap_end, item_greater);
			}
			else
			{
				--heap_end;
			}
		}
	}

////////////////////////////////////////////////////////////////////////////////////////////////
#if 0
	// merge a range (defined by 'begin' and 'end') of pointers to *sorted* sequences into 'result'
	// 'result' will should support
	//   - reserve(size_t)
	//   - push_back(value const&)
	//   - compare(value, value) - should return <0 for less, ==0 for equal, >0 for greater
	template<class Merger, class Iterator>
	inline void multi_merge__binheap(Merger *result, Iterator begin, Iterator end)
	{
		using SequencePtr = typename std::iterator_traits<Iterator>::value_type;
		static_assert(std::is_pointer<SequencePtr>::value, "expected a range of pointers to sequences");

		using SequenceT    = typename std::remove_pointer<SequencePtr>::type;
		using SequenceIter = typename SequenceT::const_iterator;

		using merge_item_t = detail::merge_heap_item_t<SequenceT, SequenceIter>;

		// FIXME: benchmark labmda vs free template function
		auto const item_greater = [&result](merge_item_t const& l, merge_item_t const& r)
		{
			return result->compare(*l.iter, *r.iter) > 0;
		};

		size_t       result_length = 0;
		size_t const input_size = std::distance(begin, end);

		merge_item_t heap[input_size]; // VLA, yo
		merge_item_t *heap_end = heap + input_size;

		{
			size_t offset = 0;
			for (auto i = begin; i != end; i = std::next(i))
			{
				auto *sequence = *i;

				auto const curr_b = std::begin(*sequence);
				auto const curr_e = std::end(*sequence);

				if (curr_b == curr_e)
					continue;

				result_length += detail::maybe_calculate_size(curr_b, curr_e);
				// heap[offset++] = { .seq = sequence, .iter = curr_b };
				heap[offset].seq = sequence;
				heap[offset].iter = curr_b;
				offset++;
			}

			heap_end = heap + offset;
		}

		if (result_length > 0)
			result->reserve(result_length);

		std::make_heap(heap, heap_end, item_greater);

		while (heap != heap_end)
		{
			// ff::fmt(stdout, "heap: {{ ");
			// for (merge_item_t *i = heap; i != heap_end; i++)
			// {
			// 	ff::fmt(stdout, "{0}:{1} ", i->iter->bucket_id, i->iter->value);
			// }
			// ff::fmt(stdout, "}\n");

			std::pop_heap(heap, heap_end, item_greater);
			merge_item_t *last = heap_end - 1;

			result->push_back(last->seq, *last->iter);

			// advance to next item if exists
			auto const next_it = std::next(last->iter);
			if (next_it != std::end(*last->seq))
			{
				last->iter = next_it;
				std::push_heap(heap, heap_end, item_greater);
			}
			else
			{
				--heap_end;
			}
		}
	}
#endif

	template<class Merger, class Iterator>
	inline void multi_merge(Merger *result, Iterator begin, Iterator end)
	{
		return multi_merge__stdlib(result, begin, end);
		// return multi_merge__binheap(result, begin, end);
	}

////////////////////////////////////////////////////////////////////////////////////////////////
} // namespace pinba {
////////////////////////////////////////////////////////////////////////////////////////////////

#endif // PINBA__MULTI_MERGE_H_
