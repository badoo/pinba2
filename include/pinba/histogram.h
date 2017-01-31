#ifndef PINBA__HISTOGRAM_H_
#define PINBA__HISTOGRAM_H_

#include <cstdint>
#include <boost/noncopyable.hpp>
#include <sparsehash/sparse_hash_map>

////////////////////////////////////////////////////////////////////////////////////////////////

struct histogram_t
	// FIXME: put this back, when report internal row structures, containing this one by value, stop being copyable
	// : private boost::noncopyable
{
	// map time_interval -> request_count
	//
	// sparse_hash_map is said to be more efficient memory-wise
	// and we kinda need that, since there is a histogram per row per timeslice
	//
	// TODO: a couple of ideas on tuning this
	//  1. maybe make hash map impl configureable, so that if report has few rows
	//     a faster hash map can be use (like dense_hash_map)
	//  2. experiment with hash function for integers here (maybe a simple identity will work?)
	//  3. control hash map grows as much as possible, to save memory (at least use small initial size)
	//  4. keep as little state in this object as possible to save memory
	//     all confguration information (like histogram max size and hash initial size can be passed from outside)
	//  5. maybe use memory pools to allocate objects like this one (x84_64)
	//     sizeof(unordered_map) == 56    // as of gcc 4.9.4
	//     sizeof(dense_hash_map) == 80
	//     sizeof(sparse_hash_map) == 88
	//  6. try https://github.com/greg7mdp/sparsepp (within 1% of sparse_hash_map by mem usage, but faster lookup/insert)
	//     sizeof(spp::sparse_hash_map) == 88
	//  7. sparse_hash_set should use less memory due to using uint32_t but expect 64bit alignments?
	//  8. maybe use google in-memory btree, since we need to sort to calc percentiles,
	//     or do N a zillion hash lookups to traverse hashmap in a 'sorted' way by key
	//
	typedef google::sparse_hash_map<uint32_t, uint32_t> map_t;
	map_t map;
};

////////////////////////////////////////////////////////////////////////////////////////////////

#endif // PINBA__HISTOGRAM_H_
