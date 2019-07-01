#ifndef PINBA__REPORT_UTIL_H_
#define PINBA__REPORT_UTIL_H_

#include <limits>
#include <string>
#include <vector>
#include <utility>

#include <t1ha/t1ha.h>

#include <meow/stopwatch.hpp>
#include <meow/format/format_to_string.hpp>

#include "misc/nmpa.h"

#include "pinba/globals.h"
#include "pinba/snapshot_dictionary.h"
#include "pinba/histogram.h"
#include "pinba/report_key.h"

////////////////////////////////////////////////////////////////////////////////////////////////

template<size_t N>
using report_key_impl_t = std::array<uint32_t, N>;

struct report_key_impl___hasher_t
{
	template<size_t N>
	inline size_t operator()(report_key_impl_t<N> const& key) const
	{
		return t1ha0(key.data(), key.size() * sizeof(key[0]), 0);
	}
};

struct report_key_impl___equal_t
{
	template<size_t N>
	inline bool operator()(report_key_impl_t<N> const& l, report_key_impl_t<N> const& r) const
	{
		return l == r;
	}
};

template<size_t N>
inline report_key_impl_t<N> report_key_impl___make_empty()
{
	report_key_impl_t<N> result;
	result.fill(PINBA_INTERNAL___EMPTY_KEY_PART);
	return result;
}

template<size_t N>
inline std::string report_key_impl___to_string(report_key_impl_t<N> const& k)
{
	std::string result;

	for (size_t i = 0; i < k.size(); ++i)
	{
		ff::write(result, (i == 0) ? "" : "|", k[i]);
	}

	return result;
}

////////////////////////////////////////////////////////////////////////////////////////////////

struct report_key__hasher_t
{
	template<size_t N>
	inline size_t operator()(report_key_base_t<N> const& key) const
	{
		return t1ha0(key.data(), key.size() * sizeof(typename report_key_base_t<N>::value_type), 0);
	}
};

struct report_key__equal_t
{
	template<size_t N>
	inline bool operator()(report_key_base_t<N> const& l, report_key_base_t<N> const& r) const
	{
		return (l.size() == r.size())
				? (0 == memcmp(l.data(), r.data(), (l.size() * sizeof(typename report_key_base_t<N>::value_type)) ))
				: false
				;
	}
};

template<size_t N>
std::string report_key_to_string(report_key_base_t<N> const& k)
{
	std::string result;

	for (size_t i = 0; i < k.size(); ++i)
	{
		ff::write(result, (i == 0) ? "" : "|", k[i]);
	}

	return result;
}

template<size_t N>
std::string report_key_to_string(report_key_base_t<N> const& k, dictionary_t const *dict)
{
	std::string result;

	for (size_t i = 0; i < k.size(); ++i)
	{
		ff::fmt(result, "{0}{1}<{2}>", (i == 0) ? "" : "|", k[i], dict->get_word(k[i]));
	}

	return result;
}

////////////////////////////////////////////////////////////////////////////////////////////////

struct nmpa_autofree_t : public nmpa_s
{
	nmpa_autofree_t() : nmpa_autofree_t(1024)
	{
	}

	explicit nmpa_autofree_t(size_t block_sz)
	{
		nmpa_init(this, block_sz);
	}

	nmpa_autofree_t(struct nmpa_s const& nmpa)
	{
		*((nmpa_s*)this) = nmpa;
	}

	~nmpa_autofree_t()
	{
		nmpa_free(this);
	}
};

////////////////////////////////////////////////////////////////////////////////////////////////

/*
struct report_snapshot_traits___example
{
	using src_ticks_t = ; // source ticks_ringbuffer_t
	using hashtable_t = ; // result hashtable (that we're going to iterate over)
	using totals_t    = ; // struct, holding merged report data totals

	// merge ticks from src_ticks_t to hashtable_t
	static void merge_ticks_into_data(
			  report_snapshot_ctx_t *snapshot_ctx
			, src_ticks_t& ticks
			, hashtable_t& to
			, report_snapshot_t::merge_flags_t flags);

	// calculate raw report stats
	static void calculate_raw_stats(
		  report_snapshot_ctx_t *snapshot_ctx
		, src_ticks_t& ticks
		, report_raw_stats_t *stats);

	// calculate totals over the final report
	static void calculate_totals(
		  report_snapshot_ctx_t *snapshot_ctx
		, hashtable_t const& data
		, totals_t *totals);

	// get iterator keys/values/histograms at iterator
	static report_key_t key_at_position(hashtable_t const&, iterator_t const&);
	static void*        value_at_position(hashtable_t const&, iterator_t const&);
	static histogram_t* hv_at_position(hashtable_t const&, iterator_t const&);
};
*/

struct report_raw_stats_t
{
	uint64_t row_count; // total number of rows in all ticks
};

// FIXME: stats pointer in this struct should be refcounted,
//        since report might get deleted while we're touching snapshot
struct report_snapshot_ctx_t
{
	pinba_globals_t     *globals;        // globals for logging / dictionary
	report_stats_t      *stats;          // stats that we might want to update
	report_info_t       rinfo;           // report info, immutable copy taken in ctor
	report_estimates_t  estimates;       // estimates of row count, etc. immutable copy taken in ctor
	histogram_conf_t    hv_conf;         // histogram conf, immutable copy taken in ctor
	nmpa_autofree_t     nmpa;            // snapshot-local nmpa, initialize with nmpa_create() or nmpa_init() or {}

	// extra state we should carry along with ticks
	// there is no need to merge anything really, just take them along for the lifetime of the snapshot
	// report usually should not care about these when creating snapshot
	using repacker_state_v_t = std::vector<repacker_state_ptr>;
	repacker_state_v_t  repacker_state_v;

	// report_snapshot_ctx_t(pinba_globals_t *g, report_stats_t *st, report_info_t const& ri, histogram_conf_t const& hvcf, struct nmpa_s n)
	// 	: globals(g)
	// 	, stats(st) // TODO:
	// 	, rinfo(ri)
	// 	, hv_conf(hvcf)
	// 	, nmpa(n)
	// {
	// }

	// ~report_snapshot_ctx_t()
	// {
	// 	// nmpa MUST have been initialized by the calling code
	// 	nmpa_free(&nmpa);
	// }

	pinba_logger_t* logger() const
	{
		return globals->logger();
	}
};

template<class Traits>
struct report_snapshot__impl_t
	: public report_snapshot_t
	, public report_snapshot_ctx_t
{
	using src_ticks_t = typename Traits::src_ticks_t;
	using hashtable_t = typename Traits::hashtable_t;
	using iterator_t  = typename hashtable_t::iterator;
	using totals_t    = typename Traits::totals_t;

public: // intentional, internal use only

	hashtable_t            data_;      // real data we iterate over
	src_ticks_t            ticks_;     // ticks we merge our data from (in other thread potentially)
	snapshot_dictionary_t  snap_d_;    // local snapshot dictionary word_id -> word cache
	totals_t               totals_;    // totals storage
	bool                   prepared_;  // has data been prepared?

public:

	report_snapshot__impl_t(report_snapshot_ctx_t ctx, src_ticks_t const& ticks)
		: report_snapshot_ctx_t(ctx)
		, ticks_(ticks)
		, snap_d_(ctx.globals->dictionary())
		, prepared_(false)
	{
	}

private:

	virtual report_info_t const* report_info() const override
	{
		return &rinfo;
	}

	virtual histogram_conf_t const* histogram_conf() const override
	{
		return &hv_conf;
	}

	virtual dictionary_t const* dictionary() const override
	{
		return globals->dictionary();
	}

	virtual snapshot_dictionary_t const* snapshot_dictionary() const override
	{
		return &snap_d_;
	}

	virtual void prepare(merge_flags_t flags) override
	{
		if (this->is_prepared())
			return;

		report_raw_stats_t raw_stats = {};

		if (this->stats)
		{
			Traits::calculate_raw_stats(this, ticks_, &raw_stats);
			this->stats->last_snapshot_src_rows = raw_stats.row_count;
		}

		// accumulate repacker state
		this->repacker_state_v.reserve(ticks_.size());
		for (auto& tick : ticks_)
		{
			if (!tick)
				continue;

			this->repacker_state_v.push_back(tick->repacker_state);
		}

		// merge, measure the time
		meow::stopwatch_t sw;

		Traits::merge_ticks_into_data(this, ticks_, data_, flags);

		prepared_ = true;

		if (this->stats)
		{
			this->stats->last_snapshot_merge_d = duration_from_timeval(sw.stamp());
			this->stats->last_snapshot_uniq_rows = this->row_count();
		}

		if (flags & merge_flags::with_totals)
			Traits::calculate_totals(this, data_, &totals_);

		// do NOT clear ticks here, as snapshot impl might want to keep ref to it
		// ticks_.clear();
	}

	virtual bool is_prepared() const override
	{
		return prepared_;
	}

	virtual size_t row_count() const override
	{
		return data_.size();
	}

private:

	static_assert(sizeof(iterator_t) <= sizeof(position_t), "position_t must be able to hold iterator contents");

	static inline position_t const& position_from_iterator(iterator_t const& it)
	{
		return reinterpret_cast<position_t const&>(it);
	}

	static inline iterator_t const& iterator_from_position(position_t const& pos)
	{
		return reinterpret_cast<iterator_t const&>(pos);
	}

private:

	virtual position_t pos_first() override
	{
		return position_from_iterator(data_.begin());
	}

	virtual position_t pos_last() override
	{
		return position_from_iterator(data_.end());
	}

	virtual position_t pos_next(position_t const& pos) override
	{
		static_assert(std::is_base_of<
				std::forward_iterator_tag,
				typename std::iterator_traits<iterator_t>::iterator_category>::value,
			"forward iteration support");

		auto const& it = iterator_from_position(pos);
		return position_from_iterator(std::next(it));
	}

	// virtual position_t pos_prev(position_t const& pos) override
	// {
	// 	static_assert(std::is_base_of<
	// 			std::bidirectional_iterator_tag,
	// 			typename std::iterator_traits<iterator_t>::iterator_category>::value,
	// 		"backward iteration support");

	// 	auto const& it = iterator_from_position(pos);
	// 	return position_from_iterator(std::prev(it));
	// }

	virtual bool pos_equal(position_t const& l_pos, position_t const& r_pos) const override
	{
		auto const& l_it = iterator_from_position(l_pos);
		auto const& r_it = iterator_from_position(r_pos);
		return (l_it == r_it);
	}

	virtual report_key_t get_key(position_t const& pos) const override
	{
		auto const& it = iterator_from_position(pos);
		return Traits::key_at_position(data_, it);
	}

	virtual report_key_str_t get_key_str(position_t const& pos) const override
	{
		report_key_t k = this->get_key(pos);

		report_key_str_t result;
		for (uint32_t i = 0; i < k.size(); ++i)
		{
			str_ref const word = dictionary()->get_word(k[i]);
			// str_ref const word = snapshot_dictionary()->get_word(k[i]);
			result.push_back(word);
		}
		return result;
	}

	virtual int data_kind() const override
	{
		return rinfo.kind;
	}

	virtual void* get_data(position_t const& pos) override
	{
		auto const& it = iterator_from_position(pos);
		return Traits::value_at_position(data_, it);
	}

	virtual void* get_data_totals() const override
	{
		return (void*)&totals_;
	}

	virtual int histogram_kind() const override
	{
		return rinfo.hv_kind;
	}

	virtual void* get_histogram(position_t const& pos) override
	{
		if (!rinfo.hv_enabled)
			return nullptr;

		auto const& it = iterator_from_position(pos);
		return Traits::hv_at_position(data_, it);
	}
};

////////////////////////////////////////////////////////////////////////////////////////////////

template<class T>
struct ticks_ringbuffer_t : private boost::noncopyable
{
	using value_type = T;

	struct tick_t : private boost::noncopyable
	{
		timeval_t   start_tv;
		timeval_t   end_tv;
		value_type  data;

		explicit tick_t(timeval_t curr_tv)
			: start_tv(curr_tv)
			, end_tv({0,0})
			, data()
		{
			PINBA_STATS_(objects).n_report_ticks++;
		}

		~tick_t()
		{
			PINBA_STATS_(objects).n_report_ticks--;
		}
	};

	using tick_ptr     = std::shared_ptr<tick_t>;
	using ringbuffer_t = std::vector<tick_ptr>;

public:

	ticks_ringbuffer_t(uint32_t tick_count)
		: tick_count_(tick_count)
	{
	}

	void init(timeval_t curr_tv)
	{
		for (uint32_t i = 0; i < tick_count_; i++)
			ticks_.push_back({});

		curr_tick_.reset(new tick_t(curr_tv));
	}

	void tick(timeval_t curr_tv)
	{
		curr_tick_->end_tv = curr_tv;           // finish current tick

		ticks_.push_back(curr_tick_);           // copy tick to history
		curr_tick_.reset(new tick_t(curr_tv));  // and create new tick at 'current'

		// maybe truncate history, if it has grown too long
		if (ticks_.size() >  tick_count_)
		{
			ticks_.erase(ticks_.begin()); // XXX: O(N)
		}
	}

	ringbuffer_t const& get_internal_buffer() const
	{
		return ticks_;
	}

	tick_t* last()
	{
		return ticks_.back().get();
	}

	tick_t& current()
	{
		return *curr_tick_;
	}

private:
	uint32_t      tick_count_;
	ringbuffer_t  ticks_;
	tick_ptr      curr_tick_;
};

////////////////////////////////////////////////////////////////////////////////////////////////

struct report_history_ringbuffer_t : private boost::noncopyable
{
	using ringbuffer_t = std::vector<report_tick_ptr>;

public:

	report_history_ringbuffer_t(uint32_t max_ticks)
		: max_ticks_(max_ticks)
	{
	}

	report_tick_ptr append(report_tick_ptr tick)
	{
		report_tick_ptr result = {};

		ringbuffer_.emplace_back(std::move(tick));
		if (ringbuffer_.size() > max_ticks_)
		{
			result = std::move(*ringbuffer_.begin());
			ringbuffer_.erase(ringbuffer_.begin()); // XXX: O(n)
		}

		return result;
	}

	ringbuffer_t const& get_ringbuffer() const
	{
		return ringbuffer_;
	}

private:
	uint32_t      max_ticks_;
	ringbuffer_t  ringbuffer_;
};

////////////////////////////////////////////////////////////////////////////////////////////////

inline histogram_conf_t histogram___configure_with_rinfo(report_info_t const& rinfo)
{
	if (!rinfo.hv_enabled)
		return {};

	histogram_conf_t hv_conf = {
		.min_value      = rinfo.hv_min_value,
		.max_value      = rinfo.hv_min_value + (rinfo.hv_bucket_count * rinfo.hv_bucket_d),
		.unit_size      = rinfo.hv_bucket_d, // NOTE: if you change this to be != bucket_d, fix conversion hdr -> flat
		.precision_bits = 7,
		.bucket_d       = rinfo.hv_bucket_d,
		.hdr            = {},
	};

	auto const err = hdr_histogram_configure(&hv_conf.hdr, hv_conf);
	if (err)
		throw std::runtime_error(ff::fmt_str("bad histogram config: {0}", err));

	return hv_conf;
}

////////////////////////////////////////////////////////////////////////////////////////////////

#endif // PINBA__REPORT_UTIL_H_
