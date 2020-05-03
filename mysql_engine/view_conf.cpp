#include "mysql_engine/pinba_mysql.h"
#include "mysql_engine/view_conf.h"

#include <meow/str_ref_algo.hpp>
#include <meow/convert/number_from_string.hpp>

#include "pinba/globals.h"
#include "pinba/dictionary.h"
#include "pinba/report_by_request.h"
#include "pinba/report_by_timer.h"
#include "pinba/report_by_packet.h"

////////////////////////////////////////////////////////////////////////////////////////////////
namespace { namespace aux {
////////////////////////////////////////////////////////////////////////////////////////////////

	struct pinba_view_conf___internal_t : public pinba_view_conf_t
	{
		report_conf___by_packet_t   by_packet_conf;
		report_conf___by_request_t  by_request_conf;
		report_conf___by_timer_t    by_timer_conf;

		virtual report_conf___by_packet_t const*   get___by_packet() const override
		{
			assert(this->kind == pinba_view_kind::report_by_packet_data);
			return &this->by_packet_conf;
		}

		virtual report_conf___by_request_t const*  get___by_request() const override
		{
			assert(this->kind == pinba_view_kind::report_by_request_data);
			return &this->by_request_conf;
		}

		virtual report_conf___by_timer_t const*    get___by_timer() const override
		{
			assert(this->kind == pinba_view_kind::report_by_timer_data);
			return &this->by_timer_conf;
		}
	};

////////////////////////////////////////////////////////////////////////////////////////////////

	static pinba_error_t parse_aggregation_window(pinba_view_conf_t *vcf, str_ref aggregation_spec)
	{
		auto const time_window_v = meow::split_ex(aggregation_spec, ",");
		if (time_window_v.size() == 0)
			return ff::fmt_err("time_window must be set");

		auto const time_window_s = time_window_v[0];

		if (time_window_s == "default_history_time")
		{
			vcf->time_window = P_CTX_->settings.time_window;
			vcf->tick_count  = P_CTX_->settings.tick_count;
		}
		else
		{
			uint32_t time_window;
			if (!meow::number_from_string(&time_window, time_window_s))
				return ff::fmt_err("bad seconds_spec: '{0}', expected integer number of seconds", time_window_s);

			uint32_t tick_width_sec = 1; // the default
			if (time_window_v.size() > 1)
			{
				if (!meow::number_from_string(&tick_width_sec, time_window_v[1]))
					return ff::fmt_err("bad tick_width_spec: '{0}', expected integer number of seconds", time_window_v[1]);

				// check we can fit whole number of ticks into the time_window
				if (time_window != ((time_window / tick_width_sec) * tick_width_sec))
					return ff::fmt_err("can't fit a whole number of tick_width-s ({0}) into time_window ({1})", tick_width_sec, time_window);
			}

			vcf->time_window = time_window * d_second;
			vcf->tick_count  = time_window / tick_width_sec;
		}

		return {};
	}


	static pinba_error_t parse_keys(pinba_view_conf_t *vcf, str_ref key_spec)
	{
		if (key_spec == "no_keys")
			return {};

		auto const keys_v = meow::split_ex(key_spec, ",");

		for (auto const& key_s : keys_v)
		{
			if (key_s.empty())
				continue;

			vcf->keys.push_back(key_s);
		}

		if (vcf->keys.size() > PINBA_LIMIT___MAX_KEY_PARTS)
			return ff::fmt_err("we suport maximum of {0} keys (this is a tunable compile-time constant)", PINBA_LIMIT___MAX_KEY_PARTS);

		return {};
	}


	static pinba_error_t parse_histogram_and_percentiles(pinba_view_conf_t *vcf, str_ref histogram_spec)
	{
		if (histogram_spec == "no_percentiles")
			return {};

		bool hv_present = false;

		auto const pct_v = meow::split_ex(histogram_spec, ",");

		for (auto const& pct_s : pct_v)
		{
			if (meow::prefix_compare(pct_s, "hv=")) // 3 chars
			{
				auto const hv_range_s  = meow::sub_str_ref(pct_s, 3, pct_s.size());
				auto const hv_values_v = meow::split_ex(hv_range_s, ":");

				if (hv_values_v.size() != 3)
					return ff::fmt_err("3 integer parts split by ':' expected");

				// LOG_DEBUG(P_L_, "hv_values_v = {{ {0}, {1}, {2} }", hv_values_v[0], hv_values_v[1], hv_values_v[2]);

				// hv=<hv_lower_time_ms>:<hv_upper_time_ms>:<hv_bucket_count>
				uint32_t hv_lower_ms;
				if (!meow::number_from_string(&hv_lower_ms, hv_values_v[0]))
					return ff::fmt_err("can't parse hv_lower_ms from '{0}'", pct_s);

				uint32_t hv_upper_ms;
				if (!meow::number_from_string(&hv_upper_ms, hv_values_v[1]))
					return ff::fmt_err("can't parse hv_upper_ms from '{0}'", pct_s);

				uint32_t hv_bucket_count;
				if (!meow::number_from_string(&hv_bucket_count, hv_values_v[2]))
					return ff::fmt_err("can't parse hv_bucket_count from '{0}'", pct_s);

				// TODO: add separate validation step (and store raw values here, no division by hv_bucket_count!)

				if (hv_upper_ms <= hv_lower_ms)
					return ff::fmt_err("histogram_spec: hv_upper_ms must be >= hv_lower_ms, in '{0}'", pct_s);

				if (hv_bucket_count == 0)
					return ff::fmt_err("histogram_spec: hv_bucket_count must be >= 0, in '{0}'", pct_s);

				hv_present = true;

				vcf->hv_bucket_count = hv_bucket_count;
				vcf->hv_bucket_d     = (hv_upper_ms - hv_lower_ms) * d_millisecond / hv_bucket_count;
				vcf->hv_min_value    = hv_lower_ms * d_millisecond;
			}
			else if (meow::prefix_compare(pct_s, "percentiles=")) // 12 chars
			{
				auto const pct_spec      = meow::sub_str_ref(pct_s, 12, pct_s.size());
				auto const pct_values_v  = meow::split_ex(pct_spec, ":");

				for (auto const& percentile_s : pct_values_v)
				{
					double pv = 0.;
					if (!meow::number_from_string(&pv, percentile_s))
						return ff::fmt_err("bad percentile_spec: expected doubles split by ':', got '{0}'", pct_spec);

					vcf->percentiles.push_back(pv);
				}
			}
			else if (meow::prefix_compare(pct_s, "p")) // p<number>
			{
				auto const pct_spec = meow::sub_str_ref(pct_s, 1, pct_s.size());

				double pv = 0.;
				if (!meow::number_from_string(&pv, pct_spec))
					return ff::fmt_err("bad percentile_spec: expected 'p<double>', got '{0}'", pct_s);

				vcf->percentiles.push_back(pv);
			}
			else
			{
				return ff::fmt_err("unexpected token '{0}'", pct_s);
			}
		}

		// TODO: should make histogram setup optional (i.e. have sensible defaults)
		if (!hv_present)
			return ff::fmt_err("hv=<time_lower>:<time_upper>:<n_buckets> must be present");

		if (vcf->hv_bucket_count > PINBA_LIMIT___MAX_HISTOGRAM_SIZE)
			return ff::fmt_err("we support maximum of {0} histogram buckets (this is a tunable compile-time constant)", PINBA_LIMIT___MAX_HISTOGRAM_SIZE);

		// makes no sense to have histogram without percentiles specified
		bool const pct_present = (vcf->percentiles.size() > 0);
		if (!pct_present)
			return ff::fmt_err("percentiles=<double>[:<double>[...]] or p<double>[,p<double>[...]] must be present");

		return {};
	}


	static pinba_error_t parse_filters(pinba_view_conf_t *vcf, str_ref filters_spec)
	{
		if (filters_spec == "no_filters")
			return {};

		auto const items = meow::split_ex(filters_spec, ",");
		for (auto const& item_s : items)
		{
			auto const kv_s = meow::split_ex(item_s, "=");
			if (2 != kv_s.size())
				return ff::fmt_err("filters_spec: bad key=value pair '{0}'", item_s);

			str_ref const key_s   = kv_s[0];
			str_ref const value_s = kv_s[1];

			if (key_s == "min_time")
			{
				double time_value = 0.;
				if (!meow::number_from_string(&time_value, value_s))
					return ff::fmt_err("can't parse time from '{0}'", item_s);

				vcf->min_time = duration_from_double(time_value);
				continue;
			}

			if (key_s == "max_time")
			{
				double time_value = 0.;
				if (!meow::number_from_string(&time_value, value_s))
					return ff::fmt_err("can't parse time from '{0}'", item_s);

				vcf->max_time = duration_from_double(time_value);
				continue;
			}

			// key=value pair for field/rtag/timertag filtering
			vcf->filters.push_back({ key_s, value_s});
		}

		return {};
	}

////////////////////////////////////////////////////////////////////////////////////////////////

	auto pinba_view_conf_parse___internal(str_ref table_name, str_ref conf_string) -> std::shared_ptr<pinba_view_conf___internal_t>
	{
		auto result = std::make_shared<pinba_view_conf___internal_t>();
		result->orig_comment = conf_string.str();
		result->name         = table_name.str();

		auto const parts = meow::split_ex(result->orig_comment, "/");

		if (parts.size() < 2 || parts[0] != "v2")
			throw std::runtime_error("comment should have at least 'v2/<report_type>");

		auto const report_type = parts[1];

		if (report_type == "stats")
		{
			result->kind = pinba_view_kind::stats;
			return result;
		}

		if (report_type == "active")
		{
			result->kind = pinba_view_kind::active_reports;
			return result;
		}

		if (report_type == "packet" || report_type == "info") // support 'info' here for 'compatibility' with pinba_engine
		{
			result->kind = pinba_view_kind::report_by_packet_data;

			if (parts.size() != 6)
				throw std::runtime_error("'packet/info' report options are: <aggregation_spec>/<key_spec>/<histogram_spec>/<filters>");

			auto const aggregation_spec = parts[2];
			auto const key_spec         = parts[3];
			auto const histogram_spec   = parts[4];
			auto const filters_spec     = parts[5];

			pinba_error_t err;

			err = parse_aggregation_window(result.get(), aggregation_spec);
			if (err)
				throw std::runtime_error(ff::fmt_str("bad aggregation_spec: {0}", err));

			if (key_spec != "no_keys")
				throw std::runtime_error("key_spec must be 'no_keys' for 'packet' data reports");

			err = parse_histogram_and_percentiles(result.get(), histogram_spec);
			if (err)
				throw std::runtime_error(ff::fmt_str("bad histogram_spec: {0}", err));

			err = parse_filters(result.get(), filters_spec);
			if (err)
				throw std::runtime_error(ff::fmt_str("bad filters_spec: {0}", err));

			return result;
		}

		if (report_type == "request")
		{
			if (parts.size() != 6)
				throw std::runtime_error("'request' report options are: <aggregation_spec>/<key_spec>/<histogram_spec>/<filters>");

			auto const aggregation_spec = parts[2];
			auto const key_spec         = parts[3];
			auto const histogram_spec   = parts[4];
			auto const filters_spec     = parts[5];

			result->kind = pinba_view_kind::report_by_request_data;

			pinba_error_t err;

			err = parse_aggregation_window(result.get(), aggregation_spec);
			if (err)
				throw std::runtime_error(ff::fmt_str("bad aggregation_spec: {0}", err));

			err = parse_keys(result.get(), key_spec);
			if (err)
				throw std::runtime_error(ff::fmt_str("bad key_spec: {0}", err));

			err = parse_histogram_and_percentiles(result.get(), histogram_spec);
			if (err)
				throw std::runtime_error(ff::fmt_str("bad histogram_spec: {0}", err));

			err = parse_filters(result.get(), filters_spec);
			if (err)
				throw std::runtime_error(ff::fmt_str("bad filters_spec: {0}", err));

			return result;
		}

		if (report_type == "timer")
		{
			if (parts.size() != 6)
				throw std::runtime_error("'timer' report options are: <aggregation_spec>/<key_spec>/<histogram_spec>/<filters>");

			auto const aggregation_spec = parts[2];
			auto const key_spec         = parts[3];
			auto const histogram_spec   = parts[4];
			auto const filters_spec     = parts[5];

			result->kind = pinba_view_kind::report_by_timer_data;

			pinba_error_t err;

			err = parse_aggregation_window(result.get(), aggregation_spec);
			if (err)
				throw std::runtime_error(ff::fmt_str("bad aggregation_spec: {0}", err));

			err = parse_keys(result.get(), key_spec);
			if (err)
				throw std::runtime_error(ff::fmt_str("bad key_spec: {0}", err));

			err = parse_histogram_and_percentiles(result.get(), histogram_spec);
			if (err)
				throw std::runtime_error(ff::fmt_str("bad histogram_spec: {0}", err));

			err = parse_filters(result.get(), filters_spec);
			if (err)
				throw std::runtime_error(ff::fmt_str("bad filters_spec: {0}", err));

			return result;
		}

		throw std::runtime_error(ff::fmt_str("{0}; unknown v2/<table_type> {1}", __func__, report_type));
	}

////////////////////////////////////////////////////////////////////////////////////////////////

	struct key_descriptor_t
	{
		std::string name; // name, without leading ~,+,@
		int kind;         // report_by_timer.h RKD_*

		union {   // expecting the binary layout for this union to be the same
			      // as in report_by_timer/request/packet counterparts
			uint64_t                 flat_value;
			struct {
				uint32_t             timer_tag;
				uint32_t             request_tag;
				uint32_t packet_t::* request_field;
			};
		};
	};

	key_descriptor_t key_descriptor_init___request_field(str_ref name, uint32_t packet_t::* field_ptr)
	{
		key_descriptor_t d;
		d.name          = name.str();
		d.kind          = RKD_REQUEST_FIELD;
		d.request_field = field_ptr;
		return d;
	}

	key_descriptor_t key_descriptor_init___request_tag(str_ref name, uint32_t name_id)
	{
		key_descriptor_t d;
		d.name        = name.str();
		d.kind        = RKD_REQUEST_TAG;
		d.request_tag = name_id;
		return d;
	}

	key_descriptor_t key_descriptor_init___timer_tag(str_ref name, uint32_t name_id)
	{
		key_descriptor_t d;
		d.name      = name.str();
		d.kind      = RKD_TIMER_TAG;
		d.timer_tag = name_id;
		return d;
	}

	pinba_error_t key_descriptor_by_name(key_descriptor_t *out_kd, str_ref key_name)
	{
		if (key_name[0] == '~') // builtin
		{
			auto const field_name = meow::sub_str_ref(key_name, 1, key_name.size());

			if (field_name == "host")   { *out_kd = key_descriptor_init___request_field(field_name, &packet_t::host_id); return {}; }
			if (field_name == "script") { *out_kd = key_descriptor_init___request_field(field_name, &packet_t::script_id); return {}; }
			if (field_name == "server") { *out_kd = key_descriptor_init___request_field(field_name, &packet_t::server_id); return {}; }
			if (field_name == "schema") { *out_kd = key_descriptor_init___request_field(field_name, &packet_t::schema_id); return {}; }
			if (field_name == "status") { *out_kd = key_descriptor_init___request_field(field_name, &packet_t::status); return {}; }

			return ff::fmt_err("key_spec: request_field '{0}' not known "
								"(should be one of host, script, server, schema, status)", key_name);
		}

		if (key_name[0] == '+') // request_tag
		{
			auto const tag_name = meow::sub_str_ref(key_name, 1, key_name.size());
			auto const tag_id = P_G_->dictionary()->add_nameword(tag_name).id;

			*out_kd = key_descriptor_init___request_tag(tag_name, tag_id);
			return {};
		}

		// timer_tag
		{
			auto const tag_name = (key_name[0] == '@')
									? meow::sub_str_ref(key_name, 1, key_name.size())
									: key_name;

			// XXX: try to avoid modifying global state here
			auto const tag_id = P_G_->dictionary()->add_nameword(tag_name).id;

			*out_kd = key_descriptor_init___timer_tag(tag_name, tag_id);
			return {};
		}
	}

////////////////////////////////////////////////////////////////////////////////////////////////

	pinba_error_t pinba_view_conf___translate(report_conf___by_packet_t *conf, pinba_view_conf_t const& vcf)
	{
		assert(vcf.kind == pinba_view_kind::report_by_packet_data);

		conf->name            = vcf.name;
		conf->time_window     = vcf.time_window;
		conf->tick_count      = vcf.tick_count;
		conf->hv_bucket_count = vcf.hv_bucket_count;
		conf->hv_bucket_d     = vcf.hv_bucket_d;
		conf->hv_min_value    = vcf.hv_min_value;

		if (vcf.min_time.nsec)
			conf->filters.push_back(report_conf___by_packet_t::make_filter___by_min_time(vcf.min_time));

		if (vcf.max_time.nsec)
			conf->filters.push_back(report_conf___by_packet_t::make_filter___by_max_time(vcf.max_time));

		for (auto const& filter : vcf.filters)
		{
			key_descriptor_t kd;

			pinba_error_t const err = key_descriptor_by_name(&kd, filter.key);
			if (err)
				return err;

			switch (kd.kind)
			{
				case RKD_REQUEST_FIELD:
				{
					// XXX: try to avoid modifying global state here
					uint32_t const value_id = P_G_->dictionary()->get_or_add(filter.value);
					conf->filters.push_back(report_conf___by_packet_t::make_filter___by_request_field(kd.request_field, value_id));
				}
				break;

				case RKD_REQUEST_TAG:
				{
					// XXX: try to avoid modifying global state here
					uint32_t const value_id = P_G_->dictionary()->get_or_add(filter.value);
					conf->filters.push_back(report_conf___by_packet_t::make_filter___by_request_tag(kd.request_tag, value_id));
				}
				break;

				case RKD_TIMER_TAG:
					return ff::fmt_err("timer_tag filtering not supported for 'packet' reports");
				break;

				default:
					assert(!"can't be reached");
					break;
			}
		}

		return {};
	}

	pinba_error_t pinba_view_conf___translate(report_conf___by_request_t *conf, pinba_view_conf_t const& vcf)
	{
		assert(vcf.kind == pinba_view_kind::report_by_request_data);

		conf->name            = vcf.name;
		conf->time_window     = vcf.time_window;
		conf->tick_count      = vcf.tick_count;
		conf->hv_bucket_count = vcf.hv_bucket_count;
		conf->hv_bucket_d     = vcf.hv_bucket_d;
		conf->hv_min_value    = vcf.hv_min_value;

		for (auto const& key_name : vcf.keys)
		{
			key_descriptor_t kd;

			pinba_error_t const err = key_descriptor_by_name(&kd, key_name);
			if (err)
				return err;

			report_conf___by_request_t::key_descriptor_t real_kd;

			switch (kd.kind)
			{
				case RKD_REQUEST_FIELD:
					real_kd = report_conf___by_request_t::key_descriptor_by_request_field(kd.name, kd.request_field);
					break;

				case RKD_REQUEST_TAG:
					real_kd = report_conf___by_request_t::key_descriptor_by_request_tag(kd.name, kd.request_tag);
					break;

				case RKD_TIMER_TAG:
					return ff::fmt_err("key_spec: timer_tag are not allowed in 'request' reports, got '{0}'", key_name);

				default:
					assert(!"can't be reached");
					break;
			}

			conf->keys.push_back(real_kd);
		}

		if (vcf.min_time.nsec)
			conf->filters.push_back(report_conf___by_request_t::make_filter___by_min_time(vcf.min_time));

		if (vcf.max_time.nsec)
			conf->filters.push_back(report_conf___by_request_t::make_filter___by_max_time(vcf.max_time));

		for (auto const& filter : vcf.filters)
		{
			key_descriptor_t kd;

			pinba_error_t const err = key_descriptor_by_name(&kd, filter.key);
			if (err)
				return err;

			switch (kd.kind)
			{
				case RKD_REQUEST_FIELD:
				{
					// XXX: try to avoid modifying global state here
					uint32_t const value_id = P_G_->dictionary()->get_or_add(filter.value);
					conf->filters.push_back(report_conf___by_request_t::make_filter___by_request_field(kd.request_field, value_id));
				}
				break;

				case RKD_REQUEST_TAG:
				{
					// XXX: try to avoid modifying global state here
					uint32_t const value_id = P_G_->dictionary()->get_or_add(filter.value);
					conf->filters.push_back(report_conf___by_request_t::make_filter___by_request_tag(kd.request_tag, value_id));
				}
				break;

				case RKD_TIMER_TAG:
					return ff::fmt_err("timer_tag filtering not supported for 'request' reports");
				break;

				default:
					assert(!"can't be reached");
					break;
			}
		}

		return {};
	}


	pinba_error_t pinba_view_conf___translate(report_conf___by_timer_t *conf, pinba_view_conf_t const& vcf)
	{
		assert(vcf.kind == pinba_view_kind::report_by_timer_data);

		conf->name            = vcf.name;
		conf->time_window     = vcf.time_window;
		conf->tick_count      = vcf.tick_count;
		conf->hv_bucket_count = vcf.hv_bucket_count;
		conf->hv_bucket_d     = vcf.hv_bucket_d;
		conf->hv_min_value    = vcf.hv_min_value;

		for (auto const& key_name : vcf.keys)
		{
			key_descriptor_t kd;

			pinba_error_t const err = key_descriptor_by_name(&kd, key_name);
			if (err)
				return err;

			report_conf___by_timer_t::key_descriptor_t real_kd;

			switch (kd.kind)
			{
				case RKD_REQUEST_FIELD:
					real_kd = report_conf___by_timer_t::key_descriptor_by_request_field(kd.name, kd.request_field);
					break;

				case RKD_REQUEST_TAG:
					real_kd = report_conf___by_timer_t::key_descriptor_by_request_tag(kd.name, kd.request_tag);
					break;

				case RKD_TIMER_TAG:
					real_kd = report_conf___by_timer_t::key_descriptor_by_timer_tag(kd.name, kd.timer_tag);
					break;

				default:
					assert(!"can't be reached");
					break;
			}

			conf->keys.push_back(real_kd);
		}

		if (vcf.min_time.nsec)
			conf->filters.push_back(report_conf___by_timer_t::make_filter___by_min_time(vcf.min_time));

		if (vcf.max_time.nsec)
			conf->filters.push_back(report_conf___by_timer_t::make_filter___by_max_time(vcf.max_time));

		for (auto const& filter : vcf.filters)
		{
			key_descriptor_t kd;

			pinba_error_t const err = key_descriptor_by_name(&kd, filter.key);
			if (err)
				return err;

			switch (kd.kind)
			{
				case RKD_REQUEST_FIELD:
				{
					// XXX: try to avoid modifying global state here
					uint32_t const value_id = P_G_->dictionary()->get_or_add(filter.value);
					conf->filters.push_back(report_conf___by_timer_t::make_filter___by_request_field(kd.request_field, value_id));
				}
				break;

				case RKD_REQUEST_TAG:
				{
					// XXX: try to avoid modifying global state here
					uint32_t const value_id = P_G_->dictionary()->get_or_add(filter.value);
					conf->filters.push_back(report_conf___by_timer_t::make_filter___by_request_tag(kd.request_tag, value_id));
				}
				break;

				case RKD_TIMER_TAG:
				{
					// XXX: try to avoid modifying global state here
					uint32_t const value_id = P_G_->dictionary()->get_or_add(filter.value);

					LOG_DEBUG(P_L_, "{0}; report: {1}, adding timertag_filter: {2}:{3} -> {4}:{5}",
						__func__, conf->name, filter.key, kd.timer_tag, filter.value, value_id);

					conf->timertag_filters.push_back(report_conf___by_timer_t::make_timertag_filter(kd.timer_tag, value_id));
				}
				break;

				default:
					assert(!"can't be reached");
					break;
			}
		}

		return {};
	}

	pinba_error_t pinba_view_conf___translate(aux::pinba_view_conf___internal_t& vcf)
	{
		switch (vcf.kind)
		{
			case pinba_view_kind::stats:
			case pinba_view_kind::active_reports:
				return {};

			case pinba_view_kind::report_by_request_data:
				return aux::pinba_view_conf___translate(&vcf.by_request_conf, vcf);

			case pinba_view_kind::report_by_timer_data:
				return aux::pinba_view_conf___translate(&vcf.by_timer_conf, vcf);

			case pinba_view_kind::report_by_packet_data:
				return aux::pinba_view_conf___translate(&vcf.by_packet_conf, vcf);

			default:
				assert(!"must not be reached");
				return {};
		}
	}

////////////////////////////////////////////////////////////////////////////////////////////////

	pinba_error_t pinba_view_conf___validate(pinba_view_conf_t const& vcf)
	{
		if (vcf.min_time.nsec < 0)
			return ff::fmt_err("min_time must be >= 0");

		if (vcf.max_time.nsec < 0)
			return ff::fmt_err("max_time must be >= 0");

		if (vcf.min_time.nsec != 0 && vcf.max_time.nsec != 0 && vcf.min_time > vcf.max_time)
			return ff::fmt_err("min_time should be < max_time");

		return {};
	}

////////////////////////////////////////////////////////////////////////////////////////////////
}} // namespace { namespace aux {
////////////////////////////////////////////////////////////////////////////////////////////////


pinba_view_conf_ptr pinba_view_conf_parse(str_ref table_name, str_ref conf_string)
{
	auto result = aux::pinba_view_conf_parse___internal(table_name, conf_string);

	pinba_error_t err;

	err = aux::pinba_view_conf___validate(*result);
	if (err)
		throw std::runtime_error(err.what());

	err = aux::pinba_view_conf___translate(*result);
	if (err)
		throw std::runtime_error(err.what());

	return result;
}


aux::pinba_view_conf___internal_t const* pinba_view_conf_get_internal(pinba_view_conf_t const& vcf)
{
	return static_cast<aux::pinba_view_conf___internal_t const*>(&vcf);
}


report_conf___by_packet_t const*   pinba_view_conf_get___by_packet(pinba_view_conf_t const& vcf)
{
	return &pinba_view_conf_get_internal(vcf)->by_packet_conf;
}


report_conf___by_request_t const*  pinba_view_conf_get___by_request(pinba_view_conf_t const& vcf)
{
	return &pinba_view_conf_get_internal(vcf)->by_request_conf;
}


report_conf___by_timer_t const*    pinba_view_conf_get___by_timer(pinba_view_conf_t const& vcf)
{
	return &pinba_view_conf_get_internal(vcf)->by_timer_conf;
}

