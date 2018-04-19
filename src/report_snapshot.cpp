#include "pinba/globals.h"
#include "pinba/packet.h"
#include "pinba/histogram.h"
#include "pinba/report.h"
#include "pinba/report_util.h"
#include "pinba/report_by_packet.h"
#include "pinba/report_by_request.h"
#include "pinba/report_by_timer.h"

////////////////////////////////////////////////////////////////////////////////////////////////

template<class T>
inline double operator/(T const& value, duration_t d)
{
	return ((double)value / d.nsec) * nsec_in_sec;
}

void debug_dump_report_snapshot(FILE *sink, report_snapshot_t *snapshot, str_ref name)
{
	auto const write_hv = [&](report_snapshot_t::position_t const& pos)
	{
		ff::fmt(sink, " [");
		auto const *histogram = snapshot->get_histogram(pos);
		if (histogram != nullptr)
		{
			// if (HISTOGRAM_KIND__HASHTABLE == snapshot->histogram_kind())
			// {
			// 	auto const *hv = static_cast<histogram_t const*>(histogram);

			// 	auto const& hv_map = hv->map_cref();
			// 	for (auto it = hv_map.begin(), it_end = hv_map.end(); it != it_end; ++it)
			// 	{
			// 		ff::fmt(sink, "{0}{1}: {2}", (hv_map.begin() == it)?"":", ", it->first, it->second);
			// 	}
			// }
			// else if (HISTOGRAM_KIND__FLAT == snapshot->histogram_kind())
			if (HISTOGRAM_KIND__FLAT == snapshot->histogram_kind())
			{
				auto const *hv = static_cast<flat_histogram_t const*>(histogram);

				auto const& hvalues = hv->values;
				for (auto it = hvalues.begin(), it_end = hvalues.end(); it != it_end; ++it)
				{
					ff::fmt(sink, "{0}{1}: {2}", (hvalues.begin() == it)?"":", ", it->bucket_id, it->value);
				}
			}
			else if (HISTOGRAM_KIND__HDR == snapshot->histogram_kind())
			{
				auto const *hv = static_cast<hdr_histogram_t const*>(histogram);
				bool printed_something = false;

				for (uint32_t i = 0; i < hv->counts_len(); i++)
				{
					if (hv->count_at_index(i) == 0)
						continue;

					ff::fmt(sink, "{0}{1}: {2}", (printed_something)?", ":"", hv->value_at_index(i), hv->count_at_index(i));
					printed_something = true;
				}

				if (hv->negative_inf() > 0)
				{
					ff::fmt(sink, "{0}min:{1}", (printed_something)?", ":"", hv->negative_inf());
					printed_something = true;
				}

				if (hv->positive_inf() > 0)
				{
					ff::fmt(sink, "{0}max:{1}", (printed_something)?", ":"", hv->positive_inf());
					printed_something = true;
				}
			}
		}

		ff::fmt(sink, "]");
	};

	ff::fmt(sink, ">-------------- {0} ------->>\n", name);

	for (auto pos = snapshot->pos_first(), end = snapshot->pos_last(); !snapshot->pos_equal(pos, end); pos = snapshot->pos_next(pos))
	{
		auto const key = snapshot->get_key(pos);
		ff::fmt(sink, "[{0}] -> ", report_key_to_string(key, snapshot->dictionary()));

		auto const data_kind = snapshot->data_kind();

		switch (data_kind)
		{
		case REPORT_KIND__BY_PACKET_DATA:
		{
			auto const *rinfo = snapshot->report_info();
			auto const *data  = reinterpret_cast<report_row_data___by_packet_t*>(snapshot->get_data(pos));

			ff::fmt(sink, "{{ {0}, {1}, {2}, {3}, {4}, {5}, {6} }",
				data->req_count, data->timer_count, data->time_total, data->ru_utime, data->ru_stime,
				data->traffic, data->mem_used);

			auto const time_window = rinfo->time_window; // TODO: calculate real time window from snapshot data
			ff::fmt(sink, " {{ rps: {0} }",
				ff::as_printf("%.06lf", data->req_count / time_window));

			write_hv(pos);
		}
		break;

		case REPORT_KIND__BY_REQUEST_DATA:
		{
			auto const *rinfo = snapshot->report_info();
			auto const *data  = reinterpret_cast<report_row_data___by_request_t*>(snapshot->get_data(pos));

			ff::fmt(sink, "{{ {0}, {1}, {2}, {3}, {4}, {5} }",
				data->req_count, data->time_total, data->ru_utime, data->ru_stime,
				data->traffic, data->mem_used);

			auto const time_window = rinfo->time_window; // TODO: calculate real time window from snapshot data
			ff::fmt(sink, " {{ rps: {0} }",
				ff::as_printf("%.06lf", data->req_count / time_window));

			write_hv(pos);
		}
		break;

		case REPORT_KIND__BY_TIMER_DATA:
		{
			auto const *rinfo = snapshot->report_info();
			auto const *data  = reinterpret_cast<report_row_data___by_timer_t*>(snapshot->get_data(pos));

			ff::fmt(sink, "{{ {0}, {1}, {2}, {3}, {4} }",
				data->req_count, data->hit_count, data->time_total, data->ru_utime, data->ru_stime);

			auto const time_window = rinfo->time_window; // TODO: calculate real time window from snapshot data
			ff::fmt(sink, " {{ rps: {0}, tps: {1} }",
				ff::as_printf("%.06lf", data->req_count / time_window),
				ff::as_printf("%.06lf", data->hit_count / time_window));

			write_hv(pos);
		}
		break;

		default:
			// assert(!"unknown report snapshot data_kind()");
			ff::fmt(sink, "unknown report snapshot data_kind(): {0}", data_kind);
			break;
		}

		ff::fmt(sink, "\n");
	}

	ff::fmt(sink, "<<-----------------------<\n");
	ff::fmt(sink, "\n");
}