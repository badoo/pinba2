#include "pinba/globals.h"
#include "pinba/report.h"
#include "pinba/report_util.h"
#include "pinba/report_by_request.h"
#include "pinba/report_by_timer.h"

////////////////////////////////////////////////////////////////////////////////////////////////

template<class T>
inline double operator/(T const& value, duration_t d)
{
	return ((double)value / d.nsec) * nsec_in_sec;
}

void debug_dump_report_snapshot(FILE *sink, report_snapshot_t *snapshot)
{
	auto const write_hv = [&](report_snapshot_t::position_t const& pos)
	{
		ff::fmt(sink, " [");
		auto const *hv = snapshot->get_histogram(pos);
		if (NULL != hv)
		{
			auto const& hv_map = hv->map_cref();
			for (auto it = hv_map.begin(), it_end = hv_map.end(); it != it_end; ++it)
			{
				ff::fmt(sink, "{0}{1}: {2}", (hv_map.begin() == it)?"":", ", it->first, it->second);
			}
		}

		ff::fmt(sink, "]");
	};

	for (auto pos = snapshot->pos_first(), end = snapshot->pos_last(); !snapshot->pos_equal(pos, end); pos = snapshot->pos_next(pos))
	{
		auto const key = snapshot->get_key(pos);
		ff::fmt(sink, "[{0}] -> ", report_key_to_string(key, snapshot->dictionary()));

		auto const data_kind = snapshot->data_kind();

		switch (data_kind)
		{
		case REPORT_KIND__BY_REQUEST_DATA:
		{
			auto const *rinfo = snapshot->report_info();

			auto const *row   = reinterpret_cast<report_row___by_request_t*>(snapshot->get_data(pos));
			auto const& data  = row->data;

			ff::fmt(sink, "{{ {0}, {1}, {2}, {3}, {4}, {5} }",
				data.req_count, data.time_total, data.ru_utime, data.ru_stime,
				data.traffic_kb, data.mem_usage);

			auto const time_window = rinfo->time_window; // TODO: calculate real time window from snapshot data
			ff::fmt(sink, " {{ rps: {0} }",
				ff::as_printf("%.06lf", data.req_count / time_window));

			write_hv(pos);
		}
		break;

		case REPORT_KIND__BY_TIMER_DATA:
		{
			auto const *rinfo = snapshot->report_info();

			auto const *row   = reinterpret_cast<report_row___by_timer_t*>(snapshot->get_data(pos));
			auto const& data  = row->data;

			ff::fmt(sink, "{{ {0}, {1}, {2}, {3}, {4} }",
				data.req_count, data.hit_count, data.time_total, data.ru_utime, data.ru_stime);

			auto const time_window = rinfo->time_window; // TODO: calculate real time window from snapshot data
			ff::fmt(sink, " {{ rps: {0}, tps: {1} }",
				ff::as_printf("%.06lf", data.req_count / time_window),
				ff::as_printf("%.06lf", data.hit_count / time_window));

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