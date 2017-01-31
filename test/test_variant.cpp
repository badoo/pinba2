#include <string>
#include <vector>

#include <boost/variant.hpp>

#include <meow/gcc/demangle.hpp>
#include <meow/format/format_to_string.hpp>

#include "pinba/globals.h"

////////////////////////////////////////////////////////////////////////////////////////////////

typedef boost::variant<
		int64_t,
		uint64_t,
		double,
		std::string
	>
	report_field_t;

typedef std::vector<report_field_t> report_line_t;

std::vector<report_line_t> get_report_data()
{
	return {
		{ "update", "script1", (int64_t)-32, 10.003, 012.0005, },
		{ "update", "script_2", 10.003, 012.0005, },
		{ "update", "scrawddddd", 10.003, 012.0005, },
	};
}

////////////////////////////////////////////////////////////////////////////////////////////////

namespace meow { namespace format {
	template<class... A>
	struct type_tunnel<boost::variant<A...>>
	{
		struct v : boost::static_visitor<std::string>
		{
			template<class T>
			std::string operator()(T const& value) const
			{
				return ff::write_str(value);
			}

			std::string operator()(std::string const& value) const
			{
				return ff::fmt_str("'{0}'", value);
			}
		};

		inline static std::string call(boost::variant<A...> const& value)
		{
			return boost::apply_visitor(v(), value);
		}
	};
}}

////////////////////////////////////////////////////////////////////////////////////////////////


int main(int argc, char const *argv[])
try
{
	auto const rd = get_report_data();

	for (size_t i = 0; i < rd.size(); i++)
	{
		auto const& line = rd[i];
		ff::fmt(stdout, "[{0}] {1} fields\n", i, line.size());

		// for (auto const& field : line)
		for (size_t field_i = 0; field_i < line.size(); field_i++)
		{
			auto const& field = line[field_i];
			ff::fmt(stdout, "{0} ({1}) {2}", ((field_i == 0) ? "" : ", "), meow::gcc_demangle_name_tmp(field.type().name()), field);
		}

		ff::fmt(stdout, "\n");
	}

	return 0;
}
catch (std::exception const& e)
{
	ff::fmt(stderr, "error: {0}\n", e.what());
	return 1;
}
