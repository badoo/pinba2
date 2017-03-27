#include <cstdio>

#include "misc/nmpa.h"
#include "misc/nmpa_pba.h"

#include "proto/pinba.pb-c.h"

#include <meow/format/format_and_namespace.hpp>
#include <meow/format/format_to_string.hpp>

#include "pinba/globals.h"
#include "pinba/collector.h"

////////////////////////////////////////////////////////////////////////////////////////////////

int main(int argc, char const *argv[])
{
	if (argc < 2)
		throw std::runtime_error(ff::fmt_str("usage {0} <filename>", argv[0]));

	FILE *f = fopen(argv[1], "r");
	char buf[16 * 1024];
	memset(buf, 0, sizeof(buf));
	size_t n = fread(buf, 1, sizeof(buf), f);

	raw_request_ptr req;

	ProtobufCAllocator request_unpack_pba = {
		.alloc = nmpa___pba_alloc,
		.free = nmpa___pba_free,
		.allocator_data = NULL, // changed in progress
	};

	size_t batch_size = 256;

	auto const send_batch = [&](raw_request_ptr& req)
	{
		req.reset();
		request_unpack_pba.allocator_data = NULL;
	};

	for (uint32_t i = 0; i < 1000; i++)
	{
		if (!req)
		{
			req = meow::make_intrusive<raw_request_t>(batch_size, 16 * 1024);
			request_unpack_pba.allocator_data = &req->nmpa;
		}

		Pinba__Request *request = pinba__request__unpack(&request_unpack_pba, (int)n, (uint8_t*)buf);
		if (request == NULL) {
			ff::fmt(stdout, "request decode failed\n");
			continue;
		}

		req->requests[req->request_count] = request;
		req->request_count++;

		if (req->request_count >= batch_size)
		{
			send_batch(req);
		}
	}

	std::vector<uint32_t> v;

	for (uint32_t i = 0; i < 10000; i++)
	{
		v.push_back(i);
		if (v.size() > 60)
			v.erase(v.begin());
	}

	ff::fmt(stdout, "v.capacity() = {0}\n", v.capacity());

	return 0;
}
