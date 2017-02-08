# Features
- mysql engine, report configs, etc.!
- mysql tables with list of all reports and their data
- per-report nanomsg queues instead of pubsub
  - to track packets dropps, slow reports, etc.
  - we actually leak memory if any batches get dropped (due to incrementing ref counts for all present reports)
- calculate real time window for report snapshots (i.e. skip timeslices that have had no data)
  - this is debatable, but useful for correct <something>/sec calculations


# Performance
- learn to use perf like a pro :)
- recvmmsg() in udp reader (+ settings)
- flatbuffer (https://google.github.io/flatbuffers/) instead of protobuf?
- maybe replace nanomsg with something doing less locking / syscalls (thorough meamurements first!)
- per repacker dictionary caches
- per snapshot merger dicttionary caches

# Internals
- split pinba_globals_t into 'informational' and 'runtime engine' parts (to simplify testing/experiments)
  - informational: stats, ticker, dictionary, stuff that is just 'cogs'
  - runtime: udp readers, coorinator, the features meat
