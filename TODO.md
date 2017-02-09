# Features
- mysql
  - engine
  - report configs (in comments)
  - docs
- proper logging (i mean writing to stderr from a 'library' is not something you'd call nice)
- mysql tables with list of all reports and their data
- timer tag based filtering (i.e. take only timers with tag:browser=chrome)
- per-report
  - rusage
  - packet counts (+ drop counts, filtered out counts, bloom dropped counts)
- per-report nanomsg queues instead of pubsub
  - to track packets dropps, slow reports, etc.
  - we actually leak memory if any batches get dropped (due to incrementing ref counts for all present reports)
- global stats + mysql table for them
- calculate real time window for report snapshots (i.e. skip timeslices that have had no data)
  - this is debatable, but useful for correct <something>/sec calculations


# Performance
- (boring!) develop benchmark harness + learn to use perf like a pro :)
- (easy) recvmmsg() in udp reader (+ settings)
- (easy) thread affinity
- (easy) flatbuffer (https://google.github.io/flatbuffers/) instead of protobuf?
- (hard) maybe replace nanomsg with something doing less locking / syscalls (thorough meamurements first!)
- (medium, worth it?) simple bloom filtering (aka is this report interested in this packet?)
  - a library like this one? https://github.com/mavam/libbf
  - try calculating a simple bloom filter (over tag names) for all incoming packets
  - report has it's own bloom, filled with tag names it's interested in
  - compare those 2, if no match - packet doesn't need to be inspected
  - might be memory intensive and not worth it
- (easy, worth it?) per repacker dictionary caches (reduces locking on global dictionary)
- (medium, worth it?) per snapshot merger dicttionary caches

# Internals
- split pinba_globals_t into 'informational' and 'runtime engine' parts (to simplify testing/experiments)
  - informational: stats, ticker, dictionary, stuff that is just 'cogs'
  - runtime: udp readers, coorinator, the features meat
