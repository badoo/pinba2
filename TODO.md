# Features
- mysql
  - engine
  - report configs (in comments)
  - docs
  - test with 5.7
  - test with mariadb (those guys install all internal headers, should be simpler to install)
  - tables with list of all reports and their data (almost there)
- proper logging (i mean writing to stderr from a 'library' is not something you'd call nice)
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
- (?) increase udp kernel memory (or at least check for it) on startup
  - kernel udp memory is usually tuned very low
  - so beneficial to increase it to be able to handle high packet+data rates
  - should provide guidelines here (like 1gbps in traffic = ~120mb/sec, should probably reserve at least 60mb for 1/2 second hickups)
- (easy) flatbuffer (https://google.github.io/flatbuffers/) instead of protobuf?
- (hard) maybe replace nanomsg with something doing less locking / syscalls (thorough meamurements first!)
- (medium, worth it?) simple bloom filtering (aka is this report interested in this packet?)
  - a library like this one? https://github.com/mavam/libbf
  - try calculating a simple bloom filter (over tag names) for all incoming packets
  - report has it's own bloom, filled with tag names it's interested in
  - compare those 2, if no match - packet doesn't need to be inspected
  - might be memory intensive and not worth it
  - might also take a look if something like https://en.wikipedia.org/wiki/MinHash would work
- (easy, worth it?) per repacker dictionary caches (reduces locking on global dictionary)
- (medium, worth it?) per snapshot merger dictionary caches

# Internals
- split pinba_globals_t into 'informational' and 'runtime engine' parts (to simplify testing/experiments)
  - informational: stats, ticker, dictionary, stuff that is just 'cogs'
  - runtime: udp readers, coorinator, the features meat
