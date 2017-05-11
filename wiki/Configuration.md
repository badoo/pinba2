Configuration
=============

These are options that go in my.cnf.<br>
You don't need to change any of these, all options have workable defaults.

## pinba_address

Address to listen for UDP packets on.<br>
Default: 0.0.0.0 (aka - all interfaces present)

## pinba_port
Port to listen for UDP packets on.<br>
Default: 3002

## pinba_log_level
Logging level, one of: debug, info, notice, warn, error, crit, alert<br>
Use 'debug' for debugging :)<br>
Use 'warn' or 'error' for production, should be enough <br>
This value can be changed at runtime with query like `set global pinba_log_level='debug';`

## pinba_default_history_time_sec
Default aggregation time_window for all reports. This value is used if you use "default_history_time" as time window in report configuration.<br>
Default: 60 (seconds)

## pinba_udp_reader_threads
Number of UDP reader threads, default is usually enough here.<br>
Default: 4<br>
Max: 16

## pinba_repacker_threads
Number of internal packet-repack threads, default is usually enough here.<br>
Try tunning higher if stats udp_batches_lost is > 0.<br>
Default: 4<br>
Max: 16

## pinba_repacker_input_buffer
Queue buffer size for udp-reader -> packet-repack threads communication.<br>
Default: 512<br>
Max: 16K

## pinba_repacker_batch_messages
Batch size for packet-repack -> reports communication.<br>
Might want to tune higher if coordinator thread rusage is too high.<br>
Default: 1024<br>
Max: 16K

## pinba_repacker_batch_timeout_ms
Max delay between packet-repack thread batches generation (in milliseconds).<br>
This setting basically puts an upper bound on how long packet can be queued before begin transferred to report for processing. Don't set below 10ms unless you know what you're doing.<br>
Default: 100 (milliseconds)<br>
Max: 1000 (milliseconds)

## pinba_coordinator_input_buffer
Queue buffer size for packet-repack -> coordinator threads communication.<br>
Default: 128<br>
Max: 1024

## pinba_report_input_buffer
Queue buffer size for coordinator -> report threads communication. This setting is per report.<br>
Default: 128<br>
Max: 8192
