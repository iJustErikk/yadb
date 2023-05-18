# YAdb
yet another database

making a db. with ~~rocksdb~~ ~~rust~~ ~~go/cockroachdb's pebble.~~ actually going back to rust, writing my own storage engine.

storage engine:
This is a lsm tree supporting (that will support) level compaction and bloom filters. The user will be able to tune some settings for their workloads. Working on getting this working. Next is cleaning up the config/API/error handling and writing some tests.

next up:
- make this more usable (tables, client library, sql support/indices)
- concurrency support
- replication support
