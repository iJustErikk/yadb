# YAdb
yet another database

making a db. with ~~rocksdb~~ ~~rust~~ ~~go/cockroachdb's pebble.~~ actually going back to rust, writing my own storage engine.

Update: this is a tad buggy. working the kinks out.

Motivation: I wish to better my system and data engineering skills. Data intensity will only increase. 

Lofty goals for this project: 
- Build lsm based kv store
- Build distributed kv engine
- Build single node sql database
- Build distributed sql database (with simple sql, maybe fuller sql syntax for non-distributed databases) like Cockroachdb
- (possibly) build b epsilon storage engine

storage engine:
This is a lsm tree supporting (that will support) level compaction and bloom filters. The user will be able to tune some settings for their workloads. Working on getting this working. Next is cleaning up the config/API/error handling and writing some tests.

next up:
- make this more usable (tables, client library, sql support/indices)
- concurrency support
- replication support
