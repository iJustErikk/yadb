# YAdb
yet another database

making a db. with ~~rocksdb~~ ~~rust~~ ~~go/cockroachdb's pebble.~~ actually going back to rust, writing my own storage engine.

storage engine:
this is based on LSM trees. going to use the standard optimizations like bloom filters and such
need to look into other optimizations rocksdb uses. I'd like to build something like cockroach db. i really need to dig into things more. I need to review how storage engine design affects r/w/s amplification, see if I should implement multi-level lsm, check out different compaction strategies...

next up:
- implement storage engine (see other project) so I could keep using the same K/V tcp interface
- make this more usable (tables, client library, sql support/indices)
- concurrency support
- replication support
