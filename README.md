# YAdb
yet another database ![Test Status](https://github.com/iJustErikk/yadb/actions/workflows/cargo-test.yml/badge.svg)

making a db. with ~~rocksdb~~ ~~rust~~ ~~go/cockroachdb's pebble.~~ actually going back to rust, writing my own storage engine.

Update: the storage engine now provides a minimum functionality for implementing a database (minus iterators for sql-like dbs). need to improve performance (see concurrent design analysis). Putting this project on pause for a bit.

Motivation: I wish to better my system and data engineering skills. Data intensity will only increase. 

Lofty goals for this project: 
- Build lsm based kv store
- Build distributed kv engine
- Build single node sql database
- Build distributed sql database (with simple sql, maybe fuller sql syntax for non-distributed databases) like Cockroachdb
- (possibly) build b epsilon storage engine

## Running/testing/profiling LSM Based KV Storage Engine

Run `./setup.sh` to get started.

Run `cargo test` to run tests.

Run `cargo `
