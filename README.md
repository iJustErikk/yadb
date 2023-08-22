# YAdb
yet another database ![Test Status](https://github.com/iJustErikk/yadb/actions/workflows/cargo-test.yml/badge.svg)

Welcome to YAdb! This is my first (nontrival) Rust project. I am trying to implement a database from reasonably scratch. I've implemented a decent performance LSM based storage engine. Once that supports the features I need for a SQL database/distributed kv store, I'll switch to those. 

Update: Improving performance, API and adding iterators to storage engine to support SQL db workload.

Goals for this project: 
- Build lsm based kv store (almost there, see roadmap for more features)
- Build distributed kv engine (need to decide whether to implement causal (probably more fun) or strong consistency)
- Build single node sql database (simple sql to start)
- Build distributed sql database (with simple sql, maybe fuller sql syntax for non-distributed databases) like Cockroachdb
- (possibly) build b epsilon/fractal tree storage engine (have the bones, would need to implement only the data structure)

## Running/testing/profiling LSM Based KV Storage Engine

Run `./setup.sh` to get started.

Run `cargo test` to run tests.

Run `cargo test split_bench` to run current benchmark.

Run `sudo sysctl -p && cargo flamegraph --test test_storage -- split_bench` to profile current benchmark.