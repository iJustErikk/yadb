# Lock/Wait-Free Design

As this storage engine becomes more optimized, lock contention takes up a larger share of runtime. For operation heavy/bandwith low workloads, this can be 30% of runtime (as seen through the profiler, though I am growing less confident that this is the ideal way to measure performance and make decisions).

Perhaps a lock/wait free design would be better for performance. I absolutely want to delay the actual implementation of such a design, as it will incur a lot of complexity. 

Currently, there are a few points where we use mutexes/channels for synchronization. These are the mutexes around the memtable/wal state, the tree state (cache, static path, and in-memory FS representation), the cache mutexes and the WAL Batch channel. AFAIK, most of the contention time is with the memtable/wal mutex. I should report back with actual numbers for a few benchmarks.

<<<REVISIT THIS. work on iterators reduces a lot of lock contention>>>
