# Analysis

Concurrency could speed up a few parts of this application. There are a lot of moving parts, so care needs to be taken to eliminate invalid states without hurting performance.

## What could benefit from concurrency:

- In memory reads can be completely concurrently resolved
- In memory writes will need to execute serially (due to memtable size), but can be concurrently flushed to WAL via batching
- Compaction can be completed while requests are being served on uncompacted tables
- On disk reads can be concurrently resolved (target hardware is SSDs), caching will benefit greatly. 
- Concurrently compacting and resolving requests will reduce startup time and increase availability.
- On disk writes are via memtable flush. This could be done concurrently with writing to a new memtable.

## What needs to be synchronized:

- Memtable: Skiplist is lock free synchronized, however size needs to be synchronized.
- File System: Any compaction (level deletion + new table) or Memtable flush (wal restore happens before concurrent entry point) and corresponding header file/manifest WAL needs to be synchronized. Any filesystem related lock should NOT be starved. Reader latency will suffer if garbage collection ceases due to read amplification (multiple sstable searches over some uncompacted level). In memory writes cannot proceed if full memtable is not flushed.
- WAL: tasks should tell WALBatchWriter to write some kv and block until it has flushed. WALBatchWriter should either wait until k records or m miliseconds (maybe 4kb, 5ms)
- Datablock/index/filter caching: we should be able to concurrently resolve requests and follow LRU rules with maximum in memory limits
- File System (multiprocess): Only one embedded server should run at once. Should acquire some OS directory lock/lease

## Concurrent Design and Tradeoffs
- Opting for simpler, coarse-grained locks, this beginning design is very simple
- Check main directory
- Reads will execute fully concurrently
- Individual latency will increase from lock contention and execution reordering
- Write throughput should increase
- Overall time spent on WAL io should decrease (hopefully by 90% or more)

### Results

ops/sec, MB/sec massively increased. lots of time is spent on 