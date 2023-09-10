# Iterators

Iterators will be useful for building db scans or iterating over indices. 

## Iterator Semantics

Other storage engines may provide different semantics for iterators. For example, RocksDB provides a point-in-time consistent iterator, where the iterator shows the most recent version of a key wrt the version of the database when the iterator was created. This could be implemented by assigning each key update a version. 

As we use a RWMutex for the main tree state, reads/scans can execute fully concurrently. The table state will not update. So we could avoid having to add a sqeuence number. For future performance, our database will need to be able to support write/scan concurrency.

Additionally, RocksDB iterators are valid as long as they are alive. We can support validity and point-in-time consistency without supporting scan-write concurrency.

## Current Design

Stick with current implementation where only reads and scans can be concurrent. Create a MergedIterator between the memtable + all tables

## Future Design

In the future, we will want point-in-time consistency, validity and read-scan-write concurrency.

To ensure validity, we'll keep old tables along so scans can finish. Once a table hits 0 references and needs to be cleaned up, it will be deleted. This allows us to start the compaction process whilst still scanning. One issue is what scheme we will use to r/w temporary or recently compacted tables (we need to be able to support the running iterators, be durable to restarts). How will this mesh with the FS Manifest WAL? -> the solution is to depart with the physical representation of the tables and levels. tables should sit within the same folder and have a unique hash as names. the header should state information external to the table (level, number in level/name). starting a new doc for FS Layout <<<for this, cache invalidation needs to be considered. it should be invalidated only when the tables get deleted, not when their level is compacted>>>

How do figure memtable into point-in-time consistency? It will change with scan-write concurrency. The SSTables are easy, as they are immutable. One option is cloning. For many concurrency scans on a nearly full memtable, this will starve RAM. We could drastically reduce the chance of using too much RAM by only storing "snapshots" of the memtable. This is a lot of complexity to support faster read recognition. Memtables could also be flushed regularly, but this may create a lot of small memtables (and thus a lot of compaction). I think RocksDB makes this guarantee, as this is the easiest/fastest guarantee to support. There has to be a better solution. Solution: instead of updating the key, we can append versions of the key to the value vector. Then on memtable flush, we can take the most recent version (this has an issue with keeping old iterators valid, but we could clone the memtable (with all versions) for all running iterators. unless these get way behind, this should not be an issue.). Update: Alone, a multiversion memtable faces issues when trying to achieve scan-write concurrency whilst maintaining point-in-time consistency. RocksDB takes a similar strategy to cloning: at any time, there is 1 mutable memtable and 0 or more immutable memtables (flushed when reference count hits 0). Combining multiversion and the mutable-immutable memtable scheme, point-in-time consistency and write-scan concurrency can be guaranteed. is there a simpler scheme to achieve both? potentially, a scan could only reference active immutable tables. this would guarantee point-in-time consistency as well as write-scan concurrency. there might be an arbitrary delay between writing and scans respecting that write (depends on volume of writes). a configurable time limit could introduced similar to walwriters 10ms time limit.

Read-scan-write concurrency: We potentially long running scans to execute concurrently with writes. Iterators do not mutate state other than level usage. They do not need to hold any locks for their time executing. The only thing writes need to contend on is WAL batch writer and the memtable. Reads don't need to contend on anything (other than cache) so as long as the tables they wish to read do not get deleted. They can increment a table usage counter just like iterators. Then, they can just clone the current view on start of read. 


Better performance: prefetching/fetching many blocks at a time. 

## Final Full Design

We'd like point-in-time consistency, w/scan concurrency and validity. Here are changes that would allow us to get to this point:

- Replace current SSTable/Level layout with "virtual" representation- files are in the same directory, they have hashes as names and are mapped to their level/name representation in the header. This will enable validity and concurrency requirement. On startup, there will be a check to ensure all tables referenced in header exist. (this includes a redesign of the TreeState struct) -> COMPLETE.
- Replace lock-free skiplist with regular skiplist. This allows us to ensure consistency easier and we aren't useful the lock-free property just yet. -> COMPLETE.
- Implement FS-level durability discussed in `FS Layout. -> COMPLETE
- Async/tokify rest of tables. iterators -> streams -> Blocked. Need Send SkipMap Iterator implementation.
- Change memtable scheme to include 0 or more immutable memtables + reference counting for ensuring point-in-time consistency, validity and concurrency. On usage, assert existence of tables being used. Immuatable memtables should be stored outside of the mutex for the mutable memtable/wal as we wish to serve writes to the mutable memtable whilst serving reads to the immutable memtables.
- Finalize iterator implementation. Can serve SQL workloads (with external transaction support)! Slap a little serde onto iterators and writes, then a baby sql db is born! This of course provides simple SQL support, but is an important deliverable.

Proof of validity: Before using an immutable memtable or sstable, reads/scans must increment a global table reference count. Before eliminating a table (end of usage or on compaction or new immutable is created), the reference count is checked. Given it is up to date (as guaranteed above), no tables will be deleted too soon. Given read (by implementation) /scans (by contract) execute under a timely fashion, RAM will not be starved by immutable memtables
Proof of point-in-time consistency: Iterators respect writes contained only in immutable tables, so no new writes could ever appear in this view
Proof of concurrency and safety: We wish to guarantee write progression along with scan progression. Eg. neither writes nor scans block each other for large periods of time. Scans do not hold explicit locks whilst executing and grab state locks only when necessary. 
Proof of write durability (consider individual writes (consider memtable/wal path) and durability over large operations): 
For individual writes: an individual write is considered durable if it is flushed from either WAL or memtable. Another property we need to guarantee is that read versions must be monotonic for some key ie. reads must not go back in time. This is an important consistency property (linearizability) for implementing distributed consistency guarantees. So as long as writes are written to memtable + WAL durability can be guaranteed. Linearizability is guaranteed via locking + globally ordered writes. Large operations such as WAL/memtable flushes and compactions must not result in the effective deletion of the most recent version of keys. This is guaranteed (assuming no corruption). Failed memtable/wal flushes/compactions are not reflected in the header and are deleted on startup. Compactions preserve this property by definition.

From Concurrent Design Analysis:
- In memory reads can be completely concurrently resolved
- In memory writes will need to execute in a locked fashion (due to memtable size), but can be concurrently flushed to WAL via batching
- Compaction can be completed while requests are being served on uncompacted tables
- On disk reads can be concurrently resolved (target hardware is SSDs), caching will benefit greatly. 
- Concurrently compacting and resolving requests will reduce startup time and increase availability.
- On disk writes are via memtable flush. This could be done concurrently with writing to a new memtable.

With this design and plan, we preserve work on points 1, 2 and 4. With the mutable-immutable memtable scheme, we address 3, 5 and 6. Further improvements lay in optimizing IO, minimizing contention overhead in the write path, measuring r/w/s amplification and a many-core exploitative design. Saturating an ssd with 100 cores would be cool. Saturating ssd-raid 10x striped configuration would be epic (roughly 5GB/second for consumer grade ssds- helium can reach this).