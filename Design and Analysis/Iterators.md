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

How do figure memtable into point-in-time consistency? It will change with scan-write concurrency. The SSTables are easy, as they are immutable. One option is cloning. For many concurrency scans on a nearly full memtable, this will starve RAM. We could drastically reduce the chance of using too much RAM by only storing "snapshots" of the memtable. This is a lot of complexity to support faster read recognition. Memtables could also be flushed regularly, but this may create a lot of small memtables (and thus a lot of compaction). I think RocksDB makes this guarantee, as this is the easiest/fastest guarantee to support. There has to be a better solution. Solution: instead of updating the key, we can append versions of the key to the value vector. Then on memtable flush, we can take the most recent version (this has an issue with keeping old iterators valid, but we could clone the memtable (with all versions) for all running iterators. unless these get way behind, this should not be an issue.).

Read-scan-write concurrency: We potentially long running scans to execute concurrently with writes. Iterators do not mutate state other than level usage. They do not need to hold any locks for their time executing. The only thing writes need to contend on is WAL batch writer and the memtable. Reads don't need to contend on anything (other than cache) so as long as the tables they wish to read do not get deleted. They can increment a table usage counter just like iterators. Then, they can just clone the current view on start of read. 


Better performance: prefetching/fetching many blocks at a time.