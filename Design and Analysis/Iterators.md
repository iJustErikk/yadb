# Iterators

Iterators will be useful for building db scans or iterating over indices. 

## Iterator Semantics

Other storage engines may provide different semantics for iterators. For example, RocksDB provides a point-in-time consistent iterator, where the iterator shows the most recent version of a key wrt the version of the database when the iterator was created. This could be implemented by assigning each key update a version. 

As we use a RWMutex for the main tree state, reads/scans can execute fully concurrently. The table state will not update. So we could avoid having to add a sqeuence number. For future performance, our database will need to be able to support write/scan concurrency.

Additionally, RocksDB iterators are valid as long as they are alive. We can support validity and point-in-time consistency without supporting scan-write concurrency.

## Current Design

Stick with current implementation where only reads and scans can be concurrent. Create a MergedIterator between the memtable + all tables

## Future Design

We'll need to keep compacted tables alive until the iterators are finished (for validity). We'll also need to consider the memtable. If a key is updated while the iterator is still iterating, how would that work? For performance reasons, we should get more than 1 block at a time when scanning (prefetching). This could also speed up compactions. Point-in-time consistency and validity with scan-write concurrency will add quite a bit of complexity.