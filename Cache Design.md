Cache design

Writing a decent LRU cache requires a doubly linked list, which is annoying to write in rust, so I will use a crate.
The most popular crate has support for a count eviction policy, rather than a total number of bytes.

schnellru has support for this as well as removing items.

Here is what we need to support:

The cache should reflect up to date information. There will be separate caches for datablocks, filters and indices (for simplicity + configuration). They should evict on a max bytes policy.

Right now, there a single class to handle SSTable IO. This is instantiated whenever we need to r/w a file. That is fine for now, but we need to instantiate a central cache class (a LSMCache) inside of our main class. Then to access the Table logic, we can just pass an instantiated table into the LSMCache. This would only need to be done when we call search_table. Very simple.

Invalidation:

Data is never mutated, it is only deleted or written. We pull entries into the cache only when we read. So, we only need to remove stale (or would be) entries. This happens when we delete tables. We delete tables only when we compact. On compaction: iterate over tables to be deleted, delete corresponding filters, indices and blocks. For filters and indices, we can just peek and delete. For blocks, we need to know ahead of time how many blocks there are or check by prefix. There is no checking by prefix for schnellru, so we'll need to know the number of blocks for a table.

Since we do not cache table stats on startup, we'll need to get the index for that table, then count the entries or store the number of keys before the number of bytes for the offset. I'd much rather this than having to think about statistics for now. 

To summarize:

Create a LSMCache class. It will have 3 lru caches. It will expose functions for getting a key given some File (adding/evicting as needed) as well as an invalidation function taking the table names, blocknames to delete from the cahces. The invalidation function will peek these keys and delete them if they are in the cache. The function to calculate the keys to be invalidated will exist in LSMTree and it will incur 10 random read ios concurrently to get the number of blocks. 