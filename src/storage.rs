// lsm based storage engine
// level compaction whenever level hits 10 sstables

// wait: I need to figure out byte ordering
// little or big endian?

struct Tree {
    // let's start with 5 levels
    // this is a lot of space
    tables_per_level: [i32; 5],
}
// BoB are preceded by 4 bytes signifying length
struct IndexEntry {
    // key: BoB,
    // key_len: i32
    offset: i64,
    length: i64
}

struct SSTable {
    // meta (offsets, lengths for bloom filter, index, data)
    // bloom filter
    index: Vec<IndexEntry>,
    data: Vec<byte>
}

struct WALEntry {
    // operation (get put delete) how do I do enums?
    // keylen: i32
    // key: BoB,
    // value_len: i32
    value: 
    // value (for put)
}

// in memory only
struct Memtable {
    // skiplist
    // size counter
}

// init: user will specify a folder
// if empty, init
// if does not have all files, exit
// otherwise init from those, if header disagrees with file state, cleanup the uncommitted files (could be from a failed flush or compaction)
// run the WAL log
// that folder will contain a header file, a WAL file named wal and folders 0, 1, 2, 3... containing corresponing levels of sstables
// those folders will contain files named 0, 1, 2, 3... containing sstables of increasing recency

// read:
// search sstable first
// lower levels are newer than older levels. so start reading from the bottom most level and work upwards if key not found
// we should "append" sstables and read them in the order opposite of insertion into level
// this will give us the most recent key if it exists
// key exists at most once in single sstable, due to compaction or just avoiding that when writing to memtable
// so no need for LUB for finding the latest instance, can just use regular binary search
// add bloom filters after
// for bloom filter: search only if bloom filter cannot guarantee key is not in sstable

// write:
// write to memtable until it is large enough to flush (1mb?)
// memtable should be sorted and have unique keys before writing as sstable
// use skiplist for memtable
// operations:
// add/update/delete key
// read key
// preferably O(log(n))
// compress value immediately

// compaction:
// come back to this
// perform compaction when single level hits 10 sstables
// could just up to 10 pointers through, order traversal first by key and then by table age

// durability:
// use WAL, append on every write (how do I ensure this append is atomic?)
// on failure, WAL is no larger than full sstable
// when restoring (on startup, check if this is empty file):
// read into memory
// convert to sstable format (sort, remove duplicates)
// append as sstable
// clear wal
// compaction should update the LSM tree only as a last step
// all wal disk io should be flushed to disk immediately

// compression:
// find a good compression algorithm

// concurrency:
// going to add concurrency support soon
// need to figure out sync for in memory and disk ds
// could compact in another thread
// batch io

// figure out what I am missing


