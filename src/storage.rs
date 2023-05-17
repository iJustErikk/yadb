// lsm based storage engine
// level compaction whenever level hits 10 sstables

// byte ordering: default to little endian, allow user to opt to big if wanted

struct Tree {
    // let's start with 5 levels
    // this is a lot of space 111110 mb ~ 108.5 gb
    tables_per_level: [i8; 5],
    folder: Path
}

// BoB are preceded by 4 bytes signifying length
struct IndexEntry {
    key: Vec<u8>
    offset: i64,
    length: i64
}

struct SSTable {
    // meta (offsets, lengths for bloom filter, index, data)
    // bloom filter
    index: Vec<IndexEntry>,
    data: Vec<byte>
}

// could provide more ops in future, like the merge operator in pebble/rocksdb
enum Operation {
    GET, PUT, DELETE, INVALID 
}

struct WALEntry {
    operation: Operation,
    key: Vec<u8>,
    value: Vec<u8>
}

// in memory only
struct Memtable {
    // skiplist
    // look into this: skiplist can be made lock-free or wait-free (i have no idea what these mean, look into that as well)
    // need to optimize lock contention, batch io and make sure anything cpu bound is fast
    // need to make benchmarks (could steal pebble's or rocksdbs) so I can say something cool like: there are lies, damned lies and benchmarks
    // size counter
}

// init: user will specify a folder
// if empty, init
// if does not have all files, exit
// otherwise init from those, if header disagrees with file state, cleanup the uncommitted files (could be from a failed flush or compaction)
// run the WAL log
// that folder will contain a header file, a WAL file named wal and folders 0, 1, 2, 3... containing corresponing levels of sstables
// those folders will contain files named 0, 1, 2, 3... containing sstables of increasing recency
impl Tree {
    fn new(&self, bool cleanupUncommitted) -> self{
        if !self.path.exists() {
            fs::create_dir_all(path);
            // header is 5 bytes, corresponding to the i8 # of levels
            let mut header = File::create(path.clone().join("header"))?;
            header.write_all(&0_i8.to_be_bytes())?;
            File::create(path.clone().join("wal"))?;
            fs::create_dir_all(path.clone().join("0"));
            return Tree {buffer, path};
        } else {
            let mut buffer = [0; 5];
            let mut file = File::open(path.clone().join("header"))?;
            file.read_exact(&mut buffer)?;
            let mut discoveredTabels = [0; 5];
            // do sanity check over sstable levels
            // this is probably wrong
            // remove uncommitted files
            // put this into another fyucntion
            for entry_result in fs::read_dir(path)? {
                let entry = entry_result?;
                if entry.file_type()?.is_dir() {
                    for sub_entry_result in fs::read_dir(entry.path())? {
                        let sub_entry = sub_entry_result?;
                        if !entry.file_type()?.is_dir() && entry.file_name().startswith("uncommitted") {
                            remove_file(entry.path());
                        }
                    }
                }
            }
            self.cleanupUncommitted();
            self.headerSanityCheck();
            self.tryRestoreWAL();
            return Tree {buffer, path}
        }
    }
}
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


