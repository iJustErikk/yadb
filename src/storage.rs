// lsm based storage engine
// level compaction whenever level hits 10 sstables
use std::fs;
use fs::File;
use fs::remove_file;
use std::path::PathBuf;
use std::error::Error;

use std::io::Write;
use std::io::Read;
use std::fmt;
// metrics:
// - wal replays
// - wal replay total bytes
// - # sstables
// - 

// look into: checksums for integrity
// look into: retry mechanism for failed fs calls
// TODO: go through with checklist to make sure the database can fail at any point during wal replay, compaction or memtable flush
// TODO: sometime later, create tests that mock fs state, test failures
// TODO: handle unwraps

// byte ordering: rust uses whatever the user's machine is
// as this is an embedded database, they are probably going with whatever their machine uses and there is no reason to let them choose

// assumption: user does not create any files in the database

// assumption: user only runs one instance for a given folder
// I'd imagine things would get corrupted quickly
// maybe have a sanity check and some mechanism like a lock/lease?
// when testing: mark process so it can be easily killed prior to startup
#[derive(Debug)]
enum InvalidDatabaseStateError {
    MissingHeader,
    CorruptedHeader,
    UnexpectedUserFolder { folderpath: String },
    UnexpectedUserFile { filepath: String },
    LevelTooLarge { level: u8 },
    SSTableMismatch { expected: Vec<u8>, actual: Vec<u8> },
}

impl fmt::Display for InvalidDatabaseStateError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            InvalidDatabaseStateError::MissingHeader => write!(f, "Missing header"),
            InvalidDatabaseStateError::CorruptedHeader => write!(f, "Corrupted header"),
            InvalidDatabaseStateError::UnexpectedUserFolder { ref folderpath } => write!(f, "Unexpected user folder: {}", folderpath),
            InvalidDatabaseStateError::UnexpectedUserFile { ref filepath } => write!(f, "Unexpected user file: {}", filepath),
            InvalidDatabaseStateError::LevelTooLarge { ref level } => write!(f, "Level too large: {}", level),
            InvalidDatabaseStateError::SSTableMismatch { ref expected, ref actual } => write!(f, "SSTable mismatch: expected {:?}, got {:?}", expected, actual),
        }
    }
}

impl Error for InvalidDatabaseStateError {}

struct Tree {
    // let's start with 5 levels
    // this is a lot of space 111110 mb ~ 108.5 gb
    tables_per_level: Option<[u8; 5]>,
    path: PathBuf
}

// BoB are preceded by 4 bytes signifying length
struct IndexEntry {
    key: Vec<u8>,
    offset: i64,
    length: i64
}

struct SSTable {
    // meta (offsets, lengths for bloom filter, index, data)
    // bloom filter
    index: Vec<IndexEntry>,
    data: Vec<u8>
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

// in memory only, serialize to sstable
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
    pub fn new(path: PathBuf) -> Self{
        return Tree {tables_per_level: None, path}
    }
    pub fn init(&mut self) -> Result<(), Box<dyn Error>> {
        self.init_folder()?;
        self.cleanup_uncommitted()?;
        self.general_sanity_check()?;
        // self.tryRestoreWAL();
        Ok(())
    }
    fn init_folder(&mut self) -> Result<(), Box<dyn Error>> {
        // TODO:
        // right now we just pass the error up to the user
        // we should probably send them a custom error with steps to fix
        if !self.path.exists() {
            fs::create_dir_all(&self.path)?;
            let buffer = [0; 5];
            self.tables_per_level = Some(buffer);
            let mut header = File::create(self.path.clone().join("header"))?;
            header.write_all(&buffer)?;
            File::create(self.path.clone().join("wal"))?;
            
        } else {
            let mut buffer = [0; 5];
            // if this fails, alert the user with InvalidDatabaseStateError, missing header
            let mut file = File::open(self.path.clone().join("header"))?;
            // if this fails, alert the user with InvalidDatabaseStateError, corrupted header
            file.read_exact(&mut buffer)?;
            self.tables_per_level = Some(buffer);
        }
        Ok(())
    }
    // TODO: custom error and help steps as above
    // failure may happen in startup (wal replay), compaction or memtable flush
    // cleanup after failure
    fn cleanup_uncommitted(&self) -> Result<(), Box<dyn Error>>  {
        for entry_result in fs::read_dir(&self.path)? {
            let entry = entry_result?;
            if entry.file_type()?.is_dir() {
                for sub_entry_result in fs::read_dir(entry.path())? {
                    let sub_entry = sub_entry_result?;
                    if sub_entry.file_type()?.is_file() && sub_entry.file_name().into_string().unwrap().starts_with("uncommitted") {
                        remove_file(&sub_entry.path())?;
                        if fs::read_dir(sub_entry.path().parent().unwrap()).unwrap().next().is_none() {
                            // if this folder is now empty, delete it
                            fs::remove_dir_all(sub_entry.path().parent().unwrap())?;
                        }
                    }
                }
            }
        }
        Ok(())
    }

    fn general_sanity_check(&self) -> Result<(), Box<dyn Error>> {
        // any folder should be 0 - 4
        // any file in those folders should be numeric
        // # of sstables should match header
        // sstable names should be increasing (0 - # - 1)
        let mut num_levels: usize = 5;
        while num_levels != 0 && self.tables_per_level.unwrap()[num_levels - 1] == 0 {
            num_levels -= 1;
        }
        for entry_result in fs::read_dir(&self.path)? {
            let entry = entry_result?;
            if entry.file_type()?.is_dir() {
                let level = match entry.file_name().into_string().unwrap().parse::<u8>() {
                    Ok(value) => Ok(value),
                    Err(_) => Err(InvalidDatabaseStateError::UnexpectedUserFolder{folderpath: entry.file_name().into_string().unwrap()})
                }?;
                if level > 4 {
                    return Err(InvalidDatabaseStateError::LevelTooLarge{level})?;
                }
                // assumption: if there is some valid file/fodler, then it was generated by the database
                if level >= num_levels as u8 {
                    // if there is a level that is not reflected in header and given our previous assumption
                    // then it was generated by a failed compaction
                    // delete folder and continue
                    fs::remove_dir_all(entry.path())?;
                    continue;
                }
                // let's count sstables
                // let's assume that if the filename is valid, then it is a valid sstable
                // could do serialization/corruption checks with checksum
                let mut actual = Vec::new();
                for sub_entry_result in fs::read_dir(entry.path())? {
                    // ensure this is a valid 
                    let sub_entry = sub_entry_result?;
                    if sub_entry.file_type()?.is_dir() {
                        return Err(InvalidDatabaseStateError::UnexpectedUserFolder{folderpath: sub_entry.file_name().into_string().unwrap()})?;
                    }
                    let table_name = match sub_entry.file_name().into_string().unwrap().parse::<u8>() {
                        Ok(value) => Ok(value),
                        Err(_) => Err(InvalidDatabaseStateError::UnexpectedUserFile{filepath: sub_entry.file_name().into_string().unwrap()})
                    }?;
                    actual.push(table_name);
                }
                actual.sort();
                // TODO: if database fails during compaction, wal replay or memtable flush
                // there may be too many sstables and we can just delete them
                // this must be equal to the range from 0 to # expected - 1
                // must revisit
                let expected: Vec<u8> = (0..self.tables_per_level.unwrap()[level as usize]).collect();
                if actual != expected {
                    return Err(InvalidDatabaseStateError::SSTableMismatch{expected, actual})?;
                }
            } else if entry.file_type()?.is_file() && (entry.file_name() != "header" || entry.file_name() != "wal") {
                return Err(InvalidDatabaseStateError::UnexpectedUserFile{filepath: entry.path().into_os_string().into_string().unwrap()})?;
            }
        }
        Ok(())
    }
}
// read:
// search memtable first
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
// perform compaction when single level hits 10 sstables? (leveldb)
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

fn main() {}