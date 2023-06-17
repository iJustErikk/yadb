// lsm based storage engine
// level compaction whenever level hits 10 sstables
use std::fs;
use std::io::Cursor;
use std::io::Seek;
use std::io::SeekFrom;
use std::iter::Peekable;
use fs::File;
use std::fs::OpenOptions;

use fs::remove_file;
use std::path::PathBuf;
use std::error::Error;

use std::io::Write;
use std::io::Read;
use std::fmt;
extern crate byteorder;
use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};
use std::io::{self};
use std::convert::TryFrom;

use std::collections::BinaryHeap;


extern crate crossbeam_skiplist;
use crossbeam_skiplist::SkipMap;
extern crate tempfile;
use tempfile::tempdir;


// on tuning bloom filters:
// given a target false positive probability, we can calculate how many hash functions/bits per key we need
// cockroach's pebble uses 10, and they say that yields a 1% false positive rate
// that sounds like a reasonable default
// ^^ this can be the size of the skipmap or easily be calculated during compaction
// i also would like to avoid rehashing during compaction


// solution: make the index go after the datablock
// compaction algorithm:
// walk through each of the 10 files
// find smallest key of oldest file, add it to datablock creator
// datablock creator will overwrite a key if it is fed the same key (thus taking the younger one)
// if the current block size exceeds the limit, it will create a new block
// the first item of a block is added to the index
// we'll immediately write the data block
// then we write the index
// this allows us to avoid a two pass read during compaction
// unfortunately, it adds another io to reads (read the size of the datablock, skip that number of bytes)
// we could just cache that number, it is basically free (4 bytes)
// we could also cache the index, this may be infeasible. let's assume a key is 10% the number of bytes as the value and that 1% of keys in a 
// data block are represented in the sparse index
// then the index is roughly 1/1000 the size of the data block
// if we have terabytes on disk, that would be gigabytes in memory
// that is actually reasonable
// we could just cache what matters (lru)
// and we could cache compressed keys
// maybe we could have a compression function that preserves the ordering (this seems difficult)
// or we could just compress/decompress if memory gets too full (lots of cpu- this could be parallelized)


// metrics:
// - wal replays
// - wal replay total bytes
// - # sstables
// - disk usage
// - avg sstable size
// - overhead (lsm size) / (number of all bytes of keys, values, not including lengths)
// - read ios / read operation
// - write ios / write operation
// - number of corrupt rows

// look into: no copy network -> fs, I believe kafka or some other "big" tool does this
// look into: checksums for integrity
// look into: retry mechanism for failed fs calls
// look into: caching frequently read blocks (not keys, just avoid the block read io)
// TODO: go through with checklist to make sure the database can fail at any point during wal replay, compaction or memtable flush
// TODO: sometime later, create tests that mock fs state, test failures
// TODO: handle unwraps correctly

// assumption: user does not create any files in the database

// assumption: user only runs one instance for a given folder
// I'd imagine things would get corrupted quickly
// maybe have a sanity check and some mechanism like a lock/lease?
// when testing: mark process so it can be easily killed prior to startup
// assumption: in the future: user wants lexicographic ordering on keys
// as this is embedded, user could provide custom comparator or merge operator

// TODO: add more errors
// I'm wrapping IOError for now, but this is not useful for the end user.
// the user is going to want specific, actionable errors
#[derive(Debug)]
enum YAStorageError {
    IOError {error: io::Error},
    MissingHeader,
    CorruptedHeader,
    UnexpectedUserFolder { folderpath: String },
    UnexpectedUserFile { filepath: String },
    LevelTooLarge { level: u8 },
    SSTableMismatch { expected: Vec<u8>, actual: Vec<u8> },
    CorruptWalEntry
}

impl YAStorageError {
    fn new(error: io::Error) -> YAStorageError {
        YAStorageError::IOError{error}
    }
}

impl From<io::Error> for YAStorageError {
    fn from(error: io::Error) -> Self {
        YAStorageError::new(error)
    }
}

impl fmt::Display for YAStorageError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            YAStorageError::MissingHeader => write!(f, "Missing header"),
            YAStorageError::CorruptedHeader => write!(f, "Corrupted header"),
            YAStorageError::UnexpectedUserFolder { folderpath } => write!(f, "Unexpected user folder: {}", folderpath),
            YAStorageError::UnexpectedUserFile { filepath } => write!(f, "Unexpected user file: {}", filepath),
            YAStorageError::LevelTooLarge { level } => write!(f, "Level too large: {}", level),
            YAStorageError::SSTableMismatch { expected, actual } => write!(f, "SSTable mismatch: expected {:?}, got {:?}", expected, actual),
            YAStorageError::CorruptWalEntry => write!(f, "Corrupt Wal Entry"),
            YAStorageError::IOError { error } => write!(f, "IO Error: {}", error.to_string()),
        }
    }
}


impl Error for YAStorageError {}

struct Tree {
    // let's start with 5 levels
    // this is a lot of space 111110 mb ~ 108.5 gb
    num_levels: Option<usize>,
    tables_per_level: Option<[u8; 5]>,
    path: PathBuf,
    memtable: Memtable,
    wal_file: Option<File>
}

struct MergedTableIterators {
    heap: BinaryHeap<TableNum>
}

struct TableNum {
    table: Peekable<Table>, 
    num: usize
}

impl Eq for TableNum {}


impl PartialEq for TableNum {
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other) == std::cmp::Ordering::Equal
    }
}

impl PartialOrd for TableNum {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for TableNum {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        let first = self.table.peek();
        let second = other.table.peek();
        // we will not push a empty iterator back into the heap
        assert!(first.is_some());
        assert!(second.is_some());
        // if there is an error, we unfortunately cannot return it
        // TODO: cannot find an errable heap implementation, will need to spin one up
        // could very messily make errors less than non errors and then handle an error when popping
        let first = (first.unwrap().unwrap().0, self.num);
        let second = (second.unwrap().unwrap().0, other.num);
        // order first by peeked key ref, then by table number (higher is younger and higher)
        // we want this to be a min heap
        // order first by key (smaller key means greater)
        // then order by table number (smaller table number means greater)
        
        if first < second {
            std::cmp::Ordering::Greater
        } else if first > second {
            std::cmp::Ordering::Less
        } else if self.num < other.num {
            std::cmp::Ordering::Greater
        } else if self.num > other.num {
            std::cmp::Ordering::Less
        } else {
            // else does not happen
            // this will not panic
            // (this would happen if keys are equal and the table numbers are as well)
            // that could not possibly happen as we use enumerate
            panic!();
        }
    }
}

impl MergedTableIterators {
    fn new(tables: Vec<Table>) -> Self {
        let mut heap: BinaryHeap<TableNum> = BinaryHeap::new();
        for (num, table) in tables.into_iter().enumerate() {
            heap.push(TableNum{table: table.peekable(), num});
        }
        return MergedTableIterators { heap }
    }
}

impl Iterator for MergedTableIterators {
    type Item = (Vec<u8>, Vec<u8>);
    // TODO: for large sets of files (table iterators), use heaps
    // this implementation is currently only for level iterators (k = 10) -> heap likely is slower
    // since with small sets of files, the overhead of using heaps is lilely minimal, so could use it in either case

    fn next(&mut self) -> Option<Self::Item> {
        if self.heap.is_empty() {
            return None;
        }
        let res = self.heap.pop().unwrap().table.next();
        // we will not put back an empty iterator and if an iterator produced an error, we would have panicked before
        assert!(res.is_some() && res.unwrap().is_ok());
        let res = res.unwrap().unwrap();
        Some(res)
    }
}

struct Table {
    file: File,
    total_data_bytes: Option<u64>,
    current_block: Option<DataBlock>,
    current_block_idx: usize
}
impl Table {
    fn new(path: PathBuf, reading: bool) -> io::Result<Self> {
        let mut file = if reading { File::open(&path)? } else { File::create(&path)?};
        let total_data_bytes = None;
        if reading {
            file.seek(SeekFrom::End(-8))?;
            file.seek(SeekFrom::Start(0));
            let total_data_bytes = Some(file.read_u64::<LittleEndian>()?);
        }
        return Ok(Table {
            file, total_data_bytes, current_block: None, current_block_idx: 0
        });
    }
    fn get_index(&mut self) -> io::Result<Index> {
        self.file.seek(SeekFrom::End(-8))?;
        let datablock_size = self.file.read_u64::<LittleEndian>()?;
        self.file.seek(SeekFrom::Start(datablock_size))?;
        return Ok(Index::deserialize(&self.file)?);
    }
    fn get_datablock(&mut self, offset: u64) -> io::Result<DataBlock> {
        self.file.seek(SeekFrom::Start(offset))?;
        return Ok(DataBlock::deserialize(&mut self.file)?);
    }
    
    fn write_table<I>(&self, it: I) -> io::Result<()>  where I: IntoIterator<Item=(Vec<u8>, Vec<u8>)>, {
        // TODO: I could convert these to use the serialize function for DataBlock + Index
        // this would present overhead, as they would most likely be copied into the ds just to be written out
        // but it would keep the logic for serde in one impl
        let mut index: Vec<u8> = Vec::new();
        let mut data_section: Vec<u8> = Vec::new();
        let mut current_block: Vec<u8> = Vec::new();
        for (key, value) in it {
            if current_block.is_empty() {
                index.write_u64::<LittleEndian>(key.len() as u64)?;
                index.extend(key);
                index.write_u64::<LittleEndian>(data_section.len() as u64)?;
            }
            current_block.write_u64::<LittleEndian>(key.len() as u64)?;
            current_block.extend(key);
            current_block.write_u64::<LittleEndian>(value.len() as u64)?;
            current_block.extend(value);
            if current_block.len() > 4_000 {
                data_section.write_u64::<LittleEndian>(current_block.len() as u64)?;
                data_section.append(&mut current_block);
            }
        }
        // write last block if not already written
        if current_block.len() > 0 {
            data_section.write_u64::<LittleEndian>(current_block.len() as u64)?;
            data_section.append(&mut current_block);
        }

        self.file.write_all(&data_section)?;
        self.file.write_u64::<LittleEndian>(index.len() as u64)?;
        self.file.write_all(&index)?;
        // footer: 8 bytes for index offset
        self.file.write_u64::<LittleEndian>(data_section.len() as u64)?;
        Ok(())
    }
}
impl Iterator for Table {
    type Item = io::Result<(Vec<u8>, Vec<u8>)>;

    fn next(&mut self) -> Option<Self::Item> {
        // we better open this in read mode
        assert!(self.total_data_bytes.is_some());
        if self.current_block.is_none() || self.current_block_idx == self.current_block.as_ref().unwrap().entries.len() {
            if self.total_data_bytes.unwrap() == self.file.stream_position().unwrap() {
                // we've reached the end of the datablocks
                return None;
            }
            let current_block = DataBlock::deserialize(&mut self.file);
            if current_block.is_err() {
                return Some(Err(current_block.err().unwrap()));
            }
            self.current_block = Some(current_block.unwrap());
            self.current_block_idx = 0;
        }
        let kv = std::mem::replace(&mut self.current_block.as_ref().unwrap().entries[self.current_block_idx], (Vec::new(), Vec::new()));
        self.current_block_idx += 1;
        return Some(Ok(kv));
    }
}
// bloom filter:
// during compaction:
// overestimate expected n by summming number of unique keys per table
// this avoids doing a double read or more robust random sampling (many random IOs)
// use that n and 1% false positive to find size and number of hash functions

#[derive(Debug)]
// could provide more ops in future, like the merge operator in pebble/rocksdb
#[derive(PartialEq)]
#[derive(Copy, Clone)]
enum Operation {
    GET = 0, PUT = 1, DELETE = 2
}

impl TryFrom<u8> for Operation {
    type Error = ();

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            0 => Ok(Operation::GET),
            1 => Ok(Operation::PUT),
            2 => Ok(Operation::DELETE),
            _ => Err(()),
        }
    }
}

struct Index {
    entries: Vec<(Vec<u8>, u64)>,
}

// TODO: allow user to supply custom comparators
    // returns the bucket a key is potentially inside
    // each bucket i is [start key at i, start key at i + 1)
    // binary search over i
    // find i such that key is in the bucket
    // note: this always returns the last bucket for a key that is larger than the last bucket start key
    // if key in range, return mid
    // if key < start key, move right pointer to mid
    // if key >= next start key, move left pointer to right
    // proof of correctness:
    // assume value is in some bucket in the array
    // if throughout the execution the value is in the range of buckets, then it is correct
    // if all search space cuts preserve this, then we will return the bucket containing, as eventually
    // ...there will be only 1 bucket and that has to satisfy the property (could do proof by contradiction, see that if this was not the case...
    //  then there would be a different partitioning of the search space and thus a different last block)
    // if key < index_keys[mid] -> cuts right half (keeps mid's block), very much still in there
    // if key >= index_keys[mid + 1] -> cuts left half -> left half is known to be less
    // these maintain our containing invariant so this is correct
    fn search_index(index: &Index, key: &Vec<u8>) -> Option<u64> {
        assert!(index.entries.len() != 0);
        assert!(key.len() != 0);
        // TODO: do these comparators actl work?
        if key < &index.entries[0].0 {
            return None
        }
        
        let mut left = 0;
        let mut right = index.entries.len() - 1;
        loop {
            let mid = (left + right) / 2;
            if mid == index.entries.len() - 1 {
                assert!(key >= &index.entries[index.entries.len() - 1].0);
                return Some(index.entries[mid].1);
            }
            if key < &index.entries[mid].0 {
                right = mid;
            } else if key >= &index.entries[mid + 1].0  {
                left = mid + 1;
            } else {
                // if key > start && < end -> in bucket
                return Some(index.entries[mid].1);
            }
        }
    }

// index grows linearly with sstable data
// index has a key for each ~4kb block
// if we have 1tb of data
// then we have ~256m keys in the index
// that is at least 1gb memory
// that needs to be optimized, otherwise we have a hard limit
// also loading 1gb into memory is horrifically slow
// could also present ram starvation if done in parallel
// since index could not be read entirely into memory at that size
// checksums would be hard
// what is the max size?
// I could turn compaction off for top level if it become full
// what is the max size again? 1st level 10mb + 2nd 100mb + 3rd 1 gb + 4th 10gb + 5th 100gb ~< 200gb
// so the largest table would be ~ 10gb
// ~2.5 million keys in sparse index
// way less than 1gb in memory
impl Index {
    fn new() -> Self {
        return Index {entries: Vec::new()};
    }
    fn get_num_bytes(&self) -> u64 {
    let mut res = 0;
    for (k, _) in self.entries.iter() {
        res += k.len() as u64;
        res += 16;
    }
    return res;
}
    pub fn serialize(&self, mut writer: &File) -> io::Result<()> {
        writer.write_u64::<LittleEndian>(self.get_num_bytes())?;
        for (k, offset) in self.entries.iter() {
            writer.write_u64::<LittleEndian>(k.len() as u64);
            writer.write_all(&k);
            writer.write_u64::<LittleEndian>(*offset);
        }
        Ok(())
    }
    pub fn deserialize(mut reader: &File) -> io::Result<Index> {
        let mut entries = Vec::new();
        // TODO: is it the caller's responsibility to put the file pointer in the right place or is it this function's
        // I really should be wrapping these functions with functions in Table- one place to verify on disk structure implementation
        // last 8 bytes are index offset
        let index_size = reader.read_u64::<LittleEndian>()?;
        let mut bytes_read = 0;
        while bytes_read != index_size {
            let key_size = reader.read_u64::<LittleEndian>()?;
            let mut key = vec![0; key_size as usize];
            reader.read_exact(&mut key)?;
            let offset = reader.read_u64::<LittleEndian>()?;
            let u64_num_bytes = 8;
            bytes_read += 2 * u64_num_bytes + key_size;
            entries.push((key, offset));
        }
        Ok(Index{entries})
    }
}
struct WALEntry {
    operation: Operation,
    key: Vec<u8>,
    value: Vec<u8>,
}

struct DataBlock {
    entries: Vec<(Vec<u8>, Vec<u8>)>
}

impl DataBlock {
    fn new() -> Self {
        return DataBlock {entries: Vec::new()};
    }
    pub fn get_num_bytes(&self) -> u64 {
        let mut res = 0;
        for (k, v) in self.entries.iter() {
            res += k.len() as u64;
            res += v.len() as u64;
            res += 16;
        }
        return res;
    }
    pub fn serialize(&self, writer: &mut File) -> io::Result<()> {
        writer.write_u64::<LittleEndian>(self.get_num_bytes());
        for (k, v) in self.entries.iter() {
            writer.write_u64::<LittleEndian>(k.len() as u64);
            writer.write_all(&k);
            writer.write_u64::<LittleEndian>(v.len() as u64);
            writer.write_all(&v);
        }
        Ok(())
    }
    pub fn deserialize(reader: &mut File) -> io::Result<Self> {
        let block_size = reader.read_u64::<LittleEndian>()?;
        let mut block = vec![0; block_size as usize];
        reader.read_exact(&mut block)?;
        let block_len = block.len();
        let mut cursor = Cursor::new(block);
        let mut entries: Vec<(Vec<u8>, Vec<u8>)> = Vec::new();
        while (cursor.position() as usize) < block_len {
            let key_size = cursor.read_u64::<LittleEndian>()?;
            let mut key = vec![0; key_size as usize];
            cursor.read_exact(&mut key)?;


            let value_size = cursor.read_u64::<LittleEndian>()?;
            let mut value = vec![0; value_size as usize];
            cursor.read_exact(&mut value)?;
            entries.push((key, value));
        }

        Ok(DataBlock{entries: entries})
    }
}

impl WALEntry {
    pub fn serialize<W: Write>(&self, writer: &mut W) -> io::Result<()> {
        writer.write_u8(self.operation as u8)?;
        writer.write_u64::<LittleEndian>(self.key.len() as u64)?;
        writer.write_all(&self.key)?;
        writer.write_u64::<LittleEndian>(self.value.len() as u64)?;
        writer.write_all(&self.value)?;
        writer.write_all(b"\n")?;
        Ok(())
    }

    // this will fail if entry becomes corrupted or the write failed midway
    // for either, let's make the assumption that it happened on the last walentry
    // read to the end of the file
    // comeback and think of a better solution
    pub fn deserialize<R: Read>(reader: &mut R) -> io::Result<Self> {
        let operation = reader.read_u8()?;
        let operation = Operation::try_from(operation).map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "Invalid operation"))?;
        let key_len = reader.read_u64::<LittleEndian>()? as usize;
        let mut key = vec![0; key_len];
        reader.read_exact(&mut key)?;
        let value_len = reader.read_u64::<LittleEndian>()? as usize;
        let mut value = vec![0; value_len];
        reader.read_exact(&mut value)?;
        let mut newline = [0; 1];
        reader.read_exact(&mut newline)?;

        
        // if newline[0] != b'\n' {
        //     return Err(YAStorageError::CorruptWalEntry)?;
        // }
        Ok(WALEntry {
            operation,
            key,
            value,
        })
    }
}


// in memory only, serialize to sstable
// use skipmap/skiplist, as it could be made wait-free and it's faster than in memory b tree (and moreso when accounting for lock contentions)
// although I am sure a wait-free tree does exist, skiplists are much simpler
// this skiplist may or may not be wait-free...still need to look into it (maybe code my own)
struct Memtable {
    skipmap: SkipMap<Vec<u8>, Vec<u8>>,
    size: usize
}

// init: user will specify a folder
// if empty, init
// if does not have all files, exit
// otherwise init from those, if header disagrees with file state, cleanup the uncommitted files (could be from a failed flush or compaction)
// run the WAL log
// that folder will contain a header file, a WAL file named wal and folders 0, 1, 2, 3... containing corresponing levels of sstables
// those folders will contain files named 0, 1, 2, 3... containing sstables of increasing recency
impl Tree {
    pub fn new(path: &str) -> Self{
        return Tree {num_levels: None, wal_file: None, tables_per_level: None, path: PathBuf::from(path), memtable: Memtable{skipmap: SkipMap::new(), size: 0}};
    }
    pub fn init(&mut self) -> Result<(), YAStorageError> {
        self.init_folder()?;

        self.cleanup_uncommitted()?;

        self.general_sanity_check()?;

        self.restore_wal()?;
        Ok(())
    }
    fn init_folder(&mut self) -> Result<(), YAStorageError> {
        // TODO:
        // right now we just pass the error up to the user
        // we should probably send them a custom error with steps to fix
        if !self.path.exists() {
            fs::create_dir_all(&self.path)?;
        }
        if fs::read_dir(&self.path)?.next().is_none() {
            let buffer = [0; 5];
            self.tables_per_level = Some(buffer);

            // create is fine, no file to truncate
            let mut header = File::create(self.path.clone().join("header"))?;
            header.write_all(&buffer)?;
            self.wal_file = Some(OpenOptions::new()
            .read(true)
            .write(true)
            .append(true)
            .create(true)
            .open(self.path.clone().join("wal"))
            ?);

            
        } else {
            let mut buffer = [0; 5];
            // if this fails, alert the user with YAStorageError, missing header
            let mut file = File::open(self.path.clone().join("header"))?;
            // if this fails, alert the user with YAStorageError, corrupted header
            file.read_exact(&mut buffer)?;
            self.tables_per_level = Some(buffer);
            self.wal_file = Some(OpenOptions::new()
            .read(true)
            .write(true)
            .append(true)
            .open(self.path.clone().join("wal"))?);
        }
        Ok(())
    }
    // TODO: custom error and help steps as above
    // failure may happen in startup (wal replay), compaction or memtable flush
    // cleanup after failure
    fn cleanup_uncommitted(&self) -> Result<(), YAStorageError>  {
        for entry_result in fs::read_dir(&self.path)? {
            let entry = entry_result?;
            if entry.file_type()?.is_dir() {
                for sub_entry_result in fs::read_dir(entry.path())? {
                    let sub_entry = sub_entry_result?;
                    if sub_entry.file_type()?.is_file() && sub_entry.file_name().into_string().unwrap().starts_with("uncommitted") {
                        remove_file(&sub_entry.path())?;
                        if fs::read_dir(sub_entry.path().parent().unwrap())?.next().is_none() {
                            // if this folder is now empty, delete it
                            fs::remove_dir_all(sub_entry.path().parent().unwrap())?;
                        }
                    }
                }
            }
        }
        Ok(())
    }

    fn general_sanity_check(&mut self) -> Result<(), YAStorageError> {

        // any folder should be 0 - 4
        // any file in those folders should be numeric
        // # of sstables should match header
        // sstable names should be increasing (0 - # - 1)
        let mut num_levels: usize = 5;
        while num_levels != 0 && self.tables_per_level.unwrap()[num_levels - 1] == 0 {
            num_levels -= 1;
        }
        // stuff is getting initialized in too many places...is there a better way to do this?
        self.num_levels = Some(num_levels);
        for entry_result in fs::read_dir(&self.path)? {

            let entry = entry_result?;
            if entry.file_type()?.is_dir() {

                let level = match entry.file_name().into_string().unwrap().parse::<u8>() {
                    Ok(value) => Ok(value),
                    Err(_) => Err(YAStorageError::UnexpectedUserFolder{folderpath: entry.file_name().into_string().unwrap()})
                }?;
                if level > 4 {
                    return Err(YAStorageError::LevelTooLarge{level})?;
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
                        return Err(YAStorageError::UnexpectedUserFolder{folderpath: sub_entry.file_name().into_string().unwrap()})?;
                    }
                    let table_name = match sub_entry.file_name().into_string().unwrap().parse::<u8>() {
                        Ok(value) => Ok(value),
                        Err(_) => Err(YAStorageError::UnexpectedUserFile{filepath: sub_entry.file_name().into_string().unwrap()})
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
                    return Err(YAStorageError::SSTableMismatch{expected, actual})?;
                }
            } else if entry.file_type()?.is_file() && (entry.file_name() != "header" && entry.file_name() != "wal") {
                return Err(YAStorageError::UnexpectedUserFile{filepath: entry.path().into_os_string().into_string().unwrap()})?;
            }
        }

        Ok(())

    }
    
    fn search_table(&self, level: usize, table: u8, key: &Vec<u8>) -> Result<Option<Vec<u8>>, YAStorageError> {
        // not checking bloom filter yet
        let mut table: Table = Table::new(self.path.clone().join(level.to_string()).join(table.to_string()))?;
        let index = table.get_index()?;
        let byte_offset = search_index(&index, key);
        if byte_offset.is_none() {
            return Ok(None);
        }
        let mut block = table.get_datablock(byte_offset.unwrap())?;
        for (cantidate_key, value) in block.entries.iter_mut() {
            if cantidate_key == key {
                return Ok(Some(std::mem::replace(value, Vec::new())));
            }
        }
        return Ok(None)
    }
    pub fn get(&self, key: &Vec<u8>) -> Result<Option<Vec<u8>>, YAStorageError> {
        assert!(key.len() != 0);
        if let Some(value) = self.memtable.skipmap.get(key) {
            let res = value.value().to_vec();
            if res.len() == 0 {
                return Ok(None);
            }
            return Ok(Some(res));
        }
        for level in 0..self.num_levels.unwrap() {
            for sstable in (0..self.tables_per_level.unwrap()[level]).rev() {
                // TODO: if value vector is empty, this is a tombstone
                if let Some(res) = self.search_table(level, sstable, &key)? {
                    // empty length vector is tombstone
                    // clients cannot write an empty length value
                    // is this limiting?
                    if res.len() == 0 {
                        return Ok(None);
                    }
                    return Ok(Some(res));
                }
            }
        }
        return Ok(None);
    }
    fn get_next_sstable_location() {

    }
    // convert skipmap to sstable
    // start calling compaction
    
    fn write_skipmap_as_sstable(&mut self) -> Result<(), io::Error> {
        let table_to_write = self.tables_per_level.unwrap()[0];
        let filename = format!("uncommitted{}", table_to_write.to_string());
        if !self.path.join("0").exists() {
            fs::create_dir(self.path.join("0"))?;
        }
        // we better not be overwriting something
        // otherwise we are shooting ourselves in the foot
        assert!(!self.path.join("0").join(&filename).exists());

        // TODO: begin write by creating table, then passing iterator for writing
        let old_path = self.path.join("0").join(&filename);
        let table = Table::new(old_path, false)?;
        table.write_table(self.memtable.skipmap.into_iter());

        // TODO: this stuff is similar to what will be in compaction- this should be inside of table
        // call it commit_table- should be fine to call inside of write_table
        let new_path = self.path.join("0").join(table_to_write.to_string());
        // before we commit the table, update header
        self.tables_per_level.as_mut().unwrap()[0] += 1;
        if self.num_levels.unwrap() == 0 {
            self.num_levels = Some(1);
        }
        fs::write(self.path.join("header"), self.tables_per_level.unwrap())?;
        std::fs::rename(old_path, new_path)?;
        table.file.sync_all()?;
        self.memtable.skipmap = SkipMap::new();
        if self.tables_per_level.unwrap()[0] == 10 {
            self.compact()?;
        }
        Ok(())
    }
    
    fn compact_level(&mut self, level: usize) -> Result<(), io::Error> {
        assert!(level != 4);
        let new_table = self.tables_per_level.unwrap()[level + 1];
        let new_table_path = self.path.join((level + 1).to_string()).join(new_table.to_string());
        let table = Table::new(new_table_path, true)?;
        table.write_table(MergedTableIterators::new((0..10).map())
        // create new table, merged table iterators
        // finish writing file with merged table iterators
        // commit table
        // update tables_per_level and num_levels
        // flush header
        // done!
        Ok(())
    }
    fn compact(&mut self) -> Result<(), io::Error> {
        let mut level = 0;
        // this may fail between compactions, so we need to check if we need to compact on startup
        while self.tables_per_level.unwrap()[level] == 10 {
            self.compact_level(level)?;
            level += 1;
        }
        Ok(())
    }

    fn append_to_wal(&mut self, entry: WALEntry) -> Result<(), io::Error> {
        entry.serialize(&mut (self.wal_file.as_mut().unwrap()))?;
        // wal metadata should not change, so sync_data is fine to use, instead of sync_all/fsync
        self.wal_file.as_mut().unwrap().sync_data()?;
        Ok(())
    }

    fn restore_wal(&mut self) -> Result<(), io::Error> {
        let mut entries = Vec::new();
        assert!(self.path.join("wal").exists());
        loop {
            match WALEntry::deserialize(&mut (self.wal_file.as_mut().unwrap())) {
                Ok(entry) => entries.push(entry),
                Err(e) => {
                    if e.kind() == io::ErrorKind::UnexpectedEof {
                        println!("this happens");
                        break;
                    } else {
                        return Err(e);
                    }
                }
            }
        }
        

        let skipmap = SkipMap::new();

        for entry in entries {
            match entry.operation {
                Operation::PUT => { skipmap.insert(entry.key, entry.value); }
                // empty vector is tombstone
                Operation::DELETE => { skipmap.insert(entry.key, Vec::new()); }
                _ => {}
            }
        };

        self.memtable.skipmap = skipmap;

        self.write_skipmap_as_sstable()?;
        // wal file persisted, truncate
        self.wal_file.as_mut().unwrap().set_len(0)?;
        
        self.wal_file.as_mut().unwrap().sync_all()?;

        Ok(())
    }

    fn add_walentry(&mut self, operation: Operation, key: &Vec<u8>, value: &Vec<u8>) -> Result<(), YAStorageError>  {
        let old_size = if self.memtable.skipmap.contains_key(key) { key.len() + self.memtable.skipmap.get(key).unwrap().value().len()} else { 0};
        let new_size = key.len() + value.len();
        // split these up, as adding the difference causes overflow on deletes
        self.memtable.size += new_size;
        self.memtable.size -= old_size;
        // TODO: I could remove the copies one of two ways:
        // - take ownership of kv (will the end user want this?)
        // - work with references (this will put the effort on the end user)
        // right now, copying sounds best as it avoids making the user keep the kv in memory or having to copy into this function
        if operation == Operation::PUT {
            self.memtable.skipmap.insert(key.to_vec(), value.to_vec());
        } else if operation == Operation::DELETE {
            self.memtable.skipmap.insert(key.to_vec(), Vec::new());
        }

        self.append_to_wal(WALEntry { operation, key: key.to_vec(), value: value.to_vec() })?;
        // TODO: no magic values
        // add config setting
        if self.memtable.size > 1_000_000 {
            self.write_skipmap_as_sstable()?;
            self.memtable.skipmap = SkipMap::new();
            // wal persisted, truncate now
            self.wal_file.as_mut().unwrap().set_len(0).unwrap();
            self.wal_file.as_mut().unwrap().sync_all().unwrap();
        }
        Ok(())
    }

    pub fn put(&mut self, key: &Vec<u8>, value: &Vec<u8>) -> Result<(), YAStorageError> {
        assert!(key.len() != 0);
        // empty length value is tombstone
        assert!(value.len() != 0);
        self.add_walentry(Operation::PUT, key, value)?;
        Ok(())
    }

    pub fn delete(&mut self, key: &Vec<u8>) -> Result<(), YAStorageError>  {
        assert!(key.len() != 0);
        // empty value is tombstone
        self.add_walentry(Operation::DELETE, key, &Vec::new())?;
        Ok(())
    }
}

// compression:
// find a good compression algorithm

// concurrency:
// going to add concurrency support soon
// need to figure out sync for in memory and disk ds
// could compact in another thread
// batch io

// testing only
// need to get test suite going
// haha print does not flush
// reminds me of the good ole days
fn main() -> Result<(), Box<dyn Error>> {
    let mut tree = Tree::new("./yadb");
    tree.init()?;
    println!("init");
    let repeats = 1000;
    for i in 1..repeats {
        // println!("{}", i);
        let key = i.to_string();
        let value = 0.to_string();
        tree.get(&(key.as_bytes().to_vec()))?;
        tree.put(&(key.as_bytes().to_vec()), &(value.as_bytes().to_vec()))?;
        tree.delete(&(key.as_bytes().to_vec()))?;
    }
    
    Ok(())
}
// unit tests
// (try to) test individual functions
// need to research mocking
// TODO: wrap any writing to header/wal/sstable so code/tests stay consistent
// TODO: version header/wal/sstable to provide backward compatibility
#[cfg(test)]
mod init_tests {
    use std::fs::{create_dir, remove_dir};
    use super::*;
    // TODO: these check for the existence of some error
    // however, they should check for the particular error that arises
    // will fix this when I fix the API. I should be returning custom errors to the user
    #[test]
    fn test_init_create() -> Result<(), Box<dyn Error>> {
        let dir = tempdir()?;
        let mut tree = Tree::new(dir.path().as_os_str().to_str().unwrap());
        tree.init_folder().expect("Failed to init folder");
        let header_path = dir.path().clone().join("header");
        let wal_path = dir.path().clone().join("header");
        assert!(header_path.exists());
        assert!(wal_path.exists());
        let mut buf = [0; 5];
        let expected = [0; 5];
        let mut header_file = File::open(header_path)?;
        header_file.read_exact(&mut buf)?;
        assert_eq!(buf, expected);
        Ok(())
    }
    // going to simply create another tree for convenience
    // TODO: figure this out with tree locking scheme
    #[test]
    fn missing_header() -> Result<(), Box<dyn Error>> {
        // the following 3 lines are pretty repetitive
        // i'm going to opt out of using a helper function, as i am sure this will not change
        // and it will be roughly the same LOC
        let dir = tempdir()?;
        let mut tree = Tree::new(dir.path().as_os_str().to_str().unwrap());
        tree.init_folder().expect("Failed to init folder");
        remove_file(dir.path().join("header"))?;
        let mut tree = Tree::new(dir.path().as_os_str().to_str().unwrap());
        assert!(tree.init().is_err());
        Ok(())
    }
    #[test]
    fn missing_wal() -> Result<(), Box<dyn Error>> {
        let dir = tempdir()?;
        let mut tree = Tree::new(dir.path().as_os_str().to_str().unwrap());
        tree.init_folder().expect("Failed to init folder");
        remove_file(dir.path().join("wal"))?;
        let mut tree = Tree::new(dir.path().as_os_str().to_str().unwrap());
        assert!(tree.init().is_err());
        Ok(())
    }
    #[test]
    fn extraneous_file_root() -> Result<(), Box<dyn Error>> {
        let dir = tempdir()?;
        let mut tree = Tree::new(dir.path().as_os_str().to_str().unwrap());
        tree.init_folder().expect("Failed to init folder");
        File::create(dir.path().join("test"))?;
        let mut tree = Tree::new(dir.path().as_os_str().to_str().unwrap());
        assert!(tree.init().is_err());
        Ok(())
    }
    #[test]
    fn extraneous_folder_root() -> Result<(), Box<dyn Error>> {
        let dir = tempdir()?;
        let mut tree = Tree::new(dir.path().as_os_str().to_str().unwrap());
        tree.init_folder().expect("Failed to init folder");
        create_dir(dir.path().join("test"))?;
        let mut tree = Tree::new(dir.path().as_os_str().to_str().unwrap());
        assert!(tree.init().is_err());
        remove_dir(dir.path().join("test"))?;
        create_dir(dir.path().join("5"))?;
        let mut tree = Tree::new(dir.path().as_os_str().to_str().unwrap());
        assert!(tree.init().is_err());
        Ok(())
    }
    #[test]
    fn extraneous_file_nonroot() -> Result<(), Box<dyn Error>> {
        let dir = tempdir()?;
        let mut tree = Tree::new(dir.path().as_os_str().to_str().unwrap());
        tree.init_folder().expect("Failed to init folder");
        // for some level to exist, we need a sstable
        // TODO: there can be a case where a level folder (other than 0) exists, but there are no tables inside (failure during compaction)
        // see if this presents any issues
        // rewrite header with bytes 10000
        let mut header_file = File::create(dir.path().join("header"))?;
        let new_header: [u8; 5] = [1, 0, 0, 0, 0];
        header_file.write_all(&new_header)?;
        create_dir(dir.path().join("0"))?;
        File::create(dir.path().join("0").join("0"))?;
        File::create(dir.path().join("0").join("test"))?;
        let mut tree = Tree::new(dir.path().as_os_str().to_str().unwrap());
        tree.init_folder().expect("Failed to init folder");
        Ok(())
    }
    #[test]
    fn extraneous_folder_nonroot() -> Result<(), Box<dyn Error>> {
        let dir = tempdir()?;
        let mut tree = Tree::new(dir.path().as_os_str().to_str().unwrap());
        tree.init_folder().expect("Failed to init folder");

        let mut header_file = File::create(dir.path().join("header"))?;
        let new_header: [u8; 5] = [1, 0, 0, 0, 0];
        header_file.write_all(&new_header)?;
        create_dir(dir.path().join("0"))?;
        create_dir(dir.path().join("0").join("0"))?;
        let mut tree = Tree::new(dir.path().as_os_str().to_str().unwrap());
        tree.init_folder().expect("Failed to init folder");
        Ok(())
    }
    #[test]
    fn non_contiguous_sstables() -> Result<(), Box<dyn Error>> {
        let dir = tempdir()?;
        let mut tree = Tree::new(dir.path().as_os_str().to_str().unwrap());
        tree.init_folder().expect("Failed to init folder");
        let mut header_file = File::create(dir.path().join("header"))?;
        let new_header: [u8; 5] = [2, 0, 0, 0, 0];
        header_file.write_all(&new_header)?;
        create_dir(dir.path().join("0"))?;
        File::create(dir.path().join("0").join("0"))?;
        File::create(dir.path().join("0").join("2"))?;
        let mut tree = Tree::new(dir.path().as_os_str().to_str().unwrap());
        tree.init_folder().expect("Failed to init folder");
        Ok(())
    }
    #[test]
    fn no_zero_sstable() -> Result<(), Box<dyn Error>> {
        let dir = tempdir()?;
        let mut tree = Tree::new(dir.path().as_os_str().to_str().unwrap());
        tree.init_folder().expect("Failed to init folder");
        let mut header_file = File::create(dir.path().join("header"))?;
        let new_header: [u8; 5] = [1, 0, 0, 0, 0];
        header_file.write_all(&new_header)?;
        create_dir(dir.path().join("0"))?;
        File::create(dir.path().join("0").join("1"))?;
        let mut tree = Tree::new(dir.path().as_os_str().to_str().unwrap());
        tree.init_folder().expect("Failed to init folder");
        Ok(())
    }
    #[test]
    fn sstables_header_mismatch() -> Result<(), Box<dyn Error>> {
        let dir = tempdir()?;
        let mut tree = Tree::new(dir.path().as_os_str().to_str().unwrap());
        tree.init_folder().expect("Failed to init folder");
        let mut header_file = File::create(dir.path().join("header"))?;
        let new_header: [u8; 5] = [3, 1, 0, 0, 0];
        // we'll make it so there are only 2 sstables in the first level
        header_file.write_all(&new_header)?;
        create_dir(dir.path().join("0"))?;
        File::create(dir.path().join("0").join("0"))?;
        File::create(dir.path().join("0").join("1"))?;
        create_dir(dir.path().join("1"))?;
        File::create(dir.path().join("0").join("0"))?;
        let mut tree = Tree::new(dir.path().as_os_str().to_str().unwrap());
        tree.init_folder().expect("Failed to init folder");
        Ok(())
    }
}
#[cfg(test)]
mod search_index_tests {
    // fn search_index(index: &Index, key: &Vec<u8>) -> Option<u64> {
    use super::*;
    fn str_to_byte_buf(s: &str) -> Vec<u8> {
        return s.as_bytes().to_vec();
    }
    fn index_from_string_vector(slice: &[&str]) -> Index {
        return Index {
            entries: slice.iter().enumerate().map(|(i, &x)| (str_to_byte_buf(x), i as u64)).collect()
        }
    }
    // for these tests: vector ordering is lexicographic, so it will respect 1 byte character strings
    #[test]
    fn test_out_of_range_to_the_left() {
        let index = ["bombastic", "sideeye"];
        assert!(search_index(&index_from_string_vector(&index), &str_to_byte_buf(&"aeon")).is_none());
    }
    #[test]
    fn test_item_first_middle_last() {
        let index = ["affluent", "burger", "comes", "to", "town", "sunday"];
        assert!(search_index(&index_from_string_vector(&index), &str_to_byte_buf(&"apple")).unwrap() == 0);
        assert!(search_index(&index_from_string_vector(&index), &str_to_byte_buf(&"todler")).unwrap() == 3);
        assert!(search_index(&index_from_string_vector(&index), &str_to_byte_buf(&"zephyr")).unwrap() == 5);

    }    // test item on boundary of buckets (just barely less than the next)
    #[test]
    fn test_left_right_bounary() {
        let index = ["affluent", "burger", "comes"];
        assert!(search_index(&index_from_string_vector(&index), &str_to_byte_buf(&"burger")).unwrap() == 1);
        assert!(search_index(&index_from_string_vector(&index), &str_to_byte_buf(&"comeq")).unwrap() == 1);
        assert!(search_index(&index_from_string_vector(&index), &str_to_byte_buf(&"comes")).unwrap() == 2);

    }
    #[test]
    fn test_single_two_item_index() {
        let single = ["affluent"];
        let double = ["affluent", "burger"];
        assert!(search_index(&index_from_string_vector(&single), &str_to_byte_buf(&"affluent")).unwrap() == 0);
        assert!(search_index(&index_from_string_vector(&single), &str_to_byte_buf(&"burger")).unwrap() == 0);
        assert!(search_index(&index_from_string_vector(&double), &str_to_byte_buf(&"aging")).unwrap() == 0);
        // Hired as eng. Promoted to pun master.
        assert!(search_index(&index_from_string_vector(&double), &str_to_byte_buf(&"burgeroisie")).unwrap() == 1);
    }
}
/* Things to test:
persistence happens through one of three ways: memtable flush, wal restoration, compaction
should test g/p/d in each of those scenarios
should test key being in middle of datablock
should test binary search function
key found in nonfirst datablock

empty k/v
super long k/v

test compaction

out of space
test recovery - difficult to test this (could make any IO fail, but how can I do this without mocking every IO)
performance

big honking test

fuzz testing
think of more

future tests:
concurrency
merge operator
custom comparator
(different compaction strategies?)

 */