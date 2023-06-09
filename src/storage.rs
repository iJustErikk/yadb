// lsm based storage engine
// level compaction whenever level hits 10 sstables
use std::fs;
use std::io::Cursor;
use std::io::Seek;
use std::io::SeekFrom;
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
use std::string;

extern crate crossbeam_skiplist;
use crossbeam_skiplist::SkipMap;

// what we (de)serialize:
// wal log
// indexes
// data blocks
// ensure that: 
// - endianness is not an issue
// - writes and reads agree
// - leave room for error handling


// on tuning bloom filters:
// given a target false positive probability, we can calculate how many hash functions/bits per key we need
// cockroach's pebble uses 10, and they say that yields a 1% false positive rate
// that sounds like a reasonable default
// ^^ this can be the size of the skipmap or easily be calculated during compaction
// i also would like to avoid rehashing during compaction

// when compacting: how can we avoid 2 passes? (cannot do this completely in memory!)
// we cannot load everything into memory
// but we do not know the exact size of the index, so how could we write index/data blocks at same time?
// need to write them at same time, as we cannot write one before the other
// could estimate that the new index will be the size of both added together

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
#[derive(Debug)]
enum InvalidDatabaseStateError {
    MissingHeader,
    CorruptedHeader,
    UnexpectedUserFolder { folderpath: String },
    UnexpectedUserFile { filepath: String },
    LevelTooLarge { level: u8 },
    SSTableMismatch { expected: Vec<u8>, actual: Vec<u8> },
    CorruptWalEntry
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
            InvalidDatabaseStateError::CorruptWalEntry => write!(f, "Corrupt Wal Entry")
        }
    }
}


impl Error for InvalidDatabaseStateError {}

struct Tree {
    // let's start with 5 levels
    // this is a lot of space 111110 mb ~ 108.5 gb
    num_levels: Option<usize>,
    tables_per_level: Option<[u8; 5]>,
    path: PathBuf,
    memtable: Memtable,
    wal_file: Option<File>
}

// from rocksdb:
// Bloom Filter | Index Block | Data Block 1 | Data Block 2 | ... | Data Block N |
// before each block: the number of bytes for that block (u64)
// why use sparse index?
// index gets loaded either way
// so why not have it list all entries and get the exact block
// then you do not need keys for data blocks...
// this had to be more performant in some regard
// ill benchmark this to see if its really better

#[derive(Debug)]
// could provide more ops in future, like the merge operator in pebble/rocksdb
#[derive(PartialEq)]
#[derive(Copy, Clone)]
enum Operation {
    GET = 0, PUT = 1, DELETE = 2, INVALID = 3 
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
    pub fn deserialize<R: Read>(mut reader: R) -> Result<Index, Box<dyn Error>> {
        let mut entries = Vec::new();
        // for now, just store the length here
        // but, when we get to compaction
        // we won't know the length of the data or index before we start writing them
        // as they are not in memory
        // why not store this in the header?
        // we have at most 50 tables, so storing more data there will still be atomic within a single write
        let index_size = reader.read_u64::<LittleEndian>().unwrap();
        let mut bytes_read = 0;
        while bytes_read != index_size {
            let key_size = reader.read_u64::<LittleEndian>().unwrap();
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
    pub fn deserialize<R: Read>(reader: &mut R) -> io::Result<Self> {
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
        //     return Err(InvalidDatabaseStateError::CorruptWalEntry)?;
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
    pub fn init(&mut self) -> Result<(), Box<dyn Error>> {
        self.init_folder()?;

        self.cleanup_uncommitted()?;

        self.general_sanity_check()?;

        self.restore_wal()?;
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

            // create is fine, no file to truncate
            let mut header = File::create(self.path.clone().join("header"))?;
            header.write_all(&buffer)?;
            self.wal_file = Some(OpenOptions::new()
            .read(true)
            .write(true)
            .append(true)
            .create(true)
            .open(self.path.clone().join("wal"))
            .unwrap());

            
        } else {
            let mut buffer = [0; 5];
            // if this fails, alert the user with InvalidDatabaseStateError, missing header
            let mut file = File::open(self.path.clone().join("header"))?;
            // if this fails, alert the user with InvalidDatabaseStateError, corrupted header
            file.read_exact(&mut buffer)?;
            self.tables_per_level = Some(buffer);
            self.wal_file = Some(OpenOptions::new()
            .read(true)
            .write(true)
            .append(true)
            .open(self.path.clone().join("wal"))
            .unwrap());
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

    fn general_sanity_check(&mut self) -> Result<(), Box<dyn Error>> {

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
            } else if entry.file_type()?.is_file() && (entry.file_name() != "header" && entry.file_name() != "wal") {
                return Err(InvalidDatabaseStateError::UnexpectedUserFile{filepath: entry.path().into_os_string().into_string().unwrap()})?;
            }
        }

        Ok(())

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
    fn search_index(&self, index: &Index, key: &Vec<u8>) -> Option<u64> {
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
                return Some(mid as u64);
            }
            if key < &index.entries[mid].0 {
                right = mid;
            } else if key >= &index.entries[mid + 1].0  {
                left = mid + 1;
            } else {
                // if key > start && < end -> in bucket
                return Some(mid as u64);
            }
        }
    }
    fn search_table(&self, level: usize, table: u8, key: &Vec<u8>) -> Option<Vec<u8>> {
        // not checking bloom filter yet
        let mut table = File::open(self.path.clone().join(level.to_string()).join(table.to_string())).unwrap();
        let index = Index::deserialize(&table).unwrap();
        let block_num = self.search_index(&index, key);
        if block_num.is_none() {
            return None;
        }
        table.seek(SeekFrom::Current(index.entries[block_num.unwrap() as usize].1 as i64)).unwrap();
        let block = DataBlock::deserialize(&mut table).unwrap();
        for (cantidate_key, value) in block.entries {
            if &cantidate_key == key {
                return Some(value);
            }
        }
        return None
    }
    pub fn get(&self, key: &Vec<u8>) -> Option<Vec<u8>> {
        assert!(key.len() != 0);
        if let Some(value) = self.memtable.skipmap.get(key) {
            let res = value.value().to_vec();
            if res.len() == 0 {
                return None;
            }
            return Some(res);
        }
        for level in 0..self.num_levels.unwrap() {
            for sstable in (0..self.tables_per_level.unwrap()[level]).rev() {
                // TODO: if value vector is empty, this is a tombstone
                if let Some(res) = self.search_table(level, sstable, &key) {
                    // empty length vector is tombstone
                    // clients cannot write an empty length value
                    // is this limiting?
                    if res.len() == 0 {
                        return None;
                    }
                    return Some(res);
                }
            }
        }
        return None;
    }
    fn get_next_sstable_location() {

    }
    // convert skipmap to sstable
    // start calling compaction
    
    fn write_skipmap_as_sstable(&mut self) {
        if self.memtable.skipmap.len() == 0 {
            return;
        }
        println!("{}", self.memtable.skipmap.len());
        // todo: compaction
        let table_to_write = self.tables_per_level.unwrap()[0];
        let filename = format!("uncommitted{}", table_to_write.to_string());
        if !self.path.join("0").exists() {
            fs::create_dir(self.path.join("0")).unwrap();
        }
        // we better not be overwriting something
        // otherwise we are shooting ourselves in the foot
        assert!(!self.path.join("0").join(&filename).exists());
        let mut table = File::create(self.path.join("0").join(&filename)).unwrap();

        let mut index: Vec<u8> = Vec::new();
        let mut data_section: Vec<u8> = Vec::new();
        let mut current_block: Vec<u8> = Vec::new();
        let mut keys_visited = 0;
        for entry in &self.memtable.skipmap {
            let key = entry.key();
            let value = entry.value();
            keys_visited += 1;
            if current_block.is_empty() {
                index.write_u64::<LittleEndian>(key.len() as u64).unwrap();
                index.extend(key);
                index.write_u64::<LittleEndian>(data_section.len() as u64).unwrap();
            }
            current_block.write_u64::<LittleEndian>(key.len() as u64).unwrap();
            current_block.extend(key);
            current_block.write_u64::<LittleEndian>(value.len() as u64).unwrap();
            current_block.extend(value);
            if current_block.len() > 4_000 || keys_visited == self.memtable.skipmap.len() {
                data_section.write_u64::<LittleEndian>(current_block.len() as u64).unwrap();
                data_section.append(&mut current_block);
            }
        }
        table.write_u64::<LittleEndian>(index.len() as u64).unwrap();
        table.write_all(&index).unwrap();
        table.write_all(&data_section).unwrap();
        let old_path = self.path.join("0").join(&filename);
        let new_path = self.path.join("0").join(table_to_write.to_string());
        // before we commit the table, update header
        println!("before tables {}", self.tables_per_level.unwrap()[0]);
        self.tables_per_level.as_mut().unwrap()[0] += 1;
        if self.num_levels.unwrap() == 0 {
            self.num_levels = Some(1);
        }
        println!("new tables {}", self.tables_per_level.unwrap()[0]);
        fs::write(self.path.join("header"), self.tables_per_level.unwrap()).unwrap();
        std::fs::rename(old_path, new_path).unwrap();
        table.sync_all().unwrap();
        self.memtable.skipmap = SkipMap::new();
    }

    fn append_to_wal(&mut self, entry: WALEntry) {
        entry.serialize(&mut (self.wal_file.as_mut().unwrap())).unwrap();
        // wal metadata should not change, so sync_data is fine to use, instead of sync_all/fsync
        self.wal_file.as_mut().unwrap().sync_data().unwrap();
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

        self.write_skipmap_as_sstable();
        // wal file persisted, truncate
        self.wal_file.as_mut().unwrap().set_len(0).unwrap();
        
        self.wal_file.as_mut().unwrap().sync_all().unwrap();


        

        Ok(())
    }

    fn add_walentry(&mut self, operation: Operation, key: &Vec<u8>, value: &Vec<u8>) {
        let old_size = if self.memtable.skipmap.contains_key(key) { key.len() + self.memtable.skipmap.get(key).unwrap().value().len()} else { 0};
        let new_size = key.len() + value.len();
        // split these up, as adding the difference causes overflow on deletes
        self.memtable.size += new_size;
        self.memtable.size -= old_size;
        // TODO: too many copies- this may need to be optimized
        if operation == Operation::PUT {
            self.memtable.skipmap.insert(key.to_vec(), value.to_vec());
        } else if operation == Operation::DELETE {
            self.memtable.skipmap.insert(key.to_vec(), Vec::new());
        }

        self.append_to_wal(WALEntry { operation, key: key.to_vec(), value: value.to_vec() });
        // TODO: no magic values
        // add config setting
        if self.memtable.size > 1_000_000 {
            self.write_skipmap_as_sstable();
            self.memtable.skipmap = SkipMap::new();
            // wal persisted, truncate now
            self.wal_file.as_mut().unwrap().set_len(0).unwrap();
            self.wal_file.as_mut().unwrap().sync_all().unwrap();
        }
    }

    pub fn put(&mut self, key: &Vec<u8>, value: &Vec<u8>) {
        assert!(key.len() != 0);
        // empty length value is tombstone
        assert!(value.len() != 0);
        self.add_walentry(Operation::PUT, key, value);
    }

    pub fn delete(&mut self, key: &Vec<u8>) {
        assert!(key.len() != 0);
        // empty value is tombstone
        self.add_walentry(Operation::DELETE, key, &Vec::new());
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
    let repeats = 219;
    for i in 1..repeats {
        // println!("{}", i);
        let key = i.to_string();
        let value = 0.to_string();
        tree.get(&(key.as_bytes().to_vec()));
        tree.put(&(key.as_bytes().to_vec()), &(value.as_bytes().to_vec()));
        tree.delete(&(key.as_bytes().to_vec()));
    }
    
    Ok(())
}

/* Things to test:
persistence happens through one of three ways: memtable flush, wal restoration, compaction
should test g/p/d in each of those scenarios
should test key being in middle of datablock
should test binary search function
key found in nonfirst datablock - bug

empty k/v
super long k/v

test compaction?

out of space
test recovery - there are so many places this can fail (HOW CAN I MAKE THIS FAIL? I DON'T WANT TO MOCK 1000 FS calls)
^^ don't, just propogate to client code. rust analyzer will make sure no result goes unhandled
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