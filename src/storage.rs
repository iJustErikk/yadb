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

use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};
use std::io::{self, Read, Write};
use std::convert::TryFrom;

extern crate crossbeam_skiplist;
use crossbeam_skiplist::SkipMap;

// on tuning bloom filters:
// given a target false positive probability, we can calculate how many hash functions/bits per key we need
// cockroach's pebble uses 10, and they say that yields a 1% false positive rate
// that sounds like a reasonable default
// ^^ this can be the size of the skipmap or easily be calculated during compaction
// i also would like to avoid rehashing during compaction


// metrics:
// - wal replays
// - wal replay total bytes
// - # sstables
// - disk usage
// - avg sstable size

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
    num_levels: usize,
    tables_per_level: Option<[u8; 5]>,
    path: PathBuf
    memtable: Memtable
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
enum Operation {
    GET = 0, PUT = 1, DELETE = 2, INVALID = 3 
}

impl TryFrom<u8> for Operation {
    type Error = ();

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            0 => Ok(Operation::GET),
            1 => Ok(Operation::PUT),
            2 => OK(Operation::DELETE),
            _ => Err(()),
        }
    }
}

struct DataBlock {
    keys: Vec<Vec<u8>>,
    values: Vec<Vec<u8>>
}

struct WALEntry {
    operation: Operation,
    key: Vec<u8>,
    value: Vec<u8>,
}

impl DataBlock {
    // TODO: write_all and read_exact do not respect the host endianness
    pub fn deserialize<R: Read>(reader: &mut R) -> io::Result<Self> {
        let block_size = reader.read_u8()?;
        let mut block = vec![0; block_size];
        reader.read_exact(&mut block)?;
        let cursor = Cursor::new(block);
        let keys = Vec::new();
        let values = Vec::new();
        while cursor.position() != cursor.get_ref().len() as u64 {
            let key_size = cursor.read_u8()?;
            // TODO: does vec! expect all arguments to be compile time?
            let mut key = vec![0; key_size];
            reader.read_exact(&mut key)?;


            let value_size = cursor.read_u8()?;
            let mut key = vec![0; value_size];
            reader.read_exact(&mut key)?;
            keys.append(key)
            values.append(value)
        }

        return DataBlock{keys, values};
    }
}

impl WALEntry {
    // TODO: read_exact does not respect the host endianness
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
        if newline[0] != b'\n' {
            return Err(InvalidDatabaseStateError::CorruptWalEntry);
        }
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
    skipmap: SkipMap<String, Vec<u8>>
    size: u64
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
        return Tree {tables_per_level: None, path, Memtable{skipmap: SkipMap::new(), size: 0}}
    }
    pub fn init(&mut self) -> Result<(), Box<dyn Error>> {
        self.init_folder()?;
        self.cleanup_uncommitted()?;
        self.general_sanity_check()?;
        self.restore_wal();
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
            } else if entry.file_type()?.is_file() && (entry.file_name() != "header" || entry.file_name() != "wal") {
                return Err(InvalidDatabaseStateError::UnexpectedUserFile{filepath: entry.path().into_os_string().into_string().unwrap()})?;
            }
        }
        Ok(())
    }
    fn search_table(level: usize, table: u8, key) {
        // not checking bloom filter yet
        let table = File::open(self.path.clone().join(level).join(table))?;
        let index = Index::deserialize(table)?;
        let block_num = search_index(index, key);
        advance_file_pointer(block_num);
        let block = Block::deserialize(table);
        for key in block.keys {
            if key == key {
                return value
            }
        }
        return None
    }
    pub fn get(&self, key: Vec<u8>) -> Option<&Vec<u8>> {
        if let Some(value) = self.memtable.skipmap.get(&key) {
            return Some(value.value());
        }
        for level in 0..self.num_levels {
            for sstable in (0..self.tables_per_level[level]).rev() {
                // If your search_table() function returns Option<Vec<u8>>, you can return it directly.
                // You need to implement the function search_table()
                return self.search_table(level, sstable);
            }
        }
        return None;
    }
    // convert skipmap to sstable
    // start calling compaction
    fn write_skipmap_as_sstable(level, table_to_write, skipmap) {
        let mut file = File::create(self.path.clone().join(level).join("uncommited" + table_to_write))?;
        // when compacting: how can we avoid 2 passes?
        // we cannot load everything into memory
        // but we do not know the exact size of the index, so how could we write index/data blocks at same time?
        // need to write them at same time, as we cannot write one before the other
        // could estimate that the new index will be the size of both added together

        // index is key size | key | u64 offset for block <- merely the bytes written before that block, into the data section
        let index = Vec::new();
        let data_section = Vec::new();
        let current_block = Vec::new();
        let keys_visited = 0;
        for (key, value) in &skipmap {
            keys_visited++;
            // first item in block demarcates block
            if (current_block.size == 0) {
                index.append(key.size)
                index.extend(key)
                // offset for block is # bytes before block
                index.append(data_section.size)
            }
            current_block.append(key.size)
            current_block.extend(key)
            current_block.append(value.size)
            current_block.extend(value)
            // we need to flush block if its full or if its the last block
            // TODO: magic value
            if current_block.size > 4_000 || keys_visited == skipmap.size {
                data_section.append(current_block.size)
                data_section.extend(current_block)
            }

        }
        file.write(index.size)
        file.write_all(index)
        file.write_all(data_section)
        fs::rename(file, table_to_write);
        file.sync_data();
    }

    fn append_to_wal(operation) {
        operation.serialize(&mut self.wal_file);
        // wal metadata should not change, so sync_data is fine to use, instead of sync_all/fsync
        self.wal_file.sync_data();
    }

    fn restore_wal(&mut self) -> Result<(), Box<dyn Error>> {
        let mut operations = Vec::new();
        loop {
            match WALEntry::deserialize(&mut self.wal_file) {
                Ok(operation) => operations.push(operation),
                Err(e) => {
                    // when the write fails short
                    if e.kind() == io::ErrorKind::UnexpectedEof {
                        break;
                    } else {
                        return Err(Box::new(e));
                    }
                }
            }
        }

        let mut skipmap = SkipMap::new();

        for operation in operations {
            match operation.operation {
                Operation::PUT => { skipmap[operation.key] = operation.value }
                Operation::DELETE => { skipmap.remove(operation.key); }
                _ => {}
            }
        }

        write_skipmap_as_sstable(skipmap)


        fs::remove_file(self.path.clone().join("wal"))?;

        File::create(self.path.clone().join("wal"))?;

        Ok(())
    }

    fn add_operation(operation) {
        let old_size = self.memtable.skipmap.contains_key(key) ? (key.size + self.memtable.skipmap[key].size) : 0;
        let new_size = key.size + value.size;
        self.size += (new_size - old_size);
        if (operation == Operation::PUT) {
            self.memtable.skipmap[key] = value;
        } else if (operation == Operation::DELETE) {
            self.memtable.remove(key);
        }
        append_to_wal(operation);
        // TODO: no magic values
        // add config setting
        if self.size > 1_000_000 {
            write_skipmap_as_sstable(self.skipmap);
            self.skipmap = SkipMap::New();
        }
    }

    pub fn put(&self, key: Vec<u8>, value: Vec<u8>) {
        add_operation(WALEntry{operation: Operation::PUT, key, value});
    }

    pub fn delete(&self, key: Vec<u8>) {
        add_operation(WALEntry{operation: Operation::DELETE, key, value});
    }
}

// compression:
// find a good compression algorithm

// concurrency:
// going to add concurrency support soon
// need to figure out sync for in memory and disk ds
// could compact in another thread
// batch io

fn main() {}