// lsm based storage engine
// level compaction whenever level hits 10 sstables
use std::fs::{self, File};
use std::io::{Read, Write};
use std::mem;

extern crate tokio;
use self::tokio::fs::File as TokioFile;
use self::tokio::fs::OpenOptions as TokioOpenOptions;
use std::io::{self};
use std::path::PathBuf;

extern crate crossbeam_skiplist;
use self::crossbeam_skiplist::SkipMap;
extern crate cuckoofilter;
extern crate fastmurmur3;
extern crate tempfile;

mod memtable;
use self::memtable::Memtable;

mod wal;
use self::wal::{Operation, WALEntry};

mod errors;
use self::errors::YAStorageError;

// sstable- should I rename table?
mod table;
use self::table::{search_index, MergedTableIterators, Table};



// look into: no copy network -> fs, I believe kafka does something like this
// TODO: go through with checklist to make sure the database can fail at any point during wal replay, compaction or memtable flush

// assumption: user only runs one instance for a given folder
// I'd imagine things would get corrupted quickly
// maybe have a sanity check and some mechanism like a lock/lease?
// when testing: mark process so it can be easily killed prior to startup

// Magic vars (figure out how to make these configurable)
// TODO: exposing these for integration test- should allow user to specify this in code
pub const TABLES_UNTIL_COMPACTION: u8 = 3;
pub const MAX_MEMTABLE_SIZE: usize = 1_000_000;
// TODO: why does integration test make me have this
pub fn main() {}
pub struct Tree {
    // let's start with 5 levels
    // this is a lot of space 111110 mb ~ 108.5 gb
    num_levels: Option<usize>,
    tables_per_level: Option<[u8; 5]>,
    path: PathBuf,
    memtable: Memtable,
    wal_file: Option<TokioFile>,
}

impl Tree {
    pub fn new(path: &str) -> Self {
        return Tree {
            num_levels: None,
            wal_file: None,
            tables_per_level: None,
            path: PathBuf::from(path),
            memtable: Memtable {
                skipmap: SkipMap::new(),
                size: 0,
            },
        };
    }

    pub async fn init(&mut self) -> Result<(), YAStorageError> {
        self.init_folder().await?;
        self.general_sanity_check()?;
        self.restore_wal().await?;
        Ok(())
    }
    async fn init_folder(&mut self) -> Result<(), YAStorageError> {
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
            self.wal_file = Some(
                TokioOpenOptions::new()
                    .read(true)
                    .write(true)
                    .append(true)
                    .create(true)
                    .open(self.path.clone().join("wal"))
                    .await?,
            );
        } else {
            let mut buffer = [0; 5];
            // TODO: if this fails, alert the user with YAStorageError, missing header
            let mut file = File::open(self.path.clone().join("header"))?;
            // TODO: if this fails, alert the user with YAStorageError, corrupted header
            file.read_exact(&mut buffer)?;
            self.tables_per_level = Some(buffer);
            self.wal_file = Some(
                TokioOpenOptions::new()
                    .read(true)
                    .write(true)
                    .append(true)
                    .open(self.path.clone().join("wal"))
                    .await?,
            );
        }
        Ok(())
    }

    fn general_sanity_check(&mut self) -> Result<(), YAStorageError> {
        // any folder should be 0 - 4
        // any file in those folders should be numeric
        // # of sstables should match header
        // sstable names should be increasing (0 - # - 1)
        // header should agree with reality
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
                    Err(_) => Err(YAStorageError::UnexpectedUserFolder {
                        folderpath: entry.file_name().into_string().unwrap(),
                    }),
                }?;
                if level > 4 {
                    return Err(YAStorageError::LevelTooLarge { level })?;
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
                        return Err(YAStorageError::UnexpectedUserFolder {
                            folderpath: sub_entry.file_name().into_string().unwrap(),
                        })?;
                    }
                    let table_name =
                        match sub_entry.file_name().into_string().unwrap().parse::<u8>() {
                            Ok(value) => Ok(value),
                            Err(_) => Err(YAStorageError::UnexpectedUserFile {
                                filepath: sub_entry.file_name().into_string().unwrap(),
                            }),
                        }?;
                    actual.push(table_name);
                }
                actual.sort();
                // TODO: if database fails during compaction, wal replay or memtable flush
                // there may be too many sstables and we can just delete them
                // this must be equal to the range from 0 to # expected - 1
                // must revisit
                let expected: Vec<u8> =
                    (0..self.tables_per_level.unwrap()[level as usize]).collect();
                if actual != expected {
                    return Err(YAStorageError::SSTableMismatch { expected, actual })?;
                }
            } else if entry.file_type()?.is_file()
                && (entry.file_name() != "header" && entry.file_name() != "wal")
            {
                return Err(YAStorageError::UnexpectedUserFile {
                    filepath: entry.path().into_os_string().into_string().unwrap(),
                })?;
            }
        }

        Ok(())
    }

    fn search_table(
        &self,
        level: usize,
        table: u8,
        key: &Vec<u8>,
    ) -> Result<Option<Vec<u8>>, YAStorageError> {
        let mut table: Table = Table::new(
            self.path
                .clone()
                .join(level.to_string())
                .join(table.to_string()),
            true,
        )?;
        // TODO: filter is giving false negatives -> serde with filter is wrong or there is nondeterminism
        // let filter = table.get_filter()?;
        // if !filter.contains(key) {
        //     return Ok(None);
        // }
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
        return Ok(None);
    }
    // TODO: these should take in slices, not vector refs
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
                    // they might want to represent NULL- this should be a field for the user
                    if res.len() == 0 {
                        return Ok(None);
                    }
                    return Ok(Some(res));
                }
            }
        }
        return Ok(None);
    }

    fn write_skipmap_as_sstable(&mut self) -> io::Result<()> {
        let table_to_write = self.tables_per_level.unwrap()[0];
        if !self.path.join("0").exists() {
            fs::create_dir(self.path.join("0"))?;
        }
        let filename = self.tables_per_level.unwrap()[0];
        // we better not be overwriting something
        // otherwise we are shooting ourselves in the foot
        assert!(!self.path.join("0").join(filename.to_string()).exists());

        // TODO: begin write by creating table, then passing iterator for writing
        let new_path = self.path.join("0").join(table_to_write.to_string());
        let mut table = Table::new(new_path, false)?;
        let skipmap_len = self.memtable.skipmap.len();
        table.write_table(
            mem::replace(&mut self.memtable.skipmap, SkipMap::new()).into_iter(),
            skipmap_len,
        )?;
        self.memtable.size = 0;

        self.tables_per_level.as_mut().unwrap()[0] += 1;
        if self.num_levels.unwrap() == 0 {
            self.num_levels = Some(1);
        }
        self.commit_header()?;
        self.memtable.skipmap = SkipMap::new();

        if self.tables_per_level.unwrap()[0] == TABLES_UNTIL_COMPACTION {
            self.compact()?;
        }

        Ok(())
    }
    fn commit_header(&mut self) -> io::Result<()> {
        let mut header = File::create(self.path.join("header"))?;
        header.write_all(&self.tables_per_level.unwrap())?;
        header.sync_all()?;
        Ok(())
    }

    fn compact_level(&mut self, level: usize) -> io::Result<()> {
        assert!(level != 4);
        let new_table = self.tables_per_level.unwrap()[level + 1];
        let new_table_path = self
            .path
            .join((level + 1).to_string())
            .join(new_table.to_string());
        let mut table = Table::new(new_table_path, false)?;
        let mut tables = (0..TABLES_UNTIL_COMPACTION)
            .map(|x| Table::new(self.path.join(level.to_string()).join(x.to_string()), true))
            .collect::<io::Result<Vec<Table>>>()?;
        // will at worst be a k tables * num keys overestimate
        // unique keys recalculated each compaction, so this will not worsen
        let num_keys_estimation = tables
            .iter_mut()
            .map(|x| x.get_num_unique_keys())
            .collect::<io::Result<Vec<u64>>>()?
            .into_iter()
            .reduce(|acc, x| acc + x)
            .unwrap();

        table.write_table(
            MergedTableIterators::new(tables)?,
            num_keys_estimation as usize,
        )?;
        // TODO: the commit tables scheme only works for writing single sstables
        // for compaction, there is a new failure point: between writing the table and deleting the old level
        // if this fails, on init, the database will recompact the old files and duplicate the data
        // this does not impact correctness (latest table would be read first), but space performance and write performance (compaction happens sooner)
        self.tables_per_level.as_mut().unwrap()[level] = 0;
        if self.tables_per_level.unwrap()[level + 1] == 0 {
            self.num_levels = Some(self.num_levels.unwrap() + 1);
        }

        // TODO: unwrapping makes a copy if you do not do as_mut
        // investigate if there are any other bugs causes by me not doing this
        // is there a good way to avoid this?
        self.tables_per_level.as_mut().unwrap()[level + 1] += 1;
        fs::remove_dir_all(self.path.join(level.to_string()))?;

        self.commit_header()?;

        Ok(())
    }
    fn compact(&mut self) -> io::Result<()> {
        let mut level = 0;
        // this may fail between compactions, so we need to check if we need to compact on startup
        while self.tables_per_level.unwrap()[level] == TABLES_UNTIL_COMPACTION {
            self.compact_level(level)?;
            level += 1;
        }
        Ok(())
    }

    async fn append_to_wal(&mut self, entry: WALEntry) -> io::Result<()> {
        entry
            .serialize(&mut (self.wal_file.as_mut().unwrap()))
            .await?;
        // wal metadata should not change, so sync_data is fine to use, instead of sync_all/fsync
        self.wal_file.as_mut().unwrap().sync_data().await?;
        Ok(())
    }

    async fn restore_wal(&mut self) -> io::Result<()> {
        let mut entries = Vec::new();
        assert!(self.path.join("wal").exists());
        loop {
            match WALEntry::deserialize(&mut (self.wal_file.as_mut().unwrap())).await {
                Ok(entry) => entries.push(entry),
                Err(e) => {
                    if e.kind() == io::ErrorKind::UnexpectedEof {
                        break;
                    } else {
                        return Err(e);
                    }
                }
            }
        }
        if entries.len() == 0 {
            return Ok(());
        }

        let skipmap = SkipMap::new();

        for entry in entries {
            match entry.operation {
                Operation::PUT => {
                    skipmap.insert(entry.key, entry.value);
                }
                // empty vector is tombstone
                Operation::DELETE => {
                    skipmap.insert(entry.key, Vec::new());
                }
                _ => {}
            }
        }

        self.memtable.skipmap = skipmap;

        self.write_skipmap_as_sstable()?;
        // wal file persisted, truncate
        self.wal_file.as_mut().unwrap().set_len(0).await?;

        self.wal_file.as_mut().unwrap().sync_all().await?;

        Ok(())
    }

    // TODO: both the append and fsync are too slow.
    async fn add_walentry(
        &mut self,
        operation: Operation,
        key: &Vec<u8>,
        value: &Vec<u8>,
    ) -> Result<(), YAStorageError> {
        let old_size = if self.memtable.skipmap.contains_key(key) {
            key.len() + self.memtable.skipmap.get(key).unwrap().value().len()
        } else {
            0
        };
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

        self.append_to_wal(WALEntry {
            operation,
            key: key.to_vec(),
            value: value.to_vec(),
        }).await?;
        // TODO: no magic values
        // add config setting

        if self.memtable.size > MAX_MEMTABLE_SIZE {
            self.write_skipmap_as_sstable()?;
            self.memtable.skipmap = SkipMap::new();
            // wal persisted, truncate now
            self.wal_file.as_mut().unwrap().set_len(0).await.unwrap();
            self.wal_file.as_mut().unwrap().sync_all().await.unwrap();
        }
        Ok(())
    }

    pub async fn put(&mut self, key: &Vec<u8>, value: &Vec<u8>) -> Result<(), YAStorageError> {
        assert!(key.len() != 0);
        // empty length value is tombstone
        assert!(value.len() != 0);
        self.add_walentry(Operation::PUT, key, value).await?;
        Ok(())
    }

    pub async fn delete(&mut self, key: &Vec<u8>) -> Result<(), YAStorageError> {
        assert!(key.len() != 0);
        // empty value is tombstone
        self.add_walentry(Operation::DELETE, key, &Vec::new()).await?;
        Ok(())
    }
}

// compression/data integrity: use gzip or allow user to choose compression algorithm

// unit tests
// (try to) test individual functions
// need to research mocking
// TODO: wrap any writing to header/wal/sstable so code/tests stay consistent
// TODO: version header/wal/sstable to provide backward compatibility
#[cfg(test)]
mod init_tests {
    use self::fs::remove_file;
    use self::tempfile::tempdir;
    use std::error::Error;
    use std::fs::{create_dir, remove_dir};
    use std::io::Read;

    use super::*;
    // TODO: these check for the existence of some error
    // however, they should check for the particular error that arises
    // will fix this when I fix the API. I should be returning custom errors to the user
    #[tokio::test]
    async fn test_init_create() -> Result<(), Box<dyn Error>> {
        let dir = tempdir()?;
        let mut tree = Tree::new(dir.path().as_os_str().to_str().unwrap());
        tree.init_folder().await.expect("Failed to init folder");
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
    #[tokio::test]
    async fn missing_header() -> Result<(), Box<dyn Error>> {
        // the following 3 lines are pretty repetitive
        // i'm going to opt out of using a helper function, as i am sure this will not change
        // and it will be roughly the same LOC
        let dir = tempdir()?;
        let mut tree = Tree::new(dir.path().as_os_str().to_str().unwrap());
        tree.init_folder().await.expect("Failed to init folder");
        remove_file(dir.path().join("header"))?;
        let mut tree = Tree::new(dir.path().as_os_str().to_str().unwrap());
        assert!(tree.init().await.is_err());
        Ok(())
    }
    #[tokio::test]
    async fn missing_wal() -> Result<(), Box<dyn Error>> {
        let dir = tempdir()?;
        let mut tree = Tree::new(dir.path().as_os_str().to_str().unwrap());
        tree.init_folder().await.expect("Failed to init folder");
        remove_file(dir.path().join("wal"))?;
        let mut tree = Tree::new(dir.path().as_os_str().to_str().unwrap());
        assert!(tree.init().await.is_err());
        Ok(())
    }
    #[tokio::test]
    async fn extraneous_file_root() -> Result<(), Box<dyn Error>> {
        let dir = tempdir()?;
        let mut tree = Tree::new(dir.path().as_os_str().to_str().unwrap());
        tree.init_folder().await.expect("Failed to init folder");
        File::create(dir.path().join("test"))?;
        let mut tree = Tree::new(dir.path().as_os_str().to_str().unwrap());
        assert!(tree.init().await.is_err());
        Ok(())
    }
    #[tokio::test]
    async fn extraneous_folder_root() -> Result<(), Box<dyn Error>> {
        let dir = tempdir()?;
        let mut tree = Tree::new(dir.path().as_os_str().to_str().unwrap());
        tree.init_folder().await.expect("Failed to init folder");
        create_dir(dir.path().join("test"))?;
        let mut tree = Tree::new(dir.path().as_os_str().to_str().unwrap());
        assert!(tree.init().await.is_err());
        remove_dir(dir.path().join("test"))?;
        create_dir(dir.path().join("5"))?;
        let mut tree = Tree::new(dir.path().as_os_str().to_str().unwrap());
        assert!(tree.init().await.is_err());
        Ok(())
    }
    #[tokio::test]
    async fn extraneous_file_nonroot() -> Result<(), Box<dyn Error>> {
        let dir = tempdir()?;
        let mut tree = Tree::new(dir.path().as_os_str().to_str().unwrap());
        tree.init_folder().await.expect("Failed to init folder");
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
        tree.init_folder().await.expect("Failed to init folder");
        Ok(())
    }
    #[tokio::test]
    async fn extraneous_folder_nonroot() -> Result<(), Box<dyn Error>> {
        let dir = tempdir()?;
        let mut tree = Tree::new(dir.path().as_os_str().to_str().unwrap());
        tree.init_folder().await.expect("Failed to init folder");

        let mut header_file = File::create(dir.path().join("header"))?;
        let new_header: [u8; 5] = [1, 0, 0, 0, 0];
        header_file.write_all(&new_header)?;
        create_dir(dir.path().join("0"))?;
        create_dir(dir.path().join("0").join("0"))?;
        let mut tree = Tree::new(dir.path().as_os_str().to_str().unwrap());
        tree.init_folder().await.expect("Failed to init folder");
        Ok(())
    }
    #[tokio::test]
    async fn non_contiguous_sstables() -> Result<(), Box<dyn Error>> {
        let dir = tempdir()?;
        let mut tree = Tree::new(dir.path().as_os_str().to_str().unwrap());
        tree.init_folder().await.expect("Failed to init folder");
        let mut header_file = File::create(dir.path().join("header"))?;
        let new_header: [u8; 5] = [2, 0, 0, 0, 0];
        header_file.write_all(&new_header)?;
        create_dir(dir.path().join("0"))?;
        File::create(dir.path().join("0").join("0"))?;
        File::create(dir.path().join("0").join("2"))?;
        let mut tree = Tree::new(dir.path().as_os_str().to_str().unwrap());
        tree.init_folder().await.expect("Failed to init folder");
        Ok(())
    }
    #[tokio::test]
    async fn no_zero_sstable() -> Result<(), Box<dyn Error>> {
        let dir = tempdir()?;
        let mut tree = Tree::new(dir.path().as_os_str().to_str().unwrap());
        tree.init_folder().await.expect("Failed to init folder");
        let mut header_file = File::create(dir.path().join("header"))?;
        let new_header: [u8; 5] = [1, 0, 0, 0, 0];
        header_file.write_all(&new_header)?;
        create_dir(dir.path().join("0"))?;
        File::create(dir.path().join("0").join("1"))?;
        let mut tree = Tree::new(dir.path().as_os_str().to_str().unwrap());
        tree.init_folder().await.expect("Failed to init folder");
        Ok(())
    }
    #[tokio::test]
    async fn sstables_header_mismatch() -> Result<(), Box<dyn Error>> {
        let dir = tempdir()?;
        let mut tree = Tree::new(dir.path().as_os_str().to_str().unwrap());
        tree.init_folder().await.expect("Failed to init folder");
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
        tree.init_folder().await.expect("Failed to init folder");
        Ok(())
    }
}
