// lsm based storage engine
// level compaction whenever level hits 10 sstables
use std::fs::{self, File};
use std::io::{Read, Write};
use std::sync::Arc;

use futures::future::BoxFuture;
use tokio::sync::RwLock;
use tokio::task::JoinHandle;

use tokio::fs::OpenOptions as TokioOpenOptions;
use std::io::{self};
use std::path::PathBuf;

use crossbeam_skiplist::SkipMap;

mod memtable;
use memtable::Memtable;

mod wal;
use wal::{Operation, WALEntry, WALFile};

mod errors;
pub use errors::YAStorageError;

// sstable- should I rename table?
mod table;
use table::{search_index, MergedTableIterators, Table, Index, DataBlock};
pub use memtable::MAX_MEMTABLE_SIZE;

mod async_cache;
use async_cache::AsyncCache;

// look into: no copy network -> fs, I believe kafka does something like this
// TODO: go through with checklist to make sure the database can fail at any point during wal replay, compaction or memtable flush

// assumption: user only runs one instance for a given folder
// I'd imagine things would get corrupted quickly
// maybe have a sanity check and some mechanism like a lock/lease?
// when testing: mark process so it can be easily killed prior to startup

// Magic vars (figure out how to make these configurable)
// TODO: exposing these for integration test- should allow user to specify this in code
pub const TABLES_UNTIL_COMPACTION: u8 = 3;
struct TreeState {
    num_levels: Option<u8>,
    tables_per_level: Option<[u8; 5]>,
    path: PathBuf,
    cache: LSMCache
}
struct LSMCache {
    // filter_cache
    index_cache: AsyncCache<Index, Table, (), YAStorageError>,
    block_cache: AsyncCache<DataBlock, Table, u64, YAStorageError>
}
pub const MAX_INDEX_CACHE_BYTES: usize = 5_000_000;
pub const MAX_BLOCK_CACHE_BYTES: usize = 5_000_000;
impl LSMCache { 
    // making this work with async now rather than later
    fn get_index(mut table: Table, _: ()) -> BoxFuture<'static, Result<(Index, Table), YAStorageError>> {
        return Box::pin(async move {Ok((table.get_index()?, table))});
    }

    fn get_block(mut table: Table, offset: u64) -> BoxFuture<'static, Result<(DataBlock, Table), YAStorageError>> {
        return Box::pin(async move {Ok((table.get_datablock(offset)?, table))});
    }

    fn new() -> Self {
        return LSMCache {
            // filter_cache
            index_cache: AsyncCache::new(MAX_INDEX_CACHE_BYTES, Box::new(Self::get_index)),
            block_cache: AsyncCache::new(MAX_INDEX_CACHE_BYTES, Box::new(Self::get_block))
        };
    }
}
struct MemtableWal {
    memtable: Memtable,
    wal: WALFile
}
pub struct Tree {
    // let's start with 5 levels
    // this is a lot of space 111110 mb ~ 108.5 gb
    ts: Arc<RwLock<TreeState>>,
    mw: Arc<RwLock<MemtableWal>>
}



impl Tree {
    pub fn new(path: &str) -> Self {
        return Tree {
            ts: Arc::new(RwLock::new(TreeState {
                num_levels: None,
                tables_per_level: None,
                path: PathBuf::from(path),
                cache: LSMCache::new()
            })),
            mw: Arc::new(RwLock::new(MemtableWal {
                memtable: Memtable {
                    skipmap: SkipMap::new(),
                    size: 0,
                },
                wal: WALFile::new()
            })),
        };
    }

    pub async fn init(&mut self) -> Result<(), YAStorageError> {
        // get writer lock here (not concurrent, but required by rust)
        // uncontended and only on startup
        // required by rust- note that any "drilling" is due to RwLock actually owning the state
        // not sure of the best way to get around this
        
        let just_init = self.init_folder().await?;
        let mut ts = self.ts.write().await;
        let mut mw = self.mw.write().await;
        self.general_sanity_check(&mut ts)?;
        if !just_init {
            self.restore_wal(&mut ts, &mut mw).await?;
        }
        mw.wal.start_writing(TokioOpenOptions::new()
        .write(true)
        .open(ts.path.clone().join("wal"))
        .await?);
        Ok(())
    }
    // TODO: is this function doing too much or is the name not good enough?
    // returns true iff first initialization
    async fn init_folder(&mut self) -> Result<bool, YAStorageError> {
        // acquire lock here so we can preserve init_folder tests
        // this is better than the alternative of stealing shared state just to pass it back in
        let mut ts = self.ts.write().await;
        // TODO: revisit error handling here
        if !ts.path.exists() {
            fs::create_dir_all(&ts.path)?;
        }
        if fs::read_dir(&ts.path)?.next().is_none() {
            let buffer = [0; 5];
            ts.tables_per_level = Some(buffer);

            // create is fine, no file to truncate
            let mut header = File::create(ts.path.clone().join("header"))?;
            header.write_all(&buffer)?;

            TokioOpenOptions::new()
                .write(true)
                .create(true)
                .open(ts.path.clone().join("wal"))
                .await?;
            return Ok(true);
        } else {
            let mut buffer = [0; 5];
            // TODO: if this fails, alert the user with YAStorageError, missing header
            let mut file = File::open(ts.path.clone().join("header"))?;
            // TODO: if this fails, alert the user with YAStorageError, corrupted header
            file.read_exact(&mut buffer)?;
            ts.tables_per_level = Some(buffer);
            if !ts.path.join("wal").exists() {
                return Err(YAStorageError::MissingWAL);
            }
            return Ok(false);
        }
    }

    fn general_sanity_check(&self, ts: &mut TreeState) -> Result<(), YAStorageError> {
        // TODO: header file will be replaced with WAL (which will probably be garbage collected into header)
        // at very least, the header file format will be updated
        // TODO: right now this compares FS state with header
        // soon we'll have a manifest file that will reflect potentially uncommitted fs updates
        // any folder should be 0 - 4
        // any file in those folders should be numeric
        // # of sstables should match header
        // sstable names should be increasing (0 - # - 1)
        // header should agree with reality
        let mut num_levels: u8 = 5;
        while num_levels != 0 && ts.tables_per_level.unwrap()[(num_levels - 1) as usize] == 0 {
            num_levels -= 1;
        }
        // stuff is getting initialized in too many places...is there a better way to do this?
        ts.num_levels = Some(num_levels);
        for entry_result in fs::read_dir(&ts.path)? {
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
                    (0..ts.tables_per_level.unwrap()[level as usize]).collect();
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


    // PRECONDITION: wal_file exists, this is only ever run after first startup
    async fn restore_wal(&self, ts: &mut TreeState, mw: &mut MemtableWal) -> Result<(), YAStorageError> {
        let mut wal_file =
            TokioOpenOptions::new()
                .write(true)
                .read(true)
                .open(ts.path.clone().join("wal"))
                .await?;
        let skipmap = mw.wal.get_wal_as_skipmap(&mut wal_file).await?;

        if skipmap.len() == 0 {
            return Ok(())
        }

        Self::write_skipmap_as_sstable(skipmap, ts).await?;
        wal_file.set_len(0).await?;
        wal_file.sync_all().await?;

        Ok(())
    }

    async fn search_table(
        level: u8,
        table_num: u8,
        key: &Vec<u8>,
        ts: &TreeState
    ) -> Result<Option<Vec<u8>>, YAStorageError> {
        let table: Table = Table::new(
            ts.path
                .clone()
                .join(level.to_string())
                .join(table_num.to_string()),
            true,
        )?;
        // TODO: readd filters, cache them, MAKE SURE TO INVALIDATE
        // let filter = table.get_filter()?;
        // if !filter.contains(key) {
        //     return Ok(None);
        // }
        let (index, table) = ts.cache.index_cache.get(table, level.to_string() + "/" + &table_num.to_string(), ()).await?;
        let byte_offset = search_index(&index, key);
        if byte_offset.is_none() {
            return Ok(None);
        }
        let byte_offset = byte_offset.unwrap();
        let (block, _) = ts.cache.block_cache.get(table, level.to_string() + "/" + &table_num.to_string() + "/" + &byte_offset.to_string(), byte_offset).await?;

        let maybe_index = block.entries.binary_search_by(|(cantidate_key, _)| cantidate_key.cmp(key));
        if maybe_index.is_ok() {
            return Ok(Some(block.entries[maybe_index.unwrap()].1.clone()));
        }
        return Ok(None);
    }
    // TODO: these should take in slices, not vector refs
    pub fn get(&self, key: &Vec<u8>) -> JoinHandle<Result<Option<Vec<u8>>, YAStorageError>> {
        assert!(key.len() != 0);
        let ts_clone = Arc::clone(&self.ts);
        let mw_clone = Arc::clone(&self.mw);
        // TODO: figure out clone
        let key = key.clone();
        tokio::spawn(async move {
            let ts = ts_clone.read().await;
            let mw = mw_clone.read().await;
            if let Some(value) = mw.memtable.skipmap.get(&key) {
                let res = value.value().to_vec();
                if res.len() == 0 {
                    return Ok(None);
                }
                return Ok(Some(res));
            }
            for level in 0..ts.num_levels.unwrap() {
                for sstable in (0..ts.tables_per_level.unwrap()[level as usize]).rev() {
                    // TODO: if value vector is empty, this is a tombstone
                    if let Some(res) = Self::search_table(level, sstable, &key, &ts).await? {
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
        })
        
    }

    async fn write_skipmap_as_sstable(skipmap: SkipMap<Vec<u8>, Vec<u8>>, ts: &mut TreeState) -> Result<(), YAStorageError> {
        let table_to_write = ts.tables_per_level.unwrap()[0];
        if !ts.path.join("0").exists() {
            fs::create_dir(ts.path.join("0"))?;
        }
        let filename = ts.tables_per_level.unwrap()[0];

        // we better not be overwriting something
        // otherwise we are shooting ourselves in the foot
        assert!(!ts.path.join("0").join(filename.to_string()).exists());

        let new_path = ts.path.join("0").join(table_to_write.to_string());
        let mut table = Table::new(new_path, false)?;
        let skipmap_len = skipmap.len();
        table.write_table(
            skipmap.into_iter(),
            skipmap_len,
        )?;

        ts.tables_per_level.as_mut().unwrap()[0] += 1;
        if ts.num_levels.unwrap() == 0 {
            ts.num_levels = Some(1);
        }
        Self::commit_header(ts)?;
        Self::compact(ts).await?;

        Ok(())
    }
    fn commit_header(ts: &mut TreeState) -> io::Result<()> {
        let mut header = File::create(ts.path.join("header"))?;
        header.write_all(&ts.tables_per_level.unwrap())?;
        header.sync_all()?;
        Ok(())
    }

    async fn clear_cache_for_level(ts: &mut TreeState, level: u8) -> Result<(), YAStorageError> {
        for table_num in 0..TABLES_UNTIL_COMPACTION {
            // we incur 10 read ios here- we can eliminate this if we have a more robust in-memory reprsentation of on-disk state
            let num_blocks = Table::new(ts.path.join(level.to_string()).join(table_num.to_string()).clone(), true)?.get_num_blocks()?;
            ts.cache.index_cache.reset_keys([level.to_string() + "/" + &table_num.to_string()].to_vec()).await;
            let mut to_delete = Vec::new();
            for block in 0..num_blocks {
                to_delete.push(level.to_string() + "/" + &table_num.to_string() + "/" + &block.to_string());
            }
            ts.cache.block_cache.reset_keys(to_delete).await;
        }
        Ok(())
    }

    async fn compact_level(level: u8, ts: &mut TreeState) -> Result<(), YAStorageError> {
        assert!(level != 4);
        // do this before writes/deletes so we can still read indices
        Self::clear_cache_for_level(ts, level).await?;
        let new_table = ts.tables_per_level.unwrap()[(level + 1) as usize];
        let new_table_path = ts.path
            .join((level + 1).to_string())
            .join(new_table.to_string());
        let mut table = Table::new(new_table_path, false)?;
        let mut tables = (0..TABLES_UNTIL_COMPACTION)
            .map(|x| Table::new(ts.path.join(level.to_string()).join(x.to_string()), true))
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
        ts.tables_per_level.as_mut().unwrap()[level as usize] = 0;
        if ts.tables_per_level.unwrap()[(level + 1) as usize] == 0 {
            ts.num_levels = Some(ts.num_levels.unwrap() + 1);
        }

        // TODO: unwrapping makes a copy if you do not do as_mut
        // investigate if there are any other bugs causes by me not doing this
        // is there a good way to avoid this?
        ts.tables_per_level.as_mut().unwrap()[(level + 1) as usize] += 1;
        fs::remove_dir_all(ts.path.join(level.to_string()))?;

        Self::commit_header(ts)?;


        Ok(())
    }
    async fn compact(ts: &mut TreeState) -> Result<(), YAStorageError> {
        let mut level: u8 = 0;
        // this may fail between compactions, so we need to check if we need to compact on startup
        while ts.tables_per_level.unwrap()[level as usize] == TABLES_UNTIL_COMPACTION {
            Self::compact_level(level, ts).await?;
            level += 1;
        }
        Ok(())
    }


    async fn add_walentry(
        operation: Operation,
        key: &Vec<u8>,
        value: &Vec<u8>,
        ts: Arc<RwLock<TreeState>>, 
        mw: Arc<RwLock<MemtableWal>>
    ) -> Result<(), YAStorageError> {
        // TODO: update insert_or_delete when more operations are supported
        assert!(operation == Operation::PUT  || operation == Operation::DELETE);

        let mut ts = ts.write().await;
        let mut mw = mw.write().await;

        mw.memtable.insert_or_delete(key, value, operation == Operation::PUT);

        if mw.memtable.needs_flush() {
            let skipmap = mw.memtable.get_skipmap();
            Self::write_skipmap_as_sstable(skipmap, &mut ts).await?;
            mw.memtable.reset();
            mw.wal.reset().await?;
            return Ok(());
        } else {
            let fut = mw.wal.append_to_wal(WALEntry {
                operation,
                key: key.to_vec(),
                value: value.to_vec(),
            });
            // we no longer care about the state, just that the write has gone through
            // there is more analysis in batch_writer
            // release the lock to allow for concurrency
            std::mem::drop(mw);
            std::mem::drop(ts);
            fut.await.unwrap();
        }

        
        Ok(())
    }

    pub fn put(&mut self, key: &Vec<u8>, value: &Vec<u8>) -> JoinHandle<Result<(), YAStorageError>> {
        assert!(key.len() != 0);
        // empty length value is tombstone
        assert!(value.len() != 0);
        return self.start_write(key, value, Operation::PUT);
    }

    pub fn delete(&mut self, key: &Vec<u8>) -> JoinHandle<Result<(), YAStorageError>> {
        assert!(key.len() != 0);
        return self.start_write(key, &Vec::new(), Operation::DELETE);
    }

    pub fn start_write(&mut self, key: &Vec<u8>, value: &Vec<u8>, op: Operation) -> JoinHandle<Result<(), YAStorageError>>  {

        let key = key.clone();
        let value = value.clone();
        let ts = Arc::clone(&self.ts);
        let mw = Arc::clone(&self.mw);
        tokio::spawn(async move {
            // empty value is tombstone
            Self::add_walentry(op, &key, &value, ts, mw).await?;
            Ok(())
        })
    }
}


#[cfg(test)]
mod init_tests {
    use fs::remove_file;
    use tempfile::tempdir;
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
