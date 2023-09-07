use std::collections::HashSet;
// lsm based storage engine
// level compaction whenever level hits 10 sstables
use tokio::fs::{self, File};
use std::sync::Arc;

use futures::future::BoxFuture;
use tokio::io::AsyncWriteExt;
use tokio::sync::RwLock;
use tokio::task::JoinHandle;

use tokio::fs::OpenOptions as TokioOpenOptions;
use std::io::{self};
use std::path::PathBuf;

use uuid::{Uuid, Error as UuidError};

use skiplist::SkipMap;

mod memtable;
use memtable::Memtable;

mod wal;
use wal::{Operation, WALEntry, WALFile};

mod errors;
pub use errors::YAStorageError;

// sstable- should I rename table?
mod sstable;
use sstable::{search_index, MergedTableIterators, SSTable, Index, DataBlock};
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

// for implementing virtual representation:
// on flsuh/compaction create a hash, check for a file with that hash, recreate hash (make note that since there is only 1 writer, there is no race condition with the FS)
// add hash to level/name mapping
// check for orphaned files on startup
// test

pub const TABLES_UNTIL_COMPACTION: usize = 3;
struct Header {
    mapping: Vec<Vec<Uuid>>
}
impl TreeState {
    // TODO: unwrapping since 
    async fn deserialize_header(&self) -> Result<Header, YAStorageError> {
        // TODO: look at implementation of read_to_string. does it assume endianness
        let contents = fs::read_to_string(self.path.join("header")).await?;
        // string split yields empty string as first element for empty string input
        println!("{contents}");
        if contents.len() == 0 {
            return Ok(Header {
                mapping: Vec::new()
            });
        }
        let mapping: Result<Vec<Vec<Uuid>>, UuidError> = contents.split("\n").map(|x| x.split("#").map(|u| Uuid::parse_str(u)).collect()).collect();
        if mapping.is_err() {
            return Err(YAStorageError::CorruptedHeader);
        }
        // does not throw
        let mapping = mapping.unwrap();
        if mapping.len() > 4 {
            return Err(YAStorageError::LevelTooLarge { level: mapping.len() as u8 })?;
        }
        Ok(Header {
            mapping
        })
    }

    async fn serialize_header(&self) -> Result<(), YAStorageError> {
        let to_write = self.header.mapping.iter().map(|x| x.iter().map(|u| u.to_string()).collect::<Vec<String>>().join("-")).collect::<Vec<String>>().join("\n");
        let mut header = TokioOpenOptions::new()
                .write(true)
                .open(self.path.clone().join("header"))
                .await?;
        header.write_all(to_write.as_bytes()).await?;
        header.sync_all().await?;
        Ok(())
    }
}
struct TreeState {
    path: PathBuf,
    cache: LSMCache,
    header: Header
}
struct LSMCache {
    // filter_cache
    index_cache: AsyncCache<Index, SSTable, (), Option<u64>, YAStorageError>,
    block_cache: AsyncCache<DataBlock, SSTable, u64, Option<Vec<u8>>, YAStorageError>
}
pub const MAX_INDEX_CACHE_BYTES: usize = 5_000_000;
pub const MAX_BLOCK_CACHE_BYTES: usize = 5_000_000;
impl LSMCache { 
    // making this work with async now rather than later
    fn get_index(mut table: SSTable, _: ()) -> BoxFuture<'static, Result<(Index, SSTable), YAStorageError>> {
        return Box::pin(async move {Ok((table.get_index()?, table))});
    }

    fn get_block(mut table: SSTable, offset: u64) -> BoxFuture<'static, Result<(DataBlock, SSTable), YAStorageError>> {
        return Box::pin(async move {Ok((table.get_datablock(offset)?, table))});
    }

    fn index_callback(index: &Index, key: Vec<u8>) -> Option<u64> {
        return search_index(index, &key);
    }

    fn block_callback(block: &DataBlock, search_key: Vec<u8>) -> Option<Vec<u8>> {
        let maybe_index = block.entries.binary_search_by(|(cantidate_key, _)| cantidate_key.cmp(&search_key));
        if maybe_index.is_ok() {
            return Some(block.entries[maybe_index.unwrap()].1.clone());
        }
        return None;
    }

    fn new() -> Self {
        return LSMCache {
            // filter_cache
            index_cache: AsyncCache::new(MAX_INDEX_CACHE_BYTES, Box::new(Self::get_index), Box::new(Self::index_callback)),
            block_cache: AsyncCache::new(MAX_INDEX_CACHE_BYTES, Box::new(Self::get_block), Box::new(Self::block_callback))
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
                path: PathBuf::from(path),
                cache: LSMCache::new(),
                header: Header {mapping: Vec::new()}
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
        self.sanity_check_and_cleanup(&mut ts).await?;
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
        if !ts.path.exists() || fs::read_dir(&ts.path).await?.next_entry().await?.is_none() {
            fs::create_dir_all(&ts.path).await?;
            File::create(ts.path.clone().join("header")).await?;
            File::create(ts.path.clone().join("wal")).await?;
            return Ok(true);
        } else {
            if !ts.path.join("header").exists() {
                return Err(YAStorageError::MissingHeader);
            }
            File::open(ts.path.clone().join("header")).await?;
            ts.header = ts.deserialize_header().await?;
            if !ts.path.join("wal").exists() {
                return Err(YAStorageError::MissingWAL);
            }
            return Ok(false);
        }
    }

    async fn sanity_check_and_cleanup(&self, ts: &mut TreeState) -> Result<(), YAStorageError> {
        // only files should be WAL, header and hashes
        // flatmap header to get set of expected hashes
        // delete anything not in that set and store everything found in another
        // if those aren't equal, throw sstablemismatch
        // if user or outside program puts non hash file or hash file or some folder, we'll just silently delete that
        // if flushing/compaction fail, then there will be an unreflected table. we aren't using a WAL/single block write intent file- so we could not recover progress.
        let mut expected_filenames: HashSet<String> = ts.header.mapping.iter().flat_map(|x| x.iter().map(|x| x.to_string()).collect::<Vec<String>>()).collect(); // functional 
        expected_filenames.insert(String::from("wal"));
        expected_filenames.insert(String::from("header"));
        let mut found: HashSet<String> = HashSet::new();
        let mut readdir = fs::read_dir(&ts.path).await?;
        while let Some(entry) = readdir.next_entry().await? {
            if entry.file_type().await?.is_dir() {
                fs::remove_dir_all(entry.path()).await?;
            } else {
                if expected_filenames.contains(entry.file_name().to_str().unwrap()) {
                    found.insert(entry.file_name().to_str().unwrap().to_string());
                } else {
                    tokio::fs::remove_file(entry.path()).await?;
                }
            }
        }
        if found != expected_filenames {
            return Err(YAStorageError::FSInitMismatch { expected: expected_filenames, actual: found })
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
        filename: &Uuid,
        key: &Vec<u8>,
        ts: &TreeState
    ) -> Result<Option<Vec<u8>>, YAStorageError> {
        let mut table: SSTable = SSTable::new(
            ts.path
                .clone()
                .join(filename.to_string()),
            true,
        )?;
        // TODO: readd filters, cache them, MAKE SURE TO INVALIDATE
        let filter = table.get_filter()?;
        if !filter.contains(key) {
            return Ok(None);
        }
        let (byte_offset, table) = ts.cache.index_cache.get(table, filename.to_string(), (), key.clone()).await?;
        if byte_offset.is_none() {
            return Ok(None);
        }
        let byte_offset = byte_offset.unwrap();
        let (in_block, _) = ts.cache.block_cache.get(table, filename.to_string() + "/" + &byte_offset.to_string(), byte_offset, key.clone()).await?;

        return Ok(in_block);
    }

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
                let res = value.to_vec();
                if res.len() == 0 {
                    return Ok(None);
                }
                return Ok(Some(res));
            }
            for level in 0..ts.header.mapping.len(){
                for filename in ts.header.mapping[level as usize].iter().rev() {
                    // TODO: if value vector is empty, this is a tombstone
                    if let Some(res) = Self::search_table(filename, &key, &ts).await? {
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
        if !ts.path.join("0").exists() {
            fs::create_dir(ts.path.join("0")).await?;
        }
        let filename = Uuid::new_v4();

        // we better not be overwriting something
        // otherwise we are shooting ourselves in the foot
        // this should rarely happen if ever
        let new_path = ts.path.join(filename.to_string());
        assert!(!new_path.exists());

        let mut table = SSTable::new(new_path, false)?;
        let skipmap_len = skipmap.len();
        table.write_table(
            skipmap.into_iter(),
            skipmap_len,
        )?;
        if ts.header.mapping.len() == 0 {
            ts.header.mapping.push(Vec::new());
        }
        ts.header.mapping[0].push(filename);
        Self::commit_header(ts).await?;
        Self::compact(ts).await?;

        Ok(())
    }
    async fn commit_header(ts: &mut TreeState) -> Result<(), YAStorageError> {
        // DURABILITY: is create write_all sync_all atomic? yes of course. it is a single block write. sync_all just awaits for the synchronization to finish.
        ts.serialize_header().await?;
        Ok(())
    }

    async fn clear_cache_for_level(ts: &mut TreeState, level: u8) -> Result<(), YAStorageError> {

        for filename in ts.header.mapping[level as usize].iter() {
            // we incur 10 read ios here- we can eliminate this if we have a more robust in-memory reprsentation of on-disk state
            let num_blocks = SSTable::new(ts.path.join(filename.to_string()).clone(), true)?.get_num_blocks()?;
            ts.cache.index_cache.reset_keys([filename.to_string()].to_vec()).await;
            let mut to_delete = Vec::new();
            for block in 0..num_blocks {
                to_delete.push(filename.to_string() + "/" + &block.to_string());
            }
            ts.cache.block_cache.reset_keys(to_delete).await;
        }
        Ok(())
    }

    async fn compact_level(level: u8, ts: &mut TreeState) -> Result<(), YAStorageError> {
        assert!(level != 4);
        // do this before writes/deletes so we can still read indices
        Self::clear_cache_for_level(ts, level).await?;
        let filename = Uuid::new_v4();
        let new_table_path = ts.path
            .join(filename.to_string());
        let mut table = SSTable::new(new_table_path, false)?;
        let mut tables = (0..TABLES_UNTIL_COMPACTION)
            .map(|x| SSTable::new(ts.path.join(ts.header.mapping[level as usize][x].to_string()), true))
            .collect::<io::Result<Vec<SSTable>>>()?;
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

        ts.header.mapping[level as usize] = Vec::new();
        if ts.header.mapping.len() <= (level + 1) as usize {
            ts.header.mapping.push(Vec::new());
        }
        ts.header.mapping[(level + 1) as usize].push(filename);
        Self::commit_header(ts).await?;


        Ok(())
    }
    async fn compact(ts: &mut TreeState) -> Result<(), YAStorageError> {
        let mut level: u8 = 0;
        // this may fail between compactions, so we need to check if we need to compact on startup
        while ts.header.mapping[level as usize].len() == TABLES_UNTIL_COMPACTION {
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
            // we do not need to write to WAL! the entry is in the memtable!
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
    use tempfile::{tempdir, TempDir};
    use tokio::fs::remove_file;
    use uuid::Uuid;
    use std::error::Error;
    use super::*;

    async fn setup_nonfresh() -> Result<(TempDir, Vec<String>), Box<dyn Error>> {
        // create 2 hashes + files, make header to reference
        let hashes = vec![Uuid::new_v4().to_string(), Uuid::new_v4().to_string()];
        let dir = tempdir()?;
    
        let mut header = File::create(dir.path().clone().join("header")).await?;
        header.write_all(hashes.join("#").as_bytes()).await?;
        header.sync_all().await?;
        File::create(dir.path().clone().join("wal")).await?;
        File::create(dir.path().clone().join(hashes[0].clone())).await?;
        File::create(dir.path().clone().join(hashes[1].clone())).await?;
        Ok((dir, hashes))
    }
    
    #[tokio::test]
    async fn test_init_nonfresh() -> Result<(), Box<dyn Error>> {
        let (dir, _) = setup_nonfresh().await?;
        let mut tree = Tree::new(dir.path().as_os_str().to_str().unwrap());
        tree.init().await.expect("Failed to init folder");
        Ok(())
    }
    #[tokio::test]
    async fn test_init_fresh() -> Result<(), Box<dyn Error>> {
        let dir = tempdir()?;
        let mut tree = Tree::new(dir.path().as_os_str().to_str().unwrap());
        tree.init().await.expect("Failed to init folder");
        Ok(())
    }
    #[tokio::test]
    async fn test_init_nonfresh_failed_op() -> Result<(), Box<dyn Error>> {
        let (dir, _) = setup_nonfresh().await?;
        let failed_op_name = Uuid::new_v4().to_string();
        File::create(dir.path().clone().join(failed_op_name.clone())).await?;
        let mut tree = Tree::new(dir.path().as_os_str().to_str().unwrap());
        tree.init().await.expect("Failed to init folder");
        assert!(!dir.path().clone().join(failed_op_name).exists());
        Ok(())
    }
    #[tokio::test]
    async fn test_init_nonfresh_missing_sstable() -> Result<(), Box<dyn Error>> {
        let (dir, hashes) = setup_nonfresh().await?;

        let mut tree = Tree::new(dir.path().as_os_str().to_str().unwrap());
        remove_file(dir.path().join(hashes[0].clone())).await?;
        tree.init().await.expect_err("Initialized folder with missing sstable");
        Ok(())
    }

    #[tokio::test]
    async fn test_init_nonfresh_missing_wal() -> Result<(), Box<dyn Error>> {
        let (dir, _) = setup_nonfresh().await?;

        let mut tree = Tree::new(dir.path().as_os_str().to_str().unwrap());
        remove_file(dir.path().join("wal")).await?;
        tree.init().await.expect_err("Initialized folder with missing wal");
        Ok(())
    }

    #[tokio::test]
    async fn test_init_nonfresh_missing_header() -> Result<(), Box<dyn Error>> {
        let (dir, _) = setup_nonfresh().await?;

        let mut tree = Tree::new(dir.path().as_os_str().to_str().unwrap());
        remove_file(dir.path().join("header")).await?;
        tree.init().await.expect_err("Initialized folder with missing header");
        Ok(())
    }

    #[tokio::test]
    async fn test_init_nonfresh_corrupt_header() -> Result<(), Box<dyn Error>> {
        let (dir, _) = setup_nonfresh().await?;
        let mut header = TokioOpenOptions::new()
        .write(true)
        .open(dir.path().clone().join("header"))
        .await?;
        header.write_all(String::from("Random words").as_bytes()).await?;
        header.sync_all().await?;
        let mut tree = Tree::new(dir.path().as_os_str().to_str().unwrap());
        tree.init().await.expect_err("Initialized folder with corrupt header");
        Ok(())
    }

}
