use fs::File;
use growable_bloom_filter::GrowableBloom;
use std::cell::RefCell;

use std::fs;
use std::hash::BuildHasher;
use std::hash::Hasher;
use std::io;
use std::io::Cursor;
use std::io::Seek;
use std::io::SeekFrom;
use std::iter::Peekable;
use std::path::PathBuf;

use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};
use std::collections::BinaryHeap;
use std::io::Read;
use std::io::Write;



// facade for table accesses. combines serde with knowledge of ondisk structure to provide clean api.
// also provides API for a couple of different types of iterators to write table.
// TODO: implementation has tons of magic values. is there a better way to do this?
// I think the best thing to do is isolate seek logic + offset io to internal functions
// TODO: this needs to be asyncified, but there are lots of issues with that (async iterators, ord in heaps with async)
pub struct SSTable {
    file: File,
    total_data_bytes: Option<u64>,
    current_block: Option<DataBlock>,
    current_block_idx: usize,
}

pub const BLOCK_SIZE: usize = 4_000;

impl SSTable {
    pub fn new(path: PathBuf, reading: bool) -> io::Result<Self> {
        if !reading && !path.parent().unwrap().exists() {
            fs::create_dir(path.parent().unwrap())?;
        }
        let mut file = if reading {
            File::open(&path)?
        } else {
            File::create(&path)?
        };
        let total_data_bytes = None;
        if reading {
            file.seek(SeekFrom::End(-24))?;
            let total_data_bytes = Some(file.read_u64::<LittleEndian>()?);
            file.seek(SeekFrom::Start(0))?;
            return Ok(SSTable {
                file,
                total_data_bytes,
                current_block: None,
                current_block_idx: 0,
            });
        }
        return Ok(SSTable {
            file,
            total_data_bytes,
            current_block: None,
            current_block_idx: 0,
        });
    }
    // TODO: rework this to become private
    pub fn get_num_unique_keys(&mut self) -> io::Result<u64> {
        self.file.seek(SeekFrom::End(-8))?;
        let num_keys = self.file.read_u64::<LittleEndian>()?;
        self.file.seek(SeekFrom::Start(0))?;
        Ok(num_keys)
    }
    // TODO: this should be private
    pub fn get_index(&mut self) -> io::Result<Index> {
        // third to last u64 in file
        self.file.seek(SeekFrom::End(-24))?;
        let datablock_size = self.file.read_u64::<LittleEndian>()?;
        self.file.seek(SeekFrom::Start(datablock_size))?;
        let index = Index::deserialize(&self.file)?;
        self.file.seek(SeekFrom::Start(0))?;
        Ok(index)
    }

    pub fn get_num_blocks(&mut self) -> io::Result<usize> {
        Ok(self.get_index()?.entries.len())
    }
    // TODO: fix filter
    pub fn get_filter(&mut self) -> io::Result<GrowableBloom> {
        self.file.seek(SeekFrom::End(-16))?;
        let filter_offset = self.file.read_u64::<LittleEndian>()?;
        self.file.seek(SeekFrom::Start(filter_offset))?;
        let filter_size = self.file.read_u64::<LittleEndian>()?;
        let mut filter_bytes: Vec<u8> = vec![0; filter_size as usize];
        self.file.read_exact(&mut filter_bytes)?;
        let filter = bincode::deserialize(&filter_bytes).unwrap();
        self.file.seek(SeekFrom::Start(0))?;
        Ok(filter)
    }
    pub fn get_datablock(&mut self, offset: u64) -> io::Result<DataBlock> {
        self.file.seek(SeekFrom::Start(offset))?;
        let db = DataBlock::deserialize(&mut self.file)?;
        self.file.seek(SeekFrom::Start(0))?;
        return Ok(db);
    }

    pub fn write_table<I>(&mut self, it: I, num_unique_keys: usize) -> io::Result<()>
    where
        I: IntoIterator<Item = (Vec<u8>, Vec<u8>)>,
    {
        // TODO: have I been doing this entirely in memory? I thought I was doing this out of memory. need to fix this
        // TODO: I could convert these to use the serialize function for DataBlock + Index
        // this would present overhead, as they would most likely be copied into the ds just to be written out
        // but it would keep the logic for serde in one impl
        // TODO: we can avoid writing the tombstone if a the key is not in any older block
        // we can surely do this when compacting the current top level
        // however, how can we do this if its not the current top level?
        // could use bloom filter...however the ~97% chance tp tn exponentially decreases
        // even by 20 tables, this is still better than guessing (~54%)
        // so if we cache the bloom filters on startup, this is an option.
        let mut index: Vec<u8> = Vec::new();
        let mut data_len = 0;
        // TODO: most blocks will be slightly larger than 4kb- we can reserve to save some time
        let mut current_block: Vec<u8> = Vec::new();
        current_block.reserve(4096);
        // needs to be roughly uniform, deterministic and fast -> not the default hasher
        // interesting read: https://www.eecs.harvard.edu/~michaelm/postscripts/tr-02-05.pdf
        // TODO: look into hash DOS attacks
        // using growable-bloom-filter since it has serde support and decent documentation
        let r: f64 = 0.03;
        let mut filter = GrowableBloom::new(r, num_unique_keys);
        let mut unique_keys: u64 = 0;
        for (key, value) in it {
            // println!("{unique_keys} {num_unique_keys}");
            filter.insert(&key);
            if current_block.is_empty() {
                index.write_u64::<LittleEndian>(key.len() as u64)?;
                index.extend(&key);
                index.write_u64::<LittleEndian>(data_len as u64)?;
            }
            // TODO: this will pack things into blocks until they exceed 4kb
            // that means most reads will be 2 blocks
            // no bueno, fix this
            current_block.write_u64::<LittleEndian>(key.len() as u64)?;
            current_block.extend(key);
            current_block.write_u64::<LittleEndian>(value.len() as u64)?;
            current_block.extend(value);
            if current_block.len() >= BLOCK_SIZE {
                self.file.write_u64::<LittleEndian>(current_block.len() as u64)?;
                self.file.write_all(&mut current_block)?;
                data_len += current_block.len() + 8;
                current_block = Vec::new();
                current_block.reserve(4096);
            }
            unique_keys += 1;
        }
        // write last block if not already written
        if current_block.len() > 0 {
            self.file.write_u64::<LittleEndian>(current_block.len() as u64)?;
            self.file.write_all(&mut current_block)?;
            data_len += current_block.len() + 8;
        }

        self.file.write_u64::<LittleEndian>(index.len() as u64)?;
        self.file.write_all(&index)?;
        let ex_filter = bincode::serialize(&filter).unwrap();
        self.file
            .write_u64::<LittleEndian>(ex_filter.len() as u64)?;
        self.file.write_all(&ex_filter)?;
        // footer: 8 bytes for index offset, 8 for filter offset, 8 for unique keys
        // TODO: magic value
        let filter_offset = index.len() + data_len + 8;
        self.file
            .write_u64::<LittleEndian>(data_len as u64)?;
        self.file.write_u64::<LittleEndian>(filter_offset as u64)?;
        self.file.write_u64::<LittleEndian>(unique_keys)?;
        self.file.sync_all()?;
        Ok(())
    }
}

impl Iterator for SSTable {
    // this iterator can return errors
    type Item = io::Result<(Vec<u8>, Vec<u8>)>;

    fn next(&mut self) -> Option<Self::Item> {
        // we better open this in read mode
        assert!(self.total_data_bytes.is_some());
        if self.current_block.is_none()
            || self.current_block_idx == self.current_block.as_ref().unwrap().entries.len()
        {
            // we better not be past the data bytes
            assert!(self.total_data_bytes.unwrap() >= self.file.stream_position().unwrap());
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
        let kv = std::mem::replace(
            &mut self.current_block.as_mut().unwrap().entries[self.current_block_idx],
            (Vec::new(), Vec::new()),
        );
        self.current_block_idx += 1;
        return Some(Ok(kv));
    }
}

// TODO: expose search_entries to keep this private
pub struct DataBlock {
    pub entries: Vec<(Vec<u8>, Vec<u8>)>,
}

impl Clone for DataBlock {
    fn clone(self: &DataBlock) -> Self {
        return DataBlock { entries: self.entries.clone() };
    }
}

impl DataBlock {
    fn deserialize(reader: &mut File) -> io::Result<Self> {
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

        Ok(DataBlock { entries: entries })
    }
}

impl Clone for Index {
    fn clone(&self) -> Self {
        return Index {
            entries: self.entries.clone()
        }
    }
}

// index grows linearly with sstable keys/data
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
    fn deserialize(mut reader: &File) -> io::Result<Index> {
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
        Ok(Index { entries })
    }
}

pub struct Index {
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
// TODO: shouldn't this be private? we should steal search_table from other Tree
pub fn search_index(index: &Index, key: &Vec<u8>) -> Option<u64> {
    assert!(index.entries.len() != 0);
    assert!(key.len() != 0);
    if key < &index.entries[0].0 {
        return None;
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
        } else if key >= &index.entries[mid + 1].0 {
            left = mid + 1;
        } else {
            // if key > start && < end -> in bucket
            return Some(index.entries[mid].1);
        }
    }
}
pub struct MergedTableIterators {
    heap: BinaryHeap<TableNum>,
}

struct TableNum {
    // we are using refcell so we can peek
    table: RefCell<Peekable<SSTable>>,
    num: usize,
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
        let mut first_table = self.table.borrow_mut();
        let mut second_table = other.table.borrow_mut();
        let first = first_table.peek();
        let second = second_table.peek();
        // we will not push a empty iterator back into the heap
        assert!(first.is_some());
        assert!(second.is_some());
        // if there is an error, we unfortunately cannot return it
        // TODO: cannot find an errable heap implementation, will need to spin one up
        // could very messily order errors to be less than non errors and then handle an error when popping
        let first = &(match first {
            Some(Ok(first)) => Some(first),
            // won't happen
            _ => None,
        }
        .unwrap()
        .0);
        let second = &(match second {
            Some(Ok(second)) => Some(second),
            // won't happen
            _ => None,
        }
        .unwrap()
        .0);
        let first = (first, self.num);
        let second = (second, other.num);
        // order first by peeked key ref, then by table number (higher is younger and higher)
        // we want this to be a min heap
        // order first by key (smaller key means greater)
        // then order by table number (smaller table number means greater)
        // TODO: custom comparators
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
    pub fn new(tables: Vec<SSTable>) -> io::Result<Self> {
        let mut heap: BinaryHeap<TableNum> = BinaryHeap::new();
        for (num, table) in tables.into_iter().enumerate() {
            let mut peekable_table = table.peekable();
            let first_res = peekable_table.peek();
            assert!(first_res.is_some());
            if first_res.unwrap().is_err() {
                let next = peekable_table.next().unwrap().err().unwrap();
                return Err(next);
            }
            heap.push(TableNum {
                table: RefCell::new(peekable_table),
                num,
            });
        }
        return Ok(MergedTableIterators { heap });
    }
}

impl Iterator for MergedTableIterators {
    type Item = (Vec<u8>, Vec<u8>);

    fn next(&mut self) -> Option<Self::Item> {
        if self.heap.is_empty() {
            return None;
        }
        let popped_value = self.heap.pop().unwrap();
        let (res, should_push) = {
            let mut table = popped_value.table.borrow_mut();
            let res = table.next();
            // we will not put back an empty iterator and if an iterator produced an error, we would have panicked before
            assert!((&res).is_some());
            let res = res.unwrap();
            assert!((&res).is_ok());
            (res.unwrap(), table.peek().is_some())
        };
        if should_push {
            self.heap.push(popped_value);
        }
        Some(res)
    }
}

// wrap murmur3 function so we can use it to type filter
struct FastMurmur3;
struct FastMurmur3Hasher {
    data: Vec<u8>,
}

impl Hasher for FastMurmur3Hasher {
    fn finish(&self) -> u64 {
        let big_hash: u128 = fastmurmur3::hash(&self.data);
        // we'll just take the last 64 bits
        // this moves the 1 to 65th spot, the subtraction carries all the way so we end up with a nice 64 bitmask
        (big_hash & ((1u128 << 64) - 1)) as u64
    }

    fn write(&mut self, bytes: &[u8]) {
        self.data.extend_from_slice(bytes);
    }
}

impl Default for FastMurmur3Hasher {
    fn default() -> Self {
        Self { data: Vec::new() }
    }
}

impl BuildHasher for FastMurmur3 {
    type Hasher = FastMurmur3Hasher;

    fn build_hasher(&self) -> Self::Hasher {
        FastMurmur3Hasher::default()
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
            entries: slice
                .iter()
                .enumerate()
                .map(|(i, &x)| (str_to_byte_buf(x), i as u64))
                .collect(),
        };
    }
    // for these tests: vector ordering is lexicographic, so it will respect 1 byte character strings
    #[test]
    fn test_out_of_range_to_the_left() {
        let index = ["bombastic", "sideeye"];
        assert!(
            search_index(&index_from_string_vector(&index), &str_to_byte_buf(&"aeon")).is_none()
        );
    }
    #[test]
    fn test_item_first_middle_last() {
        let index = ["affluent", "burger", "comes", "to", "town", "sunday"];
        assert!(
            search_index(
                &index_from_string_vector(&index),
                &str_to_byte_buf(&"apple")
            )
            .unwrap()
                == 0
        );
        assert!(
            search_index(
                &index_from_string_vector(&index),
                &str_to_byte_buf(&"todler")
            )
            .unwrap()
                == 3
        );
        assert!(
            search_index(
                &index_from_string_vector(&index),
                &str_to_byte_buf(&"zephyr")
            )
            .unwrap()
                == 5
        );
    } // test item on boundary of buckets (just barely less than the next)
    #[test]
    fn test_left_right_bounary() {
        let index = ["affluent", "burger", "comes"];
        assert!(
            search_index(
                &index_from_string_vector(&index),
                &str_to_byte_buf(&"burger")
            )
            .unwrap()
                == 1
        );
        assert!(
            search_index(
                &index_from_string_vector(&index),
                &str_to_byte_buf(&"comeq")
            )
            .unwrap()
                == 1
        );
        assert!(
            search_index(
                &index_from_string_vector(&index),
                &str_to_byte_buf(&"comes")
            )
            .unwrap()
                == 2
        );
    }
    #[test]
    fn test_single_two_item_index() {
        let single = ["affluent"];
        let double = ["affluent", "burger"];
        assert!(
            search_index(
                &index_from_string_vector(&single),
                &str_to_byte_buf(&"affluent")
            )
            .unwrap()
                == 0
        );
        assert!(
            search_index(
                &index_from_string_vector(&single),
                &str_to_byte_buf(&"burger")
            )
            .unwrap()
                == 0
        );
        assert!(
            search_index(
                &index_from_string_vector(&double),
                &str_to_byte_buf(&"aging")
            )
            .unwrap()
                == 0
        );
        // Hired as eng. Promoted to pun master.
        assert!(
            search_index(
                &index_from_string_vector(&double),
                &str_to_byte_buf(&"burgeroisie")
            )
            .unwrap()
                == 1
        );
    }
}
