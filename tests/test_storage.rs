// integration tests for storage engine (kv, garbage collection, bloom filter, iterators (soon))
// see src/storage.rs for unit tests
// TODO: test db recovery better (how to inject failures?)
extern crate tempfile;
use self::tempfile::tempdir;
use std::error::Error;
extern crate yadb;
use yadb::storage::*;



fn str_to_byte_buf(s: &String) -> Vec<u8> {
    return s.as_bytes().to_vec();
}
#[test]
fn gpd_flush() -> Result<(), Box<dyn Error>> {
    let dir = tempdir()?;
    let mut tree = Tree::new(dir.path().as_os_str().to_str().unwrap());
    tree.init().expect("Failed to init folder");
    let item_size = MAX_MEMTABLE_SIZE / 3;
    for i in 0..4 {
        let value: Vec<u8> = vec![i; item_size];
        let key = i.to_string();
        tree.put(&str_to_byte_buf(&key), &value)?;
        if i == 1 {
            tree.delete(&str_to_byte_buf(&key))?;
        }
    }
    for i in 0..4 {
        let key = i.to_string();
        let value: Vec<u8> = vec![i; item_size];
        if i != 1 {
            assert!(tree.get(&(str_to_byte_buf(&key)))?.unwrap() == value);
        } else {
            assert!(tree.get(&(str_to_byte_buf(&key)))?.is_none());
        }
    }
    assert!(tree.get(&vec![0,1])?.is_none());
    Ok(())
}
#[test]
fn gpd_in_memory() -> Result<(), Box<dyn Error>> {
    let dir = tempdir()?;
    let mut tree = Tree::new(dir.path().as_os_str().to_str().unwrap());
    tree.init().expect("Failed to init folder");
    assert!(tree.get(&vec![0])?.is_none());
    tree.put(&vec![0], &vec![2])?;
    assert!(tree.get(&vec![0])?.unwrap() == vec![2]);
    tree.delete(&vec![0])?;
    tree.put(&vec![1], &vec![2])?;
    assert!(tree.get(&vec![0])?.is_none());
    Ok(())
}
#[test]
fn filter_false_negative() -> Result<(), Box<dyn Error>> {
    let dir = tempdir()?;
    let mut tree = Tree::new(dir.path().as_os_str().to_str().unwrap());
    tree.init().expect("Failed to init folder");
    for i in 0..5000 {
        let key = i.to_string();
        let value: Vec<u8> = vec![0; 10];
        tree.put(&(str_to_byte_buf(&key)), &value)?;
    }
    let mut tree = Tree::new(dir.path().as_os_str().to_str().unwrap());
    tree.init().expect("Failed to init folder");
    for i in 0..5000 {
        let key = i.to_string();
        assert!(tree.get(&(str_to_byte_buf(&key)))?.is_some());
    }
    Ok(())
}

#[test]
fn gpd_wal() -> Result<(), Box<dyn Error>> {
    let dir = tempdir()?;
    let mut tree = Tree::new(dir.path().as_os_str().to_str().unwrap());
    tree.init().expect("Failed to init folder");
    for i in 0..10 {
        let key = i.to_string();
        let value: Vec<u8> = vec![i; 1000];
        tree.get(&(str_to_byte_buf(&key)))?;
        tree.delete(&(str_to_byte_buf(&key)))?;
        tree.put(&(str_to_byte_buf(&key)), &value)?;
    }
    let mut tree = Tree::new(dir.path().as_os_str().to_str().unwrap());
    tree.init().expect("Failed to init folder");
    for i in 0..10 {
        let key = i.to_string();
        let value: Vec<u8> = vec![i; 1000];
        println!("{} blah", key);
        assert!(tree.get(&(str_to_byte_buf(&key))).unwrap().unwrap() == value);
        tree.delete(&(str_to_byte_buf(&key)))?;
        tree.put(&(str_to_byte_buf(&key)), &value)?;
    }
    Ok(())
}

#[test]
fn gpd_compaction() -> Result<(), Box<dyn Error>> {
    let dir = tempdir()?;
    for i in 0..TABLES_UNTIL_COMPACTION {
        let mut tree = Tree::new(dir.path().as_os_str().to_str().unwrap());
        tree.init().expect("Failed to init folder");
        for j in 0..10 {
            let key = j.to_string();
            let value = i.to_string();
            if i != 0 {
                let prev_value = (i-1).to_string();
                assert!(tree.get(&(str_to_byte_buf(&key)))?.unwrap() == str_to_byte_buf(&prev_value));
            }
            tree.delete(&(str_to_byte_buf(&key)))?;
            tree.put(&(str_to_byte_buf(&key)), &str_to_byte_buf(&value))?;
        }
    }
    Ok(())
}

/* Things to test:

empty k/v
super long k/v

test compaction

out of space
test recovery - difficult to test this (could make any IO fail, but how can I do this without mocking every IO)
performance

fuzz testing
think of more

future tests:
concurrency
merge operator
custom comparator
(different compaction strategies?)

*/