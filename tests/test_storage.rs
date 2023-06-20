// integration tests for storage
// recovery, compaction, g/p/d
extern crate tempfile;
use self::tempfile::tempdir;
use std::error::Error;
extern crate yadb;
use yadb::storage::Tree;

#[test]
fn interesting_case() -> Result<(), Box<dyn Error>> {
    let dir = tempdir()?;
    let mut tree = Tree::new(dir.path().as_os_str().to_str().unwrap());
    tree.init().expect("Failed to init folder");
    for i in 0..3 {
        let key = i.to_string();
        let value: Vec<u8> = vec![i; 500_000];
        println!("get");
        tree.get(&(key.as_bytes().to_vec()))?;
        println!("delete");
        tree.delete(&(key.as_bytes().to_vec()))?;
        println!("put");
        tree.put(&(key.as_bytes().to_vec()), &value)?;
    }
    Ok(())
}

#[test]
fn gpd_wal() -> Result<(), Box<dyn Error>> {
    let dir = tempdir()?;
    let mut tree = Tree::new(dir.path().as_os_str().to_str().unwrap());
    tree.init().expect("Failed to init folder");
    for i in 0..3 {
        let key = i.to_string();
        let value: Vec<u64> = vec![i; 1000];
        tree.get(&(key.as_bytes().to_vec()))?;
        tree.delete(&(key.as_bytes().to_vec()))?;
        tree.put(&(key.as_bytes().to_vec()), &(value.iter().flat_map(|&x| x.to_le_bytes().to_vec()).collect()))?;
    }
    let mut tree = Tree::new(dir.path().as_os_str().to_str().unwrap());
    tree.init().expect("Failed to init folder");
    for i in 0..3 {
        let key = i.to_string();
        let value: Vec<u64> = vec![i; 1000];
        let value_bytes: Vec<u8> = value.iter().flat_map(|&x| x.to_le_bytes().to_vec()).collect();
        assert!(tree.get(&(key.as_bytes().to_vec())).unwrap().unwrap() == value_bytes);
        tree.delete(&(key.as_bytes().to_vec()))?;
        tree.put(&(key.as_bytes().to_vec()), &value_bytes)?;
    }
    Ok(())
}

#[test]
fn gpd_compaction() -> Result<(), Box<dyn Error>> {
    Ok(())
}