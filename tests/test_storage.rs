use futures::FutureExt;
use rand::SeedableRng;
use rand::distributions::Uniform;
use rand::distributions::WeightedIndex;
use rand::prelude::Distribution;
use rand::rngs::SmallRng;
// integration tests for storage engine (kv, garbage collection, bloom filter, iterators (soon))
// see src/storage.rs for unit tests
// TODO: test db recovery better (how to inject failures?)
use tempfile::tempdir;
use std::future::Future;
use tokio::task::JoinError;
use std::error::Error;
use yadb::storage::*;
use futures::stream::FuturesUnordered;
use futures::stream::StreamExt;


fn str_to_byte_buf(s: &String) -> Vec<u8> {
    return s.as_bytes().to_vec();
}
#[tokio::test]
async fn gpd_flush() -> Result<(), Box<dyn Error>> {
    let dir = tempdir()?;
    let mut tree = Tree::new(dir.path().as_os_str().to_str().unwrap());
    tree.init().await.expect("Failed to init folder");
    let item_size = MAX_MEMTABLE_SIZE / 3;
    for i in 0..4 {
        let value: Vec<u8> = vec![i; item_size];
        let key = i.to_string();
        tree.put(&str_to_byte_buf(&key), &value).await??;
        if i == 1 {
            tree.delete(&str_to_byte_buf(&key)).await??;
        }
    }
    for i in 0..4 {
        let key = i.to_string();
        let value: Vec<u8> = vec![i; item_size];
        if i != 1 {
            assert!(tree.get(&(str_to_byte_buf(&key))).await??.unwrap() == value);
        } else {
            assert!(tree.get(&(str_to_byte_buf(&key))).await??.is_none());
        }
    }
    assert!(tree.get(&vec![0, 1]).await??.is_none());
    Ok(())
}
#[tokio::test]
async fn gpd_in_memory() -> Result<(), Box<dyn Error>> {
    let dir = tempdir()?;
    let mut tree = Tree::new(dir.path().as_os_str().to_str().unwrap());
    tree.init().await.expect("Failed to init folder");
    assert!(tree.get(&vec![0]).await??.is_none());
    tree.put(&vec![0], &vec![2]).await??;
    assert!(tree.get(&vec![0]).await??.unwrap() == vec![2]);
    tree.delete(&vec![0]).await??;
    tree.put(&vec![1], &vec![2]).await??;
    assert!(tree.get(&vec![0]).await??.is_none());
    Ok(())
}

#[tokio::test]
async fn filter_false_negative() -> Result<(), Box<dyn Error>> {
    let dir = tempdir()?;
    let mut tree = Tree::new(dir.path().as_os_str().to_str().unwrap());
    tree.init().await.expect("Failed to init folder");
    let mut futures = FuturesUnordered::new();
    for i in 0..5000 {
        let key = i.to_string();
        let value: Vec<u8> = vec![0; 10];
        futures.push(tree.put(&(str_to_byte_buf(&key)), &value));
    }

    while let Some(result) = futures.next().await {
        result??;
    }
    let mut tree = Tree::new(dir.path().as_os_str().to_str().unwrap());
    tree.init().await.expect("Failed to init folder");

    let mut futures = FuturesUnordered::new();
    
    for i in 0..5000 {
        let key = i.to_string();
        futures.push(tree.get(&(str_to_byte_buf(&key))));
    }
    while let Some(result) = futures.next().await {
        assert!(result??.is_some());
    }
    Ok(())
}
// benchmarks
#[tokio::test]
async fn write_test_async() -> Result<(), Box<dyn Error>> {
    let dir = tempdir()?;
    let mut tree = Tree::new(dir.path().as_os_str().to_str().unwrap());
    tree.init().await.expect("Failed to init folder");
    let mut futures = FuturesUnordered::new();

    for i in 0..5000 {
        let key = i.to_string();
        let value: Vec<u8> = vec![0; 10];
        futures.push(tree.put(&(str_to_byte_buf(&key)), &value));
    }

    while let Some(result) = futures.next().await {
        result??;
    }
    Ok(())
}
#[tokio::test]
async fn gpd_wal() -> Result<(), Box<dyn Error>> {
    let dir = tempdir()?;
    let mut tree = Tree::new(dir.path().as_os_str().to_str().unwrap());
    tree.init().await.expect("Failed to init folder");
    for i in 0..10 {
        let key = i.to_string();
        let value: Vec<u8> = vec![i; 1000];
        tree.get(&(str_to_byte_buf(&key))).await??;
        tree.delete(&(str_to_byte_buf(&key))).await??;
        tree.put(&(str_to_byte_buf(&key)), &value).await??;
    }
    let mut tree = Tree::new(dir.path().as_os_str().to_str().unwrap());
    tree.init().await.expect("Failed to init folder");
    for i in 0..10 {
        let key = i.to_string();
        let value: Vec<u8> = vec![i; 1000];
        assert!(tree.get(&(str_to_byte_buf(&key))).await.unwrap().unwrap().unwrap() == value);
        tree.delete(&(str_to_byte_buf(&key))).await??;
        tree.put(&(str_to_byte_buf(&key)), &value).await??;
    }
    Ok(())
}

#[tokio::test]
async fn gpd_compaction() -> Result<(), Box<dyn Error>> {
    let dir = tempdir()?;
    for i in 0..TABLES_UNTIL_COMPACTION {
        let mut tree = Tree::new(dir.path().as_os_str().to_str().unwrap());
        tree.init().await.expect("Failed to init folder");
        for j in 0..10 {
            let key = j.to_string();
            let value = i.to_string();
            if i != 0 {
                let prev_value = (i - 1).to_string();
                assert!(
                    tree.get(&(str_to_byte_buf(&key))).await??.unwrap() == str_to_byte_buf(&prev_value)
                );
            }
            tree.delete(&(str_to_byte_buf(&key))).await??;
            tree.put(&(str_to_byte_buf(&key)), &str_to_byte_buf(&value)).await??;
        }
    }
    Ok(())
}

#[tokio::test]
async fn concurrent_writes_causing_flush() -> Result<(), Box<dyn Error>> {
    let dir = tempdir()?;
    let mut tree = Tree::new(dir.path().as_os_str().to_str().unwrap());
    tree.init().await.expect("Failed to init folder");
    let mut futures = FuturesUnordered::new();
    for i in 0..1000 {
        let key = i.to_string();
        let value: Vec<u8> = vec![0; 1000];
        futures.push(tree.put(&(str_to_byte_buf(&key)), &value));

    while let Some(result) = futures.next().await {
        result??;
    }
    }
    Ok(())
}

// this takes 30 seconds
// TODO:
// changing for loop range from 0..300 to 0..3000 and value len from 10000 to 1000 makes this take 20-50 times longer...
#[tokio::test]
async fn concurrent_writes_causing_compaction() -> Result<(), Box<dyn Error>> {
    let dir = tempdir()?;
    let mut tree = Tree::new(dir.path().as_os_str().to_str().unwrap());
    tree.init().await.expect("Failed to init folder");
    let mut futures = FuturesUnordered::new();
    for i in 0..300 {
        let key = i.to_string();
        let value: Vec<u8> = vec![0; 10000];
        futures.push(tree.put(&(str_to_byte_buf(&key)), &value));

    while let Some(result) = futures.next().await {
        result??;
    }
    }
    Ok(())
}
// TODO: these are only used for the benchmark
enum DBResult {
    Empty(Result<Result<(), yadb::storage::YAStorageError>, JoinError>),
    Get(Result<Result<Option<Vec<u8>>, yadb::storage::YAStorageError>, JoinError>)
}

// could make this better
// zipf key distribution, scans
fn generate_benchmark(mut tree: Tree, rounds: i32, gpd_weights: [i32; 3], key_range: [i32; 2], val_bytes: usize) -> FuturesUnordered<std::pin::Pin<Box<dyn Future<Output = DBResult> + Send>>> {
    let futures = FuturesUnordered::new();
    let mut rng = SmallRng::seed_from_u64(42);
    // TODO: there has to be a cleaner way to do this
    let key_range = Uniform::from(key_range[0]..key_range[1]);
    let gpd_ind_dist = WeightedIndex::new(&gpd_weights).unwrap();
    for _ in 0..rounds {
        let key = key_range.sample(&mut rng).to_string();
        let value: Vec<u8> = vec![0; val_bytes];
        // sample from gpd distribution
        let gpd_type = gpd_ind_dist.sample(&mut rng);
        match gpd_type {
            0 => {
                futures.push(
                    tree.put(&(str_to_byte_buf(&key)), &value).map(DBResult::Empty)
                .boxed());
            },
            1 => {

            futures.push(
                tree.get(&(str_to_byte_buf(&key))).map(DBResult::Get)
            .boxed());
            },
            2 => {

                futures.push(
                    tree.delete(&(str_to_byte_buf(&key))).map(DBResult::Empty)
                .boxed());
            }
            // it should not sample anything else
            _ => todo!()
        }
        

    } 
    return futures;
}

// 50000 rounds gp probs of 50% each, 0-50k key range, 100 byte value -> 1.5 seconds, 5MB data -> 34K ops/second, 3.4MB/second
// not a 5x improvement, but a good result nonetheless
// what if we just passed synchronous closures? this is stretching the definition of a cache but avoids very complicated workarounds to ensuring cache entry lifetimes.
// interesting: why not specify expected collision rate rather than key range?
// then we can compute the key range and we have a better knob
// uniform distribution
#[tokio::test]
async fn split_bench() -> Result<(), Box<dyn Error>> {
    let dir = tempdir()?;
    let mut tree = Tree::new(dir.path().as_os_str().to_str().unwrap());
    tree.init().await.expect("Failed to init folder");
    let mut futures = generate_benchmark(tree, 50000, [50, 50, 0], [0, 50000], 100);
       

    while let Some(result) = futures.next().await {
        match result {
            DBResult::Get(res) => { res??; },
            DBResult::Empty(res) => { res??; },
        }
    }
    Ok(())
}

// 50MB in 5 seconds -> 10MB/second
#[tokio::test]
async fn split_bench_big_value() -> Result<(), Box<dyn Error>> {
    let dir = tempdir()?;
    let mut tree = Tree::new(dir.path().as_os_str().to_str().unwrap());
    tree.init().await.expect("Failed to init folder");
    let mut futures = generate_benchmark(tree, 50000, [50, 50, 0], [0, 50000], 10000);
       

    while let Some(result) = futures.next().await {
        match result {
            DBResult::Get(res) => { res??; },
            DBResult::Empty(res) => { res??; },
        }
    }
    Ok(())
}

// running above test for values of 10KB results in a panick- that is only 500MB but it uses all levels?

/* Things to test:
test compaction

out of space
test recovery -> difficult to test this (could make any IO fail, but how can I do this without mocking every IO)
performance -> 50/50 r/w 90/10 r/w 10/90 r/w

fuzz testing
think of more

future tests:
concurrency (concurrent writes in memory, from fs (after wal recovery, compaction or flush))

merge operator
custom comparator
(different compaction strategies?)

*/
