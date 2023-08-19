use tokio::sync::oneshot;
use tokio::sync::oneshot::Sender;
use std::collections::HashMap;
use tokio::sync::Mutex;
use std::sync::Arc;
use schnellru::ByMemoryUsage;
use std::marker::PhantomData;
use schnellru::LruMap;

use futures::future::BoxFuture; 

// lock on this state
struct CacheState<T> {
    cache: LruMap<String, T, ByMemoryUsage>,
    // let's use a vector of oneshot channels, as barrier does not send any data
    waiting: HashMap<String, Vec<Sender<T>>>
}
// made this generic so I can use sister types + so I can use this in b-epsilon storage implementation
// T: what we cache, S: what we use in get to retrieve, E: templated to be able to use yadberror from sister file
pub struct AsyncCache<T, S, K, E> 
where T: Clone,
{
    cache: Arc<Mutex<CacheState<T>>>,
    // oh golly this is pretty ugly
    retrieve: Box<dyn Fn(S, K) -> BoxFuture<'static, Result<(T, S), E>> + Send + Sync>,
    // needed for rust compiler to be okay with S, K
    marker: PhantomData<S>,
    marker_2: PhantomData<K>,
}

impl<T, S, K, E> AsyncCache<T, S, K, E> 
where
// debug is temporary
    T: Clone,
    {
    pub fn new(max_bytes: usize, retrieve: Box<dyn Fn(S, K) -> BoxFuture<'static, Result<(T, S), E>> + Send + Sync>) -> Self {
        return Self {cache: Arc::new(Mutex::new(CacheState { cache: LruMap::new(ByMemoryUsage::new(max_bytes)), waiting: HashMap::new() })), retrieve, marker: PhantomData, marker_2: PhantomData};
    }
    // called during compaction. used to eliminate old caches
    // PRECONDITION: waiting must not contain any of these keys
    // more strongly, it should be empty if this is called, as there should be no waiting threads
    // this will be called at least 30 times during compaction
    // the lock will be uncontended though
    pub async fn reset_keys(&self, keys: Vec<String>) {
        let cloned = self.cache.clone();
        let mut cache = cloned.lock().await;
        for key in keys {
            cache.cache.remove(&key);
        }
    }
    // we pass along the io_wrapper so we don't have to deal with references and lifetimes
    pub async fn get(&self, io_wrapper: S, key: String, metadata: K) -> Result<(T, S), E> {
        let cloned = self.cache.clone();
        let mut cache = cloned.lock().await;
        let potential_hit = cache.cache.get(&key);
        if potential_hit.is_some() {
            return Ok((potential_hit.unwrap().clone(), io_wrapper));
        }
        // not a hit, is there a waiting list? 
        // if so, join it
        if cache.waiting.contains_key(&key) {
            let (tx, rx) = oneshot::channel();
            cache.waiting.get_mut(&key).unwrap().push(tx);
            return Ok((rx.await.unwrap(), io_wrapper));
        }
        // let's start the waiting for everyone
        cache.waiting.insert(key.clone(), Vec::new());
        // drop lock for concurrency, send read IO
        std::mem::drop(cache);
        let (res, io_wrapper) = (self.retrieve)(io_wrapper, metadata).await?;
        // get lock back, store value, alert others, return for ourselves
        let cloned = self.cache.clone();
        let mut cache = cloned.lock().await;
        for chan in cache.waiting.get_mut(&key).unwrap().drain(..) {
            chan.send(res.clone()).unwrap_or_else(|_| panic!("DEBUG: Something is very wrong"));
        }
        cache.cache.insert(key.clone(), res.clone());
        cache.waiting.remove(&key);
        return Ok((res, io_wrapper));
    }
}