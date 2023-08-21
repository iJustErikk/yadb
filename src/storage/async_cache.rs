use tokio::sync::oneshot;
use tokio::sync::oneshot::Sender;
use std::collections::HashMap;
use tokio::sync::Mutex;
use std::sync::Arc;
use schnellru::ByMemoryUsage;
use std::marker::PhantomData;
use schnellru::LruMap;

use futures::future::BoxFuture;
// AsyncCache
// Caches disk reads for multiple consumers. A previous version of this had performance issues with cloning (copying big chunks is slow)
// So the new/current version will also take a callback to act on the retrieved value to prevent data from needing to be moved around.

// lock on this state
struct CacheState<T, C> {
    cache: LruMap<String, T, ByMemoryUsage>,
    // let's use a vector of oneshot channels, as barrier does not send any data
    waiting: HashMap<String, Vec<(Sender<C>, Vec<u8>)>>
}
// made this generic so I can use sister types + so I can use this in b-epsilon storage implementation
// T: what we cache, S: what we use in get to retrieve, K: any extra information to help retrieval, C: what the user supplied callback returns, E: templated to be able to use yadberror from sister file
pub struct AsyncCache<T, S, K, C, E> 
where T: Clone,
C: Clone
{
    cache: Arc<Mutex<CacheState<T, C>>>,
    retrieve: Box<dyn Fn(S, K) -> BoxFuture<'static, Result<(T, S), E>> + Send + Sync>,
    // Vec is not a reference as it will be annoying to get this to compile
    // it surely will be alive though. how do I convince the compiler?
    callback: Box<dyn Fn(&T, Vec<u8>) -> C + Send + Sync>,
    // needed for rust compiler to be okay with S, K, C
    marker: PhantomData<S>,
    marker_2: PhantomData<K>,
    marker_3: PhantomData<C>,
}

impl<T, S, K, C, E> AsyncCache<T, S, K, C, E> 
where
// debug is temporary
    T: Clone,
    C: Clone
    {
    pub fn new(max_bytes: usize, retrieve: Box<dyn Fn(S, K) -> BoxFuture<'static, Result<(T, S), E>> + Send + Sync>, callback: Box<dyn Fn(&T, Vec<u8>) -> C + Send + Sync>) -> Self {
        return Self {cache: Arc::new(Mutex::new(CacheState { cache: LruMap::new(ByMemoryUsage::new(max_bytes)), waiting: HashMap::new() })), retrieve, marker: PhantomData, marker_2: PhantomData, marker_3: PhantomData, callback};
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
    // this got complicated
    // we pass along the io_wrapper so we don't have to deal with references and lifetimes
    pub async fn get(&self, io_wrapper: S, cache_key: String, metadata: K, callback_key: Vec<u8>) -> Result<(C, S), E> {
        let cloned = self.cache.clone();
        let mut cache = cloned.lock().await;
        let potential_hit = cache.cache.get(&cache_key);
        if potential_hit.is_some() {
            return Ok(((self.callback)(potential_hit.unwrap(), callback_key), io_wrapper));
        }
        // not a hit, is there a waiting list? 
        // if so, join it
        if cache.waiting.contains_key(&cache_key) {
            let (tx, rx) = oneshot::channel();
            cache.waiting.get_mut(&cache_key).unwrap().push((tx, callback_key));
            return Ok((rx.await.unwrap(), io_wrapper));
        }
        // let's start the waiting for everyone
        cache.waiting.insert(cache_key.clone(), Vec::new());
        // drop lock for concurrency, send read IO
        std::mem::drop(cache);
        let (retrieved, io_wrapper) = (self.retrieve)(io_wrapper, metadata).await?;
        // get lock back, store value, alert others, return for ourselves
        let cloned = self.cache.clone();
        let mut cache = cloned.lock().await;
        for (chan, cur_callback_key) in cache.waiting.get_mut(&cache_key).unwrap().drain(..) {
            chan.send((self.callback)(&retrieved, cur_callback_key)).unwrap_or_else(|_| panic!("DEBUG: Something is very wrong"));
        }
        let res = (self.callback)(&retrieved, callback_key);
        cache.cache.insert(cache_key.clone(), retrieved);
        cache.waiting.remove(&cache_key);
        return Ok((res, io_wrapper));
    }
}