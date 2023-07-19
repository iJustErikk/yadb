use crossbeam_skiplist::SkipMap;
// TODO: we 'reach' into here often. we should expose functions


pub const MAX_MEMTABLE_SIZE: usize = 1_000_000;
pub struct Memtable {
    // TODO: should not be pub :)
    pub skipmap: SkipMap<Vec<u8>, Vec<u8>>,
    pub size: usize,
}

impl Memtable {
    pub fn reset(&mut self) {
        self.skipmap = SkipMap::new();
        self.size = 0;
    }
    pub fn get_skipmap(&mut self) -> SkipMap<Vec<u8>, Vec<u8>>{
        let mut skipmap: SkipMap<Vec<u8>, Vec<u8>> = SkipMap::new();
        std::mem::swap(&mut skipmap, &mut self.skipmap);
        return skipmap;
    }
    pub fn needs_flush(&self) -> bool {
        return self.size > MAX_MEMTABLE_SIZE;
    }
    pub fn insert_or_delete(&mut self, key: &Vec<u8>, val: &Vec<u8>, is_put: bool) {
        let old_size = if self.skipmap.contains_key(key) {
            key.len() + self.skipmap.get(key).unwrap().value().len()
        } else {
            0
        };
        let new_size = key.len() + val.len();
        // split these up, as adding the difference causes overflow on deletes (cannot have negatices)
        self.size += new_size;
        self.size -= old_size;

        // TODO: I could remove the copies one of two ways:
        // - take ownership of kv (will the end user want this?)
        // - work with references (this will put the effort on the end user)
        // right now, copying sounds best as it avoids making the user keep the kv in memory or having to copy into this 
        // function
        // 
        if is_put {
            self.skipmap.insert(key.to_vec(), val.to_vec());
        } else {
            self.skipmap.insert(key.to_vec(), Vec::new());
        }
    }
}