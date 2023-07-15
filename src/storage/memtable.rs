extern crate crossbeam_skiplist;
use self::crossbeam_skiplist::SkipMap;
// TODO: we 'reach' into here often. we should expose functions
pub struct Memtable {
    // TODO: should not be pub :)
    pub skipmap: SkipMap<Vec<u8>, Vec<u8>>,
    pub size: usize
}