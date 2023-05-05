use rocksdb::{DB, Options};

fn main() {
    let mut opts = Options::default();
    opts.create_if_missing(true);

    
    let db = DB::open(&opts, "yadb").unwrap();
    db.put(b"key", b"value").unwrap();

    
    match db.get(b"key") {
        Ok(Some(value)) => {
            let value_str = String::from_utf8(value).unwrap();
            println!("Stored value: {}", value_str);
        }
        Ok(None) => println!("No value found for the given key."),
        Err(e) => println!("Error reading from the database: {}", e),
    }

    db.delete(b"key").unwrap();
}
