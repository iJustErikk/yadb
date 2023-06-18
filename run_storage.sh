# testing only
cargo build --bin storage
rm -rf yadb
cargo run --bin storage
