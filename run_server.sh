# TODO: the server should make this if it does not exist
mkdir logs && touch logs/log
cargo build --bin server
cargo run --bin server