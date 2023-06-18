
cargo build --bin storage
rm -rf yadb
export CARGO_PROFILE_RELEASE_DEBUG=true
cargo flamegraph --bin=storage -o ./profile.svg