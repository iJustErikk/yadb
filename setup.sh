#!/bin/bash
if which cargo > /dev/null; then
  echo "bash is annoying, ignore this"
else
    curl --proto '=https' --tlsv1.2 https://sh.rustup.rs -sSf | sh
    source "$HOME/.cargo/env"
fi
sudo apt install build-essential
sudo apt install -y clang
sudo apt-get install libclang-dev
rustup toolchain install nightly
cargo +nightly install -Z sparse-registry --debug cargo-ament-build