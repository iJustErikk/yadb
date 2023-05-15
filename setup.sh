#!/bin/bash
if ! which cargo > /dev/null; then
    curl --proto '=https' --tlsv1.2 https://sh.rustup.rs -sSf | sh
    . "$HOME/.cargo/env"
fi

# sudo apt install build-essential
# sudo apt install -y clang
# sudo apt-get install libclang-dev
# rustup toolchain install nightly
# cargo +nightly install -Z sparse-registry --debug cargo-ament-build