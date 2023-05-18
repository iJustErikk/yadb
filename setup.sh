#!/bin/bash
if ! which cargo > /dev/null; then
    curl --proto '=https' --tlsv1.2 https://sh.rustup.rs -sSf | sh
    # this does not work: . "$HOME/.cargo/env"
fi