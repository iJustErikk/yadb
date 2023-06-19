#!/bin/bash
sudo apt-get update
# install perf for cargo flamegraph
sudo apt-get install linux-tools-common linux-tools-generic
if ! which cargo > /dev/null; then
    curl --proto '=https' --tlsv1.2 https://sh.rustup.rs -sSf | sh
fi

# avoid perf issue
echo "kernel.perf_event_paranoid = 1" | sudo tee -a /etc/sysctl.conf
sudo sysctl -p

cargo install flamegraph