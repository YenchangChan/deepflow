#!/bin/bash
cd "$(dirname "$0")/.."

CARGO_CACHE_DIR="${HOME}/.cargo-docker-cache"
mkdir -p "${CARGO_CACHE_DIR}"/{registry,git}

sudo docker run --privileged --rm -it \
    -v "$(pwd)":/deepflow \
    -v "${CARGO_CACHE_DIR}/registry":/usr/local/cargo/registry \
    -v "${CARGO_CACHE_DIR}/git":/usr/local/cargo/git \
    hub.deepflow.yunshan.net/public/rust-build \
    bash -c "cd /deepflow/agent && cargo build"
