#!/bin/bash
cd "$(dirname "$0")/.."
sudo docker run --privileged --rm -it -v "$(pwd)":/deepflow hub.deepflow.yunshan.net/public/rust-build bash -c "cd /deepflow/agent && cargo build"
