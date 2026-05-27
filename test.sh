#!/bin/sh

set -e

(
    cd "$(dirname "$0")"
    cargo build --release --target-dir=/tmp/build-redis-rust --manifest-path Cargo.toml
)

exec /tmp/codecrafters-build-redis-rust/release/my-redis "$@"
