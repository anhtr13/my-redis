#!/bin/sh

set -e # Exit early if any commands fail

(
    cd "$(dirname "$0")" # Ensure compile steps are run within the repository directory
    cargo build --release --target-dir=/tmp/codecrafters-build-redis-rust --manifest-path Cargo.toml
)

exec /tmp/codecrafters-build-redis-rust/release/my-redis "$@"
