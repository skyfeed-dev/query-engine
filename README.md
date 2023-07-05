# SkyFeed Builder Query Engine

Query Engine used for custom feeds created with the SkyFeed Builder.

Loads all posts from the DB in memory, fetches new posts every 100 seconds, updates like count and runs queries to build feeds.

Depends on a Surreal DB instance with data from <https://github.com/skyfeed-dev/indexer>.

## System Requirements

Uses between 0.5 and 1.0 GB RAM for every 24h you want to keep in memory. CPU usage for queries is pretty low.

## How to run

1. Install Rust (https://www.rust-lang.org/tools/install)
2. Run `cargo build --release`
3. Copy `.env.example` to `.env` and edit the values
4. Run with `./target/release/skyfeed-query-engine`