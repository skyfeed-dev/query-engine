# SkyFeed Query Engine

Query Engine used for custom feeds created with the SkyFeed Builder.

Loads all posts from the DB in memory, fetches new posts every 100 seconds, updates like count and runs queries to build feeds.

Depends on a Surreal DB instance with data from <https://github.com/skyfeed-dev/indexer>.