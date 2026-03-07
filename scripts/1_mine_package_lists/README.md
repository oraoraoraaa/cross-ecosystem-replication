# Step 1: Mine Package Lists

## Overview

The scripts under this folder mine package metadata (name, homepage URL, repository URL) from six ecosystems:

- **Crates.io** (Rust) — downloads and parses the database dump
- **Maven Central** (Java) — uses Maven Indexer to read the central index
- **NPM** (JavaScript) — queries the CouchDB `_all_docs` endpoint
- **Packagist** (PHP) — queries the Packagist API
- **PyPI** (Python) — uses the Simple API and JSON API
- **RubyGems** (Ruby) — queries the RubyGems API

## Run

### All scripts except Maven miner

```sh
./setup.sh
source venv/bin/activate
python <script.py>
```

### Maven miner

```sh
cd mine_maven
./build.sh
./run.sh
```

## Output

```sh
data/package-lists/{Crates,Maven,NPM,PHP,PyPI,Ruby}.csv
```

## Note

All six miners can be run **in parallel**. This step may take **several hours or days** depending on network speed, registry scale, and API rate limits.
