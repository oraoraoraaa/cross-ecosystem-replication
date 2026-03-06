# Step 2: Filter Cross-Ecosystem Packages

## Overview

The script under this folder identifies packages that are published to two or more ecosystems using two detection methods:

- **Mono-repo detection:** Same normalized GitHub URL appears in 2+ ecosystem registries
- **Multi-repo detection:** Same GitHub owner has repositories with ecosystem-specific suffixes (e.g., `project-py`, `project-js`)

## Run

```sh
./run.sh
```

## Input

```sh
data/package-lists/*.csv
```

## Output

```sh
data/cross-ecosystem-filter/cross_ecosystem_packages.json
```
