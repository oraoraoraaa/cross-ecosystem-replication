# Step 4: Mine Directory Structures

## Overview

The script under this folder fetches complete directory trees for all valid mono-repo cross-ecosystem packages via the GitHub Tree API.

## Run

```sh
./run.sh
```

## Input

```sh
data/github-metrics/github_metrics.json
```

## Output

```sh
data/directory-structures/directory_structures.json
```

## Note

A **cache** is provided in `data/cache/directory-structures`. It contains the directory structures we use at the moment of our study. This is would avoid re-mining previously collected data and speed up the script.
