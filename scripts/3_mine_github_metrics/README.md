# Step 3: Mine GitHub Metrics

## Overview

The scripts under this folder would:

1. Collect six health metrics for all cross-ecosystem packages from the GitHub API:

- **Popularity:** stars, forks, contributors
- **Activity:** commits, pull requests, issues

and mine language proportions provided by GitHub for each repository.

2. Compute the count of unique merged contributors across multi-repo groups.

## Run

```sh
./run.sh
```

## Input

```sh
data/cross-ecosystem-filter/cross_ecosystem_packages.json
```

## Output

```sh
data/github-metrics/github_metrics.json
data/github-metrics/summary.txt
```
