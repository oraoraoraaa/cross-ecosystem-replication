# Step 3: Mine GitHub Metrics

## Overview

The scripts under this folder collect six health metrics for all cross-ecosystem packages from the GitHub API:

- **Popularity:** stars, forks, contributors
- **Activity:** commits, pull requests, issues

Also mines language proportions provided by GitHub for each repository and computes unique merged contributors across multi-repo groups.

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

## Note

A **cache** is provided in `data/cache/github-metrics/`. It contains the GitHub metrics we use at the moment of our study. This is would avoid re-mining previously collected data and speed up the script.
