# Cross-Ecosystem Packages: Replication Package

This is the replication package for the paper **"Cross-Ecosystem Packages As Multilingual: Prevalence, Architecture, and Health"**.

## Overview

This package provides all scripts and tools needed to replicate the data collection pipeline and analysis presented in the paper. The pipeline mines package metadata from six major ecosystems, identifies cross-ecosystem packages, collects GitHub metrics and directory structures, detects ecosystem presence, and analyzes architectural patterns.

## Prerequisites

- **Python 3.8+** with `pip`
- **Java 11+** and **Apache Maven** (for Maven Central miner only)
- **Node.js** and **npm** (for NPM miner only)
- **GitHub API token(s)** — Steps 3 and 4 require GitHub API access. Set via:

  ```bash
  export GITHUB_TOKEN=ghp_your_token_here
  ```

The script would also prompt you to input the token if you do not set the environment variables.

Multiple tokens can be passed for higher rate limits (see individual script `--help`).

## Run

Navigate to `scripts/` and run the scripts inside step by step.

## Studied Ecosystems

| Ecosystem     | Language              | Registry                      |
| ------------- | --------------------- | ----------------------------- |
| Crates.io     | Rust                  | https://crates.io/            |
| Maven Central | Java/JVM              | https://central.sonatype.com/ |
| NPM           | JavaScript/TypeScript | https://www.npmjs.com/        |
| Packagist     | PHP                   | https://packagist.org/        |
| PyPI          | Python                | https://pypi.org/             |
| RubyGems      | Ruby                  | https://rubygems.org/         |

## Package Structure

```
.
├── requirements.txt            # Python dependencies
├── README.md
├── data/
│   ├── package-lists/          # [Output] Step 1: Package list CSVs
│   ├── cross-ecosystem-filter/ # [Output] Step 2: Cross-ecosystem package JSON
│   ├── github-metrics/         # [Output] Step 3: GitHub metrics JSON
│   ├── directory-structures/   # [Output] Step 4: Repository directory trees
│   ├── ecosystem-detection/    # [Output] Step 5: Ecosystem detection results
│   └── analysis/               # [Output] Step 6: Pattern analysis results
│       ├── naming-convention/
│       ├── binding/
│       ├── platform-folders/
│       ├── consolidated-patterns/
│       └── pattern-correlation/
└── scripts/
    ├── 1_mine_package_lists/         # Step 1: Mine package metadata from 6 ecosystems
    ├── 2_filter_cross_ecosystem/     # Step 2: Identify cross-ecosystem packages
    ├── 3_mine_github_metrics/        # Step 3: Collect GitHub health metrics
    ├── 4_mine_directory_structures/  # Step 4: Mine repository directory trees
    ├── 5_detect_ecosystems/          # Step 5: Detect ecosystem presence
    └── 6_analyze_patterns/           # Step 6: Architectural pattern analysis
```

## Identified Architectural Patterns

| Pattern             | Description                                                                  |
| ------------------- | ---------------------------------------------------------------------------- |
| P1: Multi-repo      | Separate repositories per language under the same GitHub owner               |
| P2: Missing code    | Repository lacks source code for one or more registered ecosystems           |
| P3: Designated Dir. | Language-specific named folders (e.g., `python/`, `java/`, `rust/`)          |
| P4: Templating      | IDL-based code generation (Protocol Buffers, Thrift, FlatBuffers)            |
| P5: Bind/Wrap       | Language bindings (JSII, WASM, PyO3) or prebuilt native binary distributions |
