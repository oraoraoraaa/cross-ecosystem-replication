# Step 5: Detect Ecosystems

## Overview

The scripts under this folder will:

1. **Verify multi-repo languages** — Checks that multi-repo packages contain their expected language
2. **Detect ecosystems** — Scans directory structures for source file extensions to determine which ecosystems are present; classifies packages as fully matched, partially matched, or fully mismatched
3. **Detect special patterns** — Identifies template generation (.proto, .thrift, .fbs), JSII/WASM/PyO3 bindings, Maven WebJars, and PHP Composer wrappers

## Run

```sh
./run.sh
```

## Input

```sh
data/directory-structures/directory_structures.json
data/github-metrics/github_metrics.json
```

## Output

```sh
data/ecosystem-detection/fully_matched.json
data/ecosystem-detection/partially_matched.json
data/ecosystem-detection/fully_mismatched.csv
data/ecosystem-detection/special_patterns/
```
