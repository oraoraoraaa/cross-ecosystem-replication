# Step 5: Detect Ecosystems

## Overview

The scripts under this folder will:

1. **Detect naming conventions** — Identifies language-specific named folders (e.g., `python/`, `java/`)
2. **Detect binding patterns** — Identifies binding/wrapper folder and file patterns
3. **Detect platform folders** — Identifies OS/architecture-specific distribution folders
4. **Consolidate patterns** — Merges all detections into five per-pattern JSON files (P1–P5)
5. **Analyze correlations** — Computes descriptive statistics and top/bottom 10% pattern distributions across health metrics

## Run

```sh
./run.sh
```

## Input

All outputs from previous steps.

## Output

```sh
data/analysis/naming-convention/
data/analysis/binding/
data/analysis/platform-folders/
data/analysis/consolidated-patterns/
data/analysis/pattern-correlation/
```
