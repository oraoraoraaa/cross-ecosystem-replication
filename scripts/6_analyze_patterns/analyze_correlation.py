#!/usr/bin/env python3
"""
Analyze pattern correlation with GitHub metrics.

Produces:
1. Box + strip plots showing the distribution of each metric
   (stars, forks, commits, pull_requests, issues, contributors)
   across the 5 patterns.
2. Top 10% vs. bottom 10% pattern distribution for every metric
   (stars, forks, commits, pull_requests, issues, contributors).

Input:  data/analysis/consolidated-patterns/pattern_*.json
Output: data/analysis/pattern-correlation/
"""

import json
from pathlib import Path

import matplotlib.pyplot as plt
import pandas as pd
import seaborn as sns

# ============================================================================
# PATH CONFIGURATION
# ============================================================================
DATA_DIR = Path(__file__).resolve().parent.parent.parent / "data"
INPUT_PATH = DATA_DIR / "analysis" / "consolidated-patterns"
OUTPUT_PATH = DATA_DIR / "analysis" / "pattern-correlation"

PATTERN_FILES = {
    "P1: URL-Not-Matched": "pattern_1_url_not_matched.json",
    "P2: Source-Code-Not-Found": "pattern_2_source_code_not_found.json",
    "P3: Lang-Specific-Folder": "pattern_3_language_specific_folder.json",
    "P4: Protocol-Buffers": "pattern_4_protocol_buffers.json",
    "P5: Binding/Wrapper": "pattern_5_binding_wrapper.json",
}

METRICS = ["stars", "forks", "commits", "pull_requests", "issues", "contributors"]

METRIC_LABELS = {
    "stars": "Stars",
    "forks": "Forks",
    "commits": "Commits",
    "pull_requests": "Pull Requests",
    "issues": "Issues",
    "contributors": "Contributors",
}


# ============================================================================
# DATA LOADING
# ============================================================================
def load_all_patterns() -> pd.DataFrame:
    """Load all 5 pattern JSON files into a single DataFrame."""
    rows = []
    for pattern_label, filename in PATTERN_FILES.items():
        filepath = INPUT_PATH / filename
        if not filepath.exists():
            print(f"WARNING: {filepath} not found, skipping.")
            continue
        with open(filepath, "r") as f:
            data = json.load(f)
        for pkg in data["packages"]:
            gm = pkg.get("github_metrics")
            if not gm:
                continue
            row = {"pattern": pattern_label}
            for m in METRICS:
                row[m] = gm.get(m)
            rows.append(row)
    df = pd.DataFrame(rows)
    # Replace empty strings with 0, then coerce to numeric
    for m in METRICS:
        df[m] = df[m].replace("", 0)
        df[m] = pd.to_numeric(df[m], errors="coerce")
    df.dropna(subset=METRICS, inplace=True)
    for m in METRICS:
        df[m] = df[m].astype(int)
    print(f"Loaded {len(df)} packages across {df['pattern'].nunique()} patterns.")
    return df


# ============================================================================
# 1. METRIC DISTRIBUTION BOX + STRIP PLOTS
# ============================================================================
def plot_metric_distribution(df: pd.DataFrame, metric: str, output_dir: Path):
    """Create a horizontal box + strip plot for one metric across patterns."""
    sns.set_theme(style="ticks")

    f, ax = plt.subplots(figsize=(9, 6))
    ax.set_xscale("log")

    # Filter out zeros for log scale
    plot_df = df[df[metric] > 0].copy()

    sns.boxplot(
        data=plot_df,
        x=metric,
        y="pattern",
        hue="pattern",
        whis=[0, 100],
        width=0.6,
        palette="vlag",
        legend=False,
    )

    sns.stripplot(
        data=plot_df,
        x=metric,
        y="pattern",
        size=2.5,
        color=".3",
        alpha=0.4,
        jitter=True,
    )

    ax.xaxis.grid(True)
    ax.set_xlabel(METRIC_LABELS[metric])
    ax.set_ylabel("")
    ax.set_title(f"Distribution of {METRIC_LABELS[metric]} by Pattern")
    sns.despine(trim=True, left=True)

    plt.tight_layout()
    out = output_dir / f"distribution_{metric}.png"
    f.savefig(out, dpi=200)
    plt.close(f)
    print(f"  Saved {out.name}")


# ============================================================================
# ANALYSIS HELPERS
# ============================================================================
def _build_dist(groups_and_subsets: list, total_n: dict) -> pd.DataFrame:
    results = []
    for group_name, subset in groups_and_subsets:
        total = len(subset)
        for pat in PATTERN_FILES.keys():
            cnt = int((subset["pattern"] == pat).sum())
            results.append(
                {
                    "group": group_name,
                    "pattern": pat,
                    "count": cnt,
                    "proportion": cnt / total if total else 0,
                }
            )
    return pd.DataFrame(results)


RANDOM_SEED = 42


def _select_extreme_n(df: pd.DataFrame, metric: str, n: int, largest: bool) -> pd.DataFrame:
    """
    Select exactly n rows representing the top (largest=True) or bottom
    (largest=False) of `metric`.  Rows strictly beyond the boundary are
    taken first; ties at the boundary are broken by random sampling so that
    all patterns are fairly represented even when many packages share the
    same boundary value (e.g. 0 stars).
    """
    sorted_df = df.sort_values(metric, ascending=not largest)
    boundary_val = sorted_df.iloc[n - 1][metric]

    if largest:
        strict = sorted_df[sorted_df[metric] > boundary_val]
        tied   = sorted_df[sorted_df[metric] == boundary_val]
    else:
        strict = sorted_df[sorted_df[metric] < boundary_val]
        tied   = sorted_df[sorted_df[metric] == boundary_val]

    need = n - len(strict)
    sampled_tied = tied.sample(n=need, random_state=RANDOM_SEED)
    return pd.concat([strict, sampled_tied])


def compute_top_bottom_distribution(
    df: pd.DataFrame, metric: str, label: str
) -> pd.DataFrame:
    """Top 10% vs. bottom 10% by `metric`, with fair tie-breaking."""
    n = max(1, int(len(df) * 0.10))
    top_df = _select_extreme_n(df, metric, n, largest=True)
    bot_df = _select_extreme_n(df, metric, n, largest=False)
    return _build_dist(
        [(f"Top 10% {label}", top_df), (f"Bottom 10% {label}", bot_df)], {}
    )



# ============================================================================
# SUMMARY
# ============================================================================
def _fmt_dist_table(dist_df: pd.DataFrame) -> str:
    """Format a distribution DataFrame as a fixed-width paper-ready table."""
    groups = dist_df["group"].unique()
    patterns = list(PATTERN_FILES.keys())

    # Header
    col_w = 26
    group_col = 30
    header = f"{'Pattern':<{col_w}}" + "".join(f"{g:>{group_col}}" for g in groups)
    sep = "-" * len(header)

    rows = [header, sep]
    for pat in patterns:
        row = f"{pat:<{col_w}}"
        for g in groups:
            cell = dist_df.loc[
                (dist_df["group"] == g) & (dist_df["pattern"] == pat)
            ]
            if len(cell):
                cnt = int(cell["count"].values[0])
                prop = cell["proportion"].values[0]
                row += f"{cnt:>12,} ({prop:>6.1%}){' ' * (group_col - 20)}"
            else:
                row += f"{'—':>{group_col}}"
        rows.append(row)

    # Totals
    rows.append(sep)
    tot_row = f"{'Total':<{col_w}}"
    for g in groups:
        n = int(dist_df.loc[dist_df["group"] == g, "count"].sum())
        tot_row += f"{n:>12,}{' ' * (group_col - 12)}"
    rows.append(tot_row)
    return "\n".join(rows)


# Mapping from metric key -> (human label, short description for table header)
_METRIC_TABLE_META = [
    ("stars",         "Stars",         "Star Count"),
    ("forks",         "Forks",         "Fork Count"),
    ("commits",       "Commits",       "Commit Count"),
    ("pull_requests", "Pull Requests", "Pull-Request Count"),
    ("issues",        "Issues",        "Issue Count"),
    ("contributors",  "Contributors",  "Contributor Count"),
]


def save_summary(
    metric_dists: dict,
    df: pd.DataFrame,
    output_dir: Path,
):
    """Write a paper-ready summary text file."""
    WIDTH = 90
    n10 = max(1, int(len(df) * 0.10))
    out = output_dir / "summary.txt"

    with open(out, "w") as f:
        f.write("=" * WIDTH + "\n")
        f.write("Pattern Correlation Analysis — Summary\n")
        f.write(f"Total packages: {len(df):,}  |  Patterns: {df['pattern'].nunique()}\n")
        f.write("=" * WIDTH + "\n\n")

        # ── Table 1: per-pattern metric statistics ────────────────────────────
        f.write("Table 1. Descriptive Statistics of GitHub Metrics per Pattern\n")
        f.write("-" * WIDTH + "\n")
        col_names = ["Metric", "Pattern", "n", "Median", "Mean", "Std", "Q1", "Q3", "Min", "Max"]
        col_w = [16, 28, 7, 8, 10, 10, 8, 8, 8, 10]
        header = "".join(f"{c:>{w}}" for c, w in zip(col_names, col_w))
        f.write(header + "\n")
        f.write("-" * WIDTH + "\n")
        for m in METRICS:
            first = True
            for pat in PATTERN_FILES.keys():
                sub = df[df["pattern"] == pat][m]
                metric_col = METRIC_LABELS[m] if first else ""
                first = False
                f.write(
                    f"{metric_col:>{col_w[0]}}"
                    f"{pat:>{col_w[1]}}"
                    f"{len(sub):>{col_w[2]},}"
                    f"{sub.median():>{col_w[3]}.0f}"
                    f"{sub.mean():>{col_w[4]}.1f}"
                    f"{sub.std():>{col_w[5]}.1f}"
                    f"{sub.quantile(0.25):>{col_w[6]}.0f}"
                    f"{sub.quantile(0.75):>{col_w[7]}.0f}"
                    f"{sub.min():>{col_w[8]}}"
                    f"{sub.max():>{col_w[9]}}\n"
                )
            f.write("-" * WIDTH + "\n")

        # ── Tables 2-7: top 10% vs. bottom 10% for each metric ───────────────
        for table_idx, (metric_key, metric_label, metric_desc) in enumerate(
            _METRIC_TABLE_META, start=2
        ):
            dist = metric_dists[metric_key]
            f.write("\n")
            f.write(
                f"Table {table_idx}. Pattern Distribution — "
                f"Top 10% vs. Bottom 10% (by {metric_desc})\n"
            )
            f.write(f"  Top 10%: {n10:,} packages with highest {metric_label.lower()} counts\n")
            f.write(f"  Bottom 10%: {n10:,} packages with lowest {metric_label.lower()} counts\n")
            f.write(f"  (Ties at the boundary are broken by random sampling, seed={RANDOM_SEED})\n")
            f.write("-" * WIDTH + "\n")
            f.write(_fmt_dist_table(dist) + "\n")

    print(f"  Saved {out.name}")


# ============================================================================
# MAIN
# ============================================================================
def main():
    OUTPUT_PATH.mkdir(parents=True, exist_ok=True)

    df = load_all_patterns()

    # 1. Metric distribution plots
    print("\nGenerating metric distribution plots...")
    for metric in METRICS:
        plot_metric_distribution(df, metric, OUTPUT_PATH)

    # 2. Top 10% vs. bottom 10% analysis for all metrics
    print("\nComputing top/bottom 10% distributions for all metrics...")
    metric_dists = {}
    for metric_key, metric_label, _ in _METRIC_TABLE_META:
        metric_dists[metric_key] = compute_top_bottom_distribution(
            df, metric_key, metric_label
        )
        print(f"  Done: {metric_label}")

    # 3. Summary
    print("\nWriting summary...")
    save_summary(metric_dists, df, OUTPUT_PATH)

    print(f"\nAll outputs saved to {OUTPUT_PATH}")


if __name__ == "__main__":
    main()
