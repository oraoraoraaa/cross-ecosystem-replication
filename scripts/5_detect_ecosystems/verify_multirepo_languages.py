#!/usr/bin/env python3
"""
Multirepo Language Verifier

Verifies that multirepo packages have the corresponding programming language
present in their repository's language proportions (as reported by GitHub).

For example:
  - github.com/author/repo-py    → should have Python in language_proportions
  - github.com/author/repo-js    → should have JavaScript in language_proportions
  - github.com/author/java-sdk   → should have Java in language_proportions

Process:
1. Load github_metrics.json and filter to valid multirepo packages
   (skip forked, archived, and entries with errors).
2. For each valid package, tokenize the repository name (splitting on -, _, .)
   and scan all tokens for known ecosystem suffix patterns.
3. Map detected suffix(es) to expected language names on GitHub.
4. Check whether those languages appear in the `language_proportions` field.
5. Classify each package as:
     - matched:            All detected ecosystem languages are present
     - mismatched:         At least one detected ecosystem language is absent
     - no_suffix_detected: No ecosystem suffix found in repo name
6. Output a single JSON file with metadata/statistics and matched/mismatched lists.
"""

import json
import re
from collections import defaultdict
from datetime import datetime
from pathlib import Path
from typing import Optional

from tqdm import tqdm

# ==============================================================================
# PATH CONFIGURATION
# ==============================================================================

DATA_DIR = Path(__file__).resolve().parent.parent.parent / "data"

INPUT_FILE = DATA_DIR / "github-metrics" / "github_metrics.json"
OUTPUT_DIR = DATA_DIR / "ecosystem-detection"
OUTPUT_FILE = OUTPUT_DIR / "multirepo_language_verification.json"

OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

# ==============================================================================
# ECOSYSTEM SUFFIX PATTERNS
# Sourced from filter_cross_ecosystem.py
# ==============================================================================

ECOSYSTEM_SUFFIX_PATTERNS: dict[str, list[str]] = {
    'PyPI': [
        'python', 'py', 'pypi', 'python2', 'python3', 'py2', 'py3',
        'cpython', 'pysdk', 'pyclient', 'pylib'
    ],
    'Crates': [
        'rust', 'rs', 'cargo', 'rustlang', 'crate', 'crates'
    ],
    'NPM': [
        'js', 'javascript', 'node', 'nodejs', 'npm', 'ts', 'typescript',
        'jsclient', 'tsclient', 'jssdk', 'tssdk', 'jslib', 'tslib'
    ],
    'Maven': [
        'java', 'jvm', 'maven', 'scala', 'kotlin', 'kt', 'kts',
        'javaclient', 'javasdk', 'javalib', 'scalaclient', 'scalalib'
    ],
    'Ruby': [
        'ruby', 'rb', 'gem', 'rubygem', 'rubyclient', 'rubylib'
    ],
    'PHP': [
        'php', 'php5', 'php7', 'php8', 'phpclient', 'phpsdk', 'phplib'
    ]
}

# Build reverse lookup: suffix token → ecosystem
SUFFIX_TO_ECOSYSTEM: dict[str, str] = {}
for ecosystem, suffixes in ECOSYSTEM_SUFFIX_PATTERNS.items():
    for suffix in suffixes:
        # Longer/more specific suffixes take precedence if there is ever a
        # collision (e.g., 'typescript' > 'ts').  We sort by descending
        # length so the first assignment wins when iterating longest-first.
        if suffix not in SUFFIX_TO_ECOSYSTEM:
            SUFFIX_TO_ECOSYSTEM[suffix] = ecosystem

# Pre-sort suffix list by descending length for greedy matching
ALL_SUFFIXES_SORTED = sorted(SUFFIX_TO_ECOSYSTEM.keys(), key=len, reverse=True)

# ==============================================================================
# ECOSYSTEM → EXPECTED GITHUB LANGUAGE NAMES
# These are the language labels as they appear in GitHub's language_proportions.
# A package is "matched" for an ecosystem when at least one of its expected
# languages is present (case-insensitive) in language_proportions.
# ==============================================================================

ECOSYSTEM_LANGUAGES: dict[str, list[str]] = {
    'PyPI':   ['Python'],
    'Crates': ['Rust'],
    'NPM':    ['JavaScript', 'TypeScript', 'CoffeeScript'],
    'Maven':  ['Java', 'Scala', 'Kotlin', 'Groovy'],
    'Ruby':   ['Ruby'],
    'PHP':    ['PHP']
}


# ==============================================================================
# HELPERS
# ==============================================================================

def tokenize_repo_name(repo_name: str) -> list[str]:
    """
    Split a repository name into tokens on common separators (-, _, .)
    and return all non-empty lowercase tokens.

    Examples:
        'octobat-npm'        → ['octobat', 'npm']
        'java-forex-quotes'  → ['java', 'forex', 'quotes']
        'sdk.python'         → ['sdk', 'python']
        '0g-ts-sdk'          → ['0g', 'ts', 'sdk']
    """
    return [t for t in re.split(r'[-_.]', repo_name.lower()) if t]


def detect_ecosystems_in_repo_name(repo_name: str) -> list[tuple[str, str]]:
    """
    Return all (token, ecosystem) pairs where a token from the repo name
    matches a known ecosystem suffix.  Order preserves the token's position
    in the repo name.

    De-duplicates by ecosystem: only the first matching token per ecosystem
    is returned.
    """
    tokens = tokenize_repo_name(repo_name)
    seen_ecosystems: set[str] = set()
    results: list[tuple[str, str]] = []

    for token in tokens:
        # Greedy: check longer suffixes first
        for suffix in ALL_SUFFIXES_SORTED:
            if token == suffix and suffix in SUFFIX_TO_ECOSYSTEM:
                ecosystem = SUFFIX_TO_ECOSYSTEM[suffix]
                if ecosystem not in seen_ecosystems:
                    seen_ecosystems.add(ecosystem)
                    results.append((token, ecosystem))
                break  # stop checking suffixes once we matched this token

    return results


def parse_language_proportions(lang_str: str) -> dict[str, float]:
    """
    Parse a language_proportions string like:
        "Ruby:82.33%;C:11.41%;Java:4.28%"
    into a dict:
        {'Ruby': 82.33, 'C': 11.41, 'Java': 4.28}

    Returns an empty dict for empty/missing values.
    """
    if not lang_str or not lang_str.strip():
        return {}

    result: dict[str, float] = {}
    for part in lang_str.split(';'):
        part = part.strip()
        if ':' in part:
            lang, _, pct_str = part.partition(':')
            try:
                result[lang.strip()] = float(pct_str.replace('%', '').strip())
            except ValueError:
                pass
    return result


def languages_present_for_ecosystem(
    ecosystem: str,
    lang_proportions: dict[str, float]
) -> list[str]:
    """
    Return the list of expected languages (for the given ecosystem) that
    are actually present in lang_proportions (case-insensitive match).
    """
    expected = ECOSYSTEM_LANGUAGES.get(ecosystem, [])
    found: list[str] = []
    lang_proportions_lower = {k.lower(): k for k in lang_proportions}
    for lang in expected:
        if lang.lower() in lang_proportions_lower:
            found.append(lang_proportions_lower[lang.lower()])
    return found


# ==============================================================================
# MAIN LOGIC
# ==============================================================================

def load_and_filter_multirepo(input_path: Path):
    """
    Load github_metrics.json and return only valid multirepo packages:
      - source == "multirepo"
      - not forked
      - not archived
      - no error field
    """
    print(f"Loading {input_path} ...")
    with open(input_path, encoding='utf-8') as f:
        data = json.load(f)

    packages = data.get("packages", {})

    total_multirepo = 0
    stats_excluded = {
        'forked_only': 0,
        'archived_only': 0,
        'forked_and_archived': 0,
        'errors': 0,
    }
    valid: list[dict] = []

    for key, entry in packages.items():
        if entry.get("source") != "multirepo":
            continue
        total_multirepo += 1

        is_fork     = bool(entry.get("is_fork", False))
        is_archived = bool(entry.get("is_archived", False))
        has_error   = "error" in entry

        if has_error:
            stats_excluded['errors'] += 1
            continue
        if is_fork and is_archived:
            stats_excluded['forked_and_archived'] += 1
            continue
        if is_fork:
            stats_excluded['forked_only'] += 1
            continue
        if is_archived:
            stats_excluded['archived_only'] += 1
            continue

        valid.append(entry)

    total_excluded = sum(stats_excluded.values())
    print(f"  Total multirepo packages  : {total_multirepo:,}")
    print(f"  Excluded (fork/archive/err): {total_excluded:,}")
    print(f"  Valid for analysis         : {len(valid):,}")

    return valid, total_multirepo, stats_excluded


def verify_packages(valid_packages: list[dict]):
    """
    For each valid multirepo package:
      1. Detect ecosystem(s) from repo name tokens.
      2. Check language_proportions for expected languages.
      3. Classify as matched / mismatched / no_suffix_detected.

    Returns three lists: matched, mismatched, no_suffix_detected.
    """
    matched:              list[dict] = []
    mismatched:           list[dict] = []
    no_suffix_detected:   list[dict] = []

    for entry in tqdm(valid_packages, desc="Verifying language proportions"):
        owner_repo = entry.get("owner_repo", "")
        repo_name  = owner_repo.split("/", 1)[1] if "/" in owner_repo else owner_repo

        detected = detect_ecosystems_in_repo_name(repo_name)
        lang_proportions = parse_language_proportions(
            entry.get("language_proportions", "")
        )

        if not detected:
            record = _build_record(entry, repo_name, [], lang_proportions, None)
            no_suffix_detected.append(record)
            continue

        # For each detected (token, ecosystem) pair, check language presence
        ecosystem_verification: list[dict] = []
        all_matched = True

        for token, ecosystem in detected:
            found_langs = languages_present_for_ecosystem(ecosystem, lang_proportions)
            is_ok = len(found_langs) > 0
            if not is_ok:
                all_matched = False
            ecosystem_verification.append({
                "matched_token":     token,
                "detected_ecosystem": ecosystem,
                "expected_languages": ECOSYSTEM_LANGUAGES.get(ecosystem, []),
                "languages_found":   found_langs,
                "language_matched":  is_ok
            })

        record = _build_record(entry, repo_name, ecosystem_verification,
                               lang_proportions, all_matched)

        if all_matched:
            matched.append(record)
        else:
            mismatched.append(record)

    return matched, mismatched, no_suffix_detected


def _build_record(
    entry: dict,
    repo_name: str,
    ecosystem_verification: list[dict],
    lang_proportions: dict[str, float],
    is_matched: Optional[bool]
) -> dict:
    """Build a unified output record for a package."""
    return {
        "owner_repo":            entry.get("owner_repo", ""),
        "repo_url":              entry.get("repo_url", ""),
        "repo_name":             repo_name,
        "ecosystem_verification": ecosystem_verification,
        "language_proportions":  entry.get("language_proportions", ""),
        "top_language":          entry.get("top_language", ""),
        "stars":                 entry.get("stars", 0),
        "forks":                 entry.get("forks", 0),
        "commits":               entry.get("commits", 0),
        "contributors":          entry.get("contributors", 0),
        "dependents":            entry.get("dependents", 0),
    }


def compute_statistics(
    total_multirepo: int,
    stats_excluded: dict,
    matched: list[dict],
    mismatched: list[dict]
) -> dict:
    """Compute summary statistics for the metadata block."""
    valid_total = len(matched) + len(mismatched)

    # Per-ecosystem counts
    ecosystem_matched_counts:   dict[str, int] = defaultdict(int)
    ecosystem_mismatched_counts: dict[str, int] = defaultdict(int)

    for rec in matched:
        for ev in rec["ecosystem_verification"]:
            ecosystem_matched_counts[ev["detected_ecosystem"]] += 1

    for rec in mismatched:
        for ev in rec["ecosystem_verification"]:
            if not ev["language_matched"]:
                ecosystem_mismatched_counts[ev["detected_ecosystem"]] += 1

    # Union of all ecosystems seen
    all_ecosystems = sorted(
        set(ecosystem_matched_counts) | set(ecosystem_mismatched_counts)
    )
    per_ecosystem: dict[str, dict] = {}
    for eco in all_ecosystems:
        m  = ecosystem_matched_counts[eco]
        mm = ecosystem_mismatched_counts[eco]
        total_eco = m + mm
        per_ecosystem[eco] = {
            "matched":    m,
            "mismatched": mm,
            "total":      total_eco,
            "match_rate_pct": round(m / total_eco * 100, 2) if total_eco else 0.0
        }

    total_excluded = sum(stats_excluded.values())

    return {
        "total_multirepo_in_input": total_multirepo,
        "excluded": {
            "total":    total_excluded,
            "forked_only":         stats_excluded['forked_only'],
            "archived_only":       stats_excluded['archived_only'],
            "forked_and_archived": stats_excluded['forked_and_archived'],
            "errors":              stats_excluded['errors'],
        },
        "valid_for_analysis": valid_total,
        "verification_results": {
            "matched": {
                "count":       len(matched),
                "percentage":  round(len(matched)   / valid_total * 100, 2) if valid_total else 0.0,
                "description": "The expected ecosystem language (determined by the suffix in the repo name) is present in its GitHub language proportions"
            },
            "mismatched": {
                "count":       len(mismatched),
                "percentage":  round(len(mismatched) / valid_total * 100, 2) if valid_total else 0.0,
                "description": "The expected ecosystem language (determined by the suffix in the repo name) is absent from its GitHub language proportions"
            }
        },
        "per_ecosystem_statistics": per_ecosystem
    }


def main():
    print("=" * 72)
    print("Multirepo Language Verifier")
    print("=" * 72)

    # 1. Load and filter
    valid_packages, total_multirepo, stats_excluded = load_and_filter_multirepo(INPUT_FILE)

    # 2. Verify
    print("\nVerifying ecosystem language presence ...")
    matched, mismatched, no_suffix = verify_packages(valid_packages)

    print(f"\n  Matched             : {len(matched):,}")
    print(f"  Mismatched          : {len(mismatched):,}")

    # 3. Statistics
    stats = compute_statistics(
        total_multirepo, stats_excluded, matched, mismatched
    )

    # 4. Build output
    output = {
        "metadata": {
            "description": (
                "Verification of multirepo packages: each repository has exactly one "
                "expected ecosystem determined by the language suffix in its name "
                "(e.g., repo-py → PyPI/Python, repo-js → NPM/JavaScript). "
                "Checks whether that expected language appears in the repository's "
                "GitHub language proportions."
            ),
            "input_file":    str(INPUT_FILE.resolve()),
            "output_file":   str(OUTPUT_FILE.resolve()),
            "ecosystem_suffix_patterns":  ECOSYSTEM_SUFFIX_PATTERNS,
            "ecosystem_language_mapping": ECOSYSTEM_LANGUAGES,
            "statistics":    stats,
            "generated_at":  datetime.now().isoformat(timespec='seconds')
        },
        "mismatched": mismatched
    }

    # 5. Write output
    print(f"\nWriting results to {OUTPUT_FILE} ...")
    with open(OUTPUT_FILE, 'w', encoding='utf-8') as f:
        json.dump(output, f, indent=2, ensure_ascii=False)

    # 6. Print summary
    valid_total = stats["valid_for_analysis"]
    vr = stats["verification_results"]
    print("\n" + "=" * 72)
    print("SUMMARY")
    print("=" * 72)
    print(f"Total multirepo packages in input : {total_multirepo:,}")
    print(f"Excluded (fork/archive/error)      : {stats['excluded']['total']:,}")
    print(f"Valid for analysis                 : {valid_total:,}")
    print()
    print(f"Matched             : {vr['matched']['count']:>6,}  ({vr['matched']['percentage']:.2f}%)")
    print(f"Mismatched          : {vr['mismatched']['count']:>6,}  ({vr['mismatched']['percentage']:.2f}%)")
    print()
    print("Per-ecosystem breakdown (matched / mismatched):")
    for eco, s in sorted(stats["per_ecosystem_statistics"].items()):
        print(f"  {eco:<8}: {s['matched']:>5,} matched  {s['mismatched']:>5,} mismatched"
              f"  ({s['match_rate_pct']:.1f}% match rate)")
    print()
    print(f"Output saved to: {OUTPUT_FILE}")
    print("=" * 72)


if __name__ == "__main__":
    main()
