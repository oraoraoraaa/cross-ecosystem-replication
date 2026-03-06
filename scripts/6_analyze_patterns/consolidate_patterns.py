#!/usr/bin/env python3
"""
Consolidate cross-ecosystem package data into 5 JSON files by pattern.

Pattern 1: URL-Not-Matched
Pattern 2: Ecosystem Source Code Not Found
Pattern 3: Language-Specifically-Named Folder
Pattern 4: Protocol Buffers Usage
Pattern 5: Binding/Wrapper

Each output entry contains:
- repo_url
- source
- claimed_ecosystem
- claimed_ecosystem_package_name
- detected_ecosystem (if applicable)
- github_metrics
"""

import json
import csv
import re
import os
import sys
from pathlib import Path
from collections import defaultdict
from datetime import datetime

# ============================================================================
# PATH CONFIGURATION
# ============================================================================
DATA_DIR = Path(__file__).resolve().parent.parent.parent / "data"
PACKAGE_LIST_PATH = DATA_DIR / "package-lists"
METRICS_PATH = DATA_DIR / "github-metrics" / "github_metrics.json"
OUTPUT_PATH = DATA_DIR / "analysis" / "consolidated-patterns"

# Pattern 2 sources
FULLY_MISMATCHED_CSV = DATA_DIR / "ecosystem-detection" / "fully_mismatched.csv"
PARTIALLY_MATCHED_JSON = DATA_DIR / "ecosystem-detection" / "partially_matched.json"

# Pattern 3 sources
NAMING_CONVENTION_PATH = DATA_DIR / "analysis" / "naming-convention"

# Pattern 4 sources
TEMPLATE_GEN_JSON = DATA_DIR / "ecosystem-detection" / "special_patterns" / "template_generation.json"

# Pattern 5 sources
JSII_JSON = DATA_DIR / "ecosystem-detection" / "special_patterns" / "jsii_binding.json"
PYO3_JSON = DATA_DIR / "ecosystem-detection" / "special_patterns" / "pyo3_maturin.json"
WASM_JSON = DATA_DIR / "ecosystem-detection" / "special_patterns" / "wasm_binding.json"
BINDING_CSV = DATA_DIR / "analysis" / "binding" / "binding_named.csv"
PLATFORM_CSV = DATA_DIR / "analysis" / "platform-folders" / "platform_folders.csv"


# ============================================================================
# ECOSYSTEM SUFFIX PATTERNS (from analyze_pattern_correlation.py)
# ============================================================================
ECOSYSTEM_SUFFIX_PATTERNS = {
    'PyPI': [
        'python', 'py', 'pypi', 'python2', 'python3', 'py2', 'py3',
        'cpython', 'pysdk', 'pyclient', 'pylib'
    ],
    'Crates': [
        'rust', 'rs', 'cargo', 'rustlang', 'crate', 'crates'
    ],
    'Go': [
        'go', 'golang', 'goclient', 'gosdk', 'golib'
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
    ],
    'Other': [
        'net', 'dotnet', 'csharp', 'cs', 'fsharp', 'fs',
        'cpp', 'cxx', 'cplusplus', 'c', 'clang',
        'swift', 'swiftclient',
        'elixir', 'ex', 'exs',
        'dart', 'flutter',
        'perl', 'pl',
        'lua',
        'r', 'rlang',
        'haskell', 'hs',
        'ocaml', 'ml',
        'erlang', 'erl',
        'clojure', 'clj',
        'groovy', 'gvy'
    ]
}

# Flatten all suffixes
ECOSYSTEM_SUFFIXES = []
for ecosystem, suffixes in ECOSYSTEM_SUFFIX_PATTERNS.items():
    ECOSYSTEM_SUFFIXES.extend(suffixes)
seen = set()
ECOSYSTEM_SUFFIXES = [x for x in ECOSYSTEM_SUFFIXES if not (x in seen or seen.add(x))]

# Build reverse mapping: suffix -> ecosystem (excluding 'Other')
SUFFIX_TO_ECOSYSTEM = {}
for ecosystem, suffixes in ECOSYSTEM_SUFFIX_PATTERNS.items():
    if ecosystem == 'Other':
        continue
    for suffix in suffixes:
        SUFFIX_TO_ECOSYSTEM[suffix] = ecosystem


# ============================================================================
# URL NORMALIZATION
# ============================================================================

def normalize_github_url(url):
    """
    Normalize GitHub repository URLs to github.com/owner/repo format.
    Handles various URL formats (https, git+https, ssh, etc.).
    """
    if not url:
        return None
    if isinstance(url, float):
        return None
    url = str(url).strip().lower()
    if "github.com" not in url:
        return None

    url = re.sub(r"\.git$", "", url)
    url = re.sub(r"/$", "", url)
    url = url.replace('git+https://', 'https://')
    url = url.replace('git+ssh://', 'ssh://')
    url = url.replace('git://', 'https://')

    try:
        match = re.search(r"github\.com[:/]([^/]+/[^/\s]+)", url)
        if match:
            repo_path = match.group(1)
            repo_path = re.split(r"[\s#?]", repo_path)[0]
            repo_path = re.sub(r"\.git$", "", repo_path)
            repo_path = repo_path.rstrip('/')
            parts = repo_path.split('/')
            if len(parts) == 2 and parts[0] and parts[1]:
                return f"github.com/{repo_path}"
    except:
        pass
    return None


def normalize_url_simple(url):
    """Normalize a URL already close to github.com/owner/repo format."""
    if not url:
        return None
    url = str(url).lower().strip()
    if url.startswith('https://'):
        url = url[8:]
    elif url.startswith('http://'):
        url = url[7:]
    url = url.rstrip('/')
    if url.startswith('github.com/'):
        parts = url.split('/')
        if len(parts) >= 3 and parts[1] and parts[2]:
            return f"github.com/{parts[1]}/{parts[2]}"
    return None


# ============================================================================
# MULTIREPO GROUPING (from analyze_pattern_correlation.py)
# ============================================================================

def remove_ecosystem_patterns(repo_name):
    """
    Remove ecosystem patterns from a repository name, keeping separators.
    Used to normalize repo names for multirepo grouping.
    """
    if not repo_name:
        return None
    normalized = repo_name.lower()
    for suffix in ECOSYSTEM_SUFFIXES:
        pattern = r'(?:^|(?<=[-_.\s]))' + re.escape(suffix) + r'(?=[-_.\s]|$)'
        normalized = re.sub(pattern, '', normalized)
    if not normalized or len(normalized) < 2:
        return None
    return normalized


def get_multirepo_group_key(url):
    """
    Get group key for a multirepo URL by removing ecosystem patterns.
    Returns (owner, normalized_repo_name) tuple or None.
    """
    if not url:
        return None
    # Parse github.com/owner/repo
    parts = url.lower().split('/')
    if len(parts) < 3 or parts[0] != 'github.com':
        return None
    owner = parts[1]
    repo = parts[2]
    normalized_repo = remove_ecosystem_patterns(repo)
    if normalized_repo and normalized_repo != repo:
        return (owner, normalized_repo)
    return None


def detect_ecosystem_from_url(url):
    """
    Detect ecosystem from URL suffix.
    Returns list of detected ecosystems based on suffix patterns.
    """
    if not url:
        return []
    parts = url.lower().split('/')
    if len(parts) < 3:
        return []
    repo = parts[-1]
    ecosystems = []
    for suffix in ECOSYSTEM_SUFFIXES:
        pattern = r'(?:^|[-_.\s])' + re.escape(suffix) + r'(?:[-_.\s]|$)'
        if re.search(pattern, repo):
            eco = SUFFIX_TO_ECOSYSTEM.get(suffix)
            if eco and eco not in ecosystems:
                ecosystems.append(eco)
    return ecosystems


# ============================================================================
# DATA LOADING
# ============================================================================

def load_package_list_index():
    """
    Load all package CSV files and build lookup index.
    Returns dict: { normalized_url: { ecosystem: [package_name, ...] } }

    Multiple packages can map to the same repository URL, so we store lists.
    Uses both Repository URL and Homepage URL (as fallback) for normalization.
    """
    print("Loading package list index...")
    index = defaultdict(lambda: defaultdict(list))

    files = {
        "Maven": PACKAGE_LIST_PATH / "Maven.csv",
        "NPM": PACKAGE_LIST_PATH / "NPM.csv",
        "PyPI": PACKAGE_LIST_PATH / "PyPI.csv",
        "Crates": PACKAGE_LIST_PATH / "Crates.csv",
        "PHP": PACKAGE_LIST_PATH / "PHP.csv",
        "Ruby": PACKAGE_LIST_PATH / "Ruby.csv",
    }

    for ecosystem, filepath in files.items():
        if not filepath.exists():
            print(f"  Warning: {filepath} not found, skipping")
            continue
        print(f"  Loading {ecosystem} from {filepath.name}...")
        count = 0
        with open(filepath, 'r', encoding='utf-8', errors='replace') as f:
            reader = csv.DictReader(f)
            for row in reader:
                # Try Repository URL first, fallback to Homepage URL
                repo_url = normalize_github_url(row.get('Repository URL', ''))
                if not repo_url:
                    repo_url = normalize_github_url(row.get('Homepage URL', ''))
                if not repo_url:
                    continue

                pkg_name = row.get('Name', '')
                if pkg_name and pkg_name not in index[repo_url][ecosystem]:
                    index[repo_url][ecosystem].append(pkg_name)
                    count += 1
        print(f"    Indexed {count} packages")

    # Convert to regular dict
    result = {}
    for url, eco_dict in index.items():
        result[url] = dict(eco_dict)
    print(f"  Total unique URLs indexed: {len(result)}")
    return result


def load_github_metrics():
    """
    Load github_metrics.json.
    Returns dict: { normalized_url: metrics_entry }
    """
    print("Loading GitHub metrics...")
    with open(METRICS_PATH, 'r') as f:
        data = json.load(f)

    metrics = {}
    for key, entry in data['packages'].items():
        url = normalize_url_simple(entry.get('repo_url', ''))
        if url:
            metrics[url] = entry

    print(f"  Loaded {len(metrics)} entries")
    return metrics


def extract_github_metrics(entry):
    """Extract standardized github metrics from a metrics entry."""
    if not entry:
        return None
    return {
        "stars": entry.get("stars", 0),
        "forks": entry.get("forks", 0),
        "commits": entry.get("commits", 0),
        "pull_requests": entry.get("all_pull_requests", 0),
        "contributors": entry.get("contributors", 0),
        "issues": entry.get("all_issues", 0),
        "language_proportions": entry.get("language_proportions", "")
    }


def get_metrics_for_url(url, metrics_data):
    """Get standardized github metrics for a single URL."""
    entry = metrics_data.get(url)
    return extract_github_metrics(entry)


def lookup_package_names(url, claimed_ecosystems, pkg_index):
    """
    Look up package names for a URL in specified ecosystems.
    Returns dict: { ecosystem: [name, ...] }
    """
    url_pkgs = pkg_index.get(url, {})
    result = {}
    for eco in claimed_ecosystems:
        eco = eco.strip()
        if eco in url_pkgs:
            result[eco] = url_pkgs[eco]
    return result


def lookup_all_package_names(url, pkg_index):
    """Look up all package names across all ecosystems for a URL."""
    return pkg_index.get(url, {})


def parse_ecosystem_list(eco_str):
    """Parse comma-separated ecosystem string into sorted list."""
    if not eco_str:
        return []
    return [e.strip() for e in eco_str.split(',') if e.strip()]


# ============================================================================
# PATTERN 1: URL-NOT-MATCHED
# ============================================================================

def build_pattern_1(metrics_data, pkg_index):
    """
    Build Pattern 1: URL-Not-Matched.

    - Source: github_metrics.json entries with source == "multirepo"
    - Group URLs that belong to the same base project
    - Merge metrics by summing (except contributors: use contributors_unique_merged)
    - Claimed ecosystems: from package list lookup for each URL
    """
    print("\n" + "=" * 60)
    print("Pattern 1: URL-Not-Matched")
    print("=" * 60)

    # Collect all multirepo entries (exclude forked, archived, and error entries)
    multirepo_entries = {}
    excluded_counts = {"forked": 0, "archived": 0, "error": 0}
    for url, entry in metrics_data.items():
        if entry.get('source') == 'multirepo':
            if entry.get('is_fork'):
                excluded_counts["forked"] += 1
                continue
            if entry.get('is_archived'):
                excluded_counts["archived"] += 1
                continue
            if entry.get('error'):
                excluded_counts["error"] += 1
                continue
            multirepo_entries[url] = entry

    print(f"  Found {len(multirepo_entries)} valid multirepo URLs "
          f"(excluded: {excluded_counts['forked']} forked, "
          f"{excluded_counts['archived']} archived, "
          f"{excluded_counts['error']} errors)")

    # Group by base project using ecosystem suffix removal
    groups = defaultdict(list)
    ungrouped = []

    for url, entry in multirepo_entries.items():
        key = get_multirepo_group_key(url)
        if key:
            groups[key].append((url, entry))
        else:
            # Can't determine group key - treat as own group
            ungrouped.append((url, entry))

    for url, entry in ungrouped:
        groups[("__ungrouped__", url)].append((url, entry))

    print(f"  Grouped into {len(groups)} base projects ({len(ungrouped)} ungrouped)")

    results = []
    for group_key, urls_entries in groups.items():
        urls = [ue[0] for ue in urls_entries]
        entries = [ue[1] for ue in urls_entries]

        # Merge metrics: sum all except contributors
        merged_metrics = {
            "stars": sum(e.get("stars", 0) or 0 for e in entries),
            "forks": sum(e.get("forks", 0) or 0 for e in entries),
            "commits": sum(e.get("commits", 0) or 0 for e in entries),
            "pull_requests": sum(e.get("all_pull_requests", 0) or 0 for e in entries),
            "issues": sum(e.get("all_issues", 0) or 0 for e in entries),
            "language_proportions": "; ".join(
                f"{e.get('owner_repo', '')}: {e.get('language_proportions', '')}"
                for e in entries if e.get('language_proportions')
            ),
        }

        # Contributors: use contributors_unique_merged (consistent across group members)
        contributors_merged = None
        for e in entries:
            val = e.get("contributors_unique_merged")
            if val is not None:
                contributors_merged = val
                break
        if contributors_merged is not None:
            merged_metrics["contributors"] = contributors_merged
        else:
            merged_metrics["contributors"] = sum(
                e.get("contributors", 0) or 0 for e in entries
            )

        # Determine claimed ecosystems from package list lookup
        all_claimed = {}
        for url in urls:
            url_pkgs = lookup_all_package_names(url, pkg_index)
            for eco, names in url_pkgs.items():
                if eco not in all_claimed:
                    all_claimed[eco] = []
                for name in names:
                    if name not in all_claimed[eco]:
                        all_claimed[eco].append(name)

        claimed_ecosystems = sorted(all_claimed.keys())

        results.append({
            "repo_url": sorted(urls),
            "source": "github_metrics.json",
            "claimed_ecosystem": claimed_ecosystems,
            "claimed_ecosystem_package_name": all_claimed,
            "detected_ecosystem": None,
            "github_metrics": merged_metrics
        })

    print(f"  Output: {len(results)} entries")
    return results


# ============================================================================
# PATTERN 2: ECOSYSTEM SOURCE CODE NOT FOUND
# ============================================================================

def build_pattern_2(metrics_data, pkg_index):
    """
    Build Pattern 2: Ecosystem Source Code Not Found.

    - Source 1: fully_mismatched.csv
      All packages with ecosystem source files not found in their repositories
      claimed_ecosystem: from "Registered Ecosystems" CSV column
      detected_ecosystem: from "Detected Ecosystems" CSV column
    - Source 2: partially_matched.json
      Packages where some claimed ecosystems are missing from the repository
      claimed_ecosystem: from "claimed_ecosystems" JSON field
      detected_ecosystem: from "result_ecosystems" JSON field
    """
    print("\n" + "=" * 60)
    print("Pattern 2: Ecosystem Source Code Not Found")
    print("=" * 60)

    entries = {}  # normalized_url -> entry dict

    # --- Source 1: fully_mismatched.csv ---
    if not FULLY_MISMATCHED_CSV.exists():
        print(f"  Warning: {FULLY_MISMATCHED_CSV} not found")
    else:
        print(f"  Loading fully_mismatched.csv...")
        with open(FULLY_MISMATCHED_CSV, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            for row in reader:
                url = normalize_url_simple(row.get('Repository', ''))
                if not url:
                    continue

                if url not in entries:
                    claimed_eco = parse_ecosystem_list(row.get('Registered Ecosystems', ''))
                    detected_eco = parse_ecosystem_list(row.get('Detected Ecosystems', ''))
                    entries[url] = {
                        'url': url,
                        'source': 'fully_mismatched.csv',
                        'claimed_ecosystems': claimed_eco,
                        'detected_ecosystems': detected_eco,
                    }

        print(f"  Unique repos from fully_mismatched.csv: {len(entries)}")

    # --- Source 2: partially_matched.json ---
    if not PARTIALLY_MATCHED_JSON.exists():
        print(f"  Warning: {PARTIALLY_MATCHED_JSON} not found")
    else:
        print(f"  Loading partially_matched.json...")
        count_before = len(entries)
        with open(PARTIALLY_MATCHED_JSON, 'r', encoding='utf-8') as f:
            data = json.load(f)
        for pkg in data.get('packages', []):
            repo = pkg.get('repository', '')
            if not repo:
                continue
            url = normalize_url_simple(repo)
            if not url:
                continue
            if url not in entries:
                claimed_eco = sorted(pkg.get('claimed_ecosystems', []))
                detected_eco = sorted(pkg.get('result_ecosystems', []))
                entries[url] = {
                    'url': url,
                    'source': 'partially_matched.json',
                    'claimed_ecosystems': claimed_eco,
                    'detected_ecosystems': detected_eco,
                }
        print(f"  Unique repos added from partially_matched.json: {len(entries) - count_before}")

    print(f"  Total unique repos: {len(entries)}")

    results = []
    for url, info in entries.items():
        all_pkgs = lookup_all_package_names(url, pkg_index)
        gm = get_metrics_for_url(url, metrics_data)

        results.append({
            "repo_url": url,
            "source": info['source'],
            "claimed_ecosystem": info['claimed_ecosystems'],
            "claimed_ecosystem_package_name": all_pkgs,
            "detected_ecosystem": info['detected_ecosystems'],
            "github_metrics": gm,
        })

    print(f"  Output: {len(results)} entries")
    return results


# ============================================================================
# PATTERN 3: LANGUAGE-SPECIFICALLY-NAMED FOLDER
# ============================================================================

def build_pattern_3(metrics_data, pkg_index):
    """
    Build Pattern 3: Language-Specifically-Named Folder.

    - Sources: concentrated_complete.csv, concentrated_partial.csv,
      mixed.csv, low_coverage.csv
    - claimed_ecosystem: from package list CSV lookup
    - detected_ecosystem: from "detected_ecosystems" column in CSV
    - Deduplicate by URL across all source files
    """
    print("\n" + "=" * 60)
    print("Pattern 3: Language-Specifically-Named Folder")
    print("=" * 60)

    entries = {}  # normalized_url -> entry dict

    csv_files = [
        "concentrated_complete.csv",
        "concentrated_partial.csv",
        "mixed.csv",
        "low_coverage.csv",
    ]

    for csv_name in csv_files:
        csv_path = NAMING_CONVENTION_PATH / csv_name
        if not csv_path.exists():
            print(f"  Warning: {csv_path} not found")
            continue

        print(f"  Loading {csv_name}...")
        with open(csv_path, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            for row in reader:
                url = normalize_github_url(row.get('repo_url', ''))
                if not url:
                    continue

                if url not in entries:
                    detected_eco = parse_ecosystem_list(row.get('detected_ecosystems', ''))
                    entries[url] = {
                        'url': url,
                        'source': csv_name,
                        'detected_ecosystems': detected_eco,
                    }

    print(f"  Unique repos: {len(entries)}")

    results = []
    for url, info in entries.items():
        all_pkgs = lookup_all_package_names(url, pkg_index)
        claimed_eco = list(all_pkgs.keys())
        gm = get_metrics_for_url(url, metrics_data)

        results.append({
            "repo_url": url,
            "source": info['source'],
            "claimed_ecosystem": claimed_eco,
            "claimed_ecosystem_package_name": all_pkgs,
            "detected_ecosystem": info['detected_ecosystems'],
            "github_metrics": gm,
        })

    print(f"  Output: {len(results)} entries")
    return results


# ============================================================================
# PATTERN 4: PROTOCOL BUFFERS USAGE
# ============================================================================

def build_pattern_4(metrics_data, pkg_index):
    """
    Build Pattern 4: Protocol Buffers Usage.

    - Source: template_generation.json
    - claimed_ecosystem: from "claimed_ecosystems" field in JSON
    - detected_ecosystem: not available, set to null
    """
    print("\n" + "=" * 60)
    print("Pattern 4: Protocol Buffers Usage")
    print("=" * 60)

    with open(TEMPLATE_GEN_JSON, 'r') as f:
        data = json.load(f)

    packages = data.get('packages', [])
    print(f"  Loaded {len(packages)} packages")

    results = []
    for pkg in packages:
        url = pkg.get('repository', '')
        if not url:
            continue
        url = normalize_url_simple(url)
        if not url:
            continue

        claimed_list = pkg.get('claimed_ecosystems', [])
        pkg_names = lookup_package_names(url, claimed_list, pkg_index)
        gm = get_metrics_for_url(url, metrics_data)

        results.append({
            "repo_url": url,
            "source": "template_generation.json",
            "claimed_ecosystem": claimed_list,
            "claimed_ecosystem_package_name": pkg_names,
            "detected_ecosystem": None,
            "github_metrics": gm,
        })

    print(f"  Output: {len(results)} entries")
    return results


# ============================================================================
# PATTERN 5: BINDING/WRAPPER
# ============================================================================

def build_pattern_5(metrics_data, pkg_index):
    """
    Build Pattern 5: Binding/Wrapper.

    - JSON sources (jsii, pyo3/maturin, wasm): claimed_ecosystems from JSON,
      detected_ecosystem = null
    - CSV sources (binding_named, platform_folders): claimed_ecosystems and
      detected_ecosystems from CSV columns
    - Deduplicate by URL across all sources
    """
    print("\n" + "=" * 60)
    print("Pattern 5: Binding/Wrapper")
    print("=" * 60)

    entries = {}  # normalized_url -> entry dict

    # Load JSON files (no detected_ecosystems)
    for json_path, source_name in [
        (JSII_JSON, "jsii_binding.json"),
        (PYO3_JSON, "pyo3_maturin_binding.json"),
        (WASM_JSON, "wasm_binding.json"),
    ]:
        if not json_path.exists():
            print(f"  Warning: {json_path} not found, skipping")
            continue
        print(f"  Loading {source_name}...")
        with open(json_path, 'r') as f:
            data = json.load(f)

        pkg_count = 0
        for pkg in data.get('packages', []):
            url = normalize_url_simple(pkg.get('repository', ''))
            if not url:
                continue

            claimed_list = pkg.get('claimed_ecosystems', [])

            if url not in entries:
                entries[url] = {
                    'sources': [],
                    'claimed_ecosystems': claimed_list,
                    'detected_ecosystems': None,
                }

            if source_name not in entries[url]['sources']:
                entries[url]['sources'].append(source_name)
            pkg_count += 1
        print(f"    Loaded {pkg_count} packages")

    # Load CSV files (with detected_ecosystems)
    for csv_path, source_name in [
        (BINDING_CSV, "binding_named.csv"),
        (PLATFORM_CSV, "platform_folders.csv"),
    ]:
        if not csv_path.exists():
            print(f"  Warning: {csv_path} not found, skipping")
            continue
        print(f"  Loading {source_name}...")
        count = 0
        with open(csv_path, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            for row in reader:
                url = normalize_github_url(row.get('repo_url', ''))
                if not url:
                    continue

                claimed = parse_ecosystem_list(row.get('claimed_ecosystems', ''))
                detected = parse_ecosystem_list(row.get('detected_ecosystems', ''))

                if url not in entries:
                    entries[url] = {
                        'sources': [],
                        'claimed_ecosystems': claimed,
                        'detected_ecosystems': detected,
                    }
                else:
                    # Update detected ecosystems if not yet set (from JSON sources)
                    if entries[url]['detected_ecosystems'] is None and detected:
                        entries[url]['detected_ecosystems'] = detected

                if source_name not in entries[url]['sources']:
                    entries[url]['sources'].append(source_name)
                count += 1
        print(f"    Loaded {count} rows")

    print(f"  Unique repos: {len(entries)}")

    results = []
    for url, info in entries.items():
        claimed_list = info['claimed_ecosystems']
        detected = info['detected_ecosystems']

        pkg_names = lookup_package_names(url, claimed_list, pkg_index)
        gm = get_metrics_for_url(url, metrics_data)

        results.append({
            "repo_url": url,
            "source": info['sources'],
            "claimed_ecosystem": claimed_list,
            "claimed_ecosystem_package_name": pkg_names,
            "detected_ecosystem": detected,
            "github_metrics": gm,
        })

    print(f"  Output: {len(results)} entries")
    return results

# ============================================================================
# OUTPUT
# ============================================================================

def save_json(data, filepath, pattern_name):
    """Save pattern data as formatted JSON."""
    os.makedirs(filepath.parent, exist_ok=True)
    output = {
        "metadata": {
            "pattern": pattern_name,
            "total_entries": len(data),
            "generated": datetime.now().isoformat(),
        },
        "packages": data,
    }
    with open(filepath, 'w', encoding='utf-8') as f:
        json.dump(output, f, indent=2, ensure_ascii=False)
    print(f"  Saved to {filepath} ({len(data)} entries)")


def extract_urls_from_pattern(pattern_data):
    """
    Extract all unique URLs from a pattern's data.
    For Pattern 1 (multirepo), URLs are lists, so we flatten them.
    For other patterns, URLs are single strings.
    """
    urls = set()
    for entry in pattern_data:
        repo_url = entry.get('repo_url', '')
        if isinstance(repo_url, list):
            # Pattern 1 multirepo has lists of URLs
            urls.update(repo_url)
        elif repo_url:
            # Other patterns have single URLs
            urls.add(repo_url)
    return urls


def count_by_source(pattern_data):
    """Count entries by source file."""
    source_counts = defaultdict(int)
    for entry in pattern_data:
        source = entry.get('source', '')
        if isinstance(source, list):
            # Multiple sources for this entry
            for s in source:
                source_counts[s] += 1
        elif source:
            source_counts[source] += 1
    return dict(source_counts)


def count_by_ecosystem(pattern_data):
    """
    Count entries per ecosystem based on claimed_ecosystem field.
    An entry claiming multiple ecosystems is counted once per ecosystem.
    """
    eco_counts = defaultdict(int)
    for entry in pattern_data:
        claimed = entry.get('claimed_ecosystem', [])
        if isinstance(claimed, list):
            for eco in claimed:
                if eco:
                    eco_counts[eco] += 1
        elif claimed:
            eco_counts[claimed] += 1
    return dict(eco_counts)


def get_top_examples(pattern_data, n=2):
    """
    Return top-n entries from pattern_data ranked by star count (descending).
    Entries with no github_metrics or null stars are ranked last.
    """
    def star_key(entry):
        gm = entry.get('github_metrics')
        if not gm:
            return -1
        return gm.get('stars') or 0

    return sorted(pattern_data, key=star_key, reverse=True)[:n]


def format_example_block(example, rank):
    """
    Format a single example entry into readable lines.
    Returns a list of strings.
    """
    lines = []
    repo = example.get('repo_url', '')
    if isinstance(repo, list):
        repo_str = ', '.join(repo)
    else:
        repo_str = repo

    claimed = example.get('claimed_ecosystem', [])
    if isinstance(claimed, list):
        claimed_str = ', '.join(claimed) if claimed else '(none)'
    else:
        claimed_str = claimed or '(none)'

    pkg_names = example.get('claimed_ecosystem_package_name', {})
    pkg_str_parts = []
    for eco, names in pkg_names.items():
        if isinstance(names, list):
            pkg_str_parts.append(f"{eco}: {', '.join(names[:3])}{'...' if len(names) > 3 else ''}")
        else:
            pkg_str_parts.append(f"{eco}: {names}")
    pkg_str = '; '.join(pkg_str_parts) if pkg_str_parts else '(none)'

    gm = example.get('github_metrics') or {}
    stars        = gm.get('stars', 0) or 0
    forks        = gm.get('forks', 0) or 0
    commits      = gm.get('commits', 0) or 0
    prs          = gm.get('pull_requests', 0) or 0
    contributors = gm.get('contributors', 0) or 0
    issues       = gm.get('issues', 0) or 0

    lines.append(f"  [{rank}] {repo_str}")
    lines.append(f"      Ecosystems : {claimed_str}")
    lines.append(f"      Packages   : {pkg_str}")
    lines.append(f"      Stars: {stars:,}  Forks: {forks:,}  Commits: {commits:,}  "
                 f"PRs: {prs:,}  Contributors: {contributors:,}  Issues: {issues:,}")
    return lines


def generate_statistics(p1, p2, p3, p4, p5):
    """
    Generate statistics about patterns and overlaps.
    Returns a formatted string for output to summary.txt.
    """
    # Extract URLs from each pattern
    urls_p1 = extract_urls_from_pattern(p1)
    urls_p2 = extract_urls_from_pattern(p2)
    urls_p3 = extract_urls_from_pattern(p3)
    urls_p4 = extract_urls_from_pattern(p4)
    urls_p5 = extract_urls_from_pattern(p5)

    # Count by source for each pattern
    sources_p1 = count_by_source(p1)
    sources_p2 = count_by_source(p2)
    sources_p3 = count_by_source(p3)
    sources_p4 = count_by_source(p4)
    sources_p5 = count_by_source(p5)

    # Total unique URLs
    all_urls = urls_p1 | urls_p2 | urls_p3 | urls_p4 | urls_p5

    # Build URL -> patterns mapping for overlap analysis
    url_to_patterns = defaultdict(set)
    for url in urls_p1:
        url_to_patterns[url].add("P1")
    for url in urls_p2:
        url_to_patterns[url].add("P2")
    for url in urls_p3:
        url_to_patterns[url].add("P3")
    for url in urls_p4:
        url_to_patterns[url].add("P4")
    for url in urls_p5:
        url_to_patterns[url].add("P5")

    # Find overlaps
    overlapping_urls = {url: patterns for url, patterns in url_to_patterns.items() if len(patterns) > 1}

    # Count overlaps by pattern combination
    overlap_counts = defaultdict(int)
    for url, patterns in overlapping_urls.items():
        pattern_combo = ", ".join(sorted(patterns))
        overlap_counts[pattern_combo] += 1

    # Build output text
    lines = []
    lines.append("=" * 80)
    lines.append("CROSS-ECOSYSTEM PACKAGE CONSOLIDATION STATISTICS")
    lines.append("=" * 80)
    lines.append(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    lines.append("")

    lines.append("PATTERN SUMMARY")
    lines.append("-" * 80)
    lines.append(f"Pattern 1 (URL-Not-Matched):                      {len(p1):>6} entries  ({len(urls_p1):>6} unique repos)")
    lines.append(f"Pattern 2 (Source Code Not Found):                {len(p2):>6} entries  ({len(urls_p2):>6} unique repos)")
    lines.append(f"Pattern 3 (Language-Specifically-Named Folder):   {len(p3):>6} entries  ({len(urls_p3):>6} unique repos)")
    lines.append(f"Pattern 4 (Protocol Buffers Usage):               {len(p4):>6} entries  ({len(urls_p4):>6} unique repos)")
    lines.append(f"Pattern 5 (Binding/Wrapper):                      {len(p5):>6} entries  ({len(urls_p5):>6} unique repos)")
    lines.append("-" * 80)
    lines.append(f"Total entries across all patterns:  {len(p1)+len(p2)+len(p3)+len(p4)+len(p5):>6}")
    lines.append(f"Total unique repositories:          {len(all_urls):>6}")
    lines.append("")

    lines.append("OVERLAP ANALYSIS")
    lines.append("-" * 80)
    lines.append(f"Repositories appearing in only 1 pattern:  {len(all_urls) - len(overlapping_urls):>6}")
    lines.append(f"Repositories appearing in multiple patterns: {len(overlapping_urls):>6}")
    lines.append("")

    if overlap_counts:
        lines.append("Overlap Distribution:")
        lines.append("")
        # Sort by number of patterns (descending), then by combination name
        sorted_overlaps = sorted(overlap_counts.items(), 
                                key=lambda x: (-len(x[0].split(", ")), x[0]))
        for pattern_combo, count in sorted_overlaps:
            lines.append(f"  {pattern_combo:<30} {count:>6} repositories")
    
    lines.append("")
    lines.append("ECOSYSTEM DISTRIBUTION BY PATTERN")
    lines.append("-" * 80)

    eco_p1 = count_by_ecosystem(p1)
    eco_p2 = count_by_ecosystem(p2)
    eco_p3 = count_by_ecosystem(p3)
    eco_p4 = count_by_ecosystem(p4)
    eco_p5 = count_by_ecosystem(p5)

    # Collect all ecosystems across all patterns
    all_eco_keys = set(eco_p1) | set(eco_p2) | set(eco_p3) | set(eco_p4) | set(eco_p5)

    # Preferred display order; append any extras alphabetically
    preferred_order = ['PyPI', 'Crates', 'Maven', 'NPM', 'Ruby', 'PHP', 'Go']
    ordered_ecosystems = [e for e in preferred_order if e in all_eco_keys]
    ordered_ecosystems += sorted(e for e in all_eco_keys if e not in preferred_order)

    col_eco  = 18
    col_num  = 9
    header_cells = [f"{'Ecosystem':<{col_eco}}",
                    f"{'P1 (URL-Not-Matched)':>{col_num}}",
                    f"{'P2 (Src-Not-Found)':>{col_num}}",
                    f"{'P3 (Lang-Folder)':>{col_num}}",
                    f"{'P4 (Proto-Buf)':>{col_num}}",
                    f"{'P5 (Binding)':>{col_num}}",
                    f"{'Total':>{col_num}}"]
    header_row = "  ".join(header_cells)
    separator  = "-" * len(header_row)

    lines.append(header_row)
    lines.append(separator)

    for eco in ordered_ecosystems:
        c1 = eco_p1.get(eco, 0)
        c2 = eco_p2.get(eco, 0)
        c3 = eco_p3.get(eco, 0)
        c4 = eco_p4.get(eco, 0)
        c5 = eco_p5.get(eco, 0)
        total = c1 + c2 + c3 + c4 + c5
        row = "  ".join([
            f"{eco:<{col_eco}}",
            f"{c1:>{col_num}}",
            f"{c2:>{col_num}}",
            f"{c3:>{col_num}}",
            f"{c4:>{col_num}}",
            f"{c5:>{col_num}}",
            f"{total:>{col_num}}",
        ])
        lines.append(row)

    # Totals row
    t1 = sum(eco_p1.values())
    t2 = sum(eco_p2.values())
    t3 = sum(eco_p3.values())
    t4 = sum(eco_p4.values())
    t5 = sum(eco_p5.values())
    lines.append(separator)
    lines.append("  ".join([
        f"{'Total':<{col_eco}}",
        f"{t1:>{col_num}}",
        f"{t2:>{col_num}}",
        f"{t3:>{col_num}}",
        f"{t4:>{col_num}}",
        f"{t5:>{col_num}}",
        f"{t1+t2+t3+t4+t5:>{col_num}}",
    ]))
    lines.append("")
    lines.append("Note: entries claiming multiple ecosystems are counted once per ecosystem.")
    lines.append("")

    lines.append("TOP EXAMPLES BY PATTERN (ranked by star count)")
    lines.append("-" * 80)
    for pat_label, pat_data in [
        ("Pattern 1: URL-Not-Matched",                  p1),
        ("Pattern 2: Ecosystem Source Code Not Found",   p2),
        ("Pattern 3: Language-Specifically-Named Folder", p3),
        ("Pattern 4: Protocol Buffers Usage",            p4),
        ("Pattern 5: Binding/Wrapper",                   p5),
    ]:
        lines.append(pat_label)
        examples = get_top_examples(pat_data, n=2)
        if examples:
            for rank, ex in enumerate(examples, start=1):
                lines.extend(format_example_block(ex, rank))
        else:
            lines.append("  (no data)")
        lines.append("")

    lines.append("DATA SOURCES")
    lines.append("-" * 80)
    lines.append("Pattern 1: URL-Not-Matched")
    lines.append("  - github_metrics.json (entries with source == 'multirepo')")
    if sources_p1:
        for source, count in sorted(sources_p1.items()):
            lines.append(f"    └─ {source}: {count} entries")
    lines.append("")
    lines.append("Pattern 2: Ecosystem Source Code Not Found")
    lines.append("  - fully_mismatched.csv")
    if sources_p2:
        for source, count in sorted(sources_p2.items()):
            lines.append(f"    └─ {source}: {count} entries")
    lines.append("")
    lines.append("Pattern 3: Language-Specifically-Named Folder")
    lines.append("  - concentrated_complete.csv")
    lines.append("  - concentrated_partial.csv")
    lines.append("  - mixed.csv")
    lines.append("  - low_coverage.csv")
    if sources_p3:
        for source, count in sorted(sources_p3.items()):
            lines.append(f"    └─ {source}: {count} entries")
    else:
        lines.append("    └─ (no entries found)")
    lines.append("")
    lines.append("Pattern 4: Protocol Buffers Usage")
    lines.append("  - template_generation.json")
    if sources_p4:
        for source, count in sorted(sources_p4.items()):
            lines.append(f"    └─ {source}: {count} entries")
    lines.append("")
    lines.append("Pattern 5: Binding/Wrapper")
    lines.append("  - jsii_binding.json")
    lines.append("  - pyo3_maturin_binding.json")
    lines.append("  - wasm_binding.json (WARNING: file not found)")
    lines.append("  - binding_named.csv")
    lines.append("  - platform_folders.csv")
    if sources_p5:
        total_source_entries = sum(sources_p5.values())
        lines.append(f"    Total source entries: {total_source_entries} ({total_source_entries - len(p5)} duplicates removed)")
        for source, count in sorted(sources_p5.items(), key=lambda x: -x[1]):
            lines.append(f"    └─ {source}: {count} entries")
    lines.append("")
    lines.append("PATTERN DESCRIPTIONS")
    lines.append("-" * 80)
    lines.append("P1: URL-Not-Matched - Separate repositories for each ecosystem (e.g., owner/project-js, owner/project-py)")
    lines.append("P2: Source Code Not Found - Packages with ecosystem source files not found in repositories")
    lines.append("P3: Language-Specifically-Named Folder - Projects with ecosystem-specific folders (npm/, python/, etc.)")
    lines.append("P4: Protocol Buffers Usage - Projects using template files (.proto, .thrift, .fbs)")
    lines.append("P5: Binding/Wrapper - Projects with language bindings (jsii, PyO3, WASM, etc.)")
    lines.append("=" * 80)

    return "\n".join(lines)


def save_statistics(stats_text, filepath):
    """Save statistics to a text file."""
    os.makedirs(filepath.parent, exist_ok=True)
    with open(filepath, 'w', encoding='utf-8') as f:
        f.write(stats_text)
    print(f"  Saved statistics to {filepath}")


# ============================================================================
# MAIN
# ============================================================================

def main():
    print("=" * 60)
    print("Consolidating Cross-Ecosystem Package Patterns")
    print("=" * 60)

    # Load shared data
    pkg_index = load_package_list_index()
    metrics_data = load_github_metrics()

    # Build each pattern
    p1 = build_pattern_1(metrics_data, pkg_index)
    save_json(p1, OUTPUT_PATH / "pattern_1_url_not_matched.json", "URL-Not-Matched")

    p2 = build_pattern_2(metrics_data, pkg_index)
    save_json(p2, OUTPUT_PATH / "pattern_2_source_code_not_found.json", "Ecosystem Source Code Not Found")

    p3 = build_pattern_3(metrics_data, pkg_index)
    save_json(p3, OUTPUT_PATH / "pattern_3_language_specific_folder.json", "Language-Specifically-Named Folder")

    p4 = build_pattern_4(metrics_data, pkg_index)
    save_json(p4, OUTPUT_PATH / "pattern_4_protocol_buffers.json", "Protocol Buffers Usage")

    p5 = build_pattern_5(metrics_data, pkg_index)
    save_json(p5, OUTPUT_PATH / "pattern_5_binding_wrapper.json", "Binding/Wrapper")

    # Generate and save statistics
    print("\n" + "=" * 60)
    print("Generating Statistics")
    print("=" * 60)
    stats_text = generate_statistics(p1, p2, p3, p4, p5)
    save_statistics(stats_text, OUTPUT_PATH / "summary.txt")

    # Summary
    print("\n" + "=" * 60)
    print("Summary")
    print("=" * 60)
    print(f"  Pattern 1 (URL-Not-Matched):             {len(p1):>6} entries")
    print(f"  Pattern 2 (Source Code Not Found):       {len(p2):>6} entries")
    print(f"  Pattern 3 (Language-Specific Folder):    {len(p3):>6} entries")
    print(f"  Pattern 4 (Protocol Buffers Usage):      {len(p4):>6} entries")
    print(f"  Pattern 5 (Binding/Wrapper):             {len(p5):>6} entries")
    print(f"  Total:                                   {len(p1)+len(p2)+len(p3)+len(p4)+len(p5):>6} entries")
    print(f"\n  Output directory: {OUTPUT_PATH}")
    print("=" * 60)


if __name__ == "__main__":
    main()
