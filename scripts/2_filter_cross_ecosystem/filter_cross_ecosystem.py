#!/usr/bin/env python3
"""
Script to identify cross-ecosystem packages across Maven, NPM, PyPI, Crates, PHP, and Ruby.

Process:
1. Detects and filters out multirepo patterns (e.g., owner/project-js, owner/project-py)
2. Identifies genuine cross-ecosystem packages based on exact GitHub repository URL matching

Results are categorized by ecosystem count (2 ecosystems, 3 ecosystems, etc.)
and a summary file is generated.
"""

import pandas as pd
import re
import json
from pathlib import Path
from tqdm import tqdm
from collections import defaultdict
from multiprocessing import Pool, cpu_count


# ============================================================================
# PATH CONFIGURATION
# ============================================================================
DATA_DIR = Path(__file__).resolve().parent.parent.parent / "data"

# Input path: Location of the source package CSV files
INPUT_BASE_PATH = DATA_DIR / "package-lists"

# Output path: Location where results will be saved
OUTPUT_RESULTS_PATH = DATA_DIR / "cross-ecosystem-filter"

# ============================================================================


def normalize_github_url(url):
    """
    Normalize GitHub repository URLs to a standard format for comparison.
    Returns None if URL is invalid, empty, or not a GitHub URL.
    """
    if pd.isna(url) or not url or url.strip() == "":
        return None

    url = str(url).strip().lower()

    # Check if it's a GitHub URL
    if "github.com" not in url:
        return None

    # Remove common suffixes and prefixes
    url = re.sub(r"\.git$", "", url)
    url = re.sub(r"/$", "", url)
    
    # Remove git protocol prefixes
    url = url.replace('git+https://', 'https://')
    url = url.replace('git+ssh://', 'ssh://')
    url = url.replace('git://', 'https://')

    # Extract path from URL
    try:
        # Handle various GitHub URL formats (https, ssh, git@)
        match = re.search(r"github\.com[:/]([^/]+/[^/\s]+)", url)
        if match:
            repo_path = match.group(1)
            # Remove trailing content after repository name
            repo_path = re.split(r"[\s#?]", repo_path)[0]
            # Remove .git suffix if still present in the extracted path
            repo_path = re.sub(r"\.git$", "", repo_path)
            # Remove trailing slash
            repo_path = repo_path.rstrip('/')
            
            # Validate that we have both owner and repo
            parts = repo_path.split('/')
            if len(parts) == 2 and parts[0] and parts[1]:
                return f"github.com/{repo_path}"
    except:
        pass

    return None


def normalize_github_url_with_fallback(repo_url, homepage_url):
    """
    Normalize GitHub URL with Homepage URL as fallback.
    First tries Repository URL, if that fails, tries Homepage URL.
    Returns None if neither contains a valid GitHub URL.
    """
    # Try Repository URL first
    normalized = normalize_github_url(repo_url)
    if normalized:
        return normalized
    
    # Fallback to Homepage URL
    normalized = normalize_github_url(homepage_url)
    return normalized


# ==============================================================================
# ECOSYSTEM SUFFIX PATTERNS (Same as detect_multirepo.py)
# ==============================================================================

# Ecosystem suffixes organized by package manager/ecosystem
# These are used to detect multi-repo patterns like: project-python, project-rust, etc.
ECOSYSTEM_SUFFIX_PATTERNS = {
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

# Flatten all suffixes into a single list
ECOSYSTEM_SUFFIXES = []
for ecosystem, suffixes in ECOSYSTEM_SUFFIX_PATTERNS.items():
    ECOSYSTEM_SUFFIXES.extend(suffixes)

# Remove duplicates while preserving order
seen = set()
ECOSYSTEM_SUFFIXES = [x for x in ECOSYSTEM_SUFFIXES if not (x in seen or seen.add(x))]

def remove_ecosystem_patterns(repo_name):
    """
    Remove only ecosystem patterns from a repository name, keeping separators intact.
    
    This is used to normalize repo names for comparison.
    If two repos become identical after removing patterns (including separators), 
    they're likely multirepo.
    
    Examples:
        - 'libsql-js' -> 'libsql-'
        - 'libsql-python' -> 'libsql-'
        - 'libsql.php' -> 'libsql.'
        - 'repo-js' and 'repo-python' match (both -> 'repo-')
        - 'repo.php' doesn't match with above (-> 'repo.')
    
    Args:
        repo_name: Repository name (last part of owner/repo)
    
    Returns:
        Name with ecosystem patterns removed but separators preserved
    """
    if not repo_name:
        return None
    
    normalized = repo_name.lower()
    
    # Remove all ecosystem patterns, but keep separators intact
    for suffix in ECOSYSTEM_SUFFIXES:
        # Remove pattern with any common separator or at boundaries
        # This handles: -suffix, _suffix, .suffix, suffix-, suffix_, suffix.
        pattern = r'(?:^|(?<=[-_.\s]))' + re.escape(suffix) + r'(?=[-_.\s]|$)'
        normalized = re.sub(pattern, '', normalized)
    
    # Return None if nothing meaningful remains
    if not normalized or len(normalized) < 2:
        return None
    
    return normalized


def analyze_owner_repos(owner_data):
    """
    Analyze repos for a single owner to detect multirepo patterns.
    This function is designed to be run in parallel.
    
    Args:
        owner_data: Tuple of (owner_name, list_of_repo_info_dicts)
    
    Returns:
        Tuple of (multirepo_groups_found, multirepo_urls_found)
    """
    owner, repos = owner_data
    
    # Only check owners with multiple repos
    if len(repos) < 2:
        return [], set()
    
    # Group repos by normalized name (after removing ecosystem patterns)
    normalized_groups = defaultdict(list)
    
    for repo_info in repos:
        repo_name = repo_info['repo']
        normalized = remove_ecosystem_patterns(repo_name)
        
        # Only consider repos where we removed something (i.e., had ecosystem patterns)
        if normalized and normalized != repo_name.lower():
            repo_info['normalized'] = normalized
            normalized_groups[normalized].append(repo_info)
    
    # Check each normalized group for multirepo pattern
    multirepo_groups = []
    multirepo_urls = set()
    
    for normalized, group_repos in normalized_groups.items():
        # Get unique ecosystems in this group
        ecosystems = set(r['ecosystem'] for r in group_repos)
        
        # Get unique repository URLs in this group
        unique_urls = set(r['url'] for r in group_repos)
        
        # Multirepo pattern: repos that become identical after removing ecosystem patterns,
        # and they're in different ecosystems, AND they have different repository URLs
        # (if all repos have the same URL, it's a monorepo, not a multirepo)
        if len(ecosystems) >= 2 and len(group_repos) >= 2 and len(unique_urls) >= 2:
            multirepo_groups.append({
                'owner': owner,
                'normalized': normalized,
                'repos': group_repos,
                'ecosystem_count': len(ecosystems)
            })
            
            # Add all URLs in this group to the set
            for repo in group_repos:
                multirepo_urls.add(repo['url'])
    
    return multirepo_groups, multirepo_urls


def detect_multirepo_patterns(packages):
    """
    Detect multirepo patterns across all ecosystems.
    
    Steps:
    1. Detect ecosystem patterns in repo names
    2. Compare if ecosystem pattern is the only thing different
    
    If multiple repos from the same owner become identical after removing
    ecosystem patterns, and they're in different ecosystems, it's a multirepo.
    
    Args:
        packages: Dictionary mapping ecosystem name to DataFrame
    
    Returns:
        Set of normalized URLs that are part of multirepo patterns
    """
    print("\n" + "=" * 80)
    print("Detecting multirepo patterns...")
    print("=" * 80)
    
    # Step 1: Group by owner
    print("  Step 1: Grouping by owner...")
    owner_repos = defaultdict(list)
    
    for ecosystem, df in packages.items():
        for _, row in tqdm(
            df.iterrows(), 
            total=len(df), 
            desc=f"    Processing {ecosystem}",
            leave=False
        ):
            url = row['normalized_repo']
            if pd.isna(url) or not url or not isinstance(url, str):
                continue
            
            # Extract owner and repo name from normalized URL
            # Format: github.com/owner/repo
            parts = url.split('/')
            if len(parts) >= 3:
                owner = parts[1].lower()
                repo = parts[2].lower()
                
                owner_repos[owner].append({
                    'url': url,
                    'ecosystem': ecosystem,
                    'repo': repo,
                    'owner': owner
                })
    
    print(f"    Found {len(owner_repos)} unique owners")
    
    # Step 2: Detect ecosystem patterns and compare (using parallel processing)
    print("  Step 2: Detecting ecosystem patterns and comparing...")
    
    # Determine number of CPU cores to use
    num_cores = max(1, cpu_count() - 1)  # Leave one core free
    print(f"    Using {num_cores} CPU cores for parallel processing")
    
    multirepo_urls = set()
    multirepo_groups = []
    
    # Convert dict items to list for parallel processing
    owner_items = list(owner_repos.items())
    
    # Process owners in parallel
    with Pool(processes=num_cores) as pool:
        # Use imap_unordered for progress bar support
        results = list(tqdm(
            pool.imap_unordered(analyze_owner_repos, owner_items, chunksize=100),
            total=len(owner_items),
            desc="    Analyzing owners",
            leave=False
        ))
    
    # Aggregate results from all processes
    for groups, urls in results:
        multirepo_groups.extend(groups)
        multirepo_urls.update(urls)
    
    # Report findings
    print(f"\n  Found {len(multirepo_groups)} multirepo patterns")
    print(f"  Total packages in multirepo patterns: {len(multirepo_urls)}")
    
    # Show some examples
    if multirepo_groups:
        print("\n  Example multirepo patterns (top 10 by ecosystem count):")
        for group in sorted(multirepo_groups, key=lambda x: x['ecosystem_count'], reverse=True)[:10]:
            ecosystems_str = ', '.join(sorted(set(r['ecosystem'] for r in group['repos'])))
            repos_list = sorted(set(r['repo'] for r in group['repos']))
            repos_str = ', '.join(repos_list[:5])
            if len(repos_list) > 5:
                repos_str += f" (and {len(repos_list) - 5} more)"
            
            print(f"    {group['owner']}/{group['normalized']}:")
            print(f"      Repos: {repos_str}")
            print(f"      Ecosystems ({group['ecosystem_count']}): {ecosystems_str}")
    
    return multirepo_groups, multirepo_urls


def load_package_data(base_path):
    """Load all package CSV files and create normalized lookup structures."""

    packages = {}

    # Define file paths
    files = {
        "Maven": [base_path / "Maven.csv"],
        "NPM": [base_path / "NPM.csv"],
        "PyPI": [base_path / "PyPI.csv"],
        "Crates": [base_path / "Crates.csv"],
        "PHP": [base_path / "PHP.csv"],
        "Ruby": [base_path / "Ruby.csv"],
    }

    print("Loading package data...")

    for ecosystem, filepaths in files.items():
        print(f"  Loading {ecosystem}...")

        # Load and combine multiple files if necessary (e.g., Go parts)
        dfs = []
        for filepath in filepaths:
            if filepath.exists():
                df_part = pd.read_csv(filepath, low_memory=False)
                dfs.append(df_part)
            else:
                print(f"    Warning: {filepath} not found, skipping...")

        if not dfs:
            print(f"    Error: No files found for {ecosystem}, skipping...")
            continue

        # Combine all parts into one DataFrame
        df = pd.concat(dfs, ignore_index=True)

        # Add normalized columns with progress bar
        # Use Homepage URL as fallback when Repository URL is missing
        tqdm.pandas(desc=f"    Normalizing {ecosystem} repos", leave=False)
        df["normalized_repo"] = df.progress_apply(
            lambda row: normalize_github_url_with_fallback(
                row["Repository URL"], row["Homepage URL"]
            ),
            axis=1
        )

        packages[ecosystem] = df
        print(f"    Loaded {len(df)} packages")

    return packages


def build_lookup_index(df, ecosystem_name, multirepo_urls=None, include_all=False):
    """
    Build efficient lookup indices for a package DataFrame.
    Returns a dictionary mapping normalized repo URLs to package data.
    
    Args:
        df: DataFrame containing package data
        ecosystem_name: Name of the ecosystem
        multirepo_urls: Set of URLs to exclude (multirepo patterns)
        include_all: If True, include all packages (don't filter multirepo)
    
    Returns:
        Dictionary mapping normalized repo URLs to package data
    """
    lookup = {}
    filtered_count = 0

    for _, row in tqdm(
        df.iterrows(), total=len(df), desc=f"    Indexing {ecosystem_name}", leave=False
    ):
        repo = row["normalized_repo"]

        # Only index packages with valid repo (must not be None, empty, or NaN)
        if repo and pd.notna(repo) and isinstance(repo, str) and repo.strip():
            # Skip multirepo patterns unless include_all is True
            if not include_all and multirepo_urls and repo in multirepo_urls:
                filtered_count += 1
                continue
            
            # Store package data with repo as key
            lookup[repo] = {
                "ID": row["ID"],
                "Name": row["Name"],
                "Homepage": row["Homepage URL"],
                "Repo": row["Repository URL"],
            }
    
    if filtered_count > 0:
        print(f"      Filtered out {filtered_count} multirepo packages")

    return lookup


def find_cross_ecosystem_packages(lookups):
    """
    Find packages that appear in multiple ecosystems by checking each unique URL
    against all ecosystem lookups.

    Args:
        lookups: Dictionary of lookup indices by ecosystem

    Returns:
        List of cross-ecosystem package dictionaries
    """
    # Collect all unique normalized URLs across all ecosystems
    all_urls = set()
    for ecosystem_lookup in lookups.values():
        all_urls.update(ecosystem_lookup.keys())
    
    print(f"  Total unique URLs to check: {len(all_urls):,}")
    
    cross_ecosystem_packages = []
    
    # For each URL, check which ecosystems contain it
    for url in tqdm(all_urls, desc="  Finding cross-ecosystem packages", leave=False):
        package_info = {
            "normalized_url": url,
            "ecosystems": [],
            "packages": {}
        }
        
        # Check each ecosystem for this URL
        for ecosystem, lookup in lookups.items():
            if url in lookup:
                data = lookup[url]
                package_info["ecosystems"].append(ecosystem)
                package_info["packages"][ecosystem] = {
                    "id": data["ID"],
                    "name": data["Name"],
                    "homepage_url": data["Homepage"],
                    "repository_url": data["Repo"],
                }
        
        # Only include if URL appears in 2+ ecosystems
        if len(package_info["ecosystems"]) >= 2:
            package_info["ecosystems"] = sorted(package_info["ecosystems"])
            package_info["ecosystem_count"] = len(package_info["ecosystems"])
            cross_ecosystem_packages.append(package_info)
    
    return cross_ecosystem_packages


def main():
    """Main execution function."""

    # Set up paths (using configuration at top of file)
    base_path = INPUT_BASE_PATH
    results_path = OUTPUT_RESULTS_PATH
    results_path.mkdir(parents=True, exist_ok=True)

    print("=" * 80)
    print("Cross-Ecosystem Package Analysis")
    print("=" * 80)

    # Load all package data
    packages = load_package_data(base_path)

    # Calculate input statistics
    total_input_packages = 0
    all_valid_repos = set()
    ecosystem_package_counts = {}
    ecosystem_valid_repo_counts = {}
    for ecosystem, df in packages.items():
        total_input_packages += len(df)
        ecosystem_package_counts[ecosystem] = len(df)
        # valid repos only
        repos = df["normalized_repo"].dropna()
        all_valid_repos.update(repos)
        # Count valid repos per ecosystem (packages with normalized_repo)
        ecosystem_valid_repo_counts[ecosystem] = df["normalized_repo"].notna().sum()
    
    valid_packages_count = len(all_valid_repos)

    # Get list of available ecosystems
    ecosystems = sorted(packages.keys())
    print(f"\nAvailable ecosystems: {', '.join(ecosystems)}")

    # Detect multirepo patterns
    multirepo_groups, multirepo_urls = detect_multirepo_patterns(packages)

    # Build comprehensive lookup indices for ALL packages (used to build the filtered monorepo lookups)
    print("\n" + "=" * 80)
    print("Building lookup indices (all packages)...")
    print("=" * 80)
    all_packages_lookups = {}
    for ecosystem, df in packages.items():
        print(f"  Building index for {ecosystem}...")
        all_packages_lookups[ecosystem] = build_lookup_index(df, ecosystem, include_all=True)
        print(f"    Indexed {len(all_packages_lookups[ecosystem])} packages with valid repo")

    # Create filtered lookups (excluding multirepo patterns) for monorepo detection
    print("\n" + "=" * 80)
    print("Filtering out multirepo patterns for monorepo detection...")
    print("=" * 80)
    lookups = {}
    for ecosystem in ecosystems:
        lookups[ecosystem] = {
            url: data for url, data in all_packages_lookups[ecosystem].items()
            if url not in multirepo_urls
        }
        filtered_count = len(all_packages_lookups[ecosystem]) - len(lookups[ecosystem])
        print(f"  {ecosystem}: {len(lookups[ecosystem])} packages (filtered out {filtered_count} multirepo)")

    print("\n" + "=" * 80)
    print("Finding cross-ecosystem packages...")
    print("=" * 80)

    # Find all cross-ecosystem packages in one pass (monorepo only)
    monorepo_packages = find_cross_ecosystem_packages(lookups)
    
    print(f"\n  Found {len(monorepo_packages):,} monorepo cross-ecosystem packages")
    
    # cross_ecosystem_packages contains only monorepo packages;
    # multirepo packages are represented as a flat URL list in the output
    cross_ecosystem_packages = monorepo_packages
    cross_ecosystem_packages.sort(key=lambda x: (-x["ecosystem_count"], x.get("normalized_url", "")))

    # Calculate statistics
    monorepo_cross_ecosystem = len(monorepo_packages)
    monorepo_percentage = (monorepo_cross_ecosystem / valid_packages_count * 100) if valid_packages_count > 0 else 0
    
    multirepo_count = len(multirepo_urls)
    multirepo_percentage = (multirepo_count / valid_packages_count * 100) if valid_packages_count > 0 else 0
    
    total_cross_ecosystem = monorepo_cross_ecosystem + multirepo_count
    total_cross_ecosystem_percentage = (total_cross_ecosystem / valid_packages_count * 100) if valid_packages_count > 0 else 0

    # Calculate per-ecosystem statistics
    print("\n" + "=" * 80)
    print("Calculating per-ecosystem statistics...")
    print("=" * 80)
    
    ecosystem_stats = {}
    for ecosystem in ecosystems:
        total_packages_loaded = ecosystem_package_counts[ecosystem]
        total_packages_with_repo = ecosystem_valid_repo_counts[ecosystem]
        
        # Count monorepo packages that include this ecosystem
        ecosystem_monorepo_count = sum(
            1 for pkg in monorepo_packages 
            if ecosystem in pkg["ecosystems"]
        )
        
        # Count multirepo groups that include this ecosystem
        ecosystem_multirepo_count = sum(
            1 for group in multirepo_groups
            if any(r['ecosystem'] == ecosystem for r in group['repos'])
        )
        
        # Total cross-ecosystem packages for this ecosystem (monorepo + multirepo)
        cross_ecosystem_count = ecosystem_monorepo_count + ecosystem_multirepo_count
        
        percentage = (cross_ecosystem_count / total_packages_with_repo * 100) if total_packages_with_repo > 0 else 0
        
        ecosystem_stats[ecosystem] = {
            "total_packages": total_packages_loaded,
            "valid_repos": int(total_packages_with_repo),
            "cross_ecosystem_packages": cross_ecosystem_count,
            "monorepo_packages": ecosystem_monorepo_count,
            "multirepo_packages": ecosystem_multirepo_count,
            "percentage": round(percentage, 2)
        }
        
        print(f"  {ecosystem}: {cross_ecosystem_count}/{total_packages_with_repo} ({percentage:.2f}%) [Monorepo: {ecosystem_monorepo_count}, Multirepo: {ecosystem_multirepo_count}]")

    # Calculate statistics by ecosystem count (monorepo packages)
    stats_by_count = {}
    for pkg in cross_ecosystem_packages:
        count = str(pkg["ecosystem_count"])
        if count not in stats_by_count:
            stats_by_count[count] = {"package_count": 0}
        stats_by_count[count]["package_count"] += 1
    
    # Include multirepo groups in the ecosystem count statistics
    # Count individual URLs (not groups) to match the multirepo_urls flat count
    for group in multirepo_groups:
        eco_count = str(len(set(r['ecosystem'] for r in group['repos'])))
        if eco_count not in stats_by_count:
            stats_by_count[eco_count] = {"package_count": 0}
        stats_by_count[eco_count]["package_count"] += len(set(r['url'] for r in group['repos']))

    # Calculate ecosystem count matrix (ecosystem × number of ecosystems)
    print("\n" + "=" * 80)
    print("Calculating ecosystem count matrix...")
    print("=" * 80)
    
    ecosystem_count_matrix = {eco: {} for eco in ecosystems}
    
    # Find max ecosystem count across both monorepo packages and multirepo groups
    max_ecosystem_count_mono = max((pkg["ecosystem_count"] for pkg in cross_ecosystem_packages), default=0)
    max_ecosystem_count_multi = max(
        (len(set(r['ecosystem'] for r in group['repos'])) for group in multirepo_groups),
        default=0
    )
    max_ecosystem_count = max(max_ecosystem_count_mono, max_ecosystem_count_multi)
    
    # Initialize all counts to 0
    for ecosystem in ecosystems:
        for count in range(2, max_ecosystem_count + 1):
            ecosystem_count_matrix[ecosystem][count] = 0
    
    # Count monorepo packages by ecosystem and ecosystem_count
    for pkg in cross_ecosystem_packages:
        eco_count = pkg["ecosystem_count"]
        for ecosystem in pkg["ecosystems"]:
            ecosystem_count_matrix[ecosystem][eco_count] += 1
    
    # Count multirepo groups by ecosystem and ecosystem_count
    for group in multirepo_groups:
        group_ecosystems = sorted(set(r['ecosystem'] for r in group['repos']))
        eco_count = len(group_ecosystems)
        for ecosystem in group_ecosystems:
            if ecosystem in ecosystem_count_matrix:
                ecosystem_count_matrix[ecosystem][eco_count] += 1
    
    print("  Ecosystem count matrix calculated")

    # Build final JSON output
    output_data = {
        "metadata": {
            "description": "Cross-ecosystem package analysis results",
            "ecosystems_analyzed": ecosystems,
            "input_statistics": {
                "total_packages_inputted": total_input_packages,
                "total_valid_repositories": valid_packages_count,
                "packages_per_ecosystem": ecosystem_package_counts
            },
            "cross_ecosystem_summary": {
                "total_cross_ecosystem_packages": total_cross_ecosystem,
                "total_cross_ecosystem_percentage": round(total_cross_ecosystem_percentage, 2),
                "monorepo_packages": {
                    "count": monorepo_cross_ecosystem,
                    "percentage": round(monorepo_percentage, 2),
                    "description": "Same repository published to multiple package ecosystems (exact URL matches)"
                },
                "multirepo_packages": {
                    "count": multirepo_count,
                    "percentage": round(multirepo_percentage, 2),
                    "description": "Same project split into language-specific repositories (e.g., owner/project-js, owner/project-py)"
                }
            },
            "per_ecosystem_statistics": ecosystem_stats,
            "statistics_by_ecosystem_count": stats_by_count
        },
        "monorepo_packages": cross_ecosystem_packages,
        "multirepo_urls": sorted(multirepo_urls)
    }

    # Save to JSON file
    output_json_path = results_path / "cross_ecosystem_packages.json"
    with open(output_json_path, 'w', encoding='utf-8') as f:
        json.dump(output_data, f, indent=2, ensure_ascii=False)

    # Save summary.txt with comprehensive statistics
    summary_txt_path = results_path / "summary.txt"
    with open(summary_txt_path, "w") as f:
        f.write("=" * 80 + "\n")
        f.write("CROSS-ECOSYSTEM PACKAGE ANALYSIS SUMMARY\n")
        f.write("=" * 80 + "\n\n")

        f.write("INPUT STATISTICS\n")
        f.write("-" * 80 + "\n")
        f.write(f"Total Packages Inputted: {total_input_packages:,}\n")
        f.write(f"Total Valid Repositories: {valid_packages_count:,}\n")
        f.write("  (The count of unique normalized GitHub repository URLs across all input packages)\n\n")
        
        f.write("Packages per Ecosystem:\n")
        for ecosystem in sorted(ecosystem_package_counts.keys()):
            f.write(f"  {ecosystem}: {ecosystem_package_counts[ecosystem]:,}\n")
        f.write("\n")
        
        f.write("=" * 80 + "\n")
        f.write("CROSS-ECOSYSTEM PACKAGES (ALL)\n")
        f.write("-" * 80 + "\n")
        f.write(f"Total Cross-Ecosystem Packages: {total_cross_ecosystem:,}\n")
        f.write(f"Percentage of Valid Repositories: {total_cross_ecosystem_percentage:.2f}%\n")
        f.write("Note: Includes both monorepo packages (same repo published to multiple\n")
        f.write("      ecosystems) and multirepo packages (language-specific repos).\n\n")
        
        f.write("=" * 80 + "\n")
        f.write("BREAKDOWN BY TYPE\n")
        f.write("-" * 80 + "\n")
        f.write(f"Monorepo Cross-Ecosystem Packages: {monorepo_cross_ecosystem:,}\n")
        f.write(f"  Percentage: {monorepo_percentage:.2f}%\n")
        f.write(f"  Description: Same repository published to multiple package ecosystems\n")
        f.write(f"               (exact URL matches across ecosystems).\n\n")
        f.write(f"Multirepo Cross-Ecosystem Packages: {multirepo_count:,}\n")
        f.write(f"  Percentage: {multirepo_percentage:.2f}%\n")
        f.write(f"  Description: Same project split into language-specific repositories\n")
        f.write(f"               (e.g., owner/project-js, owner/project-py).\n\n")

        f.write("=" * 80 + "\n")
        f.write("STATISTICS BY ECOSYSTEM COUNT\n")
        f.write("-" * 80 + "\n")
        for count in sorted(stats_by_count.keys(), key=int):
            stats = stats_by_count[count]
            f.write(f"  {count} ecosystems: {stats['package_count']:,} packages\n")
        f.write("\n")

        f.write("=" * 80 + "\n")
        f.write("PACKAGE COUNT BY ECOSYSTEM AND ECOSYSTEM COUNT\n")
        f.write("-" * 80 + "\n")
        f.write("Note: Includes both monorepo and multirepo cross-ecosystem packages.\n")
        f.write("      Shows how many packages from each ecosystem appear in N ecosystems.\n\n")
        
        # Determine max ecosystem count for column headers (from all cross-ecosystem packages)
        max_eco_count = max((pkg["ecosystem_count"] for pkg in cross_ecosystem_packages), default=0)
        
        # Build header row
        header = f"{'Ecosystem':<12}"
        for count in range(2, max_eco_count + 1):
            header += f" {count:>12}"
        header += f" {'Total':>12}"
        f.write(header + "\n")
        f.write("-" * (12 + 13 * (max_eco_count - 1 + 1)) + "\n")
        
        # Write data rows
        for ecosystem in sorted(ecosystems):
            row = f"{ecosystem:<12}"
            row_total = 0
            for count in range(2, max_eco_count + 1):
                count_value = ecosystem_count_matrix[ecosystem].get(count, 0)
                row += f" {count_value:>12,}"
                row_total += count_value
            row += f" {row_total:>12,}"
            f.write(row + "\n")
        
        # Write total row
        f.write("-" * (12 + 13 * (max_eco_count - 1 + 1)) + "\n")
        total_row = f"{'Total':<12}"
        grand_total = 0
        for count in range(2, max_eco_count + 1):
            col_total = sum(ecosystem_count_matrix[eco].get(count, 0) for eco in ecosystems)
            total_row += f" {col_total:>12,}"
            grand_total += col_total
        total_row += f" {grand_total:>12,}"
        f.write(total_row + "\n")

        # Write unique total row (counts each cross-ecosystem package only once per column)
        unique_row = f"{'Unique':<12}"
        unique_grand_total = 0
        for count in range(2, max_eco_count + 1):
            unique_count = stats_by_count.get(str(count), {}).get("package_count", 0)
            unique_row += f" {unique_count:>12,}"
            unique_grand_total += unique_count
        unique_row += f" {unique_grand_total:>12,}"
        f.write(unique_row + "\n")
        f.write("\n")

        f.write("=" * 80 + "\n")
        f.write("PER-ECOSYSTEM STATISTICS\n")
        f.write("-" * 80 + "\n")
        f.write("Note: Statistics below include both monorepo and multirepo packages.\n\n")
        f.write(f"{'Ecosystem':<12} {'Total Pkgs':>12} {'Valid Repos':>12} {'Valid %':>10} {'Cross-Eco':>12} {'Cross %':>10} {'Mono':>10} {'Multi':>10}\n")
        f.write("-" * 90 + "\n")
        for ecosystem, stats in sorted(ecosystem_stats.items(), key=lambda x: -x[1]["cross_ecosystem_packages"]):
            valid_pct = (stats['valid_repos'] / stats['total_packages'] * 100) if stats['total_packages'] > 0 else 0
            f.write(f"{ecosystem:<12} {stats['total_packages']:>12,} {stats['valid_repos']:>12,} {valid_pct:>9.1f}% {stats['cross_ecosystem_packages']:>12,} {stats['percentage']:>9.2f}% {stats['monorepo_packages']:>10,} {stats['multirepo_packages']:>10,}\n")
        f.write("-" * 90 + "\n")
        # Calculate totals
        total_pkgs_sum = sum(s['total_packages'] for s in ecosystem_stats.values())
        total_valid_sum = sum(s['valid_repos'] for s in ecosystem_stats.values())
        total_cross_sum = sum(s['cross_ecosystem_packages'] for s in ecosystem_stats.values())
        total_mono_sum = sum(s['monorepo_packages'] for s in ecosystem_stats.values())
        total_multi_sum = sum(s['multirepo_packages'] for s in ecosystem_stats.values())
        total_valid_pct = (total_valid_sum / total_pkgs_sum * 100) if total_pkgs_sum > 0 else 0
        total_cross_pct = (total_cross_sum / total_valid_sum * 100) if total_valid_sum > 0 else 0
        f.write(f"{'Total':<12} {total_pkgs_sum:>12,} {total_valid_sum:>12,} {total_valid_pct:>9.1f}% {total_cross_sum:>12,} {total_cross_pct:>9.2f}% {total_mono_sum:>10,} {total_multi_sum:>10,}\n")
        f.write("\n")

        f.write("=" * 80 + "\n")
        f.write("OUTPUT FILES\n")
        f.write("-" * 80 + "\n")
        f.write(f"JSON Output: {output_json_path}\n")
        f.write(f"Summary: {summary_txt_path}\n")
        f.write("=" * 80 + "\n")

    # Print summary
    print("\n" + "=" * 80)
    print("SUMMARY")
    print("=" * 80)
    print(f"Total input packages: {total_input_packages:,}")
    print(f"Total valid repositories: {valid_packages_count:,}")
    print(f"\nCross-ecosystem packages (monorepo): {monorepo_cross_ecosystem:,} ({monorepo_percentage:.2f}%)")
    print(f"Cross-ecosystem packages (multirepo): {multirepo_count:,} ({multirepo_percentage:.2f}%)")
    print(f"Total cross-ecosystem packages: {total_cross_ecosystem:,} ({total_cross_ecosystem_percentage:.2f}%)")
    
    print("\n" + "=" * 80)
    print("STATISTICS BY ECOSYSTEM COUNT")
    print("=" * 80)
    for count, stats in stats_by_count.items():
        print(f"  {count} ecosystems: {stats['package_count']} packages")

    print("\n" + "=" * 80)
    print("PER-ECOSYSTEM STATISTICS")
    print("=" * 80)
    for ecosystem, stats in sorted(ecosystem_stats.items(), key=lambda x: -x[1]["cross_ecosystem_packages"]):
        print(f"  {ecosystem}: {stats['cross_ecosystem_packages']}/{stats['valid_repos']} ({stats['percentage']}%) [Monorepo: {stats['monorepo_packages']}, Multirepo: {stats['multirepo_packages']}]")

    print("\n" + "=" * 80)
    print(f"Results saved to: {output_json_path}")
    print(f"Summary saved to: {summary_txt_path}")
    print("=" * 80)


if __name__ == "__main__":
    main()
