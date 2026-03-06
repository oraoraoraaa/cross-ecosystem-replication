#!/usr/bin/env python3
"""
Mine Unique Merged Contributors for Multirepo Packages

This script:
1. Parses github_metrics.json and filters for multirepo packages only
2. Skips packages that are monorepo, forked, archived, have errors, or already have contributors_unique_merged
3. Groups multirepo packages by normalized name (after removing ecosystem patterns)
4. For each group, fetches all contributors from each repo
5. Calculates unique merged contributors across all repos in the group
6. Adds a "contributors_unique_merged" field to each entry

Example:
- microsoft/durabletask-java and microsoft/durabletask-js are grouped together
  because they both normalize to "microsoft/durabletask-"
- If java has [John, Mary, May] and js has [John, Mary, Martin]
- The unique merged count is 4 (John, Mary, May, Martin)

Usage:
    python mine_unique_contributors.py
    python mine_unique_contributors.py -i input.json -o output.json
    python mine_unique_contributors.py --dry-run  # Preview without saving
    
    # Multiple tokens for rate limit rotation
    python mine_unique_contributors.py -t token1 token2 token3
    python mine_unique_contributors.py --token-file tokens.txt
"""
import os
import json
import requests
import re
import time
import argparse
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Set, Tuple
from collections import defaultdict
from tqdm import tqdm
from threading import Lock


# ============================================================================
# PATH CONFIGURATION
# ============================================================================
SCRIPT_DIR = Path(__file__).parent
DATA_DIR = SCRIPT_DIR.resolve().parent.parent / "data"
DEFAULT_INPUT_FILE = DATA_DIR / "github-metrics" / "github_metrics.json"
DEFAULT_OUTPUT_FILE = DATA_DIR / "github-metrics" / "github_metrics.json"
DEFAULT_BACKUP_FILE = DATA_DIR / "github-metrics" / "github_metrics_backup_unique.json"


# ============================================================================
# GITHUB API CONFIGURATION
# ============================================================================
BASE_URL = "https://api.github.com"
MAX_RETRIES = 3
RETRY_DELAY = 2
CONTRIBUTORS_PER_PAGE = 100  # Max allowed by GitHub API


# ============================================================================
# TOKEN MANAGER (Copied from mine_github_metrics.py for consistency)
# ============================================================================

class TokenManager:
    """Manages GitHub API tokens with rotation and validation."""
    
    def __init__(self, tokens: List[str] = None):
        """Initialize token manager with a list of tokens."""
        self.tokens = tokens or []     
        self.current_token_index = 0
        self.base_url = "https://api.github.com"
        self.lock = Lock()  # Thread-safe token rotation
        self.rate_limit_threshold = 10  # Start looking for alternatives when below this
        self.logger = None  # Can be set by main for logging
    
    @property
    def current_token(self) -> Optional[str]:
        """Get the currently active token."""
        if not self.tokens:
            return None
        return self.tokens[self.current_token_index]
    
    @property
    def headers(self) -> dict:
        """Get headers with current authentication token."""
        headers = {}
        token = self.current_token
        if token:
            headers['Authorization'] = f'token {token}'
        return headers
    
    def rotate_token(self) -> bool:
        """Switch to the next available token. Returns True if switched, False if no other tokens."""
        with self.lock:
            if not self.tokens or len(self.tokens) <= 1:
                return False
            
            self.current_token_index = (self.current_token_index + 1) % len(self.tokens)
            tqdm.write(f"  ⟳ Switched to token #{self.current_token_index + 1}")
            return True
    
    def get_next_token(self) -> Optional[str]:
        """Get next token in round-robin fashion for parallel requests."""
        with self.lock:
            if not self.tokens:
                return None
            token = self.tokens[self.current_token_index]
            self.current_token_index = (self.current_token_index + 1) % len(self.tokens)
            return token
    
    def validate_tokens(self) -> None:
        """Check and display status of all provided tokens."""
        if not self.tokens:
            return
        
        print("\nToken Status Check:")
        print("-" * 65)
        print(f"{'Token':<12} {'Status':<15} {'Remaining':<15} {'Reset Time':<15}")
        print("-" * 65)
        
        for i, token in enumerate(self.tokens):
            headers = {'Authorization': f'token {token}'}
            try:
                response = requests.get(
                    f"{self.base_url}/rate_limit", headers=headers, timeout=5
                )
                
                if response.status_code == 200:
                    data = response.json()
                    core = data.get('resources', {}).get('core', {})
                    remaining = core.get('remaining', 'N/A')
                    reset_timestamp = core.get('reset', 0)
                    reset_time = datetime.fromtimestamp(reset_timestamp).strftime('%H:%M:%S')
                    status = "✓ Valid" if remaining > 0 else "⚠ Depleted"
                elif response.status_code == 401:
                    status = "✗ Invalid"
                    remaining = "N/A"
                    reset_time = "N/A"
                else:
                    status = f"Error {response.status_code}"
                    remaining = "N/A"
                    reset_time = "N/A"
            
            except Exception:
                status = "Connection Error"
                remaining = "N/A"
                reset_time = "N/A"
            
            print(f"Token #{i+1:<6} {status:<15} {remaining:<15} {reset_time:<15}")
        
        print("-" * 65 + "\n")

    def get_rate_limit_status(self, token: str = None) -> Tuple[int, int]:
        """
        Get current rate limit status for a token.
        Returns: (remaining, reset_timestamp)
        """
        if token is None:
            token = self.current_token
        
        if not token:
            return (0, 0)
        
        headers = {'Authorization': f'token {token}'}
        try:
            response = requests.get(
                f"{self.base_url}/rate_limit", 
                headers=headers, 
                timeout=5
            )
            if response.status_code == 200:
                data = response.json()
                core = data.get('resources', {}).get('core', {})
                remaining = core.get('remaining', 0)
                reset = core.get('reset', 0)
                return (remaining, reset)
        except Exception:
            pass
        return (0, 0)

    def find_available_token(self) -> Optional[str]:
        """
        Find a token with available rate limit.
        Returns token if found, None otherwise.
        """
        if not self.tokens:
            return None
        
        # Check all tokens for available rate limit
        for i in range(len(self.tokens)):
            token = self.tokens[i]
            remaining, reset = self.get_rate_limit_status(token)
            
            if remaining > self.rate_limit_threshold:
                # Found a good token, switch to it
                with self.lock:
                    self.current_token_index = i
                return token
        
        return None

    def wait_for_rate_limit_reset(self):
        """Wait for the rate limit to reset on any available token."""
        if not self.tokens:
            # No tokens, wait default time
            time.sleep(60)
            return
        
        # Find the token with the earliest reset time
        earliest_reset = float('inf')
        for token in self.tokens:
            remaining, reset = self.get_rate_limit_status(token)
            if reset > 0 and reset < earliest_reset:
                earliest_reset = reset
        
        if earliest_reset != float('inf'):
            wait_time = max(earliest_reset - time.time(), 0) + 5
            reset_time_str = datetime.fromtimestamp(earliest_reset).strftime('%H:%M:%S')
            tqdm.write(f"\n⏳ All tokens exhausted. Waiting {wait_time:.0f}s until {reset_time_str}...")
            time.sleep(wait_time)
        else:
            # Fallback: wait 60 seconds
            tqdm.write(f"\n⏳ Rate limit status unknown. Waiting 60 seconds...")
            time.sleep(60)

    def ensure_rate_limit(self) -> str:
        """
        Ensure we have rate limit available. 
        Will rotate tokens or wait as needed.
        Returns a token with available rate limit.
        """
        # Check current token
        if self.current_token:
            remaining, reset = self.get_rate_limit_status(self.current_token)
            
            if remaining > self.rate_limit_threshold:
                return self.current_token
            
            # Current token is low, try to find another
            available_token = self.find_available_token()
            if available_token:
                return available_token
            
            # All tokens exhausted, wait for reset
            self.wait_for_rate_limit_reset()
            
            # After waiting, return current token
            return self.current_token
        
        return None



# Global token manager instance (initialized in main)
token_manager: TokenManager = None


# ============================================================================
# ECOSYSTEM SUFFIX PATTERNS (Same as filter_multirepo_common_package.py)
# ============================================================================
ECOSYSTEM_SUFFIX_PATTERNS = {
    "PyPI": [
        "python",
        "py",
        "pypi",
        "python2",
        "python3",
        "py2",
        "py3",
        "cpython",
        "pysdk",
        "pyclient",
        "pylib",
    ],
    "Crates": [
        "rust",
        "rs",
        "cargo",
        "rustlang",
        "crate",
        "crates",
    ],
    "Go": [
        "go",
        "golang",
        "goclient",
        "gosdk",
        "golib",
    ],
    "NPM": [
        "js",
        "javascript",
        "node",
        "nodejs",
        "npm",
        "ts",
        "typescript",
        "jsclient",
        "tsclient",
        "jssdk",
        "tssdk",
        "jslib",
        "tslib",
    ],
    "Maven": [
        "java",
        "jvm",
        "maven",
        "scala",
        "kotlin",
        "kt",
        "kts",
        "javaclient",
        "javasdk",
        "javalib",
        "scalaclient",
        "scalalib",
    ],
    "Ruby": [
        "ruby",
        "rb",
        "gem",
        "rubygem",
        "rubyclient",
        "rubylib",
    ],
    "PHP": [
        "php",
        "php5",
        "php7",
        "php8",
        "phpclient",
        "phpsdk",
        "phplib",
    ],
    "Other": [
        "net",
        "dotnet",
        "csharp",
        "cs",
        "fsharp",
        "fs",
        "cpp",
        "cxx",
        "cplusplus",
        "c",
        "clang",
        "swift",
        "swiftclient",
        "elixir",
        "ex",
        "exs",
        "dart",
        "flutter",
        "perl",
        "pl",
        "lua",
        "r",
        "rlang",
        "haskell",
        "hs",
        "ocaml",
        "ml",
        "erlang",
        "erl",
        "clojure",
        "clj",
        "groovy",
        "gvy",
    ],
}

# Flatten all suffixes into a single list
ECOSYSTEM_SUFFIXES = []
for ecosystem, suffixes in ECOSYSTEM_SUFFIX_PATTERNS.items():
    ECOSYSTEM_SUFFIXES.extend(suffixes)

# Remove duplicates while preserving order
seen = set()
ECOSYSTEM_SUFFIXES = [x for x in ECOSYSTEM_SUFFIXES if not (x in seen or seen.add(x))]


# ============================================================================
# HELPER FUNCTIONS
# ============================================================================









def remove_ecosystem_patterns(repo_name: str) -> str:
    """
    Remove ecosystem patterns from a repository name, keeping separators intact.

    Examples:
        - 'libsql-js' -> 'libsql-'
        - 'libsql-python' -> 'libsql-'
        - 'durabletask-java' -> 'durabletask-'

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


def get_normalized_key(owner_repo: str) -> str:
    """
    Get normalized key for grouping multirepo packages.

    Args:
        owner_repo: Full owner/repo string (e.g., "microsoft/durabletask-java")

    Returns:
        Normalized key (e.g., "microsoft/durabletask-") or None if invalid
    """
    if "/" not in owner_repo:
        return None

    owner, repo = owner_repo.split("/", 1)
    normalized_repo = remove_ecosystem_patterns(repo)

    if not normalized_repo:
        return None

    return f"{owner.lower()}/{normalized_repo}"


def get_all_contributors(owner: str, repo: str) -> Set[str]:
    """
    Get all contributors for a repository using pagination.
    Returns a set of contributor identifiers (login for users, email for anonymous).

    Args:
        owner: Repository owner
        repo: Repository name

    Returns:
        Set of contributor identifiers
    """
    contributors = set()
    page = 1

    while True:
        for attempt in range(MAX_RETRIES + 1):
            try:
                token_manager.ensure_rate_limit()
                
                url = f"{BASE_URL}/repos/{owner}/{repo}/contributors"
                params = {
                    "per_page": CONTRIBUTORS_PER_PAGE,
                    "anon": "true",
                    "page": page
                }
                
                response = requests.get(url, headers=token_manager.headers, params=params, timeout=15)

                if response.status_code == 200:
                    data = response.json()

                    if not data:  # Empty page, we're done
                        return contributors

                    for contributor in data:
                        if contributor.get("type") == "Anonymous":
                            # For anonymous contributors, use email as identifier
                            email = contributor.get("email", "")
                            if email:
                                contributors.add(f"anon:{email.lower()}")
                        else:
                            # For regular users, use login
                            login = contributor.get("login", "")
                            if login:
                                contributors.add(login.lower())

                    # Check if there are more pages
                    if len(data) < CONTRIBUTORS_PER_PAGE:
                        return contributors

                    page += 1
                    break  # Success, continue to next page

                elif response.status_code == 404:
                    # Repository not found
                    return contributors

                elif response.status_code == 403:
                    # Rate limit or forbidden
                    if attempt < MAX_RETRIES:
                        time.sleep(RETRY_DELAY ** (attempt + 1))
                        continue
                    return contributors

                else:
                    if attempt < MAX_RETRIES:
                        time.sleep(RETRY_DELAY ** (attempt + 1))
                        continue
                    return contributors

            except requests.exceptions.Timeout:
                if attempt < MAX_RETRIES:
                    time.sleep(RETRY_DELAY ** (attempt + 1))
                    continue
                return contributors

            except Exception as e:
                if attempt < MAX_RETRIES:
                    time.sleep(RETRY_DELAY ** (attempt + 1))
                    continue
                return contributors
        else:
            # All retries exhausted for this page
            return contributors

    return contributors


# ============================================================================
# MAIN PROCESSING FUNCTIONS
# ============================================================================
def load_metrics_data(input_file: Path) -> dict:
    """Load the GitHub metrics JSON file."""
    print(f"Loading metrics from: {input_file}")
    with open(input_file, "r", encoding="utf-8") as f:
        data = json.load(f)
    print(f"✓ Loaded {len(data.get('packages', {}))} packages")
    return data


def save_metrics_data(output_file: Path, data: dict):
    """Save the updated metrics data."""
    print(f"\nSaving updated metrics to: {output_file}")
    with open(output_file, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)
    print("✓ Saved successfully")


def backup_metrics_data(input_file: Path, backup_file: Path):
    """Create a backup of the original metrics file."""
    print(f"Creating backup: {backup_file}")
    with open(input_file, "r", encoding="utf-8") as f:
        data = json.load(f)
    with open(backup_file, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)
    print("✓ Backup created")


def filter_multirepo_packages(data: dict) -> Dict[str, dict]:
    """
    Filter packages to only include multirepo entries that don't have contributors_unique_merged yet.
    Also skips forked, archived, and error packages.

    Returns:
        Dictionary of package_key -> package_data for multirepo packages
    """
    multirepo_packages = {}
    packages = data.get("packages", {})

    skipped_monorepo = 0
    skipped_already_processed = 0
    skipped_forked = 0
    skipped_archived = 0
    skipped_error = 0

    for package_key, package_data in packages.items():
        source = package_data.get("source", "")

        # Skip non-multirepo packages (monorepo or other)
        if source != "multirepo":
            skipped_monorepo += 1
            continue

        # Skip packages that already have contributors_unique_merged populated
        contributors_unique_merged = package_data.get("contributors_unique_merged")
        if contributors_unique_merged is not None and contributors_unique_merged != "":
            skipped_already_processed += 1
            continue

        # Skip forked repositories
        if package_data.get("is_fork", False):
            skipped_forked += 1
            continue

        # Skip archived repositories
        if package_data.get("is_archived", False):
            skipped_archived += 1
            continue

        # Skip repositories with errors
        if "error_detail" in package_data:
            skipped_error += 1
            continue

        multirepo_packages[package_key] = package_data

    print(
        f"  Skipped {skipped_monorepo} non-multirepo packages")
    print(
        f"  Skipped {skipped_already_processed} packages with existing contributors_unique_merged")
    print(
        f"  Skipped {skipped_forked} forked repositories")
    print(
        f"  Skipped {skipped_archived} archived repositories")
    print(
        f"  Skipped {skipped_error} packages with errors")

    return multirepo_packages


def group_multirepo_by_normalized_name(multirepo_packages: Dict[str, dict]) -> Dict[str, List[Tuple[str, dict]]]:
    """
    Group multirepo packages by their normalized name (after removing ecosystem patterns).

    Args:
        multirepo_packages: Dictionary of package_key -> package_data

    Returns:
        Dictionary of normalized_key -> list of (package_key, package_data) tuples
    """
    groups = defaultdict(list)
    ungrouped_count = 0

    for package_key, package_data in multirepo_packages.items():
        owner_repo = package_data.get("owner_repo", "")
        normalized_key = get_normalized_key(owner_repo)

        if normalized_key:
            groups[normalized_key].append((package_key, package_data))
        else:
            ungrouped_count += 1

    if ungrouped_count > 0:
        print(f"  ⚠ {ungrouped_count} packages could not be normalized")

    return groups


def process_multirepo_groups(data: dict, groups: Dict[str, List[Tuple[str, dict]]], dry_run: bool = False) -> Tuple[int, int, int]:
    """
    Process each multirepo group to calculate unique merged contributors.

    Args:
        data: Full metrics data (will be modified in place)
        groups: Dictionary of normalized_key -> list of (package_key, package_data)
        dry_run: If True, don't modify data, just show what would be done

    Returns:
        Tuple of (groups_processed, packages_updated, groups_failed)
    """
    groups_processed = 0
    packages_updated = 0
    groups_failed = 0

    print(f"\n{'='*70}")
    print(f"Processing {len(groups)} multirepo groups")
    print(f"{'='*70}\n")

    for normalized_key, packages_in_group in tqdm(groups.items(), desc="Processing groups"):
        # Collect all unique contributors across all repos in this group
        all_contributors = set()
        group_success = True

        for package_key, package_data in packages_in_group:
            owner_repo = package_data.get("owner_repo", "")

            if "/" not in owner_repo:
                continue

            owner, repo = owner_repo.split("/", 1)

            # Get all contributors for this repo
            contributors = get_all_contributors(owner, repo)

            if contributors:
                all_contributors.update(contributors)
            else:
                # If we couldn't get contributors for any repo, mark as partial failure
                tqdm.write(f"  ⚠ Could not fetch contributors for {owner_repo}")

        # Calculate unique merged count
        unique_merged_count = len(all_contributors)

        if unique_merged_count > 0:
            groups_processed += 1

            # Update all packages in this group with the merged count
            for package_key, package_data in packages_in_group:
                old_count = package_data.get("contributors", 0)

                if not dry_run:
                    data["packages"][package_key]["contributors_unique_merged"] = unique_merged_count

                packages_updated += 1

                # Log significant changes
                if len(packages_in_group) > 1:
                    tqdm.write(
                        f"  ✓ {normalized_key}: {len(packages_in_group)} repos → {unique_merged_count} unique contributors")
        else:
            groups_failed += 1
            tqdm.write(f"  ✗ Failed to process group: {normalized_key}")

    return groups_processed, packages_updated, groups_failed


def main():
    """Main function."""
    global token_manager

    parser = argparse.ArgumentParser(
        description='Mine unique merged contributors for multirepo packages'
    )
    parser.add_argument(
        '-i', '--input',
        type=Path,
        default=DEFAULT_INPUT_FILE,
        help='Input JSON file path (default: github_metrics.json)'
    )
    parser.add_argument(
        '-o', '--output',
        type=Path,
        default=DEFAULT_OUTPUT_FILE,
        help='Output JSON file path (default: same as input)'
    )
    parser.add_argument(
        '-b', '--backup',
        type=Path,
        default=DEFAULT_BACKUP_FILE,
        help='Backup file path (default: github_metrics_backup_unique.json)'
    )
    parser.add_argument(
        '--no-backup',
        action='store_true',
        help='Skip creating backup file'
    )
    parser.add_argument(
        '--dry-run',
        action='store_true',
        help='Preview changes without saving'
    )
    parser.add_argument(
        '-t', '--tokens',
        type=str,
        nargs='+',
        default='',
        help='GitHub API tokens (space-separated). Multiple tokens enable rotation on rate limit.'
    )
    parser.add_argument(
        '--token-file',
        type=Path,
        help='File containing GitHub tokens (one per line)'
    )

    args = parser.parse_args()

    # Collect tokens
    tokens = args.tokens or []
    
    # Check for environment variable
    env_token = os.environ.get('GITHUB_TOKEN', '').strip()
    if env_token and not tokens:
        if "," in env_token:
            tokens = [t.strip() for t in env_token.split(",") if t.strip()]
        else:
            tokens = [env_token]
        print(f"\nUsing {len(tokens)} token(s) from GITHUB_TOKEN environment variable")
    
    # Prompt for tokens if not provided
    if not tokens:
        print("\n" + "=" * 80)
        print("GitHub Token Input")
        print("=" * 80)
        print("Enter your GitHub token(s). You can:")
        print("  • Enter multiple tokens separated by commas")
        print("  • Press Enter to use unauthenticated API (60 requests/hour)")
        print("  • Press Ctrl+C to cancel")
        print("=" * 80)

        try:
            token_input = input("\nEnter token(s): ").strip()
            if token_input:
                tokens = [t.strip() for t in re.split(r'[\s,]+', token_input) if t.strip()]
        except (KeyboardInterrupt, EOFError):
            print("\n\n✗ Operation cancelled by user.")
            return

    # Initialize token manager
    token_manager = TokenManager(tokens)

    if token_manager.tokens:
        token_manager.validate_tokens()
        try:
            response = (
                input("Do you want to proceed with these tokens? (yes/no): ")
                .strip()
                .lower()
            )
            if response not in ["yes", "y"]:
                print("\n✗ Operation cancelled by user.")
                return
        except (KeyboardInterrupt, EOFError):
            print("\n\n✗ Operation cancelled by user.")
            return
    else:
        print("\n" + "!" * 80)
        print("⚠  WARNING: No GitHub Token Provided")
        print("!" * 80)
        print("\nRunning without authentication has severe limitations:")
        print("  • Rate limit: Only 60 requests per hour (vs 5,000 with token)")
        print("  • Cannot access private repositories")
        print("  • May encounter frequent rate limit errors")
        print("  • Processing will be significantly slower")
        print("\nTo use a token:")
        print("  • Set GITHUB_TOKEN environment variable, or")
        print("  • Use --token argument with your personal access token")
        print("\nGet a token at: https://github.com/settings/tokens")
        print("=" * 80)

        try:
            response = (
                input("\nDo you want to continue without a token? (yes/no): ")
                .strip()
                .lower()
            )
            if response not in ["yes", "y"]:
                print("\n✗ Operation cancelled by user.")
                return
            print("\n✓ Continuing without token...")
        except (KeyboardInterrupt, EOFError):
            print("\n\n✗ Operation cancelled by user.")
            return


    # Validate input file exists
    if not args.input.exists():
        print(f"✗ Error: Input file not found: {args.input}")
        return 1

    # Check rate limit status for all tokens
    print("\nChecking GitHub API rate limits...")
    token_manager.validate_tokens()

    # Load data
    data = load_metrics_data(args.input)

    # Filter for multirepo packages only
    print("\nFiltering for multirepo packages...")
    multirepo_packages = filter_multirepo_packages(data)
    print(f"  Found {len(multirepo_packages)} multirepo packages")

    if not multirepo_packages:
        print("✓ No multirepo packages found")
        return 0

    # Group by normalized name
    print("\nGrouping by normalized name (after removing ecosystem patterns)...")
    groups = group_multirepo_by_normalized_name(multirepo_packages)

    # Filter to only groups with 2+ packages (actual multirepo sets)
    actual_groups = {k: v for k, v in groups.items() if len(v) >= 2}
    single_packages = {k: v for k, v in groups.items() if len(v) == 1}

    print(f"  Groups with 2+ packages: {len(actual_groups)}")
    print(f"  Single packages (no match): {len(single_packages)}")

    # Show some examples
    if actual_groups:
        print("\nExamples of multirepo groups:")
        for i, (normalized_key, packages_in_group) in enumerate(list(actual_groups.items())[:5]):
            repos = [p[1].get('owner_repo', '') for p in packages_in_group]
            print(f"  {i+1}. {normalized_key}")
            for repo in repos[:3]:
                print(f"      - {repo}")
            if len(repos) > 3:
                print(f"      ... and {len(repos) - 3} more")
        if len(actual_groups) > 5:
            print(f"  ... and {len(actual_groups) - 5} more groups")

    if args.dry_run:
        print("\n[DRY RUN MODE - No changes will be saved]")

    # Create backup
    if not args.no_backup and not args.dry_run:
        backup_metrics_data(args.input, args.backup)

    # Process groups and calculate unique merged contributors
    groups_processed, packages_updated, groups_failed = process_multirepo_groups(
        data, actual_groups, dry_run=args.dry_run
    )

    # Also process single packages (set their unique merged count to their own count)
    print("\nProcessing single-repo packages...")
    for normalized_key, packages_in_group in tqdm(single_packages.items(), desc="Single packages"):
        for package_key, package_data in packages_in_group:
            # For single packages, unique merged count equals null
            if not args.dry_run:
                data['packages'][package_key]['contributors_unique_merged'] = ''
            packages_updated += 1

    # Save updated data
    if not args.dry_run:
        save_metrics_data(args.output, data)

    # Summary
    print(f"\n{'='*70}")
    print("Summary:")
    print(f"  Total multirepo packages: {len(multirepo_packages)}")
    print(f"  Multirepo groups (2+ packages): {len(actual_groups)}")
    print(f"  Single packages: {len(single_packages)}")
    print(f"  Groups processed successfully: {groups_processed}")
    print(f"  Packages updated: {packages_updated}")
    print(f"  Groups failed: {groups_failed}")
    if args.dry_run:
        print("  [DRY RUN - No changes saved]")
    print(f"{'='*70}")

    return 0


if __name__ == "__main__":
    exit(main())
