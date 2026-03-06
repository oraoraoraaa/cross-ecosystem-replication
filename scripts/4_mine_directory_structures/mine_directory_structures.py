#!/usr/bin/env python3
"""
Directory Structure Miner
Mines directory structures from cross-ecosystem packages based on github_metrics.json input.
Supports both JSON and text-based cache formats.
Outputs a single JSON file with all packages.
"""

import argparse
import json
import os
import re
import sys
from pathlib import Path
from typing import List, Dict, Optional, Tuple
from datetime import datetime
from tqdm import tqdm
import requests


# ============================================================================
# PATH CONFIGURATION
# ============================================================================

DATA_DIR = Path(__file__).resolve().parent.parent.parent / "data"
DEFAULT_INPUT_FILE = DATA_DIR / "github-metrics" / "github_metrics.json"
DEFAULT_OUTPUT_DIR = DATA_DIR / "directory-structures"
DEFAULT_ERROR_LOG_DIR = DATA_DIR / "directory-structures" / "error-log"
DEFAULT_CACHE_DIR = DATA_DIR / "cache" / "directory-structures"

# ============================================================================


def normalize_github_url(url: str) -> Optional[str]:
    """
    Normalize GitHub repository URLs to a standard format (github.com/owner/repo).
    Returns None if URL is invalid, empty, or not a GitHub URL.
    """
    if not url or (isinstance(url, str) and url.strip() == ""):
        return None

    url = str(url).strip().lower()

    if "github.com" not in url:
        return None

    # Remove common suffixes and prefixes
    url = re.sub(r"\.git$", "", url)
    url = re.sub(r"/$", "", url)
    url = url.replace('git+https://', 'https://')
    url = url.replace('git+ssh://', 'ssh://')
    url = url.replace('git://', 'https://')

    # Extract path from URL
    try:
        match = re.search(r"github\.com[/:]([^/]+)/([^/]+)", url)
        if match:
            owner = match.group(1).lower()
            repo = match.group(2).lower()
            # Remove .git suffix if present
            repo = re.sub(r"\.git$", "", repo)
            return f"github.com/{owner}/{repo}"
    except:
        pass

    return None


class GlobalCacheIndex:
    """
    Global search index for cached directory structures.
    Supports three cache formats:
    1. Consolidated JSON (single directory_structures.json in cache root)
    2. Per-ecosystem JSON files (in *_ecosystems subdirectories)
    3. Legacy text format (in *_ecosystems subdirectories)
    """
    
    def __init__(self, cache_dirs: List[Path] = None):
        """
        Initialize the global cache index.
        
        Args:
            cache_dirs: List of directories containing cached directory structures
        """
        self.cache_dirs = cache_dirs or []
        self.index: Dict[str, List[str]] = {}  # normalized_url -> directory_structure paths
        self.loaded = False
        self.stats = {
            "total_entries": 0,
            "consolidated_json_loaded": 0,
            "json_files_parsed": 0,
            "txt_files_parsed": 0,
            "parse_errors": 0,
        }
    
    @staticmethod
    def _visual_tree_to_paths(tree_str: str) -> List[str]:
        """
        Convert a visual tree string to a list of paths.
        
        Args:
            tree_str: Visual tree representation with ├──, │, └── etc.
            
        Returns:
            List of file/directory paths (without the root repo name prefix)
        """
        paths = []
        path_stack = []
        
        for line in tree_str.split('\n'):
            if not line.strip():
                continue
            
            clean_line = line.rstrip()
            
            # Find tree branch indicators (├── or └──)
            branch_pos = max(clean_line.rfind('├── '), clean_line.rfind('└── '))
            
            if branch_pos >= 0:
                name = clean_line[branch_pos + 4:].strip()
                depth = branch_pos // 4
            else:
                # Root directory line (e.g., "repo-name/")
                name = clean_line.strip().rstrip('/')
                if not name:
                    continue
                path_stack = []
                continue
            
            if not name:
                continue
            
            name = name.rstrip('/')
            path_stack = path_stack[:depth]
            path_stack.append(name)
            
            full_path = '/'.join(path_stack)
            if full_path:
                paths.append(full_path)
        
        return paths
    
    def _parse_json_cache_file(self, file_path: Path) -> int:
        """Parse a JSON cache file and add entries to index."""
        count = 0
        try:
            with open(file_path, "r", encoding="utf-8") as f:
                data = json.load(f)
            
            packages = data.get("packages", [])
            for pkg in packages:
                repo = pkg.get("repository", "")
                directory_structure = pkg.get("directory_structure", [])
                
                if not repo or not directory_structure:
                    continue
                
                # Normalize the URL
                if repo.startswith("github.com/"):
                    # Already in format "github.com/owner/repo"
                    normalized_url = repo.lower()
                elif repo.startswith("http"):
                    # Full URL like "https://github.com/owner/repo"
                    normalized_url = normalize_github_url(repo)
                elif "/" in repo:
                    # Format like "owner/repo"
                    normalized_url = f"github.com/{repo.lower()}"
                else:
                    normalized_url = None
                
                if normalized_url and normalized_url not in self.index:
                    self.index[normalized_url] = directory_structure
                    count += 1
            
            self.stats["json_files_parsed"] += 1
        except Exception as e:
            self.stats["parse_errors"] += 1
            tqdm.write(f"  ⚠ Error parsing {file_path.name}: {e}")
        
        return count
    
    def _parse_txt_cache_file(self, file_path: Path) -> int:
        """Parse a text cache file and add entries to index."""
        count = 0
        try:
            with open(file_path, "r", encoding="utf-8") as f:
                content = f.read()
            
            # Split by package boundaries
            package_blocks = re.split(r'\n={80}\nPackage \d+/\d+\n={80}\n', content)
            
            for block in package_blocks[1:]:
                if not block.strip():
                    continue
                
                repo_match = re.search(r'^Repository: (.+)$', block, re.MULTILINE)
                if not repo_match:
                    continue
                
                repo_url = repo_match.group(1).strip()
                normalized_url = normalize_github_url(repo_url)
                
                if not normalized_url:
                    continue
                
                tree_match = re.search(r'Directory Structure:\n-{80}\n\n(.+?)(?=\n\n={80}|\Z)', block, re.DOTALL)
                
                if tree_match and normalized_url not in self.index:
                    tree_str = tree_match.group(1).strip()
                    paths = self._visual_tree_to_paths(tree_str)
                    self.index[normalized_url] = paths
                    count += 1
            
            self.stats["txt_files_parsed"] += 1
        except Exception as e:
            self.stats["parse_errors"] += 1
            tqdm.write(f"  ⚠ Error parsing {file_path.name}: {e}")
        
        return count
    
    def _load_consolidated_json(self, file_path: Path) -> int:
        """
        Load a consolidated JSON cache file (directory_structures.json).
        This is a single large JSON file containing all cached entries.
        Uses streaming JSON parsing for memory efficiency.
        """
        count = 0
        try:
            print(f"  Loading consolidated cache: {file_path.name}")
            
            # For large files, we need to be memory-efficient
            # Use ijson for streaming if available, otherwise fall back to standard json
            try:
                import ijson
                
                with open(file_path, "rb") as f:
                    # Stream through packages array
                    for pkg in ijson.items(f, "packages.item"):
                        repo = pkg.get("repository", "")
                        directory_structure = pkg.get("directory_structure", [])
                        
                        if not repo or not directory_structure:
                            continue
                        
                        # Normalize the URL
                        if repo.startswith("github.com/"):
                            normalized_url = repo.lower()
                        elif repo.startswith("http"):
                            normalized_url = normalize_github_url(repo)
                        elif "/" in repo:
                            normalized_url = f"github.com/{repo.lower()}"
                        else:
                            normalized_url = None
                        
                        if normalized_url and normalized_url not in self.index:
                            self.index[normalized_url] = directory_structure
                            count += 1
                
            except ImportError:
                # Fall back to standard json (higher memory usage)
                tqdm.write("  ⚠ ijson not installed, using standard json parser (higher memory usage)")
                with open(file_path, "r", encoding="utf-8") as f:
                    data = json.load(f)
                
                packages = data.get("packages", [])
                for pkg in packages:
                    repo = pkg.get("repository", "")
                    directory_structure = pkg.get("directory_structure", [])
                    
                    if not repo or not directory_structure:
                        continue
                    
                    # Normalize the URL
                    if repo.startswith("github.com/"):
                        normalized_url = repo.lower()
                    elif repo.startswith("http"):
                        normalized_url = normalize_github_url(repo)
                    elif "/" in repo:
                        normalized_url = f"github.com/{repo.lower()}"
                    else:
                        normalized_url = None
                    
                    if normalized_url and normalized_url not in self.index:
                        self.index[normalized_url] = directory_structure
                        count += 1
            
            self.stats["consolidated_json_loaded"] += 1
            print(f"  ✓ Loaded {count:,} entries from consolidated cache")
            
        except Exception as e:
            self.stats["parse_errors"] += 1
            tqdm.write(f"  ⚠ Error loading consolidated cache {file_path.name}: {e}")
        
        return count
    
    def build_index(self) -> None:
        """Build the global search index from all cache files."""
        if self.loaded:
            return
        
        print(f"\n{'=' * 80}")
        print("Building Global Cache Index")
        print(f"{'=' * 80}")
        
        # First, check for consolidated JSON files (directory_structures.json)
        # This is the preferred format as it contains all entries in one file
        consolidated_files = []
        json_files = []
        txt_files = []
        
        for cache_dir in self.cache_dirs:
            if not cache_dir or not cache_dir.exists():
                continue
            
            print(f"Scanning: {cache_dir}")
            
            # Check for consolidated cache file in cache root
            consolidated_cache = cache_dir / "directory_structures.json"
            if consolidated_cache.exists():
                consolidated_files.append(consolidated_cache)
            
            # Find ecosystem subdirectories for per-ecosystem files
            for subdir in cache_dir.iterdir():
                if subdir.is_dir() and "_ecosystems" in subdir.name:
                    json_files.extend(subdir.glob("*.json"))
                    txt_files.extend(subdir.glob("*.txt"))
            
            # Also check for other JSON/TXT files directly in cache_dir
            for f in cache_dir.glob("*.json"):
                if f.name != "directory_structures.json" and f.name != "summary.json":
                    json_files.append(f)
            for f in cache_dir.glob("*.txt"):
                if f.name != "summary.txt":
                    txt_files.append(f)
        
        # Remove duplicates
        consolidated_files = list(set(consolidated_files))
        json_files = list(set(json_files))
        txt_files = list(set(txt_files))
        
        total_sources = len(consolidated_files) + len(json_files) + len(txt_files)
        print(f"Found {len(consolidated_files)} consolidated JSON, {len(json_files)} per-ecosystem JSON, and {len(txt_files)} TXT files")
        
        if total_sources == 0:
            print("⚠ No cache files found")
            self.loaded = True
            return
        
        # Phase 1: Load consolidated JSON files first (most efficient)
        if consolidated_files:
            print(f"\nLoading {len(consolidated_files)} consolidated cache file(s)...")
            for file_path in consolidated_files:
                count = self._load_consolidated_json(file_path)
                self.stats["total_entries"] += count
        
        # Phase 2: Parse per-ecosystem JSON files (newer format)
        if json_files:
            print(f"\nParsing {len(json_files)} per-ecosystem JSON cache files...")
            for file_path in tqdm(json_files, desc="JSON files", unit="file"):
                count = self._parse_json_cache_file(file_path)
                self.stats["total_entries"] += count
        
        # Phase 3: Parse TXT files (legacy format)
        if txt_files:
            print(f"\nParsing {len(txt_files)} TXT cache files...")
            for file_path in tqdm(txt_files, desc="TXT files", unit="file"):
                count = self._parse_txt_cache_file(file_path)
                self.stats["total_entries"] += count
        
        self.loaded = True
        
        print(f"\n✓ Cache index built successfully!")
        print(f"  • Total entries: {self.stats['total_entries']:,}")
        print(f"  • Consolidated JSON loaded: {self.stats['consolidated_json_loaded']}")
        print(f"  • Per-ecosystem JSON parsed: {self.stats['json_files_parsed']}")
        print(f"  • TXT files parsed: {self.stats['txt_files_parsed']}")
        print(f"  • Parse errors: {self.stats['parse_errors']}")
    
    def lookup(self, normalized_url: str) -> Optional[List[str]]:
        """Look up a repository in the index."""
        if not self.loaded:
            self.build_index()
        return self.index.get(normalized_url)
    
    def contains(self, normalized_url: str) -> bool:
        """Check if a repository is in the index."""
        if not self.loaded:
            self.build_index()
        return normalized_url in self.index


class GitHubDirectoryMiner:
    """Mines directory structures from GitHub repositories."""

    def __init__(
        self,
        github_tokens: List[str] = None,
        error_log_dir: Optional[str] = None,
        cache: Optional[GlobalCacheIndex] = None,
    ):
        self.tokens = github_tokens or []
        if not self.tokens:
            env_token = os.environ.get("GITHUB_TOKEN")
            if env_token:
                self.tokens = [t.strip() for t in env_token.split(",") if t.strip()]
        
        self.current_token_index = 0
        self.error_log_dir = Path(error_log_dir) if error_log_dir else None
        self.cache = cache
        self.error_logs: Dict[str, List[Dict]] = {}
        self.error_stats: Dict[str, Dict[str, int]] = {}

    @property
    def current_token(self) -> Optional[str]:
        if not self.tokens:
            return None
        return self.tokens[self.current_token_index % len(self.tokens)]

    @property
    def headers(self) -> dict:
        headers = {"Accept": "application/vnd.github.v3+json"}
        if self.current_token:
            headers["Authorization"] = f"token {self.current_token}"
        return headers

    def rotate_token(self) -> bool:
        """Rotate to the next token. Returns True if rotation successful."""
        if len(self.tokens) <= 1:
            return False
        self.current_token_index = (self.current_token_index + 1) % len(self.tokens)
        return True

    def log_error(
        self,
        repo_url: str,
        ecosystems: List[str],
        error_type: str,
        error_msg: str,
        solution: str = "",
    ):
        """Log an error for later analysis."""
        log_key = "errors"
        
        if log_key not in self.error_logs:
            self.error_logs[log_key] = []
            self.error_stats[log_key] = {}
        
        self.error_logs[log_key].append({
            "repository": repo_url,
            "ecosystems": ecosystems,
            "error_type": error_type,
            "error_message": error_msg,
            "solution": solution,
            "timestamp": datetime.now().isoformat(),
        })
        
        if error_type not in self.error_stats[log_key]:
            self.error_stats[log_key][error_type] = 0
        self.error_stats[log_key][error_type] += 1

    def write_error_logs(self):
        """Write all error logs to files."""
        if not self.error_log_dir or not self.error_logs:
            return
        
        self.error_log_dir.mkdir(parents=True, exist_ok=True)
        
        for log_key, errors in self.error_logs.items():
            if not errors:
                continue
            
            log_file = self.error_log_dir / f"{log_key}.log"
            
            with open(log_file, "w", encoding="utf-8") as f:
                f.write(f"Error Log - Generated: {datetime.now().isoformat()}\n")
                f.write(f"Total Errors: {len(errors)}\n")
                f.write("=" * 80 + "\n\n")
                
                # Error statistics
                stats = self.error_stats.get(log_key, {})
                if stats:
                    f.write("Error Type Summary:\n")
                    for error_type, count in sorted(stats.items(), key=lambda x: -x[1]):
                        f.write(f"  • {error_type}: {count}\n")
                    f.write("\n" + "=" * 80 + "\n\n")
                
                # Individual errors
                for i, error in enumerate(errors, 1):
                    f.write(f"Error {i}/{len(errors)}\n")
                    f.write("-" * 40 + "\n")
                    f.write(f"Repository: {error['repository']}\n")
                    f.write(f"Ecosystems: {', '.join(error['ecosystems'])}\n")
                    f.write(f"Type: {error['error_type']}\n")
                    f.write(f"Message: {error['error_message']}\n")
                    if error.get('solution'):
                        f.write(f"Solution: {error['solution']}\n")
                    f.write("\n")
            
            tqdm.write(f"  • {log_file.name}: {len(errors)} errors")

    def parse_github_url(self, url: str) -> Optional[Tuple[str, str]]:
        """Parse a GitHub URL and return (owner, repo) tuple."""
        if not url:
            return None

        url = str(url).strip()
        
        # Handle github.com/owner/repo format
        if url.startswith("github.com/"):
            parts = url.replace("github.com/", "").split("/")
            if len(parts) >= 2:
                return (parts[0], parts[1])
        
        # Handle full URLs
        patterns = [
            r"github\.com/([^/]+)/([^/]+?)(?:\.git)?/?$",
            r"github\.com/([^/]+)/([^/]+)",
        ]

        for pattern in patterns:
            match = re.search(pattern, url, re.IGNORECASE)
            if match:
                return (match.group(1), match.group(2).rstrip("/").replace(".git", ""))

        return None

    def validate_tokens(self) -> None:
        """Validate all tokens and remove invalid ones."""
        if not self.tokens:
            return
        
        valid_tokens = []
        for i, token in enumerate(self.tokens):
            try:
                response = requests.get(
                    "https://api.github.com/user",
                    headers={"Authorization": f"token {token}"},
                    timeout=10,
                )
                if response.status_code == 200:
                    valid_tokens.append(token)
                    user_data = response.json()
                    tqdm.write(f"  ✓ Token {i+1}: Valid (user: {user_data.get('login', 'unknown')})")
                else:
                    tqdm.write(f"  ✗ Token {i+1}: Invalid (status {response.status_code})")
            except Exception as e:
                tqdm.write(f"  ✗ Token {i+1}: Error - {e}")
        
        self.tokens = valid_tokens

    def check_rate_limit(self, show_output: bool = False) -> Dict:
        """Check current rate limit status."""
        try:
            response = requests.get(
                "https://api.github.com/rate_limit",
                headers=self.headers,
                timeout=10,
            )
            if response.status_code == 200:
                data = response.json()
                core = data.get("resources", {}).get("core", {})
                if show_output:
                    remaining = core.get("remaining", 0)
                    limit = core.get("limit", 0)
                    tqdm.write(f"  Rate limit: {remaining}/{limit}")
                return core
        except:
            pass
        return {}

    def get_tree(
        self,
        owner: str,
        repo: str,
        max_depth: Optional[int] = None,
        repo_url: str = "",
        ecosystems: List[str] = None,
        retry_count: int = 0,
    ) -> Optional[List[str]]:
        """
        Get the directory tree of a repository using GitHub API.
        
        Returns:
            List of file/directory paths, or None on failure
        """
        ecosystems = ecosystems or []
        
        try:
            # Get default branch
            repo_response = requests.get(
                f"https://api.github.com/repos/{owner}/{repo}",
                headers=self.headers,
                timeout=30,
            )
            
            if repo_response.status_code == 404:
                self.log_error(
                    repo_url, ecosystems, "NOT_FOUND",
                    f"Repository not found: {owner}/{repo}",
                    "Repository may have been deleted, renamed, or made private"
                )
                return None
            
            if repo_response.status_code == 403:
                rate_info = self.check_rate_limit()
                remaining = rate_info.get("remaining", 0)
                
                if remaining == 0 and self.rotate_token():
                    return self.get_tree(owner, repo, max_depth, repo_url, ecosystems, retry_count)
                
                self.log_error(
                    repo_url, ecosystems, "RATE_LIMITED",
                    "Rate limit exceeded",
                    "Wait for rate limit reset or add more tokens"
                )
                return None
            
            if repo_response.status_code != 200:
                self.log_error(
                    repo_url, ecosystems, "API_ERROR",
                    f"Failed to get repo info: HTTP {repo_response.status_code}"
                )
                return None
            
            repo_data = repo_response.json()
            default_branch = repo_data.get("default_branch", "main")
            
            # Get tree
            tree_response = requests.get(
                f"https://api.github.com/repos/{owner}/{repo}/git/trees/{default_branch}?recursive=1",
                headers=self.headers,
                timeout=60,
            )
            
            if tree_response.status_code == 403:
                rate_info = self.check_rate_limit()
                remaining = rate_info.get("remaining", 0)
                
                if remaining == 0 and self.rotate_token():
                    return self.get_tree(owner, repo, max_depth, repo_url, ecosystems, retry_count)
                
                self.log_error(
                    repo_url, ecosystems, "RATE_LIMITED",
                    "Rate limit exceeded during tree fetch"
                )
                return None
            
            if tree_response.status_code != 200:
                self.log_error(
                    repo_url, ecosystems, "TREE_ERROR",
                    f"Failed to get tree: HTTP {tree_response.status_code}"
                )
                return None
            
            tree_data = tree_response.json()
            tree_items = tree_data.get("tree", [])
            
            if tree_data.get("truncated", False):
                tqdm.write(f"  ⚠ Tree truncated for {owner}/{repo}")
            
            # Extract paths
            paths = []
            for item in tree_items:
                path = item.get("path", "")
                if not path:
                    continue
                
                if max_depth:
                    depth = path.count("/") + 1
                    if depth > max_depth:
                        continue
                
                paths.append(path)
            
            return sorted(paths)
            
        except requests.exceptions.Timeout:
            if retry_count < 2:
                return self.get_tree(owner, repo, max_depth, repo_url, ecosystems, retry_count + 1)
            self.log_error(repo_url, ecosystems, "TIMEOUT", "Request timed out after retries")
            return None
            
        except Exception as e:
            self.log_error(repo_url, ecosystems, "EXCEPTION", str(e))
            return None

    def lookup_cache(self, normalized_url: str) -> Optional[List[str]]:
        """Look up a repository in the cache."""
        if self.cache:
            return self.cache.lookup(normalized_url)
        return None
    
    def is_cached(self, normalized_url: str) -> bool:
        """Check if a repository is in the cache."""
        if self.cache:
            return self.cache.contains(normalized_url)
        return False


def load_packages_from_metrics(input_file: Path) -> Dict[str, Dict]:
    """
    Load packages from github_metrics.json file.
    Skips packages that are forked, archived, have errors, or are not monorepo.
    
    Returns:
        Dictionary mapping normalized_url -> package info
    """
    print(f"\nLoading packages from: {input_file}")
    
    with open(input_file, "r", encoding="utf-8") as f:
        data = json.load(f)
    
    packages_data = data.get("packages", {})
    packages = {}
    
    # Statistics for skipped packages
    skipped_forked = 0
    skipped_archived = 0
    skipped_error = 0
    skipped_multirepo = 0
    
    for key, pkg_info in packages_data.items():
        repo_url = pkg_info.get("repo_url", "")
        if not repo_url:
            continue
        
        # Skip non-monorepo packages (only mine monorepo packages)
        if pkg_info.get("source", "monorepo") != "monorepo":
            skipped_multirepo += 1
            continue
        
        # Skip forked repositories
        if pkg_info.get("is_fork", False):
            skipped_forked += 1
            continue
        
        # Skip archived repositories
        if pkg_info.get("is_archived", False):
            skipped_archived += 1
            continue
        
        # Skip packages with errors
        if pkg_info.get("error"):
            skipped_error += 1
            continue
        
        normalized_url = normalize_github_url(repo_url)
        if not normalized_url:
            continue
        
        # Parse ecosystems from comma-separated string
        ecosystems_str = pkg_info.get("ecosystems", "")
        ecosystems = [e.strip() for e in ecosystems_str.split(",") if e.strip()]
        
        packages[normalized_url] = {
            "repo_url": repo_url,
            "ecosystems": ecosystems,
            "owner_repo": pkg_info.get("owner_repo", ""),
        }
    
    print(f"  • Loaded {len(packages):,} unique monorepo packages")
    print(f"  • Skipped {skipped_multirepo:,} multirepo packages")
    print(f"  • Skipped {skipped_forked:,} forked repositories")
    print(f"  • Skipped {skipped_archived:,} archived repositories")
    print(f"  • Skipped {skipped_error:,} packages with errors")
    return packages


def save_checkpoint(checkpoint_file: Path, processed_packages: List[Dict], processed_repos: set):
    """Save current progress to checkpoint file."""
    checkpoint_data = {
        "timestamp": datetime.now().isoformat(),
        "processed_count": len(processed_repos),
        "processed_repos": list(processed_repos),
        "packages": processed_packages,
    }
    
    with open(checkpoint_file, "w", encoding="utf-8") as f:
        json.dump(checkpoint_data, f, indent=2)


def load_checkpoint(checkpoint_file: Path) -> Tuple[List[Dict], set]:
    """Load checkpoint data if exists."""
    if not checkpoint_file.exists():
        return [], set()
    
    try:
        with open(checkpoint_file, "r", encoding="utf-8") as f:
            data = json.load(f)
        
        packages = data.get("packages", [])
        processed_repos = set(data.get("processed_repos", []))
        
        print(f"  • Loaded checkpoint: {len(processed_repos):,} packages already processed")
        return packages, processed_repos
    except Exception as e:
        print(f"  ⚠ Failed to load checkpoint: {e}")
        return [], set()


def write_output(
    output_file: Path,
    all_packages: List[Dict],
    total_cache_hits: int,
    total_api_mined: int,
):
    """Write final output to JSON file."""
    output_data = {
        "metadata": {
            "generated": datetime.now().isoformat(),
            "total_packages": len(all_packages),
            "cache_hits": total_cache_hits,
            "api_mined": total_api_mined,
            "cache_hit_rate": f"{total_cache_hits/len(all_packages)*100:.2f}%" if all_packages else "0%",
        },
        "packages": all_packages,
    }
    
    with open(output_file, "w", encoding="utf-8") as f:
        json.dump(output_data, f, indent=2)
    
    print(f"\n✓ Output written to: {output_file}")
    print(f"  • Total packages: {len(all_packages):,}")
    print(f"  • Cache hits: {total_cache_hits:,}")
    print(f"  • API mined: {total_api_mined:,}")


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="Mine directory structures from cross-ecosystem packages"
    )
    parser.add_argument(
        "--input",
        default=DEFAULT_INPUT_FILE,
        help=f"Input github_metrics.json file (default: {DEFAULT_INPUT_FILE})",
    )
    parser.add_argument(
        "--output-dir",
        default=DEFAULT_OUTPUT_DIR,
        help=f"Output directory (default: {DEFAULT_OUTPUT_DIR})",
    )
    parser.add_argument(
        "--output-file",
        default="directory_structures.json",
        help="Output filename (default: directory_structures.json)",
    )
    parser.add_argument(
        "--error-log-dir",
        default=DEFAULT_ERROR_LOG_DIR,
        help=f"Directory to save error logs (default: {DEFAULT_ERROR_LOG_DIR})",
    )
    parser.add_argument(
        "--cache-dir",
        nargs="+",
        default=[DEFAULT_CACHE_DIR],
        help=f"Cache directories (default: {DEFAULT_CACHE_DIR})",
    )
    parser.add_argument(
        "--token",
        nargs="+",
        help="GitHub personal access token(s) (or set GITHUB_TOKEN env var)",
    )
    parser.add_argument(
        "--max-depth",
        type=int,
        help="Maximum depth for directory structure (default: unlimited)",
    )
    parser.add_argument(
        "--no-resume",
        action="store_true",
        help="Start fresh, ignoring any existing checkpoint",
    )
    parser.add_argument(
        "--no-cache",
        action="store_true",
        help="Disable cache lookup, always fetch from API",
    )
    parser.add_argument(
        "--checkpoint-interval",
        type=int,
        default=50,
        help="Save checkpoint every N packages (default: 50)",
    )

    args = parser.parse_args()

    # Setup paths
    script_dir = Path(__file__).parent
    input_file = Path(args.input).resolve() if Path(args.input).is_absolute() else (script_dir / args.input).resolve()
    output_dir = Path(args.output_dir).resolve() if Path(args.output_dir).is_absolute() else (script_dir / args.output_dir).resolve()
    error_log_dir = Path(args.error_log_dir).resolve() if Path(args.error_log_dir).is_absolute() else (script_dir / args.error_log_dir).resolve()
    
    cache_dirs = []
    for cache_dir in args.cache_dir:
        cache_path = Path(cache_dir).resolve() if Path(cache_dir).is_absolute() else (script_dir / cache_dir).resolve()
        if cache_path.exists():
            cache_dirs.append(cache_path)

    # Create output directories
    output_dir.mkdir(parents=True, exist_ok=True)
    error_log_dir.mkdir(parents=True, exist_ok=True)

    print(f"\n{'=' * 80}")
    print("Directory Structure Miner")
    print(f"{'=' * 80}")
    print(f"Input file: {input_file}")
    print(f"Output directory: {output_dir}")
    print(f"Output file: {args.output_file}")
    print(f"Error log directory: {error_log_dir}")
    print(f"Cache directories: {', '.join(str(d) for d in cache_dirs) if cache_dirs else 'None'}")
    
    # Check input file
    if not input_file.exists():
        print(f"\n✗ Input file not found: {input_file}")
        sys.exit(1)
    
    # Build cache index
    cache = None
    if not args.no_cache and cache_dirs:
        cache = GlobalCacheIndex(cache_dirs)
        cache.build_index()
    
    # Initialize miner
    miner = GitHubDirectoryMiner(
        github_tokens=args.token,
        error_log_dir=str(error_log_dir),
        cache=cache,
    )

    # Handle tokens
    if not miner.tokens:
        print("\n⚠ No GitHub tokens provided.")
        print("  Set GITHUB_TOKEN environment variable or use --token argument.")
        
        try:
            token_input = input("\nPlease enter GitHub token(s) (comma-separated) or press Enter to continue without: ")
            if token_input.strip():
                miner.tokens = [t.strip() for t in token_input.split(",") if t.strip()]
            
            if not miner.tokens:
                response = input("\nContinue without authentication (60 requests/hour limit)? [y/N]: ")
                if response.lower() != "y":
                    sys.exit(0)

        except (KeyboardInterrupt, EOFError):
            print("\nAborted.")
            sys.exit(0)

    if miner.tokens:
        print(f"\nValidating {len(miner.tokens)} token(s)...")
        miner.validate_tokens()
        
        if not miner.tokens:
            print("\n✗ No valid tokens available.")
            sys.exit(1)
        
        print(f"\n✓ {len(miner.tokens)} valid token(s) available")
        miner.check_rate_limit(show_output=True)
    else:
        print("\nContinuing without authentication.")

    # Load packages
    all_packages_info = load_packages_from_metrics(input_file)
    
    if not all_packages_info:
        print("\n✗ No packages found in input file")
        sys.exit(1)
    
    # Load checkpoint
    checkpoint_file = output_dir / "checkpoint.json"
    all_packages = []
    processed_repos = set()
    
    if not args.no_resume:
        all_packages, processed_repos = load_checkpoint(checkpoint_file)
    
    # Calculate statistics
    urls_from_cache = []
    urls_need_api = []
    
    for normalized_url in all_packages_info:
        if normalized_url in processed_repos:
            continue
        if cache and cache.contains(normalized_url):
            urls_from_cache.append(normalized_url)
        else:
            urls_need_api.append(normalized_url)
    
    print(f"\n{'=' * 80}")
    print("Processing Plan")
    print(f"{'=' * 80}")
    print(f"  • Total packages: {len(all_packages_info):,}")
    print(f"  • Already processed (checkpoint): {len(processed_repos):,}")
    print(f"  • Available in cache: {len(urls_from_cache):,}")
    print(f"  • Need API mining: {len(urls_need_api):,}")
    
    remaining = len(urls_from_cache) + len(urls_need_api)
    if remaining == 0:
        print("\n✓ All packages already processed!")
    else:
        try:
            response = input(f"\nProceed with mining {remaining:,} packages? [Y/n]: ")
            if response.lower() == "n":
                sys.exit(0)
        except (KeyboardInterrupt, EOFError):
            print("\nAborted.")
            sys.exit(0)
    
    # Phase 1: Transfer from cache
    total_cache_hits = len([p for p in all_packages if p.get("from_cache", False)])
    
    if urls_from_cache:
        print(f"\n{'=' * 80}")
        print("Phase 1: Transferring from Cache")
        print(f"{'=' * 80}")
        
        with tqdm(total=len(urls_from_cache), desc="Cache transfer", unit="pkg") as pbar:
            for normalized_url in urls_from_cache:
                pkg_info = all_packages_info[normalized_url]
                directory_structure = cache.lookup(normalized_url)
                
                if directory_structure:
                    all_packages.append({
                        "repository": normalized_url,
                        "repo_url": pkg_info["repo_url"],
                        "claimed_ecosystems": pkg_info["ecosystems"],
                        "directory_structure": directory_structure,
                        "from_cache": True,
                    })
                    processed_repos.add(normalized_url)
                    total_cache_hits += 1
                
                pbar.update(1)
        
        # Save checkpoint after cache phase
        save_checkpoint(checkpoint_file, all_packages, processed_repos)
        print(f"\n✓ Cache transfer complete: {total_cache_hits:,} packages")
    
    # Phase 2: Mine from API
    total_api_mined = len([p for p in all_packages if not p.get("from_cache", False)])
    
    if urls_need_api:
        print(f"\n{'=' * 80}")
        print("Phase 2: Mining from GitHub API")
        print(f"{'=' * 80}")
        
        api_success = 0
        api_failed = 0
        
        with tqdm(total=len(urls_need_api), desc="API mining", unit="pkg") as pbar:
            for i, normalized_url in enumerate(urls_need_api):
                pkg_info = all_packages_info[normalized_url]
                
                parsed = miner.parse_github_url(normalized_url)
                if not parsed:
                    api_failed += 1
                    pbar.update(1)
                    continue
                
                owner, repo = parsed
                directory_structure = miner.get_tree(
                    owner, repo,
                    max_depth=args.max_depth,
                    repo_url=pkg_info["repo_url"],
                    ecosystems=pkg_info["ecosystems"],
                )
                
                if directory_structure:
                    all_packages.append({
                        "repository": normalized_url,
                        "repo_url": pkg_info["repo_url"],
                        "claimed_ecosystems": pkg_info["ecosystems"],
                        "directory_structure": directory_structure,
                        "from_cache": False,
                    })
                    processed_repos.add(normalized_url)
                    api_success += 1
                    total_api_mined += 1
                else:
                    api_failed += 1
                
                pbar.update(1)
                
                # Save checkpoint periodically
                if (i + 1) % args.checkpoint_interval == 0:
                    save_checkpoint(checkpoint_file, all_packages, processed_repos)
        
        print(f"\n✓ API mining complete:")
        print(f"  • Successful: {api_success:,}")
        print(f"  • Failed: {api_failed:,}")
    
    # Write error logs
    print(f"\n{'=' * 80}")
    print("Writing error logs...")
    miner.write_error_logs()
    
    # Write final output
    output_file = output_dir / args.output_file
    write_output(output_file, all_packages, total_cache_hits, total_api_mined)
    
    # Remove checkpoint on success
    if checkpoint_file.exists():
        checkpoint_file.unlink()
        print("  • Checkpoint file removed")
    
    print(f"\n{'=' * 80}")
    print("Mining Complete!")
    print(f"{'=' * 80}")


if __name__ == "__main__":
    main()
