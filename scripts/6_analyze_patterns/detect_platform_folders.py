#!/usr/bin/env python3
"""
Platform/Architecture Folder Detector

Detects packages that contain platform or architecture-specific folder patterns:
- OS patterns: darwin, linux, windows, macos, freebsd, etc.
- Architecture patterns: arm64, x64, aarch64, amd64, x86, etc.
- Combined patterns: darwin-arm64, linux-x64, win32-x64, etc.
- Variant patterns: musl, glibc, android, ios

This pre-processes directory structures to identify packages with platform-specific builds,
which is a strong indicator of wrapper/binding patterns.
"""

import csv
import re
import json
from pathlib import Path
from collections import defaultdict
from datetime import datetime
from typing import Dict, List, Optional, Set, Tuple
from tqdm import tqdm

# ==============================================================================
# CONFIGURATION
# ==============================================================================

# Define paths
DATA_DIR = Path(__file__).resolve().parent.parent.parent / "data"

# Input sources from ecosystem detection (JSON files)
FULLY_MATCHED_JSON = DATA_DIR / "ecosystem-detection" / "fully_matched.json"

# Output directory
OUTPUT_DIR = DATA_DIR / "analysis" / "platform-folders"

# ==============================================================================
# EXCLUSION PATTERNS (folders to exclude from analysis)
# ==============================================================================

EXCLUDED_FOLDERS = [
    # Test folders
    r'^tests?$', r'^testing$', r'^spec$', r'^specs$',
    r'.*[-_]tests?$', r'.*[-_]test$',
    r'^__tests__$', r'^test[-_]?data$',
    # Hidden test folders
    r'^\.tests?$', r'^\.testing$', r'^\.spec$', r'^\.specs$',
    # Example/sample folders
    r'^examples?$', r'^samples?$', r'^demos?$',
    # Hidden example folders
    r'^\.examples?$', r'^\.samples?$', r'^\.demos?$',
    # Documentation folders
    r'^docs?$', r'^documentation$', r'^api[-_]?docs?$',
    # Hidden documentation folders
    r'^\.docs?$', r'^\.documentation$',
    # Benchmark folders
    r'^benchmarks?$', r'^benches$', r'^perf$',
    # Hidden benchmark folders
    r'^\.benchmarks?$', r'^\.benches$',
    # Fixture/mock folders
    r'^fixtures?$', r'^mocks?$', r'^stubs?$', r'^fakes?$',
    # Cache/build folders
    r'^__pycache__$', r'^\.pytest_cache$',
    r'^node_modules$', r'^vendor$', r'^target$',
    r'^dist$', r'^build$', r'^out$', r'^\.git$',
    r'^\.tox$', r'^\.venv$', r'^venv$', r'^env$',
    # Third-party/vendored code
    r'^third[-_]?party$', r'^external$', r'^deps$',
    r'^vendored$', r'^contrib$',
    # Icon folder
    r'^icon$', r'^icons$'
]

EXCLUDED_PATTERNS_COMPILED = [re.compile(p, re.IGNORECASE) for p in EXCLUDED_FOLDERS]

# ==============================================================================
# PLATFORM/ARCHITECTURE PATTERNS
# ==============================================================================

# Individual platform patterns
OS_PATTERNS = [
    r'darwin',
    r'macos',
    r'mac',  # matches only "mac" as a complete word (word boundaries handle exclusions)
    r'osx',
    r'linux',
    r'ubuntu',
    r'debian',
    r'centos',
    r'windows',
    r'win32',
    r'win64',
    r'freebsd',
    r'openbsd',
    r'netbsd',
]

ARCH_PATTERNS = [
    r'arm64',
    r'aarch64',
    r'x64',
    r'x86_64',
    r'amd64',
    r'x86',
    r'i386',
    r'i686',
    r'armv7',
    r'armv6',
    r'arm(?!64)',  # arm but not arm64
    r'ppc64',
    r'ppc64le',
    r's390x',
    r'mips',
    r'mips64',
    r'riscv64',
]

VARIANT_PATTERNS = [
    r'musl',
    r'glibc',
    r'gnu',
    r'msvc',
    r'mingw',
]

# Combined regex for folder detection
PLATFORM_OS_REGEX = re.compile(
    r'\b(' + '|'.join(OS_PATTERNS) + r')\b',
    re.IGNORECASE
)

PLATFORM_ARCH_REGEX = re.compile(
    r'\b(' + '|'.join(ARCH_PATTERNS) + r')\b',
    re.IGNORECASE
)

# Combined OS-ARCH pattern (e.g., darwin-arm64, linux-x64)
COMBINED_PATTERN = re.compile(
    r'\b(darwin|linux|windows|win|win32|win64|macos|freebsd|android|ios)[-_]?(arm64|aarch64|x64|x86_64|amd64|x86|arm|armv7|musl|gnu)\b',
    re.IGNORECASE
)

# ==============================================================================
# HELPER FUNCTIONS
# ==============================================================================

def extract_folders_from_directory_structure(directory_structure: List[str]) -> Set[Tuple[str, str]]:
    """
    Extract all unique folder paths from directory_structure.
    
    The directory_structure is a list of all paths (both files and folders).
    Folders are entries that don't have a file extension or have subdirectories.
    
    Args:
        directory_structure: List of all paths in the repository
        
    Returns:
        Set of (folder_path, folder_name) tuples
    """
    folders = set()
    
    # Efficiently extract all parent folders in a single pass
    for path in directory_structure:
        parts = path.split('/')
        
        # Extract all parent folders from the path
        for i in range(len(parts)):
            folder_path = '/'.join(parts[:i+1])
            folder_name = parts[i]
            folders.add((folder_path, folder_name))
    
    return folders


def build_folder_hierarchy(directory_structure: List[str]) -> Dict[str, int]:
    """
    Build a hierarchy of folders with their depth.
    
    Args:
        directory_structure: List of all paths in the repository
        
    Returns:
        Dict mapping folder_path to depth
    """
    folder_depths = {}
    
    for path in directory_structure:
        parts = path.split('/')
        for i in range(len(parts)):
            folder_path = '/'.join(parts[:i+1])
            folder_depths[folder_path] = i
    
    return folder_depths


def detect_platform_folders_from_structure(directory_structure: List[str]) -> Tuple[bool, List[Dict], Dict]:
    """Detect platform/architecture-specific folders from directory structure.
    
    Extracts folders from the complete directory structure and checks for platform patterns.
    
    Returns: (has_platform_folders, list_of_folder_info, details)
    where folder_info is {'name': str, 'path': str, 'type': str}
    """
    matched_folders = []  # List of {'name': folder_name, 'path': full_path, 'type': str}
    seen_folders = set()  # To avoid duplicates
    details = {
        'os_only': [],      # Folders with only OS (darwin, linux, windows)
        'arch_only': [],    # Folders with only arch (arm64, x64)
        'combined': [],     # Folders with OS-arch (darwin-arm64)
        'variants': [],     # Folders with variants (musl, glibc)
    }
    
    if not directory_structure:
        return False, [], details
    
    # Extract all folders from directory structure
    folders = extract_folders_from_directory_structure(directory_structure)
    
    # Pre-compile variant pattern for efficiency
    variant_pattern = re.compile(r'\b(' + '|'.join(VARIANT_PATTERNS) + r')\b', re.IGNORECASE)
    
    for full_path, folder_name in folders:
        # Skip excluded folders
        if is_excluded_folder(folder_name):
            continue
        
        # Skip if already seen
        if full_path in seen_folders:
            continue
        
        # Check for platform patterns (cache regex results)
        folder_info = None
        
        # Check for combined OS-arch pattern (highest confidence)
        combined_match = COMBINED_PATTERN.search(folder_name)
        if combined_match:
            folder_info = {'name': folder_name, 'path': full_path, 'type': 'combined'}
            details['combined'].append(full_path)
        else:
            # Check for standalone OS or arch patterns
            os_match = PLATFORM_OS_REGEX.search(folder_name)
            arch_match = PLATFORM_ARCH_REGEX.search(folder_name)
            
            if os_match and arch_match:
                folder_info = {'name': folder_name, 'path': full_path, 'type': 'combined'}
                details['combined'].append(full_path)
            elif os_match and is_platform_folder_context(folder_name):
                folder_info = {'name': folder_name, 'path': full_path, 'type': 'os_only'}
                details['os_only'].append(full_path)
            elif arch_match and is_platform_folder_context(folder_name):
                folder_info = {'name': folder_name, 'path': full_path, 'type': 'arch_only'}
                details['arch_only'].append(full_path)
        
        if folder_info:
            # Only include combined patterns (OS-arch)
            if folder_info['type'] == 'combined':
                # Check for variant patterns with pre-compiled regex
                if variant_pattern.search(folder_name):
                    details['variants'].append(full_path)
                
                matched_folders.append(folder_info)
                seen_folders.add(full_path)
    
    return len(matched_folders) > 0, matched_folders, details


def is_excluded_folder(folder_name: str) -> bool:
    """Check if a folder should be excluded from analysis."""
    for pattern in EXCLUDED_PATTERNS_COMPILED:
        if pattern.match(folder_name):
            return True
    return False


def is_platform_folder_context(folder: str) -> bool:
    """Check if folder appears to be in a platform-specific context.
    
    Validates that standalone OS or arch names look like platform folders,
    not random words that happen to match.
    """
    folder_lower = folder.lower()
    
    # Patterns that strongly suggest platform folder
    strong_indicators = [
        r'^(darwin|linux|windows|win32|win64|macos|freebsd|android|ios)$',
        r'^(arm64|aarch64|x64|x86_64|amd64|x86|i386|arm)$',
        r'[-_](darwin|linux|windows|macos|arm64|x64|amd64)',
        r'(darwin|linux|windows|macos|arm64|x64|amd64)[-_]',
        r'^(target|bin|release|debug)[-_]',
        r'[-_](release|debug)$',
        r'prebuilt',
        r'prebuild',
        r'native',
        r'platform',
    ]
    
    for pattern in strong_indicators:
        if re.search(pattern, folder_lower, re.IGNORECASE):
            return True
    
    # Folder contains version-like suffix (common in prebuilt binaries)
    if re.search(r'[-_]v?\d+(\.\d+)*$', folder):
        return True
    
    return False


def process_package_from_json(package_data: Dict, source_type: str) -> Optional[Dict]:
    """
    Process a single package from JSON data.
    
    Args:
        package_data: Package dict from JSON with repository, directory_structure, etc.
        source_type: 'fully_matched'
        
    Returns:
        Processed result dict or None if invalid
    """
    repository = package_data.get('repository', '')
    directory_structure = package_data.get('directory_structure', [])
    result_ecosystems = package_data.get('result_ecosystems', [])
    claimed_ecosystems = package_data.get('claimed_ecosystems', [])
    
    if not repository or not directory_structure:
        return None
    
    # Extract owner/repo from repository URL
    # Format: github.com/owner/repo
    owner_repo = '/'.join(repository.split('/')[-2:]) if '/' in repository else repository
    
    # Detect platform folders from complete directory structure
    has_platform, folders, details = detect_platform_folders_from_structure(directory_structure)
    
    return {
        'repo_url': f"https://{repository}",
        'owner_repo': owner_repo,
        'claimed_ecosystems': ', '.join(sorted(claimed_ecosystems)),
        'detected_ecosystems': ', '.join(sorted(result_ecosystems)),
        'source_type': source_type,
        'has_platform_folders': has_platform,
        'platform_folders': folders,
        'details': details
    }


def process_json_file(json_path: Path, source_type: str) -> Dict[str, List[Dict]]:
    """
    Process a JSON file containing package data.
    
    Args:
        json_path: Path to fully_matched.json
        source_type: 'fully_matched'
        
    Returns:
        Dict with 'has_platform' and 'no_platform' lists
    """
    all_results = {
        'has_platform': [],
        'no_platform': []
    }
    
    # Track processed repos to avoid duplicates
    processed_repos = set()
    
    try:
        with open(json_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
    except Exception as e:
        print(f"Error reading {json_path}: {e}")
        return all_results
    
    packages = data.get('packages', [])
    total_packages = len(packages)
    
    print(f"Processing {total_packages} packages from {json_path.name}...")
    
    # Process in batches to reduce overhead
    for package_data in tqdm(packages, desc=source_type, mininterval=0.5):
        result = process_package_from_json(package_data, source_type)
        
        if not result:
            continue
        
        repo_url = result['repo_url']
        
        # Skip duplicates
        if repo_url in processed_repos:
            continue
        processed_repos.add(repo_url)
        
        # Categorize based on platform folders found
        if result['has_platform_folders']:
            all_results['has_platform'].append(result)
        else:
            all_results['no_platform'].append(result)
    
    return all_results


def write_platform_output(results: List[Dict], output_dir: Path):
    """Write platform folder detection results."""
    csv_file = output_dir / "platform_folders.csv"
    detailed_file = output_dir / "platform_folders_detailed.txt"
    
    # CSV output
    with open(csv_file, 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow([
            'repo_url', 'owner_repo', 'source_type', 'claimed_ecosystems', 'detected_ecosystems',
            'folder_count', 'platform_folder_paths'
        ])
        
        for result in results:
            # Extract paths from folder info dicts
            folder_paths = [f['path'] for f in result['platform_folders']]
            writer.writerow([
                result['repo_url'],
                result['owner_repo'],
                result['source_type'],
                result['claimed_ecosystems'],
                result['detected_ecosystems'],
                len(result['platform_folders']),
                '; '.join(folder_paths[:10])  # Limit to first 10
            ])
    
    # Detailed text output
    with open(detailed_file, 'w', encoding='utf-8') as f:
        f.write("=" * 80 + "\n")
        f.write("PLATFORM/ARCHITECTURE FOLDER DETECTION\n")
        f.write("=" * 80 + "\n")
        f.write(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
        f.write(f"Total Packages with Platform Folders: {len(results)}\n")
        f.write("=" * 80 + "\n\n")
        
        for i, result in enumerate(results, 1):
            f.write(f"Package {i}/{len(results)}\n")
            f.write("-" * 80 + "\n")
            f.write(f"Repository: {result['repo_url']}\n")
            f.write(f"Owner/Repo: {result['owner_repo']}\n")
            f.write(f"Source Type: {result['source_type']}\n")
            f.write(f"Claimed Ecosystems: {result['claimed_ecosystems']}\n")
            f.write(f"Detected Ecosystems: {result['detected_ecosystems']}\n\n")
            
            f.write(f"Platform Folders Found ({len(result['platform_folders'])}):\n")
            for folder_info in result['platform_folders']:
                f.write(f"  - {folder_info['path']}/ ({folder_info['type']})\n")
            
            details = result['details']
            if details['combined']:
                f.write(f"\nCombined (OS-Arch) Paths:\n")
                for path in details['combined']:
                    f.write(f"  - {path}/\n")
            if details['os_only']:
                f.write(f"\nOS Only Paths:\n")
                for path in details['os_only']:
                    f.write(f"  - {path}/\n")
            if details['arch_only']:
                f.write(f"\nArch Only Paths:\n")
                for path in details['arch_only']:
                    f.write(f"  - {path}/\n")
            if details['variants']:
                f.write(f"\nVariant Paths:\n")
                for path in details['variants']:
                    f.write(f"  - {path}/\n")
            
            f.write("\n" + "=" * 80 + "\n\n")
    
    print(f"  Written: {csv_file}")
    print(f"  Written: {detailed_file}")


def write_summary(all_results: Dict[str, List[Dict]], output_dir: Path):
    """Write overall summary statistics."""
    summary_file = output_dir / "summary.txt"
    
    total_has_platform = len(all_results['has_platform'])
    total_no_platform = len(all_results['no_platform'])
    total = total_has_platform + total_no_platform
    
    # Count by detail type
    combined_count = 0
    os_only_count = 0
    arch_only_count = 0
    
    for result in all_results['has_platform']:
        details = result['details']
        if details['combined']:
            combined_count += 1
        if details['os_only'] and not details['combined']:
            os_only_count += 1
        if details['arch_only'] and not details['combined'] and not details['os_only']:
            arch_only_count += 1
    
    with open(summary_file, 'w', encoding='utf-8') as f:
        f.write("=" * 80 + "\n")
        f.write("PLATFORM/ARCHITECTURE FOLDER DETECTION SUMMARY\n")
        f.write("=" * 80 + "\n")
        f.write(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n")
        
        f.write("OVERALL STATISTICS\n")
        f.write("-" * 80 + "\n")
        f.write(f"Total Packages Analyzed:       {total}\n\n")
        f.write(f"Has Platform Folders:          {total_has_platform:5d} ({total_has_platform/total*100:.2f}%)\n")
        f.write(f"No Platform Folders:           {total_no_platform:5d} ({total_no_platform/total*100:.2f}%)\n\n")
        
        f.write("BREAKDOWN BY PATTERN TYPE\n")
        f.write("-" * 80 + "\n")
        f.write(f"Combined (OS-Arch):            {combined_count:5d} (e.g., darwin-arm64, linux-x64)\n")
        f.write(f"OS Only:                       {os_only_count:5d} (e.g., darwin, linux, windows)\n")
        f.write(f"Arch Only:                     {arch_only_count:5d} (e.g., arm64, x64, amd64)\n")
    
    print(f"  Written: {summary_file}")


def main():
    """Main entry point."""
    print("\n" + "=" * 80)
    print("PLATFORM/ARCHITECTURE FOLDER DETECTOR")
    print("=" * 80)
    
    # Check if source exists
    if not FULLY_MATCHED_JSON.exists():
        print(f"\nERROR: Ecosystem-Detector JSON file not found")
        print(f"Expected: {FULLY_MATCHED_JSON}")
        return
    
    # Set output directory
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    print(f"Output directory: {OUTPUT_DIR}")
    
    all_results = {
        'has_platform': [],
        'no_platform': []
    }
    
    # Process fully matched JSON
    print("\n" + "=" * 80)
    print("PROCESSING FULLY MATCHED PACKAGES")
    print("=" * 80)
    fully_matched_results = process_json_file(FULLY_MATCHED_JSON, 'fully_matched')
    
    for category in ['has_platform', 'no_platform']:
        all_results[category].extend(fully_matched_results[category])
    
    print(f"  Fully matched: {sum(len(v) for v in fully_matched_results.values())} packages")
    
    # Write outputs
    print("\n" + "=" * 80)
    print("WRITING OUTPUTS")
    print("=" * 80)
    write_platform_output(all_results['has_platform'], OUTPUT_DIR)
    write_summary(all_results, OUTPUT_DIR)
    
    # Final statistics
    print("\n" + "=" * 80)
    print("COMPLETED")
    print("=" * 80)
    print(f"Has Platform Folders: {len(all_results['has_platform']):5d} packages")
    print(f"No Platform Folders:  {len(all_results['no_platform']):5d} packages")
    print(f"Total:                {sum(len(v) for v in all_results.values()):5d} packages")
    print(f"\nOutput: {OUTPUT_DIR}")
    print("=" * 80)


if __name__ == "__main__":
    main()
