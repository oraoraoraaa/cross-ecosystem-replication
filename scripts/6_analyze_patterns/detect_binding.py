#!/usr/bin/env python3
"""
Binding Pattern Detector

Detects packages that contain binding naming patterns in their folder structure:
- Binding folder patterns: "binding", "bindings", "napi", "node-addon", "addon",
                          "ffi", "native", "jni", "cgo", "cffi", "ctypes", "pybind", "cython"
- Binding file patterns: .gyp, .node, .pyd, binding.gyp, etc.

This pre-processes directory structures to avoid memory issues in the web UI.

Input: JSON file from Ecosystem-Detector (fully_matched.json)
"""

import csv
import json
import re
from pathlib import Path
from datetime import datetime
from typing import Dict, List, Optional
from tqdm import tqdm

# ==============================================================================
# CONFIGURATION
# ==============================================================================

# Define paths
DATA_DIR = Path(__file__).resolve().parent.parent.parent / "data"

# Input sources from ecosystem detection (JSON files)
FULLY_MATCHED_JSON = DATA_DIR / "ecosystem-detection" / "fully_matched.json"

# Output directory
OUTPUT_DIR = DATA_DIR / "analysis" / "binding"

# Binding folder name patterns and file patterns
# High-confidence binding folder patterns
BINDING_FOLDER_PATTERNS = re.compile(
    r'\b('
    # Node.js native addon patterns
    r'binding|bindings|napi|node-addon|addon|'
    # FFI patterns (cross-language)
    r'ffi|cffi|ctypes|'
    # Native code patterns
    r'native|natives|'
    # Language-specific binding patterns
    r'jni|cgo|'
    # Python binding patterns
    r'pybind|pybind11|cython|'
    # Rust binding patterns
    r'neon|napi-rs'
    r')\b',
    re.IGNORECASE
)

# Binding file patterns - files that strongly indicate native bindings
BINDING_FILE_PATTERNS = re.compile(
    r'('
    # Node.js native addon build files
    r'binding\.gyp$|'
    r'\.node-gyp$|'
    r'\.gyp$|'
    # Node.js native addon binaries
    r'\.node$|'
    # Python extension modules
    r'\.pyd$|'
    r'\.swg$|'
    # Cython files
    r'\.pyx$'
    # SWIG interface files (.i removed - too broad, conflicts with Objective-C headers)|'
    r'\.pxd$'
    r')',
    re.IGNORECASE
)

# Excluded folders - same as detect_designated_folder.py for consistency
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

# Pre-compile excluded folder patterns for efficiency
# Note: EXCLUDED_FOLDERS contains regex patterns, so compile them directly
EXCLUDED_PATTERNS_COMPILED = [
    re.compile(folder, re.IGNORECASE) 
    for folder in EXCLUDED_FOLDERS
]


def is_excluded_folder(folder_name: str) -> bool:
    """Check if folder name matches any excluded patterns."""
    for pattern in EXCLUDED_PATTERNS_COMPILED:
        if pattern.match(folder_name):
            return True
    return False


def is_path_in_excluded_folder(path: str) -> bool:
    """Check if any component in the path is an excluded folder."""
    parts = path.split('/')
    for part in parts[:-1]:  # Check all parts except the last (which could be file or folder)
        if is_excluded_folder(part):
            return True
    return False


# ==============================================================================
# HELPER FUNCTIONS
# ==============================================================================

def load_json_data(file_path: Path) -> Optional[Dict]:
    """Load JSON data from file."""
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            return json.load(f)
    except Exception as e:
        print(f"Error loading {file_path}: {e}")
        return None


def identify_folders(paths: List[str]) -> set:
    """Identify which paths are folders based on having children."""
    folders = set()
    sorted_paths = sorted(paths)
    
    for i, path in enumerate(sorted_paths):
        # Check if any subsequent path starts with this path + '/'
        prefix = path + '/'
        for j in range(i + 1, len(sorted_paths)):
            if sorted_paths[j].startswith(prefix):
                folders.add(path)
                break
            elif not sorted_paths[j].startswith(path):
                # No need to check further as paths are sorted
                break
    
    return folders


def find_binding_indicators(dir_structure: List[str]) -> Dict[str, List[str]]:
    """Find folder/file paths that match binding indicator patterns.
    
    Args:
        dir_structure: List of file/folder paths
    
    Returns:
        Dict with 'folders' and 'files' keys, each containing list of matching paths.
    """
    if not dir_structure:
        return {'folders': [], 'files': []}
    
    binding_folders = []
    binding_files = []
    
    # Identify which paths are folders
    folders = identify_folders(dir_structure)
    
    for path in dir_structure:
        # Skip paths in excluded folders
        if is_path_in_excluded_folder(path):
            continue
        
        # Get the name (last component of path)
        name = path.split('/')[-1]
        
        if path in folders:
            # It's a folder - check folder name pattern
            if BINDING_FOLDER_PATTERNS.search(name):
                binding_folders.append(path)
        else:
            # It's a file - check file pattern
            if BINDING_FILE_PATTERNS.search(name):
                binding_files.append(path)
    
    return {'folders': binding_folders, 'files': binding_files}


def format_tree_structure(paths: List[str]) -> str:
    """Convert flat path list to tree-like structure for display.
    
    Args:
        paths: List of file/folder paths
    
    Returns:
        String representation of directory tree
    """
    if not paths:
        return ""
    
    # Identify folders
    folders = identify_folders(paths)
    
    # Sort paths for consistent output
    sorted_paths = sorted(paths)
    
    lines = []
    for path in sorted_paths:
        depth = path.count('/')
        indent = "│   " * depth
        name = path.split('/')[-1]
        
        # Determine if last item at this level
        prefix = "├── "
        
        if path in folders:
            lines.append(f"{indent}{prefix}{name}/")
        else:
            lines.append(f"{indent}{prefix}{name}")
    
    return '\n'.join(lines)


def process_package(package: Dict, source_file: str) -> Dict:
    """Process a single package from JSON data.
    
    Args:
        package: Package dict from JSON
        source_file: Name of source JSON file
    
    Returns:
        Processed result dict
    """
    repository = package.get('repository', '')
    result_ecosystems = package.get('result_ecosystems', [])
    claimed_ecosystems = package.get('claimed_ecosystems', [])
    dir_structure = package.get('directory_structure', [])
    
    # Find binding indicators
    binding_indicators = find_binding_indicators(dir_structure)
    
    has_binding = len(binding_indicators['folders']) > 0 or len(binding_indicators['files']) > 0
    
    # Format ecosystems as comma-separated strings
    claimed_ecosystems_str = ', '.join(sorted(claimed_ecosystems))
    detected_ecosystems_str = ', '.join(sorted(result_ecosystems))
    
    return {
        'repo_url': f"https://{repository}",
        'owner_repo': '/'.join(repository.split('/')[-2:]) if '/' in repository else repository,
        'claimed_ecosystems': claimed_ecosystems_str,
        'detected_ecosystems': detected_ecosystems_str,
        'source_file': source_file,
        'has_binding': has_binding,
        'binding_folders': binding_indicators['folders'],
        'binding_files': binding_indicators['files'],
        'dir_structure': format_tree_structure(dir_structure)
    }


def process_json_file(file_path: Path) -> Dict[str, List[Dict]]:
    """Process a JSON file and categorize packages by binding pattern.
    
    Args:
        file_path: Path to JSON file
    
    Returns:
        Dict with 'binding' and 'none' keys containing package lists
    """
    results = {
        'binding': [],
        'none': []
    }
    
    data = load_json_data(file_path)
    if not data:
        return results
    
    packages = data.get('packages', [])
    source_file = file_path.name
    
    print(f"Processing {len(packages)} packages from {source_file}...")
    
    for package in tqdm(packages, desc=source_file):
        result = process_package(package, source_file)
        
        if result['has_binding']:
            results['binding'].append(result)
        else:
            results['none'].append(result)
    
    return results


def write_binding_output(results: List[Dict], output_dir: Path):
    """Write binding pattern results."""
    csv_file = output_dir / "binding_named.csv"
    detailed_file = output_dir / "binding_named_detailed.txt"
    
    # CSV output
    with open(csv_file, 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow([
            'repo_url', 'owner_repo', 'source_file', 'claimed_ecosystems', 'detected_ecosystems',
            'binding_folders', 'binding_files'
        ])
        
        for result in results:
            binding_folders = result.get('binding_folders', [])
            binding_files = result.get('binding_files', [])
            writer.writerow([
                result['repo_url'],
                result['owner_repo'],
                result['source_file'],
                result['claimed_ecosystems'],
                result['detected_ecosystems'],
                '; '.join(binding_folders[:10]),  # Limit to first 10 for CSV
                '; '.join(binding_files[:10])  # Limit to first 10 for CSV
            ])
    
    # Detailed text output
    with open(detailed_file, 'w', encoding='utf-8') as f:
        f.write("=" * 80 + "\n")
        f.write("BINDING NAMED PATTERN\n")
        f.write("=" * 80 + "\n")
        f.write(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
        f.write(f"Total Packages: {len(results)}\n")
        f.write("=" * 80 + "\n\n")
        
        for i, result in enumerate(results, 1):
            f.write(f"Package {i}/{len(results)}\n")
            f.write("-" * 80 + "\n")
            f.write(f"Repository: {result['repo_url']}\n")
            f.write(f"Owner/Repo: {result['owner_repo']}\n")
            f.write(f"Source File: {result['source_file']}\n")
            f.write(f"Claimed Ecosystems: {result['claimed_ecosystems']}\n")
            f.write(f"Detected Ecosystems: {result['detected_ecosystems']}\n\n")
            
            # Show detected binding folders as evidence
            binding_folders = result.get('binding_folders', [])
            f.write(f"Detected Binding Folders ({len(binding_folders)}):\n")
            if binding_folders:
                for folder_path in binding_folders:
                    f.write(f"  - {folder_path}/\n")
            else:
                f.write("  (none)\n")
            
            # Show detected binding files as evidence
            binding_files = result.get('binding_files', [])
            f.write(f"\nDetected Binding Files ({len(binding_files)}):\n")
            if binding_files:
                for file_path in binding_files:
                    f.write(f"  - {file_path}\n")
            else:
                f.write("  (none)\n")
            f.write("\n" + "=" * 80 + "\n\n")
    
    print(f"  Written: {csv_file}")
    print(f"  Written: {detailed_file}")


def write_summary(all_results: Dict[str, List[Dict]], output_dir: Path):
    """Write overall summary statistics."""
    summary_file = output_dir / "summary.txt"
    
    total_binding = len(all_results['binding'])
    total_none = len(all_results['none'])
    total = total_binding + total_none
    
    with open(summary_file, 'w', encoding='utf-8') as f:
        f.write("=" * 80 + "\n")
        f.write("BINDING PATTERN DETECTION SUMMARY\n")
        f.write("=" * 80 + "\n")
        f.write(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n")
        
        f.write("OVERALL STATISTICS\n")
        f.write("-" * 80 + "\n")
        f.write(f"Total Packages Analyzed: {total}\n\n")
        f.write(f"With Binding Pattern:    {total_binding:5d} ({total_binding/total*100:.2f}%)\n")
        f.write(f"No Binding Pattern:      {total_none:5d} ({total_none/total*100:.2f}%)\n")
    
    print(f"  Written: {summary_file}")


def main():
    """Main entry point."""
    print("\n" + "=" * 80)
    print("BINDING PATTERN DETECTOR")
    print("=" * 80)
    
    # Check if input file exists
    if not FULLY_MATCHED_JSON.exists():
        print(f"\nERROR: Ecosystem-Detector JSON file not found")
        print(f"Expected: {FULLY_MATCHED_JSON}")
        return
    
    # Create output directory
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    print(f"Output directory: {OUTPUT_DIR}")
    
    all_results = {
        'binding': [],
        'none': []
    }
    
    # Process fully_matched.json
    print("\n" + "=" * 80)
    print("PROCESSING FULLY MATCHED PACKAGES")
    print("=" * 80)
    fully_results = process_json_file(FULLY_MATCHED_JSON)
    
    for category in ['binding', 'none']:
        all_results[category].extend(fully_results[category])
    
    print(f"  Fully matched: {sum(len(v) for v in fully_results.values())} packages")
    
    # Write combined outputs
    print("\n" + "=" * 80)
    print("WRITING OUTPUTS")
    print("=" * 80)
    write_binding_output(all_results['binding'], OUTPUT_DIR)
    write_summary(all_results, OUTPUT_DIR)
    
    # Final statistics
    print("\n" + "=" * 80)
    print("COMPLETED")
    print("=" * 80)
    print(f"With Binding:     {len(all_results['binding']):5d} packages")
    print(f"No Binding:       {len(all_results['none']):5d} packages")
    print(f"Total:            {sum(len(v) for v in all_results.values()):5d} packages")
    print(f"\nOutput: {OUTPUT_DIR}")
    print("=" * 80)


if __name__ == "__main__":
    main()

