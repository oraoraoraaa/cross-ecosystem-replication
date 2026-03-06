#!/usr/bin/env python3
"""
Ecosystem Detector - Source Files Only

Detects ecosystems present in repository directory structures by scanning
for source file extensions ONLY. Compares detected ecosystems with registered
ecosystems from upstream data.

Process:
1. Scan directory tree for source file extensions (excluding test/, doc/, etc.)
2. Compare detected ecosystems with registered ecosystems from input
3. Result = Intersection of (Registered Ecosystems) ∩ (Detected Ecosystems)
4. Fully matched: ALL registered ecosystems are detected
5. Partially matched: 2+ (but not all) registered ecosystems are detected
6. Fully mismatched: 0 or 1 registered ecosystems are detected
7. Output summary statistics

Example:
- Registered: PyPI, Crates, NPM | Detected: .py, .rs → Partially matched (PyPI, Crates), missing NPM
- Registered: PyPI, Crates | Detected: .py, .rs, .js → Fully matched (PyPI, Crates) [intersection]
"""

import json
import csv
import re
import sys
import os
from pathlib import Path
from collections import defaultdict
from datetime import datetime
from typing import Dict, List, Set, Tuple, Optional
from tqdm import tqdm
from concurrent.futures import ProcessPoolExecutor, as_completed
import multiprocessing

# ==============================================================================
# CONFIGURATION
# ==============================================================================

# Define paths
DATA_DIR = Path(__file__).resolve().parent.parent.parent / "data"
INPUT_FILE = DATA_DIR / "directory-structures" / "directory_structures.json"
OUTPUT_DIR = DATA_DIR / "ecosystem-detection"

# Create output directory
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

# Minimum files to consider an ecosystem as "present"
MIN_FILES_FOR_ECOSYSTEM = 1

# Parallel processing configuration
MAX_WORKERS = max(1, multiprocessing.cpu_count() - 1)  # Leave one CPU free
CHUNK_SIZE = 500  # Number of packages per chunk for parallel processing

# ==============================================================================
# SOURCE FILE EXTENSIONS BY ECOSYSTEM
# ==============================================================================

SOURCE_EXTENSIONS = {
    'PyPI': ['.py', '.pyx', '.pxd', '.pyi'],
    'Crates': ['.rs'],
    'NPM': ['.js', '.jsx', '.ts', '.tsx', '.mjs', '.cjs', '.css', '.scss'],
    'Maven': ['.java', '.scala', '.kotlin', '.sc', '.kt'],
    'Ruby': ['.rb', '.rake'],
    'PHP': ['.php']
}

# .pyx: Main source file used in Cython, a language that simplifies writing C extensions
#       for Python
# .pxd: A Cython declaration file, acting like a C header for Python code
# .pyi: Python stub file or interface file, is used to provide type hints for
#       Python code without containing any actual runtime logic or implementation

# .jsx: A JavaScript file that contains JSX (JavaScript XML) syntax, primarily used in
#       the React framework for defining user interface components
# .ts: A TypeScript file, a plain text file with type-annotated JavaScript code
# .tsx: A TypeScript file that contains JSX (JavaScript XML) syntax
# .mjs: A JavaScript source code file that specifically contains an ECMAScript module
# .cjs: A JavaScript source code file that explicitly contains CommonJS (CJS) modules

# .rake: Used for files that contain Rake tasks, which are a type of task automation
#        script written in the Ruby programming language.

# Build reverse mapping: extension -> ecosystem
EXTENSION_TO_ECOSYSTEM = {}
for ecosystem, extensions in SOURCE_EXTENSIONS.items():
    for ext in extensions:
        EXTENSION_TO_ECOSYSTEM[ext] = ecosystem

# ==============================================================================
# EXCLUSION PATTERNS (folders to exclude from ecosystem detection)
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

COMPILED_EXCLUDED_PATTERNS = [re.compile(p, re.IGNORECASE) for p in EXCLUDED_FOLDERS]


# ==============================================================================
# HELPER FUNCTIONS
# ==============================================================================

def is_excluded_folder(folder_name: str) -> bool:
    """Check if a folder should be excluded from ecosystem detection."""
    for pattern in COMPILED_EXCLUDED_PATTERNS:
        if pattern.match(folder_name):
            return True
    return False


def get_file_extension(filename: str) -> str:
    """Extract file extension from filename."""
    idx = filename.rfind('.')
    if idx > 0:
        return filename[idx:].lower()
    return ''


def parse_directory_tree(dir_structure) -> Dict:
    """
    Parse a directory tree into a structured format.
    
    Supports both:
    - List of paths (new JSON format): ['src/main.py', 'src/utils/', ...]
    - Tree-formatted string (legacy format)
    
    Returns:
    {
        'files': [(path, filename), ...],
        'folders': {path: [children], ...}
    }
    """
    result = {
        'files': [],
        'folders': defaultdict(list)
    }
    
    if not dir_structure:
        return result
    
    # Handle new JSON format: list of paths
    if isinstance(dir_structure, list):
        for path in dir_structure:
            if not path:
                continue
            
            # Check if it's a folder (ends with /)
            is_folder = path.endswith('/')
            if is_folder:
                # It's a folder, record it
                folder_path = path.rstrip('/')
                parts = folder_path.rsplit('/', 1)
                if len(parts) == 2:
                    parent, name = parts
                    result['folders'][parent].append(name)
                else:
                    result['folders'][''].append(folder_path)
            else:
                # It's a file
                parts = path.rsplit('/', 1)
                if len(parts) == 2:
                    folder_path, filename = parts
                else:
                    folder_path, filename = '', path
                result['files'].append((folder_path, filename))
        
        return result
    
    # Handle legacy format: tree-formatted string
    lines = dir_structure.strip().split('\n')
    path_stack = []  # Stack of (depth, folder_name)
    
    for line in lines:
        if not line.strip():
            continue
        
        # Calculate depth based on tree characters
        clean_line = line.replace('│', ' ').replace('├──', '   ').replace('└──', '   ')
        
        # Count leading spaces
        stripped = clean_line.lstrip(' ')
        leading_spaces = len(clean_line) - len(stripped)
        depth = leading_spaces // 4
        
        # Extract the actual name
        name = line.replace('├──', '').replace('└──', '').replace('│', '').strip()
        
        if not name:
            continue
        
        # Check if it's a folder (ends with /)
        is_folder = name.endswith('/')
        if is_folder:
            name = name.rstrip('/')
        
        # Update path stack
        while path_stack and path_stack[-1][0] >= depth:
            path_stack.pop()
        
        # Build current path
        current_path = '/'.join([p[1] for p in path_stack])
        
        if is_folder:
            path_stack.append((depth, name))
            result['folders'][current_path].append(name)
        else:
            result['files'].append((current_path, name))
    
    return result


def is_path_excluded(folder_path: str) -> bool:
    """Check if any part of the path is in an excluded folder."""
    if not folder_path:
        return False
    
    path_parts = folder_path.split('/')
    for part in path_parts:
        if is_excluded_folder(part):
            return True
    return False


def detect_ecosystems_from_tree(parsed_tree: Dict) -> tuple:
    """
    Detect ecosystems present in the directory tree based on file extensions.
    
    Returns:
        Tuple of (counts_dict, paths_dict) where:
        - counts_dict: Dict mapping ecosystem name to file count
        - paths_dict: Dict mapping ecosystem name to list of file paths
    """
    ecosystem_files = defaultdict(int)
    ecosystem_paths = defaultdict(list)
    
    for folder_path, filename in parsed_tree['files']:
        # Skip files in excluded folders
        if is_path_excluded(folder_path):
            continue
        
        ext = get_file_extension(filename)
        ecosystem = EXTENSION_TO_ECOSYSTEM.get(ext)
        
        if ecosystem:
            ecosystem_files[ecosystem] += 1
            full_path = f"{folder_path}/{filename}" if folder_path else filename
            ecosystem_paths[ecosystem].append(full_path)
    
    # Filter by minimum file threshold
    filtered_counts = {
        eco: count 
        for eco, count in ecosystem_files.items() 
        if count >= MIN_FILES_FOR_ECOSYSTEM
    }
    filtered_paths = {
        eco: paths 
        for eco, paths in ecosystem_paths.items() 
        if eco in filtered_counts
    }
    return filtered_counts, filtered_paths


def compare_ecosystems(registered: List[str], detected: Dict[str, int],
                       detected_paths: Dict[str, list] = None) -> Dict:
    """
    Compare registered ecosystems with detected ecosystems.
    
    Result = Intersection of (Registered Ecosystems) ∩ (Detected Ecosystems)
    
    - Fully matched: ALL registered ecosystems are found in detected
    - Partially matched: 2+ (but not all) registered ecosystems are found
    - Fully mismatched: NONE of the registered ecosystems are found
    
    Returns:
        {
            'match_type': 'full' | 'partial' | 'none',
            'claimed': set,
            'detected': set,
            'intersection': set,  # The result ecosystems (registered ∩ detected)
            'missing_claimed': set,  # Registered but not detected
            'extra_detected': set,   # Detected but not registered
            'detected_counts': dict,
            'detected_paths': dict
        }
    """
    registered_set = set(registered)
    detected_set = set(detected.keys())
    
    # Result = intersection of registered and detected
    intersection = registered_set & detected_set
    missing_registered = registered_set - detected_set
    extra_detected = detected_set - registered_set
    
    # Determine match type
    # Fully matched: ALL registered ecosystems are detected
    # Partially matched: 2+ (but not all) registered ecosystems are detected
    # Fully mismatched: 0 or 1 registered ecosystems are detected
    if len(intersection) == len(registered_set):
        match_type = 'full'
    elif len(intersection) >= 2:
        match_type = 'partial'
    else:
        match_type = 'none'
    
    return {
        'match_type': match_type,
        'claimed': registered_set,
        'detected': detected_set,
        'intersection': intersection,
        'missing_claimed': missing_registered,
        'extra_detected': extra_detected,
        'detected_counts': detected,
        'detected_paths': detected_paths or {}
    }


# ==============================================================================
# PARSING FUNCTIONS
# ==============================================================================

def extract_ecosystems_from_filename(filename: str) -> List[str]:
    """
    Extract ecosystems from filename.
    
    Examples:
        'Crates_NPM.json' -> ['Crates', 'NPM']
        'PyPI.json' -> ['PyPI']
        'Maven_NPM_PyPI.json' -> ['Maven', 'NPM', 'PyPI']
    """
    # Remove .json or .txt extension
    name_without_ext = filename.replace('.json', '').replace('.txt', '')
    
    # Split by underscore
    parts = name_without_ext.split('_')
    
    # Known ecosystem names (case-sensitive matching)
    known_ecosystems = {'Crates', 'Maven', 'NPM', 'PHP', 'PyPI', 'Ruby'}
    
    # Filter to only valid ecosystems
    ecosystems = [part for part in parts if part in known_ecosystems]
    
    return ecosystems


def process_json_file(file_path: Path) -> List[Dict]:
    """Process a single JSON directory structure file from Directory-Structure-Miner."""
    results = []
    
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
    except Exception as e:
        print(f"\nError reading {file_path}: {e}")
        return results
    
    # Extract expected ecosystems from metadata or filename
    metadata = data.get('metadata', {})
    expected_ecosystems = metadata.get('claimed_ecosystems', [])
    if not expected_ecosystems:
        expected_ecosystems = extract_ecosystems_from_filename(file_path.name)
    
    packages = data.get('packages', [])
    
    for package in packages:
        repository = package.get('repository', '')
        registered_ecosystems = package.get('claimed_ecosystems', expected_ecosystems)
        dir_structure = package.get('directory_structure', [])
        
        if not repository:
            continue
        
        # Extract owner/repo from repository URL
        owner_repo = repository
        if owner_repo.startswith('github.com/'):
            owner_repo = owner_repo.replace('github.com/', '')
        
        # Parse directory tree
        parsed_tree = parse_directory_tree(dir_structure)
        
        # Detect ecosystems from file extensions (source files only)
        detected_ecosystems, detected_paths = detect_ecosystems_from_tree(parsed_tree)
        
        # Compare with expected ecosystems
        comparison = compare_ecosystems(registered_ecosystems, detected_ecosystems, detected_paths)
        
        results.append({
            'repository': repository,
            'owner_repo': owner_repo,
            'claimed_ecosystems': registered_ecosystems,
            'detected_ecosystems': detected_ecosystems,
            'detected_paths': detected_paths,
            'comparison': comparison,
            'dir_structure': dir_structure,
            'source_file': file_path.name
        })
    
    return results


def process_single_package(package: Dict) -> Optional[Dict]:
    """
    Process a single package for ecosystem detection.
    This function is designed to be called by parallel workers.
    
    Args:
        package: Package dictionary with repository, registered_ecosystems, directory_structure
        
    Returns:
        Result dictionary or None if processing failed
    """
    try:
        repository = package.get('repository', '')
        registered_ecosystems = package.get('claimed_ecosystems', [])
        dir_structure = package.get('directory_structure', [])
        
        if not repository:
            return None
        
        # Extract owner/repo from repository URL
        owner_repo = repository
        if owner_repo.startswith('github.com/'):
            owner_repo = owner_repo.replace('github.com/', '')
        
        # Parse directory tree
        parsed_tree = parse_directory_tree(dir_structure)
        
        detected_ecosystems, detected_paths = detect_ecosystems_from_tree(parsed_tree)
        comparison = compare_ecosystems(registered_ecosystems, detected_ecosystems, detected_paths)

        return {
            'repository': repository,
            'owner_repo': owner_repo,
            'claimed_ecosystems': registered_ecosystems,
            'detected_ecosystems': detected_ecosystems,
            'detected_paths': detected_paths,
            'comparison': comparison,
            'dir_structure': dir_structure,
            'source_file': 'directory_structures.json'
        }
    except Exception:
        return None


# ==============================================================================
# MAIN PROCESSING
# ==============================================================================

def process_all_files() -> Dict:
    """
    Process directory structures from the single directory_structures.json file.

    Returns:
        {
            'fully_matched': [...],     # ALL registered ecosystems are detected
            'partially_matched': [...], # 2+ (but not all) registered ecosystems are detected
            'fully_mismatched': [...]   # NONE of the registered ecosystems are detected
        }
    """
    all_results = {
        'fully_matched': [],
        'partially_matched': [],
        'fully_mismatched': []
    }
    
    # Track processed repos to avoid duplicates
    processed_repos = set()
    
    # Check if input file exists
    if not INPUT_FILE.exists():
        print(f"Error: Input file not found: {INPUT_FILE}")
        return all_results
    
    # Load the single JSON file
    print(f"\nLoading: {INPUT_FILE}")
    try:
        with open(INPUT_FILE, 'r', encoding='utf-8') as f:
            data = json.load(f)
    except Exception as e:
        print(f"Error reading {INPUT_FILE}: {e}")
        return all_results
    
    packages = data.get('packages', [])
    metadata = data.get('metadata', {})
    
    print(f"Total packages in file: {len(packages):,}")
    if metadata:
        print(f"Generated: {metadata.get('generated', 'N/A')}")
        print(f"Cache hits: {metadata.get('cache_hits', 'N/A')}")
    
    # Filter out duplicates first
    unique_packages = []
    for package in packages:
        repository = package.get('repository', '')
        if repository and repository not in processed_repos:
            processed_repos.add(repository)
            unique_packages.append(package)
    
    print(f"Unique packages to process: {len(unique_packages):,}")
    print(f"Using {MAX_WORKERS} parallel workers...")
    sys.stdout.flush()
    
    # Process packages in parallel
    with ProcessPoolExecutor(max_workers=MAX_WORKERS) as executor:
        # Submit all packages for processing
        futures = {executor.submit(process_single_package, pkg): pkg for pkg in unique_packages}
        
        # Collect results with progress bar
        for future in tqdm(as_completed(futures), total=len(futures), 
                          desc="Processing", mininterval=0.5, dynamic_ncols=True, file=sys.stdout):
            try:
                result = future.result()
                if result is None:
                    continue
                
                comparison = result['comparison']
                match_type = comparison['match_type']

                if match_type == 'full':
                    all_results['fully_matched'].append(result)
                elif match_type == 'partial':
                    all_results['partially_matched'].append(result)
                else:
                    all_results['fully_mismatched'].append(result)

            except Exception:
                pass

    return all_results


def write_fully_matched_output(results: List[Dict], output_dir: Path):
    """Write fully matched packages to a single JSON file."""
    output_file = output_dir / "fully_matched.json"
    
    # Prepare JSON output
    output_data = {
        'metadata': {
            'generated': datetime.now().isoformat(),
            'detection_method': 'source_files_only',
            'total_packages': len(results),
            'match_type': 'fully_matched',
            'note': 'All registered ecosystems are detected'
        },
        'packages': []
    }
    
    for result in results:
        comp = result['comparison']
        package_data = {
            'repository': result['repository'],
            'claimed_ecosystems': result['claimed_ecosystems'],
            'detected_ecosystems': list(comp['detected']),
            'result_ecosystems': sorted(list(comp['intersection'])),
            'extra_detected': sorted(list(comp['extra_detected'])),
            'detected_ecosystem_counts': result['detected_ecosystems'],
            'detected_paths': result['detected_paths'],
            'directory_structure': result['dir_structure']
        }
        output_data['packages'].append(package_data)
    
    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(output_data, f, indent=2, ensure_ascii=False)
    
    print(f"  Written fully matched packages to: {output_file}")
    print(f"  Total packages: {len(results)}")


def write_partially_matched_output(results: List[Dict], output_dir: Path):
    """Write partially matched packages to a single JSON file."""
    output_file = output_dir / "partially_matched.json"
    
    # Prepare JSON output
    output_data = {
        'metadata': {
            'generated': datetime.now().isoformat(),
            'detection_method': 'source_files_only',
            'total_packages': len(results),
            'match_type': 'partially_matched',
            'note': 'Some but not all registered ecosystems are detected (2+ ecosystems matched)'
        },
        'packages': []
    }
    
    for result in results:
        comp = result['comparison']
        package_data = {
            'repository': result['repository'],
            'claimed_ecosystems': result['claimed_ecosystems'],
            'detected_ecosystems': list(comp['detected']),
            'result_ecosystems': sorted(list(comp['intersection'])),
            'missing_claimed': sorted(list(comp['missing_claimed'])),
            'extra_detected': sorted(list(comp['extra_detected'])),
            'detected_ecosystem_counts': result['detected_ecosystems'],
            'detected_paths': result['detected_paths'],
            'directory_structure': result['dir_structure']
        }
        output_data['packages'].append(package_data)
    
    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(output_data, f, indent=2, ensure_ascii=False)
    
    print(f"  Written partially matched packages to: {output_file}")
    print(f"  Total packages: {len(results)}")


def write_fully_mismatched_output(results: List[Dict], output_dir: Path):
    """Write fully mismatched packages to a CSV file."""
    output_file = output_dir / "fully_mismatched.csv"
    
    # Prepare CSV output
    with open(output_file, 'w', encoding='utf-8', newline='') as f:
        writer = csv.writer(f)
        
        # Write header
        writer.writerow([
            'Repository',
            'Registered Ecosystems',
            'Detected Ecosystems',
            'Result Ecosystems',
            'Missing Registered',
            'Extra Detected',
            'Registered Count',
            'Detected Count',
            'Result Count',
            'Detected Ecosystem File Counts'
        ])
        
        # Write data rows
        for result in results:
            comp = result['comparison']
            
            # Format ecosystems as comma-separated strings
            registered_str = ', '.join(sorted(result['claimed_ecosystems']))
            detected_str = ', '.join(sorted(comp['detected']))
            result_str = ', '.join(sorted(comp['intersection']))
            missing_str = ', '.join(sorted(comp['missing_claimed']))
            extra_str = ', '.join(sorted(comp['extra_detected']))
            
            # Format detected counts as "Ecosystem: count" pairs
            counts_parts = []
            for eco in sorted(result['detected_ecosystems'].keys()):
                count = result['detected_ecosystems'][eco]
                counts_parts.append(f"{eco}: {count}")
            counts_str = ', '.join(counts_parts) if counts_parts else 'None'
            
            writer.writerow([
                result['repository'],
                registered_str,
                detected_str,
                result_str if result_str else 'None',
                missing_str if missing_str else 'None',
                extra_str if extra_str else 'None',
                len(comp['claimed']),
                len(comp['detected']),
                len(comp['intersection']),
                counts_str
            ])
    
    print(f"  Written fully mismatched packages to: {output_file}")
    print(f"  Total packages: {len(results)}")


def write_summary(all_results: Dict, output_dir: Path):
    """Write source-file ecosystem detection summary."""
    summary_file = output_dir / "summary.txt"

    fully_matched = all_results['fully_matched']
    partially_matched = all_results['partially_matched']
    fully_mismatched = all_results['fully_mismatched']
    total = len(fully_matched) + len(partially_matched) + len(fully_mismatched)

    if total == 0:
        print("  No packages processed, skipping summary.")
        return

    # Individual ecosystem statistics
    ecosystem_stats = {eco: {'claimed': 0, 'detected': 0, 'matched': 0}
                       for eco in SOURCE_EXTENSIONS.keys()}

    for result in fully_matched + partially_matched + fully_mismatched:
        comp = result['comparison']
        for eco in comp['claimed']:
            ecosystem_stats[eco]['claimed'] += 1
        for eco in comp['detected']:
            ecosystem_stats[eco]['detected'] += 1
        for eco in comp['intersection']:
            ecosystem_stats[eco]['matched'] += 1

    with open(summary_file, 'w', encoding='utf-8') as f:
        f.write("=" * 80 + "\n")
        f.write("ECOSYSTEM DETECTION SUMMARY (SOURCE FILES ONLY)\n")
        f.write("=" * 80 + "\n")
        f.write(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
        f.write(f"Source: {INPUT_FILE}\n")
        f.write("Detection Method: Source Files Only (no config file check)\n")
        f.write("Result: Intersection of (Registered \u2229 Detected)\n\n")

        f.write("OVERALL STATISTICS\n")
        f.write("-" * 80 + "\n")
        f.write(f"Total Packages Processed:   {total:6d}\n")
        f.write(f"Fully Matched:              {len(fully_matched):6d} ({len(fully_matched)/total*100:.1f}%)\n")
        f.write(f"Partially Matched:          {len(partially_matched):6d} ({len(partially_matched)/total*100:.1f}%)\n")
        f.write(f"Fully Mismatched:           {len(fully_mismatched):6d} ({len(fully_mismatched)/total*100:.1f}%)\n\n")

        f.write("Legend:\n")
        f.write("  - Fully Matched: ALL registered ecosystems are detected\n")
        f.write("  - Partially Matched: 2+ (but not all) registered ecosystems are detected\n")
        f.write("  - Fully Mismatched: 0 or 1 registered ecosystems are detected\n\n")

        f.write("=" * 80 + "\n")
        f.write("INDIVIDUAL ECOSYSTEM STATISTICS\n")
        f.write("=" * 80 + "\n")
        f.write(f"{'Ecosystem':<12} {'Registered':<12} {'Detected':<12} {'Matched':<12} {'Match Rate'}\n")
        f.write("-" * 80 + "\n")
        for eco in sorted(ecosystem_stats.keys()):
            s = ecosystem_stats[eco]
            match_rate = (s['matched'] / s['claimed'] * 100) if s['claimed'] > 0 else 0
            f.write(f"{eco:<12} {s['claimed']:<12} {s['detected']:<12} {s['matched']:<12} {match_rate:>6.1f}%\n")
        f.write("\n")

        f.write("=" * 80 + "\n")
        f.write("DETECTION CONFIGURATION\n")
        f.write("=" * 80 + "\n")
        f.write(f"Minimum files for ecosystem: {MIN_FILES_FOR_ECOSYSTEM}\n")
        f.write(f"Excluded folder patterns: {len(EXCLUDED_FOLDERS)}\n")
        f.write("\nSupported Ecosystems and Extensions:\n")
        for eco, exts in sorted(SOURCE_EXTENSIONS.items()):
            f.write(f"  {eco}: {', '.join(exts)}\n")
        f.write("\nExcluded Folder Patterns:\n")
        for pattern in EXCLUDED_FOLDERS[:10]:
            f.write(f"  {pattern}\n")
        if len(EXCLUDED_FOLDERS) > 10:
            f.write(f"  ... and {len(EXCLUDED_FOLDERS) - 10} more patterns\n")

    print(f"  Written summary: {summary_file}")


def main():
    """Main entry point."""
    print("=" * 80)
    print("ECOSYSTEM DETECTOR - SOURCE FILES ONLY")
    print("=" * 80)
    print(f"Input: {INPUT_FILE}")
    print(f"Output: {OUTPUT_DIR}")
    print(f"Min files for ecosystem: {MIN_FILES_FOR_ECOSYSTEM}")
    print(f"Parallel workers: {MAX_WORKERS}")
    print("Detection Method: Source Files Only (no config file check)")
    print("Result: Intersection of (Registered ∩ Detected)")
    print("=" * 80)
    
    all_results = process_all_files()

    print("\n" + "=" * 80)
    print("WRITING OUTPUTS")
    print("=" * 80)

    print("\nFully matched packages (all registered ecosystems detected):")
    write_fully_matched_output(all_results['fully_matched'], OUTPUT_DIR)

    print("\nPartially matched packages (2+ registered ecosystems detected):")
    write_partially_matched_output(all_results['partially_matched'], OUTPUT_DIR)

    print("\nFully mismatched packages (0 or 1 registered ecosystems detected):")
    write_fully_mismatched_output(all_results['fully_mismatched'], OUTPUT_DIR)

    print("\nSummary:")
    write_summary(all_results, OUTPUT_DIR)

    total = (len(all_results['fully_matched']) + len(all_results['partially_matched'])
             + len(all_results['fully_mismatched']))

    print("\n" + "=" * 80)
    print("COMPLETED")
    print("=" * 80)

    if total == 0:
        print("No packages processed.")
    else:
        print(f"Total Processed:      {total:6d} packages")
        print(f"  Fully Matched:      {len(all_results['fully_matched']):6d} packages ({len(all_results['fully_matched'])/total*100:.1f}%)")
        print(f"  Partially Matched:  {len(all_results['partially_matched']):6d} packages ({len(all_results['partially_matched'])/total*100:.1f}%)")
        print(f"  Fully Mismatched:   {len(all_results['fully_mismatched']):6d} packages ({len(all_results['fully_mismatched'])/total*100:.1f}%)")
    print("=" * 80)


if __name__ == "__main__":
    main()
