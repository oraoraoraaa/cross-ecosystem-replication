#!/usr/bin/env python3
"""
Language-Specific Named Folder Detector - Hybrid Approach

Detects cross-ecosystem packages that organize code by language-named folders:
- go/, python/, rust/, java/, etc.

Classification (Hybrid Approach - Completeness + Quality):
- Concentrated-Complete: All ecosystem folders found AND all highly concentrated (>= 80%)
- Concentrated-Partial: Some ecosystem folders found AND all found folders highly concentrated (>= 80%)
- Mixed: Some folders >= 40%, but not all >= 80%
- Low: All found folders < 40%
- No-Language-Specific-Folder: No language-specific named folders found
"""

import csv
import re
import sys
from pathlib import Path
from collections import defaultdict, Counter
from datetime import datetime
from typing import Dict, List, Tuple, Optional, Set
from tqdm import tqdm
import json

# ==============================================================================
# CONFIGURATION
# ==============================================================================

# Define paths
DATA_DIR = Path(__file__).resolve().parent.parent.parent / "data"

# Input sources from ecosystem detection (JSON files)
FULLY_MATCHED_JSON = DATA_DIR / "ecosystem-detection" / "fully_matched.json"

# Base output directory
OUTPUT_BASE_DIR = DATA_DIR / "analysis" / "naming-convention"

# Thresholds for classification
THRESHOLD_CLEAN = 0.80      # >= 80% for Concentrated (Clean)
THRESHOLD_PARTIAL = 0.40    # >= 40% for Mixed (Partial)
MIN_FILES_THRESHOLD = 1     # Minimum files to consider a valid detection

# Detection modes
DETECTION_MODE_DEFAULT = 'count>=2'      
DETECTION_MODE_COUNT = 'count>=2'       # Language-specific folder count >= 2
DETECTION_MODE_DEPTH = 'count>=2_same_depth'  # Count >= 2 AND same depth

# Current detection mode (will be set via command-line argument)
DETECTION_MODE = DETECTION_MODE_DEFAULT

# ==============================================================================
# FOLDER NAME VOCABULARY (Expanded)
# ==============================================================================

# Language-specific folder patterns for each ecosystem (case-insensitive matching)
LANGUAGE_SPECIFIC_FOLDER_PATTERNS = {
    'PyPI': [
        r'^python$', r'^py$', r'^pypi$', r'^pysrc$', r'^python[-_]?src$',
        r'^python[-_]?lib$', r'^pylib$'
    ],
    'Crates': [
        r'^rust$', r'^cargo$', r'^crates?$', r'^rs$', r'^rust[-_]?src$',
        r'^rustlib$'
    ],
    'Go': [
        r'^go$', r'^golang$', r'^go[-_]?src$', r'^golib$'
    ],
    'NPM': [
        r'^js$', r'^javascript$', r'^node$', r'^nodejs$', r'^npm$',
        r'^ts$', r'^typescript$', r'^jssrc$', r'^node[-_]?src$'
    ],
    'Maven': [
        r'^java$', r'^jvm$', r'^maven$', r'^scala$', r'^kotlin$',
        r'^javasrc$', r'^java[-_]?src$'
    ],
    'Ruby': [
        r'^ruby$', r'^rb$', r'^rubygem$', r'^rubylib$'
    ],
    'PHP': [
        r'^php$', r'^phpsrc$', r'^php[-_]?src$', r'^phplib$'
    ]
}

# ==============================================================================
# SOURCE FILE EXTENSIONS
# ==============================================================================

SOURCE_EXTENSIONS = {
    'PyPI': ['.py', '.pyx', '.pxd', '.pyi'],
    'Crates': ['.rs'],
    'Go': ['.go'],
    'NPM': ['.js', '.jsx', '.ts', '.tsx', '.mjs', '.cjs', '.css', '.scss'],
    'Maven': ['.java', '.scala', '.kotlin', '.sc', '.kt'],
    'Ruby': ['.rb', '.rake'],
    'PHP': ['.php']
}

# ==============================================================================
# EXCLUSION PATTERNS (folders to exclude from percentage calculation)
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


# ==============================================================================
# HELPER FUNCTIONS
# ==============================================================================

def compile_patterns():
    """Compile all regex patterns for efficiency."""
    compiled = {
        'lang_specific': {},
        'excluded': [re.compile(p, re.IGNORECASE) for p in EXCLUDED_FOLDERS]
    }
    
    for ecosystem, patterns in LANGUAGE_SPECIFIC_FOLDER_PATTERNS.items():
        compiled['lang_specific'][ecosystem] = [re.compile(p, re.IGNORECASE) for p in patterns]
    
    return compiled


def is_excluded_folder(folder_name: str, compiled_patterns: dict) -> bool:
    """Check if a folder should be excluded from analysis."""
    for pattern in compiled_patterns['excluded']:
        if pattern.match(folder_name):
            return True
    return False


def get_file_extension(filename: str) -> str:
    """Extract file extension from filename."""
    idx = filename.rfind('.')
    if idx > 0:
        return filename[idx:].lower()
    return ''


def get_ecosystem_for_extension(extension: str) -> Optional[str]:
    """Get the ecosystem for a given file extension."""
    for ecosystem, extensions in SOURCE_EXTENSIONS.items():
        if extension in extensions:
            return ecosystem
    return None


def analyze_file_distribution(parsed_tree: Dict, compiled_patterns: dict) -> Dict:
    """
    Analyze file distribution for each ecosystem.
    
    Step 1: Parse ALL files and detect their ecosystems from file extensions
    Step 2: Find ALL language-specific folders (python/, java/, rust/, etc.)
    Step 3: Calculate proportions for classification
    
    Returns:
    {
        ecosystem: {
            'total_files': int,
            'language_specific_folders': {
                folder_path: {
                    'files_inside': int,
                    'coverage': float
                }
            },
            'file_locations': {
                folder_path: int  # file count
            }
        }
    }
    """
    
    # STEP 1: Parse ALL files and categorize by detected ecosystem
    # Build a map: ecosystem -> [(folder_path, filename), ...]
    all_ecosystem_files = defaultdict(list)
    
    for folder_path, filename in parsed_tree['files']:
        ext = get_file_extension(filename)
        detected_ecosystem = get_ecosystem_for_extension(ext)
        
        if detected_ecosystem:
            # Check if file is in excluded folder
            path_parts = folder_path.split('/') if folder_path else []
            excluded = False
            for part in path_parts:
                if is_excluded_folder(part, compiled_patterns):
                    excluded = True
                    break
            
            if not excluded:
                all_ecosystem_files[detected_ecosystem].append((folder_path, filename))
    
    # STEP 2: Find ALL language-specific folders (for any ecosystem)
    # Search all folders and identify which ecosystem each language-specific folder is for
    all_lang_specific_folders = defaultdict(list)  # ecosystem -> [folder_paths]
    
    def search_all_lang_specific_folders(parent_path: str, depth: int = 0):
        """Search for ALL language-specific named folders at any depth."""
        folders = parsed_tree['folders'].get(parent_path, [])
        
        for folder in folders:
            # Skip excluded folders (tests, docs, build, etc.)
            if is_excluded_folder(folder, compiled_patterns):
                continue
            
            # Build full path
            if parent_path:
                full_path = f"{parent_path}/{folder}"
            else:
                full_path = folder
            
            # Check if this folder matches language-specific folder patterns for ANY ecosystem
            for eco, patterns in compiled_patterns['lang_specific'].items():
                for pattern in patterns:
                    if pattern.match(folder):
                        all_lang_specific_folders[eco].append({
                            'path': full_path,
                            'depth': depth
                        })
                        break  # Found a match for this ecosystem, no need to check other patterns
            
            # Recursively search subfolders (limit depth to avoid excessive recursion)
            if depth < 5:
                search_all_lang_specific_folders(full_path, depth + 1)
    
    # Start recursive search from root
    search_all_lang_specific_folders('', 0)
    
    # STEP 3: Calculate proportions for each detected ecosystem
    result = {}
    
    # Process only ecosystems detected from actual files
    all_relevant_ecosystems = set(all_ecosystem_files.keys())
    
    for ecosystem in all_relevant_ecosystems:
        ecosystem_files = all_ecosystem_files.get(ecosystem, [])
        total_files = len(ecosystem_files)
        
        # Get language-specific folders for this ecosystem
        lang_specific_folders = {}
        for folder_info in all_lang_specific_folders.get(ecosystem, []):
            folder_path = folder_info['path']
            folder_path_lower = folder_path.lower()
            
            # Count files inside this language-specific folder (case-insensitive comparison)
            files_inside = sum(
                1 for (fpath, _) in ecosystem_files
                if fpath.lower().startswith(folder_path_lower + '/') or fpath.lower() == folder_path_lower
            )
            
            lang_specific_folders[folder_path] = {
                'files_inside': files_inside,
                'coverage': files_inside / total_files if total_files > 0 else 0,
                'depth': folder_info['depth']
            }
        
        # Count files by location (for finding actual concentration)
        file_locations = Counter()
        for folder_path, _ in ecosystem_files:
            if folder_path:
                # Use complete folder path for thorough distribution
                file_locations[folder_path] += 1
            else:
                file_locations['<root>'] += 1
        
        result[ecosystem] = {
            'total_files': total_files,
            'language_specific_folders': lang_specific_folders,
            'file_locations': dict(file_locations)
        }
    
    return result


def classify_package(analysis: Dict, detection_mode: str = DETECTION_MODE_DEFAULT) -> Dict:
    """
    Classify a package based on language-specific folder analysis (Hybrid Approach).
    
    Classification logic (Hybrid: Completeness + Quality):
    - Concentrated-Complete: All ecosystem folders found AND all highly concentrated (>= 80%)
    - Concentrated-Partial: Some ecosystem folders found AND all found folders highly concentrated (>= 80%)
    - Mixed: Some folders >= 40%, but not all >= 80%
    - Low: All found folders < 40%
    - No-Language-Specific-Folder: No language-specific named folders found
    
    Detection modes:
    - default: No additional filtering
    - count>=2: Language-specific folder count must be >= 2
    - count>=2_same_depth: Count >= 2 AND all folders at same depth
    
    Returns:
    {
        'pattern': 'Concentrated-Complete' | 'Concentrated-Partial' | 'Mixed' | 'Low' | 'No-Language-Specific-Folder',
        'language_specific_folders_found': [
            {
                'ecosystem': str,
                'folder': str,
                'coverage': float,
                'files_in_folder': int,
                'total_files': int
            }
        ],
        'file_distribution': {ecosystem: {folder: count, ...}, ...}
    }
    """
    result = {
        'language_specific_folders_found': [],
        'file_distribution': {}
    }
    
    # Determine which ecosystems are present (detected from files)
    detected_ecosystems = set(analysis.keys())
    
    # Track which ecosystems have language-specific folders
    ecosystems_with_folders = set()
    all_lang_specific_folders = []
    
    for ecosystem, eco_analysis in analysis.items():
        total_files = eco_analysis.get('total_files', 0)
        lang_specific_folders = eco_analysis.get('language_specific_folders', {})
        file_locations = eco_analysis.get('file_locations', {})
        
        # Store file distribution
        result['file_distribution'][ecosystem] = file_locations
        
        # Skip if insufficient files
        if total_files < MIN_FILES_THRESHOLD:
            continue
        
        # Process language-specific folders for this ecosystem
        if lang_specific_folders:
            ecosystems_with_folders.add(ecosystem)
            
            for folder_path, folder_data in lang_specific_folders.items():
                coverage = folder_data['coverage']
                files_inside = folder_data['files_inside']
                
                all_lang_specific_folders.append({
                    'ecosystem': ecosystem,
                    'folder': folder_path,
                    'coverage': coverage,
                    'files_in_folder': files_inside,
                    'total_files': total_files,
                    'depth': folder_data.get('depth', 0)
                })
    
    result['language_specific_folders_found'] = all_lang_specific_folders
    
    # Apply detection mode filtering
    if detection_mode == DETECTION_MODE_COUNT:
        # Mode: Count >= 2
        if len(all_lang_specific_folders) < 2:
            result['pattern'] = 'No-Language-Specific-Folder'
            result['reason'] = f'Detection mode requires >= 2 language-specific folders, found {len(all_lang_specific_folders)}'
            return result
    elif detection_mode == DETECTION_MODE_DEPTH:
        # Mode: Count >= 2 AND same depth
        if len(all_lang_specific_folders) < 2:
            result['pattern'] = 'No-Language-Specific-Folder'
            result['reason'] = f'Detection mode requires >= 2 language-specific folders, found {len(all_lang_specific_folders)}'
            return result
        
        # Check if all folders have the same depth
        depths = [df['depth'] for df in all_lang_specific_folders]
        if len(set(depths)) > 1:
            result['pattern'] = 'No-Language-Specific-Folder'
            result['reason'] = f'Detection mode requires same depth for all folders, found depths: {sorted(set(depths))}'
            return result
    
    # Classification logic (Hybrid Approach)
    if not all_lang_specific_folders:
        # No language-specific folders found
        result['pattern'] = 'No-Language-Specific-Folder'
        result['reason'] = 'No language-specific named folders found'
    else:
        # Check completeness and quality
        found_all_folders = (ecosystems_with_folders == detected_ecosystems)
        all_highly_concentrated = all(df['coverage'] >= THRESHOLD_CLEAN for df in all_lang_specific_folders)
        any_partial = any(df['coverage'] >= THRESHOLD_PARTIAL for df in all_lang_specific_folders)
        
        if found_all_folders and all_highly_concentrated:
            # Concentrated-Complete: All folders found + all >= 80%
            result['pattern'] = 'Concentrated-Complete'
            result['reason'] = f"All {len(detected_ecosystems)} ecosystem folder(s) found, all highly concentrated (>= 80%)"
        elif all_highly_concentrated:
            # Concentrated-Partial: Some folders found + all found >= 80%
            missing_count = len(detected_ecosystems) - len(ecosystems_with_folders)
            result['pattern'] = 'Concentrated-Partial'
            result['reason'] = f"All {len(all_lang_specific_folders)} found folder(s) highly concentrated (>= 80%), but {missing_count} ecosystem(s) without folders"
        elif any_partial:
            # Mixed: Some folders >= 40%, not all >= 80%
            clean_count = sum(1 for df in all_lang_specific_folders if df['coverage'] >= THRESHOLD_CLEAN)
            partial_count = sum(1 for df in all_lang_specific_folders if THRESHOLD_PARTIAL <= df['coverage'] < THRESHOLD_CLEAN)
            result['pattern'] = 'Mixed'
            result['reason'] = f"Mixed quality: {clean_count} clean (>=80%), {partial_count} partial (>=40%)"
        else:
            # Low: All folders < 40%
            result['pattern'] = 'Low'
            max_coverage = max(df['coverage'] for df in all_lang_specific_folders)
            result['reason'] = f"Low coverage: {len(all_lang_specific_folders)} folder(s) found but all < 40% (max: {max_coverage:.1%})"
    
    return result


# ==============================================================================
# MAIN PROCESSING FUNCTIONS
# ==============================================================================

def build_file_list_from_detected_paths(detected_paths: Dict[str, List[str]]) -> List[Tuple[str, str]]:
    """
    Build a file list from detected_paths in JSON format.
    
    Args:
        detected_paths: Dict mapping ecosystem to list of file paths
        
    Returns:
        List of (folder_path, filename) tuples
    """
    files = []
    for ecosystem, paths in detected_paths.items():
        for path in paths:
            if '/' in path:
                folder_path = '/'.join(path.split('/')[:-1])
                filename = path.split('/')[-1]
            else:
                folder_path = ''
                filename = path
            files.append((folder_path, filename))
    return files


def build_folder_structure_from_paths(detected_paths: Dict[str, List[str]]) -> Dict[str, List[str]]:
    """
    Build folder structure from detected_paths.
    
    Args:
        detected_paths: Dict mapping ecosystem to list of file paths
        
    Returns:
        Dict mapping parent folder to list of child folders
    """
    folders = defaultdict(set)
    
    for ecosystem, paths in detected_paths.items():
        for path in paths:
            parts = path.split('/')
            # Build folder hierarchy
            for i in range(len(parts) - 1):  # Exclude filename
                parent = '/'.join(parts[:i]) if i > 0 else ''
                child = parts[i]
                folders[parent].add(child)
    
    # Convert sets to lists
    return {k: list(v) for k, v in folders.items()}


def process_package_from_json(package_data: Dict, compiled_patterns: dict, source_type: str, detection_mode: str = DETECTION_MODE_DEFAULT) -> Optional[Dict]:
    """
    Process a single package from JSON data.
    
    Args:
        package_data: Package dict from JSON with repository, detected_paths, etc.
        compiled_patterns: Compiled regex patterns
        source_type: 'fully_matched'
        
    Returns:
        Processed result dict or None if invalid
    """
    repository = package_data.get('repository', '')
    detected_paths = package_data.get('detected_paths', {})
    claimed_ecosystems = package_data.get('claimed_ecosystems', [])
    
    if not repository or not detected_paths:
        return None
    
    # Extract owner/repo from repository URL
    # Format: github.com/owner/repo
    owner_repo = '/'.join(repository.split('/')[-2:]) if '/' in repository else repository
    
    # Build parsed tree structure from detected_paths
    files = build_file_list_from_detected_paths(detected_paths)
    folders = build_folder_structure_from_paths(detected_paths)
    
    parsed_tree = {
        'files': files,
        'folders': folders
    }
    
    # Analyze file distribution
    analysis = analyze_file_distribution(parsed_tree, compiled_patterns)
    
    # Classify package based on language-specific folder analysis
    classification = classify_package(analysis, detection_mode)
    
    # Determine detected ecosystems from analysis
    detected_ecosystems = sorted(analysis.keys())
    
    return {
        'repo_url': f"https://{repository}",
        'owner_repo': owner_repo,
        'detected_ecosystems': detected_ecosystems,
        'claimed_ecosystems': claimed_ecosystems,
        'analysis': analysis,
        'classification': classification,
        'source_type': source_type
    }


def process_json_file(json_path: Path, compiled_patterns: dict, source_type: str, detection_mode: str = DETECTION_MODE_DEFAULT) -> Dict[str, List[Dict]]:
    """
    Process a JSON file containing package data.
    
    Args:
        json_path: Path to fully_matched.json
        compiled_patterns: Compiled regex patterns
        source_type: 'fully_matched'
        
    Returns:
        Dict mapping pattern names to lists of results
    """
    all_results = {
        'Concentrated-Complete': [],  # All folders found + all >= 80%
        'Concentrated-Partial': [],   # Some folders found + all found >= 80%
        'Mixed': [],                  # Some folders >= 40%, not all >= 80%
        'Low': [],                    # All folders < 40%
        'No-Language-Specific-Folder': []  # No language-specific folders
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
    
    for package_data in tqdm(packages, desc=source_type):
        result = process_package_from_json(package_data, compiled_patterns, source_type, detection_mode)
        
        if not result:
            continue
        
        repo_url = result['repo_url']
        
        # Skip duplicates
        if repo_url in processed_repos:
            continue
        processed_repos.add(repo_url)
        
        # Add to appropriate category
        pattern = result['classification']['pattern']
        all_results[pattern].append(result)
    
    return all_results


def write_concentrated_output(results: List[Dict], output_dir: Path, subpattern: str = 'Complete'):
    """Write Concentrated results (Complete or Partial)."""
    suffix = subpattern.lower()
    output_file = output_dir / f"concentrated_{suffix}.csv"
    detailed_file = output_dir / f"concentrated_{suffix}_detailed.txt"
    
    # CSV output
    with open(output_file, 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow([
            'repo_url', 'owner_repo', 'source_type',
            'detected_ecosystems', 'claimed_ecosystems', 'ecosystem', 'language_specific_folder', 'coverage', 
            'files_in_folder', 'total_files'
        ])
        
        for result in results:
            ecosystems_str = ', '.join(result['detected_ecosystems'])
            claimed_str = ', '.join(result.get('claimed_ecosystems', []))
            
            for df_info in result['classification']['language_specific_folders_found']:
                writer.writerow([
                    result['repo_url'],
                    result['owner_repo'],
                    result.get('source_type', 'unknown'),
                    ecosystems_str,
                    claimed_str,
                    df_info['ecosystem'],
                    df_info['folder'],
                    f"{df_info['coverage']:.2%}",
                    df_info['files_in_folder'],
                    df_info['total_files']
                ])
    
    # Detailed text output
    with open(detailed_file, 'w', encoding='utf-8') as f:
        f.write("=" * 80 + "\n")
        f.write(f"Concentrated-{subpattern.upper()}: HIGHLY CONCENTRATED LANGUAGE-SPECIFIC NAMED FOLDERS\n")
        f.write("=" * 80 + "\n")
        f.write(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
        f.write(f"Total Packages: {len(results)}\n")
        f.write("=" * 80 + "\n\n")
        
        for i, result in enumerate(results, 1):
            f.write(f"Package {i}/{len(results)}\n")
            f.write("-" * 80 + "\n")
            f.write(f"Repository: {result['repo_url']}\n")
            f.write(f"Owner/Repo: {result['owner_repo']}\n")
            f.write(f"Source Type: {result.get('source_type', 'unknown')}\n")
            f.write(f"Detected Ecosystems: {', '.join(result['detected_ecosystems'])}\n\n")
            
            pattern = result['classification']['pattern']
            f.write(f"Classification: {pattern}\n")
            f.write(f"Reason: {result['classification'].get('reason', 'N/A')}\n\n")
            
            f.write("Language-Specific Folders Found:\n")
            # Group by ecosystem, then sort by coverage within each ecosystem
            from collections import defaultdict
            folders_by_ecosystem = defaultdict(list)
            for df_info in result['classification']['language_specific_folders_found']:
                folders_by_ecosystem[df_info['ecosystem']].append(df_info)
            
            # Sort ecosystems alphabetically, then sort folders within each ecosystem by coverage
            for ecosystem in sorted(folders_by_ecosystem.keys()):
                sorted_folders = sorted(folders_by_ecosystem[ecosystem], 
                                      key=lambda x: x['coverage'], reverse=True)
                for df_info in sorted_folders:
                    f.write(f"  {df_info['ecosystem']} -> {df_info['folder']}/\n")
                    f.write(f"    files_in_folder: {df_info['files_in_folder']}\n")
                    f.write(f"    total_files: {df_info['total_files']}\n")
                    f.write(f"    coverage: {df_info['coverage']:.1%}\n")
            
            f.write("\nFile Distribution by Ecosystem:\n")
            for ecosystem in sorted(result['detected_ecosystems']):
                if ecosystem in result['analysis']:
                    total_files = result['analysis'][ecosystem]['total_files']
                    f.write(f"  {ecosystem}: (Total: {total_files} files)\n")
                    locations = result['analysis'][ecosystem]['file_locations']
                    sorted_locs = sorted(locations.items(), key=lambda x: x[1], reverse=True)
                    for loc, count in sorted_locs:
                        f.write(f"    {loc}/: {count} files\n")
            
            f.write("\n" + "=" * 80 + "\n\n")
    
    print(f"  Written: {output_file}")
    print(f"  Written: {detailed_file}")


def write_mixed_output(results: List[Dict], output_dir: Path):
    """Write Mixed (Partial) results."""
    output_file = output_dir / "mixed.csv"
    detailed_file = output_dir / "mixed_detailed.txt"
    
    # CSV output
    with open(output_file, 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow([
            'repo_url', 'owner_repo', 'source_type',
            'detected_ecosystems', 'claimed_ecosystems', 'ecosystem', 'language_specific_folder', 'coverage',
            'files_in_folder', 'total_files'
        ])
        
        for result in results:
            ecosystems_str = ', '.join(result['detected_ecosystems'])
            claimed_str = ', '.join(result.get('claimed_ecosystems', []))
            
            for df_info in result['classification']['language_specific_folders_found']:
                writer.writerow([
                    result['repo_url'],
                    result['owner_repo'],
                    result.get('source_type', 'unknown'),
                    ecosystems_str,
                    claimed_str,
                    df_info['ecosystem'],
                    df_info['folder'],
                    f"{df_info['coverage']:.2%}",
                    df_info['files_in_folder'],
                    df_info['total_files']
                ])
    
    # Detailed text output
    with open(detailed_file, 'w', encoding='utf-8') as f:
        f.write("=" * 80 + "\n")
        f.write("Mixed: PARTIAL LANGUAGE-SPECIFIC NAMED FOLDERS\n")
        f.write("=" * 80 + "\n")
        f.write(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
        f.write(f"Total Packages: {len(results)}\n")
        f.write("=" * 80 + "\n\n")
        
        for i, result in enumerate(results, 1):
            f.write(f"Package {i}/{len(results)}\n")
            f.write("-" * 80 + "\n")
            f.write(f"Repository: {result['repo_url']}\n")
            f.write(f"Owner/Repo: {result['owner_repo']}\n")
            f.write(f"Source Type: {result.get('source_type', 'unknown')}\n")
            f.write(f"Detected Ecosystems: {', '.join(result['detected_ecosystems'])}\n\n")
            
            f.write("Classification: Mixed\n")
            f.write(f"Reason: {result['classification'].get('reason', 'N/A')}\n\n")
            
            f.write("Language-Specific Folders Found:\n")
            # Group by ecosystem, then sort by coverage within each ecosystem
            from collections import defaultdict
            folders_by_ecosystem = defaultdict(list)
            for df_info in result['classification']['language_specific_folders_found']:
                folders_by_ecosystem[df_info['ecosystem']].append(df_info)
            
            # Sort ecosystems alphabetically, then sort folders within each ecosystem by coverage
            for ecosystem in sorted(folders_by_ecosystem.keys()):
                sorted_folders = sorted(folders_by_ecosystem[ecosystem], 
                                      key=lambda x: x['coverage'], reverse=True)
                for df_info in sorted_folders:
                    f.write(f"  {df_info['ecosystem']} -> {df_info['folder']}/\n")
                    f.write(f"    files_in_folder: {df_info['files_in_folder']}\n")
                    f.write(f"    total_files: {df_info['total_files']}\n")
                    f.write(f"    coverage: {df_info['coverage']:.1%}\n")
            
            f.write("\nFile Distribution by Ecosystem:\n")
            for ecosystem in sorted(result['detected_ecosystems']):
                if ecosystem in result['analysis']:
                    total_files = result['analysis'][ecosystem]['total_files']
                    f.write(f"  {ecosystem}: (Total: {total_files} files)\n")
                    locations = result['analysis'][ecosystem]['file_locations']
                    sorted_locs = sorted(locations.items(), key=lambda x: x[1], reverse=True)
                    for loc, count in sorted_locs:
                        pct = (count / total_files * 100) if total_files > 0 else 0
                        f.write(f"    {loc}/: {count} files ({pct:.1f}%)\n")
            
            f.write("\n" + "=" * 80 + "\n\n")
    
    print(f"  Written: {output_file}")
    print(f"  Written: {detailed_file}")


def write_low_coverage_output(results: List[Dict], output_dir: Path):
    """Write Low (Low Coverage) results."""
    output_file = output_dir / "low_coverage.csv"
    detailed_file = output_dir / "low_coverage_detailed.txt"
    
    # CSV output
    with open(output_file, 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow([
            'repo_url', 'owner_repo', 'source_type',
            'detected_ecosystems', 'claimed_ecosystems', 'ecosystem', 'language_specific_folder', 'coverage',
            'files_in_folder', 'total_files'
        ])
        
        for result in results:
            ecosystems_str = ', '.join(result['detected_ecosystems'])
            claimed_str = ', '.join(result.get('claimed_ecosystems', []))
            
            for df_info in result['classification']['language_specific_folders_found']:
                writer.writerow([
                    result['repo_url'],
                    result['owner_repo'],
                    result.get('source_type', 'unknown'),
                    ecosystems_str,
                    claimed_str,
                    df_info['ecosystem'],
                    df_info['folder'],
                    f"{df_info['coverage']:.2%}",
                    df_info['files_in_folder'],
                    df_info['total_files']
                ])
    
    # Detailed text output
    with open(detailed_file, 'w', encoding='utf-8') as f:
        f.write("=" * 80 + "\n")
        f.write("Low: LOW COVERAGE LANGUAGE-SPECIFIC NAMED FOLDERS\n")
        f.write("=" * 80 + "\n")
        f.write(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
        f.write(f"Total Packages: {len(results)}\n")
        f.write("=" * 80 + "\n\n")
        
        for i, result in enumerate(results, 1):
            f.write(f"Package {i}/{len(results)}\n")
            f.write("-" * 80 + "\n")
            f.write(f"Repository: {result['repo_url']}\n")
            f.write(f"Owner/Repo: {result['owner_repo']}\n")
            f.write(f"Source Type: {result.get('source_type', 'unknown')}\n")
            f.write(f"Detected Ecosystems: {', '.join(result['detected_ecosystems'])}\n\n")
            
            f.write("Classification: Low\n")
            f.write(f"Reason: {result['classification'].get('reason', 'N/A')}\n\n")
            
            f.write("Language-Specific Folders Found (all with low coverage):\n")
            # Group by ecosystem, then sort by coverage within each ecosystem
            from collections import defaultdict
            folders_by_ecosystem = defaultdict(list)
            for df_info in result['classification']['language_specific_folders_found']:
                folders_by_ecosystem[df_info['ecosystem']].append(df_info)
            
            # Sort ecosystems alphabetically, then sort folders within each ecosystem by coverage
            for ecosystem in sorted(folders_by_ecosystem.keys()):
                sorted_folders = sorted(folders_by_ecosystem[ecosystem], 
                                      key=lambda x: x['coverage'], reverse=True)
                for df_info in sorted_folders:
                    f.write(f"  {df_info['ecosystem']} -> {df_info['folder']}/\n")
                    f.write(f"    files_in_folder: {df_info['files_in_folder']}\n")
                    f.write(f"    total_files: {df_info['total_files']}\n")
                    f.write(f"    coverage: {df_info['coverage']:.1%}\n")
            
            f.write("\nFile Distribution by Ecosystem:\n")
            for ecosystem in sorted(result['detected_ecosystems']):
                if ecosystem in result['analysis']:
                    total_files = result['analysis'][ecosystem]['total_files']
                    f.write(f"  {ecosystem}: (Total: {total_files} files)\n")
                    locations = result['analysis'][ecosystem]['file_locations']
                    sorted_locs = sorted(locations.items(), key=lambda x: x[1], reverse=True)
                    for loc, count in sorted_locs:
                        f.write(f"    {loc}/: {count} files\n")
            
            f.write("\n" + "=" * 80 + "\n\n")
    
    print(f"  Written: {output_file}")
    print(f"  Written: {detailed_file}")


def write_no_folder_output(results: List[Dict], output_dir: Path):
    """Write No-Language-Specific-Folder results - CSV summary only.
    
    Directory structures are NOT written since they can be found in the original
    Ecosystem-Detector results (fully_matched.json).
    """
    # CSV summary only
    summary_file = output_dir / "no_language_specific_folder.csv"
    
    # Write CSV summary
    with open(summary_file, 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow([
            'repo_url', 'owner_repo', 'source_type', 'detected_ecosystems', 'claimed_ecosystems', 'reason'
        ])
        
        for result in results:
            writer.writerow([
                result['repo_url'],
                result['owner_repo'],
                result.get('source_type', 'unknown'),
                ', '.join(result['detected_ecosystems']),
                ', '.join(result.get('claimed_ecosystems', [])),
                result['classification'].get('reason', 'No language-specific named folder found')
            ])
    
    print(f"  Written: {summary_file} ({len(results)} packages)")


def write_summary(all_results: Dict[str, List[Dict]], output_dir: Path,
                 detection_mode: str = DETECTION_MODE_DEFAULT):
    """Write overall summary statistics with separate and combined views."""
    summary_file = output_dir / "summary.txt"
    
    total_conc_complete = len(all_results['Concentrated-Complete'])
    total_conc_partial = len(all_results['Concentrated-Partial'])
    total_mixed = len(all_results['Mixed'])
    total_low = len(all_results['Low'])
    total_no_folder = len(all_results['No-Language-Specific-Folder'])
    total = total_conc_complete + total_conc_partial + total_mixed + total_low + total_no_folder
    
    # Collect detailed statistics
    ecosystem_stats = defaultdict(lambda: {'Concentrated-Complete': 0, 'Concentrated-Partial': 0, 'Mixed': 0, 'Low': 0, 'No-Language-Specific-Folder': 0})
    folder_types = Counter()  # e.g., "python/", "bindings/python/"
    
    # Collect ecosystem count statistics (how many ecosystems per package)
    ecosystem_count_stats = defaultdict(lambda: defaultdict(int))  # pattern -> eco_count -> count
    ecosystem_count_totals = defaultdict(int)  # eco_count -> total packages with that count
    
    for pattern, results in all_results.items():
        for result in results:
            eco_count = len(result.get('claimed_ecosystems', []))
            ecosystem_count_stats[pattern][eco_count] += 1
            ecosystem_count_totals[eco_count] += 1
            
            for eco in result.get('detected_ecosystems', []):
                ecosystem_stats[eco][pattern] += 1
            
            for df_info in result['classification'].get('language_specific_folders_found', []):
                folder_types[df_info['folder']] += 1
    
    with open(summary_file, 'w', encoding='utf-8') as f:
        f.write("=" * 80 + "\n")
        f.write("LANGUAGE-SPECIFIC NAMED FOLDER DETECTION SUMMARY (HYBRID APPROACH)\n")
        f.write("=" * 80 + "\n")
        f.write(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
        f.write(f"Detection Mode: {detection_mode}\n\n")
        
        # Calculate totals
        total_with_folder = total_conc_complete + total_conc_partial + total_mixed + total_low
        total_concentrated = total_conc_complete + total_conc_partial
        
        f.write("OVERALL STATISTICS\n")
        f.write("-" * 80 + "\n")
        f.write(f"Total Packages Analyzed:         {total}\n")
        f.write(f"With Language-Specific Folders:  {total_with_folder:5d} ({total_with_folder/total*100:.2f}%)\n")
        f.write(f"  Concentrated (All):            {total_concentrated:5d} ({total_concentrated/total*100:.2f}%)\n")
        f.write(f"    Concentrated-Complete:       {total_conc_complete:5d} ({total_conc_complete/total*100:.2f}%)\n")
        f.write(f"    Concentrated-Partial:        {total_conc_partial:5d} ({total_conc_partial/total*100:.2f}%)\n")
        f.write(f"  Mixed:                         {total_mixed:5d} ({total_mixed/total*100:.2f}%)\n")
        f.write(f"  Low:                           {total_low:5d} ({total_low/total*100:.2f}%)\n")
        f.write(f"No Language-Specific Folder:     {total_no_folder:5d} ({total_no_folder/total*100:.2f}%)\n\n")
        
        f.write("CLASSIFICATION BY ECOSYSTEM\n")
        f.write("-" * 80 + "\n")
        f.write(f"{'Ecosystem':<12} {'Conc-C':<12} {'Conc-P':<12} {'Mixed':<10} {'Low':<10} {'No-Folder':<10}\n")
        f.write("-" * 80 + "\n")
        for eco in sorted(ecosystem_stats.keys()):
            stats = ecosystem_stats[eco]
            eco_total = stats['Concentrated-Complete'] + stats['Concentrated-Partial'] + stats['Mixed'] + stats['Low'] + stats['No-Language-Specific-Folder']
            f.write(f"{eco:<12} {stats['Concentrated-Complete']:3d} ({stats['Concentrated-Complete']/eco_total*100:4.1f}%)")
            f.write(f"  {stats['Concentrated-Partial']:3d} ({stats['Concentrated-Partial']/eco_total*100:4.1f}%)")
            f.write(f"  {stats['Mixed']:3d} ({stats['Mixed']/eco_total*100:4.1f}%)")
            f.write(f"  {stats['Low']:3d} ({stats['Low']/eco_total*100:4.1f}%)")
            f.write(f"  {stats['No-Language-Specific-Folder']:3d} ({stats['No-Language-Specific-Folder']/eco_total*100:4.1f}%)\n")
        f.write("\n")
        
        # Add ecosystem count statistics
        f.write("CLASSIFICATION BY ECOSYSTEM COUNT\n")
        f.write("-" * 80 + "\n")
        
        # Find all ecosystem counts present
        all_eco_counts = sorted(ecosystem_count_totals.keys())
        
        # Column width for each ecosystem count column
        COL_WIDTH = 20
        
        # Header row
        header = f"{'Pattern':<12}"
        for eco_count in all_eco_counts:
            header += f"{f'{eco_count} Eco':^{COL_WIDTH}}"
        header += f"{'Total':>7}"
        f.write(header + "\n")
        f.write("-" * 80 + "\n")
        
        # Data rows for each pattern
        pattern_order = ['Concentrated-Complete', 'Concentrated-Partial', 'Mixed', 'Low', 'No-Language-Specific-Folder']
        pattern_labels = {
            'Concentrated-Complete': 'Conc-C',
            'Concentrated-Partial': 'Conc-P',
            'Mixed': 'Mixed',
            'Low': 'Low',
            'No-Language-Specific-Folder': 'No-Folder'
        }
        
        for pattern in pattern_order:
            # Calculate total for this pattern
            pattern_total = len(all_results[pattern])
            
            row = f"{pattern_labels[pattern]:<12}"
            for eco_count in all_eco_counts:
                pattern_count = ecosystem_count_stats[pattern][eco_count]
                if pattern_total > 0:
                    pct = pattern_count / pattern_total * 100
                    cell = f"{pattern_count}/{pattern_total} ({pct:4.1f}%)"
                else:
                    cell = f"{pattern_count}/{pattern_total} ( 0.0%)"
                row += f"{cell:^{COL_WIDTH}}"
            row += f"{pattern_total:>7}"
            f.write(row + "\n")
        f.write("\n")
        
        f.write("TOP LANGUAGE-SPECIFIC FOLDER NAMES\n")
        f.write("-" * 80 + "\n")
        for folder, count in folder_types.most_common(20):
            f.write(f"{folder + '/':<30} {count:5d}\n")
        f.write("\n")
        
        f.write("CLASSIFICATION CRITERIA (HYBRID APPROACH)\n")
        f.write("-" * 80 + "\n")
        f.write("Concentrated-Complete: All ecosystem folders found + all >= 80%\n")
        f.write("Concentrated-Partial:  Some ecosystem folders found + all found >= 80%\n")
        f.write("Mixed:                 Some folders >= 40%, not all >= 80%\n")
        f.write("Low:                   All found folders < 40%\n")
        f.write("No-Language-Specific-Folder: No language-specific named folders found\n\n")
        f.write("DETECTION MODE\n")
        f.write("-" * 80 + "\n")
        if detection_mode == DETECTION_MODE_COUNT:
            f.write("Mode: count>=2 (Folder count must be >= 2)\n")
        elif detection_mode == DETECTION_MODE_DEPTH:
            f.write("Mode: count>=2_same_depth (Folder count >= 2 AND same depth)\n")
        else:
            f.write("Mode: default (No additional filtering)\n")
        f.write("\n")
        f.write("THRESHOLDS USED\n")
        f.write("-" * 80 + "\n")
        f.write(f"High concentration:  >= {THRESHOLD_CLEAN:.0%}\n")
        f.write(f"Partial quality:     >= {THRESHOLD_PARTIAL:.0%}\n")
        f.write(f"Minimum files:       >= {MIN_FILES_THRESHOLD}\n")
    
    print(f"  Written: {summary_file}")





def main():
    """Main entry point."""
    print("\n" + "=" * 80)
    print("LANGUAGE-SPECIFIC NAMED FOLDER DETECTOR")
    print("=" * 80)
    print(f"Thresholds: Clean >= {THRESHOLD_CLEAN:.0%}, Partial >= {THRESHOLD_PARTIAL:.0%}")
    print("=" * 80)
    
    detection_mode = DETECTION_MODE_COUNT
    
    print("\n" + "=" * 80)
    print(f"Detection Mode: {detection_mode}")
    print("=" * 80)
    
    # Compile patterns
    print("\nCompiling patterns...")
    compiled_patterns = compile_patterns()
    
    print(f"\nInput Source: {FULLY_MATCHED_JSON}")
    print(f"Will output results to: {OUTPUT_BASE_DIR}")
    
    # Check if source exists
    if not FULLY_MATCHED_JSON.exists():
        print(f"\nError: Ecosystem-Detector JSON file not found!")
        print(f"  Expected: {FULLY_MATCHED_JSON}")
        return
    
    # Initialize combined results
    combined_results = {
        'Concentrated-Complete': [],
        'Concentrated-Partial': [],
        'Mixed': [],
        'Low': [],
        'No-Language-Specific-Folder': []
    }
    
    # Create single output directory
    OUTPUT_BASE_DIR.mkdir(parents=True, exist_ok=True)
    print(f"Output: {OUTPUT_BASE_DIR}")
    
    # Process fully matched JSON
    print(f"\n" + "=" * 80)
    print("Processing FULLY MATCHED packages")
    print("=" * 80)
    print(f"Input: {FULLY_MATCHED_JSON}")
    
    fully_matched_results = process_json_file(FULLY_MATCHED_JSON, compiled_patterns, 'fully_matched', detection_mode)
    
    for pattern in combined_results.keys():
        combined_results[pattern].extend(fully_matched_results[pattern])
    
    print(f"\nResults: Conc-Complete={len(fully_matched_results['Concentrated-Complete'])}, Conc-Partial={len(fully_matched_results['Concentrated-Partial'])}, Mixed={len(fully_matched_results['Mixed'])}, Low={len(fully_matched_results['Low'])}, No-Folder={len(fully_matched_results['No-Language-Specific-Folder'])}")
    
    # Write merged output files
    print("\n" + "=" * 80)
    print("Writing merged output files...")
    print("=" * 80)
    print("\nConcentrated-Complete:")
    write_concentrated_output(combined_results['Concentrated-Complete'], OUTPUT_BASE_DIR, 'Complete')
    print("\nConcentrated-Partial:")
    write_concentrated_output(combined_results['Concentrated-Partial'], OUTPUT_BASE_DIR, 'Partial')
    print("\nMixed:")
    write_mixed_output(combined_results['Mixed'], OUTPUT_BASE_DIR)
    print("\nLow:")
    write_low_coverage_output(combined_results['Low'], OUTPUT_BASE_DIR)
    print("\nNo-Language-Specific-Folder:")
    write_no_folder_output(combined_results['No-Language-Specific-Folder'], OUTPUT_BASE_DIR)
    
    # Write summary
    print("\n" + "=" * 80)
    print("Writing summary...")
    print("=" * 80)
    write_summary(combined_results, OUTPUT_BASE_DIR, detection_mode)
    
    # Final statistics
    print("\n" + "=" * 80)
    print("COMPLETED - MERGED OUTPUT GENERATED")
    print("=" * 80)
    print(f"\nOutput Directory: {OUTPUT_BASE_DIR}")
    print(f"Summary: {OUTPUT_BASE_DIR / 'summary.txt'}")
    print("=" * 80)


if __name__ == "__main__":
    main()
