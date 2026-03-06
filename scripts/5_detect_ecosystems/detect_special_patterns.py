#!/usr/bin/env python3
"""
Special Pattern Detector

Detects six special patterns across all packages (fully_matched, partially_matched,
fully_mismatched) and reports how many packages match each pattern.

Patterns detected:
  1. Template Generation — .proto, .thrift, .fbs files
  2. JSII Binding       — .projenrc.js/.ts files, or .projen/ directory
  3. WASM Binding       — 'wasm' in any path (threshold ≥ 3 occurrences)
  4. PyO3/Maturin       — 'pyo3' or 'maturin' in any path
  5. Maven WebJar/mvnpm — Maven package name starts with org.webjars* or org.mvnpm*
  6. PHP Composer       — composer.json present in directory structure

Outputs only a summary.txt with per-pattern counts and overlap statistics.
No promotion logic — purely detection and counting.
"""

import csv
import json
import re
import sys
from collections import Counter, defaultdict
from datetime import datetime
from functools import lru_cache
from pathlib import Path
from typing import Dict, List, Set, Tuple
from tqdm import tqdm

# ==============================================================================
# PATH CONFIGURATION
# ==============================================================================

DATA_DIR = Path(__file__).resolve().parent.parent.parent / "data"

# Input files
FULLY_MATCHED_FILE = DATA_DIR / "ecosystem-detection" / "fully_matched.json"
PARTIALLY_MATCHED_FILE = DATA_DIR / "ecosystem-detection" / "partially_matched.json"
FULLY_MISMATCHED_FILE = DATA_DIR / "ecosystem-detection" / "fully_mismatched.csv"
CROSS_ECOSYSTEM_FILE = DATA_DIR / "cross-ecosystem-filter" / "cross_ecosystem_packages.json"
DIRECTORY_STRUCTURES_FILE = DATA_DIR / "directory-structures" / "directory_structures.json"

# Output directory (subfolder of ecosystem-detection)
OUTPUT_DIR = DATA_DIR / "ecosystem-detection" / "special_patterns"

# ==============================================================================
# WEBJAR / MVNPM PREFIXES
# ==============================================================================

WEBJAR_MVNPM_PREFIXES = (
    "org.webjars.npm:",
    "org.webjars.bower:",
    "org.webjars.bowergithub.",
    "org.webjars:",
    "org.mvnpm:",
    "org.mvnpm.",
)

# ==============================================================================
# SOURCE EXTENSIONS (mirrored from detect_ecosystems.py)
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

EXCLUDED_FOLDERS = [
    r'^tests?$', r'^testing$', r'^spec$', r'^specs$',
    r'.*[-_]tests?$', r'.*[-_]test$',
    r'^__tests__$', r'^test[-_]?data$',
    r'^\.tests?$', r'^\.testing$', r'^\.spec$', r'^\.specs$',
    r'^examples?$', r'^samples?$', r'^demos?$',
    r'^\.examples?$', r'^\.samples?$', r'^\.demos?$',
    r'^docs?$', r'^documentation$', r'^api[-_]?docs?$',
    r'^\.docs?$', r'^\.documentation$',
    r'^benchmarks?$', r'^benches$', r'^perf$',
    r'^\.benchmarks?$', r'^\.benches$',
    r'^fixtures?$', r'^mocks?$', r'^stubs?$', r'^fakes?$',
    r'^__pycache__$', r'^\.pytest_cache$',
    r'^node_modules$', r'^vendor$', r'^target$',
    r'^dist$', r'^build$', r'^out$', r'^\.git$',
    r'^\.tox$', r'^\.venv$', r'^venv$', r'^env$',
    r'^third[-_]?party$', r'^external$', r'^deps$',
    r'^vendored$', r'^contrib$',
    r'^icon$', r'^icons$'
]

COMPILED_EXCLUDED_PATTERNS = [re.compile(p, re.IGNORECASE) for p in EXCLUDED_FOLDERS]

MIN_FILES_FOR_ECOSYSTEM = 1


# ==============================================================================
# HELPER FUNCTIONS (mirrored from detect_ecosystems.py for re-detection)
# ==============================================================================

@lru_cache(maxsize=None)
def is_excluded_folder(folder_name: str) -> bool:
    """Check if a folder should be excluded from ecosystem detection."""
    for pattern in COMPILED_EXCLUDED_PATTERNS:
        if pattern.match(folder_name):
            return True
    return False


@lru_cache(maxsize=None)
def is_path_excluded(folder_path: str) -> bool:
    """Check if any component in the path is excluded."""
    parts = folder_path.split('/')
    for part in parts:
        if is_excluded_folder(part):
            return True
    return False


def get_file_extension(filename: str) -> str:
    """Extract file extension from filename."""
    idx = filename.rfind('.')
    if idx > 0:
        return filename[idx:].lower()
    return ''


EXTENSION_TO_ECOSYSTEM = {}
for _eco, _exts in SOURCE_EXTENSIONS.items():
    for _ext in _exts:
        EXTENSION_TO_ECOSYSTEM[_ext] = _eco


def detect_ecosystems_from_dir_structure(dir_structure: List[str]) -> Tuple[Dict[str, int], Dict[str, List[str]]]:
    """
    Detect ecosystems from a flat list of file paths (directory_structure).
    Returns (ecosystem_counts, ecosystem_paths).
    """
    ecosystem_counts: Dict[str, int] = {}
    ecosystem_paths: Dict[str, List[str]] = {}

    for path in dir_structure:
        # Skip directories (end with /)
        if path.endswith('/'):
            continue

        # Get the folder portion for exclusion check
        last_slash = path.rfind('/')
        if last_slash > 0:
            folder_path = path[:last_slash]
            if is_path_excluded(folder_path):
                continue

        # Get file extension
        filename = path.rsplit('/', 1)[-1] if '/' in path else path
        ext = get_file_extension(filename)
        if not ext:
            continue

        eco = EXTENSION_TO_ECOSYSTEM.get(ext)
        if eco:
            ecosystem_counts[eco] = ecosystem_counts.get(eco, 0) + 1
            if eco not in ecosystem_paths:
                ecosystem_paths[eco] = []
            ecosystem_paths[eco].append(path)

    # Apply minimum threshold
    filtered_counts = {}
    filtered_paths = {}
    for eco, count in ecosystem_counts.items():
        if count >= MIN_FILES_FOR_ECOSYSTEM:
            filtered_counts[eco] = count
            filtered_paths[eco] = ecosystem_paths[eco]

    return filtered_counts, filtered_paths


# ==============================================================================
# SPECIAL PATTERN INDICATOR FUNCTIONS
# ==============================================================================

_TEMPLATE_EXTS = frozenset(('.proto', '.thrift', '.fbs'))
_JSII_FILES = frozenset(('.projenrc.js', '.projenrc.ts'))


def detect_all_path_patterns(dir_structure: List[str]) -> Dict[str, Tuple[bool, List[str]]]:
    """
    Single pass over dir_structure detecting all path-based patterns.
    Returns a dict mapping each pattern key to (matched: bool, evidence: List[str]).

    Patterns:
      - template_generation (.proto/.thrift/.fbs)
      - jsii_binding        (.projenrc.js/.ts or .projen entry)
      - wasm_binding        ('wasm' in path, threshold >= 3)
      - pyo3_maturin        ('pyo3' or 'maturin' in path)
      - composer_json       (composer.json present)
    """
    tmpl_ev: List[str] = []
    jsii_ev: List[str] = []
    wasm_ev: List[str] = []
    pyo3_ev: List[str] = []
    composer_ev: List[str] = []
    projen_seen = False

    for path in dir_structure:
        stripped = path[:-1] if path.endswith('/') else path
        slash_idx = stripped.rfind('/')
        filename = stripped[slash_idx + 1:] if slash_idx >= 0 else stripped
        folder_path = stripped[:slash_idx] if slash_idx > 0 else ''

        # JSII — no exclusion check
        if filename in _JSII_FILES:
            jsii_ev.append(path)
        elif filename == '.projen' and not projen_seen:
            jsii_ev.append(path if path.endswith('/') else path + '/')
            projen_seen = True

        # composer.json — no exclusion check
        if filename == 'composer.json':
            composer_ev.append(path)

        # Everything below respects path exclusion
        if folder_path and is_path_excluded(folder_path):
            continue

        path_lower = path.lower()

        # Template Generation (files only)
        if not path.endswith('/'):
            for ext in _TEMPLATE_EXTS:
                if filename.endswith(ext):
                    tmpl_ev.append(path)
                    break

        # WASM
        if 'wasm' in path_lower:
            wasm_ev.append(path)

        # PyO3 / Maturin
        if 'pyo3' in path_lower or 'maturin' in path_lower:
            pyo3_ev.append(path)

    return {
        'template_generation': (bool(tmpl_ev), tmpl_ev),
        'jsii_binding':        (bool(jsii_ev), jsii_ev),
        'wasm_binding':        (len(wasm_ev) >= 3, wasm_ev),
        'pyo3_maturin':        (bool(pyo3_ev), pyo3_ev),
        'composer_json':       (bool(composer_ev), composer_ev),
    }


# Keep the individual functions available for external callers.
def has_template_files(dir_structure: List[str]) -> Tuple[bool, List[str]]:
    """Detect .proto/.thrift/.fbs files in the directory structure."""
    found = [p for p in dir_structure
             if not p.endswith('/')
             and any(p.endswith(e) for e in _TEMPLATE_EXTS)
             and not (p.rfind('/') > 0 and is_path_excluded(p[:p.rfind('/')]))]
    return bool(found), found


def has_jsii_indicators(dir_structure: List[str]) -> Tuple[bool, List[str]]:
    """Detect .projenrc.js, .projenrc.ts, or .projen/ entries."""
    hit, ev = detect_all_path_patterns(dir_structure)['jsii_binding']
    return hit, ev


def has_wasm_indicators(dir_structure: List[str]) -> Tuple[bool, List[str]]:
    """Detect 'wasm' in any non-excluded path (threshold >= 3)."""
    hit, ev = detect_all_path_patterns(dir_structure)['wasm_binding']
    return hit, ev


def has_pyo3_maturin_indicators(dir_structure: List[str]) -> Tuple[bool, List[str]]:
    """Detect 'pyo3' or 'maturin' in any non-excluded path."""
    hit, ev = detect_all_path_patterns(dir_structure)['pyo3_maturin']
    return hit, ev


def is_webjar_or_mvnpm(maven_package_name: str) -> bool:
    """Check if a Maven package name indicates a WebJar or mvnpm package."""
    if not maven_package_name:
        return False
    name_lower = maven_package_name.lower()
    for prefix in WEBJAR_MVNPM_PREFIXES:
        if name_lower.startswith(prefix):
            return True
    return False


def has_composer_json(dir_structure: List[str]) -> bool:
    """Check if the directory structure contains a composer.json file."""
    for path in dir_structure:
        filename = path.rsplit('/', 1)[-1] if '/' in path else path
        if filename == 'composer.json':
            return True
    return False


def classify_match(registered: Set[str], result: Set[str]) -> str:
    """
    Classify the match type:
    - 'full': ALL registered ecosystems are in result
    - 'partial': 2+ (but not all) registered ecosystems in result
    - 'none': 0 or 1 registered ecosystems in result
    """
    if len(result) == len(registered):
        return 'full'
    elif len(result) >= 2:
        return 'partial'
    else:
        return 'none'


# ==============================================================================
# LOADING FUNCTIONS
# ==============================================================================

def load_cross_ecosystem_index(filepath: Path) -> Dict[str, str]:
    """
    Build an index: normalized_url -> Maven package name.
    Only includes packages that have a Maven entry.
    """
    print(f"\nLoading cross-ecosystem packages from: {filepath}")
    with open(filepath, 'r', encoding='utf-8') as f:
        data = json.load(f)

    index = {}
    packages = data.get('monorepo_packages', data.get('packages', []))
    for pkg in tqdm(packages, desc='  Indexing Maven entries', unit='pkg',
                    dynamic_ncols=True, file=sys.stdout):
        maven_info = pkg.get('packages', {}).get('Maven', {})
        maven_name = maven_info.get('name', '')
        if maven_name:
            norm_url = pkg.get('normalized_url', '')
            if norm_url:
                index[norm_url] = maven_name

    print(f"  • Indexed {len(index):,} packages with Maven entries")

    # Stats
    webjar_count = sum(1 for n in index.values() if n.lower().startswith('org.webjars'))
    mvnpm_count = sum(1 for n in index.values() if n.lower().startswith('org.mvnpm'))
    print(f"  • WebJar packages (org.webjars*): {webjar_count:,}")
    print(f"  • mvnpm packages (org.mvnpm*): {mvnpm_count:,}")
    print(f"  • Other Maven packages: {len(index) - webjar_count - mvnpm_count:,}")

    return index


def load_directory_structures_index(filepath: Path) -> Dict[str, List[str]]:
    """
    Build an index: normalized_url (repository) -> directory_structure list.
    Used to look up directory structures for fully_mismatched packages (CSV has no dir structure).
    """
    print(f"\nLoading directory structures from: {filepath}")
    with open(filepath, 'r', encoding='utf-8') as f:
        data = json.load(f)

    index = {}
    all_pkgs = data.get('packages', [])
    for pkg in tqdm(all_pkgs, desc='  Indexing dir structures', unit='pkg',
                    dynamic_ncols=True, file=sys.stdout):
        repo = pkg.get('repository', '')
        dir_struct = pkg.get('directory_structure', [])
        if repo and dir_struct:
            index[repo] = dir_struct

    print(f"  • Indexed {len(index):,} directory structures")
    return index


def load_fully_matched(filepath: Path) -> List[Dict]:
    """Load fully_matched.json packages."""
    print(f"\nLoading fully matched from: {filepath}")
    with open(filepath, 'r', encoding='utf-8') as f:
        data = json.load(f)
    packages = data.get('packages', [])
    print(f"  • Loaded {len(packages):,} fully matched packages")
    return packages


def load_partially_matched(filepath: Path) -> List[Dict]:
    """Load partially_matched.json packages."""
    print(f"\nLoading partially matched from: {filepath}")
    with open(filepath, 'r', encoding='utf-8') as f:
        data = json.load(f)
    packages = data.get('packages', [])
    print(f"  • Loaded {len(packages):,} partially matched packages")
    return packages


def load_fully_mismatched(filepath: Path) -> List[Dict]:
    """
    Load fully_mismatched.csv into list of dicts.
    """
    print(f"\nLoading fully mismatched from: {filepath}")
    packages = []
    with open(filepath, 'r', encoding='utf-8', newline='') as f:
        reader = csv.DictReader(f)
        for row in reader:
            # Parse comma-separated ecosystem lists
            registered = [e.strip() for e in row['Registered Ecosystems'].split(',') if e.strip()]
            detected_str = row.get('Detected Ecosystems', '')
            detected = [e.strip() for e in detected_str.split(',') if e.strip()]
            result_str = row.get('Result Ecosystems', '')
            result = [e.strip() for e in result_str.split(',') if e.strip() and e.strip() != 'None']

            # Parse detected ecosystem file counts (e.g., "NPM: 17, PyPI: 3")
            counts_str = row.get('Detected Ecosystem File Counts', '')
            detected_counts = {}
            if counts_str and counts_str != 'None':
                for part in counts_str.split(','):
                    part = part.strip()
                    if ':' in part:
                        eco, cnt = part.split(':', 1)
                        try:
                            detected_counts[eco.strip()] = int(cnt.strip())
                        except ValueError:
                            pass

            packages.append({
                'repository': row['Repository'],
                'claimed_ecosystems': registered,
                'detected_ecosystems': detected,
                'result_ecosystems': result,
                'detected_ecosystem_counts': detected_counts,
            })

    print(f"  • Loaded {len(packages):,} fully mismatched packages")
    return packages


# ==============================================================================
# OUTPUT / SUMMARY
# ==============================================================================

_PATTERN_META = {
    'template_generation': {
        'filename': 'template_generation.json',
        'description': 'Packages containing template/IDL files (.proto, .thrift, .fbs)',
        'evidence_label': 'template_files',
    },
    'jsii_binding': {
        'filename': 'jsii_binding.json',
        'description': 'Packages with jsii/projen indicators (.projenrc.js, .projenrc.ts, .projen/)',
        'evidence_label': 'jsii_indicators',
    },
    'wasm_binding': {
        'filename': 'wasm_binding.json',
        'description': 'Packages with WebAssembly indicators ("wasm" in paths, ≥3 occurrences)',
        'evidence_label': 'wasm_paths',
    },
    'pyo3_maturin': {
        'filename': 'pyo3_maturin.json',
        'description': 'Packages with PyO3/Maturin indicators ("pyo3" or "maturin" in paths)',
        'evidence_label': 'pyo3_maturin_paths',
    },
    'maven_webjar': {
        'filename': 'maven_webjar.json',
        'description': 'Maven packages distributed as WebJar or mvnpm (org.webjars* / org.mvnpm*)',
        'evidence_label': 'maven_package_name',
    },
    'php_composer': {
        'filename': 'php_composer.json',
        'description': 'PHP packages detected via composer.json (no PHP source files)',
        'evidence_label': 'composer_json_paths',
    },
}


def write_pattern_output(pattern_key: str, packages: List[Dict], output_dir: Path):
    """Write per-pattern JSON output file with evidence per package."""
    meta = _PATTERN_META[pattern_key]
    output_file = output_dir / meta['filename']

    output_data = {
        'metadata': {
            'generated': datetime.now().isoformat(),
            'pattern': pattern_key,
            'description': meta['description'],
            'total_packages': len(packages),
        },
        'packages': packages,
    }

    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(output_data, f, indent=2, ensure_ascii=False)

    print(f"  Written {len(packages):,} packages → {output_file.name}")

def write_summary(stats: Dict, output_dir: Path):
    """Write special pattern detection summary."""
    summary_file = output_dir / "summary.txt"

    total = stats['total_packages']
    if total == 0:
        print("  No packages processed, skipping summary.")
        return

    pattern_keys = [
        ('template_generation', 'Template Generation (.proto/.thrift/.fbs)'),
        ('jsii_binding',        'JSII Binding (.projenrc.js/.ts, .projen/)'),
        ('wasm_binding',        'WASM Binding ("wasm" in paths, ≥3 occurrences)'),
        ('pyo3_maturin',        'PyO3/Maturin ("pyo3" or "maturin" in paths)'),
        ('maven_webjar',        'Maven WebJar/mvnpm (org.webjars* or org.mvnpm*)'),
        ('php_composer',        'PHP Composer (composer.json present)'),
    ]

    with open(summary_file, 'w', encoding='utf-8') as f:
        f.write('=' * 80 + '\n')
        f.write('SPECIAL PATTERN DETECTION SUMMARY\n')
        f.write('=' * 80 + '\n')
        f.write(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
        f.write(f"Total packages examined: {total:,}\n\n")

        f.write('PER-PATTERN COUNTS\n')
        f.write('-' * 80 + '\n')
        for key, label in pattern_keys:
            count = stats['counts'].get(key, 0)
            pct = count / total * 100 if total > 0 else 0
            f.write(f"  {label}\n")
            f.write(f"    Packages matched: {count:6,} ({pct:.1f}%)\n")
        f.write('\n')

        f.write('PATTERN OVERLAP STATISTICS\n')
        f.write('-' * 80 + '\n')
        overlap_combinations = stats.get('overlap_combinations', {})
        if overlap_combinations:
            sorted_combos = sorted(overlap_combinations.items(), key=lambda x: -x[1])
            for combo, count in sorted_combos:
                pct = count / total * 100
                f.write(f"  {combo:<55} {count:6,} ({pct:.1f}%)\n")
        else:
            f.write('  No overlaps detected.\n')
        f.write('\n')

        f.write('PACKAGES MATCHING ANY PATTERN\n')
        f.write('-' * 80 + '\n')
        any_count = stats.get('any_pattern', 0)
        pct = any_count / total * 100 if total > 0 else 0
        f.write(f"  {any_count:,} packages ({pct:.1f}%) match at least one pattern\n")
        none_count = total - any_count
        pct_none = none_count / total * 100 if total > 0 else 0
        f.write(f"  {none_count:,} packages ({pct_none:.1f}%) match no patterns\n")

    print(f"  Written summary: {summary_file}")


# ==============================================================================
# MAIN
# ==============================================================================

def main():
    print('=' * 80)
    print('SPECIAL PATTERN DETECTOR')
    print('=' * 80)
    print(f'Inputs:')
    print(f'  Fully matched:            {FULLY_MATCHED_FILE}')
    print(f'  Partially matched:        {PARTIALLY_MATCHED_FILE}')
    print(f'  Fully mismatched:         {FULLY_MISMATCHED_FILE}')
    print(f'  Cross-ecosystem packages: {CROSS_ECOSYSTEM_FILE}')
    print(f'  Directory structures:     {DIRECTORY_STRUCTURES_FILE}')
    print(f'Output: {OUTPUT_DIR}')
    print('=' * 80)

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    # ---- Load data ----
    maven_index = load_cross_ecosystem_index(CROSS_ECOSYSTEM_FILE)
    dir_struct_index = load_directory_structures_index(DIRECTORY_STRUCTURES_FILE)
    fully_matched_pkgs = load_fully_matched(FULLY_MATCHED_FILE)
    partially_matched_pkgs = load_partially_matched(PARTIALLY_MATCHED_FILE)
    fully_mismatched_pkgs = load_fully_mismatched(FULLY_MISMATCHED_FILE)

    all_packages = fully_matched_pkgs + partially_matched_pkgs + fully_mismatched_pkgs
    total = len(all_packages)

    print(f'\nTotal packages to examine: {total:,}')

    # ---- Detect patterns for every package ----
    pattern_keys = ['template_generation', 'jsii_binding', 'wasm_binding',
                    'pyo3_maturin', 'maven_webjar', 'php_composer']
    counts = {k: 0 for k in pattern_keys}
    pattern_packages: Dict[str, List[Dict]] = {k: [] for k in pattern_keys}
    overlap_combinations: Counter = Counter()
    any_pattern_count = 0

    print('\nDetecting patterns...')
    pbar = tqdm(all_packages, desc='Detecting', unit='pkg',
                dynamic_ncols=True, file=sys.stdout, mininterval=0.3)
    for pkg in pbar:
        repo = pkg['repository']
        claimed = pkg.get('claimed_ecosystems', [])
        detected = pkg.get('detected_ecosystems', [])
        dir_structure = (
            pkg.get('directory_structure')
            or dir_struct_index.get(repo, [])
        )

        matched = []

        # Single pass for all 4 path-based patterns
        pat = detect_all_path_patterns(dir_structure)

        def _add(key: str, evidence):
            matched.append(key)
            pattern_packages[key].append({
                'repository': repo,
                'claimed_ecosystems': claimed,
                'detected_ecosystems': detected,
                _PATTERN_META[key]['evidence_label']: evidence,
            })

        hit, ev = pat['template_generation']
        if hit:
            _add('template_generation', ev)

        hit, ev = pat['jsii_binding']
        if hit:
            _add('jsii_binding', ev)

        hit, ev = pat['wasm_binding']
        if hit:
            _add('wasm_binding', ev)

        hit, ev = pat['pyo3_maturin']
        if hit:
            _add('pyo3_maturin', ev)

        # 5. Maven WebJar/mvnpm
        registered_set = set(claimed)
        detected_set = set(detected)
        if 'Maven' in registered_set and 'Maven' not in detected_set:
            maven_name = maven_index.get(repo, '')
            if is_webjar_or_mvnpm(maven_name):
                name_lower = maven_name.lower()
                webjar_type = 'mvnpm' if name_lower.startswith('org.mvnpm') else 'webjar'
                matched.append('maven_webjar')
                pattern_packages['maven_webjar'].append({
                    'repository': repo,
                    'claimed_ecosystems': claimed,
                    'detected_ecosystems': detected,
                    'maven_package_name': maven_name,
                    'webjar_type': webjar_type,
                })

        # 6. PHP Composer
        if 'PHP' in registered_set and 'PHP' not in detected_set:
            hit, ev = pat['composer_json']
            if hit:
                matched.append('php_composer')
                pattern_packages['php_composer'].append({
                    'repository': repo,
                    'claimed_ecosystems': claimed,
                    'detected_ecosystems': detected,
                    'composer_json_paths': ev,
                })

        # Accumulate stats
        for key in matched:
            counts[key] += 1
        if matched:
            any_pattern_count += 1
            combo = '+'.join(sorted(matched))
            overlap_combinations[combo] += 1

        # Update progress bar postfix with running counts
        pbar.set_postfix({
            'tmpl': counts['template_generation'],
            'jsii': counts['jsii_binding'],
            'wasm': counts['wasm_binding'],
            'pyo3': counts['pyo3_maturin'],
            'webjar': counts['maven_webjar'],
            'composer': counts['php_composer'],
        }, refresh=False)

    pbar.close()

    stats = {
        'total_packages': total,
        'counts': counts,
        'any_pattern': any_pattern_count,
        'overlap_combinations': dict(overlap_combinations),
    }

    print('\n' + '=' * 80)
    print('WRITING OUTPUTS')
    print('=' * 80)
    print('\nPattern JSON files:')
    for key in pattern_keys:
        write_pattern_output(key, pattern_packages[key], OUTPUT_DIR)
    print('\nSummary:')
    write_summary(stats, OUTPUT_DIR)

    print('\n' + '=' * 80)
    print('COMPLETED')
    print('=' * 80)
    print(f'Total examined: {total:,}')
    for key, label in [
        ('template_generation', 'Template Generation'),
        ('jsii_binding',        'JSII Binding'),
        ('wasm_binding',        'WASM Binding'),
        ('pyo3_maturin',        'PyO3/Maturin'),
        ('maven_webjar',        'Maven WebJar/mvnpm'),
        ('php_composer',        'PHP Composer'),
    ]:
        pct = counts[key] / total * 100 if total > 0 else 0
        print(f'  {label:<25}: {counts[key]:,} ({pct:.1f}%)')
    print('=' * 80)


if __name__ == "__main__":
    main()
