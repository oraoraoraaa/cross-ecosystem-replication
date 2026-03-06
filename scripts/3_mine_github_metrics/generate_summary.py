#!/usr/bin/env python3
"""
Regenerate GitHub Metrics Mining Summary

This script parses the actual github_metrics.json and cross_ecosystem_packages.json
files to calculate accurate statistics and regenerate the summary.txt file.

Usage:
    python regenerate_summary.py
"""

import json
from pathlib import Path
from datetime import datetime
from collections import defaultdict

# ============================================================================
# PATH CONFIGURATION
# ============================================================================
DATA_DIR = Path(__file__).resolve().parent.parent.parent / "data"
METRICS_PATH = DATA_DIR / "github-metrics" / "github_metrics.json"
CROSS_ECOSYSTEM_PATH = DATA_DIR / "cross-ecosystem-filter" / "cross_ecosystem_packages.json"
OUTPUT_PATH = DATA_DIR / "github-metrics" / "summary.txt"


def load_github_metrics():
    """Load and parse github_metrics.json."""
    print("Loading github_metrics.json...")
    with open(METRICS_PATH, 'r') as f:
        data = json.load(f)
    
    packages = data.get('packages', {})
    print(f"  Loaded {len(packages)} total entries")
    return packages


def load_cross_ecosystem_data():
    """Load cross_ecosystem_packages.json to get input counts."""
    print("Loading cross_ecosystem_packages.json...")
    with open(CROSS_ECOSYSTEM_PATH, 'r') as f:
        data = json.load(f)
    
    metadata = data.get('metadata', {})
    cross_ecosystem_summary = metadata.get('cross_ecosystem_summary', {})
    
    monorepo_count = cross_ecosystem_summary.get('monorepo_packages', {}).get('count', 0)
    multirepo_count = cross_ecosystem_summary.get('multirepo_packages', {}).get('count', 0)
    total_count = cross_ecosystem_summary.get('total_cross_ecosystem_packages', 0)
    
    print(f"  Input statistics from cross_ecosystem_packages.json:")
    print(f"    Total cross-ecosystem packages: {total_count}")
    print(f"    Monorepo packages: {monorepo_count}")
    print(f"    Multirepo packages: {multirepo_count}")
    
    return {
        'total': total_count,
        'monorepo': monorepo_count,
        'multirepo': multirepo_count
    }


def analyze_metrics(packages):
    """Analyze the github_metrics.json data by source type."""
    print("\nAnalyzing metrics data...")
    
    stats = {
        'monorepo': {
            'total': 0,
            'has_error': 0,
            'is_forked': 0,
            'is_archived': 0,
            'forked_and_archived': 0,
            'valid': 0,
            'from_cache': 0,
            'from_api': 0,
            'errors_by_type': defaultdict(int)
        },
        'multirepo': {
            'total': 0,
            'has_error': 0,
            'is_forked': 0,
            'is_archived': 0,
            'forked_and_archived': 0,
            'valid': 0,
            'from_cache': 0,
            'from_api': 0,
            'errors_by_type': defaultdict(int)
        }
    }
    
    for key, metrics in packages.items():
        source = metrics.get('source', '').lower()
        
        if source not in ['monorepo', 'multirepo']:
            continue
        
        s = stats[source]
        s['total'] += 1
        
        has_error = 'error' in metrics
        is_forked = metrics.get('is_fork', False)
        is_archived = metrics.get('is_archived', False)
        from_cache = metrics.get('from_cache', False)
        
        if has_error:
            s['has_error'] += 1
            error_type = metrics.get('error', 'UNKNOWN')
            s['errors_by_type'][error_type] += 1
        
        if is_forked and is_archived:
            s['forked_and_archived'] += 1
        elif is_forked:
            s['is_forked'] += 1
        elif is_archived:
            s['is_archived'] += 1
        
        # Valid = no error, not forked, not archived
        if not has_error and not is_forked and not is_archived:
            s['valid'] += 1
        
        # Data source tracking (only for non-error entries)
        if not has_error:
            if from_cache:
                s['from_cache'] += 1
            else:
                s['from_api'] += 1
    
    return stats


def calculate_summary_stats(stats, input_counts):
    """Calculate the summary statistics matching the original format."""
    
    summary = {}
    
    for source in ['monorepo', 'multirepo']:
        s = stats[source]
        input_count = input_counts[source]
        
        # Total forked includes those that are also archived
        total_forked = s['is_forked'] + s['forked_and_archived']
        # Total archived includes those that are also forked
        total_archived = s['is_archived'] + s['forked_and_archived']
        
        # Successfully mined = total in JSON - errors
        successfully_mined = s['total'] - s['has_error']
        
        # Excluded = forked OR archived OR errors (with proper set union)
        # excluded = forked + archived + errors - (forked AND archived)
        # But we need to be careful: errors might overlap with forked/archived
        # In practice, error entries don't have forked/archived info reliably
        # So: excluded = errors + (total_forked) + (total_archived) - forked_and_archived
        # But this might double count. Let's compute properly:
        
        # Count entries that are excluded (error OR forked OR archived)
        excluded_count = s['has_error'] + s['is_forked'] + s['is_archived'] + s['forked_and_archived']
        
        summary[source] = {
            'input_total': input_count,
            'json_total': s['total'],
            'successfully_mined': successfully_mined,
            'errors': s['has_error'],
            'forked': total_forked,
            'archived': total_archived,
            'forked_and_archived': s['forked_and_archived'],
            'excluded': excluded_count,
            'valid': s['valid'],
            'from_cache': s['from_cache'],
            'from_api': s['from_api'],
            'errors_by_type': dict(s['errors_by_type'])
        }
    
    return summary


def generate_summary_text(summary, input_counts):
    """Generate the summary.txt content."""
    
    mono = summary['monorepo']
    multi = summary['multirepo']
    
    # Combined stats
    combined = {
        'input_total': input_counts['total'],
        'json_total': mono['json_total'] + multi['json_total'],
        'successfully_mined': mono['successfully_mined'] + multi['successfully_mined'],
        'errors': mono['errors'] + multi['errors'],
        'forked': mono['forked'] + multi['forked'],
        'archived': mono['archived'] + multi['archived'],
        'forked_and_archived': mono['forked_and_archived'] + multi['forked_and_archived'],
        'excluded': mono['excluded'] + multi['excluded'],
        'valid': mono['valid'] + multi['valid'],
        'from_cache': mono['from_cache'] + multi['from_cache'],
        'from_api': mono['from_api'] + multi['from_api'],
    }
    
    # Merge error types
    combined_errors = defaultdict(int)
    for error_type, count in mono['errors_by_type'].items():
        combined_errors[error_type] += count
    for error_type, count in multi['errors_by_type'].items():
        combined_errors[error_type] += count
    
    def pct(part, total):
        if total == 0:
            return 0.0
        return (part / total) * 100
    
    lines = []
    lines.append("=" * 80)
    lines.append("GITHUB METRICS MINING SUMMARY")
    lines.append("=" * 80)
    lines.append("")
    
    # Monorepo section
    lines.append("=" * 80)
    lines.append("MONOREPO PACKAGES (cross-ecosystem packages in single repository)")
    lines.append("-" * 80)
    lines.append(f"Total Input Repositories: {mono['input_total']:,}")
    lines.append(f"Entries in JSON: {mono['json_total']:,}")
    lines.append(f"Successfully Mined: {mono['successfully_mined']:,} ({pct(mono['successfully_mined'], mono['json_total']):.2f}%)")
    lines.append(f"Errors: {mono['errors']:,} ({pct(mono['errors'], mono['json_total']):.2f}%)")
    lines.append("")
    lines.append("Repository Status:")
    lines.append(f"  Forked: {mono['forked']:,} ({pct(mono['forked'], mono['json_total']):.2f}%)")
    lines.append(f"  Archived: {mono['archived']:,} ({pct(mono['archived'], mono['json_total']):.2f}%)")
    lines.append(f"  Forked AND Archived: {mono['forked_and_archived']:,} ({pct(mono['forked_and_archived'], mono['json_total']):.2f}%)")
    lines.append(f"  Excluded (Forked | Archived | Errors): {mono['excluded']:,} ({pct(mono['excluded'], mono['json_total']):.2f}%)")
    lines.append(f"  Valid (for analysis): {mono['valid']:,} ({pct(mono['valid'], mono['json_total']):.2f}%)")
    lines.append("")
    if mono['errors_by_type']:
        lines.append("Error Breakdown:")
        for error_type, count in sorted(mono['errors_by_type'].items()):
            lines.append(f"  {error_type}: {count:,} ({pct(count, mono['json_total']):.2f}%)")
    lines.append("")
    
    # Multirepo section
    lines.append("=" * 80)
    lines.append("MULTIREPO PACKAGES (language-specific repositories like project-js, project-py)")
    lines.append("-" * 80)
    lines.append(f"Total Input Repositories: {multi['input_total']:,}")
    lines.append(f"Entries in JSON: {multi['json_total']:,}")
    lines.append(f"Successfully Mined: {multi['successfully_mined']:,} ({pct(multi['successfully_mined'], multi['json_total']):.2f}%)")
    lines.append(f"Errors: {multi['errors']:,} ({pct(multi['errors'], multi['json_total']):.2f}%)")
    lines.append("")
    lines.append("Repository Status:")
    lines.append(f"  Forked: {multi['forked']:,} ({pct(multi['forked'], multi['json_total']):.2f}%)")
    lines.append(f"  Archived: {multi['archived']:,} ({pct(multi['archived'], multi['json_total']):.2f}%)")
    lines.append(f"  Forked AND Archived: {multi['forked_and_archived']:,} ({pct(multi['forked_and_archived'], multi['json_total']):.2f}%)")
    lines.append(f"  Excluded (Forked | Archived | Errors): {multi['excluded']:,} ({pct(multi['excluded'], multi['json_total']):.2f}%)")
    lines.append(f"  Valid (for analysis): {multi['valid']:,} ({pct(multi['valid'], multi['json_total']):.2f}%)")
    lines.append("")
    if multi['errors_by_type']:
        lines.append("Error Breakdown:")
        for error_type, count in sorted(multi['errors_by_type'].items()):
            lines.append(f"  {error_type}: {count:,} ({pct(count, multi['json_total']):.2f}%)")
    lines.append("")
    
    # Combined section
    lines.append("=" * 80)
    lines.append("COMBINED (ALL PACKAGES)")
    lines.append("-" * 80)
    lines.append(f"Total Input Repositories: {combined['input_total']:,}")
    lines.append(f"Entries in JSON: {combined['json_total']:,}")
    lines.append(f"Successfully Mined: {combined['successfully_mined']:,} ({pct(combined['successfully_mined'], combined['json_total']):.2f}%)")
    lines.append(f"Errors: {combined['errors']:,} ({pct(combined['errors'], combined['json_total']):.2f}%)")
    lines.append("")
    lines.append("Repository Status:")
    lines.append(f"  Forked: {combined['forked']:,} ({pct(combined['forked'], combined['json_total']):.2f}%)")
    lines.append(f"  Archived: {combined['archived']:,} ({pct(combined['archived'], combined['json_total']):.2f}%)")
    lines.append(f"  Forked AND Archived: {combined['forked_and_archived']:,} ({pct(combined['forked_and_archived'], combined['json_total']):.2f}%)")
    lines.append(f"  Excluded (Forked | Archived | Errors): {combined['excluded']:,} ({pct(combined['excluded'], combined['json_total']):.2f}%)")
    lines.append(f"  Valid (for analysis): {combined['valid']:,} ({pct(combined['valid'], combined['json_total']):.2f}%)")
    lines.append("")
    lines.append("Data Source:")
    lines.append(f"  From Cache: {combined['from_cache']:,} ({pct(combined['from_cache'], combined['successfully_mined']):.2f}%)")
    lines.append(f"  From API: {combined['from_api']:,} ({pct(combined['from_api'], combined['successfully_mined']):.2f}%)")
    lines.append("")
    if combined_errors:
        lines.append("Error Breakdown:")
        for error_type, count in sorted(combined_errors.items()):
            lines.append(f"  {error_type}: {count:,} ({pct(count, combined['json_total']):.2f}%)")
    lines.append("")
    
    # Timestamp
    lines.append("=" * 80)
    lines.append(f"Summary generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    lines.append("=" * 80)
    
    return "\n".join(lines)


def main():
    """Main execution function."""
    print("=" * 80)
    print("REGENERATING GITHUB METRICS MINING SUMMARY")
    print("=" * 80)
    print()
    
    # Load data
    packages = load_github_metrics()
    input_counts = load_cross_ecosystem_data()
    
    # Analyze
    stats = analyze_metrics(packages)
    
    # Print analysis results
    print("\n" + "=" * 80)
    print("ANALYSIS RESULTS")
    print("=" * 80)
    
    for source in ['monorepo', 'multirepo']:
        s = stats[source]
        print(f"\n{source.upper()}:")
        print(f"  Total in JSON: {s['total']:,}")
        print(f"  Has error: {s['has_error']:,}")
        print(f"  Is forked (only): {s['is_forked']:,}")
        print(f"  Is archived (only): {s['is_archived']:,}")
        print(f"  Forked AND archived: {s['forked_and_archived']:,}")
        print(f"  Valid (no error, not forked, not archived): {s['valid']:,}")
        if s['errors_by_type']:
            print(f"  Error types: {dict(s['errors_by_type'])}")
    
    # Calculate summary
    summary = calculate_summary_stats(stats, input_counts)
    
    # Generate summary text
    summary_text = generate_summary_text(summary, input_counts)
    
    # Write to file
    print(f"\n\nWriting summary to: {OUTPUT_PATH}")
    with open(OUTPUT_PATH, 'w') as f:
        f.write(summary_text)
    
    print("\n" + "=" * 80)
    print("SUMMARY REGENERATED SUCCESSFULLY")
    print("=" * 80)
    
    # Print the summary
    print("\n" + summary_text)


if __name__ == "__main__":
    main()
