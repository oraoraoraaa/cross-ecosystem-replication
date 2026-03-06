#!/bin/bash
# Step 6: Analyze architectural patterns
# 1. Detect language-specific named folders (P3: Designated Directory)
# 2. Detect binding/wrapper patterns (P5: Bind/Wrap)
# 3. Detect platform-specific folder patterns (part of P5)
# 4. Consolidate all patterns into per-pattern JSON files
# 5. Analyze correlation between patterns and GitHub health metrics

set -e
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$SCRIPT_DIR"

echo "=============================================="
echo "Step 6: Analyzing Architectural Patterns"
echo "=============================================="

# Check if Python 3 is installed
if ! command -v python3 &> /dev/null; then
    echo "Error: Python 3 is not installed"
    exit 1
fi

# Create and activate virtual environment
if [ ! -d "venv" ]; then
    echo "Creating virtual environment..."
    python3 -m venv venv
fi
source venv/bin/activate
pip install -q -r ../../requirements.txt

# Run the scripts in sequence
echo ""
echo "[6a] Detecting language-specific named folders..."
python detect_naming_convention.py

echo ""
echo "[6b] Detecting binding/wrapper patterns..."
python detect_binding.py

echo ""
echo "[6c] Detecting platform-specific folders..."
python detect_platform_folders.py

echo ""
echo "[6d] Consolidating all patterns..."
python consolidate_patterns.py

echo ""
echo "[6e] Analyzing pattern-metric correlations..."
python analyze_correlation.py

echo ""
echo "Step 6 completed successfully."
