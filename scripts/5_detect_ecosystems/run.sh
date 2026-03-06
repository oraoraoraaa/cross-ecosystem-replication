#!/bin/bash
# Step 5: Detect ecosystems present in repository directory structures
# 1. Verify multi-repo packages have expected languages
# 2. Detect ecosystems from source file extensions in directory structures
# 3. Detect special patterns (templates, bindings, WebJars, etc.)

set -e
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$SCRIPT_DIR"

echo "=============================================="
echo "Step 5: Detecting Ecosystems"
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
echo "[5a] Verifying multi-repo languages..."
python verify_multirepo_languages.py

echo ""
echo "[5b] Detecting ecosystems from source files..."
python detect_ecosystems.py

echo ""
echo "[5c] Detecting special patterns..."
python detect_special_patterns.py

echo ""
echo "Step 5 completed successfully."
