#!/bin/bash
# Step 4: Mine directory structures from GitHub repositories
# Fetches complete directory trees for all mono-repo cross-ecosystem packages.
# Requires GitHub API token(s) via GITHUB_TOKEN env var or --token flag.

set -e
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$SCRIPT_DIR"

echo "=============================================="
echo "Step 4: Mining Directory Structures"
echo "=============================================="

# Check for GitHub token
if [ -z "$GITHUB_TOKEN" ]; then
    echo "Warning: GITHUB_TOKEN environment variable not set."
    echo "Set it with: export GITHUB_TOKEN=ghp_your_token_here"
    echo ""
fi

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

# Run the miner
python mine_directory_structures.py "$@"

echo ""
echo "Step 4 completed successfully."
