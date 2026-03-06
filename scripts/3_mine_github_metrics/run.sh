#!/bin/bash
# Step 3: Mine GitHub metrics for all cross-ecosystem packages
# Collects stars, forks, commits, PRs, issues, contributors, and language proportions.
# Requires GitHub API token(s) via GITHUB_TOKEN env var or --token flag.

set -e
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$SCRIPT_DIR"

echo "=============================================="
echo "Step 3: Mining GitHub Metrics"
echo "=============================================="

# Check for GitHub token
if [ -z "$GITHUB_TOKEN" ]; then
    echo "Warning: GITHUB_TOKEN environment variable not set."
    echo "Set it with: export GITHUB_TOKEN=ghp_your_token_here"
    echo "Or pass tokens via: python mine_github_metrics.py --token ghp_token1 ghp_token2"
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

# Run the scripts in sequence
echo ""
echo "[3a] Mining GitHub metrics..."
python mine_github_metrics.py "$@"

echo ""
echo "[3b] Mining unique contributors for multi-repo groups..."
python mine_unique_contributors.py "$@"

echo ""
echo "[3c] Generating summary..."
python generate_summary.py

echo ""
echo "Step 3 completed successfully."
