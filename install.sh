#!/bin/bash
# Installation script for Sparkless package

set -e

echo "🚀 Installing Sparkless package..."

# Check if Python is available
if ! command -v python &> /dev/null; then
    echo "❌ Python is not installed or not in PATH"
    exit 1
fi

# Check Python version
python_version=$(python -c "import sys; print(f'{sys.version_info.major}.{sys.version_info.minor}')")
required_version="3.8"

if [ "$(printf '%s\n' "$required_version" "$python_version" | sort -V | head -n1)" != "$required_version" ]; then
    echo "❌ Python $required_version or higher is required. Found: $python_version"
    exit 1
fi

echo "✅ Python $python_version detected"

# Install the package
echo "📦 Installing Sparkless..."
pip install -e .

# Run full test suite with proper isolation
echo "🧪 Running full test suite..."
bash tests/run_all_tests.sh

echo "✅ Sparkless installed successfully!"
echo ""
echo "Usage:"
echo "  from sparkless.sql import SparkSession"
echo "  spark = SparkSession('MyApp')"
echo ""
echo "For development:"
echo "  pip install -e .[dev]"
