#!/bin/bash
# Package Lambda deployment zip.
#
# Creates build/lambda.zip containing:
#   - src/common/   (analyzer, url_parser, config, search_engines.toml)
#   - src/lambda/   (handler)
#   - duckdb        (installed into the package)
#
# Usage:
#   chmod +x scripts/package_lambda.sh
#   ./scripts/package_lambda.sh
#
# Output: build/lambda.zip (referenced by terraform/variables.tf)

set -euo pipefail

PROJECT_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
BUILD_DIR="$PROJECT_ROOT/build"
PACKAGE_DIR="$BUILD_DIR/package"

echo "=== Cleaning previous build ==="
rm -rf "$BUILD_DIR"
mkdir -p "$PACKAGE_DIR"

echo "=== Installing duckdb into package ==="
pip install duckdb \
    --target "$PACKAGE_DIR" \
    --platform manylinux2014_x86_64 \
    --only-binary=:all: \
    --python-version 3.12 \
    --quiet

echo "=== Copying source code ==="
# Copy common module (analyzer, url_parser, config, TOML)
cp -r "$PROJECT_ROOT/src/common" "$PACKAGE_DIR/common"

# Copy lambda handler as a top-level module
cp -r "$PROJECT_ROOT/src/lambda" "$PACKAGE_DIR/lambda"

echo "=== Creating zip ==="
cd "$PACKAGE_DIR"
zip -r "$BUILD_DIR/lambda.zip" . -q

echo "=== Done ==="
echo "Output: $BUILD_DIR/lambda.zip"
echo "Size: $(du -h "$BUILD_DIR/lambda.zip" | cut -f1)"
