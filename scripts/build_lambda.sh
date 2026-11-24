#!/usr/bin/env bash
# Build the poller Lambda package (arm64, Python 3.12) into build/lambda/.
# Terraform zips that folder. Run from the repository root.
set -euo pipefail

OUT=build/lambda
rm -rf "$OUT"
mkdir -p "$OUT/config"

uv export --no-dev --no-hashes --no-emit-project --format requirements.txt > build/requirements-lambda.txt
uv pip install \
  --quiet \
  --target "$OUT" \
  --python-version 3.12 \
  --python-platform aarch64-manylinux2014 \
  --only-binary :all: \
  -r build/requirements-lambda.txt

cp -R src/bahn_pulse "$OUT/"
cp config/station_tiers.csv "$OUT/config/"
find "$OUT" -type d -name "__pycache__" -prune -exec rm -rf {} +

echo "Built $OUT ($(du -sh "$OUT" | cut -f1))"
