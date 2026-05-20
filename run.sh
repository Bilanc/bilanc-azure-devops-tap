#!/usr/bin/env bash
set -euo pipefail

CONFIG="config.json"
CATALOG="catalog.json"
CATALOG_SELECTED="catalog_selected.json"
OUTPUT="data.singer"

# ── Preflight ────────────────────────────────────────────────────────────────

if [ ! -f "$CONFIG" ]; then
  echo "ERROR: $CONFIG not found. Create it before running this script."
  exit 1
fi

if ! command -v jq &>/dev/null; then
  echo "ERROR: jq is required. Install it with: brew install jq"
  exit 1
fi

# ── Virtual environment ──────────────────────────────────────────────────────

echo "Creating virtual environment..."
rm -rf .venv
python3 -m venv .venv
source .venv/bin/activate

echo "Installing tap..."
pip install -e . --quiet

# ── Discover ─────────────────────────────────────────────────────────────────

echo "Running discovery..."
tap-azure-devops --config "$CONFIG" --discover > "$CATALOG"
echo "Catalog written to $CATALOG"

# ── Select all streams ───────────────────────────────────────────────────────

echo "Selecting all streams..."
jq '
  .streams[].metadata[] |= (
    if .breadcrumb == []
    then .metadata.selected = true
    else .
    end
  )
' "$CATALOG" > "$CATALOG_SELECTED"

# ── Sync ─────────────────────────────────────────────────────────────────────

echo "Running sync -> $OUTPUT"
tap-azure-devops --config "$CONFIG" --properties "$CATALOG_SELECTED" > "$OUTPUT"

RECORD_COUNT=$(grep -c '^{"type":"RECORD"' "$OUTPUT" 2>/dev/null || echo 0)
echo "Done. $RECORD_COUNT records written to $OUTPUT"
