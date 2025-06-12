#!/usr/bin/env bash
# Usage:  ./amalgamate.sh <root_dir> <max_depth> <output_file>
# Example: ./amalgamate.sh ./src 3 amalgamated.cpp
#
# Collects every *.h / *.cpp reachable from <root_dir> up to <max_depth>
# and concatenates them into <output_file>.
# Each chunk is preceded by a C-style comment with its relative path.

set -euo pipefail

ROOT=${1:-.}
DEPTH=${2:-3}
OUT=${3:-amalgamated.cpp}

# Start clean
: > "$OUT"

# Depth-limited DFS using `find`
find "$ROOT" -mindepth 1 -maxdepth "$DEPTH" \
     -type f \( -name '*.h' -o -name '*.cpp' \) \
     | sort | while IFS= read -r F; do
        REL=${F#"$ROOT"/}
        printf '\n// ===== %s =====\n\n' "$REL" >> "$OUT"
        cat "$F" >> "$OUT"
done
