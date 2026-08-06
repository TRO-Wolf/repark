#!/usr/bin/env bash
# ===========================================================================================
# map.md lockstep guard.
#
# Enforces this repo's hard rule: any change to a `.rs`/`.py`/`Cargo.toml`/`pyproject.toml` file must update that directory's
# `map.md` in the SAME commit (and the directory must HAVE a map.md). Wired into the pre-commit
# hook (.pre-commit-config.yaml / `make install-hooks`). Exits non-zero on any violation.
# ===========================================================================================
set -euo pipefail

# Staged files (exclude deletions).
staged="$(git diff --cached --name-only --diff-filter=d)"
staged_all="$(git diff --cached --name-only)"
missing=0
declare -A checked

while IFS= read -r file; do
  [[ -z "$file" ]] && continue
  case "$file" in
    *.rs | *.py) ;;
    # Manifests (any directory). Bash case is path-suffix aware via */name patterns.
    # Lockfiles intentionally excluded: Cargo.lock / uv.lock churn without map updates.
    */Cargo.toml | Cargo.toml | */pyproject.toml | pyproject.toml) ;;
    *) continue ;;
  esac
  dir="$(dirname "$file")"
  [[ -n "${checked[$dir]:-}" ]] && continue
  checked[$dir]=1

  # Root-level paths: dirname is "."; map path is "map.md" not "./map.md".
  if [[ "$dir" == "." ]]; then
    map_path="map.md"
  else
    map_path="$dir/map.md"
  fi

  if [[ ! -f "$map_path" ]]; then
    echo "ERROR: $dir has staged code but no map.md (every directory needs one)." >&2
    missing=1
    continue
  fi
  if ! grep -qx "$map_path" <<<"$staged_all"; then
    echo "ERROR: $map_path was not updated in this commit (map.md lockstep rule)." >&2
    missing=1
  fi
done <<<"$staged"

exit "$missing"
