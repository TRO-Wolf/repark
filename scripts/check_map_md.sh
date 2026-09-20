#!/usr/bin/env bash
set -euo pipefail

if [[ "${1:-}" == "--base" ]]; then
  base="${2:?usage: check_map_md.sh [--base <ref>]}"
  changed_code="$(git diff --name-only --diff-filter=d "${base}...HEAD")"
  changed_all="$(git diff --name-only "${base}...HEAD")"
  warn_only=0
elif [[ $# -gt 0 ]]; then
  echo "usage: check_map_md.sh [--base <ref>]" >&2
  exit 2
else
  changed_code="$(git diff --cached --name-only --diff-filter=d)"
  changed_all="$(git diff --cached --name-only)"
  warn_only=1
fi

missing=0
declare -A checked

while IFS= read -r file; do
  [[ -z "$file" ]] && continue
  case "$file" in
    *.rs | *.py) ;;
    */Cargo.toml | Cargo.toml | */pyproject.toml | pyproject.toml) ;;
    *) continue ;;
  esac
  dir="$(dirname "$file")"
  [[ -n "${checked[$dir]:-}" ]] && continue
  checked[$dir]=1

  if [[ "$dir" == "." ]]; then
    map_path="map.md"
  else
    map_path="$dir/map.md"
  fi

  if [[ ! -f "$map_path" ]]; then
    if [[ "$warn_only" == 1 ]]; then
      echo "WARNING: $dir has staged code but no map.md (every directory needs one)." >&2
    else
      echo "ERROR: $dir has changed code but no map.md (every directory needs one)." >&2
    fi
    missing=1
    continue
  fi
  if ! grep -qx "$map_path" <<<"$changed_all"; then
    if [[ "$warn_only" == 1 ]]; then
      echo "WARNING: $map_path was not staged with $dir's code (map.md lockstep rule)." >&2
    else
      echo "ERROR: $map_path was not updated on this branch (map.md lockstep rule)." >&2
    fi
    missing=1
  fi
done <<<"$changed_code"

if [[ "$warn_only" == 1 ]]; then
  if [[ "$missing" == 1 ]]; then
    echo "note: the pull-request gate holding this rule is ci.yml's map.md guard step (make check-map-md, BASE=origin/main)." >&2
  fi
  exit 0
fi

exit "$missing"
