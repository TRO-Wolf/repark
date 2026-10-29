#!/usr/bin/env bash
set -euo pipefail

if [[ "${1:-}" == "--base" ]]; then
  base="${2:?usage: check_map_md.sh [--base <ref>]}"
  if ! git rev-parse --verify --quiet "${base}^{commit}" >/dev/null; then
    echo "ERROR: --base ${base} is not a commit in this repository" >&2
    exit 2
  fi
  changed_code="$(git diff --name-only --diff-filter=d "${base}...HEAD")"
  changed_all="$(git diff --name-only "${base}...HEAD")"
  branch_mode=1
elif [[ $# -gt 0 ]]; then
  echo "usage: check_map_md.sh [--base <ref>]" >&2
  exit 2
else
  changed_code="$(git diff --cached --name-only --diff-filter=d)"
  changed_all="$(git diff --cached --name-only)"
  branch_mode=0
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
    if [[ "$branch_mode" == 1 ]]; then
      echo "WARNING: $dir has changed code but no map.md (every directory needs one)." >&2
    else
      echo "WARNING: $dir has staged code but no map.md (every directory needs one)." >&2
    fi
    missing=1
    continue
  fi
  if ! grep -qx "$map_path" <<<"$changed_all"; then
    if [[ "$branch_mode" == 1 ]]; then
      echo "WARNING: $map_path was not updated on this branch (map.md lockstep rule)." >&2
    else
      echo "WARNING: $map_path was not staged with $dir's code (map.md lockstep rule)." >&2
    fi
    missing=1
  fi
done <<<"$changed_code"

if [[ "$missing" == 1 ]]; then
  echo "note: map.md lockstep is advisory; the required map gates are scripts/sync_map_md.py --check and scripts/check_map_size.py." >&2
fi
exit 0
