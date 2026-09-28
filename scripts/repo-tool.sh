#!/usr/bin/env bash
set -euo pipefail
root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
manifest="$root/scripts/repo-tool/Cargo.toml"
target="$root/scripts/repo-tool/target"
build_status=127
if command -v cargo >/dev/null 2>&1; then
  if CARGO_BUILD_JOBS="${CARGO_BUILD_JOBS:-2}" cargo build --quiet --locked --release --manifest-path "$manifest" --target-dir "$target"; then
    exec "$target/release/repark-repo" --repo "$root" "$@"
  else
    build_status=$?
  fi
fi
if [[ "${1:-}" == "--snapshot" && "${2:-}" == "index" && "${3:-}" == "maps" && "${4:-}" == "--check" ]]; then
  echo "repo-tool: Rust build unavailable; checking staged maps with Python" >&2
  exec python3 "$root/scripts/repo-tool-fallback.py" --repo "$root" "$@"
fi
exit "$build_status"
