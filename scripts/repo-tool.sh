#!/usr/bin/env bash
set -euo pipefail
root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
manifest="$root/scripts/repo-tool/Cargo.toml"
target="$root/scripts/repo-tool/target"
CARGO_BUILD_JOBS="${CARGO_BUILD_JOBS:-2}" cargo build --quiet --locked --release --manifest-path "$manifest" --target-dir "$target"
exec "$target/release/repark-repo" --repo "$root" "$@"
