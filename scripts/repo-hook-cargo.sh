#!/usr/bin/env bash
set -euo pipefail

case "${1:-}" in
  dag)
    changed="$(git diff --cached --name-only -- Cargo.toml ':(glob)**/Cargo.toml')"
    [[ -z "$changed" ]] && exit 0
    if ! command -v cargo >/dev/null 2>&1; then
      echo "repo hook: Cargo is required for staged Cargo.toml changes" >&2
      exit 2
    fi
    exec scripts/check_crate_dag.sh
    ;;
  fmt)
    changed="$(git diff --cached --name-only --diff-filter=ACMR -- '*.rs')"
    [[ -z "$changed" ]] && exit 0
    if ! command -v cargo >/dev/null 2>&1; then
      echo "repo hook: Cargo is required for staged Rust changes" >&2
      exit 2
    fi
    exec cargo fmt --check
    ;;
  *)
    echo "usage: repo-hook-cargo.sh dag|fmt" >&2
    exit 2
    ;;
esac
