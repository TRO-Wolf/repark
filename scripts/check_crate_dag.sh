#!/usr/bin/env bash
# ===========================================================================================
# Crate-DAG layering guard (BH4).
#
# The layering SSOT is `check_crate_dag.py` (it holds the tier map); this wrapper only feeds it
# `cargo metadata`. Prose (`crates/map.md`, `AGENTS.md`) points at the .py and never restates
# the tier map.
#
# The rule: every internal (`repark-*`) NORMAL dependency edge must point at a tier that is
# NOT STRICTLY HIGHER than the depending crate's tier. Same-tier edges are ALLOWED
# (sibling crates at one tier are legitimate); only an edge that reaches UP a tier is a
# violation.
#
# Why not strict inequality: strict "must be a lower tier" over measured depth approximates
# a cycle check, and Cargo already makes cycles a hard error. The failure this guard exists
# to catch is the acyclic-but-INVERTED edge — a foundation crate reaching up into an
# orchestration crate — which Cargo happily accepts.
#
# Scope: NORMAL dependency edges only. dev-dependencies and build-dependencies are EXCLUDED
# (a test-only edge is not a layering statement), and third-party crates are out of scope.
#
# Wired into `make check-crate-dag` (part of the `make ci` chain) and the pre-commit hook
# installed by `make install-hooks`. Exits non-zero on any violation, naming the offending
# edge and both tiers.
# ===========================================================================================
set -euo pipefail

repo_root="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)"

cargo metadata --format-version 1 --no-deps --locked --manifest-path "$repo_root/Cargo.toml" \
  | python3 "$repo_root/scripts/check_crate_dag.py"
