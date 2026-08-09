#!/usr/bin/env bash
# ===========================================================================================
# Crate dependency-policy guard (BH4).
#
# The policy SSOT is `check_crate_dag.py` (it holds the tier map, the crate roles, and the
# explicit allowed-edge table); this wrapper only feeds it `cargo metadata`. Prose
# (`crates/map.md`, `ARCHITECTURE.md`, `AGENTS.md`, `repo-manifest.toml`) points at the .py and
# never restates any of the three.
#
# The rules, in the order the .py applies them:
#   1. the declared policy must itself obey the structural rules (rule 3) — a forbidden edge
#      cannot be legalized by writing it down;
#   2. EVERY internal edge must be DECLARED in the allowed-edge table, with its dependency
#      KIND (normal / optional / dev / build) permitted for that pair — internal = any Cargo
#      workspace member (membership, not the `repark-` name, is the test); a new same-tier
#      edge fails until it is declared with a reason;
#   3. structural rules over crate ROLES: no door -> door edge outside `dev`; nothing may depend
#      on the bindings adapter; the foundation crate depends on nothing internal; a capability
#      (kernel/function) crate never depends on a user-facing door;
#   4. layering: no PRODUCT edge (normal / optional) may point at a STRICTLY HIGHER tier.
#      Same-tier edges are ALLOWED — sibling crates at one tier are legitimate.
#
# Why layering is not strict inequality: strict "must be a lower tier" over measured depth
# approximates a cycle check, and Cargo already makes cycles a hard error. The failure rule 4
# exists to catch is the acyclic-but-INVERTED edge — a foundation crate reaching up into an
# orchestration crate — which Cargo happily accepts. Rules 1-3 catch what a tier map cannot
# state at all: which edges exist, and in which kind.
#
# Scope: INTERNAL = any Cargo workspace member. All four kinds are inspected against the edge
# table (that is how the dev-only cross-door edge is expressible without permitting a product
# one); only PRODUCT kinds are subject to the tier rule, because a test-only edge is not a
# layering statement. Third-party crates are out of scope.
#
# Wired into `make check-crate-dag` (part of the `make ci` chain) and the pre-commit hook
# installed by `make install-hooks`. Exits non-zero on any violation, naming the offending
# source, target and kind.
# ===========================================================================================
set -euo pipefail

repo_root="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)"

cargo metadata --format-version 1 --no-deps --locked --manifest-path "$repo_root/Cargo.toml" \
  | python3 "$repo_root/scripts/check_crate_dag.py"
