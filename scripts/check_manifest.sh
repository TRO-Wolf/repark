#!/usr/bin/env bash
# ===========================================================================================
# Structural-manifest guard (FD-3).
#
# SSOT is repo-manifest.toml (the structural facts) + check_manifest.py (the rules). This
# wrapper only invokes it — no cargo, no network: it reads Cargo.toml, the Makefile, STATUS.md,
# the declared documents and the crate-root map.md files, so it is a pure-text sub-second gate.
#
# What it enforces: every Cargo member is declared and every delivered component is a member;
# delivered components exist at their path; a planned path must NOT exist; layers agree with
# the dependency-policy SSOT (check_crate_dag.py); the canonical make targets and declared
# documents exist; STATUS.md states the manifest's phase; and each delivered component's
# hand-written crate-root map.md names it and its tier. It CHECKS map.md files — it never
# generates one.
#
# Dual-wired: make check-manifest (in make ci) AND a ci.yml guards-job step — change one,
# change the other. Pre-commit via install-hooks.
# ===========================================================================================
set -euo pipefail
repo_root="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)"
exec python3 "$repo_root/scripts/check_manifest.py"
