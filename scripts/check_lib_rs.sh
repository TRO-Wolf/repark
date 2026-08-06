#!/usr/bin/env bash
# ===========================================================================================
# lib.rs thinness guard (r26 LR2).
#
# SSOT is check_lib_rs.py (ceilings + EXCEPTIONS). This wrapper only invokes it.
# Dual-wired: make check-lib-rs (in make ci) AND a ci.yml guards-job step (the dual-wire
# rule carried from the private v1 repository). Pre-commit via install-hooks when measured <1s.
# ===========================================================================================
set -euo pipefail
repo_root="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)"
exec python3 "$repo_root/scripts/check_lib_rs.py"
