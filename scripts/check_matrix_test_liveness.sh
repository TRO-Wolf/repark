#!/usr/bin/env bash
# ===========================================================================================
# Surface-matrix test-name liveness guard (H-2 G8).
#
# SSOT is check_matrix_test_liveness.py (`cargo test -- --list` vs both doors' matrix.rs
# Tested cites). This wrapper only invokes it.
# Dual-wired: make check-matrix-test-liveness (in make ci / make preflight) AND a
# ci.yml rust-test-job step. Change one, change the other.
# ===========================================================================================
set -euo pipefail
repo_root="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)"
exec python3 "$repo_root/scripts/check_matrix_test_liveness.py"
