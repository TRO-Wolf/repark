#!/usr/bin/env bash
# ===========================================================================================
# parity-live dual-wire guard (G-6).
#
# SSOT is check_parity_live_dual_wire.py (compares Makefile parity-live ↔ parity-live.yml
# to each other — never to a third hand-maintained list). This wrapper only invokes it.
# Dual-wired: make check-parity-live-dual-wire (in make ci) AND a ci.yml guards-job step.
# ===========================================================================================
set -euo pipefail
repo_root="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)"
exec python3 "$repo_root/scripts/check_parity_live_dual_wire.py"
