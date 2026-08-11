#!/usr/bin/env bash
# ===========================================================================================
# General Rust file-size guard (G-8 companion to check_lib_rs).
#
# SSOT is check_rust_file_size.py (default ceiling + EXCEPTIONS). This wrapper only invokes it.
# Dual-wired: make check-rust-file-size (in make ci) AND a ci.yml guards-job step.
# Pre-commit via install-hooks / .pre-commit-config.yaml when measured <1s.
# ===========================================================================================
set -euo pipefail
repo_root="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)"
exec python3 "$repo_root/scripts/check_rust_file_size.py"
