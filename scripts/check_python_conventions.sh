#!/usr/bin/env bash
# ===========================================================================================
# Python conventions guard — the two rules Ruff cannot express.
#
# SSOT is check_python_conventions.py (the nested-def ban + the Pydantic-not-dataclasses rule,
# with their EXCEPTIONS tables). This wrapper only invokes it.
# Dual-wired: make check-python-conventions (in make ci) AND a ci.yml python-job step.
# Not on pre-commit as of PYC-5 (hook budget).
# ===========================================================================================
set -euo pipefail
repo_root="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)"
exec python3 "$repo_root/scripts/check_python_conventions.py"
