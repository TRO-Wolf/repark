#!/usr/bin/env bash
# ===========================================================================================
# Example-coverage drift guard — public names vs docs/examples COVERS vs backlog ratchet.
#
# SSOT is check_example_coverage.py (enumerator, coverage rules, backlog baseline).
# This wrapper only invokes it. Wired: make check-example-coverage (in make ci).
# ===========================================================================================
set -euo pipefail
repo_root="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)"
exec python3 -I "$repo_root/scripts/check_example_coverage.py"
