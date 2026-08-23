#!/usr/bin/env bash
# ===========================================================================================
# Public-docstring presence guard — Ruff D101/D102/D103/D105/D107 with a ratchet.
#
# SSOT is check_docstring_presence.py (the five presence rules + EXCEPTIONS table).
# This wrapper only invokes it. Style D is declined and is not selected.
# Dual-wired: make check-docstring-presence (in make ci) AND a ci.yml python-job step.
# On pre-commit: ruff JSON over the scan set is sub-second (measured at arming).
# ===========================================================================================
set -euo pipefail
repo_root="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)"
exec python3 -I "$repo_root/scripts/check_docstring_presence.py"
