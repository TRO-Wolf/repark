#!/usr/bin/env bash
# One-command census gate (classic + expand + expand2). SSOT procedure for the port's
# acceptance runs. Not CI-wired (Actions-minutes; ~20 min wall per module).
#
# Ported from the port source with ONE behavioral change (phase-3 EC-8, design §5 F1): the
# classic cohort is run with `--classic`, never `--stretch`. `--stretch` APPENDS the C3
# expand modules to night-1, which blends the C3 cohort into the classic /345 denominator.
# The report output paths are unchanged in shape.
#
# Requires the facade package at `python/repark`; it arrives with the phase-3 facade PR, so
# this script is runnable in this repository from that point on. The recorded procedure —
# environment recipe, stability run, cohort argument vectors — is docs/port/census.md.
set -euo pipefail
REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$REPO_ROOT"

DATE_STAMP="${CENSUS_DATE:-$(date -u +%Y-%m-%d)}"
SCRATCH="${REPARK_CENSUS_SCRATCH:-/tmp/repark-census-${DATE_STAMP}}"
VENV="${CENSUS_VENV:-$SCRATCH/venv}"
REPORT_DIR="${CENSUS_REPORT_DIR:-$REPO_ROOT/task}"
mkdir -p "$SCRATCH" "$REPORT_DIR"

if [[ ! -d "$VENV" ]]; then
  uv venv "$VENV"
fi
# shellcheck disable=SC1091
source "$VENV/bin/activate"

# Provision: repark editable + pyspark + record extras when available
uv pip install -e "python/repark" "pyspark==4.1.2" pytest pyarrow 2>/dev/null \
  || uv pip install -e python/repark pyspark pytest pyarrow
(cd python/repark && uvx maturin@1.14.1 develop) >/dev/null

export PYTHONPATH="${REPO_ROOT}/python/repark-parity:${REPO_ROOT}/python/repark/src${PYTHONPATH:+:$PYTHONPATH}"

run_cohort() {
  local name="$1"; shift
  local out="$SCRATCH/$name"
  mkdir -p "$out"
  export REPARK_COMPAT_SCRATCH="$out"
  echo "==> census cohort: $name"
  python -m compat.runner "$@" \
    --output "$out/compat-report.json" \
    --markdown "$REPORT_DIR/pyspark-compat-report-${name}-${DATE_STAMP}.md"
  # print denominators from markdown if present
  if [[ -f "$REPORT_DIR/pyspark-compat-report-${name}-${DATE_STAMP}.md" ]]; then
    grep -E 'pass / all_collected|pass / engine' \
      "$REPORT_DIR/pyspark-compat-report-${name}-${DATE_STAMP}.md" | head -5 || true
  fi
}

# classic 5-module cohort, denominator-isolated (never --stretch — see the header note)
run_cohort classic --classic
# expand (C3)
run_cohort expand --c3-expand
# expand2 (C4)
run_cohort expand2 --c4-expand

echo "census: wrote markdown under $REPORT_DIR (JSON under $SCRATCH)"
echo "census: classic/expand/expand2 complete"
