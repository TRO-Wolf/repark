#!/usr/bin/env bash
# One-command census gate (classic + expand + expand2). SSOT procedure for the port's
# acceptance runs. Not CI-wired (Actions-minutes; ~20 min wall per module).
#
# Ported from the port source with TWO declared changes:
#
# 1. (phase-3 EC-8, design §5 F1) the classic cohort is run with `--classic`, never
#    `--stretch`. `--stretch` APPENDS the C3 expand modules to night-1, which blends the C3
#    cohort into the classic /345 denominator. The report output paths are unchanged in shape.
# 2. the run's ENVIRONMENT is recorded, not assumed: a verbatim `pip freeze` plus a machine
#    manifest (`census-manifest.json`) carrying the versions the comparator gates —
#    `python_version`, `pyspark_version`, `pandas_version`, `pyarrow_version`. Design §5 F2:
#    "a baseline whose environment is not recorded is not a baseline", and the pandas major
#    in particular changes the measurement (docs/port/census.md §1). The comparator now
#    REFUSES to diff a run whose pandas major is unrecorded, so the manifest is an input to
#    the gate, not documentation.
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

# ---- record the environment (design §5 F2; docs/port/census.md §1) ------------------------
# The freeze is the human-readable half; census-manifest.json is the half the comparator
# reads through --manifest-baseline / --manifest-candidate. Both are written BEFORE any
# cohort runs, so a crashed run still leaves the environment it crashed in on disk.
uv pip freeze > "$SCRATCH/census-venv-freeze.txt"
if [[ ! -s "$SCRATCH/census-venv-freeze.txt" ]]; then
  echo "census: FATAL — pip freeze is empty; a run whose environment is not recorded is" \
       "not a baseline (design §5 F2)" >&2
  exit 2
fi
python - "$SCRATCH/census-manifest.json" <<'PY'
import importlib.metadata as md
import json
import platform
import sys


def version(name: str) -> str:
    try:
        return md.version(name)
    except md.PackageNotFoundError:
        return ""


manifest = {
    "python_version": platform.python_version(),
    "pyspark_version": version("pyspark"),
    "pandas_version": version("pandas"),
    "pyarrow_version": version("pyarrow"),
}
missing = [key for key, value in manifest.items() if not value]
if missing:
    raise SystemExit(f"census: FATAL — unrecorded environment key(s): {', '.join(missing)}")
if int(manifest["pandas_version"].split(".")[0]) >= 3:
    raise SystemExit(
        "census: FATAL — pandas "
        + manifest["pandas_version"]
        + " violates the recipe pin pandas>=2.1,<3. A census run under pandas 3 is a "
        "different measurement, not a noisier one (docs/port/census.md §1)."
    )
with open(sys.argv[1], "w", encoding="utf-8") as handle:
    json.dump(manifest, handle, indent=2, sort_keys=True)
    handle.write("\n")
print("census: environment manifest —", json.dumps(manifest, sort_keys=True))
PY

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

# ---- redact absolute paths, through each artifact's parser (docs/port/census.md §3) -------
# Committed census artifacts are evidence in a public repository, so scratch paths are
# replaced by stable tokens. The transform runs through `compat.redact` — never `sed` — so
# the reports stay valid JSON and the JUnit XML stays well-formed; a textual substitution
# over these bytes breaks the string escaping and the artifact stops parsing, which makes the
# comparator exit 2 on its own baseline.
python -m compat.redact \
  --map "${SCRATCH}=<scratch>" \
  --map "${REPO_ROOT}=<repo>" \
  --map "${HOME}=<home>" \
  "$SCRATCH"/*/compat-report.json \
  "$SCRATCH/census-venv-freeze.txt" \
  "$SCRATCH/census-manifest.json"

echo "census: wrote markdown under $REPORT_DIR (JSON + freeze + manifest under $SCRATCH)"
echo "census: classic/expand/expand2 complete"
