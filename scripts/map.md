# map — scripts/

## Purpose

Repository helper scripts wired into the dev workflow.

## Contents

- `check_map_md.sh` — the map.md lockstep guard: fails a commit if a staged `.rs`/`.py`/
  `Cargo.toml`/`pyproject.toml` file's directory has no same-change `map.md` update (lockfiles
  excluded; root-level manifests map to `map.md`, not `./map.md`). Invoked by
  `.pre-commit-config.yaml`, `make check-map-md`, and the hook installed by `make install-hooks`.
- `check_workflows_parse.py` — every GitHub Actions workflow must be parseable YAML. zizmor
  SKIPS files it cannot parse (exits 0 with "no auditable inputs"), so a broken workflow would
  pass the blocking lint gate while GitHub silently never runs it. Wired as a prerequisite of
  `make workflows-lint`.
- `check_crate_dag.sh` + `check_crate_dag.py` — the crate-DAG layering guard. The `.sh` runs
  `cargo metadata --format-version 1 --no-deps --locked` and pipes it to the `.py`, which holds
  the **tier map** and the rule: no `repark-*` crate may depend on a **strictly higher** tier;
  same-tier edges are ALLOWED. NORMAL edges only — dev/build deps excluded, third-party out of
  scope. `check_crate_dag.py` is the layering **SSOT**: prose points here and never restates
  the map (phase-1 target: `repark-common` tier 0, `repark-iceberg` tier 1, `repark-core`
  tier 2; phase-2 pre-declares tier 3 "surface crates": `repark-functions`, `repark-ta`,
  `repark-spark`, `repark-sql`; phase-3 pre-declares `repark-ml` at tier 3 and
  `repark-python` at tier 4 "bindings" — the only tier-4 crate, nothing may depend on it;
  NOTE the binding's dep contract — only core/functions/ta/spark/ml, non-edges repark-sql +
  repark-iceberg — is enforced by review, not by this guard, which only bans upward edges;
  mapped crates that have not landed yet are simply not inspected). A new `repark-*`
  crate that is not in `TIERS` fails the guard. Wired into `make check-crate-dag` (in the
  `make ci` chain), `.pre-commit-config.yaml`, and the hook installed by `make install-hooks`.
  **Dual-wired:** the `crate-DAG layering guard` step in the ci.yml `guards` job mirrors the
  Makefile target — change one, change the other.
- `check_lib_rs.sh` + `check_lib_rs.py` — the lib.rs thinness guard. No inline
  `#[cfg(test)] mod {…}` (file-backed only; same-line `#[cfg(test)] mod … {` also fails);
  non-test line ceilings with an EXCEPTIONS-with-reason table in the `.py` (SSOT; ratchet down
  only; empty at phase-1 PR-A; rows so far: `repark-functions` — registration glue,
  `repark-python` — the 180-line PyO3 crate root, a MANIFEST (module decls incl. the
  file-backed `exceptions` taxonomy module, the two error folds, the `#[pymodule]`
  registration) that already uses the sanctioned file-backed test module (phase-3 PR-3, EC-10;
  ceiling ratcheted 230 → 190 when the taxonomy moved to src/exceptions.rs — without the row
  every slate reds on the crate's arrival), and `repark-ta` — the verbatim-ported kernel root's `TaError`
  contract + flat re-export surface).
  Dual-wired: `make check-lib-rs` (in `make ci`) AND a ci.yml
  `guards`-job step; pre-commit via `install-hooks` and `.pre-commit-config.yaml`
  (`lib-rs-guard`). Pure text — sub-second. EXCEPTIONS reason
  strings stay ≤100 cols (ruff E501; keep ruff-format clean).

- `run_census.sh` — one-command census gate (classic + expand + expand2): provisions a scratch
  venv, builds the native module, then runs the three cohorts and writes JSON + markdown reports.
  Not CI-wired (~20 min wall per module). Ported with **one** behavioral change (phase-3 EC-8):
  the classic cohort runs `--classic`, never `--stretch` — `--stretch` appends the C3 modules and
  blends them into the classic /345 denominator. Report output paths are unchanged in shape.
  Second declared change: the run's **environment is recorded, not assumed** — a verbatim
  `pip freeze` (empty = fatal) plus `census-manifest.json` carrying the versions the comparator
  gates (`python_version`, `pyspark_version`, `pandas_version`, `pyarrow_version`), and the run
  aborts outright under pandas ≥ 3. Artifacts are then redacted via `python -m compat.redact`
  (through each format's parser), never `sed`. The
  script needs the facade package at `python/repark`, which arrives with the facade PR; the
  recorded procedure it implements is [../docs/port/census.md](../docs/port/census.md).

- `check_lib_py.sh` + `check_lib_py.py` — the **Python** thinness guard, sibling of
  `check_lib_rs` and the SSOT for facade file size (ported verbatim at phase-3 PR-5 with the
  facade package it inspects). Over every `*.py` under `python/repark/src/repark/**`: a per-file
  line ceiling (default 2500) with an `EXCEPTIONS`-with-reason table in the `.py` (ratchet DOWN
  only; four rows at the pin — `dataframe/core.py`, `functions.py`,
  `ml/feature/_transformers.py`, `session/_funcs.py`), plus the no-stub rule (a module whose body
  is only a docstring + imports/re-exports/`__all__`/`pass` must open its docstring with the
  exact substring `re-export binding`; package `__init__.py` files are exempt from the no-stub
  rule but not from the ceiling). Pure text — sub-second. Wired by the orchestrator into
  `make check-lib-py` and the ci.yml `python` job in the same PR.

Not ported yet (return with their phase — see [../docs/port/PLAN.md](../docs/port/PLAN.md)):
`test_lock_gate.sh` (uv lock-gate detector self-test, phase 3),
`generate_excel_fixtures.py` (synthetic .xlsx fixtures, phase 1+).

## I want to...

| I want to... | go to |
|---|---|
| Understand why a commit was blocked on map.md | `check_map_md.sh` |
| Change or inspect the crate tier map | `check_crate_dag.py` (`TIERS` — the SSOT) |
| Raise/lower a lib.rs line ceiling | `check_lib_rs.py` (`EXCEPTIONS` — reason required) |
| Raise/lower a facade `.py` line ceiling | `check_lib_py.py` (`EXCEPTIONS` — reason required, ratchet down only) |
| Validate workflow YAML locally | `make workflows-parse` |
| Install the pre-commit hook | `make install-hooks` |
| Run the Apache-suite census | `bash scripts/run_census.sh` + [../docs/port/census.md](../docs/port/census.md) |

## Pointers

- Up: [../map.md](../map.md)
- Related: [../.pre-commit-config.yaml](../.pre-commit-config.yaml), [../Makefile](../Makefile).

## Debug

| Symptom | First check |
|---|---|
| Guard blocks a commit | Add/update the `map.md` in the directory of the staged code |
| Guard not running | `make install-hooks` (or use the `pre-commit` framework) |
| `crate-dag: layering inversion …` | The named edge points UP a tier. Either the edge is wrong (remove it) or the tier map is wrong — fix `check_crate_dag.py` `TIERS` and say why in the commit |
| `… is not in the tier map` | A new `repark-*` crate was added; classify it in `check_crate_dag.py` `TIERS` |
| `crate-dag inspected zero internal crates/edges` | `cargo metadata` returned nothing internal — wrong manifest path or a broken workspace (a single-crate workspace with zero edges is fine) |
| `lib-rs: … inline #[cfg(test)] mod` | Move the test body to a file-backed module (`src/<name>.rs` + `#[cfg(test)] mod <name>;`) |
| `lib-rs: … lines (ceiling …)` | Extract production code into a named module, or add an `EXCEPTIONS` entry with a reason (ratchet down only) |
| `lib-py: … lines (ceiling …)` | Split the module, or add an `EXCEPTIONS` row in `check_lib_py.py` with a reason (ceilings ratchet down only) |
| `lib-py: … re-export-only module must start its docstring …` | Open the module docstring's FIRST line with the exact substring `re-export binding`, or give the module real content |
| `lib-py: python/repark/src/repark not found` | The guard runs from the repo root and needs the facade package present |
| `workflows-parse` red | Fix the named workflow's YAML — GitHub would never run it as-is |
| `run_census.sh` fails on `python/repark` | The facade package arrives with the facade PR; until then only the port-source side of the procedure is runnable |
| A census cohort's denominator looks blended | `--stretch` was used for the classic cohort; use `--classic` ([../docs/port/census.md](../docs/port/census.md) §2) |
| `run_census.sh` aborts on the environment | Intended: an empty `pip freeze`, a missing gated version, or pandas ≥ 3 all fail the run at provisioning time. A run whose environment is not recorded is not a baseline (design §5 F2) |

First checks: `bash scripts/check_map_md.sh`, `bash scripts/check_crate_dag.sh`,
`bash scripts/check_lib_rs.sh`, `bash scripts/check_lib_py.sh`, `make workflows-parse`. Escalate to: [../map.md#debug](../map.md).
