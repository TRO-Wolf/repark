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
  tier 2; phase-2 pre-declares tier 3 "spark surface": `repark-functions`, `repark-ta`,
  `repark-spark`, `repark-sql`; mapped crates that have not landed yet are simply not inspected). A new `repark-*`
  crate that is not in `TIERS` fails the guard. Wired into `make check-crate-dag` (in the
  `make ci` chain), `.pre-commit-config.yaml`, and the hook installed by `make install-hooks`.
  **Dual-wired:** the `crate-DAG layering guard` step in the ci.yml `guards` job mirrors the
  Makefile target — change one, change the other.
- `check_lib_rs.sh` + `check_lib_rs.py` — the lib.rs thinness guard. No inline
  `#[cfg(test)] mod {…}` (file-backed only; same-line `#[cfg(test)] mod … {` also fails);
  non-test line ceilings with an EXCEPTIONS-with-reason table in the `.py` (SSOT; ratchet down
  only; empty at phase-1 PR-A). Dual-wired: `make check-lib-rs` (in `make ci`) AND a ci.yml
  `guards`-job step; pre-commit via `install-hooks` and `.pre-commit-config.yaml`
  (`lib-rs-guard`). Pure text — sub-second. EXCEPTIONS reason
  strings stay ≤100 cols (ruff E501; keep ruff-format clean).

Not ported yet (return with their phase — see [../docs/port/PLAN.md](../docs/port/PLAN.md)):
`check_lib_py.sh`/`.py` (Python thinness guard, phase 3), `run_census.sh` (Apache-suite census,
phase 3), `test_lock_gate.sh` (uv lock-gate detector self-test, phase 3),
`generate_excel_fixtures.py` (synthetic .xlsx fixtures, phase 1+).

## I want to...

| I want to... | go to |
|---|---|
| Understand why a commit was blocked on map.md | `check_map_md.sh` |
| Change or inspect the crate tier map | `check_crate_dag.py` (`TIERS` — the SSOT) |
| Raise/lower a lib.rs line ceiling | `check_lib_rs.py` (`EXCEPTIONS` — reason required) |
| Validate workflow YAML locally | `make workflows-parse` |
| Install the pre-commit hook | `make install-hooks` |

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
| `workflows-parse` red | Fix the named workflow's YAML — GitHub would never run it as-is |

First checks: `bash scripts/check_map_md.sh`, `bash scripts/check_crate_dag.sh`,
`bash scripts/check_lib_rs.sh`, `make workflows-parse`. Escalate to: [../map.md#debug](../map.md).
