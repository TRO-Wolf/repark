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

Not ported yet (return with their phase — see [../docs/port/PLAN.md](../docs/port/PLAN.md)):
`check_crate_dag.sh`/`.py` (crate-DAG layering guard, phase 1), `check_lib_rs.sh`/`.py` (lib.rs
thinness guard, phase 1), `check_lib_py.sh`/`.py` (Python thinness guard, phase 3),
`run_census.sh` (Apache-suite census, phase 3), `test_lock_gate.sh` (uv lock-gate detector
self-test, phase 3), `generate_excel_fixtures.py` (synthetic .xlsx fixtures, phase 1+).

## I want to...

| I want to... | go to |
|---|---|
| Understand why a commit was blocked on map.md | `check_map_md.sh` |
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
| `workflows-parse` red | Fix the named workflow's YAML — GitHub would never run it as-is |

First checks: `bash scripts/check_map_md.sh`, `make workflows-parse`. Escalate to:
[../map.md](../map.md).
