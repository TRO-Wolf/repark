# map — scripts/

## Purpose

Repository helper scripts wired into the dev workflow.

## Contents

- `bump_fork_pin.sh` — bumps the iceberg-rust fork `[patch.crates-io]` pin: rewrites all five
  `rev` lines + `Cargo.lock` together (single-writer-per-pin invariant-checked), prints the
  fork changelog URL for the PR body. Wrapped by `make bump-fork-pin REV=<sha|branch>`;
  contract in [../docs/fork-sync.md](../docs/fork-sync.md).
- `check_map_md.sh` — the map.md lockstep guard: fails a commit if a staged `.rs`/`.py`/
  `Cargo.toml`/`pyproject.toml` file's directory has no same-change `map.md` update (lockfiles
  excluded; root-level manifests map to `map.md`, not `./map.md`). Invoked by
  `.pre-commit-config.yaml`, `make check-map-md`, and the hook installed by `make install-hooks`.
- `check_workflows_parse.py` — every GitHub Actions workflow must be parseable YAML. zizmor
  SKIPS files it cannot parse (exits 0 with "no auditable inputs"), so a broken workflow would
  pass the blocking lint gate while GitHub silently never runs it. Wired as a prerequisite of
  `make workflows-lint`.
- `check_crate_dag.sh` + `check_crate_dag.py` — the crate **dependency-policy** guard. The `.sh`
  runs `cargo metadata --format-version 1 --no-deps --locked` and pipes it to the `.py`, which
  holds three tables and is the **SSOT** for all three: the **tier map** (`TIERS`), the crate
  **roles** (`ROLES` — foundation / table service / engine / capability / door / bindings), and
  the explicit **allowed-edge table** (`ALLOWED_EDGES`: every internal edge, the dependency
  KINDS it may take, and why it exists). Prose points here and never restates them. Four rules,
  in order: (1) the declared policy must itself obey the structural rules — a forbidden edge
  cannot be legalized by writing it down; (2) every observed `repark-*` edge must be DECLARED,
  with its kind (`normal` / `optional` / `dev` / `build`) permitted for that pair — a new
  same-tier edge reds until it is declared with a reason, and a stale row whose edge is gone
  reds too; (3) the structural rules over roles — no door → door edge outside `dev`, nothing may
  depend on the bindings adapter, the foundation crate depends on nothing internal, a capability
  crate never depends on a door; (4) layering — no PRODUCT edge (`normal`/`optional`) may point
  at a strictly higher tier, same-tier edges ALLOWED. Kinds are what let the cross-door
  `repark-sql → repark-spark` test edge be permitted as `dev` while the same edge as `normal`
  is the forbidden door→door product edge. Third-party crates are out of scope (internal = any
  Cargo workspace member — membership, not the `repark-` name, is the test); a new workspace
  member missing from `TIERS`/`ROLES` fails the guard; mapped crates that have not landed yet are
  simply not inspected. NOTE the binding's deliberate **non-edges** (no `repark-sql`, no
  `repark-iceberg`) are still enforced by review, not here — this guard bans edges, it never
  requires one. Wired into `make check-crate-dag` (in the `make ci` chain),
  `.pre-commit-config.yaml`, and the hook installed by `make install-hooks`.
  **Dual-wired:** the `crate-DAG layering guard` step in the ci.yml `guards` job mirrors the
  Makefile target — change one, change the other.

- `check_manifest.sh` + `check_manifest.py` — the **structural-manifest** guard (FD-3), which
  makes [`../repo-manifest.toml`](../repo-manifest.toml) true instead of decorative. Pure text
  (no cargo, no network): it reads `Cargo.toml`, the `Makefile`, `STATUS.md`, the declared
  documents and the crate-root `map.md` files. Nine rules — inventory both ways (every Cargo
  member is declared; every `delivered` component is a member at that exact path), delivered
  components exist (`<path>/Cargo.toml`), `planned` paths must NOT exist (planned ≠ delivered),
  layers are recognized **and equal the tier name `check_crate_dag.py` assigns** (imported, not
  copied — the manifest mirrors the dependency-policy SSOT and can never override it), every
  delivered crate is covered by `TIERS` + `ROLES`, the `[project.gates]` commands name live
  `make` targets, every `[documentation]` path exists, `STATUS.md` states the manifest's
  phase/release words (`## Current milestone` / `## Release state`), and each delivered
  component's crate-root `map.md` exists at the declared path, names the component in its
  heading and names its layer as `tier N`. That last rule is the **only** `map.md` automation in
  the repository and it **checks** a hand-written file — it never generates, scaffolds or
  rewrites one. Wired into `make check-manifest` (in the `make ci` chain),
  `.pre-commit-config.yaml`, and the hook installed by `make install-hooks`. **Dual-wired:** the
  `repo-manifest guard (check_manifest)` step in the ci.yml `guards` job mirrors the Makefile
  target.
- `check_lib_rs.sh` + `check_lib_rs.py` — the lib.rs thinness guard. No inline
  `#[cfg(test)] mod {…}` (file-backed only; same-line `#[cfg(test)] mod … {` also fails);
  non-test line ceilings with an EXCEPTIONS-with-reason table in the `.py` (SSOT; ratchet down
  only; empty at phase-1 PR-A; rows so far: `repark-functions` — registration glue
  (ceiling 175 after U5 `pub mod ansi;`),
  `repark-python` — the 180-line PyO3 crate root, a MANIFEST (module decls incl. the
  file-backed `exceptions` taxonomy module, the two error folds, the `#[pymodule]`
  registration) that already uses the sanctioned file-backed test module (phase-3 PR-3, EC-10;
  ceiling ratcheted 230 → 190 when the taxonomy moved to src/exceptions.rs — without the row
  every slate reds on the crate's arrival), and `repark-ta` — the verbatim-ported kernel root's `TaError`
  contract + flat re-export surface).
  **Stale EXCEPTIONS keys fail closed** (WC 2026-08-11): a crate-name key whose
  `crates/<key>/src/lib.rs` is missing is an ERROR (G-8 mold; keys are crate names, not
  paths). Dual-wired: `make check-lib-rs` (in `make ci`) AND a ci.yml
  `guards`-job step; pre-commit via `install-hooks` and `.pre-commit-config.yaml`
  (`lib-rs-guard`). Pure text — sub-second. EXCEPTIONS reason
  strings stay ≤100 cols (ruff E501; keep ruff-format clean).

- `run_census.sh` — one-command census gate (classic + expand + expand2): provisions a scratch
  venv, builds the native module, then runs the three cohorts and writes JSON + markdown reports.
  Not CI-wired (~20 min wall per module). Ported with **three** declared changes, each stated in
  the script header:
  1. (phase-3 EC-8) the classic cohort runs `--classic`, never `--stretch` — `--stretch` appends
     the C3 modules and blends them into the classic /345 denominator. Report output paths are
     unchanged in shape.
  2. the run's **environment is recorded, not assumed** — a verbatim `pip freeze` (empty = fatal)
     plus `census-manifest.json` carrying the versions the comparator gates (`python_version`,
     `pyspark_version`, `pandas_version`, `pyarrow_version`), and the run aborts outright under
     pandas ≥ 3.
  3. the markdown reports default to the **gitignored** `target/census-reports/` (not `task/`).
     A run is a run OUTPUT until it is curated, so it must not land look-alike markdown beside
     the committed evidence in `task/census/<run>/`, nor dirty `git status`. Override with
     `CENSUS_REPORT_DIR=…`; promotion to evidence stays a deliberate copy into
     `task/census/<run>/` in the commit that records it.

  Artifacts are then redacted via `python -m compat.redact` (through each format's parser), never
  `sed`. The script needs the facade package at `python/repark`, which arrives with the facade
  PR; the recorded procedure it implements is
  [../docs/port/census.md](../docs/port/census.md).

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

- `check_rust_file_size.sh` + `check_rust_file_size.py` — the **general Rust file-size** guard
  (G-8 companion to `check_lib_rs`). Over every `*.rs` under `crates/**` (recursive): a per-file
  line ceiling with an `EXCEPTIONS`-with-reason table in the `.py` (ratchet DOWN only). Default
  and exception numbers live **only** in the `.py` — prose never restates them. Seeded from the
  post-G-4 measured tree (the former 14.5-KLOC `tests.rs` monolith is gone and is not
  grandfathered). Fail-closed: unreadable file, empty scan set, or a stale `EXCEPTIONS` key
  (path no longer on disk) is an error. Pure text — sub-second. Dual-wired:
  `make check-rust-file-size` (in `make ci`) AND the ci.yml `guards`-job step; also both
  pre-commit paths (`make install-hooks` + `.pre-commit-config.yaml`).

- `check_parity_live_dual_wire.sh` + `check_parity_live_dual_wire.py` — the **parity-live dual-wire**
  guard (G-6). Compares `make parity-live` and `.github/workflows/parity-live.yml` to **each
  other** on load-bearing tokens (`uv sync` flag/extra set, `--no-install-package repark`,
  maturin pin + `develop`, `uv run --locked --no-sync` + pytest path, `REPARK_PARITY_LIVE` /
  `SPARK_LOCAL_IP`). No third hand-maintained expected-flags list. Fail-closed on a parse miss.
  Scope is this one pair only (a one-line extensibility comment lives in the `.py`; there is no
  multi-pair framework). Dual-wired: `make check-parity-live-dual-wire` (in `make ci`) AND the
  ci.yml `guards`-job step.

- `check_matrix_test_liveness.sh` + `check_matrix_test_liveness.py` — the **surface-matrix
  test-name liveness** guard (H-2 G8). Diffs every `Row::Tested { test }` in
  `crates/repark-spark/src/matrix.rs` and `crates/repark-sql/src/matrix.rs` against
  `cargo test --locked --workspace --lib --tests --bins -- --list`. A dead cite reds the
  gate. Fail-closed on a parse miss, a missing matrix file, zero extracted cites, zero
  listed names, or a non-zero cargo exit. Dual-wired: `make check-matrix-test-liveness`
  (in `make ci` / `make preflight`) AND the ci.yml `rust-test` job step (not the guards
  job — the check needs compiled test binaries).

Not re-homed (the port is complete — each returns only with a concrete driver):
`test_lock_gate.sh` (uv lock-gate detector self-test — a lock-gate change that needs it),
`generate_excel_fixtures.py` (synthetic .xlsx fixtures — the deferred `repark-excel` reader; see
[../STATUS.md](../STATUS.md) "Deferred capabilities").

## I want to...

| I want to... | go to |
|---|---|
| Understand why a commit was blocked on map.md | `check_map_md.sh` |
| Change or inspect the crate tier map | `check_crate_dag.py` (`TIERS` — the SSOT) |
| Add / remove an internal crate dependency | `check_crate_dag.py` (`ALLOWED_EDGES` — declare the edge, its kind and a reason) |
| Declare a new crate, doc, or gate command | [`../repo-manifest.toml`](../repo-manifest.toml), then `bash scripts/check_manifest.sh` |
| Raise/lower a lib.rs line ceiling | `check_lib_rs.py` (`EXCEPTIONS` — reason required) |
| Raise/lower a facade `.py` line ceiling | `check_lib_py.py` (`EXCEPTIONS` — reason required, ratchet down only) |
| Raise/lower a general Rust file line ceiling | `check_rust_file_size.py` (`EXCEPTIONS` — reason required, ratchet down only) |
| Validate workflow YAML locally | `make workflows-parse` |
| Check `make parity-live` still matches `parity-live.yml` | `make check-parity-live-dual-wire` |
| Check a matrix.rs Tested cite still exists | `make check-matrix-test-liveness` |
| Install the pre-commit hook | `make install-hooks` |
| Run the Apache-suite census | `bash scripts/run_census.sh` + [../docs/port/census.md](../docs/port/census.md) |

## Pointers

- Up: [../map.md](../map.md)
- Related: [../.pre-commit-config.yaml](../.pre-commit-config.yaml), [../Makefile](../Makefile),
  [../repo-manifest.toml](../repo-manifest.toml) (the structural facts `check_manifest.py`
  validates).

## Debug

| Symptom | First check |
|---|---|
| Guard blocks a commit | Add/update the `map.md` in the directory of the staged code |
| Guard not running | `make install-hooks` (or use the `pre-commit` framework) |
| `crate-dag: layering inversion …` | The named edge points UP a tier. Either the edge is wrong (remove it) or the tier map is wrong — fix `check_crate_dag.py` `TIERS` and say why in the commit |
| `ERROR: undeclared dependency edge …` | A new internal dependency landed without a policy row — declare it in `check_crate_dag.py` `ALLOWED_EDGES` with its kind and a reason, or drop the dependency |
| `ERROR: dependency kind not permitted …` | The edge exists in the policy but not in this kind (the classic case: a `dev`-only cross-door edge promoted to `normal`) |
| `ERROR: forbidden edge …` / `the policy DECLARES a forbidden edge` | A structural rule fired (door→door, anything→bindings, foundation→internal, capability→door). The second form means the *table* was edited to legalize it — the rule fires on the declaration too |
| `ERROR: stale policy row …` | An `ALLOWED_EDGES` row survives an edge that was removed; delete the row |
| `… is not in the tier map` | A new `repark-*` crate was added; classify it in `check_crate_dag.py` `TIERS` (and `ROLES`) |
| `manifest: … is not declared in repo-manifest.toml` | A new Cargo member landed; add its `[components.<name>]` entry (path, layer, status) |
| `manifest: … declared delivered, but …` / `declared planned, but … exists` | The manifest and the tree disagree about what exists; fix whichever is stale — a component is delivered exactly when its code is there |
| `manifest: … layer … disagrees with the dependency-policy SSOT` | `repo-manifest.toml` mirrors `check_crate_dag.py`; change the tier map there, then the mirror |
| `manifest: … map.md never names its layer` | Say the crate's tier in its crate-root `map.md` (hand-written — the guard never writes one) |
| `manifest: … STATUS.md … does not state …` | STATUS.md is the status SSOT; the phase moved in one file and not the other |
| `crate-dag inspected zero internal crates/edges` | `cargo metadata` returned nothing internal — wrong manifest path or a broken workspace (a single-crate workspace with zero edges is fine) |
| `lib-rs: … inline #[cfg(test)] mod` | Move the test body to a file-backed module (`src/<name>.rs` + `#[cfg(test)] mod <name>;`) |
| `lib-rs: … lines (ceiling …)` | Extract production code into a named module, or add an `EXCEPTIONS` entry with a reason (ratchet down only) |
| `lib-py: … lines (ceiling …)` | Split the module, or add an `EXCEPTIONS` row in `check_lib_py.py` with a reason (ceilings ratchet down only) |
| `lib-py: … re-export-only module must start its docstring …` | Open the module docstring's FIRST line with the exact substring `re-export binding`, or give the module real content |
| `lib-py: python/repark/src/repark not found` | The guard runs from the repo root and needs the facade package present |
| `rust-file-size: … lines (ceiling …)` | Split the module, or add an `EXCEPTIONS` row in `check_rust_file_size.py` with a reason (ceilings ratchet down only) |
| `rust-file-size: … scan set is empty` | Fail-closed: the guard found zero `crates/**/*.rs` files — fix the tree or the scan root |
| `rust-file-size: EXCEPTIONS key has no file on disk` | Remove the stale row or restore the path (fail-closed; not a silent skip) |
| `workflows-parse` red | Fix the named workflow's YAML — GitHub would never run it as-is |
| `run_census.sh` fails on `python/repark` | The facade package arrives with the facade PR; until then only the port-source side of the procedure is runnable |
| A census cohort's denominator looks blended | `--stretch` was used for the classic cohort; use `--classic` ([../docs/port/census.md](../docs/port/census.md) §2) |
| `run_census.sh` aborts on the environment | Intended: an empty `pip freeze`, a missing gated version, or pandas ≥ 3 all fail the run at provisioning time. A run whose environment is not recorded is not a baseline (design §5 F2) |
| A census run's markdown reports are "missing" from `task/` | They are not written there: the default `CENSUS_REPORT_DIR` is the gitignored `target/census-reports/` (declared change 3). The final line of the run echoes the directory it wrote |
| `parity-live dual-wire: FAIL` / parse incomplete | A load-bearing flag drifted between `Makefile` `parity-live` and `.github/workflows/parity-live.yml` — change one, change the other. A parse miss is also red (fail-closed); fix the surface or the extractor in `check_parity_live_dual_wire.py` |
| `matrix-test-liveness: FAIL` / dead cite | A `matrix.rs` `Tested` row names a test `cargo test -- --list` does not print — rename the cite with the test, or flip the row to `DeliberatelyAbsent`. A parse miss or cargo non-zero is also red (fail-closed); SSOT: `check_matrix_test_liveness.py` |

First checks: `bash scripts/check_map_md.sh`, `bash scripts/check_crate_dag.sh`,
`bash scripts/check_lib_rs.sh`, `bash scripts/check_lib_py.sh`, `bash scripts/check_manifest.sh`,
`bash scripts/check_parity_live_dual_wire.sh`, `bash scripts/check_matrix_test_liveness.sh`,
`make workflows-parse`. Escalate to:
[../map.md#debug](../map.md).
