# Unit ledger — P3E: `python/repark` (the facade wheel + its suite) (PR-5)

**Unit:** phase-3 PR-5 · **Brief:**
[../briefs/phase-3-python-facade.md](../briefs/phase-3-python-facade.md) §1 "PR-5" · **Design:**
[../docs/design/python-facade.md](../docs/design/python-facade.md) §2.3 (the Python tree + the
load-bearing ruff ignores), §3 EC-4 / EC-7 / EC-9, §6.3 (the full-extras facade cohort), §6.5
(reconciliation), §8 (testing discipline), §9 PR-5 · **Port-Source:** the private v1 engine
repository at the frozen port pin `fc3f48102` (read-only worktree) · **Status:** IN FLIGHT —
**carries one BLOCKING finding (B-1) for the panel** · **Depends on:** PR-3 (the binding) and
PR-4 (the parity package + the recorded baseline).

## Scope

The largest PR of the phase, and a fidelity claim rather than a design opportunity. Five
deliverables:

1. **`python/repark` ported verbatim** — `pyproject.toml` (maturin backend, `manifest-path
   ../../crates/repark-python/Cargo.toml`, `module-name repark._native`, `python-source src`,
   `features ["extension-module"]`, version `0.0.0` — the `dynamic = ["version"]` change is a
   release-PR edit per design §4 Q6 and deliberately does not land here), `README.md`, `py.typed`,
   and all **53** source modules byte-identical: the `repark.sql` pyspark-alias package, the r26
   `session/` and `dataframe/` region splits with their `_wire()` loops and frozen import paths,
   and `ml/` (15 modules). **ZERO source edits except the single enumerated EC-9 site.**
2. **The test suite** — **127** files at the pin, **126** ported (one whole file defers), minus
   the empirically generated EC-4 deferral ledger.
3. **uv workspace extension** — the root `pyproject.toml` member list gains `python/repark`, the
   ruff `src` gains the facade src path, and the **three facade per-file-ignore blocks**
   (`ml/**`, `session/**`, `dataframe/**`) land **verbatim** from the port source. `uv.lock`
   regenerated and committed.
4. **`check_lib_py` returns** — `scripts/check_lib_py.sh` + `.py` ported byte-identical (verified
   `diff` empty against the pin). Makefile / ci.yml wiring is an orchestrator carve-out.
5. **The real-artifact obligation discharged** — the wheel is built and the whole suite runs
   against it in a bare interpreter outside the workspace.

Out of scope and deliberately absent: `.github/wheels.yml`, the Makefile targets, `AGENTS.md` /
`CLAUDE.md` — all orchestrator carve-outs landing after these commits. `task/todo.md` untouched.
No AWS call was made; no `REPARK_*` / `TABLE_BUCKET_ARN` / `AWS_*` variable was set at any point.

## The two-commit plan (the declared intra-PR stall fallback, design §9 PR-5)

Design §9 PR-5 pre-declares the split: *"split **within the PR** into a package commit and an
immediately following test commit — never across a merge boundary."* Executed exactly:

| commit | contents | why the boundary is safe |
|---|---|---|
| 1 — package | `python/repark/{pyproject.toml,README.md,map.md,src/**}`, root `pyproject.toml` + `uv.lock`, `python/map.md`, `scripts/check_lib_py.{sh,py}` + `scripts/map.md` | The package ships **no new behavior of its own** — it is a facade over the binding that PR-3 already landed with its own tests. Nothing here is reachable from an installed artifact until commit 2's suite exists, and `make ci` / `make test` are green at this commit. |
| 2 — tests | `python/repark/tests/**`, `task/port/deferred-python-tests.txt`, `task/port/deferred-tests.md` + `task/port/map.md`, `python/repark-parity/tests/test_deferred_ledger.py` + its `map.md`, this ledger, `task/map.md` | The branch is never left with behavior and no tests **beyond this boundary** — the two commits are pushed together and reviewed as one PR. |

## EC-9 — the public-hygiene scrub (declared, outcome-neutral)

This repository is public. Every literal below was found by the standing forbidden-content grep
over the ported tree **plus** the three files the design/panel record names. Each is replaced by a
synthetic equivalent that keeps every assertion's semantics: namespace and bucket names are
arbitrary to the in-memory catalog and to the pure string builders, no gated test changes its skip
condition, and **no node id changes**. Old literals are written here in redacted form
(`<private-*>`) on purpose — spelling them out would red this repository's own hygiene grep, which
is exactly the forcing function EC-9 relies on.

| # | file:line (pre-scrub) | old → new | why outcome-neutral |
|---|---|---|---|
| 1 | `python/repark/tests/_acceptance.py:8` | `` ``<private-org-repo>/spark/scripts/process_silver.py`` `` → `` `process_silver.py` `` | Module docstring prose. Not read by any assertion; the (non-identifying) script name is kept so every other `process_silver` reference in the suite still reads coherently. |
| 2 | `python/repark/tests/_acceptance.py:21` | `BRONZE_BUCKET = "<private-bronze-bucket>"` → `"example-bronze-bucket-v1"` | A pure string constant consumed only by `bronze_path()`, which concatenates it. The one assertion over it (site 5) is updated in lockstep, so the `s3a://` scheme pin and the path shape pin are unchanged. |
| 3 | `python/repark/tests/_acceptance.py:28` | `GLUE_WAREHOUSE = "s3://<private-warehouse-bucket>/"` → `"s3://example-warehouse/"` | Same: a pure constant. The **trailing slash is preserved**, which is what `acceptance_namespace_location()`'s double-separator pin actually tests. No AWS is contacted by any always-run test. |
| 4 | `python/repark/tests/_acceptance.py:39` | `PRODUCTION_NAMESPACE = "<private-production-namespace>"` → `"example_silver"` | The constant exists "solely to assert we never touch it"; its only use is `assert ACCEPTANCE_NAMESPACE != PRODUCTION_NAMESPACE` (`test_acceptance_helpers.py:135`), which holds identically for any value other than `testing_repark_acceptance`. |
| 5 | `python/repark/tests/test_acceptance_helpers.py:37, 58, 75` | the three expected strings derived from sites 2 and 3 | These are the *oracles* for the constants above; changing them in lockstep is what keeps the tests passing for the same reason they passed before. All three still assert the same structure (scheme, separator handling, the four-key Glue config block). |
| 6 | `python/repark/tests/test_aws_acceptance.py:3` | `` ``<private-org-repo>/spark/scripts/process_silver.py`` `` → `` `process_silver.py` `` | Module docstring prose in an env-gated module. |
| 7 | `python/repark/tests/test_aws_acceptance.py:21` | `` ``<private-production-namespace>`` `` → `` `example_silver` `` | Docstring in the CLEANUP banner. The module-level `skipif` on `REPARK_AWS_ACCEPTANCE` is untouched, so the module still skips identically. |
| 8 | `python/repark/tests/test_catalog_flow.py:18, 32, 78, 113, 159, 196, 197` | `glue_catalog.<private-production-namespace>.*` → `glue_catalog.example_silver.*` | **The one non-gated test file naming a fully-qualified production table.** The catalog is a `register_memory_catalog` in-memory catalog rooted at `tmp_path`; the namespace is created by the fixture's own `CREATE NAMESPACE` and is arbitrary. Every CTAS / MERGE / `tableExists` assertion is over names the test itself created. The catalog *name* (`glue_catalog`) is generic, is not private, and is **kept** — renaming it would touch the `spark.sql.catalog.*` config keys and enlarge the diff for no hygiene gain. |
| 9 | `python/repark/src/repark/session/session_core.py:1337` | docstring example `CREATE NAMESPACE glue_catalog.<private-production-namespace>` → `glue_catalog.example_silver` | **The only edit to any of the 53 source modules.** A `register_memory_catalog` docstring example — not executed, not doctested, not asserted anywhere in the suite (grep-verified). |
| 10 | `python/repark/tests/test_dogfood_gaps.py:3-4` | reference to a private local plan path (`<private-plans-dir>/…-dogfood-report.md`) + an internal brief name → "recorded in the dogfood report for that run" | Module docstring prose. The behavioral pins and their recorded PySpark 4.1.2 oracle values are untouched. |

**Verification.** `grep -rnIiE -f <forbidden-patterns> python/repark/` over the ported tree exits
1 (no matches) both before commit 1 and before commit 2. An independent sweep for `arn:aws:*` and
12-digit account-shaped literals found only the already-synthetic `000000000000` dummy ARN and
ordinary decimal test fixtures.

**Deliberately NOT scrubbed** (recorded so a reviewer does not read the omission as an oversight):
the bare filename `process_silver.py`, which appears in ~25 places across the suite and the
`map.md` files. It is a generic ETL script name, carries no organizational identity, and does not
match any forbidden pattern; the private thing was the **path prefix**, and that is gone.

## EC-4 — the empirical deferral ledger

**Method, per design §3 EC-4** — "generated empirically, never transcribed by file". All 127 test
files were ported first. The candidate files were then run **against the installed wheel** with
the EC-3 refuse-arms in place, and each failing node was adjudicated by *where the exception is
raised*. Verdicts:

| candidate node | verdict | where the exception is raised (evidence) |
|---|---|---|
| `test_excel_reader.py` — all **10** nodes | **DEFER** (whole file) | `repark.errors.UnsupportedOperationException: spark.read.excel (read_excel) is not available in this build …` / `spark.read.sheet_names (excel_sheet_names) …` — the binding refuse-arms (EC-3). 10/10 nodes, so the whole file defers. |
| `test_pg_catalog.py::test_postgres_catalog_config_redacts_in_engine_errors` | **DEFER** | `UnsupportedOperationException: postgres catalog 'pg' registration is not available in the phase-1 engine core — it returns with the postgres connector crate (the spec parsed; nothing was registered)`, raised inside `_native.PyReparkSession(...)` at `session_core.py:2263` — repark-core's `CatalogKind::Postgres` `NotImplemented` registration. The test never reaches its `repr()` assertion. |
| `test_pg_catalog.py::test_postgres_catalog_requires_url_at_build` | **PORT (passes)** | Refused at spec **parse**, before registration. The design record anticipated "the pg catalog-config registration tests" plural; empirically only one defers — deferring the file would have withheld a green test. |
| `test_pg_catalog.py::test_live_registered_catalog_schema_table_or_skip` | **PORT (skips)** | `REPARK_PG_DSN` unset → skip-loud. A skip is an outcome, not a deferral. |
| `test_pg_jdbc_options.py::test_jdbc_num_partitions_above_cap_is_unsupported` | **DEFER** | The `read_postgres` refuse-arm answers first, so the message is the refusal, not the engine's cap error: `AssertionError: Regex pattern did not match. Expected regex: '16'. Actual message: 'spark.read.jdbc (read_postgres) is not available in this build …'`. The pinned cap value can never be produced while the arm is armed. |
| `test_pg_jdbc_options.py` — the other **10** nodes | **PORT (pass)** | Each raises `IllegalArgumentException` **facade-side** (`dbtable` missing, empty `predicates`, XOR violations, format aliases) before any native reader is reached — the judges' verified prior, confirmed. |
| `test_pg_acceptance.py`, `test_aws_acceptance.py` | **PORT (skip)** | Env-gated; skip for the recorded reason with every gate variable unset. |

**Result: 12 deferred node ids.** Allowlist:
[port/deferred-python-tests.txt](port/deferred-python-tests.txt); prose:
[port/deferred-tests.md](port/deferred-tests.md) "Python — the facade suite". EC-4 requires the
two to be one record, so `python/repark-parity/tests/test_deferred_ledger.py` (7 tests) pins:
the ledger path is the one the comparator's documented invocation names; it parses through the
comparator's own `load_ledger`; every id is a **pin-collected** name (under-subtraction guard);
every id is **absent from the ported tree** (over-subtraction guard, checked statically via `ast`
so no wheel is needed); and the prose half names every machine-readable id.

## Census — collect-only identity (design §6.5, the PR's dedicated census lens)

Both sides generated, never hand-counted. The oracle is the committed
`task/census/baseline-fc3f48102/facade/collected.txt`.

```
# identical invocation, both sides
python -m pytest python/repark/tests --collect-only -q

v1 pin (recorded) : 2509 node ids
this PR, pre-excision : 2509 node ids   → sorted diff EMPTY
this PR, as committed : 2497 node ids
```

The **pre-excision** run is the load-bearing measurement: with all 127 files ported the two
collections are byte-flat, which proves the port itself moved no name. The committed diff is then
**exactly the 12 deferred ids, baseline side only**:

```
670,679d669
< tests/test_excel_reader.py::test_excel_basic_types_header_and_values
< tests/test_excel_reader.py::test_excel_dates_serial_1900_trap
< tests/test_excel_reader.py::test_excel_default_sheet_is_first
< tests/test_excel_reader.py::test_excel_empty_sheet_zero_rows
< tests/test_excel_reader.py::test_excel_formulas_cached_values
< tests/test_excel_reader.py::test_excel_missing_sheet_loud
< tests/test_excel_reader.py::test_excel_no_header_c_names
< tests/test_excel_reader.py::test_excel_sheet_names
< tests/test_excel_reader.py::test_excel_sheet_name_select
< tests/test_excel_reader.py::test_excel_skip_rows
1556d1545
< tests/test_pg_catalog.py::test_postgres_catalog_config_redacts_in_engine_errors
1565d1553
< tests/test_pg_jdbc_options.py::test_jdbc_num_partitions_above_cap_is_unsupported
```

(2,497 ported ∪ 12 deferred) = 2,509 — disjoint by construction, no id on both sides, none on
neither.

## The full run — predicted before it was executed

Design §6.4's attribution rule requires the expected delta to be stated *before* measuring, so the
prediction is recorded here as it was made:

> The 12 deferred ids were all **passing** at the pin (the baseline records zero failures), so the
> passed count must fall by exactly 12 (2,471 → 2,459), the skipped count must not move at all
> (46 → 46), and the JUnit row count must fall by exactly 12 (2,517 → 2,505 — the 8-row excess
> over the collection is the module-level collection-skip records for the pyspark/duckdb-gated
> modules, which are unaffected).

Observed:

| quantity | pin baseline | this PR | delta | attributed? |
|---|---|---|---|---|
| collected | 2,509 | 2,497 | −12 | yes — the deferral list, exactly |
| JUnit rows | 2,517 | 2,505 | −12 | yes — same |
| passed | 2,471 | **2,458** | −13 | 12 deferred **+ 1 unexplained** |
| skipped | 46 | 46 | 0 | — |
| failed | 0 | **1** | +1 | **finding B-1 — see below** |
| errors | 0 | 0 | 0 | — |

Run tail: `1 failed, 2458 passed, 46 skipped, 37 warnings in 73.03s (0:01:13)`.

Every warning in the tail matches the baseline's recorded warning summary (the master-URL notice
and the StorageLevel cosmetic notice, same modules, same counts).

## FINDING B-1 (BLOCKING, reported not fixed) — `datafusion.runtime.memory_limit` is refused at session build

**Node:** `tests/test_t2_sort_memory.py::test_builder_datafusion_memory_limit_alone_applies`
(passes at the pin, fails here). **Not a deferred surface, and NOT deferrable** — nothing about it
touches Excel or Postgres, so entering it in the EC-4 ledger would be a false deferral and a gate
hole. Per the unit's standing instruction, unexplained pass/fail movement is a **stop-and-report**,
not something to fix silently, so it is reported here and the code is left untouched.

**Reproduction** (bare, no gates, wheel-installed):

```
ReparkSession.builder.config("datafusion.runtime.memory_limit", "256M").getOrCreate()
→ repark.errors.IllegalArgumentException: repark config error: invalid DataFusion session config
  'datafusion.runtime.memory_limit' = '256M': Invalid or Unsupported Configuration:
  Config value "runtime" not found on ConfigOptions
  (raised in _native.PyReparkSession(...), python/repark/src/repark/session/session_core.py:2263)
```

**Root cause, confirmed by reading both trees — this is NOT a PR-5 defect and NOT a port defect:**

* `datafusion.runtime.memory_limit` is a **repark-owned pseudo-key**, not a DataFusion
  `ConfigOptions` key. At the pin it never reached `ConfigOptions`: v1's engine stored the builder
  config map unread by DataFusion, and the facade forwarded `datafusion.*` onto the **live**
  session after build (`_apply_builder_datafusion_conf` /
  `_DATAFUSION_RUNTIME_MEMORY_LIMIT_KEY` in `session/_funcs.py`), where the runtime pool re-size
  is handled.
* This repository's `crates/repark-core/src/session.rs::apply_datafusion_config_keys` — **new code
  from the phase-2 P2G R2 fix** (the string "invalid DataFusion session config" exists nowhere in
  the port source) — pushes *every* `datafusion.*` builder key at `SessionConfig::options_mut()`
  and fails loud on an unknown one. That is the right behavior for real DF keys and the wrong
  behavior for the one repark-owned key that shares the prefix.
* Both repositories pin datafusion **54.1.0** (`Cargo.toml` and `Cargo.lock` checked on both
  sides), so this is not a dependency drift.

**Why it surfaced only now:** the facade is the only caller that uses the pseudo-key, and no
facade existed here until this PR. This is precisely the class of defect the census gate exists to
expose, and it is exposed **attributed and named**, not waved through.

**Proposed fix (for the orchestrator to schedule — deliberately NOT applied here, since
`crates/**` is outside this unit's scope):** exclude the repark-owned pseudo-key from
`apply_datafusion_config_keys`'s prefix sweep (it is not a `ConfigOptions` key), and land it with
a repark-core unit test pinning that a builder carrying only that key builds, plus the ported
facade node as the boundary proof. `tests/test_t2_sort_memory.py::test_dual_memory_knobs_refuse_loud`
already passes, so the "one truth" refusal is unaffected either way.

**Consequence for the wheels `smoke` job (PR-5's orchestrator carve-out):** the packaged-wheel
facade run is red on exactly this one node until B-1 is fixed. Arming that job as a required check
before the fix would red `main` on merge.

## The wheel proof — §6.3 clauses, each verified

Built from `python/repark` with the pinned maturin:

```
uvx maturin@1.14.1 build --out /tmp/wheels-pr5
→ repark-0.0.0-cp312-abi3-manylinux_2_39_x86_64.whl   (debug profile, abi3 ≥ 3.12)
```

Installed into a **bare venv created outside the workspace**, by **explicit file path** with the
four extras, plus `pytest`, plus the parity package by path (the bare interpreter is outside the uv
workspace, so `repark_parity` would not resolve implicitly). The bare name `repark` is never
installed from an index — a PyPI name-reservation package exists and would shadow the local wheel.

| §6.3 clause | verified |
|---|---|
| wheel installed **by explicit file path** | yes — `…/repark-0.0.0-cp312-abi3-manylinux_2_39_x86_64.whl[numpy,pandas,polars,ml-ext]` |
| interpreter **outside the workspace** | yes — venv under the session scratchpad, not `.venv` |
| four extras present **by name** | `numpy 2.5.1`, `pandas 3.0.5`, `polars 1.43.2`, ml-ext (`xgboost 3.4.0`, `lightgbm 4.7.0`, `scikit-learn 1.9.0`) |
| `pyspark` **ABSENT** | yes — not in the freeze; the ten `importorskip` sites skip with `could not import 'pyspark'` |
| `duckdb` **ABSENT** | yes — not in the freeze (it is a dev-group dep, never an extra) |
| **no JVM on `PATH`** | yes — a symlink shim mirroring each PATH directory that carried a `java*` entry, minus those entries; `JAVA_HOME` unset; `which -a java javac` returns nothing |
| every gate variable **unset by name** | `REPARK_AWS_ACCEPTANCE REPARK_ACCEPT_DS REPARK_ACCEPT_ENTITY REPARK_ACCEPT_ID_COL REPARK_PARITY_LIVE REPARK_PG_DSN REPARK_PG_SCALE REPARK_PYSPARK_COMPAT REPARK_COMPAT_SCRATCH REPARK_COMPAT_SERIES REPARK_TPCDS_FULL REPARK_ML_FORMAT REPARK_ML_VERSION REPARK_LOG REPARK_WAREHOUSE REPARK_CATALOG_PREFIX TABLE_BUCKET_ARN` — each passed to `env -u`, none present in the ambient environment either |
| **pandas MAJOR 3** | `3.0.5` — matching the recorded baseline environment; a pandas-2 run fails `test_to_pandas_with_nulls_values_and_dtypes` |
| python | `3.12.3` (both sides) |
| both invocations recorded | `--collect-only -q` and `-q --junitxml` |

**The real-artifact obligation** (`task/port/deferred-tests.md`, owed by PR-3, discharged here) is
satisfied structurally: producer and consumer no longer compile together. Import smoke plus 2,458
passing rows crossing the boundary, including the Arrow C-stream export path asserted on value AND
type via `to_arrow` / `collect` (never only `show`).

## EC-7 — map.md regeneration and dead-link repointing

| file | action |
|---|---|
| `python/repark/map.md` | **REGENERATED** truthfully. The pin's copy documented a flat `session.py` / `dataframe.py` layout that the r26 splits replaced and omitted eight modules; it also carried a mangled `Contents` line (`- r26 T1:  and  packages`). Rewritten against the real tree, with the dated §4 Q1 layout note. map.md files carry no test names, so this costs no census cell. |
| `python/repark/src/repark/map.md` | ported; one dead link repointed (`docs/ml-design.md` → `docs/design/python-facade.md` §4 Q3, the PR-2/PR-3 precedent) at two sites |
| `python/repark/src/repark/ml/map.md` | ported; `docs/ml-design.md` ×3 repointed; the dead v1-only campaign brief link repointed to `briefs/phase-3-python-facade.md` |
| `python/repark/src/repark/ml/ext/map.md`, `ml/feature/map.md` | ported; `docs/ml-design.md` repointed; the dead `task/q1-ml-quantile-ledger.md` line dropped (no counterpart here) |
| `python/repark/src/map.md`, `src/repark/sql/map.md`, `session/map.md`, `dataframe/map.md` | ported byte-identical (no dead links) |
| `python/repark/tests/map.md` | ported, plus: the three EC-4 annotations in place, a cohort/deferral note in `Purpose`, and an appended `I want to…` / `Pointers` / `Debug` block — the pin's copy is an append-log that ends mid-stream and carries none of the three sections CLAUDE.md requires |
| `python/map.md`, `scripts/map.md`, `task/port/map.md`, `python/repark-parity/tests/map.md`, `task/map.md` | lockstep updates for the new members, the returned guard, the new allowlist, and the new harness test |

**Rider filed, not silently fixed:** nine ported **source** files still reference `docs/ml-design.md`
in comments and docstrings, one of them (`session/_funcs.py:5029`) inside a runtime f-string. EC-6's
dead-pointer rider was scoped to `crates/repark-ml` and was discharged in PR-3; extending it into
`python/repark/src` would break this unit's byte-identity claim for the sake of a doc pointer. No
test asserts the string (grep-verified). Recorded here for a post-milestone-one doc-pointer sweep.

## Declared edits beyond the verbatim copy — the complete list

A diff line fitting none of these is a defect.

1. The 10 EC-9 hygiene sites above (one of them in `src/`; the rest in `tests/`).
2. The EC-4 excisions: `tests/test_excel_reader.py` deleted whole; one function removed from each
   of `tests/test_pg_catalog.py` and `tests/test_pg_jdbc_options.py`; and — a **consequence, not a
   choice** — the now-unused `UnsupportedOperationException` name dropped from
   `test_pg_jdbc_options.py`'s import (ruff `F401`; the `**/tests/**` per-file-ignore block does
   not ignore `F401`).
3. The EC-7 map.md work above.
4. Root `pyproject.toml`: `python/repark` added to `[tool.uv.workspace] members` and to ruff
   `src`; the three facade per-file-ignore blocks added **verbatim** from the port source; the
   staging comment about the member list (now obsolete) removed and the `dev`-group comment's
   "when the facade lands" tense corrected. `uv.lock` regenerated.
5. `scripts/check_lib_py.{sh,py}`: byte-identical to the pin (`diff` empty).
6. `python/repark-parity/tests/test_deferred_ledger.py`: **new code**, required by EC-4.
7. `task/port/deferred-python-tests.txt`: **new**, generated.

Explicitly **not** done, per design §8 "do not clean up on the way past": no restyling of ported
code, no import reordering, no de-duplication, no ceiling ratchets in `check_lib_py.py`.

## Gate results

| gate | result |
|---|---|
| `make ci` | **exit 0** — incl. `uvx ruff@0.15.22 check .` **All checks passed!** over the ported ~46 KLOC with the three facade ignore blocks in place, `ruff format --check` 237 files clean, `crate-dag: 16 internal edges clean across 9 of 9 mapped crates`, `lib-rs: 9 crate roots clean`, taplo, typos |
| `make test` | **exit 0** — `cargo test --workspace` (never `--all-features`), zero failed, zero ignored |
| `make py-test` | **exit 0** — 135 passed (128 + the 7 new EC-4 harness tests) |
| `make py-lock-check` | **exit 0** — `uv lock --locked`, 29 packages resolved |
| `bash scripts/check_lib_py.sh` | **exit 0** — `lib-py: 53 files clean (ceilings held; no-stub rule held)`. The four ported `EXCEPTIONS` rows all hold against the real files; no ratchet was touched. |
| facade suite vs the wheel | 2,458 passed / 46 skipped / **1 failed** — the failure is finding B-1 |
| hygiene grep (both passes) | **zero hits** |
| pre-commit hook | green on both commits |

**Ruff surprise, reported rather than restyled:** none. Ruff was clean over the entire ported tree
on the first run with the three per-file-ignore blocks — which is the evidence that those blocks
are load-bearing rather than decorative (design §2.3). The one format finding in the whole PR was
in `test_deferred_ledger.py`, the only new file, and was fixed by `ruff format` before commit.

## What the panel must scrutinize

1. **Finding B-1** — the one unattributed-then-attributed movement. It is an engine defect, not a
   port defect; it is reported, root-caused, and NOT fixed here. The panel decides whether the fix
   rides a fixer commit on this branch (with a repark-core test) or its own PR, and whether the
   wheels `smoke` job is armed before or after.
2. **The EC-9 table** — every site, and the two deliberate non-scrubs (the generic
   `process_silver.py` filename; the generic `glue_catalog` catalog name).
3. **The EC-4 adjudication** — specifically that the pg **catalog-config** deferral is one node
   and not the file the design record's plural phrasing implied, and that eleven offline JDBC pins
   port green because they refuse facade-side.
4. **The `python/repark/map.md` regeneration** — it is the one file here that is authored rather
   than copied, and EC-7 makes that a requirement rather than a liberty.
5. **The two-commit boundary** — commit 1 carries the package and commit 2 the suite; both are on
   the branch together and neither is a merge boundary.

## Note on what lands after these commits (orchestrator carve-outs)

`.github/workflows/wheels.yml` (the `smoke` job → required with **no** paths filter, and the
tag-only `release-wheels` job with no rust-cache step), the `check_lib_py` step in the ci.yml
`python` job, and the Makefile targets (`check-lib-py`, `py-test-facade`, `build-wheel`,
`develop`). Branch protection gains the `smoke` context in the same change. See design §7.2/§7.5 —
and finding B-1's consequence for the arming order.

## Orchestrator pass: B-1 FIXED, B-2 scrubbed, carve-outs landed

- **B-1 (census-gate catch) FIXED in repark-core**, not deferred and not waved: phase-2's
  `apply_datafusion_config_keys` sweep now excludes the port source's facade-owned pseudo-key
  `datafusion.runtime.memory_limit` via an EXACT-KEY const (`REPARK_OWNED_DATAFUSION_PSEUDO_KEYS`)
  — never a prefix, so a typo of the pseudo-key still fails loud (both directions pinned:
  `builder_pseudo_key_datafusion_runtime_memory_limit_builds` +
  `builder_pseudo_key_typo_still_fails_loud`; repark-core 90 passed). Boundary proof through the
  REBUILT wheel in a §6.3-conforming venv: the red node
  `test_builder_datafusion_memory_limit_alone_applies` passes, and the full suite foots exactly —
  **2,459 passed + 46 skipped (= baseline 2,471 − 12 deferred), 2,497 collected (= 2,509 − 12),
  exit 0.** No other movement.
- **B-2 (pre-existing literals on main) scrubbed forward** per the Tier-2 rule (fix content in a
  new commit; never rewrite history): 21 sites across 5 already-merged Rust files — every one the
  same private team/bucket-name fragment (now on the forbidden list; deliberately not spelled
  here) in doc comments / test fixtures (verified: no account number, no
  real ARN — the arn: hits are shape prefixes with the sanctioned fixture account) — replaced
  uniformly with `example-team`; fixtures and oracles changed together; 690 affected-crate tests
  green. The forbidden-pattern list gained the fragment (2026-08-08), which is what surfaced these.
- **Carve-outs**: `wheels.yml` ported with two declared changes (PR trigger UN-path-filtered —
  required-check rule; wheel installs by EXPLICIT file path — the PyPI name-reservation
  shadowing found at the baseline runs, which v1's `--find-links … repark` form is exposed to);
  ci.yml python job gains the `check_lib_py` step; Makefile gains `check-lib-py` / `develop` /
  `py-test-facade` / `build-wheel` and `install-hooks` wires `check_lib_py.sh`.

## Fixer pass (2026-08-08) — confirmed panel findings, dispositions

One commit. Every fix below was reproduced first, fixed minimally, and re-proved. Baseline
artifacts under `task/census/baseline-fc3f48102/` were **not** touched — they are recorded
evidence.

### FIXED — the deferral ledger did not subtract in `--junit` mode (HIGH, census lens)

`compat/compare_reports.py` keyed JUnit rows as `classname::name`
(`tests.test_excel_reader::test_excel_skip_rows`) while `task/port/deferred-python-tests.txt`
carries collect-only node ids (`tests/test_excel_reader.py::test_excel_skip_rows`). The two id
spaces never met, so the ledger subtracted **nothing** in the only mode the facade cohort can
run in, and the documented acceptance invocation (`docs/port/census.md` §5, §7 item 3) could
never exit 0 — precisely the drift EC-4 forbids ("a ledger that can drift from the gate it feeds
is not a ledger", design §3 EC-4; the comparator's contract is §6.4).

Reproduced at HEAD, ledger unchanged: `deferred_subtracted: 0`,
`deferred_not_present_in_baseline: 12`, `vanished (only in v1): 12`,
`pass: v1=2471 v2=2459 <== DIFFERS`, **exit 1**.

Fix (declared deviation, NEW code — `compare_reports.py` is new in this repository, not a
verbatim port): one new pure function `junit_node_id()`, applied in `compare()` to the deferred
and quarantine lists **once**, only when `junit=True`. The translation runs *collect-only →
JUnit* on purpose: path→dotted is total and injective (strip `.py`, `/`→`.`), whereas
dotted→path is ambiguous — nothing in `tests.test_facade.TestX` says where the module ends and
the class begins, so normalizing inside `load_junit_report` (the other candidate site) would
mis-handle class-based ids. The function is idempotent, so it is safe over a ledger already
written in either form. Measured over the recorded baseline XML: classname dot-count histogram
`{1: 2509, 0: 8}` — zero class-based tests in this cohort, the 8 zero-dot rows are module-level
collection rows and are left untouched.

Re-proved with the two real reports (recorded pin XML vs a fresh HEAD run):
`deferred_subtracted: 12`, `deferred_not_present_in_baseline: 0`,
`pass: v1=2459 v2=2459`, `all_collected: v1=2505 v2=2505`, all five delta directions 0,
**sorted-rendering byte comparison: IDENTICAL — exit 0**.

Tests land with the code (docs/testing.md hard block):

- `test_deferred_ledger.py::test_every_deferred_id_subtracts_through_the_junit_loader` — the
  assertion the panel named as the one that would have caught this: every ledger id must resolve
  to a row of the recorded baseline JUnit XML **through `load_junit_report`**, not through the
  collect-only oracle.
- `test_compare_reports.py::test_junit_node_id_translates_ledger_ids_into_the_junit_id_space` —
  6 parametrized cases: plain, parametrized `[suffix]`, class-based, nested directories, and
  both idempotence forms.
- `test_compare_reports.py::test_junit_deferred_ledger_in_collect_only_form_actually_subtracts` —
  the end-to-end regression through `main()`: a collect-only ledger id must remove the matching
  JUnit row (`deferred_subtracted: 1`, `vanished … 0`, exit 0). It fails on the pre-fix code.

`compat/map.md` and `tests/map.md` updated in the same change (lockstep rule).

### FIXED — `task/pg-integration-report.md` was a tracked test output (MED, fidelity)

`python/repark/tests/test_pg_acceptance.py` rewrites this file on every facade run at a
CWD-relative path, so the suite mutated a **tracked** file — and the new `wheels.yml` smoke job
runs the suite from the repo root, so CI would too (with a live DSN it writes timing content and
dirties the tree). It appeared in no declared edit class and `task/map.md` never listed it, so
the lockstep truth rule was unmet either way.

Disposition: it is a run output, not a record. `git rm --cached` + a `/task/pg-integration-report.md`
entry in `.gitignore` with the reason, + a `task/map.md` Debug note so a reader who sees it
locally knows not to `git add` it. Proved: after a full facade run from the repo root,
`git status --porcelain` shows the file neither modified nor untracked. (The v1 pin tracks it;
carrying a generated artifact forward is a wart the port need not inherit — design §8 forbids
cleaning up ported *code* on the way past, not adopting a generated file as a record.)

### FIXED — `task/port/deferred-tests.md` was stale at HEAD (MED ×2, testing + census lens)

The checked-in reconciliation record — the phase-boundary SSOT, required by
`docs/port/census.md` §7 item 5 — still described the builder's pre-fix run
("2,458 passed … **1 failed**", "the residual open item is … **B-1**") after B-1 was fixed in
`20d1665` on this same branch. Only `p3e-facade-ledger.md` had received the orchestrator
addendum. Both clauses corrected to the measured branch state (**2,459 passed + 46 skipped +
0 failed, exit 0**), with the pre-fix reading kept as parenthetical history rather than deleted,
and the `--junit` comparator verdict added. The arithmetic clause (2,497 ported ∪ 12 deferred =
2,509) was already correct and is unchanged.

### RECORDED, not fixed here — the B-2 literal is already published (MED, hygiene)

The B-2 forward-scrub cleaned the branch tip only. The scrubbed fragment is **already reachable
on the public remote** (`origin` = `git@github.com:TRO-Wolf/repark.git`): `main` and
`origin/main` are the same commit, the earliest carrier is `e3e8131`, and that commit is
contained in `origin/main` plus roughly eight stale merged `origin/phase-2/*` branches. The
Tier-2 rule ("fix content in a new commit; never rewrite history") is what was followed and
remains correct for this branch — but it does not by itself close the exposure, and the ledger
previously recorded no disclosure decision.

This is an **operator decision, not a fixer edit**: the remediation options are (a) accept —
the fragment is a private team/bucket-name string with no credential, no account number and no
real ARN (verified during B-2), so the residual risk is name disclosure only; (b) delete the
stale merged `origin/phase-2/*` branches, which removes the easy-to-find copies but not
`origin/main`'s history; (c) a history rewrite of the public repo, which breaks every existing
clone and is out of scope for any PR. **Recommendation: (a) + (b)**, decided by the user, since
only the user can authorize a remote-branch deletion (CLAUDE.md "Destructive / outward-facing
operations"). Nothing in this commit acts on the remote.

### NOTES FOR THE ORCHESTRATOR — findings in files this fixer must not touch

`.github/` and the `Makefile` are outside the fixer's write set, so these four confirmed
findings are handed over rather than fixed. All were reproduced.

1. **HIGH — `.github/workflows/wheels.yml` line 85: the facade step's `file://` URL is built
   from a RELATIVE wheel path**, so pip hard-errors before installing and the job the design
   makes a required, un-path-filtered check is red on every PR (and would red `main` on merge —
   the §7.2 failure class). `WHEEL=$(ls python/repark/dist/repark-*.whl)` then
   `pip install "repark[pandas] @ file://$WHEEL"` yields netloc `python`:
   `ValueError: non-local file URIs are not supported on this platform`. Reproduced on pip 24.0
   and 26.2.1 (the step upgrades pip first, so it gets the new one). The earlier import-smoke
   step (line 72, bare `pip install "$WHEEL"`) is fine — only the `file://` composition is
   broken, and it is introduced by declared change #2, not inherited. Fix: `file://$PWD/$WHEEL`,
   or drop the scheme entirely (`"./$WHEEL[pandas]"`). **This must land before the `smoke`
   context is made required.**
2. **MED — the required `smoke` job installs `repark[pandas]` only**, so 67 tests that pass in
   the design §6.3 four-extras acceptance cohort never execute in CI: measured full-extras
   2,459 passed / 46 skipped vs CI-shaped 2,392 passed / 97 skipped, the delta being 21
   `test_ml_boost_oracle`, 13 `test_interchange_parity`, 13 `test_polars_differential`, 6
   `test_create_dataframe_materialize`, 5 `test_polars_core`, 3 `test_polars_ns`, 3
   `test_t1_cdf_ingest`, 1 each in `test_session` / `test_t3_ux_polish` /
   `test_write_bench_unit`. §6.3: "a cohort that lets an install decision change its denominator
   is not a gate", and §7.3 declines a separate `facade` job precisely because "the packaged-wheel
   run is the one that must be required". The step's comment ("polars is left out deliberately —
   its facade test self-skips", singular) understates it: three whole polars modules plus 24
   polars-gated cases in six others, and the entire ml-ext delegated-backend surface. CI is
   green today; this is gate strength, not a red build. Fix: install
   `repark[numpy,pandas,polars,ml-ext]`.
3. **MED — the `smoke` required-check transition is documented by JOB ID only.** Branch
   protection matches on the job's **display name**, which here is
   `build + import smoke (debug, host)` — a string that appears in no document in the repo
   (`grep -rn 'build + import smoke' --include='*.md' .` → nothing). `task/lessons.md` records
   this deadlock class twice; a required context that never reports blocks every PR forever.
   The same defect applies to the design's list at `docs/design/python-facade.md:665`, which
   names `rust-lint` / `rust-test` / `guards` (job ids) where the display names are
   `Rust lint (fmt + clippy + check)` / `Rust test (workspace)` / `Repo guards (…)`; only
   `python` coincides.
4. **MED — `check-lib-py` and `py-lock-check` are not wired into `make ci`**, while ci.yml's
   `python` job runs both, so the canonical local gate is green on a change that reds CI. The
   port source's `make ci` includes both. Design §7.5 requires each returning target to be
   dual-wired with the CI step it mirrors, and the Makefile's own new comment already claims the
   dual wiring. Related §7.5 gap (PR-4 scope): `py-audit`, the mirror of the landed
   `pip-audit.yml`, has no Makefile target at all.

### Fixer gate results

| gate | result |
|---|---|
| `make ci` | **exit 0** — ruff check + `ruff format --check` (237 files) clean, crate-dag, lib-rs, taplo, typos |
| `make test` | **exit 0** — `cargo test --workspace`, **1,268 passed / 0 failed / 0 ignored** |
| `make py-test` | **exit 0** — **143 passed** (135 + the 8 new fixer tests) |
| facade suite vs the wheel (§6.3 venv) | **exit 0** — **2,459 passed / 46 skipped / 0 failed** in 74s; tree clean afterwards |
| `--junit` acceptance comparator (pin XML vs fresh run, ledger as the only subtraction input) | **exit 0** — byte-identical, `deferred_subtracted: 12` |
| hygiene pass 1 — forbidden-pattern list vs ADDED lines | **zero hits** (all 15 patterns, this commit's diff and the whole branch) |
| hygiene pass 2 — ADDED-LINES content semantics | clean — no private names, paths, hosts, or account identifiers; every added line is comparator code, test code, ledger prose, or a `.gitignore`/map note |
