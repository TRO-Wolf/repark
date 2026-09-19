# Unit ledger — ICE-TT-RESOLVE-1 · Spark Iceberg time-travel resolution on every door

## Round 1 (2026-09-19)

**Date:** 2026-09-19 · **Branch:** `fix/ice-tt-resolve-1` · **Base:** `origin/main` ·
**Model:** muse-spark-1.3-contributor · **Policy:** [../../../AGENTS.md](../../../AGENTS.md).
**Path:** STANDARD. **Rubric:** STANDARD. `risk_tier: standard`.
**Registry:** `ICE-TT-RESOLVE-1` (rating row parity MT-1 area).

**Retires:** this ledger moves to `../completed/` when the unit's last commit lands.

**Owner ruling 2026-09-18 (binds this unit), quoted:**

> 1:1 parity with Spark's Iceberg integration.

**Why one shared resolver.** The 94-cell oracle (round 1 step 1, commit `3413c537`) shows the
divergence class is silent wrong answers: bare `TIMESTAMP AS OF <ms>` read ms where Spark reads
seconds, and the built-ins plus expression forms silently read current where Spark answers or
refuses. One resolver in `repark-core` serves the reader options and both SQL doors so the three
doors cannot drift again.

## PROPOSITION LEDGER — ICE-TT-RESOLVE-1 — 2026-09-19

| Clause | Proposition (checkable) | Proof obligation | Verdict | Evidence |
|---|---|---|---|---|
| C-001 | The 94 time-travel shapes on format versions 2 and 3 are recorded from live PySpark 4.1.2 with Iceberg 1.11.0 and pinned red on main. | `ice_tt_resolve_1_spark_oracle.json` plus `_record_ice_tt_resolve_1_oracle.py`; `test_ice_tt_resolve_1.py` red on main. | **PROVEN** | Step 1 commit `3413c537`: 94 cells (reader `versionAsOf`/`timestampAsOf`, SQL `TIMESTAMP AS OF` / `FOR SYSTEM_TIME AS OF` on both zones, every refusal shape); red on main 130 of 155 offline cells, mostly silent current-snapshot reads. |
| C-002 | One shared resolver serves the reader options: `versionAsOf`/`timestampAsOf` travel as raw strings, integer means epoch seconds, combined pins raise Spark's refusal texts, legacy pins alone keep their behavior. | `resolve_reader_spec` in `crates/repark-core/src/time_travel.rs`; `reader_spec_builtin_pins_refuse_loud`, `reader_spec_legacy_pins_still_resolve`; flipped ms pins in `crates/repark-spark/src/tests/time_travel.rs`. | **PROVEN** | `ReaderTimeTravel` carries the raw built-ins; version+timestamp refuses `INVALID_TIME_TRAVEL_SPEC`, legacy beside a built-in refuses the Spark sunset text, branch beside a built-in refuses `Can't time travel in branch`. Integer timestamps multiply to ms. Rust batteries green (§4). |
| C-003 | Both SQL doors evaluate the `AS OF` expression as a constant in the session zone through the one shared function; column refs and non-determinism refuse with Spark's messages. | `evaluate_sql_timestamp_asof` in `crates/repark-core/src/time_travel/sql_eval.rs`; `sql_timestamp_asof_evaluates_constants_in_session_zone`; door maps. | **PROVEN** | Determinism check plus zone rewrite on the AST, then one DataFusion `SELECT` of the rewritten expression; `INVALID_TIME_TRAVEL_TIMESTAMP_EXPR.INPUT` / `.NON_DETERMINISTIC` and the column refusal pinned by the battery (§4). Both doors re-slice the original token stream so spaced expressions parse. |
| C-004 | The facade forwards the built-ins as raw strings and keeps the legacy pins' loud errors. | `_iceberg_time_travel_opts` in `reader.py`, `_parse_snapshot_id_option` / `_parse_as_of_timestamp_option` in `reader_support.py`, raw kwargs in `session_core.py` and PyO3 `read_iceberg_table`. | **PROVEN** | `test_snapshot_id_option_parses_int_and_range` and `test_version_asof_options_forward_raw_without_engine` in `test_facade_polish.py` green (§4); legacy-only paths byte-identical to main. |
| C-005 | The 94 cells answer green offline on the release native. | `test_ice_tt_resolve_1.py` offline `-n 4`. | **PROVEN** | 155 passed, 1 skipped (the live-only leg) on the rebuilt release native (§4). |
| C-006 | The live tier re-derives the fixture on PySpark 4.1.2 with Iceberg 1.11.0. | `test_live_cells_rederive_the_fixture` under `REPARK_PARITY_LIVE=1`. | **PROVEN** | 1 passed: every shape replays against a live-seeded table and matches value and refusal class and normalized message (§4). The leg forces `TZ=UTC` like the recorder and normalizes table echoes, positions, and SQL echo blocks. |
| C-007 | The IPI-18 boundary holds: legacy pins alone keep their behavior and are in scope only where the fixture shows them. | Legacy-only pins in `test_time_travel.py` and the red-suite cells. | **PROVEN** | `snapshot-id` / `as-of-timestamp` (ms) / `branch` / `tag` alone resolve exactly as on main; combined with a built-in they raise Spark's texts (C-002). No other legacy surface changed. |
| C-008 | A dated FIXED registry row names the before/after, the pins, and the IPI-18 boundary. | Registry row `ICE-TT-RESOLVE-1` in `docs/spark-sql-iceberg-parity.md`. | **PROVEN** | Row `ICE-TT-RESOLVE-1` sits after `V3-COV-6` beside the MT-1 time-travel rows: before (78/94 measured, 130/155 red), after (shared resolver), pins, IPI-18 boundary sentence. |
| C-009 | Every pin that asserted the old ms behavior is flipped and named. | Flipped-pin list in §2. | **PROVEN** | `crates/repark-spark/src/tests/time_travel.rs::time_travel_version_timestamp_branch_tag_and_errors` (ms pins to current, early error to seconds with the `snapshot older than` needle), `test_sql_timestamp_as_of` (ms pins to current, early error to `IllegalArgumentException`), unknown-id/ref and negative-id class flips, the expire helper needle (§2). |
| C-010 | The core module splits under the file ceiling with every public path stable. | `time_travel/sql_text.rs`, `sql_ast.rs`, `sql_eval.rs` plus root re-exports; `cargo test -p repark-core -p repark-spark -p repark-sql time_travel`. | **PROVEN** | 322/263/421/243 lines, all under the 1000 default; `lib.rs` and every `crate::time_travel::` path resolves through root re-exports; all time-travel suites green (§4). |
| C-011 | Lint, format, clippy, and the touched mechanical gates are green. | §4 gates table. | **PROVEN** | `cargo clippy --locked -p repark-core -p repark-spark -p repark-sql --all-targets -- -D warnings -A clippy::disallowed_methods` clean, `cargo fmt --all --check` clean, ruff check and format clean on every touched Python file, `check_rust_file_size`, `check_lib_py`, `check_lib_rs`, `check_crate_dag`, `check_manifest`, `check-map-sync` clean (§4). Comment-ban: zero added code comments (§5). |
| C-012 | The four size-baseline amendments and the lib-rs row are visible diffs for review. | Amended rows in `scripts/check_rust_file_size.py`, `scripts/check_lib_py.py`, `scripts/check_lib_rs.py`. | **PROVEN** | `repark-python/src/session.rs` 1127 → 1135 (mandated PyO3 params), `repark-sql/src/guards/tests.rs` 1207 → 1213 and `repark-sql/src/tests.rs` 1520 → 1530 (4-arg `EngineContext::new` plumbing), `session_core.py` 2290 → 2297 (raw-string pass-through), new `repark-core` 154 lib-rs row (shared-resolver re-exports). Flagged in the hand-back for review. |

VERDICT: 12 clauses, 12 PROVEN, 0 OPEN, 0 REJECTED.

## 1. Measured on the release native in `.venv` (verbatim)

1a. Oracle file offline (`-n 4`):

```text
$ .venv/bin/python -m pytest python/repark/tests/test_ice_tt_resolve_1.py -q -p no:cacheprovider -n 4
155 passed, 1 skipped in 10.42s
```

1b. Live re-derivation (one JVM under `jvm-lock.sh`, PySpark 4.1.2, Iceberg 1.11.0, Hadoop
catalog):

```text
$ REPARK_PARITY_LIVE=1 ... /tmp/oc-worker/_lib/jvm-lock.sh .venv/bin/python -m pytest python/repark/tests/test_ice_tt_resolve_1.py::test_live_cells_rederive_the_fixture -q -p no:cacheprovider
1 passed, 1 warning in 25.41s
```

1c. Refusal classes (verbatim, release native):

```text
CLASS: repark.errors.IllegalArgumentException
MSG: Cannot find snapshot with ID 999999999
CLASS: repark.errors.IllegalArgumentException
MSG: Cannot find a snapshot older than 2026-09-19T05:00:37+00:00
CLASS: repark.errors.AnalysisException
MSG: [INVALID_TIME_TRAVEL_SPEC] Cannot specify both version and timestamp when time travelling the table. SQLSTATE: 42K0E
CLASS: repark.errors.AnalysisException
MSG: [INVALID_TIME_TRAVEL_TIMESTAMP_EXPR.INPUT] The time travel timestamp expression "not a ts" is invalid. Cannot be casted to the "TIMESTAMP" type. SQLSTATE: 42K0E
CLASS: repark.errors.AnalysisException
MSG: [INVALID_TIME_TRAVEL_TIMESTAMP_EXPR.NON_DETERMINISTIC] The time travel timestamp expression "rand()" is invalid. Must be deterministic. SQLSTATE: 42K0E
```

The controls are the legacy-only pins, which resolve exactly as on main (C-007); nothing
answered silently; no defect to stop on.

## 2. Flipped pins (the old ms behavior)

The ms-asserting pins the seconds change flips, each with its new expectation:

- `crates/repark-spark/src/tests/time_travel.rs::time_travel_version_timestamp_branch_tag_and_errors`:
  bare-ms `TIMESTAMP AS OF` pins (s1, s2, s3, mid-interval) now expect current `[9]`; the
  earlier-than-first pin uses seconds minus an hour and matches `snapshot older than`.
- `python/repark/tests/test_time_travel.py::test_sql_timestamp_as_of`: bare-ms pins now expect
  current; the early pin uses seconds and expects `IllegalArgumentException`.
- `test_unknown_snapshot_and_ref_name_the_pin` and `test_negative_snapshot_id_sql_is_recognized`:
  `AnalysisException` becomes `IllegalArgumentException` (Spark's class for the same text).
- `python/repark/tests/_acceptance.py::require_snapshot_expired` plus its helper tests: the
  expire needle becomes `Cannot find snapshot with ID {id}` under `IllegalArgumentException`.

## 3. Out of scope (observed, not worked)

- IPI-18 (legacy surface beyond the fixture): open by owner direction, boundary in C-007.
- `FOR SYSTEM_VERSION AS OF` and `VERSION AS OF` on the native `repark.sql` door: the native
  door has no `USING iceberg` spelling and its time-travel surface is unchanged by this unit.
- Tier-2 live AWS: never run against unmerged code (policy); the live leg is tier-1 JVM-local.

## 4. Gates

| Command | Result |
|---|---|
| `.venv/bin/python -m pytest python/repark/tests/test_ice_tt_resolve_1.py -q -p no:cacheprovider -n 4` | 0 — 155 passed, 1 skipped |
| `REPARK_PARITY_LIVE=1 ... test_ice_tt_resolve_1.py::test_live_cells_rederive_the_fixture -q -p no:cacheprovider` | 0 — 1 passed |
| `.venv/bin/python -m pytest python/repark/tests/test_time_travel.py python/repark/tests/test_v3e4_refs_time_travel.py python/repark/tests/test_facade_polish.py python/repark/tests/test_production_file_size.py -q -p no:cacheprovider -n 4` | 0 — 77 passed |
| `.venv/bin/python -m pytest (11-file affected set) -q -p no:cacheprovider -n 4` | 0 — 189 passed, 2 skipped |
| `cargo test -p repark-core -p repark-spark -p repark-sql time_travel` | 0 — 2 + 17 + 19 passed, 0 failed |
| `cargo clippy --locked -p repark-core -p repark-spark -p repark-sql --all-targets -- -D warnings -A clippy::disallowed_methods` | 0 — clean |
| `cargo fmt --all --check` | 0 — clean |
| `.venv/bin/ruff check` on every touched Python file | 0 — all checks passed |
| `.venv/bin/ruff format --check` on every touched Python file | 0 — formatted |
| `python3 scripts/check_rust_file_size.py` | 0 — 668 files clean |
| `python3 scripts/check_lib_py.py` | 0 — 817 files clean |
| `python3 scripts/check_lib_rs.py` | 0 — 10 crate roots clean |
| `cargo metadata --format-version 1 --no-deps \| python3 scripts/check_crate_dag.py` | 0 — 22 internal edges clean |
| `python3 scripts/check_manifest.py` | 0 — 18 components agree |
| `python3 scripts/sync_map_md.py --check` | 0 — 302 maps clean |
| `python3 scripts/check_ledger_grammar.py` | 0 — clean with this ledger filed |

Per the brief's machine rule, `make verify`, `make preflight`, `make ci`, the whole
facade suite and the parity harness were deliberately not run (single-clone lane; the
affected-file list plus the targeted gates above is the gate).

## 5. Comment-ban gate (verbatim)

```text
$ git diff -U0 origin/main...HEAD -- '*.rs' | grep "^+" | grep -v "^+++" | grep -E "\+\s*(///|//!|//)"; git diff -U0 origin/main...HEAD -- '*.py' | grep "^+" | grep -v "^+++" | grep -E "\+\s*#"
comment-ban hits=0
```

New-file audit: `crates/repark-core/src/time_travel/sql_text.rs`, `sql_ast.rs`, `sql_eval.rs`
carry zero `//` lines; moved code lost its comments in the move; the `#[allow(...)]`
attributes clippy demands are attributes, not comments.

## Review findings

```yaml
FINDING:
  id: F-ICE-TT-RESOLVE-1-001
  severity: S2
  category: AT-1
  clause: C-003
  claim: The significant-token reconstruction dropped whitespace, so spaced AS OF expressions (CAST x AS TIMESTAMP) could not re-parse at evaluation time.
  evidence: spark pin find_spans_timestamp_expression_forms failed with CAST('2020-06-01 00:00:00'ASTIMESTAMP); the shared evaluate path builds SELECT {expr} from the same tokens
  disposition: REMEDIATED (both doors re-slice the original token stream by the significant-token index range, keeping whitespace; the pin passes and the evaluator receives parseable SQL)
```

```yaml
FINDING:
  id: F-ICE-TT-RESOLVE-1-002
  severity: S1
  category: AT-6
  clause: C-006
  claim: The live leg split cell ids on the first -V, so versioned shapes (TT-DF-VAS-INT-V2) produced shape TT-DF and crashed unbound.
  evidence: UnboundLocalError on action at the first live cell; every DF cell id carries a mid-string -V
  disposition: REMEDIATED (rsplit on the version suffix; the leg re-derives all 94 cells)
```

```yaml
FINDING:
  id: F-ICE-TT-RESOLVE-1-003
  severity: S2
  category: AT-7
  clause: C-006
  claim: The live leg inherited the system zone, so naive committed_at walls shifted the as-of instant before the first snapshot.
  evidence: probe showed mid interior yet Spark refused no snapshot older than mid; the recorder forces TZ=UTC and the worker host runs EDT
  disposition: REMEDIATED (the leg forces TZ=UTC like the recorder before building the engine)
```

```yaml
FINDING:
  id: F-ICE-TT-RESOLVE-1-004
  severity: S1
  category: AT-1
  clause: C-002
  claim: The Rust integration fixture commits sub-second, so no integer second can pin s1 or s2 there.
  evidence: TIMESTAMP AS OF floor(s3) refused (no snapshot older than the floored instant) because s1 committed inside the same second
  disposition: REMEDIATED (the Rust pins assert ms-as-seconds to current plus the seconds-early refusal; genuine seconds pins live in the 94-cell fixture with 2.2s-spaced commits)
```

```yaml
FINDING:
  id: F-ICE-TT-RESOLVE-1-005
  severity: S2
  category: AT-9
  clause: C-012
  claim: The mandated plumbing grows three exact-baseline files and the crate root past their ceilings.
  evidence: check_rust_file_size (session.rs +8, guards/tests.rs +6, tests.rs +10), check_lib_py (session_core.py +7), check_lib_rs (repark-core 150 to 154)
  disposition: ACCEPTED_FLAGGED (four amended rows plus the new lib-rs row are visible diffs with the unit's reason; flagged in the hand-back for review)
```

## Coverage attestation

```yaml
COVERAGE_ATTESTATION:
  pr_unit: ice-tt-resolve-1
  categories:
    - id: AT-1
      status: ATTACKED
      evidence: The 94 cells are measured verbatim on live Spark 4.1.2 in the committed fixture (§1b, C-001) and replay green on the release native (§1a, C-005); refusal classes and Spark texts are asserted verbatim (§1c); the Rust batteries pin the resolver and evaluator units (C-002, C-003).
      artifacts: [python/repark/tests/test_ice_tt_resolve_1.py, python/repark/tests/ice_tt_resolve_1_spark_oracle.json, crates/repark-spark/src/tests/time_travel.rs]
    - id: AT-2
      status: ATTACKED
      evidence: The Spark cells are the verbatim live refusals and rows, recorded by the committed _record_ice_tt_resolve_1_oracle.py driver, which re-derives every cell and exits non-zero on drift (C-001, C-006).
      artifacts: [python/repark/tests/_record_ice_tt_resolve_1_oracle.py]
    - id: AT-3
      status: ATTACKED
      evidence: Answer paths and refusal paths are both pinned: value plus type on the Arrow path for every answering cell, class plus normalized message for every refusal; no silent-answer path remains (C-002 through C-007).
      artifacts: [python/repark/tests/test_ice_tt_resolve_1.py]
    - id: AT-4
      status: ATTACKED
      evidence: No global mutable state added; the temp-view minter stays the one shared counter and the leak pins still pass (C-010).
      artifacts: [crates/repark-spark/src/tests/time_travel.rs]
    - id: AT-5
      status: ATTACKED
      evidence: Memory catalogs under temp dirs; short-lived JVMs under jvm-lock.sh for the live leg only; no network, no credentials (C-006).
      artifacts: [task/ledgers/staging/ice-tt-resolve-1-ledger.md]
    - id: AT-6
      status: ATTACKED
      evidence: Every pinned value is a measured value (§1 verbatim blocks), not prose: snapshot multisets, refusal first lines, SQLSTATE codes (C-005, C-006).
      artifacts: [python/repark/tests/test_ice_tt_resolve_1.py]
    - id: AT-7
      status: ATTACKED
      evidence: Wall-clock claims are explicit and zoned: the fixture commits 2.2s apart, the NY door pins America/New_York resolution, the live leg forces TZ=UTC (C-006, F-ICE-TT-RESOLVE-1-003).
      artifacts: [python/repark/tests/test_ice_tt_resolve_1.py]
    - id: AT-8
      status: ATTACKED
      evidence: git status shows only the unit's resolver, doors, plumbing, pins, registry row, ledger and map entries; no Cargo.toml, lockfile, workflow or pin change.
      artifacts: [task/ledgers/staging/ice-tt-resolve-1-ledger.md]
    - id: AT-9
      status: ATTACKED
      evidence: The declaration lives in its registered home (registry row ICE-TT-RESOLVE-1) and every touched map carries the entry in the same commits (C-008, C-010, C-012).
      artifacts: [docs/spark-sql-iceberg-parity.md]
    - id: AT-10
      status: ATTACKED
      evidence: Round 1 follows the accepted step-1 oracle (commit 3413c537); the ms flips and message changes are listed in §2 with their pins.
      artifacts: [task/ledgers/staging/ice-tt-resolve-1-ledger.md]
  complete: true
```

## Hand-back

`Model: muse-spark-1.3-contributor`. `risk_tier: standard`. Round 1 step 3 files the registry
row and this ledger with all twelve clauses PROVEN, pins green, comment-ban hits=0.
