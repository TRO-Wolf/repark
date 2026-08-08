# Deferred-test manifest — phase-1 cone

## Purpose

The checked-in ledger of every v1 phase-1-cone test **not** ported yet, each with its target
phase. Together with the ported set it makes the census auditable at every phase boundary, not
just at the phase-3 census (design decision:
[../../docs/design/session-api.md](../../docs/design/session-api.md) §7, the deferred-test
manifest graft).

## Reconciliation rule (hard)

At every phase boundary: **(ported ∪ deferred) = the v1 phase-1-cone totals** at the pinned
port-source SHA. The ported side is `cargo test --workspace -- --list` in this repo under the
generated old→new rename map (the four prefix rules — never hand-written); the deferred side is
this file. The union must reconcile exactly — no test may be absent from both sides, none may
appear on both. Zero `#[ignore]`, zero skipped-in-CI: a test is either ported with its name or
listed here with a target phase.

v1 phase-1-cone totals at the pin (from the brief/design census):

| v1 crate | tests |
|---|---|
| repark-core (error seed) | 2 |
| repark-catalog | 50 |
| repark-write | 191 |
| repark-session cone (session 49 + catalog-config 26 + object-store 4, + hoisted repark-sql tests + ta_window) | audited in PR-C |

*(Corrected 2026-08-06 at PR-B: the brief's original 51/192 came from a grep over test
attributes, which counted two doc-comment `#[tokio::test]` mentions — v1
`repark-catalog/src/tests.rs:1783` and `repark-write/src/merge/mod.rs:425`; `cargo test
-- --list` at the pin is ground truth.)*

## Deferred entries

Format per row: `v1 test name (old path) | target phase | reason`. Filled by the PR that defers
the test — PR-B (repark-iceberg) and PR-C (repark-core session-test audit); empty sections mean
"no deferrals recorded yet", not "none exist".

### repark-common (from v1 repark-core)

*(none expected — both tests port in PR-A)*

### repark-iceberg — catalog/ (from v1 repark-catalog)

**Deferred: NONE** (PR-B, 2026-08-06). All 50 port under the generated rename map.

### repark-iceberg — write/ (from v1 repark-write)

**Deferred: NONE** (PR-B, 2026-08-06). All 191 port under the generated rename map
(split `merge/` shape). Zero `#[ignore]`, zero skipped-in-CI.

### repark-core — session (from v1 repark-session + hoisted repark-sql subset)

**PR-C cohort (2026-08-06): 18 deferred** of the 86-test session tier at the pin
(src/tests.rs 49 + catalog_config.rs 26 + object_store_s3.rs 4 + tests/ta_window.rs 7).
v1 name form: `repark_session::tests::<name>` (src/tests.rs) or `ta_window::<name>`
(tests/ta_window.rs). Audit method: per-test static verdict against the phase-1 surface;
uncertain ⇒ deferred — no test guessed green.

| v1 test | Target phase | Blocking surface |
|---|---|---|
| ~~tests::temp_view_then_sql_runs_the_spark_function_shim~~ | **LANDED phase-2 PR-2** (`repark-spark/tests/session_extension.rs`, real Session + `SparkExtension` + `SparkDialect`) | ~~spark functions shim (SessionExtension)~~ |
| ~~tests::ctas_end_to_end_through_spark_sql~~ | **LANDED phase-2 PR-3a** (`repark-spark/tests/ddl_sessions.rs`) | ~~CTAS lowering (SQL interception) + functions~~ |
| ~~tests::session_sql_bare_dml_applies_eagerly~~ | **LANDED phase-2 PR-3b** (`repark-spark/tests/dml_sessions.rs`) | ~~eager-DML routing (PR-3b DML arm)~~ |
| ~~tests::create_namespace_with_location_lets_ctas_succeed_on_strict_catalog~~ | **LANDED phase-2 PR-3a** (`repark-spark/tests/ddl_sessions.rs`) | ~~CTAS lowering~~ |
| ~~tests::create_namespace_with_location_stores_both_location_keys~~ | **LANDED phase-2 PR-3a** (`repark-spark/tests/ddl_sessions.rs`) | ~~CTAS end-to-end arm~~ |
| ~~tests::catalog_surface_table_exists_and_temp_views~~ | **LANDED phase-2 PR-3a** (`repark-spark/tests/ddl_sessions.rs`) | ~~CTAS mid-flow~~ |
| ~~tests::config_driven_memory_catalog_registers_and_runs~~ | **LANDED phase-2 PR-3a** (`repark-spark/tests/ddl_sessions.rs`) | ~~CTAS lowering~~ |
| tests::read_postgres_dbtable_query_mutex_is_config | post-milestone-one (decision 2026-08-07) | read_postgres + PostgresReadOptions |
| tests::read_postgres_num_partitions_cap_is_not_implemented | post-milestone-one (decision 2026-08-07) | read_postgres |
| tests::read_postgres_sslmode_require_attempts_tls | post-milestone-one (decision 2026-08-07) | read_postgres TLS path |
| tests::read_excel_basic_fixture_round_trips | post-milestone-one (decision 2026-08-07) | repark-excel crate + fixture |
| ~~ta_window::sql_route_single_series_kernels_match_the_kernel~~ | **LANDED phase-2 PR-4** (`repark-spark/tests/ta_window.rs`) | ~~ta kernels + SQL route~~ |
| ~~ta_window::sql_route_scalar_param_kernels_match_the_kernel~~ | **LANDED phase-2 PR-4** (`repark-spark/tests/ta_window.rs`) | ~~ta kernels + SQL route~~ |
| ~~ta_window::sql_route_multi_series_kernels_match_the_kernel~~ | **LANDED phase-2 PR-4** (`repark-spark/tests/ta_window.rs`) | ~~ta kernels + SQL route~~ |
| ~~ta_window::sql_route_parked_four_match_the_kernel~~ | **LANDED phase-2 PR-4** (`repark-spark/tests/ta_window.rs`) | ~~ta kernels + SQL route~~ |
| ~~ta_window::sql_route_partition_by_scopes_the_series~~ | **LANDED phase-2 PR-4** (`repark-spark/tests/ta_window.rs`) | ~~ta kernels + SQL route~~ |
| ~~ta_window::sql_route_multi_batch_partition_matches_the_kernel~~ | **LANDED phase-2 PR-4** (`repark-spark/tests/ta_window.rs`) | ~~ta kernels + SQL route~~ |
| ~~ta_window::sql_route_rejects_a_non_literal_period~~ | **LANDED phase-2 PR-4** (`repark-spark/tests/ta_window.rs`) | ~~ta period validation (SQL route)~~ |

*(Re-pointed 2026-08-07 with the phase-2 slate settled: the 7 Spark-door rows carry their
phase-2 PR (2 / 3a / 3b per the blocking surface); the 7 ta rows land with phase-2 PR-4; the
3 read_postgres + 1 read_excel rows move to the explicit post-milestone-one bucket in
[../todo.md](../todo.md) — decision 2026-08-07, brief
[../../briefs/phase-2-sql-doors.md](../../briefs/phase-2-sql-doors.md) §4.)*

~~Also deferred with the phase-2 statement router (hoist-adjacent, from the v1 SQL crate's
`time_travel` module — the SQL-TEXT half that did not hoist): the token-scan / SQL-rewrite
tests (`detects_spark_and_system_spellings`, `find_spans_*`,
`comments_do_not_false_positive_time_travel`, `system_version_string_ref_span`).~~ CLOSED
(phase-2 PR-3b census): these are v1 repark-sql census names; they landed in
`repark-spark/src/time_travel.rs` with the PR-2 spine and are covered by the PR-3b 334-name
empty sorted-diff (they were a pointer only — never session-tier rows).

Row-close note (2026-08-07 — phase-2 PR-2): deferred row #1 landed as
`repark-spark/tests/session_extension.rs::temp_view_then_sql_runs_the_spark_function_shim`
against the real `Session + SparkExtension + SparkDialect`; the repark-spark census itself
stays open until PR-3b (partial; closes PR-3b —
[../p2b-spark-skeleton-ledger.md](../p2b-spark-skeleton-ledger.md)).

Row-close note (2026-08-07 — phase-2 PR-3b): deferred row #3
(`session_sql_bare_dml_applies_eagerly`) landed as
`repark-spark/tests/dml_sessions.rs::session_sql_bare_dml_applies_eagerly` (same real-session
assembly as `ddl_sessions.rs`; memory catalog only — AWS-free; v1 body faithful with
`ReparkSession::new()` → the door-installed builder). With the repark-sql census closed at
PR-3b (334 ported names, empty sorted diff — [../p2d-spark-dml-ledger.md](../p2d-spark-dml-ledger.md)),
the manifest remainder is exactly: 7 ta rows (phase-2 PR-4) + 4 post-milestone-one
postgres/excel rows.

Row-close note (2026-08-07 — phase-2 PR-3a): deferred rows #2, #4, #5, #6, #7 landed together
as `repark-spark/tests/ddl_sessions.rs` (same real-session assembly pattern as
`session_extension.rs`: `SparkExtension` + `SparkDialect`, memory/local catalogs only —
AWS-free; v1 bodies faithful under the prefix map, with `ReparkSession::new()` →
door-installed builder and `repark_catalog::memory_catalog` →
`repark_iceberg::catalog::memory_catalog`). Row #3
(`session_sql_bare_dml_applies_eagerly`) was re-verified against the PR-3a handler set and
STAYS deferred: its CTAS setup is now unblocked, but the bare-`INSERT` eager-DML routing it
pins is the PR-3b DML arm. Session-tier remainder: 1 Spark-door row (#3, PR-3b) + 7 ta rows
(PR-4) + 4 post-milestone-one rows. Ledger:
[../p2c-spark-ddl-ledger.md](../p2c-spark-ddl-ledger.md).

Row-close note (2026-08-08 — phase-2 PR-4): deferred rows #8-#14 (the seven `ta_window::sql_route_*`
cases) landed together as `repark-spark/tests/ta_window.rs`, ported from v1
`crates/repark-session/tests/ta_window.rs` with the file shape kept and two declared edit classes:
the class-2 prefix map (`arrow::` → `datafusion::arrow::` — the door crate has no direct arrow
dev-dep; `repark_session::ReparkSession` → `repark_core::ReparkSession`) and the class-4
deferred-test session adaptation (`ReparkSession::new()` → the door-installed builder,
`with_extension(SparkExtension)` + `with_sql_dialect(SparkDialect)`, three construction sites).
The goldens path needed NO fix: `$CARGO_MANIFEST_DIR/../repark-ta/tests/goldens` resolves in this
workspace exactly as it did in v1 (repark-ta is the same sibling of the door crate). All 7 PASS;
names unchanged. The UDFs reach the session through `SparkExtension`'s composed
`repark_ta::TaExtension` — the PR-2 TA-omission rider, discharged.

**Manifest remainder is now exactly 4 rows**, all post-milestone-one by explicit decision
(2026-08-07, brief §4): the 3 `read_postgres_*` rows + `read_excel_basic_fixture_round_trips`.
Every phase-2 row is closed. Ledger: [../p2e-ta-ledger.md](../p2e-ta-ledger.md).

### Python — the facade suite (from v1 `python/repark/tests`)

**PR-5 cohort (2026-08-08): 12 deferred** of the 2,509 collected facade node ids at the pin.
Machine-readable allowlist (the ONLY subtraction input the census comparator accepts):
[deferred-python-tests.txt](deferred-python-tests.txt). The two files are bound by
`python/repark-parity/tests/test_deferred_ledger.py` (EC-4's "a ledger that can drift from the
gate it feeds is not a ledger").

**Generated empirically, never transcribed by file** (design §3 EC-4): every candidate file was
run against the **built wheel** with the EC-3 refuse-arms in place, and a test defers only if its
failure traces to a deferred surface — the criterion is *where the exception is raised*,
hand-adjudicated per node. Both judge-verified directions were confirmed rather than assumed:

* **Under-deferral direction (the priors held).** Most offline `test_pg_jdbc_options.py` pins
  raise their `IllegalArgumentException` **facade-side**, before any native reader is reached —
  11 of the 13 nodes across the two pg files PASS and port normally.
* **Over-deferral direction (the priors were narrowed by evidence).** The design record
  anticipated "the pg catalog-config registration **tests**" plural; empirically **one** node
  defers. `test_postgres_catalog_requires_url_at_build` refuses at spec parse and passes here, so
  deferring the file would have withheld a green test.
* **Env-gated pg/aws tests SKIP and port normally** — a skip is an outcome, not a deferral.

| v1 node id | Target phase | Blocking surface (where the exception is raised) |
|---|---|---|
| `tests/test_excel_reader.py::test_excel_basic_types_header_and_values` | post-milestone-one | binding `read_excel` refuse-arm (EC-3) |
| `tests/test_excel_reader.py::test_excel_dates_serial_1900_trap` | post-milestone-one | binding `read_excel` refuse-arm (EC-3) |
| `tests/test_excel_reader.py::test_excel_default_sheet_is_first` | post-milestone-one | binding `read_excel` refuse-arm (EC-3) |
| `tests/test_excel_reader.py::test_excel_empty_sheet_zero_rows` | post-milestone-one | binding `read_excel` refuse-arm (EC-3) |
| `tests/test_excel_reader.py::test_excel_formulas_cached_values` | post-milestone-one | binding `read_excel` refuse-arm (EC-3) |
| `tests/test_excel_reader.py::test_excel_missing_sheet_loud` | post-milestone-one | binding `read_excel` refuse-arm (EC-3) — the refusal replaces the "sheet not found" message the test pins |
| `tests/test_excel_reader.py::test_excel_no_header_c_names` | post-milestone-one | binding `read_excel` refuse-arm (EC-3) |
| `tests/test_excel_reader.py::test_excel_sheet_names` | post-milestone-one | binding `excel_sheet_names` refuse-arm (EC-3) |
| `tests/test_excel_reader.py::test_excel_sheet_name_select` | post-milestone-one | binding `read_excel` refuse-arm (EC-3) |
| `tests/test_excel_reader.py::test_excel_skip_rows` | post-milestone-one | binding `read_excel` refuse-arm (EC-3) |
| `tests/test_pg_catalog.py::test_postgres_catalog_config_redacts_in_engine_errors` | post-milestone-one | repark-core `CatalogKind::Postgres` `NotImplemented` registration — the session is refused at native construction |
| `tests/test_pg_jdbc_options.py::test_jdbc_num_partitions_above_cap_is_unsupported` | post-milestone-one | binding `read_postgres` refuse-arm (EC-3) pre-empts the engine's `num_partitions` cap error, so the pinned cap value is never produced |

Every test in `test_excel_reader.py` defers, so the **whole file** is withheld (the file-level
rule applies only when it is true of every test in it — verified, 10/10). The two pg rows are
**node-level** excisions inside otherwise-ported files.

These 12 join the four existing Rust rows (3 `read_postgres_*` + 1 `read_excel`) under one
reconciliation rule and one post-milestone-one bucket.

## Deferred testing-contract obligations (NOT v1 test names)

This section is deliberately **outside** the reconciliation arithmetic above: the rows here are
obligations owed to [../../docs/testing.md](../../docs/testing.md), not v1 tests awaiting a port,
so they are never counted in `(ported ∪ deferred)`. They exist because
[../../CLAUDE.md](../../CLAUDE.md)'s precedence chain puts the testing contract **above** any
design document — a design that waives a contract rule does not discharge it; the waiver has to be
recorded here, with an owner, or it is invisible.

| obligation | owed by | owed to | discharged by | recorded |
|---|---|---|---|---|
| **Real-artifact coverage for `crates/repark-python`** — docs/testing.md "Boundary changes need a real-artifact test (applies from phase 3)": the whole crate is boundary code (PyO3 seams, Arrow C-stream export, IPC ingest, abi3 surface). PR-3 lands it with **in-process** coverage only: `crates/repark-python/tests/bindings.rs` boots embedded CPython through the `auto-initialize` dev-dep in the SAME build, which is structurally the case that rule exists to exclude ("when producer and consumer compile together … layout, symbol, and lifecycle mismatches are structurally invisible"). No wheel is buildable from PR-3 — `python/repark` does not exist until PR-5 — so the obligation cannot be discharged in PR-3 by construction. | phase-3 PR-3 (`crates/repark-python`, whole crate) | docs/testing.md:114 | phase-3 **PR-5** (the wheel): `docs/design/python-facade.md` §9 PR-5, "The real-artifact rule is discharged here for the first time" — at minimum the built-wheel import smoke, plus a behavior test through the installed wheel for the Arrow C-stream export path | 2026-08-08, phase-3 PR-3 verify panel. Ledger: [../p3c-binding-ledger.md](../p3c-binding-ledger.md) "Findings from the verify panel", F-7 |

**PR-5's acceptance is blocked on this row.** If PR-5 lands without a wheel-crossing test, this
row does not close and the phase-3 retrospective must carry it forward with a named owner.

**DISCHARGED 2026-08-08 — phase-3 PR-5.** The wheel is built by `maturin build` from
`python/repark` and the whole facade suite is executed against it in a **bare interpreter outside
the uv workspace**, installed by explicit file path — producer and consumer no longer compile
together, which is the exact structural gap the rule names. Evidence in
[../p3e-facade-ledger.md](../p3e-facade-ledger.md): the import smoke, the recorded
`(node id → outcome)` multiset over 2,505 JUnit rows, and behavior across the boundary on the
Arrow C-stream export path (`to_arrow` / `collect` value-and-type assertions run inside the
2,459 passing rows, not merely `show`). The obligation closes with **no residual open item**:
the one engine regression this coverage surfaced — finding **B-1**
(`datafusion.runtime.memory_limit` refused at session build, a repark-core defect from the
phase-2 P2G R2 config-plumbing fix) — was FIXED on this branch in commit `20d1665`, and the
suite is green (exit 0). Nothing is carried into the phase-3 retrospective from this row.

## Reconciliation runs

Each phase-1 PR appends a dated entry here: the pinned-SHA v1 `--list` count, this repo's
`--list` count, the deferred count, and the empty-diff confirmation.

- **2026-08-06 — PR-B (repark-iceberg):** v1 `cargo test -p repark-catalog -p repark-write
  -- --list` at pin `fc3f48102e437e2843ded460bc161edb434dac93` = 241 (catalog 50 + write 191);
  this repo's sorted per-package `--list` = 243 (241 `repark_iceberg::*` +
  2 `repark_common::*` from PR-A); diff against the generated rename map: **EMPTY**.
  (ported 241 ∪ deferred 0) = v1 PR-B cone total 241. Evidence:
  [../p1b-repark-iceberg-ledger.md](../p1b-repark-iceberg-ledger.md). PR-B additionally adds
  one NEW fork-pin proof test (not a ported name; outside the census).
- **2026-08-06 — PR-C (repark-core):** v1 session-tier total at pin
  `fc3f48102e437e2843ded460bc161edb434dac93` = 86 (tests.rs 49 + catalog_config 26 +
  object_store_s3 4 + ta_window 7). Ported 68 (tests.rs 38 + catalog_config 26 +
  object_store_s3 4) under the four prefix rules (`tests::*` → `session::tests::*`; inline
  modules keep their paths); deferred 18 (table above); (68 ∪ 18) = 86, disjoint by
  construction. Name-by-name diff of {v1 names − deferred, prefix-rewritten} against this
  repo's `--list` session cone: **EMPTY**. Full-workspace `--list` = **321** = PR-B's 244
  + 68 ported + 2 hoisted time_travel parser pins + 7 NEW seam/gate tests (dialect 2,
  extension 2, aws_gate 3 — additive, outside the ported census). Zero `#[ignore]`.
  Evidence: [../p1c-repark-core-ledger.md](../p1c-repark-core-ledger.md).
- **2026-08-08 — phase-2 PR-4 (repark-ta):** v1 `cargo test -p repark-ta -- --list` at pin
  `fc3f48102e437e2843ded460bc161edb434dac93` = **146**; this repo's = **146**; sorted diff:
  **EMPTY**. The crate carries an optional `datafusion` feature, so a second pass pins the
  feature-gated surface: v1 `--features datafusion` = **178**, this repo's = **180**; delta =
  ADDED 2 / REMOVED 0, both the NEW door-native `TaExtension` tests
  (`extension::tests::ta_extension_register_installs_the_whole_ta_udf_set_bit_exact`,
  `extension::tests::ta_extension_configure_is_the_trait_default_pass_through`) — additive,
  outside the ported census. Plus the 7 `ta_window::sql_route_*` rows ported into
  `repark-spark/tests/ta_window.rs` (names unchanged, all passing). Deferred remainder: 4
  (post-milestone-one). Zero `#[ignore]`. Evidence: [../p2e-ta-ledger.md](../p2e-ta-ledger.md).
- **2026-08-08 — phase-3 PR-5 (python/repark facade suite):** v1 `pytest --collect-only -q` at
  pin `fc3f48102` = **2,509** node ids (the recorded oracle,
  `task/census/baseline-fc3f48102/facade/collected.txt`); this repo's collection against the
  installed wheel = **2,497**; deferred = **12** ([deferred-python-tests.txt](deferred-python-tests.txt)).
  (2,497 ported ∪ 12 deferred) = 2,509 — disjoint by construction and mechanically checked by
  `test_deferred_ledger.py`. Sorted diff of the two collections is **exactly the 12 deferred ids,
  baseline side only** — no id appears on both sides, none on neither. Outcome multiset:
  baseline 2,517 JUnit rows (2,471 passed + 46 skipped); this repo 2,505 (**2,459 passed +
  46 skipped + 0 failed**, exit 0). The 12 deferred ids were all *passing* at the pin, so the
  expected delta is 2,471 → 2,459 — which is what the branch measures. (The builder's first run
  read 2,458 + 1 failed; that one attributed movement was finding B-1,
  `datafusion.runtime.memory_limit` — a repark-core regression from the phase-2 P2G R2
  config-plumbing fix, NOT a deferred surface and NOT deferrable — and it was fixed in commit
  `20d1665` on this branch, closing the movement.) Skip count unchanged (46 → 46). The
  `--junit` comparator run over these two reports, with this ledger as its only subtraction
  input, is byte-identical and exits 0. Zero `#[ignore]`, zero `--skip`, zero commented-out
  tests. Evidence: [../p3e-facade-ledger.md](../p3e-facade-ledger.md).
