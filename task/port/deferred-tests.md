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
| tests::temp_view_then_sql_runs_the_spark_function_shim | 2 (Spark door) | spark functions shim (SessionExtension) |
| tests::ctas_end_to_end_through_spark_sql | 2 (Spark door) | CTAS lowering (SQL interception) + functions |
| tests::session_sql_bare_dml_applies_eagerly | 2 (Spark door) | CTAS setup + eager-DML routing |
| tests::create_namespace_with_location_lets_ctas_succeed_on_strict_catalog | 2 (Spark door) | CTAS lowering |
| tests::create_namespace_with_location_stores_both_location_keys | 2 (Spark door) | CTAS end-to-end arm |
| tests::catalog_surface_table_exists_and_temp_views | 2 (Spark door) | CTAS mid-flow |
| tests::config_driven_memory_catalog_registers_and_runs | 2 (Spark door) | CTAS lowering |
| tests::read_postgres_dbtable_query_mutex_is_config | 2+ (postgres/connect) | read_postgres + PostgresReadOptions |
| tests::read_postgres_num_partitions_cap_is_not_implemented | 2+ (postgres/connect) | read_postgres |
| tests::read_postgres_sslmode_require_attempts_tls | 2+ (postgres/connect) | read_postgres TLS path |
| tests::read_excel_basic_fixture_round_trips | 2+ (excel) | repark-excel crate + fixture |
| ta_window::sql_route_single_series_kernels_match_the_kernel | 2 (ta) | ta kernels + SQL route |
| ta_window::sql_route_scalar_param_kernels_match_the_kernel | 2 (ta) | ta kernels + SQL route |
| ta_window::sql_route_multi_series_kernels_match_the_kernel | 2 (ta) | ta kernels + SQL route |
| ta_window::sql_route_parked_four_match_the_kernel | 2 (ta) | ta kernels + SQL route |
| ta_window::sql_route_partition_by_scopes_the_series | 2 (ta) | ta kernels + SQL route |
| ta_window::sql_route_multi_batch_partition_matches_the_kernel | 2 (ta) | ta kernels + SQL route |
| ta_window::sql_route_rejects_a_non_literal_period | 2 (ta) | ta period validation (SQL route) |

Also deferred with the phase-2 statement router (hoist-adjacent, from the v1 SQL crate's
`time_travel` module — the SQL-TEXT half that did not hoist): the token-scan / SQL-rewrite
tests (`detects_spark_and_system_spellings`, `find_spans_*`,
`comments_do_not_false_positive_time_travel`, `system_version_string_ref_span`). They belong to
the v1 repark-sql cone (phase-2), not the session tier, and are listed here as a pointer only.

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
