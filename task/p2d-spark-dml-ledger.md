# Unit ledger — P2D: repark-spark DML + refs (the PR-3b completion)

**Unit:** phase-2 PR-3b · **Brief:**
[../briefs/phase-2-sql-doors.md](../briefs/phase-2-sql-doors.md) §1 "PR-3b" · **Design:**
[../docs/design/sql-doors.md](../docs/design/sql-doors.md) · **Port-Source:** v1 `main` @
`fc3f48102` · **Status:** IN FLIGHT · **Stacked on:** phase-2 PR-3a
([p2c-spark-ddl-ledger.md](p2c-spark-ddl-ledger.md))

## Scope

Complete the Spark door: the four remaining v1 handler modules, the last TEMPORARY refuse
arms retired, the lib-root battery, and the census close:

- Port from v1 `repark-sql`, verbatim under the prefix map: `merge.rs`, `insert_overwrite.rs`,
  `ref_ddl.rs`, `call.rs` (with their in-module batteries: 10 + 2 + 14 + 3).
- Restore `normalize.rs`'s MERGE star rewrite (the p2b/p2c declared-TEMPORARY comment goes
  live; discharges the MERGE share of p2b rider WS1-#8) and the r25 T2 write-to-branch sniff
  in `execute_with_read_only` (discharges p2b rider WS1-#5 — the p2c declared omission).
- Router: the four remaining refuse arms (MERGE, INSERT OVERWRITE, CALL, ref-DDL — statement
  arms, the pre-parse ref-DDL refuse, and the fallthrough messages) replaced by v1's arms
  VERBATIM; their 4 refuse tests deleted in the same commit; `refuse_pending` +
  `starts_with_keywords` (both PR-2-native scaffolding) deleted with them. The router now
  matches v1's `execute` family end-to-end (modulo the declared P11/postgres message sites).
- **MoR valve hoist (declared rename):** the BUG-001 valve lived in v1
  `repark-sql/src/normalize.rs`. Its catalog-handle predicate + refuse message hoist to
  `repark_iceberg::write::position_delete::refuse_mor_unpartitioned_multi_spec_dml` (+
  `MorDmlKind`), beside the position-delete path whose fork hazard it gates; the Spark door
  keeps the `ObjectName`-resolution wrapper (same name, same signature) and calls the hoisted
  fn. Its tests are the lib-root `bug001_*` set (6) — they ride in this PR's battery commit
  and pin the split end-to-end.
- Lib-root battery: v1 `src/tests.rs` ports INTACT as ONE move-only identity unit (cp + the
  four-rule sed; **zero in-file edits** — the sanctioned exclusion set turned out to live
  entirely in the separate `postgres_p11_tests.rs` module, which does not port).
- Deferred row #3 lands (`tests/dml_sessions.rs`); manifest reconciled to 7 ta + 4
  post-milestone-one rows; the time-travel pointer note closed.

Out of scope: `repark-ta` (PR-4), the ANSI door (PR-5/6), carve-outs, AWS-touching tests
(E-2: memory/local catalogs only; acceptance env vars NEVER set).

## Edit classes (declared, bounded — p2b 1/2/6 and p2c 2b/3/4 inherited)

1. **Verbatim copy** — the four handler modules + the normalize/router restorations,
   byte-faithful to the pin under the sed map.
2. **Prefix renames (mechanical)** — `repark_sql::` → `repark_spark::`; `repark_write::` →
   `repark_iceberg::write::`; `repark_catalog::` → `repark_iceberg::catalog::`;
   `repark_core::` → `repark_common::`; `repark_session::` → `repark_core::`.
   **2b** — v1 in-crate `crate::{CatalogRegistry, LocationPolicy}` →
   `repark_core::{…}` (+ `crate::time_travel::parse_timestamp_to_ms` →
   `repark_core::parse_timestamp_to_ms`, the phase-1 hoist target); rustfmt rewrap accepted.
   **2c (E-4 inherited)** — `LocationPolicy::TempFallbackAllowed` gained `{ root }` in phase
   1; `call.rs`'s one match arm becomes `TempFallbackAllowed { .. }` (commented at site).
3. **Refuse-arm restoration** — v1 arm VERBATIM in, refuse test deleted same-commit
   (the p2c class; the last four arms).
4. **Deferred-test session adaptation** (`dml_sessions.rs` only) — v1 `ReparkSession::new()`
   → the door-installed builder; body otherwise v1-faithful.
5. **Declared-rename hoist** (MoR valve) — the predicate half moves to repark-iceberg; the
   door keeps the resolution wrapper; `write/mod.rs` re-exports the two names.
6. **Battery scope reconstruction** (lib.rs only) — `#[cfg(test)]` root `use` lines +
   the v1 `#[cfg(test)]` re-export groups (describe_show 4, insert_overwrite 2) so the
   battery's `use super::*` sees v1's crate-root scope; `chrono`/`futures` as
   dev-dependencies (v1 had them as regular deps; only the battery consumes them here).
7. **Door-native test repoint** — `dialect_surfaces_router_refusals` repointed from the MERGE
   refuse arm (now live) to the permanent TRUNCATE targeted refuse (C4-L-001).

No other edit class is authorized; anything else is a STOP.

## Exclusions (by name — the ONLY unported repark-sql census names)

`postgres_p11_tests.rs` (module does not port; post-milestone-one bucket, brief §4):
`ctas_into_postgres_catalog_fails_with_p11`,
`delete_and_update_postgres_catalog_fail_with_p11`,
`insert_into_postgres_catalog_fails_with_p11`, `merge_into_postgres_catalog_fails_with_p11`,
`merge_using_postgres_source_is_not_rejected_as_unknown_pg_catalog`,
`postgres_read_only_message_contains_direction_note`.
Cross-check: `grep -i "postgres\|excel"` over v1 `tests.rs` + the four handler modules = 0
hits — no other census name has a postgres/excel subject.

Relocated (not excluded — phase-1 declared rename, verified present in repark-core `--list`):
`time_travel::tests::parse_timestamp_ms_and_strings`,
`time_travel::tests::parse_version_integer_and_ref`.

## Census reconciliation (CLOSED at PR-3b)

| Bucket | Count |
|---|---|
| v1 `repark-sql` census at pin `fc3f48102` | 342 |
| − excluded `postgres_p11_tests` (post-milestone-one) | 6 |
| − relocated to repark-core (phase-1 time-travel hoist) | 2 |
| **= ported into repark-spark lib** | **334** |
| Sorted name-by-name diff (census − exclusions) vs `--list` (minus door-native) | **EMPTY** |
| Door-native NEW tests in lib (outside census): `router::tests` 4 (`truncate_refusal_is_verbatim_v1`, `select_passthrough_still_executes`, `read_only_set_reaches_p11_refusal`, `multi_statement_still_refuses_before_refuse_arms`), `extension::tests` 4, `dialect::tests` 2 | 10 |
| **repark-spark lib `--list` total** | **344** |
| Integration binaries: `ddl_sessions` 5 + `session_extension` 1 (session-census rows, PR-2/3a) + `dml_sessions` 1 (row #3, this PR) | 7 |

Raw proof (both sorted name lists + the exclusion names): `pr3b-census-proof.txt`, kept with
the phase-2 recon artifacts outside the repo.

## Restoration checklist vs p2b/p2c declared riders / arms

| Declaration | PR-3b action | Status |
|---|---|---|
| Refuse arm: MERGE (statement + fallthrough) | restored verbatim; refuse test deleted | confirmed (integrator, 2026-08-07) |
| Refuse arm: INSERT OVERWRITE | restored verbatim; refuse test deleted | confirmed (integrator, 2026-08-07) |
| Refuse arm: CALL | restored verbatim; refuse test deleted | confirmed (integrator, 2026-08-07) |
| Refuse arm: ref-DDL (pre-parse + fallthrough) | restored verbatim (v1 ordering: `try_parse_ref_ddl` LAST in preparse); refuse test deleted | confirmed (integrator, 2026-08-07) |
| Rider WS1-#5 (write-to-branch sniff) | DISCHARGED — v1 sniff verbatim in `execute_with_read_only` | confirmed (integrator, 2026-08-07) |
| Rider WS1-#8 (normalize MERGE rewrite) | DISCHARGED — `rewrite_merge_stars` call live | confirmed (integrator, 2026-08-07) |
| Rider WS1-#9 (lib.rs re-export trim) | DISCHARGED — v1 groups complete (incl. the `#[cfg(test)]` sets) | confirmed (integrator, 2026-08-07) |
| Deferred row #3 | LANDED — `tests/dml_sessions.rs`; manifest row struck | confirmed (integrator, 2026-08-07) |
| MoR valve hoist (brief §0 declared rename) | DONE — predicate in `repark_iceberg::write::position_delete`; door wrapper calls it; `bug001_*` (6) ride | confirmed (integrator, 2026-08-07) |

## Gate results (integrator fills)

| Gate | Result | Evidence |
|---|---|---|
| `make ci` per commit | PASS | exit 0 verified at each of the four commit states before landing |
| `make preflight` (PR head) | PASS | exit 0 — zizmor "No findings to report" |
| `cargo test --workspace` (never `--all-features`) | PASS | 737 passed / 0 failed at PR head |
| `cargo test -p repark-spark` | PASS | 344 lib + 5 ddl_sessions + 1 session_extension + 1 dml_sessions |
| Census close: 334-name empty sorted diff | PASS | proof file above; reconciliation table above |
| Refuse tests of restored arms deleted same-commit | PASS | 4 deleted in the handler commit (+ dialect probe repointed) |
| Manifest = 7 ta + 4 post-milestone-one rows | PASS | port/deferred-tests.md (row #3 struck; pointer note closed) |
| Forbidden-literal sweep (diff + commit messages) | PASS | grep over `git diff phase-2/pr-3a..HEAD` + `git log` = 0 hits |
| map.md lockstep (`check_map_md.sh`) | PASS | pre-commit guard green on all four commits |

## Deviations / STOPs

- **tests.rs needed zero in-file edits (2026-08-07):** the brief sanctioned one edit class
  inside the battery (deleting postgres/excel-subject test fns); the census cross-check showed
  all six exclusions live in the separate `postgres_p11_tests.rs` module, so the battery is a
  clean cp+sed identity unit. Recorded as a narrower-than-declared outcome, not a deviation.
- **Class-6 tracing-harness hazard: not triggered (2026-08-07):** the merged binary installs
  no global tracing subscriber from the battery (grep: zero `tracing`/`subscriber` hits in v1
  `tests.rs`); the shared-harness pattern was not needed.
- **Dialect probe repoint (edit class 7, 2026-08-07):** `dialect_surfaces_router_refusals`
  pinned the MERGE refuse arm this PR deletes; repointed to the permanent TRUNCATE refuse.
  Door-native test (outside census), same seam claim.
- **`with_repark_sql_config` sed hazard (2026-08-07):** the prefix rule is applied as
  `repark_sql::` (with `::`) only, so the `repark_functions::cardinality::with_repark_sql_config`
  helper name and `repark.sql.*` config keys are untouched.

## Retrospective

*(filled at unit close, per SEPMO)*
