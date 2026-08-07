# Unit ledger — P2C: repark-spark DDL (the PR-3a restoration)

**Unit:** phase-2 PR-3a · **Brief:**
[../briefs/phase-2-sql-doors.md](../briefs/phase-2-sql-doors.md) §1 "PR-3a" · **Design:**
[../docs/design/sql-doors.md](../docs/design/sql-doors.md) · **Port-Source:** v1 `main` @
`fc3f48102` · **Status:** IN FLIGHT · **Stacked on:** phase-2 PR-2
([p2b-spark-skeleton-ledger.md](p2b-spark-skeleton-ledger.md))

## Scope

Restore the DDL half of the Spark door — the handlers PR-2 declared TEMPORARY refuse arms
for — plus the deferred-test rows those handlers unblock:

- Port from v1 `repark-sql`, verbatim under the prefix map: `ctas.rs`, `create_table.rs`,
  `alter.rs`, `namespace_ddl.rs` (completing PR-2's `consume_word`-only partial).
- COMPLETE `catalog_ops.rs`: restore PR-2's declared TRIMs byte-identical to the pin —
  `reject_path_escape_ident`, `sqlparser_err`, the r24 P7 `reregister*` family,
  `namespace_schema_name` (discharges p2b rider WS1-#6, and #7 via `namespace_ddl.rs`).
- Restore `normalize.rs`'s ALTER-related rewrites (discharges the ALTER share of p2b rider
  WS1-#8; the MERGE rewrite bits stay TEMPORARY until PR-3b — riders kept, notes updated) and
  consume the `#[expect(dead_code)]` CTAS helpers (`build_transform_field`, `property_value`,
  `build_partition_spec` go live).
- Router: the PR-3a refuse arms (CTAS, column-def CREATE TABLE, ALTER sniff, CREATE/DROP
  NAMESPACE|SCHEMA|DATABASE, DROP TABLE) replaced by v1's arms VERBATIM; those arms' refuse
  tests deleted in the same commit. The MERGE, INSERT OVERWRITE, CALL, ref-DDL refuse arms
  REMAIN (restoring PR = 3b; p2b arm-table notes updated).
- Deferred-test rows #2, #4, #5, #6, #7 land as
  `crates/repark-spark/tests/ddl_sessions.rs` (session-assembly pattern of PR-2's
  `session_extension.rs`; manifest rows closed —
  [port/deferred-tests.md](port/deferred-tests.md)). Row #3 re-verified and kept (PR-3b).
- Each restored module's battery rides with it (`alter::tests`,
  `create_table::type_mapping_tests`, the namespace/catalog_ops in-module sets, restored
  normalize tests).

Out of scope: DML handlers (merge, insert_overwrite, ref_ddl, call — PR-3b), `repark-ta`
(PR-4), the ANSI door, carve-out files, AWS-touching tests (E-2: all session-level tests here
run on memory/local catalogs; acceptance env vars NEVER set).

## Edit classes (declared, bounded — p2b classes 1, 2, 6 inherited)

1. **Verbatim copy** — the four handler modules + the catalog_ops/normalize restorations,
   byte-faithful to the pin.
2. **Prefix renames (mechanical)** — crate-internal `repark_sql` → `repark_spark`;
   `repark_write::`/`repark_catalog::` → `repark_iceberg::write::`/`repark_iceberg::catalog::`;
   `repark_core::` → `repark_common::`; `repark_session::` → `repark_core::`.
   **2b (verify-panel addendum, 2026-08-07)** — v1 in-crate `crate::` types that moved to
   repark-core in phase 1 retargeted (`crate::CatalogRegistry` / `crate::LocationPolicy` →
   `repark_core::{CatalogRegistry, LocationPolicy}`); rustfmt rewrap of call sites lengthened
   by the prefix map accepted. Diff vs the pin under the sed map is therefore mechanical, not
   byte-empty.
3. **Refuse-arm restoration** — a PR-2 TEMPORARY arm replaced by the v1 arm VERBATIM, its
   refuse test deleted in the same commit (the inverse of p2b edit class 3).
4. **Deferred-test session adaptation** (ddl_sessions.rs only) — v1 `ReparkSession::new()` →
   the door-installed builder (`with_extension(SparkExtension)` + `with_sql_dialect
   (SparkDialect)`, the PR-2 pattern); `repark_catalog::memory_catalog` →
   `repark_iceberg::catalog::memory_catalog`; test bodies otherwise v1-faithful.
5. **Shared `cfg(test)` tracing harness** (phase-1 class 6) — only if a restored module's
   battery collides on global subscriber installs.

No other edit class is authorized; anything else is a STOP.

## Restoration checklist vs p2b declared riders / TRIMs / refuse arms

| p2b declaration | PR-3a action | Status (integrator confirms) |
|---|---|---|
| Refuse arm: CTAS | restored verbatim; refuse test deleted | confirmed (integrator, 2026-08-07) |
| Refuse arm: column-def CREATE TABLE | restored verbatim; refuse test deleted | confirmed (integrator, 2026-08-07) |
| Refuse arm: DROP TABLE | restored verbatim; refuse test deleted | confirmed (integrator, 2026-08-07) |
| Refuse arm: DROP NAMESPACE\|DATABASE\|SCHEMA | restored verbatim; refuse test deleted | confirmed (integrator, 2026-08-07) |
| Refuse arm: ALTER pre-parse sniff | restored verbatim; refuse test deleted | confirmed (integrator, 2026-08-07) |
| Refuse arm: CREATE NAMESPACE\|SCHEMA\|DATABASE sniff | restored verbatim; refuse test deleted | confirmed (integrator, 2026-08-07) |
| Refuse arms: MERGE / INSERT OVERWRITE / CALL / ref-DDL | REMAIN (PR-3b); notes updated | confirmed (integrator, 2026-08-07) |
| Rider WS1-#5 (write-to-branch sniff) | REMAINS (rides `ref_ddl`, PR-3b) | unchanged |
| Rider WS1-#6 (catalog_ops PARTIAL) | DISCHARGED — TRIMs restored byte-identical | confirmed (integrator, 2026-08-07) |
| Rider WS1-#7 (namespace_ddl PARTIAL) | DISCHARGED — full module ports | confirmed (integrator, 2026-08-07) |
| Rider WS1-#8 (normalize rewrite bypass) | PARTIAL discharge — ALTER rewrites + dead_code CTAS helpers live; MERGE bits stay (PR-3b, notes updated) | confirmed (integrator, 2026-08-07) |
| Rider WS1-#9 (lib.rs re-export trim) | handler `pub(crate) use` groups return with their consumers; lib-root battery share stays (PR-3b) | confirmed (integrator, 2026-08-07) |
| Deferred rows #2, #4–#7 | LANDED — `tests/ddl_sessions.rs`; manifest rows closed | confirmed (integrator, 2026-08-07) |
| Deferred row #3 | KEPT (PR-3b eager-DML arm); manifest note updated | confirmed (integrator, 2026-08-07) |

## Staged census note — PARTIAL (closes PR-3b)

The v1 lib-root `tests` module (the ~200-name router battery, `tests::partitioned_ctas` et
al.) does **NOT** port here — it rides PR-3b intact as a move-only identity unit, when the
router completes; the 342-name `repark_sql::` → `repark_spark::` empty sorted-diff is a PR-3b
acceptance (unchanged from p2b). PR-3a's obligation: every restored module carries its full
v1 in-module battery under the prefix map; NEW-outside-census names removed: the deleted
PR-3a refuse tests; NEW-outside-census names added: none (ddl_sessions.rs tests are ported
census names, landed at their manifest rows).

## Gate results (integrator fills)

| Gate | Result | Evidence |
|---|---|---|
| `make ci` per commit | PASS | exit 0 at PR head (2026-08-07); each commit built green before landing |
| `make preflight` (PR head) | PASS | exit 0 — zizmor "No findings to report" |
| `cargo test -p repark-spark` (never `--all-features`) incl. `--test ddl_sessions` | PASS | 70/70: 64 lib + 5 ddl_sessions + 1 session_extension; full `cargo test --workspace` also green |
| restored-module batteries match v1 pin counts under the prefix map | PASS | census-partial (70 names) vs live run: empty sorted diff |
| refuse tests of restored arms deleted same-commit | PASS | 6 refuse tests removed in the handler commit (router/tests.rs, dialect/tests.rs repoint) |
| deferred rows #2, #4–#7 closed in manifest; #3 note updated | PASS | port/deferred-tests.md rows struck LANDED; #3 re-verified note dated |
| forbidden-literal sweep (tree + `git log -p`) | PASS | grep over `git diff phase-2/pr-2..HEAD --unified=0` = 0 hits |
| map.md lockstep (`check_map_md.sh`) | PASS | exit 0 |

## Deviations / STOPs

- **WS1 map.md lockstep miss (2026-08-07):** the handler workstream omitted the
  `router/map.md` + `dialect/map.md` lockstep updates; the integrator folded them into the
  handler commit before landing (guard caught it pre-commit — no post-land fix needed).
- **WS1 mid-flight build blockers (2026-08-07):** an E0533 pattern-path error and a rustfmt
  failure surfaced during the handler port and were fixed before the commit landed; per-commit
  greenness preserved.
- **Verify-panel doc findings (2026-08-07):** two LOW ledger-completeness findings (missing
  edit class 2b declaration; this Deviations section empty) — fixed in the follow-up
  `fix(pr-3a)` commit; no code change.

## Retrospective

*(filled at unit close, per SEPMO)*
