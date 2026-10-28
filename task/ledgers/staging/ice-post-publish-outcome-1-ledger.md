# Unit ledger — ICE-POST-PUBLISH-OUTCOME-1 · a committed change reports its own outcome when the session name refresh fails (AUD-002)

**Date:** 2026-10-28 · **Branch:** `fix/ice-post-publish-outcome-1` · **Base:** `321c6095` (`main` at the branch point) · **Model:** builder `claude-opus-5-5` (commits `91243499`, `042fd80f`, `24e0c3c7`, `a8ab9364` and `1c6e730b`); clerk `claude-haiku-5-5` (the records commit) · **Policy:** [../../../AGENTS.md](../../../AGENTS.md).
**Path:** STANDARD. **risk_tier: standard.**

**Retires:** this ledger moves to `../completed/` when the unit's last commit lands.

**Unit pair.** This ledger and [the ICE-DELETE-FORMAT-GUARD-1 ledger](ice-delete-format-guard-1-ledger.md) share one PR and one head (`1c6e730b`, fix round 2 on `b9d83f38`; fix round 1 was `a8ab9364`). This ledger proves AUD-002. The card for this unit, `ice-post-publish-outcome-1-card-2026-10-28.md`, is filed by PR #1204, merged to main at `f7c6cda8`; its status line on this branch reads "built as PR #1206, awaiting verification".

**Why now.** Audit finding AUD-002: after a catalog commit succeeded, a failed session name refresh made the statement fail. The caller saw an error for a change that had happened. The session's name directory also kept its old frozen listing, so the committed table was invisible to the next statement that named it.

**Not in this unit.** The native door (`repark-sql`). Its 13 post-commit refresh sites still call the old invalidation directly and report a refresh failure as the statement's error. Same defect class; carded as NATIVE-DOOR-POST-PUBLISH-1 (`native-door-post-publish-1-card-2026-10-28.md`, filed on this branch, not dispatched).

## Rulings recorded at open

- R-POST-1 (coordinator, round 2, 2026-10-28): keep AUD-002 as built. The native door is a follow-up unit, not this one (Q1).

## Design reasoning

The Spark door's refresh runs after the catalog commit, so a refresh failure cannot undo the change. The unit therefore changes what the statement reports, not what it commits. A deferred namespace provider is built through a namespace-scoped catalog and does no catalog I/O at install time, so it cannot fail on a catalog outage. The next statement that names the namespace lists it, and a failed listing is retried rather than cached (step-0 reading of the fork's lazy name directory, C-005). The after-commit variant is the only path that installs a deferred provider; it returns an error only when no provider can be installed, and that error states the commit first.

## Clauses

| Clause | Statement | Proof obligation | Verdict | Evidence |
|---|---|---|---|---|
| C-001 | A `CREATE TABLE … AS SELECT` on the Spark door whose session name refresh fails after the publish succeeds, and the next statement lists the table once the refresh recovers. | `crates/repark-spark/tests/post_publish_outcome.rs::ctas_with_a_refresh_failure_after_publish_reports_the_committed_outcome`: `Ok`, the committed id on the catalog, and the session listing after the fault clears. | PROVEN | Red on `321c6095` with the test file only: the statement returned `Err("Unexpected => table listing unavailable")`. Green at `24e0c3c7`. Builder's run, copied by the clerk, not re-run. |
| C-002 | A `CREATE OR REPLACE TABLE … AS SELECT` on the Spark door with the same post-publish refresh failure reports the replacement as committed, and the next statement lists the replaced table. | `crates/repark-spark/tests/post_publish_outcome.rs::rtas_with_a_refresh_failure_after_publish_reports_the_committed_outcome`: `Ok`, the replacement id on the catalog, and the session listing after recovery. | PROVEN | Red on `321c6095`: `Err("Unexpected => table listing unavailable")`. Green at `24e0c3c7`. Builder's run, copied. |
| C-003 | Control: a catalog failure before the publish still fails the statement and leaves no table. | `crates/repark-spark/tests/post_publish_outcome.rs::ctas_with_a_catalog_failure_before_publish_reports_failure_and_leaves_no_table`. | PROVEN | Green on `321c6095` and at `24e0c3c7`. The pin is a control: the change must not alter a pre-publish failure. |
| C-004 | When no deferred listing can be installed (the catalog name is not registered, or the registered provider is a foreign type whose rebuild failed), the statement's error begins `committed: the statement's change is committed in catalog` and names the refresh failure. | `crates/repark-iceberg/src/catalog/tests/namespace_drop.rs::refresh_after_commit_without_a_registered_provider_states_the_change_is_committed`. | PROVEN | Green at `24e0c3c7`. Builder's run, copied. |
| C-005 | The fork's name directory does not cache a failed listing, so a namespace whose listing failed lists again on its next reference. | The recovery assertions of C-001 and C-002 are the only measurement. No fork-level pin exists. | OPEN | The step-0 reading of the fork's lazy `IcebergSchemaProvider` is not measured on its own. Closing question: does the verifier want a pin that fails two listings and then succeeds? |
| C-006 | Every Spark-door post-commit refresh (CTAS, RTAS, CREATE TABLE, CREATE and ALTER NAMESPACE, DROP TABLE, the ALTER family, ref DDL, TRUNCATE, the INSERT family and the CALL procedures) takes the after-commit variant, through `catalog_ops::reregister` and `reregister_namespaces`. `reregister_drop_namespace` lists nothing and is unchanged. | Code shape: `crates/repark-spark/src/catalog_ops.rs` (the two helpers). No per-family pin. | OPEN | The builder's claim, by reading the callers: each runs after its mutation has committed. Closing question: the verifier re-reads the call sites, or a per-family pin is charted. |
| C-007 | The native door's post-commit refresh sites are not covered by this unit. They call `invalidate_catalog_namespaces` directly and report a refresh failure as the statement's error. | The site list in the registry row ICE-POST-PUBLISH-OUTCOME-1 and in the card NATIVE-DOOR-POST-PUBLISH-1: `create_table.rs` `finish` (about line 654), `insert_overwrite.rs:115`, `session_insert.rs:70`, `truncate.rs:59`, `schema_ddl.rs:45` and `:117`, `alter.rs:133`, `v3/cow.rs:249`, `:451` and `:732`, `v3/partitioned_equality_deletes.rs:235` and `:496`, `v3/branch_tag_time_travel.rs:223`. | OPEN | Measured by reading, not by a run. Closing question: does the owner charter NATIVE-DOOR-POST-PUBLISH-1? |
| C-008 | The gates at `a8ab9364` hold (the fix round 1 head): `cargo test -p repark-iceberg` 1061 passed; `cargo test -p repark-spark` 3090 passed (29 binaries); `cargo test -p repark-sql` 596 passed; `make rust-fmt-check`, `make rust-clippy`, `make rust-panic-ban`, `make check-rust-file-size` (1258 files clean, 32 exceptions; `merge.rs` at 996 of 1000) and `make check-model-ban` exit 0; the comment gate finds no hit on the added `.rs` lines. | The builder's round-3 gate table, copied. The clerk ran no cargo. | OPEN | The verifier round re-runs the gates on the head. Shared with ICE-DELETE-FORMAT-GUARD-1 C-010. |

VERDICT (2026-10-28): 8 clauses. 4 PROVEN (C-001 to C-004, each with a committed pin). 4 OPEN (C-005 the fork's retry reading, C-006 the call-site shape, C-007 the native door, C-008 the verifier's gate re-run).

VERDICT (2026-10-28, after fix round 1 at `a8ab9364`): 8 clauses, unchanged: 4 PROVEN (C-001 to C-004), 4 OPEN (C-005 to C-008).

VERDICT (2026-10-29, verification round 2, head `1c6e730b`): 8 clauses, unchanged: 4 PROVEN (C-001 to C-004), 4 OPEN (C-005 to C-008).

## Verification round 1 (2026-10-28, at `f1935e81`; fix round 1 at `a8ab9364`)

- Opus 5.5 verifier: FAIL on V-1 to V-6. Muse MAX critic: FAIL on F1 to F4. V-1 to V-3 and F1 to F4 concern the delete-format guard and are recorded in the ICE-DELETE-FORMAT-GUARD-1 ledger. V-4 and V-5 concern the delete-format records only. V-6 names both ledgers' stale `#1204 open` and `24e0c3c7` head lines, corrected here.
- None of V-1 to V-6 or F1 to F4 names a defect in the AUD-002 behaviour, so C-001 to C-004 stand as recorded.
- Fix round 1 (`a8ab9364`) changes the guard seats only (the resolver, and the delete-format, encryption and BUG-001 guards); this ledger's clauses are unchanged.

## Verification round 2 (2026-10-29, at `b9d83f38`; fix round 2 at `1c6e730b`)

- Opus 5.5 verifier: FAIL on R2-V1 to R2-V3. None of the three names the AUD-002 behaviour: R2-V1 and R2-V2 concern the branch path and the target resolver, and R2-V3 concerns the native-door map row. They are recorded in the ICE-DELETE-FORMAT-GUARD-1 ledger's round-2 section. Muse MAX critic: PASS, with F1 and F2, both on the delete-format guard and recorded there.
- Fix round 2 changes no clause of this unit. The head is shared with ICE-DELETE-FORMAT-GUARD-1. The builder's round-4 gate table, copied and not re-run by the clerk, includes `cargo test -p repark-spark` 3097 passed, which runs the post-publish pins.

## Records

- Shared head: `1c6e730b` (fix round 2, the squash of the builder's round-4 commits on `b9d83f38`) changes the delete-format guard seats only; this ledger's clauses are unchanged.
- Commits: `91243499` (the committed outcome for the Spark-door refresh, with the CTAS pin), `042fd80f` (the round-1 re-route, superseded by `24e0c3c7`), `24e0c3c7` (the round-2 head), `a8ab9364` (fix round 1, the guard seats only; this ledger's clauses are unchanged). The final behaviour is the squash of this branch; the round-1 commit body describes a re-route the head no longer has.
- The release-notes line for this unit is in `task/roadmap/mid-term/v1-5-6-release-notes-draft.md` under Behaviour changes.
- The registry row ICE-POST-PUBLISH-OUTCOME-1 is in `docs/spark-sql-iceberg-parity.md`, in the DDL section beside RTAS-OPS-1.
