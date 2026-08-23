# Charter ledger — RP-1 · fork repin (F-0, F-1, F-2, F-8a)

**Date:** 2026-08-23 · **Branch:** `feat/rp-1-fork-repin` · **Base:** `2b319de` (`main`,
post-#225) · **Policy:** [../../../AGENTS.md](../../../AGENTS.md) "Version-pin contract" ·
**Handoff:** [../../roadmap/mid-term/iceberg-rust-handoff-2026-08-23.md](../../roadmap/mid-term/iceberg-rust-handoff-2026-08-23.md)

**Retires:** this ledger moves to `../completed/` in the unit's last commit.

Owner-chartered 2026-08-23 as the first row of the post-MW sequence. The engine pin
`0c5fd58d4ab73a0113a8b28b717cf5d002b0f8f2` is a genuine ancestor of fork `main`
`5e7b2e4f8fcb0ff65943cdbc10cdd8f4132fe0b6` (20 commits ahead). Landed on that range:
F-0 `#214`, F-1 through `#213`, F-2 `#215`. Open fork F-3 `#216` is **not** in this
unit. DataFusion family does not move. MW-6 is a later unit, never a passenger.

## PROPOSITION LEDGER — RP-1 — 2026-08-23

| Clause | Proposition (checkable) | Proof obligation | Verdict | Evidence / open question |
|---|---|---|---|---|
| C-001 | Every `iceberg*` `[patch.crates-io]` rev is `5e7b2e4f8fcb0ff65943cdbc10cdd8f4132fe0b6`, and `Cargo.lock` resolves to that commit. | `rg` on workspace `Cargo.toml` + lock source entries. | PROVEN | `Cargo.toml` five identical revs; lock sources match. |
| C-002 | `datafusion`, `datafusion-spark`, `arrow*`, `parquet`, and `rust-toolchain.toml` are byte-identical to `origin/main` at `2b319de`. | `git diff origin/main -- Cargo.toml rust-toolchain.toml` shows only the iceberg patch rev (and lock iceberg sources). | PROVEN | Family pins unchanged; only iceberg patch rev moved. |
| C-003 | `NamespaceScopedCatalog` is re-enumerated against the new `Catalog` trait: every required method is forwarded; every defaulted method is either an explicit forward or a stated omission. | Diff the trait surface at the new rev against `crates/repark-iceberg/src/catalog/provider.rs` and the crate-root map's last-audit counts. | PROVEN | Still 14 required + 16 defaulted; 3 omissions. `namespace_scoped_tests.rs`. |
| C-004 | The metadata-projection shim is kept if and only if the new rev's metadata-table `scan` still ignores `projection`, including the empty-projection case. | Read fork `metadata_table.rs` at the new rev; keep or delete `metadata_projection.rs` per its stated removal criterion. | PROVEN | Fork `scan` still ignores `_projection`; shim tests keep empty-projection pins. |
| C-005 | The two metadata-table emptiness pins still pass: `crates/repark-sql/tests/introspection.rs` and `crates/repark-spark/src/tests/metadata_tables.rs`. | `cargo test` those two named tests. | PROVEN | `metadata_tables_are_hidden_from_enumeration_but_stay_queryable_through_the_{ansi,spark}_door` |
| C-006 | The `a$b` "unresolvable through the fork" residue note is gone from `crates/repark-iceberg/map.md` and `src/catalog/map.md`; the ADR-0006 enumeration filter remains. | Grep those maps; filter code still drops synthesized `$` names. | PROVEN | last-`$` filter; `the_filter_keeps_names_the_fork_did_not_synthesize` lists `a$b` alone. |
| C-007 | `CALL rewrite_position_delete_files` on a 4-file position-delete group returns `rewritten_delete_files_count = 0` and `added_delete_files_count = 0`; registry row `MOR-1` is retired. | Flip `call_mor1_compacts_below_sparks_min_input_files_floor` to equality; edit `docs/spark-sql-iceberg-parity.md`. | PROVEN | `call_mor1_…` zeros + leave-4; `call_rpdf_compacts_at_sparks_min_input_files_floor` at 5. |
| C-008 | No remaining engine test requires compaction of a 2-file or 4-file position-delete group. | `rg` for rewritten_delete_files_count assertions at 2 or 4, and for `min-input-files` / `entries.len() < 2`. | PROVEN | `_acceptance.py` `MOR_MIN_POSITION_DELETE_FILES = 5`; 4-file CALL pin expects zeros. |
| C-009 | `execute_expire_snapshots` fills Spark's three content-file columns from `CleanupReport`'s typed views; `ExpireCounts::tally` and `classify_content_files` are deleted; `call_expire_splits_content_files_like_spark` still holds. | Diff `crates/repark-spark/src/call.rs`; re-run that test. | PROVEN | typed views; expire pin `data == 4`, `position == 2`, `assert_ne`. |
| C-010 | `write.merge.isolation-level = snapshot` remains a supported opt-down (drops `validate_no_conflicting_data`); a pin records that a successful MOR delete is not undone by a concurrent `Replace` on that arm after F-0. | Existing snapshot OCC tests stay green; add or retarget a files-exist/`Replace` pin on the snapshot arm. | PROVEN | `commit_row_delta_snapshot_rejects_concurrent_replace_compaction_of_referenced_file` |

VERDICT: PASS (OPEN=0, REJECTED=0). LOGIC_SCORE = 10/10.

Clauses flipped PROVEN after Actor + cycle-1 remediations. Attestation below.

```yaml
KILLED_ASSUMPTIONS:
  - "Wait for fork F-3 before repinning": REMOVED (owner sequenced RP-1 now; F-3 is opt-in dangling-delete compose, later R135)
  - "MW-6 can share this PR": REMOVED (handoff §5: one repin per landed batch, never a passenger)
  - "Family bump rides any iceberg* rev change": REMOVED (fork did not move its DataFusion 54.1 base)
RISK_HEATMAP:
  - risk: F-0 changes commit conflict behaviour; a test that expected a silent commit of Replace-vs-delete may go red
    severity_if_realized: S1
    mitigation: C-010
  - risk: F-1 floor 5 makes MW-5's 10-delete compact still work (10 >= 5) but any 2-file helper in MW-2 tests breaks
    severity_if_realized: S1
    mitigation: C-007, C-008
CLARIFYING_QUESTIONS: []
```

```yaml
COVERAGE_ATTESTATION:
  pr_unit: RP-1
  categories:
    - id: AT-1
      status: ATTACKED
      evidence: >
        C-001..C-010 cited from tests and maps; pin rev 5e7b2e4; family freeze.
      artifacts: [Cargo.toml, crates/repark-iceberg/map.md]
    - id: AT-2
      status: ATTACKED
      evidence: >
        4-file RPDF zeros; 5-file compact; expire data==4 and position==2
        (assert_ne); snapshot Replace reject names the path.
      artifacts: [call_mor1_compacts_below_sparks_min_input_files_floor, call_rpdf_compacts_at_sparks_min_input_files_floor, call_expire_splits_content_files_like_spark, commit_row_delta_snapshot_rejects_concurrent_replace_compaction_of_referenced_file]
    - id: AT-3
      status: ATTACKED
      evidence: >
        Compact no-op is rewritten=0 and leftover file count; expire no-op would
        fail data==4 / position==delete_files.
      artifacts: [call.rs tests]
    - id: AT-4
      status: ATTACKED
      evidence: >
        Snapshot isolation still opt-down; F-0 Replace in files-exist pinned on that arm.
      artifacts: [occ_tests.rs]
    - id: AT-5
      status: N/A
      justification: no AWS/credentials/IAM in this unit; pin is a git SHA.
    - id: AT-6
      status: ATTACKED
      evidence: >
        Live row set unchanged across floor-5 compact; expire split is typed views.
      artifacts: [call_rpdf_compacts_at_sparks_min_input_files_floor]
    - id: AT-7
      status: N/A
      justification: no performance claim.
    - id: AT-8
      status: ATTACKED
      evidence: >
        MOR-1 retired in the registry; remaining rows MOR-2 ORPHAN-1 ORPHAN-2 B-MOR-3.
      artifacts: [docs/spark-sql-iceberg-parity.md]
    - id: AT-9
      status: ATTACKED
      evidence: >
        expire counts refuse overflow (count_as_i64); F-0 failure is non-retryable DataInvalid.
      artifacts: [call.rs, occ_tests.rs]
    - id: AT-10
      status: ATTACKED
      evidence: >
        Skipping the 5-MERGE seed leaves compact a no-op and fails MOR_MIN=5.
      artifacts: [python/repark/tests/_acceptance.py]
  reattested: []
  complete: true
```
