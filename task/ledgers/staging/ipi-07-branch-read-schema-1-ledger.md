# Unit ledger — IPI-07-BRANCH-READ-SCHEMA-1 · a branch read projects the table's current schema

**Date:** 2026-09-22 · **Branch:** `fix/ipi-07-branch-read-schema` · **Base:** `743f1be9` (`origin/main`)
**Model:** muse-spark-1.3-contributor (muse-worker lane bs-r1) · **Policy:** [../../../AGENTS.md](../../../AGENTS.md).
**Path:** STANDARD. **risk_tier: standard.**

**Retires:** this ledger moves to `../completed/` when the unit's last commit lands.

**Why now.** Scoreboard cell R-BRANCH-SCHEMA and the `BS-*` pc2 cells (Spark 4.1.2 +
Iceberg 1.11.0, settled 2026-09-22) show a branch read projecting the table's CURRENT
schema after schema evolution (`CREATE BRANCH b0`, `ADD COLUMN z INT`,
`SELECT * FROM t.branch_b0` → four columns, `z` NULL), while tag, snapshot-id and
timestamp reads keep the snapshot schema. RePark served the branch head's snapshot schema
(three columns) on every pin. The pinned fork already decides branch-vs-tag inside
`IcebergStaticTableProvider::try_new_from_table_ref`; this unit only calls it from the two
`VersionRef` call sites.

**Not in this unit:** the fork (RePark only, no pin bump), `crates/repark-spark/src/wap.rs`
(WAP belongs to lane xo-muse10), `write_to_branch.rs`, `crates/repark-sql/**`,
metadata-table code, `Cargo.toml`, `Cargo.lock`, `STATUS.md`, any file-size ceiling, any
existing test's expected value.

## PROPOSITION LEDGER — IPI-07-BRANCH-READ-SCHEMA-1 — 2026-09-22

| Clause | Proposition (checkable) | Proof obligation | Verdict | Evidence / open question |
|---|---|---|---|---|
| C-001 | After `CREATE BRANCH b0` + `ADD COLUMN z INT`, `SELECT * FROM t.branch_b0` answers four columns (`id` int64, `data`/`cat` string, `z` int32) with `z` NULL on both rows. | `test_branch_selector_projects_current_schema` plus the Rust `branch_selector_projects_current_schema_after_add_column`. | PROVEN | Red before the fix (`assert ['id', 'data', 'cat'] == ['id', 'data', 'cat', 'z']`); green after, on the release native and under `REPARK_PARITY_LIVE=1`. |
| C-002 | The DataFrame door `spark.read.option("branch", "b0").table(t)` answers the same four columns with `z` NULL. | `test_branch_reader_option_projects_current_schema` plus the Rust `reader_options_branch_projects_current_schema` (`read_table_at` with `VersionRef`). | PROVEN | Red before the fix (three columns); green after, offline and live. |
| C-003 | `SELECT * FROM t VERSION AS OF 'b0'` answers the same four columns with `z` NULL. | `test_version_as_of_branch_projects_current_schema` plus the Rust `version_as_of_branch_projects_current_schema`. | PROVEN | Red before the fix (three columns); green after, offline and live. |
| C-004 | `SELECT id FROM t.branch_b0 WHERE z IS NULL` answers both rows. | `test_branch_filter_on_new_column` plus the Rust `branch_selector_filters_on_the_added_column`. | PROVEN | Red before the fix (`UNRESOLVED_COLUMN` for `z`); green after, offline and live. |
| C-005 | After `DROP COLUMN data` the branch read is `(id, cat)`; after `RENAME data TO payload` it is `(id, payload, cat)` with values preserved. | `test_branch_drop_and_rename_project_current_schema` plus the two Rust DROP/RENAME pins. | PROVEN | Red before the fix (stale three-column snapshot schema on both); green after, offline and live. |
| C-006 | The tag selector, `VERSION AS OF 't0'`, the snapshot-id selector, `VERSION AS OF <id>`, `TIMESTAMP AS OF` a pre-evolution instant, and `option("versionAsOf", "t0")` keep the three-column snapshot schema with both rows. | `test_tag_snapshot_and_timestamp_keep_snapshot_schema` plus the Rust `tag_snapshot_and_timestamp_keep_the_snapshot_schema`. | PROVEN | Green before and after (regression guards); `TIMESTAMP AS OF` uses the seed commit time plus one second, which resolves the pre-evolution snapshot because evolution writes no snapshot. |
| C-007 | The legacy `tag` reader option keeps refusing with the exact pinned text. | `test_tag_reader_option_keeps_refusing`. | PROVEN | Green before and after; the brief's `.option("tag", "t0")` clause conflicts with the IPI-23 Spark-parity refusal (see FINDING BS-TAGOPT-1), so the refusal is preserved and flagged, and tag reads pin through `versionAsOf` in C-006. |
| C-008 | An unknown ref (`t.branch_nope`, `VERSION AS OF 'nope'`) keeps the exact refusal on both spellings. | `test_unknown_ref_keeps_refusal` plus the Rust `unknown_ref_keeps_the_exact_refusal` (full-string equality of both spellings). | PROVEN | Green before and after; the fork's `reference … not found` text never surfaces because RePark resolution stays first. |
| C-009 | The live tier re-derives the branch/tag shapes on Spark 4.1.2 and RePark cross-reads the adopted table. | `test_live_branch_schema_matches_spark` under `REPARK_PARITY_LIVE=1`. | PROVEN | Green in the lane gate's live leg: Spark's own branch read is four columns with `z` NULL and its tag read three; the adopted RePark reads match on the SQL and `option("branch")` doors. |

## Gates

Measured at the final head (see the hand-back for the lane-gate `.done` line):

- Lane gate `local-gate.sh` at the final head: `CB=0 R=0 T=0 U=0 L=0` (comment ban;
  `maturin develop --release`; rust `time_travel` legs 22 + 13 passed; the six pytest files
  85 passed + 5 skipped offline, 90 passed live). The gate's rust filter does not match the
  new `branch_read_schema` file, so those pins ran separately (next line).
- `cargo test -p repark-spark --lib tests::branch_read_schema`: 8 passed.
- Neighbors `tests::{time_travel, branch_ops, refs_and_wap, wap_branch, metadata_tables_asof,
  ref_ddl, call_rdf_branch, write_to_branch, branch_read_schema}`: 75 passed.
- `cargo test -p repark-core --lib time_travel`: 13 passed.
- `make rust-clippy` clean; the panic-ban gate clean; `bash scripts/check_map_md.sh --base
  origin/main` rc 0; `python3 scripts/check_ledger_grammar.py` clean.
- `python3 /tmp/oc-worker/_lib/comment_ban.py /tmp/xo55-bs origin/main HEAD` exit 0 after
  every commit.

## Findings

```text
FINDING:
  id: BS-TAGOPT-1
  severity: S2
  category: AT-1
  clause: C-007
  disposition: ACCEPTED_FLAGGED (preserved refusal, flagged for the orchestrator ruling in hand-back Q1)
  summary: The brief asks `.option("tag", "t0")` to answer three columns, but the tag reader option refuses on Spark 4.1.2 (IPI-23, measured 2026-09-22) and on RePark, pinned by test_time_travel.py::test_reader_option_branch_and_tag. Serving it would break Spark parity and an existing test's expected value, both forbidden by the brief. The unit keeps the refusal and pins tag reads through versionAsOf instead.
```

```text
COVERAGE_ATTESTATION:
  pr_unit: ipi-07-branch-read-schema-1
  categories:
    - id: AT-1
      status: ATTACKED
      evidence: Every brief clause walked against behavior; the tag-option clause conflicts with the pinned IPI-23 refusal and is preserved plus flagged as BS-TAGOPT-1 rather than implemented.
      artifacts: [task/ledgers/staging/ipi-07-branch-read-schema-1-ledger.md]
    - id: AT-2
      status: ATTACKED
      evidence: Branch, tag, snapshot-id, timestamp, unknown-ref, and empty-result inputs exercised on the SQL, selector, VERSION AS OF, and reader-option spellings.
      artifacts: [crates/repark-spark/src/tests/branch_read_schema.rs, python/repark/tests/test_ice_branch_schema_1.py]
    - id: AT-3
      status: ATTACKED
      evidence: Unknown-ref refusal text pinned full-string on both spellings; legacy tag-option refusal pinned exact; the fork unknown-ref text cannot surface (gatekeeper first).
      artifacts: [crates/repark-spark/src/tests/branch_read_schema.rs, python/repark/tests/test_ice_branch_schema_1.py]
    - id: AT-4
      status: N/A
      justification: Read-only change; the provider choice is per statement with no shared mutable state.
    - id: AT-5
      status: N/A
      justification: No privileged action, no secrets, no deserialization, no path handling.
    - id: AT-6
      status: ATTACKED
      evidence: Branch reads track ADD/DROP/RENAME COLUMN; tag, snapshot-id, and timestamp reads keep the snapshot schema; no write path touched.
      artifacts: [crates/repark-spark/src/tests/branch_read_schema.rs, python/repark/tests/test_ice_branch_schema_1.py]
    - id: AT-7
      status: N/A
      justification: Same catalog-load count as before (resolution, then provider build); no new loop or allocation.
    - id: AT-8
      status: ATTACKED
      evidence: try_new_from_table_ref verified async at the pinned fork rev with the branch-vs-tag decision inside; no RePark-side is_branch test added; error contract preserved by the gatekeeper.
      artifacts: [crates/repark-spark/src/time_travel.rs, crates/repark-core/src/time_travel.rs]
    - id: AT-9
      status: N/A
      justification: No new failure mode; refusal texts unchanged and pinned.
    - id: AT-10
      status: ATTACKED
      evidence: Six Rust and five Python behavior pins failed before the fix and pass after; both if-let arms change output (branch projects current, tag keeps snapshot); guards fail if the gatekeeper is dropped (the fork text differs).
      artifacts: [crates/repark-spark/src/tests/branch_read_schema.rs, python/repark/tests/test_ice_branch_schema_1.py]
  complete: true
```
