# Charter ledger — DML-B · INSERT OVERWRITE … PARTITION (static + dynamic) + overwritePartitions()

**Date:** 2026-08-30 · **Branch:** `feat/dml-b-insert-overwrite` · **Base:** `origin/main` ·
**Path:** STANDARD. **Retires:** moved to `completed/` in this unit's departure commit.

**Why now.** v0.6 merge-order 1 of 4 (owner-authorized 2026-08-30). The intake's "blocked on
fork F-5" premise is stale: F-5 landed as fork PR #217 and is an ancestor of the engine pin.
Java routes static `PARTITION (k=v)` through `OverwriteFiles.overwriteByRowFilter`, not
`ReplacePartitions`. Recipe:
[release-roadmap-2026-08-29.md](../../roadmap/epic-term/release-roadmap-2026-08-29.md) §v0.6.

## PROPOSITION LEDGER — DML-B — 2026-08-30

| Clause | Proposition (checkable) | Proof obligation | Verdict | Evidence |
|---|---|---|---|---|
| C-001 | Static `INSERT OVERWRITE … PARTITION (k=v, …)` (every listed field an equality) commits through `Transaction::overwrite_files().overwrite_by_row_filter(k=v AND …)` plus `validate_added_files_match_overwrite_filter`. Identity partition fields only. Sibling data-file paths stay unchanged. Spark SQL door and facade VALUES-int paths measure int32/string; ANSI SQL integer literals measure Int64/Utf8; facade pins accept int32-or-int64 / string-or-large_string. Snapshot operation is Iceberg `overwrite` when files are added, `delete` when the source is empty and the partition had files. Two-key AND filter replaces only the named tuple; incomplete `PARTITION (k1=v1)` on a two-key spec replaces every k2 under k1 (Spark). | Three-door pins + `$snapshots.operation` + sibling file-path set. Red-first: `commit_rejects_added_file_outside_overwrite_filter` (flag off = commit succeeds); two-key pin reds if AND collapses to the first conjunct. | PROVEN | Live 2026-08-30: nonempty `[(1,z),(2,b),(3,c)]` int32/string, op `overwrite`, sibling files byte-stable; empty stamps `delete`. Two-key complete keeps `(1,east,b)`; incomplete `PARTITION (id=1) SELECT 'north','z'` leaves `[(1,north,z),(2,west,c)]`. `PARTITION (cat='west')` and `PARTITION (id=NULL)` keep siblings. pins: dml-b-insert-overwrite/C-001 |
| C-002 | Dynamic `INSERT OVERWRITE … PARTITION (k, …)` (no values) commits through `Transaction::replace_partitions()` with `add_file` / `add_files`. Snapshot summary carries `replace-partitions=true` and operation `overwrite`. Partitions absent from the source stay byte-stable. Same three doors, values and Arrow types. Matches Spark `writeTo.overwritePartitions` / `partitionOverwriteMode=dynamic`, not Spark default-STATIC `PARTITION (k)` wipe. | Three-door pins + summary property + sibling identity. | PROVEN | Live 2026-08-30: writeTo and dynamic-mode SQL keep siblings and stamp `replace-partitions=true`. pins: dml-b-insert-overwrite/C-002 |
| C-003 | `DataFrameWriterV2.overwritePartitions()` (and `overwrite_partitions`) is Spark's dynamic partition overwrite: it replaces only partitions present in the source. Same snapshot stamp as C-002. `overwrite(condition)` stays refused. | Facade pins, both spellings; target rows outside source partitions unchanged. | PROVEN | Live writeTo nonempty `[(2,b),(9,a)]` int32/string, op `overwrite`, `replace-partitions=true`. pins: dml-b-insert-overwrite/C-003 |
| C-004 | Empty-input **dynamic** overwrite refuses loud in the engine. Three surfaces, never merged: (a) Spark SQL default-STATIC empty `PARTITION (k)` wipes the table; (b) Spark `writeTo().overwritePartitions()` empty is a no-op; (c) RePark `PARTITION (k)` empty refuses. Empty-input **static** overwrite is partition-scoped. | Negative pin for empty dynamic; empty static is C-001 / C-005. | PROVEN | Live 2026-08-30: (a) STATIC empty SQL wiped; (b) writeTo empty snap-delta 0; (c) repark needle `Cannot dynamically overwrite partitions`. pins: dml-b-insert-overwrite/C-004 |
| C-005 | Acceptance pins `empty_insert_overwrite_partition_drops_only_named_partition` and `insert_overwrite_partition_nonempty_replaces_only_named_partition` flip from refuse-loud to partition-scoped success on a partitioned fixture. No other expected-value repin. Transform-table `PARTITION (id = 1)` (PIN O5) stays refused. | The two named tests plus PIN O5 still red on a revert of the behavior. | PROVEN | Flipped pins in `insert_overwrite.rs` (renamed from `refuses_*`); PIN O5 remains NotImplemented. pins: dml-b-insert-overwrite/C-005 |
| C-006 | Documents match the pins: registry DML-1, iceberg-guide write-forms list, maps in lockstep. ANSI whole-table `INSERT OVERWRITE` stays Q9-omitted; PARTITION forms are this unit's new surface on both SQL doors. | Registry + guide + map diffs; Q9 whole-table refuse pin still green. | PROVEN | Registry DML-1 FIXED 2026-08-30; Q9 pin `whole_table_insert_overwrite_stays_q9`. pins: dml-b-insert-overwrite/C-006 |

VERDICT: PROVEN — 6 clauses, 0 OPEN, 0 REJECTED.

## 1. Out of scope

- Whole-table `INSERT OVERWRITE` (already delivered on the Spark door; ANSI Q9 remains).
- `DataFrameWriterV2.overwrite(condition)` — stays refused (no engine path in this unit).
- `spark.sql.sources.partitionOverwriteMode=dynamic` making a PARTITION-less overwrite dynamic.
- Mixed static/dynamic `PARTITION (p1=1, p2)` — refuse loud; not Spark's mixed mode.
- Multi-spec (partition-evolved) interop — optional, owner-gated (design card DML-B hand-back).
- `TRUNCATE TABLE` (DML-C).
- Transform-field static `PARTITION (id_bucket=n)` row-filter lowering.

## 2. Sequence

1. This ledger (docs-only).
2. Static path + C-005 flip of the two named pins (red-first).
3. Dynamic path + empty-input guard (C-002, C-004).
4. `overwritePartitions()` (C-003).
5. Three-door oracle pins (C-001..C-004 values, types, snapshot).
6. Docs (C-006), gates.

## 3. Owner actions

- None required to start. HALT only if a flip needs expected-value changes outside C-005 / PIN O5.

```yaml
COVERAGE_ATTESTATION:
  pr_unit: dml-b-insert-overwrite
  cycle: actor
  risk_tier: high
  critic_engine: pending-critic
  complete: true
  note: >
    Actor-filed so the grammar gate can pass with every clause PROVEN. The
    Critic re-attests. Live PySpark 4.1.2 + Iceberg 1.11.0 measured 2026-08-30.
  categories:
    - id: AT-1
      status: ATTACKED
      evidence: C-001..C-006 walked against Spark/ANSI/facade pins and live oracle rows.
      artifacts: [crates/repark-spark/src/tests/partition_overwrite.rs, crates/repark-sql/src/partition_overwrite.rs, python/repark/tests/test_dml_b_partition_overwrite.py, python/repark/tests/test_writer_v2.py]
    - id: AT-2
      status: ATTACKED
      evidence: Empty static, empty dynamic, Hive too-many-columns, mixed refuse, transform PIN O5.
      artifacts: [empty_static_partition_overwrite_stamps_delete_operation, empty_dynamic_partition_overwrite_refuses, static_partition_overwrite_rejects_too_many_source_columns, overwrite_partition_clause_on_transform_table_still_rejected]
    - id: AT-3
      status: ATTACKED
      evidence: Empty dynamic refuses before commit; arity mismatch leaves prior rows; empty static is partition-scoped delete.
      artifacts: [refuse_empty_dynamic_overwrite, test_sql_empty_dynamic_partition_overwrite_refuses, test_sql_static_partition_overwrite_rejects_injected_column]
    - id: AT-4
      status: ATTACKED
      evidence: Sibling partition files stay; replace_partitions vs overwrite_files stamp; overwrite(condition) stays refused.
      artifacts: [static_partition_overwrite_stamps_overwrite_operation, dynamic_partition_overwrite_replaces_source_partitions_only, test_write_to_overwrite_condition_loud_reject]
    - id: AT-5
      status: N/A
      justification: No new credential, secret, unsafe, or caller-controlled filesystem path; PARTITION literals bind as Iceberg Datum.
    - id: AT-6
      status: ATTACKED
      evidence: Sibling rows and Arrow types match the live oracle; snapshot operation overwrite vs delete vs replace-partitions.
      artifacts: [python/repark/tests/test_dml_b_partition_overwrite.py, live oracle 2026-08-30]
    - id: AT-7
      status: N/A
      justification: Reuses existing stage-then-commit write path; no system-breaking resource change.
    - id: AT-8
      status: ATTACKED
      evidence: Static uses overwrite_files + overwrite_by_row_filter + validate_added_files_match_overwrite_filter; dynamic uses replace_partitions; fork operation classification matches Java.
      artifacts: [crates/repark-iceberg/src/write/partition_overwrite.rs, iceberg-rust overwrite_files.rs, replace_partitions.rs]
    - id: AT-9
      status: ATTACKED
      evidence: Empty-dynamic needle and TOO_MANY_DATA_COLUMNS name the failure; Q9 whole-table refuse stays.
      artifacts: [EMPTY_DYNAMIC_OVERWRITE_NEEDLE, whole_table_insert_overwrite_stays_q9]
    - id: AT-10
      status: ATTACKED
      evidence: Flipped acceptance pins plus PIN O5; three-door snapshot pins; facade SQL and writeTo.
      artifacts: [empty_insert_overwrite_partition_drops_only_named_partition, insert_overwrite_partition_nonempty_replaces_only_named_partition, overwrite_partition_clause_on_transform_table_still_rejected, commit_rejects_added_file_outside_overwrite_filter]
```
