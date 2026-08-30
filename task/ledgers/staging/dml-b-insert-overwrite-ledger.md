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
| C-001 | Static `INSERT OVERWRITE … PARTITION (k=v, …)` (every listed field an equality) commits through `Transaction::overwrite_files().overwrite_by_row_filter(k=v AND …)` plus `validate_added_files_match_overwrite_filter`. Identity partition fields only. Sibling partitions stay byte-stable. Values and Arrow types match live PySpark 4.1.2 + Iceberg 1.11.0 on the Spark SQL door, the ANSI SQL door (PARTITION form), and the facade `.sql()` path. Snapshot operation is Iceberg `overwrite` when files are added, `delete` when the source is empty and the partition had files. | Three-door pins + `$snapshots.operation` + sibling-row identity. Red-first in PROGRESS.md. | OPEN | Current engine pin refuses every PARTITION form. |
| C-002 | Dynamic `INSERT OVERWRITE … PARTITION (k, …)` (no values) commits through `Transaction::replace_partitions()` with `add_file` / `add_files`. Snapshot summary carries `replace-partitions=true` and operation `overwrite`. Partitions absent from the source stay byte-stable. Same three doors, values and Arrow types. | Three-door pins + summary property + sibling identity. | OPEN | Fork `ReplacePartitions` is already on the pin. |
| C-003 | `DataFrameWriterV2.overwritePartitions()` (and `overwrite_partitions`) is Spark's dynamic partition overwrite: it replaces only partitions present in the source. Same snapshot stamp as C-002. `overwrite(condition)` stays refused. | Facade pins, both spellings; target rows outside source partitions unchanged. | OPEN | Today both spellings raise `UnsupportedOperationException`. |
| C-004 | Empty-input **dynamic** overwrite refuses loud in the engine (Spark does this engine-side). It never commits a silent full-table wipe. Empty-input **static** overwrite is partition-scoped (the named partitions go empty; siblings remain). | Negative pin for empty dynamic; empty static is C-001 / C-005. | OPEN | Whole-table empty overwrite still wipes; PARTITION empty currently refuses. |
| C-005 | Acceptance pins `empty_insert_overwrite_partition_refuses_full_wipe` and `insert_overwrite_partition_nonempty_refuses_whole_table_replace` flip from refuse-loud to partition-scoped success on a partitioned fixture. No other expected-value repin. Transform-table `PARTITION (id = 1)` (PIN O5) stays refused. | The two named tests plus PIN O5 still red on a revert of the behavior. | OPEN | Both pins currently `expect_err` on an unpartitioned table. |
| C-006 | Documents match the pins: registry DML-1, iceberg-guide write-forms list, maps in lockstep. ANSI whole-table `INSERT OVERWRITE` stays Q9-omitted; PARTITION forms are this unit's new surface on both SQL doors. | Registry + guide + map diffs; Q9 whole-table refuse pin still green. | OPEN | Closes on departure. |

VERDICT: OPEN — 6 clauses, 0 PROVEN, 0 REJECTED.

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
