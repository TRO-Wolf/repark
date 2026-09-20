# Unit ledger — ICE-MERGE-APPEND-1 · an INSERT commits through `merge_append`, as Spark's `newAppend()` does

**Date:** 2026-09-19 · **Branch:** `fix/ice-merge-append-1` · **Base:** `main` `859c6506` ·
**Model:** claude-opus-5 · **Policy:** [../../../AGENTS.md](../../../AGENTS.md).
**Path:** STANDARD. **Rubric:** STANDARD. `risk_tier: standard`.
**Campaign:** the 1-to-1 Spark–Iceberg parity campaign, IPI-11 (merge-on-commit).
**Fork pin:** `44834673` (`MergeAppendAction` present since the fork landed
`crates/iceberg/src/transaction/merge_append.rs`).

**Retires:** this ledger moves to `../completed/` when the unit's last commit lands.

**Clause numbering.** The brief's clauses `C-1`..`C-8` are this ledger's `C-001`..`C-008`,
one-to-one and in order (the repo's ledger-grammar gate parses `C-NNN`). `C-009` and `C-010`
are added by this unit: the fork-side INSERT-path finding and the no-dependency-move guard.

**Why now.** Recorded 2026-09-19, 120 sequential single-row `INSERT`s into one v2 table:
Spark 4.1.2 + Iceberg 1.11.0 merges the manifest list on commit
(`/tmp/oc-worker/qa/merge_append_truth.json` — defaults collapse 99 manifests into 1 at the
100th append, `commit.manifest.min-count-to-merge=5` keeps the table between 1 and 4
manifests, `commit.manifest-merge.enabled=false` never merges), while RePark at `main`
produces one manifest per append in ALL THREE variants (120 → 120) and honours neither
property. Manifest count per append is the read-planning cost of every subsequent scan.

**Not in this unit:** the `manifests-created` / `-kept` / `-replaced` snapshot summary keys
(fork ask, see C-006); the fork pin bump; manifest compaction procedures
(`rewrite_manifests`); delete-manifest merging (the fork's named deviation); `STATUS.md`
lifecycle rows.

## 1. The mapping — which RePark commit site is Spark's `newAppend()` (C-001)

**Evidence basis.** Bytecode of the artifact the truth file was recorded against,
`org.apache.iceberg:iceberg-spark-runtime-4.1_2.13:1.11.0`
(`/tmp/oc-worker/ice-rating/scratch/.ivy2/jars/`), disassembled with `javap -p -c`. Java's
`Table.newAppend()` is the MERGING producer and `newFastAppend()` the non-merging one; the
fork's `merge_append.rs` module doc states the same contract.

| Spark write op (class) | producer in bytecode | merges? |
|---|---|---|
| `SparkWrite$BatchAppend.commit` | `Table.newAppend()` | **yes** |
| `SparkWrite$StreamingAppend.commit` | `Table.newFastAppend()` | no |
| `SparkWrite$DynamicOverwrite.commit` | `Table.newReplacePartitions()` | n/a |
| `SparkWrite$OverwriteByFilter.commit` | `Table.newOverwrite()` | n/a |
| `SparkWrite$CopyOnWriteOperation.commit` | `Table.newOverwrite()` | n/a |
| `SparkWrite$StreamingOverwrite.commit` | `Table.newOverwrite()` | n/a |
| `SparkPositionDeltaWrite$PositionDeltaBatchWrite.commit` | `Table.newRowDelta()` | n/a |

CTAS/RTAS writes through `StagedSparkTable`, whose `Table` is
`BaseTransaction$TransactionTable`; that class's `newAppend()` forwards to
`BaseTransaction.newAppend()`, which the bytecode shows constructing
`org.apache.iceberg.MergeAppend` (and `newFastAppend()` constructing
`org.apache.iceberg.FastAppend`). **So a CTAS append is a merging append too.**

So: **every batch append in Spark merges; only the structured-streaming micro-batch append
does not.** RePark has no streaming writer (streaming is v1.6.0), so RePark has **no**
`newFastAppend()` site — every one of its append commit sites is a `newAppend()` site.

| RePark commit site | reached by | Spark counterpart | disposition |
|---|---|---|---|
| `crates/repark-iceberg/src/write/append.rs::commit_append` | public `write::append()`, native-door CTAS (`repark-sql/src/create_table.rs:452`), Spark-door CTAS without write options (`repark-spark/src/ctas.rs:548`) | `BatchAppend` (via `TransactionTable.newAppend()` for CTAS) | **route to `merge_append`** |
| `crates/repark-iceberg/src/write/commit_target.rs::commit_append_to` | `INSERT INTO … BY NAME` (`repark-spark/src/insert_by_name.rs:188`), branch-targeted | `BatchAppend` | **route to `merge_append`** |
| `crates/repark-iceberg/src/write/write_options.rs::commit_append_with_summary` | `INSERT` with statement write options (`repark-spark/src/append_with_options.rs:77`), `append_with_statement_options`, CTAS with options (`repark-spark/src/ctas.rs:581`) | `BatchAppend` | **route to `merge_append`** |
| `crates/repark-spark/src/ctas.rs:337` (staged-table append) | CTAS/RTAS through a staged table | `StagedSparkTable` + `BatchAppend` → `BaseTransaction.newAppend()` | **route to `merge_append`** |
| `overwrite_files()` sites (`commit_replace_write*`, `commit_overwrite_replace_all*`, `partition_overwrite`) | `INSERT OVERWRITE`, RTAS | `OverwriteByFilter` / `DynamicOverwrite` | leave — not an append producer |
| `write/merge/snapshot_commit.rs` (MERGE/UPDATE/DELETE) | `MERGE INTO`, `UPDATE`, `DELETE` | `PositionDeltaBatchWrite` / `CopyOnWriteOperation` | leave — not an append producer |

**The finding (C-009).** The commit site that produced the measured RePark series is **not**
in this repository. A bare `INSERT INTO cat.ns.t VALUES (…)` on either SQL door falls through
to DataFusion (`repark-spark/src/router.rs:279` → `passthrough_after_p11`), and DataFusion
plans it on the fork's provider. Measured on this tree (`EXPLAIN INSERT INTO …`):

```
physical_plan  IcebergCommitExec: table=ns.t
                 IcebergWriteExec: table=ns.t
```

`IcebergCommitExec`'s `InsertOp::Append` arm commits through `tx.fast_append()`
(fork `crates/integrations/datafusion/src/physical_plan/commit.rs:362`), and both
`IcebergWriteExec` and `IcebergCommitExec` are `pub(crate)` in the fork — RePark cannot
reuse or replace them. The facade's `df.write.insertInto(…)` /
`saveAsTable(mode="append")` lower to the same bare `INSERT INTO`, so they share the gap.
This half is a fork ask; see C-009 for its registry row and xfail.

## PROPOSITION LEDGER — ICE-MERGE-APPEND-1 — 2026-09-19

| Clause | Proposition (checkable) | Proof obligation | Verdict | Evidence / open question |
|---|---|---|---|---|
| C-001 | The mapping above is right: every RePark append commit site is Spark's `newAppend()` (merging); RePark has no `newFastAppend()` site; no overwrite/row-delta site is an append. | `javap -p -c` over the 1.11.0 runtime jar for every `SparkWrite$*`/`SparkPositionDeltaWrite$*` commit method plus `BaseTransaction`; `grep` over `crates/` for every `fast_append()` call. | **OPEN** | §1. |
| C-002 | With defaults, the RePark-owned append path reproduces Spark's `defaults` series at every probe point (1, 5, 20, 50, 99, **100 → ONE manifest**, 101, 110, 120) for manifest count and data-file count. | Series pins replaying the fixture. | **OPEN** | |
| C-003 | `commit.manifest.min-count-to-merge=5` is honoured: the `min_count_5` series matches Spark at every probe point. | Series pins. | **OPEN** | |
| C-004 | `commit.manifest-merge.enabled=false` reproduces the `merge_disabled` series (120 appends → 120 manifests) — byte-for-byte what `main` does today. | Series pins. | **OPEN** | |
| C-005 | A merging commit changes nothing else: delete manifests carry forward untouched on a MoR table, `_row_id`/`_last_updated_sequence_number` lineage and sequence numbers survive a merge, multi-spec tables never merge across spec ids, branch writes still target their ref, and the concurrent-append pins still hold. | The named regression checks. | **OPEN** | |
| C-006 | The `manifests-created` / `-kept` / `-replaced` summary keys are DECLARED, not faked: a registry row naming the fork ask and a strict xfail that flips when the fork lands. | Registry row + strict xfail. | **OPEN** | |
| C-007 | The cost of merging is measured: 120 sequential appends timed before and after on the same head, both numbers recorded. | The timing pair. | **OPEN** | |
| C-008 | The pins are mutation-proof: routing back to `fast_append` reds C-002 and C-003; ignoring `commit.manifest-merge.enabled` reds C-004. | Two recorded, reverted mutations. | **OPEN** | |
| C-009 | The bare `INSERT INTO` path commits fork-side and cannot be routed from this repository at pin `44834673`; the gap is DECLARED with a registry row naming the fork ask. | The `EXPLAIN` measurement, the fork source, the `pub(crate)` visibility, the registry row. | **OPEN** | |
| C-010 | No dependency moves: `git diff main -- Cargo.toml Cargo.lock` is empty at hand-back. | The diff. | **OPEN** | |

VERDICT: 10 clauses, 0 PROVEN, 10 OPEN, 0 REJECTED.
