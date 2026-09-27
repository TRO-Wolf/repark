# Unit ledger — R-FILEORDER-2 · the owned `INSERT OVERWRITE` reads its source in Spark's file order

**Date:** 2026-09-27 · **Branch:** `fix/r-fileorder-2` · **Base:** `5daecc43`
(`origin/main`) **Model:** Muse Spark (`muse-spark-1.3-contributor`) · **Policy:** [../../../AGENTS.md](../../../AGENTS.md).
**Path:** STANDARD. **risk_tier: standard.**

**Retires:** this ledger moves to `../completed/` when the unit's last commit lands.

**Why now.** Scoreboard cell `L-INSERT-OVERWRITE` (cells_extra.py `lin`, group
V3-LINEAGE) replays DIFFERENT on `obs.lineage`: the recorded Spark answer is
`[[2,b,5,3],[3,c,6,3],[4,d,4,3]]`, RePark usually answers `[[2,b,4,3],[3,c,5,3],[4,d,6,3]]`
and rarely EQUALs (1 run of 6 in row-lineage-order-1 R-4; 1 run of 5 on the main wheel).
The cell is unpartitioned `INSERT OVERWRITE … SELECT` on the owned overwrite path
(`router.rs` `execute_insert_overwrite` → `insert_overwrite.rs` stage-then-swap → the
fork `commit_replace_partitions_with_summary` / `commit_overwrite_replace_all_with_summary`),
which never reaches the fork's DataFusion `IcebergCommitExec` or the `DataFileCommitOrder`
hook, and `fanout_order.rs` is a no-op for 0–1 partition keys. Ruling R1 ordered Spark
measured first: three runs, and STOP with no code change if Spark's own lineage order
differs between runs. It does: the same-JVM triple agrees (C-001) but six fresh-JVM runs
split 2–4 (C-002), each scan planning an independent file order inside one combined task
(C-003). This unit HALTs per R1; the dated carve-out (C-5) is the orchestrator's (C-004).

## Clauses

| Clause | Statement | Proof obligation | Verdict | Evidence |
|---|---|---|---|---|
| C-001 | Spark 4.1.2 answers the cell `[[2,b,5,3],[3,c,6,3],[4,d,4,3]]` on three same-JVM runs with a fresh table each run. | R1 triple, banner-quoted, lineage `_row_id` per row plus the snapshot layout each run. | PROVEN | `target/probe-fileorder-2/spark-run{1,2,3}.json`; banner `spark=4.1.2 tz=UTC`; one output file, row order 4,2,3, `_row_id` 4,5,6. |
| C-002 | Spark's own lineage order differs across six fresh-JVM runs: runs 1 and 4 answer `{2:5,3:6,4:4}`, runs 2, 3, 5 and 6 answer `{2:4,3:5,4:6}`. | Six fresh sessions, same statements, `_row_id` per row each run. | PROVEN | `target/probe-fileorder-2/spark-partitions.json` runs 1–6; the R1 STOP condition is met. |
| C-003 | The order is one combined task's file order, drawn per plan: every pre-scan runs on partition 0, the pre-scan and the overwrite-scan disagree inside runs 1 and 2, the plan carries no sort or exchange, and the catalog's FileIO is in-memory. | Partition ids plus file names pre-overwrite; EXPLAIN; FileIO class from the live session. | PROVEN | `target/probe-fileorder-2/spark-partitions.json` (`pre_partitions` vs `post_sorted`) and `rule-stdout.log` (`OverwriteByExpression` → `Filter` → `ColumnarToRow` → `BatchScan`; `table_io: InMemoryFileIO`). |
| C-004 | `L-INSERT-OVERWRITE` is a dated carve-out (C-5): R3's premise (Spark reads planned splits in file order) is refuted by C-002/C-003, no deterministic RePark order equals Spark run to run, and no code change is the correct outcome. | Orchestrator ruling on this HALT's measurements. | OPEN (the carve-out is recorded orchestrator-side) | This HALT hand-back; no source file touched. |

## Measurements (live oracle, recorded verbatim)

Banner from the live session: `BANNER spark=4.1.2 tz=UTC`.

R1 triple (one JVM session, fresh table per run; sorted lineage):

- run1: `[['2','b','5','3'],['3','c','6','3'],['4','d','4','3']]`
- run2: `[['2','b','5','3'],['3','c','6','3'],['4','d','4','3']]`
- run3: `[['2','b','5','3'],['3','c','6','3'],['4','d','4','3']]`

Fresh-JVM six (new session per run; `pre` = `SELECT spark_partition_id(),
input_file_name(), id … WHERE id > 1` raw order, file names shortened to their
leading `00000-N` task index; `post` = sorted `SELECT id, _row_id` after overwrite):

- run1: pre `[2,3,4]` on partition 0; post `[['2','5'],['3','6'],['4','4']]`
- run2: pre `[4,2,3]` on partition 0; post `[['2','4'],['3','5'],['4','6']]`
- run3: pre `[4,2,3]` on partition 0; post `[['2','4'],['3','5'],['4','6']]`
- run4: pre `[4,2,3]` on partition 0; post `[['2','5'],['3','6'],['4','4']]`
- run5: pre `[2,3,4]` on partition 0; post `[['2','4'],['3','5'],['4','6']]`
- run6: pre `[4,2,3]` on partition 0; post `[['2','4'],['3','5'],['4','6']]`

The pre-scan and the overwrite write order disagree inside run1 and run2, so each scan
plans its file order independently. A 4-file variant (one row per file, two runs) wrote
`[4,2,3]` both times — newest file first, the rest in file order — which fits no file-order
rule and is reported as noise, not emulated. The lane's probe scripts and full JSON sit
under `target/probe-fileorder-2/` (untracked; the numbers above are the verbatim record).

## Residues

| # | Residue |
|---|---|
| R-1 | Dated 2026-09-27 (owned by the orchestrator's C-5 carve-out): `L-INSERT-OVERWRITE` cannot replay EQUAL deterministically while Spark's own answer flips run to run; re-measure against a future Spark before lifting the carve-out. |
| R-2 | Dated 2026-09-27 (report-only): the scoreboard `sc` catalog is `InMemoryCatalog` with `InMemoryFileIO` — table bytes never reach disk, so per-file row order is measurable only through Spark SQL (`input_file_name()`), never through the warehouse tree. |
