# Charter ledger — RP-23 · fork pin 4151b488 consequences (F-TARGET-FILE-SIZE-1 fallout)

**Date:** 2026-09-17 · **Branch:** `chore/fork-pin-ice-20c-2` · **Base:** `5d54bca6`
**Model:** muse-spark-1.3-contributor · **Policy:**
[../../../AGENTS.md](../../../AGENTS.md) "Version-pin contract".
**Path:** STANDARD. **risk_tier: standard.**

**Retires:** this ledger moves to `../completed/` in this unit's last commit.

**Why now.** The branch moves the fork pin `96fc9f1f` → `4151b488` (fork #289
F-EVO-SCAN-1, #290 F-RTAS-OPS-1 opt-in, #288 F-TARGET-FILE-SIZE-1). #288 changes
three things RePark tests can see: `RollingFileWriter` rolls on 1000-row slices
when flushed + in-progress bytes reach the target (Java 1.11.0 `ROWS_DIVISOR` /
`ParquetWriter.length()`); the DataFusion write engine writes dictionary-OFF like
Spark's files; every catalog create stamps `write.parquet.compression-codec =
zstd` into the table properties (Java `TableMetadata.newTableMetadata`). Spark
4.1.2 + Iceberg 1.11.0 measured the codec stamp on CREATE and CTAS
(`ctas_codec_props`) and the file sizes (`tfs_*`); see
`/tmp/oc-worker/ic-build/write_fidelity_spark.json`. The facade suite on the
release native reports 26 failures (`/tmp/oc-worker/jc-bump/failed-26.txt`, assertion
text in `/tmp/oc-worker/run20c/jc-bump-gates-facade.log`), in three families below.
No product code in this unit: the fork is the fix, the tests record its consequences.

**Not in this unit:** the pin itself (landed); `Cargo.toml` / `Cargo.lock`;
STATUS.md; any engine change (a real delete-writer regression is a STOP, C-003).

## PROPOSITION LEDGER — RP-23-PIN-BUMP — 2026-09-17

| Clause | Proposition (checkable) | Proof obligation | Verdict | Evidence / open question |
|---|---|---|---|---|
| C-001 | Family A answers Spark: every table-property readback that now carries `write.parquet.compression-codec=zstd` records it — the 14 `write-properties` blocks in `python/repark/tests/_sql_harden_cutover_repark.py`, the 1 in `python/repark/tests/_v3_statement_coverage_repark.py`, `test_describe_table.py::test_describe_table_extended_sections`, and `test_ice_spark_table_1.py::test_spark_created_and_repark_created_metadata_shapes` (twin gains the key; the Spark-only delta shrinks to `owner`); on each recorded Spark fixture RePark now EQUALS Spark on that key. | The updated expectations; the SPARK halves already carrying the key (14/14 + 1/1); green runs of the four files. | **OPEN** | SPARK halves already record the codec row on every block; REPARK halves record it on none. Live s2 probe on the new pin shows the codec row present. Codec sort position mirrors the SPARK halves (after `write.merge.mode`). |
| C-002 | Family B re-derives the fixtures, never weakens the assertions: `test_target_file_size_applies_at_table` expects the Java answer for a 1-byte target over 20 000 rewritten rows; the C-011 seed still seeds exactly one file and it is still in the `[0.75, 1.8] × 64 KiB` band; every mw8 seed file is still in band and still 100 %-dead-rewritten. | The updated `(tiny, plain)` pair, `C011_ROWS`, `RUNBOOK_ROWS`; green runs of the three files. | **OPEN** | Measured on the new pin: tiny target over 20 000 rows → 20 files = 20000/1000 slices, plain → 4. Seed candidates at 64 KiB target: 1000 rows → 1 × 28096 B (below band); 2000 rows → 1 × 54445 B (in band); 2500 rows → 2 files. Partitioned seed per-partition: 2000 rows → 1 × ~54 KB in band; 3000 → 2000 + 1000; 4000 → 2000 + 2000. The roll lands on the 2000-row slice boundary: no roll at ~50 KB estimated, roll at ~100 KB estimated, the Java estimated-size check. |
| C-003 | Family C is fallout, not regression: s2/s7 MoR MERGEs still write ≥ Spark's floor of 2 delete files with the recorded kinds, the concurrency cap still moves the count down, s8/s9 COW MERGEs still write zero delete files; the only golden movement is the C-001 codec row. A measured count below the floor or a capped count above default is a STOP with evidence, never a masked edit. | `/tmp/probe_del.py` raw DEL counts; green runs of the six MERGE tests. | **PROVEN** | No STOP: raw DEL rows on the new pin are s2 3 PARQUET (capped 2), s7 3 PUFFIN (capped 2), s8/s9 0/0 — every floor holds (≥ 2), capped ≤ default with strict decrease where default > floor, kinds golden. The `-vv` diff shows the codec row as the sole probe movement. Full `test_sql_harden_cutover.py` green 2026-09-17 (34 passed, 15 live-skipped) after the C-001 golden update. |
| C-004 | Gates green on the release native: every file in `failed-26.txt`, plus `test_writer_v2.py`, `test_rewrite_data_files_options.py`, `test_maintenance_call.py`; the comment fence prints nothing before every commit; `git status --porcelain` empty at hand-back. | Pasted counts per file; the fence command output; the porcelain output. | **OPEN** | `maturin develop --release` exits 0 on this branch. |

VERDICT: 4 clauses, 0 PROVEN, 4 OPEN, 0 REJECTED.

## Red first

26 failures on the release native at the new pin, all three #288 consequences:
property readbacks gain one key (family A); mid-stream rolling moves file counts
and sizes (family B: `(20, 4)` vs `(16, 4)`, one-file seeds become two, one seed
file 27760 B below the 49152 B band floor); MERGE goldens move on the same key
(family C, counts diagnosed healthy in C-003).

## Gates

| Gate | Exit |
|---|---|
| `maturin develop --release` (python/repark, release native) | 0 |

## Coverage attestation

Attestation files when no clause is OPEN.
