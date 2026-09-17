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
| C-001 | Family A answers Spark: every table-property readback that now carries `write.parquet.compression-codec=zstd` records it — the 14 `write-properties` blocks in `python/repark/tests/_sql_harden_cutover_repark.py`, the 1 in `python/repark/tests/_v3_statement_coverage_repark.py`, `test_describe_table.py::test_describe_table_extended_sections`, and `test_ice_spark_table_1.py::test_spark_created_and_repark_created_metadata_shapes` (twin gains the key; the Spark-only delta shrinks to `owner`); on each recorded Spark fixture RePark now EQUALS Spark on that key. | The updated expectations; the SPARK halves already carrying the key (14/14 + 1/1); green runs of the four files. | **PROVEN** | All 15 REPARK blocks carry the codec row in the SPARK halves' sort position; twin EQUALS the Spark fixture on that key. The stamp was the sole open difference on all 14 sql-harden rows and `create-v3-properties`, so 15 verdicts join EQUAL (a bulk flip of 5 further v3 rows was measured still-DIVERGE and reverted). Files green 2026-09-17. |
| C-002 | Family B re-derives the fixtures, never weakens the assertions: `test_target_file_size_applies_at_table` expects the Java answer for a 1-byte target over 20 000 rewritten rows; the C-011 seed still seeds exactly one file and it is still in the `[0.75, 1.8] × 64 KiB` band; every mw8 seed file is still in band and still 100 %-dead-rewritten. | The updated `(tiny, plain)` pair, `C011_ROWS`, `RUNBOOK_ROWS`; green runs of the three files. | **PROVEN** | `(20, 4)` — 20 = one file per 1000-row slice at a 1-byte target, the Java answer. `C011_ROWS` 2000: one 54,445 B file in `[49152, 117965]`, one delete file with 2,000 records and exact bounds, full reclaim leg green. `RUNBOOK_ROWS` 4000: one ~54 KB in-band file per partition, the whole 10-test runbook file green. No assertion weakened: one seed file, in band, 100 % dead, rewritten. |
| C-003 | Family C is fallout, not regression: s2/s7 MoR MERGEs still write ≥ Spark's floor of 2 delete files with the recorded kinds, the concurrency cap still moves the count down, s8/s9 COW MERGEs still write zero delete files; the only golden movement is the C-001 codec row. A measured count below the floor or a capped count above default is a STOP with evidence, never a masked edit. | `/tmp/probe_del.py` raw DEL counts; green runs of the six MERGE tests. | **PROVEN** | No STOP: raw DEL rows on the new pin are s2 3 PARQUET (capped 2), s7 3 PUFFIN (capped 2), s8/s9 0/0 — every floor holds (≥ 2), capped ≤ default with strict decrease where default > floor, kinds golden. The `-vv` diff shows the codec row as the sole probe movement. Full `test_sql_harden_cutover.py` green 2026-09-17 (34 passed, 15 live-skipped) after the C-001 golden update. |
| C-004 | Gates green on the release native: every file in `failed-26.txt`, plus `test_writer_v2.py`, `test_rewrite_data_files_options.py`, `test_maintenance_call.py`; the comment fence prints nothing before every commit; `git status --porcelain` empty at hand-back. | Pasted counts per file; the fence command output; the porcelain output. | **PROVEN** | 2026-09-17 on the release native: batch one (describe, ice_spark_table_1, mw8, profiles1, sql_harden, v3_coverage, writer_v2, rewrite_options, maintenance_call) 195 passed, 99 live-skipped; `test_mw7_scale_smoke.py` 19 passed, 1 skipped. Zero failures. Fence printed nothing before all five commits; porcelain empty. |

VERDICT: 4 clauses, 4 PROVEN, 0 OPEN, 0 REJECTED.

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
| batch one: `test_describe_table.py test_ice_spark_table_1.py test_mw8_runbook.py test_profiles1_table_properties.py test_sql_harden_cutover.py test_v3_statement_coverage.py test_writer_v2.py test_rewrite_data_files_options.py test_maintenance_call.py` | 0 — 195 passed, 99 skipped (live oracle legs) |
| `test_mw7_scale_smoke.py` (full file) | 0 — 19 passed, 1 skipped |
| comment fence before every commit | prints nothing × 5 |
| `git status --porcelain` at hand-back | empty |

## Coverage attestation

```yaml
COVERAGE_ATTESTATION:
  pr_unit: rp-23-pin-bump
  complete: true
  reattested: []
  categories:
    - id: AT-1
      status: ATTACKED
      evidence: >
        All four clauses walked against behavior: the 15 codec readbacks against
        the recorded SPARK halves, the three re-derived fixtures against the band
        and the one-file assertions, the six MERGE rows against raw DEL counts and
        floors. Every file in failed-26.txt plus the three extra gate files ran
        green on the release native.
      artifacts: [python/repark/tests/test_sql_harden_cutover.py, python/repark/tests/test_v3_statement_coverage.py, python/repark/tests/test_mw7_scale_smoke.py, python/repark/tests/test_mw8_runbook.py]
    - id: AT-2
      status: ATTACKED
      evidence: >
        Slice boundaries actually measured: 1000 rows (1 x 28096 B, below band),
        2000 rows (1 x 54445 B, in band), 2500 rows (2 files), 3000/partition
        (2000 + 1000), 4000/partition (2000 + 2000), 5000 rows (3 files), the
        1-byte target over 20000 rows (20 files) against default (4).
      artifacts: [/tmp/probe_c011.py, /tmp/probe_mw8.py]
    - id: AT-3
      status: ATTACKED
      evidence: >
        No product code changed, so no new failure path exists; the full files
        re-ran rather than the 26 failing tests alone, so the refusal, error and
        floor pins beside the edited expectations re-verified green.
      artifacts: [python/repark/tests/test_sql_harden_cutover.py, python/repark/tests/test_mw8_runbook.py]
    - id: AT-4
      status: N/A
      justification: Expectation-only edits to single-threaded test goldens and integer constants; no shared state, ordering or concurrency surface.
    - id: AT-5
      status: N/A
      justification: No privileged action, no credential, no injection or deserialization surface in test goldens.
    - id: AT-6
      status: ATTACKED
      evidence: >
        The Spark-compatibility surface is the recorded oracle: RePark now EQUALS
        Spark on the stamp key in every recorded block, the verdict joins are
        mechanically re-checked by the verdicts tests, and the twin-vs-fixture
        shape test pins the Spark-only delta down to `owner`.
      artifacts: [python/repark/tests/_sql_harden_cutover_spark.py, python/repark/tests/test_ice_spark_table_1.py]
    - id: AT-7
      status: N/A
      justification: File counts are asserted behavior, not performance; no growth, loop or leak surface in this unit.
    - id: AT-8
      status: ATTACKED
      evidence: >
        The fork contract honored from the repo side: the pin row in
        docs/fork-sync.md names #288 as the behavior source, the Spark fidelity
        oracle confirms the stamp and the rolling counts, and nothing about the
        fork was presumed — every consequence re-measured on the release native.
      artifacts: [docs/fork-sync.md, /tmp/probe_del.py, /tmp/probe_c011_full.py]
    - id: AT-9
      status: N/A
      justification: No logging, metric or alarm surface in a test-expectations unit; failures surface as assertion diffs, unchanged.
    - id: AT-10
      status: ATTACKED
      evidence: >
        Every PROVEN clause is cited from python/repark/tests/map.md; the
        verdict-join tests would catch a corrupted golden mechanically, and the
        manual join audit did catch the over-broad first v3 flip (5 rows
        reverted to DIVERGES after measuring their joins).
      artifacts: [python/repark/tests/map.md, python/repark/tests/test_sql_harden_cutover.py::test_sql_harden_verdicts_match_the_committed_halves]
```
