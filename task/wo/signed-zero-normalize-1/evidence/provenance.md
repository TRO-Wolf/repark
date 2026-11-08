# Provenance: the signed-zero domain table (SIGNED-ZERO-NORMALIZE-1, sketch section 9)

Model `claude-haiku-5-5` (Haiku 5.5, clerk grade). Records and measurement only; no product code; no frontier
critic. The legs were run on 2026-11-08 between 13:11 and 13:32 local time (box load 5 to 11 on a
64-core box). The final unit (`signed-zero-legs4`) produced every raw file in `raw/`; earlier runs were discarded
after the defects listed under "Deviations" were fixed, and only the final raw files are kept.

## Versions and inputs

| Item | Value |
|---|---|
| Spark oracle | Spark 4.1.2 on OpenJDK 17.0.15 (zulu-17), `local[1]`, session zone `America/New_York` |
| Iceberg runtime | `org.apache.iceberg_iceberg-spark-runtime-4.1_2.13-1.11.0.jar`, sha256 `d6ea6c5d099288daeb7d5a92061bd3d7d8f296492632b42378e5f2f0e3066242` |
| RePark leg | release wheel built from `fdc61f26`: `repark-1.5.5-cp312-abi3-manylinux_2_39_x86_64.whl`, sha256 `07f94d4dcf9852b7a6f895382efe4fdc3bbb6c1d090c807e15d9191850ead479`, repark 1.5.5 |
| RePark runtime | pyarrow 26.0.1, Python 3.12.3, native config `repark.sql.allowCreateFormatVersion3=true` |
| Facade memory knob | `repark.memory.limit.gb`: unset except in the D16 facade leg (`1`) |
| Native memory knob | `PyReparkSession.native(memory_limit_gb=1)` in the D16 native leg only |
| Spark spill session | `spark.memory.fraction=0.01`, broadcast join off, sort-merge preference off, adaptive execution off (see deviations) |
| Probe | `probe_signed_zero.py` in this directory; `--part reads|dml|timing|spill|spill-facade|spill-native` |

## Legs and commands

Each leg was run as `python -I probe_signed_zero.py --engine E --part P --label L --warehouse W --out raw/F`, under a
systemd user unit that ran the legs in this order: `spark-reads`, `spark-dml`, `spark-spill`, `repark-reads`,
`repark-dml`, `repark-timing`, `repark-spill-facade`, `repark-spill-native`. The Spark JVM was never run alongside
another Spark JVM, and the RePark timing leg ran with no other leg active. The render is `--render --raw-dir raw
--out-dir .`.

| Raw file | Legs | Records |
|---|---|---|
| `raw/spark-signed-zero-reads.jsonl` | D2 to D6, D8 to D10, D13, D17 (reads on the view, one-file and split layouts) | 602 cells, 600 answered, 2 refused |
| `raw/spark-signed-zero-dml.jsonl` | D1, D7, D11, D12 (DML on the zone or plain truth layout and the split or partitioned test layout) | 222 cells, all answered |
| `raw/spark-signed-zero-spill.jsonl` | D16, Spark leg | 3 queries, executed-plan spill metrics |
| `raw/repark-main-signed-zero-reads.jsonl` | the same read groups on both doors | 533 cells, 480 answered, 53 refused |
| `raw/repark-main-signed-zero-dml.jsonl` | the same DML groups | 177 cells: 132 answered, 1 refused, 44 not expressible |
| `raw/repark-main-signed-zero-timing.jsonl` | D15, RePark facade only | 2 timed queries, 5 runs each |
| `raw/repark-main-signed-zero-spill-facade.jsonl` | D16, facade at `repark.memory.limit.gb=1` | 3 queries, plan counters |
| `raw/repark-main-signed-zero-spill-native.jsonl` | D16, native at `memory_limit_gb=1` | 3 queries, plan counters |

Setup records (table creation and seeding) are in each file; a failed setup marks the dependent cells SETUP-REFUSED.
The only setup failures were the D15 and D16 seeds, which were defects in the first two runs and are fixed in the
final run (no SETUP-REFUSED remains in the final raw files).

## Truth, Spark and RePark columns

- **Truth.** For read cells the truth is Spark SQL on the same rows as an inline `VALUES` view (layout `view`, Spark
  only). No storage, no pruning: this is the sketch's "Spark SQL semantics". The sketch's alternative (Spark's
  single-file Iceberg read or a direct Parquet read, E4) was not used for the truth; the `one` layout (one-file
  Iceberg, Spark and RePark) is kept as a control column, so a difference between the view and one-file Iceberg would
  show up there.
- **Truth for DML.** Spark on the `zone` layout (rows 4 and 6 in one data file) or `plain` (rows 1, 4 and 6 in one
  file) or `m14`: a single data file whose statistics do not exclude the predicate. The `zsplit`, `part` and `m1s4`
  layouts are the test layouts: zero-only data files (rows 4 and 6 in separate files), a partitioned table on `d`,
  and a one-row zero-only file respectively.
- **Spark column.** Spark on the test layout, on the route the RePark row used (`native` is compared with Spark's
  `spark-sql` route, because Spark has no native door).
- **RePark-main column.** The release wheel on the test layout.

## Label rules (as coded in `label_of`)

Checked in order. Outcome first, then rows against the truth.

1. PANIC, NOT-EXPRESSIBLE, SETUP-REFUSED are reported as such.
2. Truth missing or setup-refused: ORACLE-MISSING.
3. Both refused: BOTH-REFUSED. RePark answered and Spark refused: ACCEPTED-WHERE-SPARK-REFUSED. RePark refused and
   Spark answered: NATIVE-REFUSED on the native route, otherwise REFUSED-WHERE-SPARK-ACCEPTED.
4. Both answered, rows differ: SIGN-ONLY if the rows agree once `-0.0` is read as `0.0`. Then PRUNED-WRONG if
   the Spark-on-Iceberg answer on the same test layout also differs from the truth and RePark's answer equals it, or
   if the record is a Spark row on a test layout (`split`, `zsplit`, `part`, `m1s4`). Otherwise WRONG-READ (reads) or
   WRONG-WRITE (DML).
5. Rows equal but order differs on an ordered cell: ORDER-DIFFER.
6. Rows equal, columns differ: WRONG-READ (names) only when no other label applies.
7. Otherwise EQUAL.

PRUNED-WRONG is assigned by rule 4. It means "Spark-on-Iceberg, on a test layout, disagrees with the truth, and
RePark agrees with Spark's wrong answer". It does not isolate the pruning mechanism cell by cell; the sketch's
mechanism (the min and max statistics of a zero-only file, E1 and E3) explains the groups it was measured on.

## Deviations from the brief and the sketch

1. **Sketch copy.** Two occurrences of the plural form of the word DELETE were spelled "DELETE statements" so the spell gate passes (Part 1).
2. **D17-6.** The sketch's `array_union(array(d), array(0.0))` is a Spark type error (DOUBLE against DECIMAL); the
   cell is `array(CAST(0.0 AS DOUBLE))`.
3. **D11 sign.** A bare `-0.0` literal in Spark SQL is a DECIMAL and has no sign, so `SET d = -0.0` stores `0.0` on
   Spark. The sketch's cell is kept as written; a `CAST('-0.0' AS DOUBLE)` variant is added to test negative-zero
   storage.
4. **Native door.** The native door refuses `TBLPROPERTIES`, so it creates v2 CoW only. The MoR and v3 native
   rows are NOT-EXPRESSIBLE (44 rows), not run.
5. **Native oracle.** Spark has no native door; the native row's Spark column is Spark's `spark-sql` answer on the
   same layout.
6. **D16 Spark EXPLAIN.** `EXPLAIN ANALYZE` is a syntax error in Spark 4.1 SQL (recorded as REFUSED with the
   ParseException text). The Spark spill is read from the executed plan's `spillSize` metrics instead. Adaptive
   execution is off on the Spark spill leg only, because the adaptive wrapper hides its children from the walk.
7. **D16 RePark.** `EXPLAIN ANALYZE` works on both RePark doors; the counters are read from the plan text.
8. **D15.** RePark only, as the brief says. The join count of 5,000,000 is RePark's answer. The IEEE expectation from
   the seed is 10,000,000 (5,000,000 zero rows, each matching both `u` rows); that expectation is arithmetic, not a
   Spark measurement. Box load was about 8.2 (one-minute average) at the start of the timing leg.
9. **D14** was skipped (a pin). **D1** Rust test was skipped (the build lane's). **D12** metadata
   (`selects_partitions`) was not read; only the answers and the state are recorded.
10. **Missing evidence not closed.** E2 (a Spark-written table read by RePark) was not attempted: no shared catalog.
    E4 (the direct Parquet read, RePark side, and the bloom-filtered file) was not run. E9 (Trino) is ORACLE-MISSING:
    no Trino image was pulled. E8 spill and E5 join count are closed by D16 and D15.
11. **Probe defects fixed before the final run.** The first runs had a seed insert with three values against a
    seven-column table (D15 and D16 empty); the RePark plan was read from its first column only; the spill counter
    filter matched output-row digits; the Spark spill walk ran with adaptive execution on. All four are fixed in the
    final probe, and the final raw files are from the final probe.
12. **Recursive CTE (D9-23).** Spark 4.1 refuses a UNION inside a recursive CTE (UNION_NOT_SUPPORTED_IN_RECURSIVE_CTE).
    The RePark native door answered or refused as recorded; Trino was not run.

## Observed intermittent refusal

`UPDATE ... WHERE d <=> 0.0` through the facade on the `zsplit` layout refused with
`Mismatch between schema and batches` in four of the five full runs (v3 in runs 1, 2, 3 and the final run; v2 CoW
in runs 2 and 3). The v2 MoR and native answers were correct in every run. Twelve isolated repeats on fresh v2 CoW
tables answered correctly. Carded as UPDATE-SCHEMA-BATCH-MISMATCH-1 in `cards.md`, with the run table. The first
hand-back said the refusal occurred in two runs; the card corrects that.

## Truth accepted

The truth for read cells is Spark SQL on an inline `VALUES` view of the same rows (no Iceberg storage, no pruning).
The orchestrator accepted this in place of Spark's direct Parquet read. E4 (the direct Parquet read, RePark side,
and the bloom-filtered file) is listed as NOT RUN.

## Spill status

At 1 GB, RePark (facade and native) showed `spill_count=0` and `spilled_bytes=0.0 B` on every operator for the E8
shape. The RePark spill path is therefore NOT exercised by this measurement. The build's table must force a spill
(a smaller limit, or more rows). Spark's spill was measured with adaptive execution off, on the Spark spill leg only.

## Render change after the legs

The D12 GROUP BY rows are SELECT statements that carry the DML family in the raw records, so they were labelled
WRONG-WRITE. The renderer now treats a SELECT statement as a read. This is a render-only change: no leg was re-run,
and the tables and summary were re-rendered from the existing raw files. Counts moved from WRONG-WRITE 20 to 18 and
WRONG-READ 174 to 176 (the two D12 GROUP BY rows, RePark, cow and mor).

## Row-order and message variance

RePark's scan order and some RePark error messages vary between runs (the cast error names whichever overflowing
value is reached first). The comparison is on sorted row sets, so row order does not change a label. `collect_set`
arrays (D17-1) are compared as sets: the element order is unspecified by Spark. Checked by diffing the first and the
second run after normalising row order: Spark DML had zero semantic changes; Spark reads changed only in D17-6 (the
fixed cell); RePark reads changed only in D17 array order and the D17-6 fix; the RePark DML change is the one
intermittent refusal above.
