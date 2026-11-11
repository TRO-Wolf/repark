# Item 3: after-tables (base `68b09d18` release build against head `015fda5ae` release build)

Legs re-run with the committed probes (`<scratch>/sz-runners/legs.sh`); labels by `item1/labels.py`; raw head legs in
[raw/](raw/). Every count is cells.

| Leg | Moves |
|---|---|
| Domain, clause rows | D1 PRUNED-WRONG→EQUAL 15, WRONG-WRITE→EQUAL 1; D2 →EQUAL 21; D3 WRONG-READ→EQUAL 5; D4 →EQUAL 47; D5 →EQUAL 63; D6 WRONG-READ→EQUAL 2, WRONG-READ→WRONG-READ (names) 3, REFUSED→WRONG-READ (names) 1; D7 PRUNED-WRONG→EQUAL 55, WRONG-WRITE→EQUAL 16; D13 REFUSED→EQUAL 2 |
| Domain, D7 refusals | PRUNED-WRONG→REFUSED 3 (`D7-update-2-v3`, `-4-cow`, `-7-v3`, zsplit) and EQUAL→REFUSED 1 (`D7-update-8-v3`), all `Mismatch between schema and batches` (`UPDATE-SCHEMA-BATCH-MISMATCH-1`) |
| Domain, outside the clause, to Spark's outcome | D12 PRUNED-WRONG→EQUAL 4 (`D12-{select,delete}-{cow,mor}`, partition `d`), D8-8 2 (`CASE d WHEN 0.0`, spark-sql REFUSED→EQUAL, native WRONG→EQUAL), D9-25 PRUNED-WRONG→EQUAL 1 |
| Additions | A1 →EQUAL 50; A2 →EQUAL 13 (native and Spark door); A3 →EQUAL 28 (10 of them were refusals, F2) |
| Table B | c1, c2, c4, c5, c6 WRONG-READ→EQUAL 16 each; c3 →EQUAL 12; m2, m3 WRONG-WRITE→EQUAL 6 each; c7, p15 rows unchanged in label |
| Extension | 56 of 58 cells change answer; 3 `E1-dml-update` cells (f1-v3, f2-v2cow, f2-v3, f5-v2cow, f5-v3 refuse in this run) go from the committed one-row write to `UPDATE-SCHEMA-BATCH-MISMATCH-1`; E6 REFUSED→ANSWERED |
| p15 (envelope) | 6 keyed cells differ in post-state; not yet labelled against the truth (successor) |

**`UPDATE-SCHEMA-BATCH-MISMATCH-1`, 50 repeats per kind** (`UPDATE … SET flag = 'upd' WHERE d <=> 0.0` on zsplit,
[schema_batch_repeat.py](schema_batch_repeat.py), `raw/repeat-*.json`): base refused v2-cow 27, v2-mor 0, v3 29;
head refused v2-cow 22, v2-mor 0, v3 30. The per-statement rate is unchanged. Every float-zero UPDATE over the
two zero-only files now reaches the same path, because it now matches both files.

#1292's, #1295's, unit C's and #1290's tables are replayed cell by cell by their committed pins (the Rust and
facade suites): green in debug and in the release facade run.
