# Cards: open items from the signed-zero domain table

Carded in this round. No new measurement is made here; each card states what the measurement already shows and what
stays open.

## UPDATE-SCHEMA-BATCH-MISMATCH-1

**Statement.** `UPDATE {dm} SET flag = 'upd' WHERE d <=> 0.0` on a RePark zero-only layout (`zsplit`: rows 4 and 6 in
separate data files), through the facade door (`spark-sql`). Cell D7-update-8.

**Exact error text.** `AnalysisException: Error during planning: Mismatch between schema and batches`

**Occurrences in the five full legs** (facade door, `zsplit`):

| Full run | Leg set | v2 CoW | v2 MoR | v3 |
|---|---|---|---|---|
| 1 | first run (original probe) | answered | answered | refused |
| 2 | `signed-zero-legs2` | refused | answered | refused |
| 3 | `signed-zero-legs3` | refused | answered | refused |
| 4 (final) | `signed-zero-legs4`, the raw in `raw/` | answered | answered | refused |

The native door answered the same statement on v2 CoW in all five runs. Every other D7 UPDATE and DELETE cell
answered in the final run.

**Clean repeats.** Twelve isolated repeats on fresh v2 CoW tables, in one process, through the facade: all twelve
answered, and both zero rows were updated (state `upd`, `upd`). No repeat was made on v3 or in the full-leg context.

**Build lane.** The build lane repeats it 50 times on each kind before and after its change.

**Correction.** The first hand-back said the refusal occurred "once in each of two full runs". That undercounted it.
The table above lists every full run, and the facade refusal recurred in four of five.

**Status.** Carded. Not explained. The RePark spill path is a separate open item (see `provenance.md`).

## E2-MISSING-1

**Missing evidence.** E2: a Spark-written table read and written by RePark.

**Why it is missing.** No catalog that both engines open exists without new setup. The RePark memory catalog
registered on Spark's warehouse root cannot see a Spark Hadoop-catalog table (the extension run reported
`Error during planning: table ... not found`).

**Decision.** Not pursued (orchestrator ruling). It is confirmation, not a blocker, per the sketch (section 9).

## E9-MISSING-1

**Missing evidence.** E9: the Trino spellings. The oracle is ORACLE-MISSING for every E9 and D9-23 native cell that
needs it.

**Why it is missing.** The Trino image was not pulled; the owner did not choose the image pull (sketch P5).

**Decision.** Not pursued (orchestrator ruling).
