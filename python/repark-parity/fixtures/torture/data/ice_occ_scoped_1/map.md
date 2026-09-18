# map — fixtures/torture/data/ice_occ_scoped_1 (Spark oracle: concurrent DML commit storms)

## Purpose

The measured Spark 4.1.2 answers for ICE-OCC-SCOPED-1 (rating row V2-20a / residue DML-5): how
many of N concurrent Iceberg writes commit, what the losers raise, and the rows left behind.
Recorded 2026-09-17 on PySpark 4.1.2 + `org.apache.iceberg:iceberg-spark-runtime-4.1_2.13:1.11.0`,
Hadoop catalog, `local[8]`, each storm released by one `threading.Barrier` so the N statements
race. The facade pins in `python/repark/tests/test_ice_occ_scoped_1.py` read every expectation
from these fixture files; no count, row or error text is a literal in a test body.

## Contents

- `spark_occ_oracle.json` — the UNSCOPED storms (`cells`, per format version `v2_` / `v3_`):
  `16_concurrent_inserts` (`INSERT INTO ins VALUES (i, i)`, i in 0..16, unpartitioned), the
  row and snapshot counts after it, `8_disjoint_merges_{serializable,snapshot}` (merge-on-read,
  unpartitioned, 100 seed rows, `MERGE … USING (SELECT CAST(i AS BIGINT) AS id, 'm<i>' AS v) s
  ON t.id = s.id WHEN MATCHED THEN UPDATE SET v = s.v`, i in 0..8) with the rows after, and
  `2_disjoint_partition_deletes` (`DELETE … WHERE k = 'a'` / `k = 'b'` on a two-row table
  partitioned by `k`, default copy-on-write). Each storm cell is `{committed, of, errors}`, each
  row cell is `{rows}`. `commit_retry_defaults` records the table properties Spark left (no
  `commit.retry.*` override).
- `spark_occ_oracle2.json` — the SCOPED storms, per `v{2,3}_{mergeonread,copyonwrite}`: the four
  partition-scoped MERGEs, UPDATE statements and DELETE statements on a table partitioned by `k` (100 rows,
  `k = ['a','b','c','d'][id % 4]`), one partition-scoped MERGE against a concurrent `INSERT INTO`
  another partition, and two range-scoped MERGEs (`t.id < 50` / `t.id >= 50`) on an
  unpartitioned table. Each storm cell embeds its exact `statements`; each row cell embeds its
  `query`.
- `spark_occ_oracle3.json` — recorded 2026-09-18 for review finding L-02: a MERGE that INSERTS
  (`… ON t.k = 'a' AND t.k = s.k AND t.id = s.id WHEN MATCHED THEN UPDATE SET v = s.v WHEN NOT
  MATCHED THEN INSERT *`) racing one concurrent `INSERT INTO … VALUES (<id>, '<key>',
  'appended')`, per `v{2,3}_{mergeonread,copyonwrite}_<cell>` with cells
  `same_key_in_on_partition` (source `(1000,'a')`, append `(1000,'a')`),
  `other_key_in_on_partition` (`(1000,'a')` / `(2000,'a')`), `other_partition` (`(1000,'a')` /
  `(1000,'b')`) and `source_key_outside_on_partition` (`(1000,'b')` / `(1000,'b')`). NOT a
  barrier storm: the interleaving is DETERMINISTIC. The MERGE's source column `v` is a Python UDF
  `gate('merged', <flag dir>)` that touches `started` and blocks until `go` exists; Spark has
  analysed the statement and pinned the target snapshot before any task runs, so the main thread
  commits the append and then releases the gate, and the MERGE validates against exactly that
  one concurrent snapshot (the UDF is deterministic: a copy-on-write MERGE refuses a
  non-deterministic source with `INVALID_NON_DETERMINISTIC_EXPRESSIONS`). Each cell records the
  `merge` and `concurrent_insert` SQL, `merge_outcome` (`ok` or the exception head),
  `snapshots` (operation, added records — the ordering witness), `rows_at_or_above_1000` and
  `count`. The Rust race pin `crates/repark-iceberg/src/write/merge/tests/occ_scoped_insert.rs`
  reads it.

## Provenance and the one normalization

The recorders were run outside the repository (they need a JVM); their SQL is reproduced above
and, for `spark_occ_oracle2.json`, embedded verbatim in each cell. Spark's error text names the
warehouse's absolute path; that prefix was replaced by the literal `<warehouse>/` in
`spark_occ_oracle.json` (5 occurrences, error strings only) because tracked files carry no
scratch paths. Nothing else was edited. Raw-recording SHA-256: `spark_occ_oracle.json`
`07bd53ec84bf526a96ebec968af91cc954c04b78c0e9c595dedf6501fe37ab3b` (before normalization),
`spark_occ_oracle2.json` `c366f3b5c795331062cc5d4f42e4cb1ee3bc11f6ae8c3a98642014281ce1b55c`
(byte-identical to the recording), `spark_occ_oracle3.json`
`fa6dc5d953f81d2a938a97066b83f15b2b36cddde9ffd0430fe1e579f03e748e` (before normalization: the
warehouse prefix became `<warehouse>/`, 8 occurrences in error strings, and the gate's flag
directory `<flags>/`, 16 occurrences in the `merge` SQL), `spark_occ_oracle4.json`
`bcaafb93b6b2685f1e9143880d6d1afed21fa0c15ad5ad42115fdaa65a9dd887` (before normalization:
the Hadoop and InMemory warehouse prefixes became `<warehouse>/`, 13 occurrences in error
strings) whose committed bytes hash to
`fd2286d2cb692d7c3ca271cb087dc7913ad38f883b201ab57e60aefab3932763`.

## Reading the cells

- The unscoped MERGE storms are Spark REFUSING: 1 of 8 commit, the losers raise
  `ValidationException: Found conflicting files that can contain records matching true`
  (serializable) or `Found new conflicting delete files that can apply to records matching true`
  (snapshot). The `ON` condition has no target-only conjunct, so Spark's conflict filter is `true`.
- `spark_occ_oracle.json`'s `v2_16_concurrent_inserts` (16 of 16) is one repetition, not
  Spark's behaviour: over six barrier-released repetitions per catalog × format version
  (fixture `spark_occ_oracle4.json`, recorded 2026-09-18) Spark 4.1.2 + Iceberg 1.11.0 commits
  Hadoop v2 15,13,11,12,14,12, Hadoop v3 14,12,13,9,10,13, InMemory v2 9,9,8,9,9,8 and InMemory
  v3 9,8,8,7,9,9. Every loser is a `CommitFailedException` (`Cannot commit: stale table
  metadata`, `Cannot commit to table … metadata location from …`; Hadoop also `Cannot commit
  changes based on stale table metadata`, `Version N already exists`) — commit-retry
  exhaustion, not a validation outcome. RePark's own storm commits v2 7,5,5,6,5,6,6,6,5,5 and
  v3 6,5,7,5,5,6,5,5,6,7 over ten repetitions (registry BACKLOG row
  ICE-OCC-SCOPED-1-INSERT-STORM, corrected 2026-09-18).
- `spark_occ_oracle3.json`: on every format version and write mode an append INSIDE the `ON`
  partition aborts the MERGE (`Found conflicting files that can contain records matching
  (not_null(ref(name="k")) and ref(name="k") == "a")` on merge-on-read, `ref(name="k") == "a"`
  on copy-on-write), whatever its key; an append to ANOTHER partition commits beside it — even
  the same `(1000, 'b')` the MERGE inserts, because `t.k = 'a'` cannot match a `k = 'b'` row in
  any serial order either (registry row ICE-OCC-SCOPED-1-NOT-MATCHED-INSERT).
- `spark_occ_oracle4.json` — recorded 2026-09-18: the 16-concurrent-`INSERT INTO` storm over
  six repetitions per catalog × format version, cells `hadoop_v2_16_concurrent_inserts`,
  `hadoop_v3_16_concurrent_inserts`, `inmemory_v2_16_concurrent_inserts` and
  `inmemory_v3_16_concurrent_inserts`, each a list of `{committed, of, errors, rows_after}`
  repetitions (`INSERT INTO ins VALUES (i, i)`, i in 0..16, one fresh unpartitioned table per
  repetition, `local[8]`, one SparkSession, 16 threads released by one `threading.Barrier`).
  The registry row ICE-OCC-SCOPED-1-INSERT-STORM carries the corrected numbers; the
  `test_insert_storm_loses_only_to_the_retry_budget` docstring cites the ranges.
- The merge-on-read range MERGE is 1 of 2: its loser raises `Found new conflicting delete files
  that can apply to records matching (not_null(ref(name="id")) and ref(name="id") < 50)` — the
  concurrent delete file carries no `id` bounds, so even a scoped filter cannot exclude it.
  Copy-on-write commits both. Spark's `range(100)` under `local[8]` writes eight
  range-contiguous files, so the two range MERGEs rewrite DIFFERENT files; the facade pin seeds
  its unpartitioned table with two INSERT statements (`id < 50`, `id >= 50`) for the same layout
  (ledger Q-21a-4).

pins: ice-occ-scoped-1/C-015, C-020
pins: ice-append-retry-1/C-001, C-002

## Pointers

- Up: [../map.md](../map.md)
- Consumer: [../../../../../repark/tests/map.md](../../../../../repark/tests/map.md)
  (`test_ice_occ_scoped_1.py`)
- Ledger: `task/ledgers/staging/ice-occ-scoped-1-ledger.md`
