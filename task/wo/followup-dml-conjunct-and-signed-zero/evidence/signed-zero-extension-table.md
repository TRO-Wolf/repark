# Signed zero extension (2026-11-08): E1 to E9 and R1 to R5

Measurement only. Model `claude-haiku-5-5`. Part 2 of the 2026-11-08 extension, on the same branch as Table B. The
oracle is Spark 4.1.2 (Iceberg 1.11.0 runtime). The RePark leg is the release wheel built from `fdc61f26`
(repark 1.5.5). Same seeds as Table B: `t` (eight rows; `-0.0` at id 4, `0.0` at id 6, NULL rows at ids 2 and 7, a NaN
at id 3, Infinity at id 5) and `u` (`(10, 0.0)`, `(11, -0.0)`). The two-file table `x_two_*` is two single-row INSERT statements,
id 4 (`-0.0`) first and id 6 (`0.0`) second, so one data file holds only `-0.0` and the other only `0.0`. Parquet
statistics read with pyarrow: the `-0.0` file has min = max = `-0.0`; the `0.0` file has min = max = `0.0`.

## Read this first: the Spark oracle is wrong on Iceberg reads of zero-only files

The brief sets Spark's own answer as the oracle for E1, E2, E3 and R1. On the two-file table, Spark answers
`d = 0.0` with one row (id 6) on v2 CoW, v2 MoR and v3, and both RePark doors agree with it. IEEE equality (and the
rule the brief states) gives two rows, 4 and 6. Spark reading the same two Parquet files directly (E4, Spark's own
Parquet reader, no Iceberg) returns both rows. So the Iceberg scan drops the file whose statistics are min = max =
`-0.0` for the predicate `d = 0.0`. E3 shows the same thing one level down: the `-0.0` page is dropped by both engines.

The per-cell lines below therefore carry two checks. `s=` and `r=` are answer row counts from Spark and RePark. The
label compares RePark with the Spark oracle, as the brief asks. `truth s=T` means Spark's answer equals the IEEE
truth computed from the seed values, and `truth r=T` means the same for RePark. An `EQUAL` label with `truth s=F
r=F` means both engines are wrong and agree; that is the signed-zero pruning, not a match.

## Legs

| Leg | Engine and version | Raw file (under `raw/`) | What it ran |
|---|---|---|---|
| spark | Spark 4.1.2, JDK 17.0.15, Iceberg 1.11.0, Hadoop catalog | `spark-signed-zero-extension.jsonl` | Oracle for every cell; writes the Spark-written tables for E2 and E4; E3 oracle table |
| repark | release wheel `fdc61f26` (repark 1.5.5), facade and native doors | `repark-main-signed-zero-extension.jsonl` | Every RePark cell; E2 attempt; E4 native attempt; E5 timing |
| spill-spark | Spark 4.1.2, `spark.memory.fraction` 0.01, broadcast join off, shuffle-hash join preferred | `spark-signed-zero-spill.jsonl` | E8 oracle |
| spill-repark | facade (default memory) and native with `memory_limit_gb=1` | `repark-main-signed-zero-spill.jsonl` | E8 |

The first Spark and RePark runs and the first spill run carried defects (a `x_u` column list, hyphenated table names,
no `x_u` in the spill legs). They were fixed and the legs were rerun in full; only the rerun raw files are kept.

## Row summaries

**E1, two-file table, `spark.sql` (v2 CoW, v2 MoR, v3) and native (v2 CoW).** Reads of `d = 0.0`, `d = -0.0`, `d <> 0.0`,
`d IN (0.0)`, `d >= 0.0`. Every RePark read agrees with the Spark oracle except `d <> 0.0` (RePark returns id 4 on all
four kinds, where Spark and the truth return nothing) and `d = -0.0` on native (WRONG-READ). Every read of a zero-only
file misses row 4 on both engines (PRUNED-WRONG; the file statistics explain it). DELETE and UPDATE with the same five
forms on fresh two-file tables: the RePark states equal the Spark states on every cell except native `d = -0.0` DELETE,
native `d = -0.0` UPDATE, and `d <> 0.0` UPDATE on all four kinds, where RePark writes `z` to row 4 and Spark writes
nothing (WRONG-WRITE). The DML states for the other forms are wrong on both engines in the same way as the reads.

**E2, Spark-written table read and written by RePark.** Not measured. The RePark memory catalog registered on Spark's
warehouse root cannot see the table: `Error during planning: table 'ice_sp.ns.x_sp' not found`, and the copies return
`NamespaceNotFound: No such namespace: NamespaceIdent(["ns"])`. This is missing evidence; RePark does not open a
Spark Hadoop-catalog table through the memory catalog.

**E3, page index on write.** Two files of 10,000 rows (ids 1 to 10,000 and 10,001 to 20,000), written with table
properties `write.parquet.page-row-limit = 1000` and `write.parquet.page-size-bytes = 2048`. `-0.0` on ids 1 to 1000,
`0.0` on ids 19,001 to 20,000. Spark's oracle table is Spark-written with the same settings (not the RePark files); the
RePark table is RePark-written. Both engines return only the 1000 zero rows of ids 19,001 to 20,000 (verified from the
raw answers) and drop the 1000 `-0.0` rows of ids 1 to 1000. File-level statistics do not exclude that file (its max is
1.5), so the candidate is the page index; it was not read from the page index.

**E4, the two files read directly.** Spark's `spark.read.parquet` over the two Spark-written files answers ids 4 and 6,
the IEEE answer. The native `read_parquet` table function is not resolved on the native door
(`UNRESOLVABLE_TABLE_VALUED_FUNCTION`), so the RePark side was not measured.

**E5, baseline on the main wheel (`spark.sql`, facade).** A 10,000,000-row table (`d = -0.0` for id % 4 = 0, `0.0` for
id % 4 = 1, else id % 1000); the count is 10,000,000. GROUP BY d: median 34.5 ms over five runs, 502 groups (`-0.0` and
`0.0` in separate groups). `x_big t JOIN x_u u ON t.d = u.d` as `count(*)`: median 20.6 ms; the join count itself was
not recorded in this run. The one-minute load average was 12.0 on every run, so these are not idle-box numbers. Spark
was not timed. This is the pre-unit baseline only.

**E6, `d <=> 0.0`.** Spark answers two rows (ids 4 and 6). RePark refuses on both `spark.sql` and `filter(str)` with
`Arrow error: Cast error: Cannot cast to Decimal128(30, 15). Overflowing on NaN`. The NaN row (id 3) triggers the
decimal cast.

**E7, comparisons on both zero rows.** `d >= 0.0`: Spark 6 rows (the truth, with NaN ordered above every number);
RePark 5 on all five routes, missing id 4. `d > -0.0`: `spark.sql` and `filter(str)` answer 4 (the truth); `F.expr`,
typed and native answer 5 and add id 6. `abs(d) = 0.0`: equal and correct on all five routes. `d * 0 = 0`: Spark 4
(the truth); RePark 3 on all five routes, missing id 4. `CAST(d AS STRING)` on ids 4 and 6: identical answers on
`spark.sql` and native.

**E8, hash aggregate and hash join on a float key with both zeros.** The table has 2,000,000 rows (1,000,000 zero rows,
500,000 each sign). Spark: GROUP BY d gives 501 groups, the zero group holds 1,000,000 rows (signs merged). RePark
facade and native: 502 groups, `-0.0` 500,000 and `0.0` 500,000. Join with `u` (which holds one row of each sign):
Spark 2,000,000 pairs; RePark 1,000,000 on both doors. Self-join on `d` restricted to `a.id % 1000 = 0`: Spark
2,000,000,000; RePark 1,000,000,000. Spill evidence was not captured. No spill counter was read. The configured
limits are recorded here; they are not proof that a spill happened.

**E9, Trino spellings on native only.** Trino not run: `docker image ls` under rootless Docker listed no Trino image, so
the oracle is ORACLE-MISSING and no image was pulled. The native answers are recorded (rows): `d = 0.0` 1, `d <> 0.0` 5,
`d < 0.0` 1, GROUP BY 6 groups, DISTINCT 6, JOIN 2, INTERSECT 2, ORDER BY 8 (ordered), `max(d)` 1 row,
`CAST(CAST('Infinity' AS DOUBLE) AS VARCHAR)` 1 row.

**R1, IN list on the two-file table.** `d IN (0.0, 10.0, 20.0, 30.0)`: both engines return id 6 only. Row 4 is in the
`-0.0`-only file (min = max = `-0.0`), so this is PRUNED-WRONG on both engines.

**R2, one-sided comparisons with CAST and native spellings.** `d <= CAST('-0.0' AS DOUBLE)`: Spark 2 rows (the truth);
RePark 1 row on all five routes (misses id 6). `d > CAST('-0.0' AS DOUBLE)`: Spark 4 (the truth); RePark 5 on all five
routes (adds id 6). `0.0 <= d`: Spark 6 (the truth); RePark 5 on all five routes (misses id 4).

**R3, `d <=> 0.0` and its negation.** `SELECT id, d <=> 0.0`: `spark.sql` REFUSED with the decimal-cast text from E6;
native WRONG-READ (native `d IS NOT DISTINCT FROM 0.0` is false on id 4, where Spark is true).
`WHERE NOT (d <=> 0.0)`: Spark 6 (the truth); `spark.sql` and `filter(str)` REFUSED with the same text; `F.expr`,
typed and native answer 7 and add id 4 (WRONG-READ).

**R4, aggregates and windows.** `COUNT(DISTINCT d)`: Spark 4 (the truth; the zeros merge, NaN is one value); RePark 5 on
both doors (WRONG-READ). `RANK() OVER (ORDER BY d)`: both engines answer eight rows; the ranks differ where the zeros
tie in Spark (WRONG-READ on both doors, ordered). `WHERE d IN (SELECT d FROM u)`: equal to the truth on both doors.

**R5, `INSERT ... SELECT` with `named_struct` equality.** v1 (`named_struct('a', i, 'b', 2) = named_struct('b', 2, 'a', 1)`):
Spark inserts nothing (8 rows); RePark inserts the two rows with `i = 1` on all four routes (10 rows), WRONG-WRITE.
v2 (with `CAST(NULL AS INT)` and a field `c`): Spark 8 rows; RePark 10 rows on all four routes (WRONG-WRITE). Spark's
answer fits a positional comparison of struct fields. RePark's answer fits a comparison by field name. The Spark source
was not checked; the RePark source was not traced.

## Per-cell lines (generated)

Each line is `cid | route | s= rows (Spark oracle) r= rows (RePark) | label | truth s= r= | refusal text if any`. Routes:
`sql` is `spark.sql` or the facade `spark.sql`; `filter-str` and `f-expr` are `DataFrame.filter` with a string and
`F.expr`; `typed` is the `F` column API; `native` is `repark.sql`. `ORACLE-MISSING` is E9 (no Trino oracle). `E5` and
`E8` lines are at the end of this block.

```
== per-cell
E1-read-f1-v2-cow | sql | s=1 r=1 | EQUAL | truth s=F r=F | 
E1-read-f1-v2-cow | native | s=1 r=1 | EQUAL | truth s=F r=F | 
E1-read-f1-v2-mor | sql | s=1 r=1 | EQUAL | truth s=F r=F | 
E1-read-f1-v3 | sql | s=1 r=1 | EQUAL | truth s=F r=F | 
E1-read-f2-v2-cow | sql | s=1 r=1 | EQUAL | truth s=F r=F | 
E1-read-f2-v2-cow | native | s=1 r=1 | WRONG-READ | truth s=F r=F | 
E1-read-f2-v2-mor | sql | s=1 r=1 | EQUAL | truth s=F r=F | 
E1-read-f2-v3 | sql | s=1 r=1 | EQUAL | truth s=F r=F | 
E1-read-f3-v2-cow | sql | s=0 r=1 | WRONG-READ | truth s=T r=F | 
E1-read-f3-v2-cow | native | s=0 r=1 | WRONG-READ | truth s=T r=F | 
E1-read-f3-v2-mor | sql | s=0 r=1 | WRONG-READ | truth s=T r=F | 
E1-read-f3-v3 | sql | s=0 r=1 | WRONG-READ | truth s=T r=F | 
E1-read-f4-v2-cow | sql | s=1 r=1 | EQUAL | truth s=F r=F | 
E1-read-f4-v2-cow | native | s=1 r=1 | EQUAL | truth s=F r=F | 
E1-read-f4-v2-mor | sql | s=1 r=1 | EQUAL | truth s=F r=F | 
E1-read-f4-v3 | sql | s=1 r=1 | EQUAL | truth s=F r=F | 
E1-read-f5-v2-cow | sql | s=1 r=1 | EQUAL | truth s=F r=F | 
E1-read-f5-v2-cow | native | s=1 r=1 | EQUAL | truth s=F r=F | 
E1-read-f5-v2-mor | sql | s=1 r=1 | EQUAL | truth s=F r=F | 
E1-read-f5-v3 | sql | s=1 r=1 | EQUAL | truth s=F r=F | 
E1-dml-delete-f1-v2cow | sql | s=1 r=1 | EQUAL | truth s=F r=F | 
E1-dml-delete-f1-v2cow | native | s=1 r=1 | EQUAL | truth s=F r=F | 
E1-dml-delete-f1-v2mor | sql | s=1 r=1 | EQUAL | truth s=F r=F | 
E1-dml-delete-f1-v3 | sql | s=1 r=1 | EQUAL | truth s=F r=F | 
E1-dml-delete-f2-v2cow | sql | s=1 r=1 | EQUAL | truth s=F r=F | 
E1-dml-delete-f2-v2cow | native | s=1 r=1 | WRONG-WRITE | truth s=F r=F | 
E1-dml-delete-f2-v2mor | sql | s=1 r=1 | EQUAL | truth s=F r=F | 
E1-dml-delete-f2-v3 | sql | s=1 r=1 | EQUAL | truth s=F r=F | 
E1-dml-delete-f3-v2cow | sql | s=1 r=1 | EQUAL | truth s=F r=F | 
E1-dml-delete-f3-v2cow | native | s=1 r=1 | EQUAL | truth s=F r=F | 
E1-dml-delete-f3-v2mor | sql | s=1 r=1 | EQUAL | truth s=F r=F | 
E1-dml-delete-f3-v3 | sql | s=1 r=1 | EQUAL | truth s=F r=F | 
E1-dml-delete-f4-v2cow | sql | s=1 r=1 | EQUAL | truth s=F r=F | 
E1-dml-delete-f4-v2cow | native | s=1 r=1 | EQUAL | truth s=F r=F | 
E1-dml-delete-f4-v2mor | sql | s=1 r=1 | EQUAL | truth s=F r=F | 
E1-dml-delete-f4-v3 | sql | s=1 r=1 | EQUAL | truth s=F r=F | 
E1-dml-delete-f5-v2cow | sql | s=1 r=1 | EQUAL | truth s=F r=F | 
E1-dml-delete-f5-v2cow | native | s=1 r=1 | EQUAL | truth s=F r=F | 
E1-dml-delete-f5-v2mor | sql | s=1 r=1 | EQUAL | truth s=F r=F | 
E1-dml-delete-f5-v3 | sql | s=1 r=1 | EQUAL | truth s=F r=F | 
E1-dml-update-f1-v2cow | sql | s=2 r=2 | EQUAL | truth s=F r=F | 
E1-dml-update-f1-v2cow | native | s=2 r=2 | EQUAL | truth s=F r=F | 
E1-dml-update-f1-v2mor | sql | s=2 r=2 | EQUAL | truth s=F r=F | 
E1-dml-update-f1-v3 | sql | s=2 r=2 | EQUAL | truth s=F r=F | 
E1-dml-update-f2-v2cow | sql | s=2 r=2 | EQUAL | truth s=F r=F | 
E1-dml-update-f2-v2cow | native | s=2 r=2 | WRONG-WRITE | truth s=F r=F | 
E1-dml-update-f2-v2mor | sql | s=2 r=2 | EQUAL | truth s=F r=F | 
E1-dml-update-f2-v3 | sql | s=2 r=2 | EQUAL | truth s=F r=F | 
E1-dml-update-f3-v2cow | sql | s=2 r=2 | WRONG-WRITE | truth s=T r=F | 
E1-dml-update-f3-v2cow | native | s=2 r=2 | WRONG-WRITE | truth s=T r=F | 
E1-dml-update-f3-v2mor | sql | s=2 r=2 | WRONG-WRITE | truth s=T r=F | 
E1-dml-update-f3-v3 | sql | s=2 r=2 | WRONG-WRITE | truth s=T r=F | 
E1-dml-update-f4-v2cow | sql | s=2 r=2 | EQUAL | truth s=F r=F | 
E1-dml-update-f4-v2cow | native | s=2 r=2 | EQUAL | truth s=F r=F | 
E1-dml-update-f4-v2mor | sql | s=2 r=2 | EQUAL | truth s=F r=F | 
E1-dml-update-f4-v3 | sql | s=2 r=2 | EQUAL | truth s=F r=F | 
E1-dml-update-f5-v2cow | sql | s=2 r=2 | EQUAL | truth s=F r=F | 
E1-dml-update-f5-v2cow | native | s=2 r=2 | EQUAL | truth s=F r=F | 
E1-dml-update-f5-v2mor | sql | s=2 r=2 | EQUAL | truth s=F r=F | 
E1-dml-update-f5-v3 | sql | s=2 r=2 | EQUAL | truth s=F r=F | 
E2-read-spark-written | sql | s=1 r= | REFUSED | truth s=F r= | Error during planning: table 'ice_sp.ns.x_sp' not found
E2-dml-delete_f1 | sql | s=1 r= | REFUSED | truth s=F r= | NamespaceNotFound => No such namespace: NamespaceIdent(["ns"])
E2-dml-update_f1 | sql | s=2 r= | REFUSED | truth s=F r= | NamespaceNotFound => No such namespace: NamespaceIdent(["ns"])
E2-dml-delete_f2 | sql | s=1 r= | REFUSED | truth s=F r= | NamespaceNotFound => No such namespace: NamespaceIdent(["ns"])
E2-dml-update_f2 | sql | s=2 r= | REFUSED | truth s=F r= | NamespaceNotFound => No such namespace: NamespaceIdent(["ns"])
E3-read-d0 | sql | s=1000 r=1000 | EQUAL | truth s=F r=F | 
E7-g1 | sql | s=6 r=5 | WRONG-READ | truth s=T r=F | 
E7-g1 | filter-str | s=6 r=5 | WRONG-READ | truth s=T r=F | 
E7-g1 | f-expr | s=6 r=5 | WRONG-READ | truth s=T r=F | 
E7-g1 | typed | s=6 r=5 | WRONG-READ | truth s=T r=F | 
E7-g1 | native | s=6 r=5 | WRONG-READ | truth s=T r=F | 
E7-g2 | sql | s=4 r=4 | EQUAL | truth s=T r=T | 
E7-g2 | filter-str | s=4 r=4 | EQUAL | truth s=T r=T | 
E7-g2 | f-expr | s=4 r=5 | WRONG-READ | truth s=T r=F | 
E7-g2 | typed | s=4 r=5 | WRONG-READ | truth s=T r=F | 
E7-g2 | native | s=4 r=5 | WRONG-READ | truth s=T r=F | 
E7-g3 | sql | s=2 r=2 | EQUAL | truth s=T r=T | 
E7-g3 | filter-str | s=2 r=2 | EQUAL | truth s=T r=T | 
E7-g3 | f-expr | s=2 r=2 | EQUAL | truth s=T r=T | 
E7-g3 | typed | s=2 r=2 | EQUAL | truth s=T r=T | 
E7-g3 | native | s=2 r=2 | EQUAL | truth s=T r=T | 
E7-g4 | sql | s=4 r=3 | WRONG-READ | truth s=T r=F | 
E7-g4 | filter-str | s=4 r=3 | WRONG-READ | truth s=T r=F | 
E7-g4 | f-expr | s=4 r=3 | WRONG-READ | truth s=T r=F | 
E7-g4 | typed | s=4 r=3 | WRONG-READ | truth s=T r=F | 
E7-g4 | native | s=4 r=3 | WRONG-READ | truth s=T r=F | 
E7-cast-string | sql | s=2 r=2 | EQUAL | truth s= r= | 
E7-cast-string | native | s=2 r=2 | EQUAL | truth s= r= | 
E6-spaceship-zero | sql | s=2 r= | REFUSED | truth s=T r= | Arrow error: Cast error: Cannot cast to Decimal128(30, 15). Overflowing on NaN
E6-spaceship-zero | filter-str | s=2 r= | REFUSED | truth s=T r= | Arrow error: Cast error: Cannot cast to Decimal128(30, 15). Overflowing on NaN
R1-in-list | sql | s=1 r=1 | EQUAL | truth s=F r=F | 
R1-in-list | native | s=1 r=1 | EQUAL | truth s=F r=F | 
R2-h1 | sql | s=2 r=1 | WRONG-READ | truth s=T r=F | 
R2-h1 | filter-str | s=2 r=1 | WRONG-READ | truth s=T r=F | 
R2-h1 | f-expr | s=2 r=1 | WRONG-READ | truth s=T r=F | 
R2-h1 | typed | s=2 r=1 | WRONG-READ | truth s=T r=F | 
R2-h1 | native | s=2 r=1 | WRONG-READ | truth s=T r=F | 
R2-h2 | sql | s=4 r=5 | WRONG-READ | truth s=T r=F | 
R2-h2 | filter-str | s=4 r=5 | WRONG-READ | truth s=T r=F | 
R2-h2 | f-expr | s=4 r=5 | WRONG-READ | truth s=T r=F | 
R2-h2 | typed | s=4 r=5 | WRONG-READ | truth s=T r=F | 
R2-h2 | native | s=4 r=5 | WRONG-READ | truth s=T r=F | 
R2-h3 | sql | s=6 r=5 | WRONG-READ | truth s=T r=F | 
R2-h3 | filter-str | s=6 r=5 | WRONG-READ | truth s=T r=F | 
R2-h3 | f-expr | s=6 r=5 | WRONG-READ | truth s=T r=F | 
R2-h3 | typed | s=6 r=5 | WRONG-READ | truth s=T r=F | 
R2-h3 | native | s=6 r=5 | WRONG-READ | truth s=T r=F | 
R3-projection | sql | s=8 r= | REFUSED | truth s= r= | Arrow error: Cast error: Cannot cast to Decimal128(30, 15). Overflowing on NaN
R3-projection | native | s=8 r=8 | WRONG-READ | truth s= r= | 
R3-not-spaceship | sql | s=6 r= | REFUSED | truth s=T r= | Arrow error: Cast error: Cannot cast to Decimal128(30, 15). Overflowing on NaN
R3-not-spaceship | filter-str | s=6 r= | REFUSED | truth s=T r= | Arrow error: Cast error: Cannot cast to Decimal128(30, 15). Overflowing on NaN
R3-not-spaceship | f-expr | s=6 r=7 | WRONG-READ | truth s=T r=F | 
R3-not-spaceship | typed | s=6 r=7 | WRONG-READ | truth s=T r=F | 
R3-not-spaceship | native | s=6 r=7 | WRONG-READ | truth s=T r=F | 
R4-count-distinct | sql | s=1 r=1 | WRONG-READ | truth s=T r=F | 
R4-count-distinct | native | s=1 r=1 | WRONG-READ | truth s=T r=F | 
R4-rank | sql | s=8 r=8 | WRONG-READ | truth s= r= | 
R4-rank | native | s=8 r=8 | WRONG-READ | truth s= r= | 
R4-in-subquery | sql | s=2 r=2 | EQUAL | truth s=T r=T | 
R4-in-subquery | native | s=2 r=2 | EQUAL | truth s=T r=T | 
R5-v1 | sql | s=8 r=10 | WRONG-WRITE | truth s= r= | 
R5-v1 | filter-str | s=8 r=10 | WRONG-WRITE | truth s= r= | 
R5-v1 | f-expr | s=8 r=10 | WRONG-WRITE | truth s= r= | 
R5-v1 | typed | s=8 r=10 | WRONG-WRITE | truth s= r= | 
R5-v2 | sql | s=8 r=10 | WRONG-WRITE | truth s= r= | 
R5-v2 | filter-str | s=8 r=10 | WRONG-WRITE | truth s= r= | 
R5-v2 | f-expr | s=8 r=10 | WRONG-WRITE | truth s= r= | 
R5-v2 | typed | s=8 r=10 | WRONG-WRITE | truth s= r= | 
E9-equal | native | s= r=1 | ORACLE-MISSING | truth s= r= | 
E9-not-equal | native | s= r=5 | ORACLE-MISSING | truth s= r= | 
E9-less | native | s= r=1 | ORACLE-MISSING | truth s= r= | 
E9-group-by | native | s= r=6 | ORACLE-MISSING | truth s= r= | 
E9-distinct | native | s= r=6 | ORACLE-MISSING | truth s= r= | 
E9-join | native | s= r=2 | ORACLE-MISSING | truth s= r= | 
E9-intersect | native | s= r=2 | ORACLE-MISSING | truth s= r= | 
E9-order-by | native | s= r=8 | ORACLE-MISSING | truth s= r= | 
E9-max | native | s= r=1 | ORACLE-MISSING | truth s= r= | 
E9-infinity-cast | native | s= r=1 | ORACLE-MISSING | truth s= r= | 
E4-read-parquet-files | native | s=2 r= | REFUSED | truth s=T r= | [UNRESOLVABLE_TABLE_VALUED_FUNCTION] Could not resolve `read_parquet` to a table-valued fu
== spark-only sql cells with no repark pair
== e5 and e8
E8-setup spill-spark create ok
E8-setup spill-spark insert-2M ok
E8-setup spill-spark x_u ok
E8 spill-spark E8-group-by-d ANSWERED 501 
E8 spill-spark E8-join-zeros ANSWERED 1 
E8 spill-spark E8-self-join-d ANSWERED 1 
E8-setup spill-repark-facade create ok
E8-setup spill-repark-facade insert-2M ok
E8-setup spill-repark-facade x_u ok
E8 spill-repark-facade E8-group-by-d ANSWERED 502 
E8 spill-repark-facade E8-join-zeros ANSWERED 1 
E8 spill-repark-facade E8-self-join-d ANSWERED 1 
E8-setup spill-repark-native-1gb create ok
E8-setup spill-repark-native-1gb insert-2M ok
E8-setup spill-repark-native-1gb x_u ok
E8 spill-repark-native-1gb E8-group-by-d ANSWERED 502 
E8 spill-repark-native-1gb E8-join-zeros ANSWERED 1 
E8 spill-repark-native-1gb E8-self-join-d ANSWERED 1 
```
Up: [map.md](map.md).
