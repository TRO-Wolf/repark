# Provenance: SIGNED-ZERO-NORMALIZE-1 part B step 0 (flat keys)

Model `claude-opus-5-5`, 2026-11-12, between 14:12 and about 15:20 UTC. Measurement and drafting only: no product
code, nothing committed. Every command ran one at a time inside
`flock <build-slots>/opus-cargo.lock nice -n 10 taskset -c 32-47 env CARGO_BUILD_JOBS=8 CARGO_INCREMENTAL=0 bash -c 'ulimit -v 67108864; …'`,
and every Spark leg additionally under `flock <build-slots>/spark-jvm.lock` (one JVM at a time). No
timing was taken, so the measure lock was not needed.

## Versions and inputs

| Item | Value |
|---|---|
| RePark | a scratch clone on branch `fix/signed-zero-normalize-1-part-b` at main `ae247767a83efbe4f4d1344e45ba43885e8b36af` (clean tree; part A is `04d84ab9`, an ancestor). Release module built into the tree's `.venv` with `uv sync --locked --extra numpy --extra pandas --extra polars --extra ml-ext --no-install-package repark` and `uvx maturin@1.14.1 develop --profile release` (exit 0). `_native.abi3.so` sha256 `1d3ca28a79ef05fd85f5b3a98ec60d42ba2f5761eb8abe4c38a942d1e48cdf49`; repark 1.5.6, pyarrow 25.0.1, Python 3.12.3 |
| Spark oracle | Spark 4.1.2, OpenJDK 17.0.15 (Zulu 17), the oracle virtualenv's interpreter (Python 3.12.3, pyspark 4.1.2), `local[2]`, `spark.sql.shuffle.partitions = 4`, session zone `America/New_York`, driver memory 4g. Nothing installed into that venv |
| Iceberg runtime | `iceberg-spark-runtime-4.1_2.13-1.11.0.jar`, sha256 `d6ea6c5d099288daeb7d5a92061bd3d7d8f296492632b42378e5f2f0e3066242` (the same jar as part A) |
| Catalogs | Spark: a Hadoop catalog `probe_ice` in a scratch warehouse. RePark: an in-memory catalog `ice` on each door (facade and native), each with its own warehouse |
| Native session | `PyReparkSession.native(config={"repark.sql.allowCreateFormatVersion3": "true"})`, installed as `repark._ANSI_NATIVE` (part A's setup) |

## Scripts (in this directory, no comments, per the owner ruling)

- `szb_cells.py`: the cell catalogue. Read sites (GB, DI, WI, OB, MM, CM, JN, SQ, SO, PR, TV), the NaN-payload group
  (NN), the write sites (WR), the partition sites (PT) and the flat matrix's floatkeys shapes, copied verbatim
  from `python/repark/tests/parity/_flat_matrix_cells.py`, in three seed variants: `matrix` (the matrix's own seeds,
  as on main since #1307), `bare` (the pre-#1307 bare-literal seeds, which step 0's raw files call `matrix`) and
  `signed` (`CAST('-0.0' AS <type>)`, the column's own type).
- `szb_probe.py`: runs a leg. `--engine spark|repark --part reads|dml|floatkeys`. It imports part A's
  `evidence/probe_signed_zero.py` for `norm`, `collect`, `attempt`, `scrub` and the classifier.
- `run_leg.sh`: the leg launcher (interpreter, `JAVA_HOME`, jar, warehouse and output paths).
- `label_render.py`: labels every RePark cell with part A's `label_of`, unchanged, and writes `labels.jsonl` and
  `table.md` (appending `named-seats.md`).
- `idf_check.py`, `partition_check.py`, `naaj_check.py`: three side checks (below).

## Legs (raw/)

| Raw file | Leg |
|---|---|
| `spark-reads.jsonl` | Spark reads: inline `VALUES` views (the truth) and one-file Iceberg tables, on `spark-sql`, `f-expr` and `typed` |
| `repark-ae247767-reads.jsonl` | RePark reads on `spark-sql`, `f-expr`, `typed` (facade) and `native` |
| `spark-dml.jsonl`, `repark-ae247767-dml.jsonl` | write sites on three kinds (Spark door) and v2 CoW (native), plus the partition sites |
| `spark-floatkeys.jsonl`, `repark-ae247767-floatkeys.jsonl` | the 28 floatkeys cells, with the pre-#1307 bare seeds (recorded as variant `matrix`) and the signed seeds, Spark door |
| `spark-naaj-check.jsonl` | `naaj_check.py`: `NOT IN` / `IN` / `NOT EXISTS` / literal `NOT IN` on Spark under `optimizeNullAwareAntiJoin` true/false and broadcast on/off, with the physical join names |

```
ce3afd055235a8e1b589013a490fed8b85c24ef06a9b7b1d56f9c3bd59c635c6  repark-ae247767-dml.jsonl
e8c15b2056a160c9d1b5952f60ad949487ab6a1e182b2b27c27c88395933e2d8  repark-ae247767-floatkeys.jsonl
7d76bc4e48efd6d01fad2c7ca8b0b096ef7faa5d20da10095804d6cb69a3f4e3  repark-ae247767-reads.jsonl
998e9cf65b34a1e3311591bf3a679bf9510a3dd7e3e56c38182a9480b4a95b1c  spark-dml.jsonl
7b0ce44d5aee2b36a028a99eda0186b5b8bda1a36e50bfe0530a0044d646cc02  spark-floatkeys.jsonl
8d2f614b971003878ccd48598838dc71564a6e9c9c8e971c401c86ada5d914fa  spark-naaj-check.jsonl
9bc40cfd39d8082e3f28cc5d23034def5644b52024c9b121100afb044185d02d  spark-reads.jsonl
```

Rerun: set `SZB_CLONE` (the RePark tree), `SZB_WORK` (a scratch directory), and for Spark `SZB_SPARK_PYTHON`,
`JAVA_HOME` and `SZB_ICEBERG_JAR`; then `bash run_leg.sh <spark|repark> <reads|dml|floatkeys> <out.jsonl>` inside
the wrapper above (Spark legs also under the JVM lock; `SZB_RAW` redirects the output directory), then
`SZB_CLONE=<tree> <tree>/.venv/bin/python -I label_render.py` (`--raw-file`, `--out` and `--base` label other legs).

## Truth, Spark and RePark columns (as part A)

- **Reads.** Truth = Spark SQL over inline `VALUES` views of the same rows (`layout view`). The Spark one-file
  Iceberg answer on the same route is the `same` column `label_of` takes. **Oracle self-check:** on every read cell,
  every Spark route, the view and the one-file Iceberg answer agree, except PR-2 (four cells): Spark-on-Iceberg drops
  the NaN row under `t.d >= -1.0`, which the view keeps (the AUD-024 NaN class; the truth is the view, and RePark
  matches it on that row).
- **Writes, partitions, floatkeys.** Truth = Spark on one-file Iceberg tables of the same kind (no zero-only files
  are involved, so no PRUNED-WRONG case arises).
- **Native.** Compared with Spark's `spark-sql` answer (Spark has no native door; E9 Trino is still ORACLE-MISSING).
- **Typed-only cell.** GB-11's typed pivot has no SQL twin, so its truth is Spark's typed answer.
- **Labels.** `label_of` from part A, unchanged. `ORDER-DIFFER` is its label for "same rows, different order"; on
  OB-1, OB-3 and NN-4 the order is fully determined by a tie-breaker, so those are wrong orders (table §7).

## Seeds

`t` (ids 1–7): zeros of both signs in the key (`0.0, -0.0, 0.0, -0.0`), a canonical NaN, `1.5`, NULL; second float
columns `e`/`fe` with the opposite sign on ids 1 and 2; `g = id % 2`; decimal columns `m`/`me` as the control
(`-0.00` is `0.00`, NaN becomes `9.99`). Builds: `up = {0.0, NaN}`, `un = {-0.0, NaN}` (single-sign, Fable's finding 2),
`ub = {0.0, -0.0}` (MERGE cardinality, union of a both-sign input). `nn`: canonical NaN, `sqrt(-1.0)`, `1.0`, `0.0`,
`-0.0`, `-NaN`. Every seed is read back with its bit patterns (table §6): both engines keep `-0.0`; RePark keeps
the sign-set NaN bits (`fff8…`) in Iceberg, Spark's Iceberg write stores `7ff8…` for them.

## Side checks

1. **`idf_check.py`** (Spark door, a temp view). `d IS DISTINCT FROM e` with two columns refuses with
   `UNRESOLVED_COLUMN … spark_catalog.default.e` on main **and** on a v1.5.6 release-branch build (head `273ff1870`,
   which does not contain part A); `NOT (d <=> e)` and `d IS DISTINCT FROM 1.5` answer. Pre-existing, not part A.
2. **`partition_check.py`** (one table `PARTITIONED BY (d)`, rows `0.0, -0.0, 0.0, -0.0, NaN, 1.5, NULL`), output:

   ```
   repark .partitions  [('-0.0', 4), ('1.5', 1), (None, 2)]
   repark .files       [('-0.0', 2), ('0.0', 2), ('1.5', 1), (None, 1), (None, 1)]
   spark  .files       [('-0.0', 2), ('0.0', 2), ('1.5', 1), ('NaN', 1), (None, 1)]
   both: SELECT id, d  keeps every stored sign
   ```

   RePark writes separate `-0.0` and `0.0` partitions like Spark, but its `.files` table shows the NaN partition
   value as NULL, and its `.partitions` table merges the two zeros and NaN with NULL. Whether the NaN partition value
   is NULL in the manifest itself (a cross-engine read) was NOT MEASURED.
3. **`naaj_check.py`** (Spark only): `d NOT IN (SELECT d FROM u)` with `u = {0.0}` returns `[2, 6]` (keeps `-0.0`)
   under the default null-aware broadcast anti join, whether or not broadcast is allowed, and `[6]` with
   `spark.sql.optimizeNullAwareAntiJoin = false` (a nested-loop join). `IN`, `NOT EXISTS` and a literal `NOT IN`
   are IEEE in every configuration.

## Deviations and limits

- One Spark JVM at a time; legs were re-run after cell additions (`F.expr` aggregate spelling, PR-1/PR-2,
  SQ-notin-ieee and WR-insert-notin-ieee, CM-12 and JN-natural, and the `ln(-1.0)` NaN row dropped because Spark
  answers NULL there and RePark NaN, a function-level divergence unrelated to keys). Only the final raw files are kept.
- The DataFrame routes have no form for some sites (subqueries, comma joins, sort-merge settings); those columns are
  empty in the per-site table, not NOT MEASURED.
- The native door cannot express MERGE, DML with a subquery, CTAS on the memory catalog or temporary views; those
  cells are NATIVE-REFUSED on every type, including the decimal control.
- Not measured in step 0: spill (D16), plan shape (D14), timing (the cost gates), Spark-written tables read by
  RePark (E2), Trino (E9).
- Box load at the legs' start: 8 to 14 (one-minute average); no timing claim depends on it.

## Filing (2026-11-12)

- The step-0 files were filed unchanged except: the RePark legs' header field `repark_file` now holds the
  tree-relative path (it held the scratch clone's absolute path; the hashes above are of the filed files);
  `run_leg.sh` takes its paths from the environment; `szb_probe.py` records the tree-relative path;
  `label_render.py` gained `--raw-file`, `--out`, `--base` and `--title`, and without them still writes `labels.jsonl` and
  `table.md` byte for byte as before; `szb_cells.py` gained the #1307 seeds as `matrix` and kept the old ones as
  `bare`.
- `named-seats.md` (and so `table.md` §7–§8) records Q3 as resolved on main by #1307.
- The Q3 re-run on main `276dc503` is in [../q3-rerun/](../q3-rerun/table.md): the same release module (the sha256
  above; #1307 changed test files only), the floatkeys part of both engines, labelled with `label_render.py`. Its
  RePark leg header says `clone_dirty: true` because the filed evidence was not yet committed; the product tree was
  `276dc503` exactly.
