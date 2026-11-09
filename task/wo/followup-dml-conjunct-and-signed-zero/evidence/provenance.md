# Provenance — DML conjunct drop and signed zero (envelope follow-up, measurement only)

Base: `origin/main` at `36dc10302` (#1270); branch `docs/followup-dml-conjunct-and-signed-zero`. The RePark leg is
the release wheel built from `fdc61f26`. Between `fdc61f26` and the base, nothing under `crates/` or in the wheel's
Python source changed. The seven changed files are `STATUS.md`, a design document, task documents, one line in a
parity test and one line in a gate script. The wheel therefore stands for `main`. Paths below are placeholders:
`<scratch>` is the session scratch directory, `<warehouse>` a per-leg catalog root, `<oracle-python>` the PySpark 4.1.2
interpreter, `<ivy-jar>` the Iceberg Spark runtime jar, `<venv-main-fdc61f26>` the RePark venv, `<main-wheel>` the
built wheel, `<jdk-17>` the Zulu 17 JDK.

## Artifacts and their hashes

| Leg | Artifact | sha256 | Python | pyarrow | Version |
|---|---|---|---|---|---|
| `spark` | Spark 4.1.2 on JDK 17.0.15, `local[1]`, UI off, log level ERROR, `IcebergSparkSessionExtensions`, a Hadoop catalog with a private name | Iceberg jar `d6ea6c5d099288daeb7d5a92061bd3d7d8f296492632b42378e5f2f0e3066242` | 3.12.3 | not recorded | spark 4.1.2 |
| `repark-main-fdc61f26` | `repark-1.5.5-cp312-abi3-manylinux_2_39_x86_64.whl` (release build of `fdc61f26`) | `07f94d4dcf9852b7a6f895382efe4fdc3bbb6c1d090c807e15d9191850ead479` | 3.12.3 | 26.0.1 | 1.5.5 |

The first JSONL line of each raw file (`"record": "leg"`) records the same facts, the session time zone
(`America/New_York` on both engines), the CREATE and seed templates, and the setup result per table. Every setup
statement succeeded on both engines, including the native FLOAT tables.

## Commands run (placeholders for paths)

One shell script ran the four legs serially, one JVM at a time, deleting each warehouse before its leg. Spark first,
then RePark. The two axes share one script (`--table conjunct` and `--table signed-zero`).

```
JAVA_HOME=<jdk-17> <oracle-python> -I probe_followup.py --table conjunct --engine spark --label spark \
    --warehouse <warehouse> --out raw/spark-conjunct.jsonl --ivy-jar <ivy-jar>
<venv-main-fdc61f26>/bin/python -I probe_followup.py --table conjunct --engine repark \
    --label repark-main-fdc61f26 --warehouse <warehouse> --out raw/repark-main-conjunct.jsonl --wheel <main-wheel>
JAVA_HOME=<jdk-17> <oracle-python> -I probe_followup.py --table signed-zero --engine spark --label spark \
    --warehouse <warehouse> --out raw/spark-signed-zero.jsonl --ivy-jar <ivy-jar>
<venv-main-fdc61f26>/bin/python -I probe_followup.py --table signed-zero --engine repark \
    --label repark-main-fdc61f26 --warehouse <warehouse> --out raw/repark-main-signed-zero.jsonl --wheel <main-wheel>
python3 -I probe_followup.py --render --table conjunct --spark raw/spark-conjunct.jsonl \
    --main raw/repark-main-conjunct.jsonl --tables-dir .
python3 -I probe_followup.py --render --table signed-zero --spark raw/spark-signed-zero.jsonl \
    --main raw/repark-main-signed-zero.jsonl --tables-dir .
uvx ruff@0.15.22 format probe_followup.py && uvx ruff@0.15.22 check probe_followup.py
```

Cell counts. Table A (conjunct): Spark 360 (answered 330, refused 30); RePark 504 (answered 378, refused 102, not
expressible 24). Table B (signed zero): Spark 132 (answered 126, refused 6); RePark 190 (answered 186, refused 4).
Wall clock: the conjunct Spark leg about 6 minutes, the conjunct RePark leg under a minute, the signed-zero legs
about a minute and a few seconds.

## Table A — the plan

Thirty-six predicates `x{atom}f{form}`: six atoms (`x1` `st.y IS NULL`, `x2` `st.x = 1`, `x3` `m['k'] = 1`, `x4`
`CASE WHEN i = 1 THEN true END`, `x5` `element_at(arr, 1) = 1`, `x6` `coalesce(i, 0) = 0`) by six forms (`f1`
`id = 1 AND X`, `f2` `X AND id = 1`, `f3` `id = 1 OR X`, `f4` `NOT (X) AND id = 1`, `f5` `id IN (1, 2) AND X`,
`f6` `(X) = true`). Seats: `select`, `delete`, `update`, `merge` (`WHEN MATCHED AND Q`). `spark-sql` runs `select` on
v2-cow (the precedent's machinery) and the three write seats on v2-cow, v2-mor and v3 (36 + 324 = 360 cells).
`native` runs all four seats on v2-cow (144 cells, of which 24 are not expressible: `x3` needs the `MAP` column the
native door cannot create). The seed table is the precedent's eight-row table; each cell runs on fresh tables.

Deviation: the merge text. The precedent's merge is `USING (SELECT id FROM t) s ON t.id = s.id`. With predicates that
name `id`, the bare `id` is ambiguous inside `WHEN MATCHED AND`, and Spark refused 90 merge cells with
`AMBIGUOUS_REFERENCE` on the first run of this round. That run was discarded. The committed merge text is
`USING (SELECT id AS sid FROM t) s ON t.id = s.sid`, so `id` in the predicate resolves to the target only. The
precedent's predicates never named `id`, so its merge results are unaffected by this change.

Native MERGE: the native door refuses every `MERGE INTO` statement (`Unsupported SQL statement`), so the native merge
seat is `NATIVE-REFUSED` regardless of predicate (30 expressible cells).

## Table A — label rules

The matrix's rules hold unchanged (`probe_envelope.py` in the precedent, `--render`). One label is added:
`CONJUNCT-DROPPED` on a `delete`, `update` or `merge` cell whose statement answered and whose touched set is not a
subset of its own door's `SELECT id ... WHERE P` set. The touched set is the ids a write changed or removed, plus any
id whose other columns changed (`ids` ∪ `lost` ∪ `damaged`). `CONJUNCT-DROPPED` joins the finding labels that
`KNOWN-EMPTY-DML` keys on. The label was also checked outside the probe: recomputing touched against the own select
from the raw rows gave an identical result. The touched set equals the own select in all 296 answered Spark DML cells
and all 312 answered RePark DML cells (227 and 242 of those have a non-empty select).

## Table B — the plan

Four variants: `t` (the precedent's eight-row seed, `d DOUBLE`, row 4 `-0.0`, row 6 `0.0`), `tf` (the same seed with
`d FLOAT`), `u` (`id BIGINT, d DOUBLE`, rows `(10, 0.0)` and `(11, -0.0)`) and `uf` (the FLOAT variant of `u`).

Reads on `v2-cow`:

- Seven comparison forms `c1`–`c7` (`d = 0.0`, `d = -0.0`, `d <=> 0.0`, `d IN (0.0)`, `d <> 0.0`, `d < 0.0`,
  `d <= 0.0`) on all four variants, through `spark-sql`, `native`, `filter-str` and `f-expr` (the last two are the
  DataFrame doors; Spark runs `spark-sql`, `filter-str` and `f-expr`; RePark runs all four). Spark 84 cells;
  RePark 112 cells.
- Six single-table operations on `spark-sql` and `native`, on each of the four variants: `GROUP BY d` with a count
  (`grp`), `SELECT DISTINCT d` (`dst`), `ORDER BY d` (`ord`, the id sequence is compared), `CAST(d AS STRING)` (`cst`),
  `max(d)` and `min(d)` over rows 4 and 6 of `t`/`tf` or rows 10 and 11 of `u`/`uf` (`mxmn`), and
  `count(*) OVER (PARTITION BY d)` (`win`). 24 cells per SQL door.
- Three pair operations on `spark-sql` and `native`, on `t`+`u` and `tf`+`uf`: the join `t.d = u.d` (`jn`), `INTERSECT`
  (`isc`) and `EXCEPT` (`exc`). 6 cells per SQL door.

Writes, `spark-sql` only, on all three kinds (18 cells per engine): `m1` `MERGE INTO t AS t USING u AS u ON t.d = u.d
WHEN MATCHED THEN UPDATE SET flag = 'z'` on `t`+`u` and `tf`+`uf`; `m2` `DELETE FROM t WHERE d = 0.0` and `m3`
`UPDATE t SET flag = 'z' WHERE d = 0.0` on `t` and `tf`. The brief's fixed list has one DML form (the MERGE); `m2` and
`m3` are the clerk's addition, because the brief asks for "the DML forms" in the plural and a sign-of-zero predicate
in a DELETE or UPDATE is where a write would reach. The writes read back `SELECT id, d, flag ... ORDER BY id`.

Native spellings: the native door is given the Trino-style alternative first and takes the first statement that is not
refused at parse or analysis time (the precedent's rule): `d IS NOT DISTINCT FROM 0.0` for `c3`, and `CAST(d AS
VARCHAR)` for `cst`. The Spark and DataFrame doors run the primary spelling only. The first run of this round let the
Spark door take the alternative, which `CAST(... AS VARCHAR)` refuses without a length; that run was discarded, and
both signed-zero legs were rerun with the primary spelling on the non-native doors. The committed raw files are from
that rerun.

Native reads are compared with the Spark `spark-sql` result for the same statement. Table A judges native against its
own select because there is no Trino oracle for predicates; the Table B operations are plain SQL that the Spark oracle
answers, so the comparison is against Spark.

## Table B — label rules

Both-answered cells compare the canonical multiset of result rows (each row's normalised values, sorted). A
difference is `WRONG-READ` for reads and `WRONG-WRITE` for writes (the post-state of `t`). `ORDER-DIFFER` is set when the
`ORDER BY` sequence differs although the multiset agrees. `SIGN-ONLY` is set when the difference disappears once every
`-0.0` is read as `0.0`, so only the sign of a displayed zero differs; membership differences are never `SIGN-ONLY`.
The refusal labels follow the Table A rules: `BOTH-REFUSED`, `ACCEPTED-WHERE-SPARK-REFUSED`,
`REFUSED-WHERE-SPARK-ACCEPTED`, and `NATIVE-REFUSED` where the native door refused a statement Spark answered. `PANIC`,
`SETUP-REFUSED` and `ORACLE-MISSING` did not occur. The normalised values keep the sign of zero (`"-0.0"` and
`"0.0"` are distinct strings), so the raw files record exactly what each engine returned.

## Table B — literal typing

Checked after the legs, with a five-statement Spark session and the RePark facade (not committed; the statements and
results are recorded here):

- Spark: `typeof(-0.0)` is `decimal(1,1)`, and `typeof(0.0)` is `decimal(1,1)`. A bare `-0.0` literal has no sign on
  Spark. `CAST('-0.0' AS DOUBLE) = 0.0` is true; `CAST('-0.0' AS DOUBLE) = -0.0` is true; `CAST('-0.0' AS DOUBLE) < 0.0`
  is false.
- RePark facade: `CAST('-0.0' AS DOUBLE) = 0.0` is false; `CAST('-0.0' AS DOUBLE) < 0.0` is true; `CAST('-0.0' AS
  DOUBLE) = -0.0` is false. The facade reads the `-0.0` literal as `+0.0`.
- RePark native: the `-0.0` literal is a signed double, so `d = -0.0` matches the `-0.0` row and not the `0.0` row
  (`c2` on `t`: native `(4)`, facade `(6)`).

## Deviations and choices the brief left open

- **Merge text** (Table A): see above; the precedent's `id` join is ambiguous once a predicate names `id`.
- **Discarded runs.** Two runs were discarded and their raw files replaced: the first conjunct run (ambiguous merge
  reference) and the first signed-zero run (non-native doors took the Trino alternative). Both are described above;
  the committed raw files are from the reruns, and nothing else was rerun in between.
- **Native CAST spelling** (Table B): the native door's `cst` uses the Trino-style `VARCHAR` alternative, which it
  accepts. Its `Infinity` renders as `inf`, a rendering difference from Spark's `Infinity` and from the facade's
  `STRING` cast; the table reports it as `WRONG-READ` on `t` and `tf` and `EQUAL` on `u` and `uf`, where there is no
  infinity. Read it as a spelling-plus-rendering finding.
- **Native DML** (Table A): the native door refuses `delete` and `update` with "No field named" errors on `x1` and
  `x2` (all six forms each) and on `x4` and `x6` (forms f2, f4 and f6). These are the matrix's refusals, unchanged.
  `NATIVE-REFUSED` totals 66 cells across delete, update and merge, the 30 merge cells being the `MERGE` refusals.
- **Wheel and main.** See the opening paragraph.
- **Reproducibility.** After the runs, the script was reformatted with the pinned ruff and two string literals were
  split; no measurement code changed. The RePark legs were rerun with the final script into scratch: the conjunct
  leg matches the committed raw on every cell except the Iceberg snapshot identifiers, which are generated per run;
  the signed-zero leg matches except that the row order of four unordered operations (`grp`, `dst`, `isc`, `exc`)
  differs. In both, the multisets and labels are identical, and the rendered tables are unchanged.
- **Labels added:** `CONJUNCT-DROPPED` (Table A, as the brief asked). Table B uses `WRONG-READ`, `WRONG-WRITE`,
  `ORDER-DIFFER` and `SIGN-ONLY`, defined above; the brief's labels for Table B were not fixed.

## TIMESTAMP(9) cell (Part 1 of the 2026-11-08 extension)

Legs. All RePark legs run the release wheel built from `fdc61f26` (repark 1.5.5, wheel sha256 `07f94d4dcf98…`). The
Spark leg runs the Spark 4.1.2 oracle on JDK 17.0.15, with the Iceberg Spark runtime 1.11.0 jar (sha256
`d6ea6c5d0992…`). The session zone is `America/New_York` on the Spark and facade legs. Each leg ran in its own JVM or
interpreter, one at a time.

- `spark`: six statements, one JVM. Raw `timestamp9-spark.jsonl`.
- `repark-facade` base: three statements, the builder without `repark.sql.allowCreateFormatVersion3`. Raw
  `timestamp9-repark-facade-base.jsonl`.
- `repark-facade` v3: three statements, the flag in the builder config, `TBLPROPERTIES ('format-version'='3')`. Raw
  `timestamp9-repark-facade-v3.jsonl`.
- `repark-native` base: three statements, `PyReparkSession.native(config={})`, so no flag. Raw
  `timestamp9-repark-native-base.jsonl`.
- `repark-native` flag control (extra, not in the brief): the native base with the flag in the config. Raw
  `timestamp9-repark-native-flag-control.jsonl`.

Commands (placeholders for paths; the runner is a session script and is not committed):

```
JAVA_HOME=<jdk-17> <oracle-python> -I <runner> --probe <probe> --engine spark --phase all --label spark-4.1.2 --warehouse <warehouse> --out raw/timestamp9-spark.jsonl --ivy-jar <ivy-jar>
<venv-main-fdc61f26>/bin/python -I <runner> --probe <probe> --engine facade --phase base --label repark-main-fdc61f26 --warehouse <warehouse> --out raw/timestamp9-repark-facade-base.jsonl --wheel <main-wheel>
<venv-main-fdc61f26>/bin/python -I <runner> --probe <probe> --engine facade --phase v3 --label repark-main-fdc61f26 --warehouse <warehouse> --out raw/timestamp9-repark-facade-v3.jsonl --wheel <main-wheel>
<venv-main-fdc61f26>/bin/python -I <runner> --probe <probe> --engine native --phase base --label repark-main-fdc61f26 --warehouse <warehouse> --out raw/timestamp9-repark-native-base.jsonl --wheel <main-wheel>
<venv-main-fdc61f26>/bin/python -I <runner> --probe <probe> --engine native --native-allow-v3 --phase base --label repark-main-fdc61f26-native-flag-control --warehouse <warehouse> --out raw/timestamp9-repark-native-flag-control.jsonl --wheel <main-wheel>
```

Deviations and choices. The probe's facade and native sessions always carry the v3 flag. The base legs here run without
it, so the base rows are the no-flag results. The Spark catalog is named `ice`, not the probe's `probe_ice`, so that the
statement text matches the brief. The Spark v3 variants were refused at the column type before the property is read.
The `<tmp>` tokens in the raw headers replace the temporary directory of the wheel's import path.

## Signed-zero extension (Part 2 of the 2026-11-08 extension)

Settings are those of Table B: the Spark 4.1.2 oracle on the Iceberg 1.11.0 runtime (JDK 17.0.15, session zone
`America/New_York`) and the release wheel built from `fdc61f26` (repark 1.5.5) with the v3 create flag on both RePark
doors, as in the probe. The runner is a session script that imports the probe's context builders and seed strings; it
is not committed. The plan is 78 cells and 128 routes (99 of them on Spark). Four raw legs, with the file names in
`signed-zero-extension-table.md`: `spark-signed-zero-extension.jsonl`, `repark-main-signed-zero-extension.jsonl`,
`spark-signed-zero-spill.jsonl`, `repark-main-signed-zero-spill.jsonl`. The first run and the first spill run carried
defects; the legs were rerun in full, and only the rerun raw files are kept.

Commands (placeholders for paths):

```
JAVA_HOME=<jdk-17> <oracle-python> -I <runner> --probe <probe> --leg spark --label spark-4.1.2 --warehouse <warehouse> --out raw/spark-signed-zero-extension.jsonl --ivy-jar <ivy-jar>
<venv-main-fdc61f26>/bin/python -I <runner> --probe <probe> --leg repark --label repark-main-fdc61f26 --warehouse <warehouse> --out raw/repark-main-signed-zero-extension.jsonl --wheel <main-wheel> --spark-warehouse <spark-warehouse>
JAVA_HOME=<jdk-17> <oracle-python> -I <runner> --probe <probe> --leg spill-spark --label spark-4.1.2 --warehouse <warehouse> --out raw/spark-signed-zero-spill.jsonl --ivy-jar <ivy-jar>
<venv-main-fdc61f26>/bin/python -I <runner> --probe <probe> --leg spill-repark --label repark-main-fdc61f26 --warehouse <warehouse> --out raw/repark-main-signed-zero-spill.jsonl --wheel <main-wheel>
```

Deviations and choices the brief left open:

- The oracle for E1, E2, E3 and R1 is Spark's own Iceberg read, as the brief sets. The `truth` column is an IEEE
  computation from the seed values (NaN ordered above every number, as Spark orders it). It is not read from a Parquet
  reader. Spark's Iceberg read is wrong on the zero-only file (see the table); E4 (Spark's direct Parquet read) agrees
  with the truth.
- E2 is not measured. The RePark memory catalog registered on Spark's warehouse root does not list the Spark-written
  table (`table 'ice_sp.ns.x_sp' not found`); the copies return `NamespaceNotFound`.
- E3's oracle is a Spark-written table with the same page settings, not the RePark-written files. The RePark table is
  RePark-written. Page settings: `write.parquet.page-row-limit = 1000`, `write.parquet.page-size-bytes = 2048`.
- E4's RePark side is refused (`UNRESOLVABLE_TABLE_VALUED_FUNCTION`) on the native door. Not measured.
- E5 reads the brief as a 10,000,000-row table (`x_big`) joined with `u`, with `count(*)` for the join (the pairs are
  not materialised). Spark is not timed. The load average was 12.0 during the runs.
- E8 spill evidence is not captured: no spill counter was read. The Spark session sets `spark.memory.fraction` to
  0.01 and turns broadcast joins off; the native door runs with `memory_limit_gb=1`; the facade has no memory knob in
  this harness. Results are recorded, spill is not claimed.
- E9: `docker image ls` under rootless Docker listed no Trino image, so the oracle is ORACLE-MISSING and no image was
  pulled. The native answers are recorded.
- R5's Spark result is the reference. The positional-versus-by-name reading of struct equality is an inference from
  the two answers; neither the Spark source nor the RePark source was checked.
- PRUNED-WRONG is applied as the brief defines it: a row present in the data is missing from the answer and its
  file's statistics (min = max = `-0.0`) explain it. The E3 page-level case is inferred, not read from the page index.
