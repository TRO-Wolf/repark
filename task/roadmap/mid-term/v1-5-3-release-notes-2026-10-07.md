# v1.5.3 release notes (2026-10-07)

v1.5.3 is the third patch on 1.5.0, from `d0c50405` (v1.5.2) to `392f2350`. It carries the
attribute-identity stack, three security fixes that keep credentials out of every display and
error, the TA single-series performance series, the grown-stack repair, and one ANSI division
fix. The API freeze holds; the attribute-identity stack tightens behaviour toward Spark, as its
section says.

## Security: credentials are never displayed (SOURCE-URL-REDACT-1, -1-FN, -2)

- **SOURCE-URL-REDACT-1 (#967, `64615dde`).** A credential embedded in a URL- or DSN-shaped
  source or catalog property was shown in plain text by `sources()` and the config dump since
  v1.4.1; it is now masked. The same masking covers conf, namespace, table and view property
  values: `DESCRIBE TABLE EXTENDED`, `SHOW CREATE TABLE`, `SHOW TABLE EXTENDED`,
  `SHOW TBLPROPERTIES`, the `DESCRIBE NAMESPACE` rows, configuration and option refusals, and
  memory-catalog spans. `SET` redacts with Spark 4.1.2's default regexes (`url` and
  `access[.]?key` included), and secret-keyed properties display as `*********(redacted)`, at
  least where Spark 4.1.2 does. Oracle and TNS-descriptor logins, JSON and YAML values
  (multi-line included), and Azure account-key, `Authorization` and personal-access-token
  settings are masked too. Credential-free storage paths such as
  `s3://lake/data@corp.example.com/ns` display unchanged, and
  `spark.catalog.getDatabase(...).locationUri` returns the stored location.
- **SOURCE-URL-REDACT-1-FN (#975, `decdd708`).** The `spark.sql.caseSensitive`,
  `spark.sql.ansi.enabled` and Iceberg merge-schema refusals mask the rejected value, on the
  session builder too, and every Python-visible error masks URL userinfo.
- **SOURCE-URL-REDACT-2 (#976, `392f2350`).** A configuration value carrying a non-URL
  credential shape (a JDBC query string, a libpq keyword string or an ODBC connection string)
  no longer echoes at the builder, `repark.toml`, dbt, `spark.conf.set` and SQL `SET` doors, in
  `datafusion.*` engine text, or in the chained `ValueError` traceback of the integer knobs.
  RePark records each configuration and reader/writer option value that carries a credential
  and masks exactly those values in every error at the Python boundary; there are no
  engine-text heuristics. Error message parameters (`getMessageParameters()`) are masked, and
  a re-raised error chains to a masked copy, never the original. Errors raised from user code
  in UDFs, pandas and Arrow UDFs, UDTFs, `applyInPandas` / `applyInArrow` and
  `mapInPandas` / `mapInArrow` mask the same way. Text without a credential is unchanged, byte
  for byte. Known limits: an exception class with its own `__str__` is not rewritten
  (ledger C-070, open), and `foreach`, `foreachPartition` and `transform` pass the user's own
  exception through unchanged (follow-up FOREACH-WRAP-1).

Ledger:
[source-url-redact-1-ledger.md](../../ledgers/staging/source-url-redact-1-ledger.md).

## Attribute identity (ATTR-ID-1, #968, `b5214494`)

Columns carry Spark-style attribute ids. The ruled release note,
[attr-id-1-release-note-2026-10-03.md](attr-id-1-release-note-2026-10-03.md), ships as is and
is reproduced below. The stack merged after PERF-ATTR-STAMP-2 met its gate: the work-equal
like ratio on release builds, median of three, is **1.0812** against the 1.10x ceiling (gate
record of 2026-10-07 in
[perf-attr-stamp-2-card-2026-10-03.md](perf-attr-stamp-2-card-2026-10-03.md), with
STAMP-2-R5P6-1, #980).

### Ambiguous self-join references now raise, as in Spark

```python
s.join(d).select(d.v)
```

with `s = d.select("id")` now raises `_LEGACY_ERROR_TEMP_1182`, as Spark Classic does,
where v1.5.2 answered d's `v` (rows `[[10], [10], [20], [20]]`; `s` has no `v`).
Probe `release_note.py` `R1.s_join_d_select_dv`. The same holds after any join whose two
sides share lineage: a `select`, `filter`, `groupBy`, `orderBy`, `withColumn` or aggregate
over a column bound before the join raises when Spark cannot tell which side it points to.
The error is Spark's, word for word, including the `ambiguousAttrs` names.

Spark's switch `spark.sql.analyzer.failAmbiguousSelfJoin=false` turns the 1182 check
off, and references then resolve by id. It is not a return to the old answers: a
reference to a column the join renewed raises
`MISSING_ATTRIBUTES.RESOLVED_ATTRIBUTE_APPEAR_IN_OPERATION`, as Spark does, and the
pattern above is one of them.

A free name over duplicate column names now raises Spark's `AMBIGUOUS_REFERENCE`
throughout. v1.5.2 already raised on every probed shape (`AMBIGUOUS_REFERENCE` for
`filter` / `describe` / `summary`, an internal `Schema error` for `selectExpr` / `F.col`),
so the change is error-class normalisation, not a new refusal. Probes `release_note.py`
`R3.*` and `rn_extra.py`. Known gap: a USING self-join
`d.join(d.withColumnRenamed('id', 'id'), 'id').selectExpr('v')` still raises a schema
error (`rn_extra.py` `using_self.selectExpr`), where Spark raises `AMBIGUOUS_REFERENCE`.

### Each read of a SQL temp view mints fresh attribute ids, like Spark

```python
left = spark.table("sv"); right = spark.table("sv")
left.join(right, left["id"] == right["id"]).select(left["v"], right["v"])
```

with `sv` created by `CREATE TEMP VIEW sv AS SELECT * FROM tv` now answers
`[(10, 10), (20, 20)]`, as Spark 4.1.2 does. Ids mint per read, like Spark. For this
shape v1.5.2 already answered the same rows, so there is no behaviour change versus
v1.5.2. Probe `release_note.py` `R4`. A DataFrame view (`createOrReplaceTempView`) keeps
carrying the registered frame's ids on every read, as Spark does.

One shape still differs from Spark, and answers exactly as main does today:

```python
a = spark.table("sv"); a.join(d, a["id"] == d["id"]).select(d["v"])
```

Here `d` is the DataFrame behind `tv`. Spark refuses this with
`_LEGACY_ERROR_TEMP_1182` ("Column v#… are ambiguous"); this release answers it
from `d`'s side. The same holds for `.select(a["v"], d["v"])`, `d["id"]` and
`.filter(d["v"] > 10)`. Card VIEW-LINEAGE-SELFJOIN-1 owns the fix.

### Further changes toward Spark, versus v1.5.2

(a) A DataFrame-view two-read self-join now raises 1182 where v1.5.2 answered:

```python
left = spark.table("tv"); right = spark.table("tv")
left.join(right, left["id"] == right["id"]).select(left["v"], right["v"])
```

raises `_LEGACY_ERROR_TEMP_1182`, as Spark does; v1.5.2 answered `[(10, 10), (20, 20)]`.
Probe `R5.dfview_two_reads`.

(b) A cross-field self-join condition now raises 1182 where v1.5.2 answered `[]`:

```python
d.join(d, d.id == d.v)
```

Probe `X5.cross_field_self_join`.

(c) Aliased joins with qualified references now answer where v1.5.2 raised
`UNRESOLVED_COLUMN` / `No field named`:

```python
d.alias("l").join(o.alias("r"), F.col("l.id") == F.col("r.id")).select("l.v", "r.v")
```

answers `[[10, 5]]`, as Spark does. Probes `X1.aliased_join_qualified_cond`,
`X2.aliased_self_join` (`.select('a.v', 'b.v')`), and `R7.alias_fix`
(`s.alias("a").join(d.alias("b")).select(F.col("b.v"))`, four rows).

(d) `withColumnsRenamed` to duplicate names answers (EX-DF-18):

```python
spark.createDataFrame([(1, 2, 3)], "g INT, k INT, v INT").withColumnsRenamed({"g": "k", "k": "k"})
```

answers `[[1, 2, 3]]`, as Spark does; v1.5.2 raised a duplicate-names error.
Probe `X4.withColumnsRenamed_dup`.

(e) CASESENS-2 S1–S3 under `caseSensitive=true`: `select('ID')`, `df['ID']`, `fillna`
and `dropDuplicates` refuse, and `withColumn('VAL')` appends; under `false`,
`select('t.ID')` answers:

```python
q = spark.createDataFrame([(1, 2)], "id INT, Val INT")
q.select("ID")
```

under `true` raises `UNRESOLVED_COLUMN.WITH_SUGGESTION`, as Spark does; v1.5.2 answered
`[[1]]`. Under `true`, `q.withColumn("VAL", F.lit(9))` appends `[[1, 2, 9]]`, as Spark
does; v1.5.2 replaced `[[1, 9]]`. Under `false`, `q.alias("t").select("t.ID")` answers
`[[1]]`, as Spark does; v1.5.2 raised. Probes `CS.true.select_ID`,
`CS.true.getitem_ID`, `CS.true.fillna_VAL`, `CS.true.dropDuplicates_VAL`,
`CS.true.withColumn_VAL`, `CS.false.alias_qualified_T_ID`.

(f) `orderBy` of a duplicate name raises `UNRESOLVED_COLUMN.WITH_SUGGESTION` where
v1.5.2 raised `AMBIGUOUS_REFERENCE`:

```python
d.join(o, d.id == o.id).orderBy("v")
```

Probe `sort_dup.py` `join_orderBy_str` (and its six siblings).

(g) With `failAmbiguousSelfJoin=false`, `s2.join(d).select(d.v)` answers where v1.5.2
raised:

```python
s2 = d.select("id", "v")
s2.join(d).select(d.v)
```

answers four rows, as Spark does; v1.5.2 raised `AMBIGUOUS_REFERENCE`.
Probe `R2.off.s2_join_d_select_dv`.

(h) A sort over exact-duplicate projection outputs answers by the source column
(fold SM-2c, 2026-10-06):

```python
dup = f0.select((f0.v * -1).alias("v"), (f0.v + 1).alias("v"), f0.id)
dup.orderBy("v")
```

answers Spark's source-column rows; v1.5.2 raised `AMBIGUOUS_REFERENCE`.
Probe `cs_sort2.py` `sel_dup_exact` string cells.

### A sort by an unheld id over twins refuses, as v1.5.2 did

```python
frame.select((frame.v * -1).alias("V"), (frame.v + 1).alias("v"), frame.id).orderBy(frame.v.desc())
```

raises `AMBIGUOUS_REFERENCE`, as v1.5.2 did. Spark sorts by the original column through
a hidden projection; card MISSING-REF-RESOLVE-1 owns that gap. Probes `halt3.py`
`flip_twins_desc` / `flip_twins_asc`.

### The fix in user code

Alias the frames and reference the alias, as Spark's own message suggests:

```python
s.alias("a").join(d.alias("b")).select(F.col("b.v"))
```

The join stays the same cross join; only the reference changes. Where v1.5.2 raised
`No field named b.v`, this release answers four rows, as Spark does (probe `R7`).

### Qualified self-joins: written files and exports

A self-join whose two sides carry the same column names answers under this
release, where main could not plan it. The internal twin names
(`__repark_l_*`, `__repark_r_*`) never reach durable surfaces or exports
(fold SM-2, 2026-10-06; SM-2c C-3, 2026-10-06, closes the drop-then-write
leak, so a frame whose duplicate display name was dropped writes its display
names to files, tables and views). Two pre-existing shapes still show engine
names, exactly as on main: `EXPLAIN` prints the true engine plan (deliberate),
and some error texts name engine fields — the `bucketBy` lookup's
`Couldn't find column` listing and `F.col('id')` after
`l.join(r, l.id == r.id).drop(r.id)` raising a schema error over
`__repark_l_*` fields:

- Writes of duplicate display names refuse with Spark's exact
  `[COLUMN_ALREADY_EXISTS] The column `<name>` already exists. Choose another
  name or rename the existing column. SQLSTATE: 42711`, before any file is
  created. This covers parquet, json, orc, `saveAsTable` and `writeTo`,
  matching Spark's case rule (case-twin columns refuse only when the
  session is case-insensitive). Csv refuses exact duplicates only; it
  writes case-twin headers as Spark does. The exact-duplicate csv
  refusal is a deliberate divergence (row FA-5 in
  `docs/spark-sql-iceberg-parity.md` §5): Spark writes the duplicate header,
  which the engine cannot plan over; `insertInto` keeps writing positionally,
  as Spark does.
- Arrow exports (`pa.table(frame)`) and `toArrow` carry the display names
  (`id, s, v, id, s, v`), exactly as Spark's `arrow_dup` answers.
  `toPandas` carries the display names the same way. Raw polars consumers
  of a duplicate-name frame (`pl.from_arrow(frame)`, `pl.DataFrame(frame)`)
  now get polars' own `DuplicateError` instead of internal `__repark_`
  names; use `to_polars()`, which disambiguates with `__1` suffixes.
- `mapInPandas` and `mapInArrow` over a frame with duplicate display names
  refuse with Spark's exact `[AMBIGUOUS_REFERENCE]`, naming the first
  duplicate with its qualified candidates (SQLSTATE 42704), instead of
  running with internal names. A frame that selects the same column twice
  (one attribute id under two display names, e.g. `d.select(d.v, d.v,
  d.id)`) still runs, and the function sees the input names `v,v,id`;
  Spark renames them to `v_0,v_1,id` (named divergence, pin
  `test_map_in_pandas_same_origin_duplicate_input_names_divergence`).
  Scalar and grouped-aggregation pandas UDF input Series are positional
  (`_0`, `_1`, …), as on Spark.
- Temp views over exact-duplicate display names refuse with
  `[COLUMN_ALREADY_EXISTS]` (SQLSTATE 42711) instead of registering twin
  engine names. Case-twin columns register and answer. This is a deliberate
  divergence (row FA-6 in `docs/spark-sql-iceberg-parity.md` §5): Spark
  registers the view, which the engine cannot plan over yet.
- `freqItems`, `describe` and `transpose` frames written to parquet,
  `saveAsTable` or a temp view carry Spark's names (`id_freqItems`;
  `summary,id,s,v`; `key,a,b`), where v1.5.2 wrote `__repark_freq_items_0`,
  `f0/f1/f2` and `__repark_transpose_N` (fold SM-2c C-3, 2026-10-06, a fix
  in passing from the registration-boundary rename). Probe `c3.py`
  `freqItems|*`, `describe|*`, `transpose|*`.
- DataFrame `USING` joins keep the left key: left-side qualified key
  references answer on every join type, and right-side references answer
  on `inner` but refuse loudly on `left`/`right`/`full` instead of
  answering left values silently (on aliased frames; a stale `r["id"]`
  from an unaliased frame still answers left values, as in v1.5.2).
  `semi`/`anti` still refuse the missing
  side as Spark does (`42703`). `select("*")` and the unqualified key over
  `right` and `full` joins show the left key (`NULL` on right-outer
  rows); the coalesced star and per-side key values are a v1.5.3
  follow-up (USING-PER-SIDE-KEYS-1).


## TA indicators on one long series (TA-SINGLE-SERIES-PARALLEL-1)

- **Order (S1, #944, `538771bd`).** A sorted single-partition frame read back through
  `.eager()`, `.cache()` or `localCheckpoint` keeps its order on plain reads, as Spark does.
- **A bare `ta.*` column is a series (S2a, `8c09a8a5`).** A `ta.*` column without `.over(...)`
  is computed over the frame's declared sort order (`df.sort("ts")`, also through `.eager()`,
  `.cache()` and `localCheckpoint`). Without one, the first timestamp column (or, without one,
  the first date column) orders the series and RePark warns once per session; with neither,
  the frame's current row order is used. Before this change the answer depended on the order a
  split read delivered. `null_lookback=True` now applies to the bare spelling too. Use
  `.over(Window.partitionBy(...).orderBy(...))` for one series per instrument.
- **Speed (S0 `ffd1da5e`, S2b #940 `73f4a2e4`, S3 #945 `7e9cdc71`).** The ANSI nonzero-divisor
  guard scans typed buffers (bit-identical answers). A one-partition window runs its
  expression groups in parallel, and the TA glue stops copying its inputs and outputs. A
  one-partition projection runs in parallel, and a useless round-robin repartition over a
  window output is dropped. On the owner's TA bench these save 211 ms (S0), 106 to 122 ms
  (S2b) and 41 to 118 ms (S3); with S2a the bench runs at 0.71x of the previous main.
  The Rust `ReparkSessionBuilder::parallel_single_partition(false)` turns the parallel rules off.

## ANSI division by -0.0 (TA-SERIES-S0b, `667985d1`)

Under ANSI mode, division by `-0.0` raises `DIVIDE_BY_ZERO`, as Spark 4.1.2 does. A NaN divisor
still answers.

## Grown-stack repair (GROWN-STACK-GATE-1, `9f41347f`)

v1.5.2's deep-chain fix grew the stack unconditionally at 34 native call sites, which slowed
ordinary work by about 5.2 %. Catalog, option and scan-planning calls now run on the plain
runtime, and the frame and SQL-text sites grow the stack only when the query is deep. Answers
are unchanged, and deep chains still answer.

## Groundwork, not exposed

C-1, C-2a and C-2b (`repark-connect`, the Postgres connection for 1.6) and MB-0, MB-1, MB-2a and
the micro-batch crash harness (1.7) are on main but not reachable from the Python package:
`cargo tree -p repark-python -i repark-connect` finds no path, the core micro-batch source has
no caller, and `readStream` still raises not-implemented.

## Also in this release

PARITY-LIVE-STOP-1 (`29a87501`) stops tests from stopping the shared live-oracle Spark context,
so the nightly `parity-live` suite reads 4 failed and 0 errors instead of 156 failed and 7,565
errors. RP-57 (`afe9ee4a`) repins the fork to `076d5f98` (fork #365, #366) for the micro-batch
groundwork.

## Matrix

The 842-cell matrix on a fresh release build of the release candidate (`392f2350` with the
version bump, scoreboard 2026-10-07, the v1.5.2 harness and compare rules) reads
**705 EQUAL / 132 SPARK-CANNOT / 5 REFUSED-REGISTERED / 0 DIFFERENT**, unchanged from v1.5.2;
no cell changed its verdict.
