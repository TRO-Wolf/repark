# ATTR-ID-1 release note (2026-10-03)

A user-visible tightening that ships with the ATTR-ID-1 stack, not v1.5.2. Self-join
references Spark refuses now refuse in RePark too, with Spark's error. Every comparison
below is versus v1.5.2 (main).

## Ambiguous self-join references now raise, as in Spark

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

## Each read of a SQL temp view mints fresh attribute ids, like Spark

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

## Further changes toward Spark, versus v1.5.2

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

## A sort by an unheld id over twins refuses, as v1.5.2 did

```python
frame.select((frame.v * -1).alias("V"), (frame.v + 1).alias("v"), frame.id).orderBy(frame.v.desc())
```

raises `AMBIGUOUS_REFERENCE`, as v1.5.2 did. Spark sorts by the original column through
a hidden projection; card MISSING-REF-RESOLVE-1 owns that gap. Probes `halt3.py`
`flip_twins_desc` / `flip_twins_asc`.

## The fix in user code

Alias the frames and reference the alias, as Spark's own message suggests:

```python
s.alias("a").join(d.alias("b")).select(F.col("b.v"))
```

The join stays the same cross join; only the reference changes. Where v1.5.2 raised
`No field named b.v`, this release answers four rows, as Spark does (probe `R7`).

## Qualified self-joins: written files and exports

A self-join whose two sides carry the same column names answers under this
release, where main could not plan it. The internal twin names
(`__repark_l_*`, `__repark_r_*`) never escape the engine (fold SM-2,
2026-10-06; SM-2c C-3, 2026-10-06, closes the drop-then-write leak, so a
frame whose duplicate display name was dropped writes its display names to
files, tables and views):

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

## Pointers

- The rule and its dated residues: row EX-DF-21 in `docs/spark-sql-iceberg-parity.md`.
- Measured on Spark 4.1.2 (2026-10-03): the pattern raises 1182 naming `v`; the aliased
  cross join answers four rows; with the switch off the pattern raises `MISSING_ATTRIBUTES`.
- Up: [map.md](map.md)
