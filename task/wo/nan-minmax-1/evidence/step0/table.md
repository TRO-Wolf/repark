# NAN-MINMAX-1 step 0: the constant-file table, RePark against Spark 4.1.2

**Provenance.** Main `631d283c`, built with `maturin build --profile release` (module sha256
`3f6032403204912193ac9c9acd7e3e89e56c0d4cb8cdd64f3400a0dfff2e89f9`), run from the lane interpreter with the wheel's
contents on `PYTHONPATH`. Spark 4.1.2 `local[1]`, JDK 17, Iceberg Spark runtime 1.11.0, hadoop catalog, session zone
UTC, through the shared Spark slot. Every command ran under the shared measure lock on cores 48–57 with a 48 GiB
address-space limit. Model `claude-opus-5-5`, 2026-11-13. No product code changed. After the runs `probe.py`
changed only by `ruff format`.

**Data** (`probe.py files <root>`, written with pyarrow). Columns `id BIGINT`, `d DOUBLE`, `f FLOAT`; every other row
of `d` and `f` is a NaN and the rest are `1.0`, so each file's footer reports `min = max = 1.0`.

- **NaN shapes:** `c` canonical (`7ff8000000000000` / `7fc00000`), `s` sign-set (`fff8…` / `ffc00000`), `p` payload
  (`7ff8deadbeef0001` / `7fc00123`).
- **Layouts:** `one` (one file, one row group, 4 rows), `rg` (one file, three row groups of 2), `many` (four files of
  2 rows). `ctl` is a NaN-free control `{1.0, 1.0}`.

**Cells** (`probe.py <repark|spark> <root> <tag> <out> [legs]`).

- **read:** `SELECT id, d, f … ORDER BY id`, `count(*)`, `count(*) WHERE isnan(d)` and `isnan(f)`,
  `max(d), min(d), max(f), min(f)` ungrouped (the statistics shortcut), and `count(*) WHERE d = CAST('NaN' AS
  DOUBLE)`; the Spark door, and on RePark the native door (`native.read_parquet` + a temp view).
- **path:** the read paths of [paths.md](paths.md) over `one_c`.
- **roundtrip:** `spark.read.parquet(src).coalesce(1).write.parquet(out)`; `stored` is pyarrow's read of the written
  files, `read_back` the engine's own read, `rt_by_spark` Spark's read of RePark's files.
- **ingest:** `CREATE TABLE … USING iceberg` (Parquet; also Avro for `one_c`), then
  `spark.read.parquet(src).writeTo(t).append()`; `read_back` is the engine's read, `stored` pyarrow's read of the
  Parquet data files, `ingest_by_spark` Spark's read of RePark's table.

`=` is Spark's answer; a rows answer shows its row count and its NaN count per column. Spark's `rt_by_spark` and
`ingest_by_spark` cells compare with Spark's read of its own round trip and table.

**Result on main.** Every NaN of every constant file reads as `1.0` on both doors, for every NaN shape, layout and
float type; `isnan` counts 0; `max` answers `1.0` where Spark answers NaN. RePark's round trip and its Iceberg
ingestion store `1.0` for each NaN row (pyarrow and Spark read `1.0`). The NaN-free control and `count(*)` equal
Spark. `= CAST('NaN' AS DOUBLE)` keeps no row on both engines: both readers prune it by the NaN-free footer
(PARQUET-NAN-EQUALITY-1's pruning half, equal to Spark on the same file).

**The head** (branch `fix/nan-minmax-stats-1`, release build, module sha256
`6eafceab44f966e788c6ab9985b96abf81834be8c7680f6651fe5d2916a38b30`; legs `raw/repark-head.jsonl`, Spark's read of its
files and tables `raw/spark-4.1.2-cross-head.jsonl`). 126 of main's 136 unequal cells now equal Spark: every read, both
doors, the native `CREATE EXTERNAL TABLE`, the round trip and the ingestion (pyarrow and Spark read the NaN rows
back). An earlier head that demoted every float bound per file gave the same 126 cells (module `5dcb3b5a…`); it is
replaced on cost (see the card). Ten cells stay unequal:

- **Four refused paths**, unchanged: `` parquet.`p` ``, `CREATE TABLE … USING parquet`, the Spark door's
  `CREATE EXTERNAL TABLE`, and a native local location outside every warehouse.
- **Six `max`/`min` cells over a sign-set NaN** (`one_s`, `rg_s`, `many_s`, both doors): the head answers
  `max 1.0, min NaN` where Spark answers `max NaN, min 1.0`. Main answered `1.0, 1.0` from the footer, so its `min`
  was right by accident of the shortcut. Without the shortcut, `max` and `min` order a sign-set NaN below every
  number (Arrow's total order); main already answers so whenever the shortcut does not fire (a filter in the
  query, data in memory). That is NAN-MINMAX-1 part 2's float accumulator (sketch §4.2(a), inventory B1/B3); the
  facade test holds these cells as a strict xfail.

The row `rt_by_repark base:…` is the head reading main's round-trip files, which hold `1.0`; Spark reads the same.

| group | source | door | cell | Spark | base | head |
|---|---|---|---|---|---|---|
| read | one_c | sql | rows | 4 rows, NaN d 2 f 2 | 4 rows, NaN d 0 f 0 | = |
| read | one_c | sql | count | [[4]] | = | = |
| read | one_c | sql | isnan_d | [[2]] | [[0]] | = |
| read | one_c | sql | isnan_f | [[2]] | [[0]] | = |
| read | one_c | sql | minmax | [["NaN", "1.0", "NaN", "1.0"]] | [["1.0", "1.0", "1.0", "1.0"]] | = |
| read | one_c | sql | eq_nan_d | [[0]] | = | = |
| read | one_c | native | rows | 4 rows, NaN d 2 f 2 | 4 rows, NaN d 0 f 0 | = |
| read | one_c | native | count | [[4]] | = | = |
| read | one_c | native | isnan_d | [[2]] | [[0]] | = |
| read | one_c | native | isnan_f | [[2]] | [[0]] | = |
| read | one_c | native | minmax | [["NaN", "1.0", "NaN", "1.0"]] | [["1.0", "1.0", "1.0", "1.0"]] | = |
| read | one_c | native | eq_nan_d | [[0]] | = | = |
| read | one_s | sql | rows | 4 rows, NaN d 2 f 2 | 4 rows, NaN d 0 f 0 | = |
| read | one_s | sql | count | [[4]] | = | = |
| read | one_s | sql | isnan_d | [[2]] | [[0]] | = |
| read | one_s | sql | isnan_f | [[2]] | [[0]] | = |
| read | one_s | sql | minmax | [["NaN", "1.0", "NaN", "1.0"]] | [["1.0", "1.0", "1.0", "1.0"]] | [["1.0", "NaN", "1.0", "NaN"]] |
| read | one_s | sql | eq_nan_d | [[0]] | = | = |
| read | one_s | native | rows | 4 rows, NaN d 2 f 2 | 4 rows, NaN d 0 f 0 | = |
| read | one_s | native | count | [[4]] | = | = |
| read | one_s | native | isnan_d | [[2]] | [[0]] | = |
| read | one_s | native | isnan_f | [[2]] | [[0]] | = |
| read | one_s | native | minmax | [["NaN", "1.0", "NaN", "1.0"]] | [["1.0", "1.0", "1.0", "1.0"]] | [["1.0", "NaN", "1.0", "NaN"]] |
| read | one_s | native | eq_nan_d | [[0]] | = | = |
| read | one_p | sql | rows | 4 rows, NaN d 2 f 2 | 4 rows, NaN d 0 f 0 | = |
| read | one_p | sql | count | [[4]] | = | = |
| read | one_p | sql | isnan_d | [[2]] | [[0]] | = |
| read | one_p | sql | isnan_f | [[2]] | [[0]] | = |
| read | one_p | sql | minmax | [["NaN", "1.0", "NaN", "1.0"]] | [["1.0", "1.0", "1.0", "1.0"]] | = |
| read | one_p | sql | eq_nan_d | [[0]] | = | = |
| read | one_p | native | rows | 4 rows, NaN d 2 f 2 | 4 rows, NaN d 0 f 0 | = |
| read | one_p | native | count | [[4]] | = | = |
| read | one_p | native | isnan_d | [[2]] | [[0]] | = |
| read | one_p | native | isnan_f | [[2]] | [[0]] | = |
| read | one_p | native | minmax | [["NaN", "1.0", "NaN", "1.0"]] | [["1.0", "1.0", "1.0", "1.0"]] | = |
| read | one_p | native | eq_nan_d | [[0]] | = | = |
| read | rg_c | sql | rows | 6 rows, NaN d 3 f 3 | 6 rows, NaN d 0 f 0 | = |
| read | rg_c | sql | count | [[6]] | = | = |
| read | rg_c | sql | isnan_d | [[3]] | [[0]] | = |
| read | rg_c | sql | isnan_f | [[3]] | [[0]] | = |
| read | rg_c | sql | minmax | [["NaN", "1.0", "NaN", "1.0"]] | [["1.0", "1.0", "1.0", "1.0"]] | = |
| read | rg_c | sql | eq_nan_d | [[0]] | = | = |
| read | rg_c | native | rows | 6 rows, NaN d 3 f 3 | 6 rows, NaN d 0 f 0 | = |
| read | rg_c | native | count | [[6]] | = | = |
| read | rg_c | native | isnan_d | [[3]] | [[0]] | = |
| read | rg_c | native | isnan_f | [[3]] | [[0]] | = |
| read | rg_c | native | minmax | [["NaN", "1.0", "NaN", "1.0"]] | [["1.0", "1.0", "1.0", "1.0"]] | = |
| read | rg_c | native | eq_nan_d | [[0]] | = | = |
| read | rg_s | sql | rows | 6 rows, NaN d 3 f 3 | 6 rows, NaN d 0 f 0 | = |
| read | rg_s | sql | count | [[6]] | = | = |
| read | rg_s | sql | isnan_d | [[3]] | [[0]] | = |
| read | rg_s | sql | isnan_f | [[3]] | [[0]] | = |
| read | rg_s | sql | minmax | [["NaN", "1.0", "NaN", "1.0"]] | [["1.0", "1.0", "1.0", "1.0"]] | [["1.0", "NaN", "1.0", "NaN"]] |
| read | rg_s | sql | eq_nan_d | [[0]] | = | = |
| read | rg_s | native | rows | 6 rows, NaN d 3 f 3 | 6 rows, NaN d 0 f 0 | = |
| read | rg_s | native | count | [[6]] | = | = |
| read | rg_s | native | isnan_d | [[3]] | [[0]] | = |
| read | rg_s | native | isnan_f | [[3]] | [[0]] | = |
| read | rg_s | native | minmax | [["NaN", "1.0", "NaN", "1.0"]] | [["1.0", "1.0", "1.0", "1.0"]] | [["1.0", "NaN", "1.0", "NaN"]] |
| read | rg_s | native | eq_nan_d | [[0]] | = | = |
| read | rg_p | sql | rows | 6 rows, NaN d 3 f 3 | 6 rows, NaN d 0 f 0 | = |
| read | rg_p | sql | count | [[6]] | = | = |
| read | rg_p | sql | isnan_d | [[3]] | [[0]] | = |
| read | rg_p | sql | isnan_f | [[3]] | [[0]] | = |
| read | rg_p | sql | minmax | [["NaN", "1.0", "NaN", "1.0"]] | [["1.0", "1.0", "1.0", "1.0"]] | = |
| read | rg_p | sql | eq_nan_d | [[0]] | = | = |
| read | rg_p | native | rows | 6 rows, NaN d 3 f 3 | 6 rows, NaN d 0 f 0 | = |
| read | rg_p | native | count | [[6]] | = | = |
| read | rg_p | native | isnan_d | [[3]] | [[0]] | = |
| read | rg_p | native | isnan_f | [[3]] | [[0]] | = |
| read | rg_p | native | minmax | [["NaN", "1.0", "NaN", "1.0"]] | [["1.0", "1.0", "1.0", "1.0"]] | = |
| read | rg_p | native | eq_nan_d | [[0]] | = | = |
| read | many_c | sql | rows | 8 rows, NaN d 4 f 4 | 8 rows, NaN d 0 f 0 | = |
| read | many_c | sql | count | [[8]] | = | = |
| read | many_c | sql | isnan_d | [[4]] | [[0]] | = |
| read | many_c | sql | isnan_f | [[4]] | [[0]] | = |
| read | many_c | sql | minmax | [["NaN", "1.0", "NaN", "1.0"]] | [["1.0", "1.0", "1.0", "1.0"]] | = |
| read | many_c | sql | eq_nan_d | [[0]] | = | = |
| read | many_c | native | rows | 8 rows, NaN d 4 f 4 | 8 rows, NaN d 0 f 0 | = |
| read | many_c | native | count | [[8]] | = | = |
| read | many_c | native | isnan_d | [[4]] | [[0]] | = |
| read | many_c | native | isnan_f | [[4]] | [[0]] | = |
| read | many_c | native | minmax | [["NaN", "1.0", "NaN", "1.0"]] | [["1.0", "1.0", "1.0", "1.0"]] | = |
| read | many_c | native | eq_nan_d | [[0]] | = | = |
| read | many_s | sql | rows | 8 rows, NaN d 4 f 4 | 8 rows, NaN d 0 f 0 | = |
| read | many_s | sql | count | [[8]] | = | = |
| read | many_s | sql | isnan_d | [[4]] | [[0]] | = |
| read | many_s | sql | isnan_f | [[4]] | [[0]] | = |
| read | many_s | sql | minmax | [["NaN", "1.0", "NaN", "1.0"]] | [["1.0", "1.0", "1.0", "1.0"]] | [["1.0", "NaN", "1.0", "NaN"]] |
| read | many_s | sql | eq_nan_d | [[0]] | = | = |
| read | many_s | native | rows | 8 rows, NaN d 4 f 4 | 8 rows, NaN d 0 f 0 | = |
| read | many_s | native | count | [[8]] | = | = |
| read | many_s | native | isnan_d | [[4]] | [[0]] | = |
| read | many_s | native | isnan_f | [[4]] | [[0]] | = |
| read | many_s | native | minmax | [["NaN", "1.0", "NaN", "1.0"]] | [["1.0", "1.0", "1.0", "1.0"]] | [["1.0", "NaN", "1.0", "NaN"]] |
| read | many_s | native | eq_nan_d | [[0]] | = | = |
| read | many_p | sql | rows | 8 rows, NaN d 4 f 4 | 8 rows, NaN d 0 f 0 | = |
| read | many_p | sql | count | [[8]] | = | = |
| read | many_p | sql | isnan_d | [[4]] | [[0]] | = |
| read | many_p | sql | isnan_f | [[4]] | [[0]] | = |
| read | many_p | sql | minmax | [["NaN", "1.0", "NaN", "1.0"]] | [["1.0", "1.0", "1.0", "1.0"]] | = |
| read | many_p | sql | eq_nan_d | [[0]] | = | = |
| read | many_p | native | rows | 8 rows, NaN d 4 f 4 | 8 rows, NaN d 0 f 0 | = |
| read | many_p | native | count | [[8]] | = | = |
| read | many_p | native | isnan_d | [[4]] | [[0]] | = |
| read | many_p | native | isnan_f | [[4]] | [[0]] | = |
| read | many_p | native | minmax | [["NaN", "1.0", "NaN", "1.0"]] | [["1.0", "1.0", "1.0", "1.0"]] | = |
| read | many_p | native | eq_nan_d | [[0]] | = | = |
| read | ctl | sql | rows | 2 rows, NaN d 0 f 0 | = | = |
| read | ctl | sql | count | [[2]] | = | = |
| read | ctl | sql | isnan_d | [[0]] | = | = |
| read | ctl | sql | isnan_f | [[0]] | = | = |
| read | ctl | sql | minmax | [["1.0", "1.0", "1.0", "1.0"]] | = | = |
| read | ctl | sql | eq_nan_d | [[0]] | = | = |
| read | ctl | native | rows | 2 rows, NaN d 0 f 0 | = | = |
| read | ctl | native | count | [[2]] | = | = |
| read | ctl | native | isnan_d | [[0]] | = | = |
| read | ctl | native | isnan_f | [[0]] | = | = |
| read | ctl | native | minmax | [["1.0", "1.0", "1.0", "1.0"]] | = | = |
| read | ctl | native | eq_nan_d | [[0]] | = | = |
| path | one_c | sql | read_parquet | 4 rows, NaN d 2 f 2 | 4 rows, NaN d 0 f 0 | = |
| path | one_c | sql | format_load | 4 rows, NaN d 2 f 2 | 4 rows, NaN d 0 f 0 | = |
| path | one_c | sql | sql_parquet_path | 4 rows, NaN d 2 f 2 | ERR AnalysisException | ERR AnalysisException |
| path | one_c | sql | create_using_parquet | 4 rows, NaN d 2 f 2 | ERR AnalysisException | ERR AnalysisException |
| path | one_c | sql | create_external_stored_parquet | 4 rows, NaN d 2 f 2 | ERR UnsupportedOperationException | ERR UnsupportedOperationException |
| path | one_c | native | read_parquet | 4 rows, NaN d 2 f 2 | 4 rows, NaN d 0 f 0 | = |
| path | one_c | native | create_external_stored_parquet | 4 rows, NaN d 2 f 2 | ERR AnalysisException | ERR AnalysisException |
| roundtrip | one_c | file | stored | 4 rows, NaN d 2 f 2 | 4 rows, NaN d 0 f 0 | = |
| roundtrip | one_c | sql | read_back | 4 rows, NaN d 2 f 2 | 4 rows, NaN d 0 f 0 | = |
| roundtrip | one_s | file | stored | 4 rows, NaN d 2 f 2 | 4 rows, NaN d 0 f 0 | = |
| roundtrip | one_s | sql | read_back | 4 rows, NaN d 2 f 2 | 4 rows, NaN d 0 f 0 | = |
| roundtrip | one_p | file | stored | 4 rows, NaN d 2 f 2 | 4 rows, NaN d 0 f 0 | = |
| roundtrip | one_p | sql | read_back | 4 rows, NaN d 2 f 2 | 4 rows, NaN d 0 f 0 | = |
| roundtrip | rg_c | file | stored | 6 rows, NaN d 3 f 3 | 6 rows, NaN d 0 f 0 | = |
| roundtrip | rg_c | sql | read_back | 6 rows, NaN d 3 f 3 | 6 rows, NaN d 0 f 0 | = |
| roundtrip | rg_s | file | stored | 6 rows, NaN d 3 f 3 | 6 rows, NaN d 0 f 0 | = |
| roundtrip | rg_s | sql | read_back | 6 rows, NaN d 3 f 3 | 6 rows, NaN d 0 f 0 | = |
| roundtrip | rg_p | file | stored | 6 rows, NaN d 3 f 3 | 6 rows, NaN d 0 f 0 | = |
| roundtrip | rg_p | sql | read_back | 6 rows, NaN d 3 f 3 | 6 rows, NaN d 0 f 0 | = |
| roundtrip | many_c | file | stored | 8 rows, NaN d 4 f 4 | 8 rows, NaN d 0 f 0 | = |
| roundtrip | many_c | sql | read_back | 8 rows, NaN d 4 f 4 | 8 rows, NaN d 0 f 0 | = |
| roundtrip | many_s | file | stored | 8 rows, NaN d 4 f 4 | 8 rows, NaN d 0 f 0 | = |
| roundtrip | many_s | sql | read_back | 8 rows, NaN d 4 f 4 | 8 rows, NaN d 0 f 0 | = |
| roundtrip | many_p | file | stored | 8 rows, NaN d 4 f 4 | 8 rows, NaN d 0 f 0 | = |
| roundtrip | many_p | sql | read_back | 8 rows, NaN d 4 f 4 | 8 rows, NaN d 0 f 0 | = |
| roundtrip | ctl | file | stored | 2 rows, NaN d 0 f 0 | = | = |
| roundtrip | ctl | sql | read_back | 2 rows, NaN d 0 f 0 | = | = |
| ingest | one_c_parquet | sql | read_back | 4 rows, NaN d 2 f 2 | 4 rows, NaN d 0 f 0 | = |
| ingest | one_c_parquet | file | stored | 4 rows, NaN d 2 f 2 | 4 rows, NaN d 0 f 0 | = |
| ingest | one_c_avro | sql | read_back | 4 rows, NaN d 2 f 2 | 4 rows, NaN d 0 f 0 | = |
| ingest | one_s_parquet | sql | read_back | 4 rows, NaN d 2 f 2 | 4 rows, NaN d 0 f 0 | = |
| ingest | one_s_parquet | file | stored | 4 rows, NaN d 2 f 2 | 4 rows, NaN d 0 f 0 | = |
| ingest | one_p_parquet | sql | read_back | 4 rows, NaN d 2 f 2 | 4 rows, NaN d 0 f 0 | = |
| ingest | one_p_parquet | file | stored | 4 rows, NaN d 2 f 2 | 4 rows, NaN d 0 f 0 | = |
| ingest | rg_c_parquet | sql | read_back | 6 rows, NaN d 3 f 3 | 6 rows, NaN d 0 f 0 | = |
| ingest | rg_c_parquet | file | stored | 6 rows, NaN d 3 f 3 | 6 rows, NaN d 0 f 0 | = |
| ingest | rg_s_parquet | sql | read_back | 6 rows, NaN d 3 f 3 | 6 rows, NaN d 0 f 0 | = |
| ingest | rg_s_parquet | file | stored | 6 rows, NaN d 3 f 3 | 6 rows, NaN d 0 f 0 | = |
| ingest | rg_p_parquet | sql | read_back | 6 rows, NaN d 3 f 3 | 6 rows, NaN d 0 f 0 | = |
| ingest | rg_p_parquet | file | stored | 6 rows, NaN d 3 f 3 | 6 rows, NaN d 0 f 0 | = |
| ingest | many_c_parquet | sql | read_back | 8 rows, NaN d 4 f 4 | 8 rows, NaN d 0 f 0 | = |
| ingest | many_c_parquet | file | stored | 8 rows, NaN d 4 f 4 | 8 rows, NaN d 0 f 0 | = |
| ingest | many_s_parquet | sql | read_back | 8 rows, NaN d 4 f 4 | 8 rows, NaN d 0 f 0 | = |
| ingest | many_s_parquet | file | stored | 8 rows, NaN d 4 f 4 | 8 rows, NaN d 0 f 0 | = |
| ingest | many_p_parquet | sql | read_back | 8 rows, NaN d 4 f 4 | 8 rows, NaN d 0 f 0 | = |
| ingest | many_p_parquet | file | stored | 8 rows, NaN d 4 f 4 | 8 rows, NaN d 0 f 0 | = |
| ingest | ctl_parquet | sql | read_back | 2 rows, NaN d 0 f 0 | = | = |
| ingest | ctl_parquet | file | stored | 2 rows, NaN d 0 f 0 | = | = |
| rt_by_repark | spark:one_c | sql | read | 4 rows, NaN d 2 f 2 | = | = |
| rt_by_repark | spark:one_s | sql | read | 4 rows, NaN d 2 f 2 | = | = |
| rt_by_repark | spark:one_p | sql | read | 4 rows, NaN d 2 f 2 | = | = |
| rt_by_repark | spark:rg_c | sql | read | 6 rows, NaN d 3 f 3 | = | = |
| rt_by_repark | spark:rg_s | sql | read | 6 rows, NaN d 3 f 3 | = | = |
| rt_by_repark | spark:rg_p | sql | read | 6 rows, NaN d 3 f 3 | = | = |
| rt_by_repark | spark:many_c | sql | read | 8 rows, NaN d 4 f 4 | = | = |
| rt_by_repark | spark:many_s | sql | read | 8 rows, NaN d 4 f 4 | = | = |
| rt_by_repark | spark:many_p | sql | read | 8 rows, NaN d 4 f 4 | = | = |
| rt_by_repark | spark:ctl | sql | read | 2 rows, NaN d 0 f 0 | = | = |
| path | one_c | native | create_external_under_warehouse | 4 rows, NaN d 2 f 2 | 4 rows, NaN d 0 f 0 | = |
| rt_by_repark | base:one_c | sql | read | 4 rows, NaN d 0 f 0 | . | = |
| rt_by_repark | base:one_s | sql | read | 4 rows, NaN d 0 f 0 | . | = |
| rt_by_repark | base:one_p | sql | read | 4 rows, NaN d 0 f 0 | . | = |
| rt_by_repark | base:rg_c | sql | read | 6 rows, NaN d 0 f 0 | . | = |
| rt_by_repark | base:rg_s | sql | read | 6 rows, NaN d 0 f 0 | . | = |
| rt_by_repark | base:rg_p | sql | read | 6 rows, NaN d 0 f 0 | . | = |
| rt_by_repark | base:many_c | sql | read | 8 rows, NaN d 0 f 0 | . | = |
| rt_by_repark | base:many_s | sql | read | 8 rows, NaN d 0 f 0 | . | = |
| rt_by_repark | base:many_p | sql | read | 8 rows, NaN d 0 f 0 | . | = |
| rt_by_repark | base:ctl | sql | read | 2 rows, NaN d 0 f 0 | . | = |
| rt_by_spark | base:one_c | sql | read | 4 rows, NaN d 2 f 2 | 4 rows, NaN d 0 f 0 | . |
| rt_by_spark | base:one_s | sql | read | 4 rows, NaN d 2 f 2 | 4 rows, NaN d 0 f 0 | . |
| rt_by_spark | base:one_p | sql | read | 4 rows, NaN d 2 f 2 | 4 rows, NaN d 0 f 0 | . |
| rt_by_spark | base:rg_c | sql | read | 6 rows, NaN d 3 f 3 | 6 rows, NaN d 0 f 0 | . |
| rt_by_spark | base:rg_s | sql | read | 6 rows, NaN d 3 f 3 | 6 rows, NaN d 0 f 0 | . |
| rt_by_spark | base:rg_p | sql | read | 6 rows, NaN d 3 f 3 | 6 rows, NaN d 0 f 0 | . |
| rt_by_spark | base:many_c | sql | read | 8 rows, NaN d 4 f 4 | 8 rows, NaN d 0 f 0 | . |
| rt_by_spark | base:many_s | sql | read | 8 rows, NaN d 4 f 4 | 8 rows, NaN d 0 f 0 | . |
| rt_by_spark | base:many_p | sql | read | 8 rows, NaN d 4 f 4 | 8 rows, NaN d 0 f 0 | . |
| rt_by_spark | base:ctl | sql | read | 2 rows, NaN d 0 f 0 | = | . |
| ingest_by_spark | base:one_c_parquet | sql | read | 4 rows, NaN d 2 f 2 | 4 rows, NaN d 0 f 0 | . |
| ingest_by_spark | base:one_c_avro | sql | read | 4 rows, NaN d 2 f 2 | 4 rows, NaN d 0 f 0 | . |
| ingest_by_spark | base:one_s_parquet | sql | read | 4 rows, NaN d 2 f 2 | 4 rows, NaN d 0 f 0 | . |
| ingest_by_spark | base:one_p_parquet | sql | read | 4 rows, NaN d 2 f 2 | 4 rows, NaN d 0 f 0 | . |
| ingest_by_spark | base:rg_c_parquet | sql | read | 6 rows, NaN d 3 f 3 | 6 rows, NaN d 0 f 0 | . |
| ingest_by_spark | base:rg_s_parquet | sql | read | 6 rows, NaN d 3 f 3 | 6 rows, NaN d 0 f 0 | . |
| ingest_by_spark | base:rg_p_parquet | sql | read | 6 rows, NaN d 3 f 3 | 6 rows, NaN d 0 f 0 | . |
| ingest_by_spark | base:many_c_parquet | sql | read | 8 rows, NaN d 4 f 4 | 8 rows, NaN d 0 f 0 | . |
| ingest_by_spark | base:many_s_parquet | sql | read | 8 rows, NaN d 4 f 4 | 8 rows, NaN d 0 f 0 | . |
| ingest_by_spark | base:many_p_parquet | sql | read | 8 rows, NaN d 4 f 4 | 8 rows, NaN d 0 f 0 | . |
| ingest_by_spark | base:ctl_parquet | sql | read | 2 rows, NaN d 0 f 0 | = | . |
| rt_by_spark | head:one_c | sql | read | 4 rows, NaN d 2 f 2 | . | = |
| rt_by_spark | head:one_s | sql | read | 4 rows, NaN d 2 f 2 | . | = |
| rt_by_spark | head:one_p | sql | read | 4 rows, NaN d 2 f 2 | . | = |
| rt_by_spark | head:rg_c | sql | read | 6 rows, NaN d 3 f 3 | . | = |
| rt_by_spark | head:rg_s | sql | read | 6 rows, NaN d 3 f 3 | . | = |
| rt_by_spark | head:rg_p | sql | read | 6 rows, NaN d 3 f 3 | . | = |
| rt_by_spark | head:many_c | sql | read | 8 rows, NaN d 4 f 4 | . | = |
| rt_by_spark | head:many_s | sql | read | 8 rows, NaN d 4 f 4 | . | = |
| rt_by_spark | head:many_p | sql | read | 8 rows, NaN d 4 f 4 | . | = |
| rt_by_spark | head:ctl | sql | read | 2 rows, NaN d 0 f 0 | . | = |
| ingest_by_spark | head:one_c_parquet | sql | read | 4 rows, NaN d 2 f 2 | . | = |
| ingest_by_spark | head:one_c_avro | sql | read | 4 rows, NaN d 2 f 2 | . | = |
| ingest_by_spark | head:one_s_parquet | sql | read | 4 rows, NaN d 2 f 2 | . | = |
| ingest_by_spark | head:one_p_parquet | sql | read | 4 rows, NaN d 2 f 2 | . | = |
| ingest_by_spark | head:rg_c_parquet | sql | read | 6 rows, NaN d 3 f 3 | . | = |
| ingest_by_spark | head:rg_s_parquet | sql | read | 6 rows, NaN d 3 f 3 | . | = |
| ingest_by_spark | head:rg_p_parquet | sql | read | 6 rows, NaN d 3 f 3 | . | = |
| ingest_by_spark | head:many_c_parquet | sql | read | 8 rows, NaN d 4 f 4 | . | = |
| ingest_by_spark | head:many_s_parquet | sql | read | 8 rows, NaN d 4 f 4 | . | = |
| ingest_by_spark | head:many_p_parquet | sql | read | 8 rows, NaN d 4 f 4 | . | = |
| ingest_by_spark | head:ctl_parquet | sql | read | 2 rows, NaN d 0 f 0 | . | = |

Unequal cells: base 136, head 10
