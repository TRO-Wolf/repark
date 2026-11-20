# CSV-STRING-FIELD-TEXT-1, the wiring unit — the bar (2026-11-20)

Head `3bde1153`, package `S/ddu-work/pkg-wire-3bde1153`; base main `9a48932f` (the kernel's merge, with #1373),
package `S/ddu-work/pkg-main-9a48932f`. `BUILT_FROM` names each commit, and every package run printed its
`repark._native` path. Spark 4.1.2 live (`local[1]`, the Iceberg 1.11.0 runtime jar), `TZ=UTC`, session zone UTC
unless a cell names one. Verdicts as in [../domain-table.md](../domain-table.md): EQUAL, WRONG (silent), RAISE
(loud), BOTHERR.

## The pins

`python/repark/tests/io/test_csv_string_field_text_1.py`: 42 read cells, 6 label writes, 3 refusals, the
`emptyValue` refusal, the plan and the option guard, each in both ANSI modes (108 tests). Every expected value is
Spark's recorded answer ([raw/pins-spark.jsonl](raw/pins-spark.jsonl), from [probes/pins.py](probes/pins.py));
Spark's 106 recorded cells (53 shapes in two modes) answer alike in both modes.

| group | cells | Spark | main | head |
|---|---|---|---|---|
| a `STRING` field over `007`, `1.50`, `1e3`, `TRUE`: the default read, `inferSchema=false`, a schema (`csv`, `format("csv").load`) | 4 | the text | `7`, `1.5`, `1000.0`, `true` (WRONG) | the text |
| a schema with `inferSchema=true` (four `STRING` fields; `x STRING, k STRING`; `x INT, k INT` over `007,abc`) | 3 | the schema wins: text, `(7, NULL)` | WRONG; RAISE `Cannot cast string 'abc'` | Spark's |
| N1, `a INT, b BIGINT` over `1e3,2.0` / `1.5,7` / `3,4` | 1 | `(NULL, NULL)`, `(NULL, 7)`, `(3, 4)` | `(1000, 2)`, `(1, 7)`, `(3, 4)` | Spark's |
| misfit NULL: `INT`, `BOOLEAN`, `DOUBLE`, `DECIMAL(10,2)` over `abc`, `2147483648`, `42`, ` 7 `, `1e3` | 4 | NULL per field, the row kept | RAISE (`DOUBLE` with ANSI off: EQUAL) | Spark's |
| pairing: `swap`, `part` (`INT`, `STRING`), an empty header name, case-only names | 6 | by position | by name: WRONG | Spark's |
| width: 3 columns and 2 fields (header on, on `INT`, off); 2 columns and 3 fields (header on, off, off `INT`) | 6 | the first M columns, NULL past the width | WRONG; header-off wide RAISE `reader schema has 3 fields but source has 2 columns`; `narrow_hdr_int` EQUAL | Spark's |
| the late misfit: 1,202 rows, `abc` at row 1,201 and `1.5` at row 1,202 (`STRING`, `INT`, the default read) | 3 | `(1202, 1202, 'abc')`, `(1202, 1200, '999')`, `(1202, 1202, 'abc')` | RAISE `Error while parsing value 'abc' as type 'Int64'` | Spark's |
| header names, no schema: `,k`; `NA,k` with `nullValue=NA`; `x,X,k` (under `spark.sql.caseSensitive=false`); ` x , k ` (and with a schema) | 5 | `_c0`; `_c0`; `x0, X1`; untrimmed kept | `''`; `NA`; RAISE `AMBIGUOUS_REFERENCE`; RAISE `No field named x` | Spark's |
| topology, local: `file://` (no schema, schema), `.csv.gz` with `compression`, `sep=';'`, a BOM, a same-header glob (no schema, schema), two files with headers in different orders (schema; the values only) | 9 | the text, positional | WRONG; the glob and two-file reads RAISE `Fail to merge schema` | Spark's |
| `emptyValue=""` | 1 | the text | WRONG | Spark's |
| the label path: four fields of `STRING`, `INT`, `DECIMAL(10,2)` over `007,1.50,1e3,TRUE`, by `writeTo().tableProperty("format-version","2").create()` and by `CREATE TABLE … USING iceberg` + `INSERT INTO … SELECT`, read back | 6 | `('007','1.50','1e3','TRUE')`, `(7, NULL, NULL, NULL)`, `(7.00, 1.50, 1000.00, NULL)` | stores `7`, `1.5`, `1000.0`, `true`; `(7, 1, 1000, 1)`; `(…, 1.00)` | stores Spark's |
| refused when the frame is planned: `DecimalType(2,5)`, `DecimalType(5,-2)`, `CHAR(5)` | 3 | `Decimal scale (5) cannot be greater than precision (2).`; `[NEGATIVE_SCALE_DISALLOWED]`; `[UNSUPPORTED_CHAR_OR_VARCHAR_AS_STRING]` | a run-time Arrow error; `unknown cast type`; answers (WRONG) | `AnalysisException` with Spark's text (BOTHERR) |

- **Red on main, green on the head.** Head: 108 of 108 pass. Main: 103 fail and 5 pass, the controls: `narrow_hdr_int`
  in both modes, `misfit/double` with ANSI off (a non-ANSI `CAST` of `abc` to `DOUBLE` is NULL), and the option guard
  in both modes.
- **`emptyValue`** other than `""` raises `reader option 'emptyValue' is not supported by repark yet` at the option
  gate (Spark answers; RePark used to drop the option silently). `emptyValue=""` reads, as Spark's default.
- **The option guard.** `text_scan` set by the user is dropped by `_native_options_for`, as `utf8_columns` is:
  `inferSchema=true` with `option("text_scan", "true")` keeps the inferred types (the `bigint` width is
  CSV-INFER-INT32-WIDTH, Q5's card).
- **The untrimmed-name raise, isolated.** On main the raise comes from the facade, not the scan: `F.col(" x ")`
  resolves the trimmed `x`, so both the schema read and the no-schema read raise `No field named x`. The head refers
  to each column by position (`frame[i]`) and returns a no-schema header read unprojected, so the names stay
  untrimmed, as Spark's.
- **A test changed.** `test_csv_infer_perf_1.py::test_header_embedded_newline_raises_until_record_aware_scan` pinned
  a raise for a header holding an embedded newline under `multiLine`, on both `inferSchema` reads. The text scan reads
  the header record-aware, so the `inferSchema=false` read now answers Spark's names and row (`id`, `no\nte`, `v`;
  `('1','a','2')`, measured): that half is a new test, and the `inferSchema=true` half still raises (DECLARED
  CSV-INFER-HEADER-NEWLINE narrows to `inferSchema=true`).

## Unit 0's bar and #1373's bar

- `python/repark/tests/io/test_csv_schema_timestamp_zone_1.py` (unit 0): 50 of 50 on the head and on main. It holds
  the `csv_s` and `csv_f` cells, the views and writes in another zone, F1's `DECIMAL` exponents beside a `TIMESTAMP`
  field, F2's `TIMESTAMP_NTZ` daylight-saving-gap cells, the five glob-residual spellings and the space and `#` paths.
- The revision's zone cells ([../oracle.md](../oracle.md), zones): Los Angeles, New York and Tokyo, 168 cells: main
  126 EQUAL, the head 168 (F2's gap cells keep the wall clock; the 42 moved are the field parse).
- `python/repark/tests/session/test_temp_view_frame_reader_parse_1.py` (#1373): 95 of 95 on the head and on main.
  `_load_csv` keeps `_reader_parse` around the kernel's frame.

## The scan topology

- **Local** (the pins above): `file://`, a glob, two files with headers in different orders (the values pinned, not
  the names: the first-listed file names the columns, readdir order locally, where Spark takes the largest file),
  `.csv.gz`, `sep`, a BOM, an empty name, untrimmed names, duplicate names.
- **S3 on moto** ([probes/s3.py](probes/s3.py); moto 5.2.3 from the local uv cache, `--offline`; no external call):
  12 cells in both modes, against Spark's answers for byte-identical local files ([raw/s3-spark.jsonl](raw/s3-spark.jsonl)).

  | cell | main | head |
  |---|---|---|
  | one file: no schema; `x STRING, k DOUBLE`; `x INT, k STRING` | WRONG (`7`, `1000.0`) | EQUAL |
  | a prefix of two files (`007`, `1.50`): no schema; `x DECIMAL(10,2)` | RAISE `Fail to merge schema` | EQUAL |
  | two files, headers in different orders: a schema; the values | RAISE | EQUAL |
  | `gzip` | WRONG | EQUAL |
  | the late misfit: `INT`, `STRING` | RAISE | EQUAL |
  | a glob `s3://…/multi/*.csv`: no schema; a schema | RAISE `No files found at s3://…/*.csv` | RAISE, the same |

  Head 20 of 24 EQUAL, main 0. **The S3 glob raises on both builds** before any CSV code runs: the listing finds
  nothing for a glob in an S3 URL. Loud, and not this unit's; named for the verifier and the owner.
- **Shapes** ([probes/shapes.py](probes/shapes.py), 15 cells): an empty file with a schema now answers Spark's empty
  frame (main RAISE); a directory whose first file is empty reads the other file's text (main WRONG); a header-only
  file and `multiLine` stay EQUAL; `x,X,k` answers `x0, X1, k` under `spark.sql.caseSensitive=false` (main RAISE;
  under `true` Spark and main answer `x, X, k`, fix round 1). Unchanged and loud on both: exact
  duplicate names (`x,x,k`, merged by the header-only schema, then a field-count raise), a short row (Spark fills
  NULL; card CSV-READ-MODES-AND-SHAPES-1), `INTERVAL DAY TO SECOND` in a DDL string (the facade's DDL parser), and
  `VOID`, `ARRAY`, `TIME` (Spark refuses too).

## The full re-run

Step 0's probes unchanged, on both packages one after the other ([probes/rerun.py](probes/rerun.py),
[raw/rerun-table.md.txt](raw/rerun-table.md.txt)); Spark's answers are step 0's recordings
([../evidence/raw/](../evidence/raw/map.md)). 13,050 records per build. Main reproduces step 0's counts exactly.

| set | cells | main EQUAL / WRONG / RAISE / BOTHERR | head |
|---|---|---|---|
| S, a user schema | 4,128 | 1,590 / 1,068 / 1,470 / 0 | **4,128** / 0 / 0 / 0 |
| N, no options | 556 | 264 / 216 / 76 / 0 | 480 / 0 / 76 / 0 |
| F, `inferSchema=false` | 480 | 264 / 216 / 0 / 0 | 480 / 0 / 0 / 0 |
| T, `inferSchema=true` | 480 | 288 / 192 / 0 / 0 | 288 / 192 / 0 / 0 |
| M, the two modes | 1,376 | 0 / 0 / 956 / 420 | the same |
| L, the label path | 684 | 320 / 216 / 148 / 0 | **684** / 0 / 0 / 0 |
| `kernel.py`'s wider texts | 88 | 49 / 18 / 21 / 0 | 88 / 0 / 0 / 0 |
| the oracle through the reader (16 types × 74 texts × 2 modes) | 2,368 | 644 / 379 / 1,185 / 160 | 1,906 / 0 / 296 / 166 |
| zones | 168 | 126 / 30 / 12 / 0 | 168 / 0 / 0 / 0 |
| topology (`P`) | 16 | 0 / 10 / 6 / 0 | 16 / 0 / 0 / 0 |
| pairing (`Q`) | 18 | 2 / 16 / 0 / 0 | 18 / 0 / 0 / 0 |
| extra (`X`) | 9 | 1 / 5 / 3 / 0 | 3 / 3 / 3 / 0 |

- **EQUAL lost: 0** of 3,548 EQUAL cells on main, in step 0's set (this table). The verifier's own 448 records are
  a different set: see fix round 1 below.
- **Moved (main → head):** S 1,068 WRONG and 1,470 RAISE to EQUAL; N 216, F 216 WRONG to EQUAL; L 216 WRONG and 148
  RAISE to EQUAL; `kernel.py` 18 WRONG and 21 RAISE; the oracle 373 WRONG and 889 RAISE to EQUAL and 6 WRONG to
  BOTHERR; zones 30 and 12; topology 10 and 6; pairing 16; extra 1 WRONG and 1 RAISE to EQUAL, 1 WRONG to RAISE.
- **What stays.** N's 76 SQL-door cells (Q6's card), M's 1,376 (Q4's card), T's 192 (Q5's card); in the oracle, the
  refused `INTERVAL DAY TO SECOND` and `VARIANT` (296 RAISE, refused by name) and `INTERVAL YEAR TO MONTH` (148
  BOTHERR); 18 oracle cells whose value Python's `datetime` cannot hold (a year past 9999) raise in the client on both
  engines, 6 of them `TIMESTAMP` cells main answered (WRONG to BOTHERR; the kernel unit checks them through
  `CAST(x AS STRING)`). In `X`, the tokenizer's two cards and quoted empty text under a `nullValue` stay WRONG, `USING
  csv` stays RAISE, and `emptyValue='E'` moves from silently dropped to refused.
- **The emulation's 1,696** (step 0's `seat_emu.py`, the `csv` door) are a subset of S, N and F above: the head
  answers all 1,696, where all-text + `TRY_CAST` answered 1,656 and lost 24.

## `EXPLAIN`

Step 0's seven plans (`domain.py` part E): the five reads with a schema or without `inferSchema=true` change, and
`inferSchema=true` with no schema does not.
- A `STRING` field and a no-schema read lose their `CAST(… AS Utf8)`: the projection is the scanned text (a header
  read with no schema has no projection at all).
- A typed field reads `__repark_csv_field__(?table?.x, Utf8("Int32"))` in place of `CAST(?table?.x AS Int32)`, in
  the projection, the filter and the scan's `partial_filters`. Since fix round 1 a read with a user schema scans
  positional columns, so the plan reads `__repark_csv_field__(?table?._c0, Utf8("Int32")) AS x`.
- The scan's own line is unchanged in the text; its schema is all Utf8.
- Pinned: `test_the_plan_parses_each_field_with_the_kernel_over_a_text_scan` (the kernel present, no `CAST(`).

## The verifier's class (c) notes, handled

- **The session zone.** A `TIMESTAMP` field's kernel reads the zone from the executing session's options, as `CAST`
  did; unit 0's views-in-another-zone and writes-in-another-zone pins are green on the head.
- **`decimal(2,5)` and `decimal(5,-2)`** refuse when the frame is planned, with Spark's texts (pinned).
- **Unicode digits** stay the kernel's declared divergence (card CSV-UNICODE-DIGITS-1, every decimal that fits).

## Fix round 1 (2026-11-20, head `8ac58878`)

The wiring verifier failed round 0's head on two class (a) findings and one class (b): header names (F1, 8 EQUAL
cells lost), a corrupt-record field in a user schema (F2, 10 cells from RAISE to a silent WRONG) and the records
(F3). Package `S/ddu-work/pkg-wire-8ac58878` (`BUILT_FROM` `8ac58878…`); base `S/ddu-work/pkg-main-9a48932f`.
Product files: still the three.

### The design

- **F1a, `spark.sql.caseSensitive`.** `CSVUtils$.makeSafeHeader(String[], boolean caseSensitive, CSVOptions)`, read
  with `javap -c -p` from `spark-sql_2.13-4.1.2.jar`: the duplicates are computed over the non-null names, each
  lowered with `String.toLowerCase()` only when `caseSensitive` is false; a null, empty or `nullValue` name is
  `_c<i>`; any other name in the duplicates takes its index. Its only callers are the schema-inference paths
  (`TextInputCSVDataSource$.infer` and `inferFromDataset`, `MultiLineCSVDataSource$.infer`), each passing
  `sessionState.conf.caseSensitiveAnalysis`. `read_options.rs`'s `spark_safe_header` follows it, lowering through
  `repark_common::java_case::string_lowered` (Java's `toLowerCase`). The facade reads the live session value through
  `_native.session_case_sensitive`, the carrier every facade name rule reads, and passes it as the `text_scan` mode:
  `header` (fold) or `header_case_sensitive`.
- **F1b, a user schema.** Spark consults header names only when it infers a schema, so a read with a user schema
  passes `text_scan=positions`: the scan skips the header and names its columns `_c<i>`, and the facade pairs them
  by position as before. `enforceSchema=false` is refused at the option gate, as on main and round 0.
- **F2, the corrupt-record column.** The orchestrator's call: before the read and before the option gate,
  `reader_support._refuse_csv_corrupt_record_field` refuses a user schema holding the field named by
  `columnNameOfCorruptRecord`: the option, else the session's `spark.sql.columnNameOfCorruptRecord`, else
  `_corrupt_record`. The match is exact, as Spark's `ExprUtils.verifyColumnNameOfCorruptRecord` uses
  `StructType.getFieldIndex`; Spark measured: `_CORRUPT_RECORD` is an ordinary field, and so is `_corrupt_record`
  when the session conf names another. One `AnalysisException`: `reader schema field '<name>' is the
  corrupt-record column named by columnNameOfCorruptRecord, …`. The capture is CSV-READ-MODES-AND-SHAPES-1's
  ([card-lines.md](card-lines.md), for the clerk).
- **The native door (verdict C4).** `text_scan` takes only `positions`, `header` or `header_case_sensitive`; any
  other value refuses by name.

### `spark.sql.caseSensitive` at RePark's other doors

- **One carrier.** `repark_functions::case_sensitive::SparkCaseSensitiveConfig` on the session config, written by
  `spark.conf.set` and `SET` (`set_runtime_config`). `repark-core` cannot name it (tier 2 below
  `repark-functions`), so core takes the value as an argument: the Spark door passes `case_insensitive` to
  `column_resolution`, and the binding reads it for writer layouts. The CSV header rule now does the same through
  the `text_scan` mode.
- **The facade's name rules** (`qualified_names.py`, `column_fields.py`, `filter_quote.py`, `subset_resolve.py`)
  read `_native.session_case_sensitive(frame._session)` per call: exact under `true`, the Java folds under `false`.
  Native frames read it through `frame_rule` (`dataframe_names.rs`).
- **The Spark SQL door** (`repark-spark`: `spark_door_case_insensitive`, `CREATE TABLE`, `INSERT … BY NAME`, write
  order, struct positions) reads the same carrier.
- **Not following it.** The native `session.read_csv` door keeps DataFusion's header names as written (no
  `makeSafeHeader` at all: `x,X,k` stays `x, X, k` whatever the setting), the CSV append path compares header names
  lowered whatever the setting (`path_write/append.rs`), and JSON keeps DataFusion's names. None of them moves here.

### The cells (Spark 4.1.2, both ANSI modes; [raw/fix1-spark.jsonl](raw/fix1-spark.jsonl), [probes/fix1.py](probes/fix1.py))

| cell | `caseSensitive` | Spark | main | round 0 | head |
|---|---|---|---|---|---|
| `x,X,k`, no schema | `true` | `x, X, k` | EQUAL | `x0, X1, k` (WRONG) | EQUAL |
| `ä,Ä,k`, no schema | `true` | `ä, Ä, k` | EQUAL | `ä0, Ä1, k` (WRONG) | EQUAL |
| `x,X,k`, no schema, then `.select("x")` | `true` | `('1',)` | EQUAL | RAISE `UNRESOLVED_COLUMN` | EQUAL |
| `_c1,,k` with `p STRING, q STRING` | `false`, `true` | `('1','2')` | EQUAL | RAISE `duplicate qualified field name` | EQUAL |
| `_c1,,k` with `p INT, q INT, k INT` | `false` | `(1, 2, 3)` | WRONG `(NULL, NULL, 3)` | RAISE | EQUAL |
| `x,X,x0` with `p, q, k STRING` | `false`, `true` | `('1','2','3')` | RAISE (`false`), EQUAL (`true`) | RAISE | EQUAL |
| `x,X,k` with `x STRING, X STRING, k STRING` | `true` | `('1','2','3')` | WRONG `('2','2','3')` | EQUAL | EQUAL |
| `x,X,k`, `ä,Ä,k`, no schema | `false` | `x0, X1, k`; `ä0, Ä1, k` | RAISE; WRONG | EQUAL | EQUAL |
| `a,A,a` with `p, q, k STRING` | `true` | `('1','2','3')` | RAISE | RAISE | RAISE (exact duplicates) |
| `_corrupt_record` first, in the middle, last (with and without a header), typed `INT`, by the option, `bad` by the option, `bad` by the session conf | `false` | answers (`INT`: `INVALID_CORRUPT_RECORD_TYPE`) | RAISE (`INT`: BOTHERR) | WRONG | refused by name (RAISE; `INT` BOTHERR) |
| `a STRING, b STRING, _corrupt_record STRING`: no row malformed | `false` | NULL in `_corrupt_record` | EQUAL | EQUAL | **refused (EQUAL lost, by the call)** |
| `_CORRUPT_RECORD` (`a INT`); `_corrupt_record` under a conf naming `bad` | `false` | ordinary fields | RAISE; EQUAL | not run | EQUAL |

### The verifier's 448 records, re-run ([raw/fix1-verifier-table.md.txt](raw/fix1-verifier-table.md.txt))

`probe.py` and `probe2.py` unchanged from the verifier's directory, on main's package and the head, against
Spark's recorded answers (the verifier's run). Main reproduces the verifier's base counts exactly.

| probe | records | main EQUAL / WRONG / RAISE / BOTHERR | round 0 (the verifier's) | head |
|---|---|---|---|---|
| `probe.py` | 336 | 87 / 50 / 185 / 14 | 186 / 8 / 122 / 20 | **190** / 4 / 122 / 20 |
| `probe2.py` | 112 | 22 / 16 / 70 / 4 | 88 / 16 / 6 / 2 | **94** / 6 / 8 / 4 |

- **F1's 8 cells are EQUAL;** **F2's 10 cells raise the new error** (8 RAISE; the 2 `_corrupt_record INT` cells
  BOTHERR, as Spark refuses the type).
- **EQUAL lost against main: 2**, `corrupt/clean_rows` in both modes. It was EQUAL by coincidence: the column sits
  past the file's width and no row is malformed. The orchestrator's call refuses every schema holding the column,
  so the cell moves to a loud refusal; the owner may overrule the call.
- **What is not EQUAL on the head, all named:** the unquoted `1,000` type files of `probe.py` and the short and long
  rows (field-count raise; CSV-READ-MODES-AND-SHAPES-1); exact duplicate header names, two empty or two `nullValue`
  names among them, and `a,A,a` under `true` (the same residual); a quoted `""` under a `nullValue`
  (CSV-TOKENIZER-QUOTE-EMPTY-1); `inferSchema=true` with no schema (T, CSV-INFER-INT32-WIDTH); the refused options
  and modes, the refused types (Spark's texts, nested types' texts differ) and `emptyValue='E'`; `load(path,
  format="csv")`; `_c1,,k` and `x,X,x0` with no schema (Spark and the head both raise); the corrupt-record column
  (F2); an empty `StructType` (WRONG on every build; the head now names the columns `_c0, _c1`, round 0 `a, b`).

### Step 0's re-run

`probes/rerun.py` over step 0's probes, unchanged, on main's package and the head (13,050 records each): the table
is byte-identical to round 0's [raw/rerun-table.md.txt](raw/rerun-table.md.txt). **EQUAL lost: 0** of 3,548, and
every move is round 0's.

### The pins

`test_csv_string_field_text_1.py` grows by 12 read cells and 9 refusals, each run in both ANSI modes with
`spark.sql.caseSensitive` set explicitly per cell (and `spark.sql.columnNameOfCorruptRecord` where the cell names
it), against Spark 4.1.2's recorded answers ([raw/fix1-spark.jsonl](raw/fix1-spark.jsonl)): 150 tests.

- **The head:** 150 of 150 pass.
- **Main:** 133 fail and 17 pass. The 17 are round 0's 5 controls; F1's no-move pins, which hold main's answer
  (`x,X,k`, `ä,Ä,k` and `.select("x")` under `true`, `_c1,,k` with a schema under `true` and `false`: 10); and
  the default-named column under a conf naming another (2).
- **Red on main, by design:** `x,X,k` and `ä,Ä,k` under `false`, the two colliding headers under a schema
  (`p INT, q INT, k INT`; `x,X,x0`), a case-only schema under `true`, `_CORRUPT_RECORD` as an ordinary field, and
  the nine corrupt-record refusals (first, in the middle, last with and without a header, `INT`, `clean_rows`, by
  the option, named by the option, named by the conf).
- **Rust:** `spark_safe_header` (case folding only when insensitive, Java lowering, `_c<i>` names), the `text_scan`
  value refusal and a positional scan over `_c1,,k` (`cargo test -p repark-core read_options`, 20 tests).

Up: [map.md](map.md).
