# CSV-STRING-FIELD-TEXT-1 — the insertion inventory (the owner's rule of 2026-11-09; revised 2026-11-20)

Every place a CSV read's column types are decided or cast. Main is `6d3739ab` (its product tree equals `8bc26cba`'s;
`git diff --name-only 8bc26cba 6d3739ab -- crates python/repark/src` is empty). Unit 1 is PR #1373 at `acd27d6e`
(read only, `S/lane-reader-parse-unit1`). Marks: **[R]** reading, **[M]** measured (the cells are in
[domain-table.md](domain-table.md)).

## 1. Who decides a column's type, in order

| # | site | main | unit 1 | what it decides |
|---|---|---|---|---|
| 1 | `crates/repark-core/src/read_options.rs` `csv_read_options_from_map` | `:13-58` | the same | DataFusion's `CsvReadOptions`: `header` (`:20-23`), `nullValue` as `null_regex` (`:38-42`), `multiLine` (`:43-45`). No schema and no `schema_infer_max_records`, so DataFusion infers. |
| 2 | `read_options.rs` `read_csv_path`, the pre-scan choice | `:224-229` | the same | `nullValue`, `multiLine` or any `utf8_columns` takes `csv_utf8_schema_from_path`: an all-Utf8 schema from the first local record (`:280-375`). It returns `None` for a path holding `://` (`:293-295`), a compressed part (`:318-324`) or a missing path, and then DataFusion infers. |
| 3 | `read_options.rs` `read_csv_path`, the scan | `:233-236` | the same | `context.read_csv`: with no schema, DataFusion infers every column (row 5). |
| 4 | `read_options.rs` `csv_utf8_column_schema`, the re-read | `:240-247`, `:252-277` | the same | When no pre-scan ran, the named `utf8_columns` are re-read as Utf8 over the inferred schema; the others keep the inferred type. |
| 5 | DataFusion 54.1.0 `CsvFormat::infer_schema` | `datafusion-datasource-csv-54.1.0/src/file_format.rs:376-409` | — | Reads up to `schema_infer_max_rec` records, default 1,000 (`datafusion-datasource-54.1.0/src/file_format.rs:43`), file by file, and merges (`:405`). |
| 6 | `infer_schema_from_stream` and `build_schema_helper` | `file_format.rs:519-625`, `:640-680` | — | Per column: one type kept; `Int64` with `Float64` merges to `Float64`; any other pair is `Utf8`; no type is `Null` (`:654-665`). **[R]** `schema_infer_max_rec = 0` sets `disable_inference` (`:620-624`), and every column is `Utf8` from the header alone. RePark never sets it. |
| 7 | arrow-csv 58.4.1 `InferredDataType` (the lock pins 58.4.1, not 59) | `arrow-csv-58.4.1/src/reader/mod.rs:186-197`, `:217-271` | — | The regex set: `(?i)true\|false` is `Boolean`; `^-?\d+$` is `Int64` (so `007` is 7, and `+5` is text); a decimal or exponent shape is `Float64` (`1.50`, `1e3`, `1E-2`, `5.`); `yyyy-MM-dd` is `Date32`; a timestamp shape is `Timestamp` without a zone. An integer of 19 or more digits that overflows `i64` is `Utf8` (`:258-262`); `NaN`, `inf` are `Float64`; the rest is `Utf8`. |
| 8 | arrow-csv's typed decode | `mod.rs:682`, `:937-946`, `:948-980`, `:982-1000` | — | The scan parses each field by the inferred type; a later record that does not fit (past the 1,000 inferred) raises `Error while parsing value`. |
| 9 | the facade's `_load_csv` | `reader.py:494-518` | `reader.py:500-525` | Forces `header=false` when unset (`:503-504`), reads (`:505`/`:511`), and re-reads the zone-less timestamp columns as text (`:506-512`/`:512-518`). Unit 1 wraps the result in `_reader_parse` (`:519`); no type changes. |
| 10 | `_session_timestamp_text_columns` | `reader.py:520-541` | `reader_support.py:584-598` | Names the scan's `timestamp_ntz` columns only, and only when the user schema holds a `TIMESTAMP` field. |
| 11 | `_apply_reader_schema_semantics`, `nullValue` | `reader.py:711-719` | `reader.py:698-706` | `when(cast(col AS STRING) == token, NULL)`; a typed column is cast to text first, so `007` meets the token as `7`. |
| 12 | `_apply_reader_schema_semantics`, a user schema | `reader.py:722-760`: casts at `:735`, `:750`, `:753`, `:758` | `:709-747`: `:722`, `:737`, `:740`, `:745` | **The defect's cast.** `CAST(<inferred column> AS <declared type>)`: an `Int64` column cast to `STRING` answers `7`; a `Float64` to `INT` answers `1000` for `1e3`, `1` for `1.5`; text cast to a type uses the engine's cast rule, not Spark's CSV rule. |
| 13 | `_finish_csv_infer_schema` (`inferSchema=true` only) | `reader_support.py:389-444` | the same | Re-reads the scan's typed dtypes `_CSV_TYPED_DTYPES` (`:358`) as Utf8 (`:420-427`), then promotes leftovers (`:447-528`, casts at `:487`, `:492`, `:524`). |
| 14 | `_apply_reader_schema_semantics`, no schema | `reader.py:786-788` | `:773-775` | **The defect's second cast.** `inferSchema` false or unset: `CAST(<inferred column> AS STRING)` over the typed scan, so the default read answers `7`, `1.5`, `1000.0`, `true`. |
| 15 | `_cast_inferred_naive_timestamps` | `reader_support.py:531-551` (cast `:544`) | the same | `inferSchema=true`: zone-less timestamps through a string to `TIMESTAMP`. |
| 16 | the engine cast every facade cast builds | `crates/repark-python/src/column/display.rs:274-287` (`engine_cast` → `cast_to`) | the same | `CAST`; `TRY_CAST` exists (`column/mod.rs:840`) and answers NULL on failure, by the Spark `CAST` rule. |

**Added in the revision (2026-11-20, the critic's items):**

| # | site | main | unit 1 | what it decides |
|---|---|---|---|---|
| 17 | `smartCsv` (a RePark extension, not a Spark door) | `reader.py:120-275` → `python/repark/src/repark/spark/_csv_smart.py:774-875` | the same | Parses rows in Python, `createDataFrame(data_rows, schema=string_schema)` (`:858`), then SQL `CAST(CASE WHEN … = '' THEN NULL ELSE … END AS <rung>)` (`:870`); the rung tokens come from `crates/repark-spark/src/type_table.rs:554` (`csv_sql_cast_token`) and `:564` (`csv_rung_type`) through `crates/repark-python/src/type_bridge.rs:383-412`. **Untouched by either seat.** |
| 18 | `CREATE [OR REPLACE] TEMPORARY VIEW … USING csv OPTIONS (path …)`, with and without a column list | no route | no route | **[M]** raises at parse on both builds: `could not parse CREATE TEMPORARY VIEW: expected AS with the view query`. Spark answers (`007`). Joins Q6's card. |
| 19 | the option gate `_reject_csv_json_parse_options` | `reader_support.py:584-618`, denylist `:59-80` | `:601-635` | Refuses `nanValue`, `positiveInf`, `negativeInf`, `dateFormat`, `timestampFormat`, `locale`, `samplingRatio`, `enforceSchema`, `columnNameOfCorruptRecord`, `ignoreLeading/TrailingWhiteSpace` and the two non-default modes. **`emptyValue` is not on it, and `_native_options_for` (`reader.py:662-669`; keys `reader_support.py:118-132`) drops it: silently ignored [M].** |
| 20 | the local pre-scan's naming | `read_options.rs:353-370`, `split_csv_header_line` `:378-401` | the same | Trims header names, maps an empty name to `_c<i>`, and hard-codes `"` as the quote. `schema_infer_max_records(0)` does neither: arrow's header row keeps the names untrimmed and an empty name `''` [M, §6 of the domain table]. |
| 21 | the listing a multi-file read infers from | `datafusion-catalog-listing-54.1.0/src/options.rs:279-310`; `object_store-0.13.2/src/local.rs:717-722`; `aws/mod.rs:286` | — | Zero-byte files are dropped and the list is not sorted. The local store walks with `WalkDir` and no `sort_by`, so readdir order; S3's `ListObjectsV2` returns keys in lexicographic order [R]. With `schema_infer_max_records(0)`, `infer_schema` stops after the first object (`file_format.rs:401-404`), so **the first-listed file alone names the columns**. Spark takes the header from the largest file [M, §6]. |
| 22 | S3, read through seat A's scan [R only; no S3 or external call was made] | `read_options.rs:217` `resolve_s3_prefix_for_read` (`object_store_s3.rs:224-240`) → `context.read_csv` | the same | A key prefix ending `/` is re-encoded; DataFusion resolves the registered S3 store, lists (row 21) and streams the first object through `read_to_delimited_chunks`, decompressing as the local path does. `csv_utf8_schema_from_path` returns `None` for any `://` path, so today an S3 read infers and the named-column re-read is the only all-text route. **A gap for the build's verifier: S3 (moto) cells for the scan.** |

Nothing else types a CSV column: `git grep -n "read_csv"` reaches only `read_options.rs:212`, `session.rs:783-791`
(core), `repark-python/src/session.rs:237-246`, `session_core.py:903-911` (pass-throughs), the two reader files
above, and `path_write/append.rs:203` (an append reads its own destination). The SQL door
``SELECT * FROM csv.`path` `` has no route at all on either build: it raises `table 'datafusion.csv./…' not found`
at planning **[M]**.

## 2. Spark's own sites, for comparison [R, from the jar]

- `UnivocityParser.makeConverter`, per declared type (`javap -c -p` on `spark-catalyst_2.13-4.1.2.jar`):
  `INT` `StringOps.toInt` (`$anonfun$makeConverter$6`), `BIGINT` `toLong` (`$8`), `DOUBLE` `toDouble` after the
  `nanValue`/`positiveInf`/`negativeInf` tokens (`$12`), `BOOLEAN` `toBoolean` (`$14`), `DECIMAL`
  `decimalParser` then `Decimal(bd, p, s)` (`$16`; `ExprUtils.getDecimalParser` under `Locale.US` is
  `new BigDecimal(s.replaceAll(",", ""))`), `DATE` `dateFormatter.parse` with the `stringToDate` fallback (`$18`),
  `TIMESTAMP` `timestampFormatter.parse` with the `stringToTimestamp(s, zoneId)` fallback (`$21`),
  `TIMESTAMP_NTZ` `parseWithoutTimeZone` (`$24`), `STRING` `UTF8String.fromString` (`$26`): the text itself.
- `nullSafeDatum`: a token equal to `nullValue` (default `""`) is NULL before any converter runs.
- `convert`: a converter that throws marks the record bad with its partial row; `FailureSafeParser` keeps the
  partial row (`PERMISSIVE`), drops the row (`DROPMALFORMED`) or raises `malformedRecordsDetectedInRecordParsingError`
  (`FAILFAST`). No converter reads the ANSI flag.
- `CSVInferSchema.inferField` (only under `inferSchema=true`): integer, long, decimal, double, then date or
  timestamp-NTZ, timestamp, boolean, string (`tryParseInteger` … `tryParseBoolean`).

## 3. Why the timestamp side gain answers text, and the others do not [R, M]

Unit 0's final head re-reads as Utf8 only the columns `_session_timestamp_text_columns` names (row 10): the scan's
zone-less timestamp columns, when the user schema holds a `TIMESTAMP` field. It re-reads them through a `file://`
URL (row 9), so `read_csv_path` skips the local all-text pre-scan (row 2) and re-reads only those names over the
inferred schema (row 4). Every other column keeps the scan's inferred type and meets the cast of row 12: `007` was
inferred `Int64` and is cast to `STRING` as `7`. So a `STRING` field answers the file's text beside a `TIMESTAMP`
field only when its own text has a timestamp shape. Delta 1 withdrew the wider gain on purpose (the bar's
"Delta 1"): F1's `DECIMAL` over exponent text raised when every column went through the text cast. The glob
residual (CSV-GLOB-LITERAL-RESIDUAL-1) is the one spelling that still takes the all-text pre-scan, so there a
`STRING` field beside a `TIMESTAMP` field answers text for every shape.

## 4. Where a seat can sit

- **Row 2/6 (Rust, the scan).** An all-Utf8 scan **whenever a user schema is given, or whenever `inferSchema` is not
  true** (a schema overrides `inferSchema=true` on Spark: the domain table §6.1). Either the existing first-record
  pre-scan, which is local only, or DataFusion's own `schema_infer_max_records(0)` (row 6), which reads the first
  listed object's header on every store (rows 21-22). Either removes the inference and unit 0's second read.
- **Rows 12, 14 (the casts).** The per-type text rule must replace `CAST` here: a parse from text by the declared
  type with Spark's NULL on misfit. Row 16's `TRY_CAST` is close but follows RePark's `CAST` rule, not the CSV
  converters: it misses 200 of the oracle's 1,184 cells ([oracle.md](oracle.md); for example `BOOLEAN` over `t`,
  `DECIMAL` over `1e3`, `DOUBLE` over ` 7 `, and a `TIMESTAMP_NTZ` gap moved through the session zone).
- **The pairing (row 12's sites).** Spark pairs a CSV schema by position (`enforceSchema=true`); the facade pairs by
  name, a branch the JSON reader shares, so a positional rule must be CSV-only (sketch Q8).
- **Untouched:** rows 13 and 15 (`inferSchema=true` with no schema), row 17 (`smartCsv`), the JSON reader, and the
  write paths.

Up: [map.md](map.md).
