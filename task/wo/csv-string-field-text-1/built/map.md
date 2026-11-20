# task/wo/csv-string-field-text-1/built/ — CSV-STRING-FIELD-TEXT-1, the wiring unit

**The unit (2026-11-20, Claude Opus 5.5 xhigh).** The second of the card's two units: CSV-FIELD-KERNEL-1's kernel
wired into the reader, under seat A and Fable's guidance of 2026-11-20 (Q1, Q2, Q7, Q8, and "For the wiring brief,
later"), acted on under the owner's delegation ("Let's go with defaults across the board", about 00:22 UTC).

- **The scan.** A CSV read with a user schema, or without `inferSchema=true`, passes the engine the facade-only
  option `text_scan` (never taken from the user's options). `read_options.rs` then resolves the schema with
  `schema_infer_max_records(0)`: the first listed object's header alone, every column Utf8. With no schema the
  header names are decided once there, by Spark's `makeSafeHeader` rule: an empty name or the `nullValue` token
  becomes `_c<i>`, a name that repeats takes its index as a suffix (ignoring case only under
  `spark.sql.caseSensitive=false`, the session value the facade passes as the `text_scan` mode), and untrimmed names
  are kept. With a user schema the scan names its columns `_c<i>` and never consults the header (fix round 1). A
  read with `inferSchema=true` and no schema keeps today's inference.
- **The fields.** `reader_support._csv_text_frame` pairs a CSV schema with the columns by position (the first M
  columns, NULL past the file's width), applies `nullValue`, and parses each non-`STRING` field with the kernel
  (`Column.csv_field`, the type marker a string). `_csv_field_spec` keeps the cast's type gates (collation, spatial)
  and refuses what Spark refuses when it parses the schema: `char`/`varchar`, a negative decimal scale, a scale over
  the precision. JSON keeps name pairing and `CAST`.
- **Removed.** Unit 0's re-read in `_load_csv` and `_session_timestamp_text_columns`; so unit 0's cost line ("one
  extra schema inference, at most 1 ms per read") retires.
- **`emptyValue`** other than `""` is refused at the option gate, by name.
- **The corrupt-record column** (fix round 1): a CSV read schema holding the field named by
  `columnNameOfCorruptRecord` is refused by name before the read (its card line is in `card-lines.md`, for the clerk).
- **Product files: 3** (`crates/repark-core/src/read_options.rs`, `session/reader.py` at 974 of 1,000 lines,
  `session/reader_support.py`), the sketch's three, in round 0 and in fix round 1.

Records:
- [bar.md](bar.md) — the pins, unit 0's and #1373's bars, the scan topology (S3 on moto included), the full
  re-run, the EQUAL cells lost, `EXPLAIN`, and fix round 1's cells.
- [cost.md](cost.md) — ten exclusive rounds with instruction counts, and C-012; fix round 1's five.
- [card-lines.md](card-lines.md) — three lines for the clerk to add to CSV-READ-MODES-AND-SHAPES-1.
- [probes/](probes/map.md) — the probes.
- [raw/](raw/map.md) — Spark's recorded cells and each package's.

Up: [../map.md](../map.md).
