# map — repark-core/src/session/text_write_format

## Purpose

Spark-compatible TIMESTAMP / TIMESTAMP_NTZ / DATE formatting for CSV and JSON
writes, shared by the local and s3a routes. The writer builds a plain
`SELECT *` COPY under a `repark_text_csv` / `repark_text_json` format name
with the zone and user patterns as `repark.text.*` OPTIONS; the sink's
per-batch serializer formats every temporal-bearing column with the same
engine the UDF used, inside the parallel serialize tasks DataFusion joins
in batch order, and the inner CSV/JSON serializer writes the strings.
Non-temporal frames keep byte-identical plain `STORED AS CSV` / `JSON`,
so their path never touches this code; partition columns pass through
unformatted, as the UDF left them unwrapped.

## Contents

- `render.rs` — the pattern renderer: Spark `DateTimeFormatter` letters for
  date, time, offset, localized offset, zone id, era, quarter, week-aligned
  `F`, and modified-Julian `g`, plus the three default renders. Zone-name
  letters (`v`, `z`) fail: names need JRE locale data, so the compiler refuses
  them eagerly for LTZ and the renderer fails them lazily for NTZ/DATE.
  `y` renders the signed proleptic year (a `+` past 9999) unless an unquoted
  `G` is present, which switches year-of-era; `VV` renders the Java display
  id the spec resolves, never the canonical zone. `g` pads the signed day
  count with zeros to the letter count (re-verify 2026-09-29). Every
  field writes into a caller buffer: no per-value `String` on the hot
  path (re-verify 2026-09-29). The compiled pattern carries `has_era`,
  decided once per pattern; `epoch_day` is computed once per value
  (re-verify 2 2026-09-30).
- `udf.rs` — the Arrow formatting engine (timestamp, date, struct, list, map
  values; map keys untouched), shared by every sink serialize call. Return-type
  mapping mirrors the recursion so batches carry `Utf8` where strings come out.
  UTC and fixed-offset zones resolve once per call with no zone lookup; other
  zones get one fresh offset cache per serialize call, never a shared `Mutex`
  across the parallel tasks (sink-format round 2026-09-30). A miss costs one
  direct lookup; the two-endpoint proof that the 15-minute window around the
  instant holds a single offset runs only when the value lands within one
  window of the previous value, so scattered misses skip a proof they cannot
  amortize. An unproven window caches nothing, and no search walks in steps.
  Default specs dispatch to the `fast.rs` loops and build each output column
  in one offsets-plus-values buffer validated once per batch (re-verify 2
  2026-09-30). The `repark_write_format_text` ScalarUDF shell is gone: grep
  found no caller outside the write path, so the sink-format round removed
  the struct, the registration and the backslash-halving literal path, and
  kept the engine behind `format_batch_for_sink`.
- `fast.rs` — the default-format fast path (re-verify 2026-09-29):
  table-driven digit emission, a day cache, a precomputed offset suffix,
  and a small-delta carry between consecutive values. Byte-identity with
  the scalar renders is pinned by a differential test, not by review.
- `select.rs` — `build_text_write_copy_parts`: per-kind user-pattern validation
  (Spark error classes) plus the COPY parts builder (plain `SELECT *`, resolved
  `STORED AS` name, spec OPTIONS SQL), and the
  `ReparkSession::text_write_copy_parts` entry over a `DataFrame`. User
  patterns ride the OPTIONS as hex: the Spark door processes backslash escapes
  while the core door passes them verbatim, so no single literal quoting
  survives both doors and hex sidesteps quoting entirely. Case-duplicate
  temporal columns all format: the serializer matches no names.
  `text_write_copy_parts` refuses
  temporal-bearing writes under
  `spark.sql.legacy.timeParserPolicy=LEGACY`: legacy rendering is carded as
  TEXT-WRITE-LEGACY-POLICY-1, not implemented. A LEGACY frame without
  temporal columns writes even when it carries temporal options, as
  Spark does (re-verify 2026-09-29). `merge_spec_options` appends the spec
  pairs to the format OPTIONS clause the builders validate first, so eager
  refusal order never changed.
  **SOURCE-URL-REDACT-1 fold 2 (2026-10-06):** the legacy-policy fallback reads the session's raw conf rows
  (`&self.conf_dump`), never the display-redacted `conf_dump()`. pins: source-url-redact-1/C-031
- `spec.rs` — `TextWriteSpec`: the session zone text with its canonical and
  Java display ids plus the three compiled format specs, built once per COPY
  in the factory's `create`. `from_format_options` strips the `repark.text.*`
  keys, hex-decodes the three `*_hex` patterns (bad hex refuses loud at plan
  time) and hands the rest to the inner CSV/JSON factory; `options_sql` emits
  the pairs the builders merge into COPY OPTIONS. Only the zone rides as a
  plain literal: a validated zone id never holds a quote or a backslash.
  **TEXT-WRITE-TIMESTAMP-ZONE-1 re-verify 4 fold (2026-09-30):**
  `TextWritePathRegistry`, a `ConfigExtension` holding one recorded-path
  collector per running s3a text write, keyed by the `repark.text.write_id`
  OPTION the commit appends; the map inside is `Arc`-shared so every plan
  clone of the session sees it, and the entry is removed when the write
  settles. pins: text-write-timestamp-zone-1/C-009
- `serializer.rs` — `ReparkTextSerializer`: maps every temporal-bearing column
  (the same `contains_temporal` predicate as `select.rs`) through
  `format_batch_for_sink`, skipping top-level partition columns by lowercased
  name so `keep_partition_by_columns` writes them native, exactly as the UDF
  left partition columns unwrapped; then delegates to the inner CSV/JSON
  serializer with the batch's `initial` flag untouched.
- `sink.rs` — `ReparkTextSink`: mirrors `CsvSink` / `JsonSink` (`config`,
  `write_all` via `FileSink::write_all`, inner serializer built exactly as
  DataFusion builds it, then wrapped). The demux strips partition columns
  before serialization and names directories from the raw values, as before.
  **TEXT-WRITE-TIMESTAMP-ZONE-1 re-verify 4 fold (2026-09-30):** the sink
  records the output paths the demux hands it into a per-write collector:
  the `spawn_writer_tasks_and_join` override records each path and forwards
  the stream into DataFusion's orchestration unchanged, so success bytes and
  keys are identical. The factory injects the collector the session
  registry holds for this COPY's write id (a miss gets a private one
  nobody reads), and the s3a commit takes the same collector to delete
  exactly what this write created on failure. Sharing stays inside one
  write: never a process-global list. pins: text-write-timestamp-zone-1/C-009
- `file_format.rs` — `ReparkTextFormat`, which delegates every method to the
  inner `CsvFormat` / `JsonFormat` except `create_writer_physical_plan`
  (same header, newlines-in-values and compression handling, over
  `ReparkTextSink`), and `ReparkTextFormatFactory`, registered on the session
  under `repark_text_csv` / `repark_text_json`. The factory ext is the
  `STORED AS` name; the format ext stays `csv` / `json`, so part files keep
  their extension. **TEXT-WRITE-TIMESTAMP-ZONE-1 re-verify 4 fold
  (2026-09-30):** the writer plan resolves this COPY's recorded-path
  collector from the session registry by the spec's write id and injects
  it into the sink. pins: text-write-timestamp-zone-1/C-009

## Debug

- A `timestampFormat` write fails before any file appears: the compiler
  rejected the pattern in `select.rs` validation; replay the pattern through
  `compile_write_pattern` in `tests/text_write_format.rs`.
- A write fails mid-stream with `Unsupported field` / `Unable to extract
  ZoneId`: a compiled pattern met a value kind that lacks the field (Spark
  fails the same write lazily); check the value kind against the pattern.
  The error now surfaces from the serializer instead of the UDF; the message
  text is identical, only the wrapper changed.
- Wrong wall or offset, right shape: the zone id or the instant is wrong, not
  the renderer — compare `UNIX_MICROS` of the literal with Spark first.
- A hand-written COPY under `STORED AS repark_text_csv` fails on a missing
  `repark.text.zone` option: only the writer builders emit the spec OPTIONS;
  add the key or write through the facade.
