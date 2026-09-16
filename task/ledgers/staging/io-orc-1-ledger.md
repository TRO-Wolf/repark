# Charter ledger — IO-ORC-1 · read-only ORC scan (`DataFrameReader.orc`, `format("orc").load`)

**Date:** 2026-09-16 · **Branch:** `feat/io-orc-1` · **Base:** `778fa9b1` · **Model:**
muse-spark-1.3-contributor · **Policy:** [../../../AGENTS.md](../../../AGENTS.md).
**Path:** STANDARD. **risk_tier: standard.**
**Registry:** `IO-ORC-1` (read implemented 2026-09-16 on orc-rust 0.8.0 per R-0;
write stays DECLARED) and `IO-ORC-SQL-1` (BACKLOG — SQL door owned by the SQL
planner unit, per O-10) at §5 of
[../../../docs/spark-sql-iceberg-parity.md](../../../docs/spark-sql-iceberg-parity.md).

**Why now.** The 1.5 PySpark-parity campaign (owner, 2026-09-14): every function in
any shape needed. PR #610 (IO-DECLARED-1) filed the ORC names as a dated declared
refusal pending owner question Q-15B-1; the owner approved `orc-rust` for a
read-only scan on 2026-09-15 (R-0). This unit turns the READ side into a real
Rust-first scan and keeps the write refusal byte-identical.

**Not in this unit:** ORC write (stays DECLARED); `functions*.py`, the Rust
function registry, `crates/repark-spark` planner edits (run 18a/18c lanes);
`STATUS.md`, `briefs/next-sequence.md`, `.github/`, `pyproject.toml`, `uv.lock`.

**Oracle.** Live PySpark 4.1.2 classic cells recorded by run 16b on 2026-09-15:
`/tmp/oc-worker/run18b/oracle/orc_probe_2026-09-15.json` (script `probe_orc.py`
beside it), fixtures under `/tmp/oc-worker/run18b/oracle/orc-fixtures/`. Copied
to `python/repark/tests/facade_orc_oracle.json` (absolute fixture paths replaced
with `{FIX}`) and `python/repark/tests/fixtures/orc/`. Every pin names its cell.
No hand-computed Spark expectation; unprobed shapes are marked UNMEASURED.

**Cells wanted.** None — every pinned shape has a recorded cell.

## PROPOSITION LEDGER — IO-ORC-1 — 2026-09-16

| Clause | Proposition (checkable) | Proof obligation | Verdict | Evidence / open question |
|---|---|---|---|---|
| C-001 | `orc-rust 0.8.0` (default-features off) lands in workspace deps + `repark-core` deps; `cargo update --workspace --offline` moves no existing crate version; Apache-2.0 + transitive licences pass the licence gate; `cargo build -p repark-core` green. | The Cargo.lock diff check; the gate output; the build rc. | OPEN |  |
| C-002 | O-1: the typed read answers the `orc_typed` cell — 19 columns, the cell's simpleString byte-exact, every column nullable, NULL row intact; `format("orc").load` answers `orc_typed_format_load`. | `python/repark/tests/test_io_orc_1.py` O-1 pins. | OPEN |  |
| C-003 | O-2: NONE/SNAPPY/ZLIB/LZ4/ZSTD/LZO read back per `orc_codec_*`; brotli has no read pin (Spark write error, empty dir). A codec orc-rust cannot decode raises AnalysisException naming the codec. | The O-2 pins. | OPEN |  |
| C-004 | O-3: directory / file / list / `*` glob reads; hidden `_`/`.` files skipped; missing path PATH_NOT_FOUND `file:` form; empty dir, nomatch filter, past modifiedBefore, non-recursive nested → UNABLE_TO_INFER_SCHEMA `42KD9`; non-ORC file → FAILED_READ_FILE.CANNOT_READ_FILE_FOOTER with the path; ignoreCorruptFiles over only-corrupt → UNABLE_TO_INFER_SCHEMA. | The O-3 pins. | OPEN |  |
| C-005 | O-4: second positional binds mergeSchema → IllegalArgumentException `For input string: "<path>"`; keyword mergeSchema with two positionals → TypeError; non-str/non-list path → PySparkTypeError `[NOT_STR_OR_LIST] Argument `path` should be a str or list, got int.` (JVM Py4JError divergence noted in registry). | The O-4 pins. | OPEN |  |
| C-006 | O-5: mergeSchema=true unions columns by name; pathGlobFilter / recursiveFileLookup / modifiedBefore / modifiedAfter filter files; basePath restores partition columns; unknown options ignored. Type-conflict merge shape UNMEASURED (no cell). | The O-5 pins. | OPEN |  |
| C-007 | O-6 + O-7: `k=v` dirs add partition columns after data columns with Spark inference (`__HIVE_DEFAULT_PARTITION__` → NULL); leaf read adds none; user schema resolves by name case-insensitively, missing → NULL, int→string reads the text form; other mismatches refuse loud (UNMEASURED). | The O-6/O-7 pins. | OPEN |  |
| C-008 | O-8 + O-9: nested-field + array select, `i = 2` filter, case-insensitive names, count; stripe/column projection is real (only projected columns decoded); stripe-statistics predicate pushdown NOT implemented (DataFusion-side filtering); `timestamp` renders in session zone, `timestamp_ntz` unshifted. | The O-8/O-9 pins. | OPEN |  |
| C-009 | O-10: `SELECT * FROM orc.`path`` and `CREATE TABLE t USING orc LOCATION` pin today's refusal (identical to the parquet twins on main — planner owned by run 18c); registry row IO-ORC-SQL-1 BACKLOG. | The O-10 pins + registry diff. | OPEN | Probed 2026-09-16: `parquet.` door errs `table 'datafusion.parquet.<path>' not found`; `USING parquet/orc LOCATION` both refuse `UnsupportedOperationException ... CREATE TABLE … LOCATION is not supported`. |
| C-010 | Registry row IO-ORC-1 rewritten in place (read implemented, write DECLARED, O-4/O-7 divergences, SQL-door state); `docs/examples/io/orc_read.py` covers the read names, the refusal example still covers the write names; inventory refreshed; EX-0 count unmoved (no new public names). | The registry diff; the example runs; the EX-0 rc. | OPEN |  |
| C-011 | No regression: `make verify`, the unit test files, the whole facade suite, the whole parity suite green with real exit codes and counts. | §Gates. | OPEN |  |

Red-first log: each clause pins red on the base tree first, then green. Red lines
pasted per clause below as they land.

### C-002 red

TBD

## Per-name decision table

| name | decision | one line of reason |
|---|---|---|
| `DataFrameReader.orc` | implemented (Rust scan) | The read side this unit builds (O-1..O-9). |
| `format("orc").load` | implemented (routes to the scan) | Same read side through the format spelling. |
| `DataFrameWriter.orc` | declared (unchanged) | ORC write stays a dated DECLARED refusal per Q-15B-1. |
| `format("orc").save` | declared (unchanged) | Same write refusal at `save()`. |
| `SELECT * FROM orc.`path`` | pinned refusal (IO-ORC-SQL-1 BACKLOG) | Planner owned by run 18c; parquet twin refuses identically today. |
| `CREATE TABLE t USING orc LOCATION` | pinned refusal (IO-ORC-SQL-1 BACKLOG) | Same planner ownership; parquet twin refuses identically today. |

## Rust-first roll-call (Q-17a-2)

Every kernel, conversion, planner rule and type rule lives in Rust
(`crates/repark-core/src/orc_scan.rs` + a native `read_orc` entry); Python holds
names, argument shapes and API plumbing only. Pieces staying in Python, with
reasons:

| piece | where | reason it stays in Python |
|---|---|---|
| `orc()` signature + mergeSchema validation + path-type check | `session/reader_orc.py` | PySpark argument shapes and error classes are API plumbing. |
| `format("orc").load` routing | `session/reader.py` (`load`) | Format-dispatch plumbing beside the other formats. |
| User-schema name/type carry (`_schema_fields`) | `session/reader_orc.py` | Facade schema-form normalization; the engine parses and applies. |
| Option carry (mergeSchema/basePath/timestamps as text) | `session/reader_orc.py` | Spark option-map semantics; parsing/filtering is Rust. |

## Settled decisions (recorded, not guessed)

- **Timestamp swap.** ORC `timestamp` (instant) decodes through orc-rust as naive
  arrow; the scan labels it `Timestamp(_, Some("UTC"))` so the facade reports
  `timestamp` and renders the instant in the session zone. ORC `timestamp with
  local time zone` decodes as tz-aware; the scan strips it to naive so the facade
  reports `timestamp_ntz` unshifted. Verified against `orc_ts_session_tz`.
- **Precision.** Microsecond (`with_timestamp_precision(Microsecond)`) — Spark
  micros, matching the cell reprs.
- **O-4 mergeSchema validation.** A non-None non-bool positional mergeSchema
  raises `IllegalArgumentException` `For input string: "<value>"` (the JVM's own
  text from `orc_two_paths`); the options-map `mergeSchema` string (`"true"`)
  parses as a boolean like Spark.
- **O-3 non-ORC shape.** A non-ORC file raises the native read error carrying
  `FAILED_READ_FILE.CANNOT_READ_FILE_FOOTER` with the `file:` path (Spark's own
  condition; the JVM-call prefix is not a contract).
- **inputFiles().** The `plan_introspect` walk only covers `FileScanConfig`
  (`DataSourceExec`); the streaming ORC scan is invisible to it, so
  `inputFiles()` on an ORC frame answers `[]` — recorded as a divergence in
  IO-ORC-1 (the `orc_input_files` cell shape noted, not pinned).
- **DF-METADATA-COL-1.** Not on main (no marker-scan machinery in
  `crates/repark-core` at `778fa9b1`); no plug-in owed. If it lands later, ORC
  takes the parquet-less field set there.
- **Brotli.** Spark itself cannot write brotli (`orc_codec_brotli` is a Spark
  write error; the fixture dir is empty) — no read pin. orc-rust 0.8.0 carries
  no brotli decoder; a brotli file errors loud naming the codec (UNMEASURED, no
  fixture exists to pin it).
- **CANNOT_MERGE_SCHEMAS.** Type-conflict merge is unprobed — the scan refuses
  loud (UNMEASURED, registry note).
- **Non-int→string user-schema mismatches.** Only the int→string arm is probed
  (`orc_user_schema_wrong_type`); other mismatches refuse loud (UNMEASURED).

## Rulings

- R-0 = Q-15B-1 (owner, 2026-09-15): `orc-rust` APPROVED for a read-only ORC
  scan in 1.5; ORC write stays a dated DECLARED refusal; XML stays declared.
- R-17a-2 (owner, 2026-09-14, ruling Q-17a-2 of 2026-09-16): Rust-first — every
  kernel, conversion, planner rule and type rule lands in Rust; Python holds
  names, argument shapes and API plumbing only.
- R-O-1..R-O-10: the card's O-1..O-10 rulings, implemented as C-002..C-009.

## Questions

None yet.

## Gates

| gate | result |
|---|---|
| `make verify` | TBD |
| whole facade suite | TBD |
| whole parity suite | TBD |
| `cargo test -p repark-core orc` | TBD |

VERDICT: 11 clauses, 0 PROVEN, 11 OPEN, 0 REJECTED.
