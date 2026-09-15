# IO-TEXT-1 — `DataFrameReader.text` and `DataFrameWriter.text` in Rust

- Date: 2026-09-14
- Branch: `feat/io-text-1`
- Base sha: `f6312c7b5a7a3c453da94dddec38603bc36e8921`
- Model: Muse Spark (muse-spark-1.3-contributor)

## Scope

Card IO-TEXT-1 of the 1.5 PySpark-parity campaign: `DataFrameReader.text(paths,
wholetext, lineSep, pathGlobFilter, recursiveFileLookup, modifiedBefore,
modifiedAfter)` + `format("text").load(...)`, and
`DataFrameWriter.text(path, compression, lineSep)` + `format("text").save(...)`,
against `python/repark/tests/facade_reader_writer_oracle.json` cells `io.text_*`
(run 15b, live PySpark 4.1.2). ORC, XML, JDBC and `na.replace` belong to
IO-DECLARED-1 and are untouched here.

Home: the text reader/writer live in Rust in `crates/repark-core/src/text_io.rs`,
next to the existing CSV/JSON read and write paths (`read_options.rs`,
`session.rs`); the Python facade binds one line each in
`session/reader.py` and `dataframe/writer_readwriter.py`, with bodies in the new
modules `session/reader_text.py` and `dataframe/writer_text.py`. DataFusion's CSV
reader cannot carry this exactly (it cannot split on `\r` alone, keep empty
lines, or take a custom multi-byte `lineSep`), so the unit writes a small
`TableProvider` / `PartitionStream` instead — said here as the card requires.

## Proposition ledger

| Clause | Claim | Verdict | Evidence |
|---|---|---|---|
| C-001 | Text read answers PySpark on both Python doors | PROVEN | `test_io_text_1.py` (19 of 21 tests): `text_read_file`, `text_linesep`, `text_wholetext`, `text_read_dir_glob`, `text_read_schema`, `text_roundtrip` read side, `format("text").load`, engine-option refusals, user-schema rename/refuse. Red first: 15 failed on base (`AttributeError: 'DataFrameReader' object has no attribute 'text'` and the writer twin), 2 passed (pre-existing refusals). |
| C-002 | Text write answers PySpark on both Python doors | PROVEN | `test_io_text_1.py`: `text_roundtrip`, `text_multi_col` and `text_non_string` with Spark's byte-exact error text, `text_write_null_row_file` (`a\n\n`), `text_write_mode_error`, custom `lineSep`, `format("text").save`, append/overwrite/ignore modes. Rust tests beside the module pin the split arms, the exact refusal text, and the null round trip. |
| C-003 | Fixture, registry rows, example coverage, declared SQL door | PROVEN | Fixture copied byte-identical (`cmp` clean) with a `tests/map.md` row; registry rows IO-TEXT-GZIP-1, IO-TEXT-SQL-1, IO-TEXT-PART-1 at §5 end; `docs/examples/io/text_read_write.py` covers both names and runs green; inventory refreshed (`len(rows) == 1008`); SQL-door refusal pinned in `test_text_sql_door_pins_today_refusal`. `pins: io-text-1/C-003` |
| C-004 | No regression | PROVEN | `test_writer.py`, `test_writer_v2.py`, `test_r1_read_formats.py`, `test_r2_read_formats2.py`, `test_e2_readwriter.py` green; full facade suite green; parity suite green; `make verify` green. |

## Per-name decision table

| Name | Decision | Reason |
|---|---|---|
| `DataFrameReader.text` | implemented | Rust scan, one `value` Utf8 column, both Python doors |
| `DataFrameWriter.text` | implemented | Rust part-file writer, both Python doors |
| text gzip compression | declared | No vendored compressor and `Cargo.toml` frozen; IO-TEXT-GZIP-1 |
| `SELECT * FROM text.\`<path>\`` | declared | SQL planner belongs to another run; IO-TEXT-SQL-1 |
| `partitionBy` text writes | declared | Partitioned layout is a future seed; IO-TEXT-PART-1 |
| text globs / remote paths | declared | Local-only scan v1; loud refusals, pinned, no registry row |

Python-allowed logic, one line per name: option overlay, path-list union, user-schema
rename, compression/partitionBy refusals, and save-mode staging are pure API plumbing
(argument checks, name binding, running no user callable); every row, split, and byte
crosses Rust.

Judgment calls the oracle does not pin: `wholetext` param default `False` wins over a
stale option (JVM-parameter theory); single-string user schemas rename, wider ones
refuse; empty read `lineSep` refuses, empty write `lineSep` joins; wholetext keeps raw
bytes including on empty files (one `""` row); hidden `_`/`.` files skipped in dirs;
`getCondition()` rides in the message text like every native error. Ceiling mechanics,
disclosed: five binding lines funded by condensing the two class docstrings (detail
moved to the maps) plus joining the CSV docstring; writer ratchets DOWN 1111 → 1109.

## Verdict

PROVEN — all four clauses green, one commit, gates below.
