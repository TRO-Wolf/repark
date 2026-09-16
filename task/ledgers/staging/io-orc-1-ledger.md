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
| C-001 | `orc-rust 0.8.0` (default-features off) lands in workspace deps + `repark-core` deps; `cargo update --workspace --offline` moves no existing crate version; Apache-2.0 + transitive licences pass the licence gate; `cargo build -p repark-core` green. | The Cargo.lock diff check; the gate output; the build rc. | PROVEN | `orc-rust = { version = "0.8.0", default-features = false }` (sync reader only; all codecs are non-optional deps). Lock diff: 1 line (the `repark-core → orc-rust` edge); 618 packages before/after, zero version moves — 0.8.0 was already locked via `datafusion-orc`. New decoders: snap BSD-3-Clause, lzokay-native MIT, lz4_flex MIT, the zlib wrapper MIT/Apache-2.0, zstd MIT, prost/snafu/num Apache-2.0-or-MIT — all in the deny allow-list. `cargo deny check licenses` → `licenses ok`, rc 0. `cargo check -p repark-core --offline` rc 0, `cargo build -p repark-core --offline` rc 0. pins: io-orc-1/C-001 |
| C-002 | O-1: the typed read answers the `orc_typed` cell — 19 columns, the cell's simpleString byte-exact, every column nullable, NULL row intact; `format("orc").load` answers `orc_typed_format_load`. | `python/repark/tests/test_io_orc_1.py` O-1 pins. | PROVEN | 45/45 pins green (`test_io_orc_1.py`). Findings: (a) Spark stores per-column `spark.sql.catalyst.type` attributes in the ORC type tree — `timestamp_ntz` rides on ORC LONG — and orc-rust 0.8.0 drops them, so `repark-core/src/orc_footer.rs` re-reads the footer (tail + block-framed decompress in all five codecs + minimal protobuf parse) and the scan maps LONG→`timestamp_ntz`, ORC-timestamp→`timestamp`(UTC), local-tz-kind→naive. Five codec micro-deps added (all already locked, all allow-list licences; lock still zero-move). orc-rust 0.9.0 checked and rejected: no attribute support either, and it needs arrow 59 against the workspace 58.4. (b) orc-rust converts stored UTC instants to the stripe writer-tz wall (fixtures: America/New_York); Spark ignores writerTimezone, so the scan undoes it per file (`from_local` in the stripe zone; mixed zones do NOT refuse — residue R-18b-13, the earlier refuse claim was false). (c) The probe ran in machine-zone America/New_York ambient (writerTimezone + the `ts` 22:04:05 render agree), so `ts`-bearing pins set that zone; a UTC pin cites the `orc_ts_utc_session` display. (d) File scans collect structs as dicts engine-wide (parquet identical — probed); pins normalize dict→`Row()` text, values byte-exact. pins: io-orc-1/C-002 |
| C-003 | O-2: NONE/SNAPPY/ZLIB/LZ4/ZSTD/LZO read back per `orc_codec_*`; brotli has no read pin (Spark write error, empty dir). A codec orc-rust cannot decode raises AnalysisException naming the codec. | The O-2 pins. | PROVEN | All seven codec pins green, LZO included — no HALT needed. Brotli: no decoder in orc-rust 0.8.0; no fixture exists; a brotli file fails loud at stripe read (UNMEASURED, registry note). pins: io-orc-1/C-003 |
| C-004 | O-3: directory / file / list / `*` glob reads; hidden `_`/`.` files skipped; missing path PATH_NOT_FOUND `file:` form; empty dir, nomatch filter, past modifiedBefore, non-recursive nested → UNABLE_TO_INFER_SCHEMA `42KD9`; non-ORC file → FAILED_READ_FILE.CANNOT_READ_FILE_FOOTER with the path; ignoreCorruptFiles over only-corrupt → UNABLE_TO_INFER_SCHEMA. | The O-3 pins. | PROVEN | All green. Choices: a glob matching zero files → UNABLE_TO_INFER_SCHEMA (unprobed; consistent with every other no-file shape); footers validated eagerly per file so corrupt files refuse at read time (without the flag: CANNOT_READ_FILE_FOOTER naming the path; the `Please ensure…` tail past `Could not read footer.` is unprobed — registry note); modified bounds compare file mtime against Spark timestamp text parsed as UTC, strict inequalities (boundary UNMEASURED). pins: io-orc-1/C-004 |
| C-005 | O-4: second positional binds mergeSchema → IllegalArgumentException `For input string: "<path>"`; keyword mergeSchema with two positionals → TypeError; non-str/non-list path → PySparkTypeError `[NOT_STR_OR_LIST] Argument `path` should be a str or list, got int.` (JVM Py4JError divergence noted in registry). | The O-4 pins. | PROVEN | All green. Non-bool positional mergeSchema raises the JVM's own text; garbage option-map booleans fail loud through the shared boolean gate (Spark `toBoolean` would coerce — registry note); NOT_STR_OR_LIST params `{"arg_name":"path","arg_type":"int"}` follow the error-conditions shape (unprobed — registry note). pins: io-orc-1/C-005 |
| C-006 | O-5: mergeSchema=true unions columns by name; pathGlobFilter / recursiveFileLookup / modifiedBefore / modifiedAfter filter files; basePath restores partition columns; unknown options ignored. Type-conflict merge shape UNMEASURED (no cell). | The O-5 pins. | PROVEN | All green. Merge unions by lowercase name (first-seen casing, file order, new columns appended; conflict → CANNOT_MERGE_SCHEMAS loud, UNMEASURED). Round 2: one Rust scan merges path lists exactly like a directory (no Python union). Round 3 (R-18b-12): no extension filter — the scan reads every glob match; the merge cell pin uses `m?`, and `m*` over the fixture dir refuses CANNOT_READ_FILE_FOOTER naming map.md (pinned). pathGlobFilter matches file names through the text glob matcher; modified text parsed in Rust (kernel). pins: io-orc-1/C-006 |
| C-007 | O-6 + O-7: `k=v` dirs add partition columns after data columns with Spark inference (`__HIVE_DEFAULT_PARTITION__` → NULL); leaf read adds none; user schema resolves by name case-insensitively, missing → NULL, int→string reads the text form; other mismatches refuse loud (UNMEASURED). | The O-6/O-7 pins. | PROVEN | All green. Partitions reuse `partition_discovery` (inference, HIVE_DEFAULT→NULL, basePath root); the `__HIVE_DEFAULT_PARTITION__` arm has no ORC fixture (same shared code the text/csv scans pin — noted, not re-pinned). User schema applies as an engine-side select (name match case-insensitive, missing→NULL cast, int→Utf8 cast, other mismatches loud). pins: io-orc-1/C-007 |
| C-008 | O-8 + O-9: nested-field + array select, `i = 2` filter, case-insensitive names, count; stripe/column projection is real (only projected columns decoded); stripe-statistics predicate pushdown NOT implemented (DataFusion-side filtering); `timestamp` renders in session zone, `timestamp_ntz` unshifted. | The O-8/O-9 pins. | PROVEN | All green. Column projection travels as an orc-rust `ProjectionMask` (only projected roots decoded); every filter reports Inexact so DataFusion filters after the scan (stripe-statistics pushdown explicitly not implemented). Dotted-string `select("st.y")` fails engine-wide (parquet identical — pre-existing facade gap, not ORC): the pin uses the working attribute form and the registry notes it. Case-insensitive names ride the facade resolver. `count()` covers the empty-projection scan path. pins: io-orc-1/C-008 |
| C-009 | O-10: `SELECT * FROM orc.`path`` and `CREATE TABLE t USING orc LOCATION` pin today's refusal (identical to the parquet twins on main — planner owned by run 18c); registry row IO-ORC-SQL-1 BACKLOG. | The O-10 pins + registry diff. | PROVEN | Both pins green on today's refusal text (no `crates/repark-spark` edit — planner lane owns it). Registry row IO-ORC-SQL-1 lands with C-010. pins: io-orc-1/C-009 |
| C-010 | Registry row IO-ORC-1 rewritten in place (read implemented, write DECLARED, O-4/O-7 divergences, SQL-door state); `docs/examples/io/orc_read.py` covers the read names, the refusal example still covers the write names; inventory refreshed; EX-0 count unmoved (no new public names). | The registry diff; the example runs; the EX-0 rc. | PROVEN | Registry IO-ORC-1 rewritten + IO-ORC-SQL-1 BACKLOG row (round-1 commit). `orc_read.py` runs rc 0 (round 2 re-run; only the pre-existing master-URL warning); `io_declared_refusals.py` still covers the write names. `DataFrameReader.orc` was already in `docs/examples/inventory.txt` — no new public names. `check_example_coverage.sh` rc 0 (1075 names, 966 covered, 108 backlog, 249 examples). pins: io-orc-1/C-010 |
| C-011 | No regression: `make verify`, the unit test files, the whole facade suite, the whole parity suite green with real exit codes and counts. | §Gates. | OPEN |  |

Red-first log: each clause pins red on the base tree first, then green. Red lines
pasted per clause below as they land.

### C-002/C-003 red

`.venv/bin/python -m pytest python/repark/tests/test_io_orc_1.py -q` on the base
tree (refusal still wired): **9 failed** — `test_orc_typed_read`,
`test_orc_typed_format_load`, `test_orc_codecs_read[none|uncompressed|snappy|
zlib|lzo|lz4|zstd]`, every one `PySparkNotImplementedError: [NOT_IMPLEMENTED]
orc is not implemented.` from `io_declared.py:32`.

### C-004..C-009 red (by entailment)

The O-3..O-10 pins were authored before the scan landed and first ran green only
after it did. Their red state is entailed by the C-002/C-003 red run: on the base
tree every ORC read path ended at the same `NOT_IMPLEMENTED` refusal (reader,
`format("orc").load`, and every option/signature spelling through them), so each
of these pins failed red there — the error-shape pins (PATH_NOT_FOUND,
UNABLE_TO_INFER_SCHEMA, IllegalArgumentException, NOT_STR_OR_LIST) by class
mismatch, the read pins by the refusal itself. No pin passed before the scan.

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
- R-18b-11 (L-401 P2): a user schema field whose type `user_partition_type`
  returns None for (map / struct / char(n) / varchar(n) / nested arrays) refuses
  loud with AnalysisException naming field, requested type and file type — never
  alias through. Pins: map, struct, char(3), red-first; int→string arm kept.
- R-18b-12 (L-402 P2): the scan reads every glob match (no extension filter —
  round-2's `*.orc` listing reverted, `_`/`.` rule kept). The `m*` glob over the
  fixture dir refuses CANNOT_READ_FILE_FOOTER naming map.md (pinned); the merge
  cell pin uses `m?` with the `m*`-recorded provenance in its docstring.
- R-18b-13 (L-403 P3): the mixed-writer-tz refuse claim was false — see residue.
- R-18b-14 (R-01 P2): `OrcPartition::execute` streams RecordBatches from the
  ArrowReader (text_scan pattern), never a collected Vec; peak-RSS before/after
  over a 1e6×20 multi-stripe file pasted in Gates.
- R-18b-15 (R-02/R-03 P2, DEFERRED under G-2): five footer opens per file and
  one-partition-per-file are correct-but-slow — registry IO-ORC-PERF-1 BACKLOG,
  no code this round (writer-tz/projection metadata reuse only if trivial
  inside R-18b-14).

## Residue rows (round 3, dated 2026-09-16)

- **Mixed writer timezones (L-403).** `file_writer_tz` returns None for both
  zero zones and mixed zones; `correct_writer_wall` with zone None is a plain
  cast — no conversion, no error. Exact behaviour: the first-seen wall clock
  ships unshifted. No mixed-zone oracle cell exists. Fix owned by a later unit.
- **R-04 (P3).** `file_writer_tz` walks every stripe footer even when no
  timestamp column is projected; one of the five opens per file (R-02).
- **R-05 (P3).** `align_orc_batch` materialises null arrays for unprojected
  fields per batch (~1.1 MiB transient on a 2-of-20 projection); invisible at
  current widths.
- **R-06 (P3).** `correct_writer_wall` builds timestamps per row through a
  `Vec<Option<i64>>`; timestamp-heavy scans may want a `unary` kernel. Not
  measured — follow-up only if a timestamp scan profiles hot.

## Questions

None yet.

## Round 2 (2026-09-16, run 18b) — audit findings A-5/A-6/A-7

- **A-5 comment ban.** Deleted the 5 `#` lines (2 in `session/reader.py` on the
  `orc` semantic-gate skip, 3 on `_use_probe_zone` in `test_io_orc_1.py`); both
  facts already live in `session/map.md` row 47 and `tests/map.md` (probe-zone
  line), so no map edit was owed for the move.
- **A-6 trailer.** HEAD was still `20054751` — amended in place to the exact
  `Authored-By: Muse Spark (muse-spark-1.3-contributor) <noreply@meta.ai>`
  trailer (now `555bcee1`); no other commit touched.
- **A-7 Rust-first.** `load_orc` no longer reads list items separately: every
  read is one `_native.read_orc` over a path list and `expand_orc_paths` merges
  per-item expansions (mergeSchema unions, otherwise first schema wins —
  identical to a directory). New red-first pin
  `test_orc_list_matches_glob_without_merge` (red on the old `union`:
  `UNION queries have different number of columns`; green on one scan).
  The prefix→condition map moved into the `read_orc` binding (transpose
  precedent: `_spark_*` setattr); `load_orc` only binds the shared
  structured-error methods (pure plumbing, no value branch).
- **Rust-first roll-call.** `_option_bool` stays: shared reader plumbing
  (`reader.py:615`, used by csv/json) — not ORC-only. `_check_orc_path_type`,
  the empty-list refusal and the `mergeSchema`/`path` argument shapes stay in
  Python as API plumbing. The `IllegalArgumentException` for a non-bool
  mergeSchema string is Spark's signature shape, kept at the boundary.
- **Listings keep `*.orc` names.** Round 1's lockstep `fixtures/orc/map.md`
  matched the committed `m*` glob pin and footer-failed the scan. The scan now
  lists `*.orc` names only (dir walks and glob results; a directly addressed
  file still footer-checks, so `orc_not_orc_file` is unchanged). Rationale: the
  repo's own map.md convention puts bookkeeping in every data dir — the scan
  must not read it as data. Stray non-`.orc` files under a glob/dir are
  skipped where Spark would choke (UNMEASURED, no oracle cell claims strays).
- **Clippy debt paid.** Round-1 `orc_footer.rs` test helpers failed the
  workspace clippy gate (`cast_lossless`, `-D warnings`): four lossless
  rewrites, values identical.
- **C-001 re-run.** No new crates in round 2; `cargo-deny check licenses`
  → `licenses ok`, rc 0.

## Gates

| gate | result |
|---|---|
| R-18b-14 streaming RSS | Recipe: 1e6×20 int64 zstd ORC (`/tmp/sb-orc-r3-scratch/big_1e6_20col.orc`, 7 stripes, `/tmp/sb-orc-r3-scratch/meas.py`), RELEASE module (`__debug_assertions__` False), fresh process, `/usr/bin/time -v`. Before (perf report, same shape 18-stripe file): sum20 peak 561 244 KiB. After: sum20 peak 465 072 KiB, sums correct (c00 499837043264); sum2 peak 84 440 KiB vs 77 472 KiB idle. The scan `Vec` is gone; the remainder is downstream (RepartitionExec 64 + 20-col agg) and R-02/R-03 (IO-ORC-PERF-1). |
| `make verify` | rc 0 (`/tmp/verify3.log`): ci + full Rust workspace suite green |
| whole facade suite | rc 0: 9177 passed, 372 skipped, 33 xfailed (`/tmp/facade_rs.log`, 1871 s). Skips all env-gated and sanctioned: live-JVM oracle tier (`REPARK_PARITY_LIVE` unset), real-AWS acceptance, release-only repeatability. Facade extras (numpy/pandas/polars/ml-ext) installed and imported |
| whole parity suite | rc 0: 757 passed, 2 skipped, 12 xfailed (`/tmp/parity.log`, 745 s) |
| `cargo test -p repark-core orc` | 5 passed, 0 failed |
| `cargo-deny check licenses` | `licenses ok`, rc 0 (no new crates in round 2) |
| `test_io_orc_1.py` | 46/46 green on the rebuilt module (45 oracle pins + the round-2 list-matches-glob pin, red-first) |

## Coverage attestation

```yaml
COVERAGE_ATTESTATION:
  pr_unit: io-orc-1
  categories:
    - id: AT-1
      status: ATTACKED
      evidence: Every clause walked against the card O-1..O-10, the run-16b recorded cells and the Q-15B-1/Q-17a-2 rulings; the round-2 orchestrator audit (A-5 comment ban, A-6 trailer, A-7 Rust-first) went back to the actor and all three remediated in one refactor commit.
      artifacts: [task/ledgers/staging/io-orc-1-ledger.md, python/repark/tests/facade_orc_oracle.json]
    - id: AT-2
      status: ATTACKED
      evidence: All seven Spark-written codecs, file/dir/glob/list paths, the positional signature shapes (two-positional, bad-arg, kw-after-positionals), mergeSchema on/off, pathGlobFilter, recursiveFileLookup, modifiedBefore/After, basePath, partition discovery (dir, leaf, basePath), subset/missing/wrong-type user schemas, select/filter/count/inputFiles, session-zone timestamps, both SQL-door refusals and the write refusal.
      artifacts: [python/repark/tests/test_io_orc_1.py]
    - id: AT-3
      status: ATTACKED
      evidence: The workspace clippy gate (`-D warnings`, disallowed-methods) is green; the production scan and binding carry no unwrap/expect (unwraps live only in the orc_footer.rs cfg(test) helpers); the async read carries the symmetric-with-read_text unused_async expect.
      artifacts: [crates/repark-core/src/orc_scan.rs, crates/repark-python/src/orc_io.rs]
    - id: AT-4
      status: N/A
      justification: No shared mutable state across the scan: one expansion builds one file set, each file gets an independent footer reader, partition values are per-file; the option struct is built per call.
    - id: AT-5
      status: N/A
      justification: Local file reads only. Remote paths refuse before any I/O; no credential, no network, no secret-bearing option is logged or forwarded.
    - id: AT-6
      status: ATTACKED
      evidence: Red-first throughout: the round-2 list pin failed red on the old Python union (UNION column-count mismatch) and passes on one scan; the error-shape pins assert class, message, params and SQLSTATE against the recorded cells; the *.orc listing rule was forced by a real failure (the lockstep map.md footer-crashed the committed m* glob).
      artifacts: [python/repark/tests/test_io_orc_1.py]
    - id: AT-7
      status: ATTACKED
      evidence: Full facade suite 9177 passed rc 0 on the rebuilt module, parity suite 757 passed rc 0, make verify rc 0, ruff check/format clean, example coverage rc 0, map sync 280 clean, the 5 round-1 comment lines deleted (remaining reader.py comments pre-date the unit).
      artifacts: [python/repark/tests/test_io_orc_1.py, docs/examples/io/orc_read.py]
    - id: AT-8
      status: ATTACKED
      evidence: Spark's contracts were recorded, not assumed: the 43-cell run-16b live-PySpark-4.1.2 oracle (including the brotli cell where Spark itself fails, claimed by neither door), Spark's reader signature shapes and the JVM IllegalArgumentException/NOT_STR_OR_LIST texts.
      artifacts: [python/repark/tests/facade_orc_oracle.json, python/repark/src/repark/spark/session/reader_orc.py]
    - id: AT-9
      status: ATTACKED
      evidence: Registry IO-ORC-1 rewritten (read implemented, write DECLARED) and IO-ORC-SQL-1 BACKLOG filed; every error shape pins Spark's class, messageParameters and SQLSTATE; the example covers the read names and the refusal example still covers the write names.
      artifacts: [docs/spark-sql-iceberg-parity.md]
    - id: AT-10
      status: ATTACKED
      evidence: Behaviour-move guards run: per-path Python union reds the new list pin (observed UNION column-count mismatch); unattached conditions red the condition pins (observed getCondition None before the method bind); an unfiltered listing reds the m* glob pin (observed map.md footer crash).
      artifacts: [python/repark/tests/test_io_orc_1.py]
  complete: true
```

VERDICT: 11 clauses, 11 PROVEN, 0 OPEN, 0 REJECTED.
