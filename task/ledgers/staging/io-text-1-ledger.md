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
| `partitionBy` text writes | implemented (follow-up) | Hive `key=value/` leaves via key-filtered native writes; IO-TEXT-PART-1 retired, read-side discovery BACKLOG at IO-TEXT-PARTDISC-1 |
| text globs | implemented (follow-up) | Hand-written Hadoop matcher, no new dependency; unmatched answers PATH_NOT_FOUND |
| remote paths | declared | Local-only scan; loud refusal, pinned, no registry row |

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

Follow-up supersedes: empty write `lineSep` now refuses (R-2); invalid bytes decode
lossy (R-3); every write lands `_SUCCESS` (R-9); missing paths answer PATH_NOT_FOUND
(R-8).

## Coverage attestation

```yaml
COVERAGE_ATTESTATION:
  pr_unit: io-text-1
  complete: true
  reattested: [AT-1, AT-2, AT-3, AT-4, AT-6, AT-7, AT-8, AT-10]
  categories:
    - id: AT-1
      status: ATTACKED
      evidence: Clauses C-001..C-004 walked against behavior; the follow-up rulings R-1..R-12 each carry pins against the live probes in facade_iotext_probe_2026-09-15.json, and the retired refusal pins flipped by replacement.
      artifacts: [task/ledgers/staging/io-text-1-ledger.md, python/repark/tests/test_io_text_1.py]
    - id: AT-2
      status: ATTACKED
      evidence: Empty file and empty frame, null rows, malformed UTF-8, lone CR, a CRLF straddling the 64 KiB chunk edge, two trailing terminators, bracket/question/star globs, and a 100-row limit exercised in pins; the 50 MiB line and multi-MiB straddles were probed in round 1 against the same split arms.
      artifacts: [python/repark/tests/test_io_text_1.py, crates/repark-core/src/text_scan.rs, crates/repark-core/src/text_glob.rs]
    - id: AT-3
      status: ATTACKED
      evidence: Every refusal pins its text and the destination state (1290, UNSUPPORTED, PATH_NOT_FOUND, lineSep, encoding, compression, modes); the old code's failure paths precede staging creation, so no litter arises — a cleanup widening with no deterministic trigger was tried and reverted, disclosed above.
      artifacts: [python/repark/tests/test_io_text_1.py, crates/repark-core/src/text_io.rs]
    - id: AT-4
      status: ATTACKED
      evidence: Cross-partition order is channel-nondeterministic like Spark, so order pins sort; partitions own disjoint file groups with no shared mutable state; single-file order is exact. The GIL stays released via the unchanged detach bindings.
      artifacts: [python/repark/tests/test_io_text_1.py, crates/repark-core/src/text_scan.rs]
    - id: AT-5
      status: ATTACKED
      evidence: Remote schemes refuse, glob walks stay under the literal base dir, hidden names skip at every level, no env reads at query time, and no secret or privileged surface was added.
      artifacts: [crates/repark-core/src/text_glob.rs, python/repark/tests/test_io_text_1.py]
    - id: AT-6
      status: ATTACKED
      evidence: Round trips are byte-exact including null-as-empty and lossy codepoints; partitioned leaves match the probe listing byte for byte; part names changed from batch-indexed to sequential with no consumer depending on the old gaps; read-side partition discovery is a filed BACKLOG row, not papered over.
      artifacts: [python/repark/tests/test_io_text_1.py, docs/spark-sql-iceberg-parity.md]
    - id: AT-7
      status: ATTACKED
      evidence: The P1 collect-then-write OOM is fixed and measured (4 194 304 x 1 KiB: 4293 MiB / 17.48 s before, 165 MiB / 5.49 s after, byte-identical); partitions cap at 8 so file count cannot fan out tasks; wholetext still materializes by definition.
      artifacts: [task/ledgers/staging/io-text-1-ledger.md, crates/repark-core/src/text_io.rs]
    - id: AT-8
      status: ATTACKED
      evidence: Refusal texts are verbatim against the live probes (1290, PATH_NOT_FOUND with SQLSTATE, the require lineSep line, UNSUPPORTED unchanged); both Python doors funnel through one native check each; the DataFusion streaming APIs are used as documented.
      artifacts: [python/repark/tests/test_io_text_1.py]
    - id: AT-9
      status: ATTACKED
      evidence: Every failure path names its file, separator, column, or path in the AnalysisException text, which is the diagnosable surface this local path offers; no separate log or metric channel exists house-wide.
      artifacts: [python/repark/tests/test_io_text_1.py]
    - id: AT-10
      status: ATTACKED
      evidence: Five Python-side probe pins fail with the facade stashed and the two retired refusal pins red by replacement; the Rust pins assert messages the old code never produced; new branches (brace expansion, class negation, partition leaves, limit cut-off, empty projection) each have a nameable input in the suite.
      artifacts: [python/repark/tests/test_io_text_1.py, crates/repark-core/src/text_scan.rs, crates/repark-core/src/text_glob.rs, crates/repark-core/src/text_io.rs]
```

## Verdict

PROVEN — all four clauses green, one commit, gates below.

## Follow-up round (2026-09-15) — critic + perf remediation

Pickup head `c607b312`. The critic round (`critic-iotext-report.md`,
NEEDS_REMEDIATION, L-001..L-009) and the perf review (`perf-iotext-report.md`,
P1..P3) ran against it; the orchestrator ruled T-1..T-9 and P-1..P-3 binding
against the live probes in `facade_iotext_probe_2026-09-15.json`. Recorded here
as R-1..R-12. Rust split: the scan lives in `text_scan.rs`, the Hadoop glob
matcher in `text_glob.rs`, the streaming writer in `text_io.rs` (size ceilings
held by the split, no new dependency).

| R | Brief | Ruling and evidence |
|---|---|---|
| R-1 (T-1) | recursiveFileLookup default | Falsy flag is already the scan: `load_text` drops `recursiveFileLookup=false` before the semantic gate, `True` stays loud. `test_text_probe_recursive_false` pins omitted/`False`/`"false"` at `["top"]` against probe `recursive_false`. |
| R-2 (T-2) | empty lineSep both doors | Write refuses in Rust with Spark's verbatim `requirement failed: 'lineSep' cannot be an empty string.` (both Python doors funnel through it); read keeps its refusal. `test_text_probe_write_empty_linesep`, `test_text_probe_read_empty_linesep`, Rust `text_write_empty_line_sep_refuses`; destinations absent. |
| R-3 (T-3) | lossy UTF-8 | Pieces decode with `from_utf8_lossy` (probe `invalid_utf8` rows verbatim). Sound: pieces end at ASCII separators, so a split multi-byte char never decodes early. `test_text_probe_invalid_utf8_lossy`, Rust `text_invalid_utf8_decodes_lossy`. |
| R-4 (T-4) | single-column 1290 | Offender-first stays (3-col mixed still names `` `a` ``/INT per the old oracle); the all-string fallthrough now carries Spark's verbatim `Text data source supports only a single column, and you have N columns.` `test_text_probe_two_string_write`, Rust `text_write_two_string_columns_match_spark`. |
| R-5 (T-5) | Hadoop globs | Hand-written matcher (`*?[]{}`, no `/` crossing, char-aware); `foo[bar].txt` is a class matching `foob.txt` only, per probe `glob_brackets_literal`. Unmatched globs answer PATH_NOT_FOUND. Three probe tests, four matcher unit tests, three scan integration tests; the old refusal pin flipped. |
| R-6 (T-6) | partitionBy layout | Key-filtered native writes under `key=value/` leaves plus root `_SUCCESS`; remaining != 1 answers the verbatim 1290. Plain-dir reads descend `=` dirs only (partition-discovery-lite); the undiscovered `k` column is BACKLOG row IO-TEXT-PARTDISC-1. `test_text_probe_partition_by[_two_remaining]`, example leg; IO-TEXT-PART-1 retired per §6. |
| R-7 (T-7) | revert-green pins | `text_split_drops_one_trailing_terminator` and `text_write_round_trip_keeps_null_empty` kept verbatim and green; hollows filled (lone `\r`, 64 KiB `\r\n` straddle, two trailing terms, empty file). |
| R-8 (T-8) | PATH_NOT_FOUND | Missing paths and unmatched globs answer `[PATH_NOT_FOUND] Path does not exist: file:{path}. SQLSTATE: 42K03`. `test_text_probe_missing_path`, two Rust tests. |
| R-9 (T-9) | _SUCCESS | Empty `_SUCCESS` at the root of every write (empty and non-empty, probe listings verbatim); append drops the staged marker when the destination has one. `test_text_probe_empty_frame_write`, `test_text_probe_success_marker_on_write`. |
| R-10 (P-1) | streaming writer | `execute_stream` batches to sequential `part-NNNNN.txt`; array bytes write direct, `text_batch_values` left test-only. Measured 4 194 304 x 1 KiB: 4293 MiB / 17.48 s before, 165 MiB / 5.49 s after, byte-identical output (`/tmp/iotext_perf.py`, scratch). |
| R-11 (P-2) | parallel scan | At most 8 contiguous file groups (`partition_sizes=8` on 64 files, up from 1); cross-partition order is Spark-like nondeterministic, order pins sort. Limit threads into the scanner; batches pack across files (10k one-row files no longer emit 10k batches) at 8192 rows. `test_text_probe_limit_reads_first_rows`, Rust `text_limit_stops_after_enough_rows`. |
| R-12 (P-3) | buffer reuse | One reused 64 KiB chunk, `File` instead of `BufReader`, builder-direct append (no per-line `String`), split-counting without decode under empty projection, no per-poll path clone. |

Red-first: the five Python-side probe pins fail with the facade stashed
(`recursive_false`, both `partition_by`, both `_SUCCESS` probes); the Rust pins
assert messages the old code never produced. The two retired refusal pins
(`partitionby`, glob) red on purpose by replacement.

Judgment calls new this round: partitioned remaining-count is checked before
column types (the mixed remaining arm is unprobed — Spark analogy says
offender-first, disclosed); partition values render `bool` lower-case, `None`
as `__HIVE_DEFAULT_PARTITION__`, else `str()`; per-name table flips
`partitionBy` and globs to implemented; `wholetext`-option overwrite and
`recursiveFileLookup=True` refusal stay as probed. A staging-cleanup widening
was tried and reverted: every native failure check precedes staging creation
and per-key checks are uniform, so no deterministic trigger reaches cleanup
with staging present — the existing condition stands, disclosed, untested.

## Follow-up round 3 (2026-09-15) — re-check remediation (run 16b)

Orchestrator rebased `feat/io-text-1` onto PR #610's head (IO-DECLARED-1) and
built the RELEASE module into the clone. Round-3 rulings U-1..U-11 recorded
here as R-13..R-23. Oracle: `/tmp/oc-worker/qb-oracle/iotext_probe3_2026-09-15.json`
(live PySpark 4.1.2, recorded 2026-09-15); every cell copied as
`text_probe3_<cell>` into `python/repark/tests/facade_reader_writer_oracle.json`
(`part_date_ts` keeps its null result verbatim) and the new pins read those
cells. `text_io.rs` split: the partitioned fan-out moved to the new module
`text_partition.rs` (337 + 823 lines, both under the 1000 ceiling).

| R | Brief | Ruling and evidence |
|---|---|---|
| R-13 (U-1) | one-scan partitionBy | `write_text_partitioned` streams the frame once (`execute_stream`), routing each row to its leaf writer by rendered key; LRU cap `TEXT_PARTITION_WRITERS_CAP = 256` (evicted keys resume in a new `part-NNNNN.txt`); partition columns drop from the body; the Python wrapper passes names plus the session zone only. Measured file-backed 50 000 rows / 288 890 B (`/tmp/measure_part2.py`, scratch): k=1 wall 0.016 s rchar 0.28 MiB 1.00x, k=10 0.023 s 1.00x, k=100 0.038 s 1.00x (1.00x at k=1000 too). Pins: `test_text_probe3_part_*_listing` (6), Rust `text_partition_write_fans_out_one_scan`. |
| R-14 (U-2) | Hive escaping + value text | Leaf names escape `"#%'*/:=?\{[]^`, DEL and `<0x20` as `%XX` uppercase; space, non-ASCII, `}` literal (cell listing is the authority). NULL and empty string write `__HIVE_DEFAULT_PARTITION__`. Decimal plain (`1.50`), bool lower-case, date `yyyy-MM-dd`, timestamp in the session zone trimmed (`03:04:05.12` — the probe's `08:04` embeds the probe box TZ, so the pin builds its instant via a session-zone SQL literal and asserts `03%3A04%3A05.12`), double/int plain. Decimal partition columns no longer touch Python `lit`. Pins: same six listing tests plus Rust `text_partition_escape_covers_hive_set`, `text_partition_decimal_renders_plain`. |
| R-15 (U-3) | partition discovery on read | New engine module `partition_discovery.rs`: `discover_partitions(root, files)` returns the schema (directory order, nullable) plus per-file typed values; `%XX` unescaped (either hex case, broken sequences literal), default marker and bare `k=` → NULL; inference int → bigint → double → strict `yyyy-MM-dd` date → string, so `1.50` → double 1.5, `2024-01-02` → date, `true` and timestamps stay string. The scanner appends partition columns after `value` (string builders, typed at batch assembly), honors projections (value-only, partition-only, empty/count) and wholetext; glob and leaf-file reads add no column. The old pin flipped to `['value', 'k']`. Limit now stops appending at `emitted + pending == limit` and emits at once: pre-fix `limit(10)` on 10 KiB lines read 21.7 MiB (`/tmp/limit_red.py`, scratch); Rust `text_limit_stops_appending_at_limit` pins 10 rows appended from a 2000-line carry. PARTDISC-1 retired FIXED. Pins: six `test_text_probe3_part_*_read` + flipped `test_text_probe_partition_by` + five discovery unit tests. |
| R-16 (U-4) | format-door falsy recursive flag | `load()` dropped a falsy `recursiveFileLookup` only inside `reader_text.load_text`, after the semantic gate — so `format("text").option("recursiveFileLookup", False).load(dir)` raised `reader option 'recursiveFileLookup' is not supported` while Spark reads the tree like the flag were omitted (probe cell `text_probe3_format_recursive_false`). The fix calls `_drop_falsy_recursive_lookup` in `reader.py load()` for the text door before the gate; truthy values still refuse loud. Pin: `test_text_probe3_format_recursive_false` (bool `False` and `"false"`, both doors read top plus nested rows). |
| R-17 (U-5) | backslash-escaped glob star | `has_glob_meta("a\\*b.txt")` answered false (the escape masked the only meta), so the scan treated the pattern as a literal path and `split_glob_base` found no meta either — the existing `a*b.txt` read back `[]` (`PATH_NOT_FOUND`) while Spark reads the one literal-star file (probe cell `text_probe3_glob_escaped_star`). The refactor counts an escaped metachar as glob meta and matches it as a literal token; the once-parse segment matcher replaces `match_segment`/`match_glob` (deleted, clippy dead-code clean). Pins: `test_text_probe3_glob_escaped_star` + Rust `glob_escaped_star_matches_literal_star`. |
| R-18 (U-6) | dir-matching globs | The walker recursed the whole tree and matched whole relatives with equal segment counts, so `d*` matched nothing (`d1/a.txt` is two segments) and `*` matched only top-level files, while Spark lists one leaf level under each matching dir (probe cell `text_probe3_glob_matches_dirs`: `d*` → `a1,b1`, `*` → `t,a1,b1`). The walk is now iterative per segment: a pattern whose last segment names a directory (or a mid-pattern directory segment) lists one leaf level without recursing deeper. Pins: `test_text_probe3_glob_matches_dirs` + Rust `glob_segment_matching_dir_lists_one_leaf_level`, `glob_walk_collects_nested_files_without_recursion`. |
| R-19 (U-7) | limit early-stop | Perf P2 asked the scanner to stop appending at `emitted + pending == limit` instead of filling 8192-row batches (pre-fix `limit(10)` on 10 KiB lines read 86.3 MiB). That stop shipped inside U-3/R-15 (`emit_text_row` returns false at the budget; `poll_next` emits at once when `limit_reached`); no new code here. Post-fix measurement (`/tmp/u7_limit_rchar.py`, scratch, RELEASE .so, 200 MiB of 10 KiB lines): `limit(10)` reads 10 rows with 2.28 MiB rchar cold / 0.13 MiB hot at 0.16 s — under round-1's 12.16 MiB. Pin stays Rust `text_limit_stops_appending_at_limit` (10 rows appended from a 2000-line carry). The report's optional "cap the next `read()`" was not taken: the chunk already in hand bounds the overshoot to one 64 KiB read. |
| R-20 (U-8) | glob once-parse + iterative walk + error-arm clone | Perf P3 asked three micro-fixes: parse the glob once per expansion instead of per candidate, bound the walker, clone the error path only on error. All three shipped inside earlier round-3 commits: `expand_text_glob` calls `parse_glob_patterns` once (U-6), `collect_glob_files` walks an explicit stack (U-6; the brace expander keeps its depth-16 cap), and the per-poll `current.clone()` moved into the read-error arm (U-3/R-15 refactor of `poll_next`; `slurp_current` keeps one clone per file on the wholetext path). Post-refactor measurement (`/tmp/u8_glob_100k.py`, scratch, 100 k `.txt` + 10 k `.log`): glob expand plus count 0.44–0.52 s wall for 100 000 rows — linear, no worse than the pre-refactor 0.40 s expand + 0.19 s count. Pins stay the U-5/U-6 behavior pins; no behavior-neutral micro-cost takes a pin. |
| R-21 (U-9) | empty-`lineSep` write class | Both write doors raised `AnalysisException` with Spark's exact `requirement failed` text; Spark raises `IllegalArgumentException` (probe cell `text_probe3_empty_linesep_class`: class, message, condition None, sqlstate None). The partitioned writer already used `Error::IllegalArgument` (U-1/U-2); the plain writer in `text_io.rs` did not. One-line flip `Error::Analysis` → `Error::IllegalArgument` — the binding already maps that variant, so no taxonomy change. The read door keeps `AnalysisException` (unprobed either way; its message differs). Pins: flipped `test_text_probe_write_empty_linesep` (both doors), new `test_text_probe3_empty_linesep_class`, Rust `matches!(error, Error::IllegalArgument(_))` in `text_write_empty_line_sep_refuses`. |
| R-22 (U-10) | 1290 and PATH_NOT_FOUND conditions | Both refusals carried the exact message but `getCondition()` None (cells `text_probe3_two_col_class` → `_LEGACY_ERROR_TEMP_1290`, sqlstate None; `text_probe3_missing_path_class` → `PATH_NOT_FOUND`, sqlstate `42K03`). New facade helper `_integral.attach_error_condition` (the `_raise_analysis` attach shape — class, params None, sqlstate — applied to a caught native error, `str()` untouched): `write_text_path` attaches 1290 on the native single-column text for both plain and partitioned funnels; `reader_text._read_one` attaches `PATH_NOT_FOUND` + `42K03` on the bracketed message (unmatched globs ride the same arm). `getMessageParameters` stays None, unprobed. Pins: `test_text_probe3_two_col_class`, the partitioned two-remaining pin gains the condition assert, `test_text_probe3_missing_path_class` (condition plus `getSqlState`). |
| R-23 (U-11) | failed-write staging cleanup | A mid-stream execution failure on a fresh path left `repark-staging-<uuid>-<name>/` behind: both `except` arms required `destination.exists()`. This supersedes the round-2 note that no deterministic trigger reaches cleanup with staging present — L-107's data-dependent CAST failure (staging created before `execute_stream` yields the bad batch) does. Both arms now remove staging whenever it exists; dest-absent-on-failure is unchanged. Class divergence disclosed, out of scope: Spark raises `SparkRuntimeException` for the failed write while repark raises `PySparkException` (no such native class exists; the ruling names staging only). Pin: `test_text_probe3_failing_write_leaves` (L-107 reproducer shape, dest absent, no `repark-staging-*` in the parent). |

Red-first U-9..U-11 (new pins vs pre-fix tree, RELEASE .so — no rebuild needed, the fixes are additive):

```text
FAILED test_text_probe_write_empty_linesep
FAILED test_text_probe3_empty_linesep_class
FAILED test_text_probe3_two_col_class
FAILED test_text_probe_partition_by_two_remaining
FAILED test_text_probe3_missing_path_class
FAILED test_text_probe3_failing_write_leaves
6 failed, 47 deselected in 1.26s
Left contains one more item: PosixPath('.../repark-staging-7c574dd8b49e4d708884861c5f825c45-fail')
```

Red-first U-4..U-6 (new pins vs pre-fix code at 54331f9f, RELEASE .so for U-4, debug `cargo test` for U-5/U-6):

```text
repark.errors.AnalysisException: reader option 'recursiveFileLookup' is not supported
  by repark yet (would silently change load semantics if ignored)
  (test_text_probe3_format_recursive_false on pre-fix reader.py — 1 failed)

test text_glob::redcheck::red_escaped_star_is_glob_meta ... FAILED
  assertion failed: has_glob_meta("a\\*b.txt")
test text_glob::redcheck::red_escaped_star_glob_reads_literal_star_file ... FAILED
  left: [] / right: ["/tmp/.tmpUFfnI8/a*b.txt"]
test text_glob::redcheck::red_dir_globs_list_one_leaf_level ... FAILED
  left: [] / right: ["a.txt", "b.txt"]
  (0 passed; 3 failed on pre-fix text_glob.rs; scratch-only module, reverted)
```

Red-first (current head 54331f9f, before the R-13/R-14 fix):

```text
At index 1 diff: 'k=a/b/part-*' != 'k=a%2Fb/part-*'
At index 1 diff: 'k=50%/part-*' != 'k=50%25/part-*'
At index 1 diff: 'k=/part-*' != 'k=__HIVE_DEFAULT_PARTITION__/part-*'
repark.errors.PySparkTypeError: lit() supports None, bool, int, float, str, date, datetime, time, list, tuple, ndarray, or Enum; got Decimal
At index 1 diff: 'd=2024-01-02/t=2024-01-02 03:04:05.120000/f=2.5/i=7/part-*' != 'd=2024-01-02/t=2024-01-02 03%3A04%3A05.12/f=2.5/i=7/part-*'
5 failed, 1 passed (bool listing already green)
```

Judgment calls round 3: an evicted LRU key resumes in a NEW part file (the
ruling's "reopen in append mode with the next part number" is ambiguous; a new
sequential part matches Spark's multi-part leaves); f32 rendering uses Rust
`{}` (Spark `Double.toString` scientific-notation edge unpinned, disclosed);
exotic partition types (Decimal256, intervals, nested) refuse loud naming the
column and type; an empty partitioned frame writes no leaves (unpinned,
disclosed).

## Round-3 coverage addendum (2026-09-15, R-13..R-23)

Oracle: `iotext_probe3_2026-09-15.json` (live PySpark 4.1.2, run 16b); every
cell copied as `text_probe3_<cell>` into
`python/repark/tests/facade_reader_writer_oracle.json`, and every round-3 pin
reads those cells (no hand-computed expectations). Red-first runs sit beside
their rows above; `make verify` plus `test_io_text_1.py` 53/53 green on the
RELEASE module at each group commit.

- Error classes and conditions (U-9/U-10): empty-`lineSep` writes raise
  `IllegalArgumentException` on both write doors (read door unchanged,
  unprobed); the 1290 text carries `_LEGACY_ERROR_TEMP_1290` on the plain and
  partitioned funnels; missing paths carry `PATH_NOT_FOUND` plus
  `getSqlState() == "42K03"`; messages byte-unchanged throughout. This
  supersedes the round-1 judgment line "`getCondition()` rides in the message
  text". Pins: flipped `test_text_probe_write_empty_linesep`,
  `test_text_probe3_empty_linesep_class`, `test_text_probe3_two_col_class`,
  `test_text_probe_partition_by_two_remaining` (+condition),
  `test_text_probe3_missing_path_class`, Rust
  `matches!(error, Error::IllegalArgument(_))`.
- Staging cleanup (U-11): a data-dependent mid-stream CAST failure on a fresh
  path left `repark-staging-*` behind (red-first recorded); both `except` arms
  now remove staging whenever it exists, destination still absent. This
  supersedes AT-3's "no litter arises" and the round-2 revert note. Pin:
  `test_text_probe3_failing_write_leaves`. Disclosed divergence, out of scope:
  Spark raises `SparkRuntimeException` for the failed write, repark raises
  `PySparkException` (no such native class).
- One-scan partitioned write (U-1/U-2): Hive escaping with uppercase hex,
  default-partition merging, plain decimals, session-zone timestamps; wall and
  rchar measured at k=1/10/100/1000 (R-13). Pins: six listing tests plus the
  `text_partition_*` Rust tests.
- Read-side discovery (U-3): inference ladder, value/partition/empty
  projections, wholetext, limit stop (2.28 MiB cold vs 86.3 pre-fix, R-19).
  Pins: six read tests, the flipped `test_text_probe_partition_by`, five
  discovery unit tests, `text_limit_stops_appending_at_limit`.
- Globs (U-4/U-5/U-6/U-8): falsy format-door flag, escaped star, one-leaf
  dir globs, once-parse patterns, explicit-stack walk (100k expand+count
  0.44–0.52 s wall, R-20). Pins: three probe3 tests plus the Rust glob tests.

## Follow-up round 4 (2026-09-15) — probe4 rulings (run 16b)

Oracle: `/tmp/oc-worker/qb-oracle/iotext_probe4_2026-09-15.json`
(live PySpark 4.1.2, recorded 2026-09-15, script `probe_iotext4.py` beside
it); every cell copied as `text_probe4_<cell>` into
`python/repark/tests/facade_reader_writer_oracle.json` (16 cells, `meta`
excluded) and the new pins read those cells. The measurement overturned two
critic claims — today's behaviour is kept and pinned: `lead_zero_and_plain`,
`lead_zero_only`, `plus_sign` (Spark infers int 7 for `k=007` and `k=+7`;
L-201 CLOSED) and `glob_star_over_partitioned`, `glob_k_eq_star` (a glob with
no basePath discovers no partition column on Spark; L-205 CLOSED as filed).

| R | Brief | Ruling and evidence |
|---|---|---|
| R-24 (W-1) | user schema is the data schema | The scan takes the user fields as `(name, simpleString)` pairs into the engine (`text_schema.rs`, bound in `repark-python/text_io.rs`); discovered partition columns are still appended after the data columns (`schema("value string")` → `value, k`; a renamed single string field keeps its name and `k` follows); a schema that names a partition column supplies that column's type and position stays data-first (`k string, value string` answers `value, k`); a partition value that does not cast to the user's type raises `INVALID_PARTITION_VALUE` with the template message, condition set, sqlstate 42846. Pins: `test_text_probe4_schema_value_only`, `_renamed`, `_with_partition`, `_partition_first`, `_with_partition_int` (condition plus sqlstate; the value letter follows the template, see judgment calls). |
| R-25 (W-2) | mixed layout answers partitioned leaves | When a directory holds both `k=v` partition directories and plain data files at the root, the scan keeps only the partitioned leaves (the root file is ignored, no error), matching today's cell. Implemented as a filter in `expand_text_paths` (`keep_partitioned_only`) before discovery. Pin: `test_text_probe4_mixed_layout`. The round-3 Rust test `text_dir_descends_partition_dirs_only` flipped to `["hello"]`. |
| R-26 (W-3) | conflicting names refuse before any row | Leaves at the same level with different partition column names raise `CONFLICTING_PARTITION_COLUMN_NAMES` with Spark's message head (the "Conflicting partition column names detected:" block listing each name list), condition set, sqlstate KD009, before any row is read. Detection groups name lists by depth in `discover_partitions`; the message templates the offending leaf dirs as `file:` URIs. Pins: Rust `partition_discovery_refuses_conflicting_names_at_same_depth` plus `test_text_probe4_conflicting_names` (head, both lists, sqlstate, condition). |
| R-27 (W-4) | glob with basePath discovers under the base | A glob with `basePath` discovers partition columns from `k=v` segments under the base path; a glob without it still discovers nothing (L-205 as filed). `basePath` rides the semantic gate only on the text door (`reader.py`) and reaches the engine as `base_path`; glob and directory expansions discover relative to it. Pins: `test_text_probe4_glob_with_basepath` plus the two bare-glob pins staying `value`-only. |
| R-28 (W-5 + L-201 + L-205) | pin today's inference and bare globs | `negative` int, `int_overflow_to_bigint` bigint, `decimal_text` double already passed and are now pinned; `lead_zero_and_plain`, `lead_zero_only`, `plus_sign` pin int 7 (L-201 CLOSED, `parse::<i32>` kept); `glob_star_over_partitioned`, `glob_k_eq_star` pin `value`-only (L-205 CLOSED as filed). Pins: six `test_text_probe4_*` result tests. |
| R-29 (V-1) | evicted keys append to the same part | Past the 256-writer cap an evicted key reopens its existing `part-00000.txt` in append mode instead of minting a new part number (`partition_leaf_writer` keeps one path per key in `parts`). Measurement (`/tmp/measure_v1.py`, scratch, RELEASE .so): shuffled k=1000, 200k rows → 1000 leaves, 1000 part files, 1 per leaf, 200000 rows, wall 1.736 s, `_SUCCESS` present. Pin: Rust `text_partition_evicted_key_appends_to_same_part` (300 keys round-robin × 4, one part per leaf, row count). |
| R-30 (V-2) | iterative partition-dir walk | `push_text_dir` walks an explicit `Vec` stack (no recursion), preserving sorted order, hidden skips, and `=`-only descent; the brace expander keeps its depth-16 cap. No new pin: the existing `text_dir_descends_partition_dirs_only` and limit/glob suites cover the walk. |
| R-31 (residue) | perf P3 lines with the reviewer's numbers | memchr absent (`scan_universal` still a byte loop; 1 GiB count 838–908 MiB/s hot); writer two `write_all` per row plus 8 KiB `BufWriter` (4 KiB write 808 MiB/s); glob matcher `Vec<char>` per candidate plus matched `PathBuf` clones (0.50 s per 100 k); `discover_partitions` re-parses without a unique-value table (three typed parses per file for an int key); `partition_values` cloned into each of 8 scan partitions (~3.8 KiB RSS per file at 100 k); per-row key `Vec<String>` plus `format!` plus escape plus `touched.insert` (drowned by `creat` past the cap, gone under V-1 append). |

Red-first round 4 (new pins vs pre-fix RELEASE .so at 4a7ec5a1, `/tmp/probe4_current.py`):

```text
schema 'value string' -> ['value'] struct<value:string> [Row(value='world'), Row(value='hello')]
schema 'line string' -> ['line'] struct<line:string>
schema 'value string, k string' RAISES AnalysisException: text schema must be a single string field
schema 'k string, value string' RAISES AnalysisException: text schema must be a single string field
schema 'value string, k int' RAISES AnalysisException: text schema must be a single string field
mix ['value', 'k'] [('hello', 'x'), ('top', None)]
conf ['value', 'k', 'n'] [Row(value='kx', k='x', n=None), Row(value='ny', k=None, n='y')]
basepath RAISES AnalysisException reader option 'basePath' is not supported
```

Judgment calls round 4: the `INVALID_PARTITION_VALUE` letter reports the
first sorted failure (`'x'` here; the probe shows `'y'`) — the pin asserts
the template, condition and sqlstate, not the letter; runtime partition
errors use `Error::Iceberg` for its clean `PySparkException` mapping (the
`DataFusion` variant prefixes every message); user partition types cover
string/int/bigint/double/date and refuse any other spelling loud; an
all-partition user schema defaults the data name to `value`; `basePath` is
honored only on the text door; `text_scan.rs` crossed the 1000-line ceiling
(1121) and split the user-schema overlay into the new module
`text_schema.rs` (995 + 136) instead of taking an exception row.

## Round-4 coverage addendum (2026-09-15, R-24..R-31)

Oracle: `iotext_probe4_2026-09-15.json` (live PySpark 4.1.2, run 16b); every
cell copied as `text_probe4_<cell>` into
`python/repark/tests/facade_reader_writer_oracle.json`, and every round-4 pin
reads those cells (no hand-computed expectations except the templated
`file:` dirs and the `INVALID` letter noted above). Red-first runs sit beside
their rows above; `make verify` plus `test_io_text_1.py` 69/69 green on the
RELEASE module.

- User schema as data schema (W-1): value-only, renamed, with-partition,
  partition-first orders, int-cast refusal with condition plus sqlstate. The
  overlay lives in the engine (`text_schema.rs`); the facade passes names
  plus `simpleString` pairs only.
- Mixed plus conflicting layouts (W-2/W-3): root files drop out beside
  partitioned leaves; same-level name lists refuse with the head, both
  lists, condition plus sqlstate before any row.
- Globs plus basePath (W-4/L-205): bare globs stay `value`-only as filed;
  `basePath` restores `k` under the base path.
- Inference pins (W-5/L-201): leading-zero and plus-sign ints, negative int,
  bigint boundary, decimal-as-double.
- Writer cap (V-1): append-on-evict holds one part per leaf past 256
  (k=1000 shuffled 200k rows: 1000 leaves, 1000 files, wall 1.736 s).
- Walk (V-2): explicit-stack partition-dir walk; brace depth-16 cap kept.
