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
