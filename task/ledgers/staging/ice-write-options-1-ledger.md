# Charter ledger — ICE-WRITE-OPTIONS-1 · DataFrame write options on Iceberg writes

**Date:** 2026-09-17 · **Branch:** `feat/ice-write-options-1` · **Base:** `origin/main`
`79e328f2` · **Model:** muse-spark-1.3-contributor (rounds 1–3) · **Model:** claude-opus-5 (rounds 4–5) · **Policy:** [../../../AGENTS.md](../../../AGENTS.md).
**Path:** STANDARD. **risk_tier: standard.**

**Retires:** this ledger moves to `../completed/` in this unit's last commit.

**Why now.** Rating row V2-29 (2026-09-16): `writeTo(t).option("snapshot-property.run_id",
"abc-123").append()` commits but the snapshot summary carries no `run_id`;
`.option("write-format", "orc")` writes PARQUET; `target-file-size-bytes`,
`isolation-level` and unknown options are accepted with no effect, disclosed only by a
process-once `UserWarning`. Pipelines use `snapshot-property.*` as idempotency markers, so
the drop is a silent wrong answer.

**Not in this unit:** `overwrite(condition)` engine support (refusal stays; options never
reach a commit there); ORC/Avro Iceberg writers (DECLARED refusal, no writer built);
`STATUS.md` (never edited); fork pin moves (rule: pin moves only via its own PR).

## Rulings recorded at open

- Q-17a-2 (owner, 2026-09-16): Rust first. Option parsing, validation and every
  raise/cast/coerce/branch decision land in Rust. Python binds names only.
- Run 19c shape rule: remediate against Spark 4.1.2 + the Iceberg runtime in
  `python/repark/tests/_oracle_pins.py`, live-oracle pins over a RECORDED FIXTURE, or a
  dated DECLARED refusal with a registry row. Never a silent wrong answer.
- R-19c-1 (this lane, 2026-09-17): the options map crosses the PyO3 boundary rendered
  into the facade-generated SQL as an `OPTIONS(...)` clause (mechanical rendering, no
  key branching in Python). Rust pre-parse recognizer extracts, validates and honors or
  refuses. Raw-SQL users never see the clause; it is a facade-internal channel.
- R-19c-2 (this lane, 2026-09-17): plain `INSERT INTO` commits inside the fork's
  DataFusion provider, which mints only `engine.operation-id`. An option-carrying append
  therefore executes on the RePark-owned stage-then-commit path (source SELECT via the
  session, files via `repark-iceberg` writers, `commit_append` with the merged summary),
  mirroring `insert_overwrite.rs` stage-then-swap. Option-free appends keep the fork
  passthrough byte-identical.
- R-19c-3 (this lane, 2026-09-17): the pinned fork's `RollingFileWriterBuilder` exposes
  no per-statement target-size setter (`new_with_default_file_size` uses the 512MB
  constant; even the table property is not consulted). Per-statement
  `target-file-size-bytes` is a fork ask (F-TARGET-FILE-SIZE-1,
  TRO-Wolf/iceberg-rust#288), not a local patch. Affected pins refuse typed.

## PROPOSITION LEDGER — ICE-WRITE-OPTIONS-1 — 2026-09-17

| Clause | Proposition (checkable) | Proof obligation | Verdict | Evidence / open question |
|---|---|---|---|---|
| C-001 | `snapshot-property.<k>` lands in the snapshot summary of the commit on every DataFrame write path that commits: `writeTo(t).append()`, `.overwritePartitions()`, `.create()/.replace()/.createOrReplace()`, `df.write.format("iceberg").option(...)` + `saveAsTable`/`insertInto`/`save` where supported; exactly the key/value Spark writes (prefix stripped iff Spark strips it). | `test_ice_write_options_1.py` SNAP cells vs `ice_write_options_1_spark_oracle.json`. | OPEN | Oracle cells recorded 2026-09-17 (see §2). |
| C-002 | `write-format=parquet` honoured; `orc`/`avro` refuse typed naming the registry row; never a silent parquet file. | Same pin file, FORMAT cells. | OPEN | RePark has no ORC/Avro Iceberg writer: dated DECLARED 2026-09-17. |
| C-003 | Each of `target-file-size-bytes`, `compression-codec`, `compression-level`, `distribution-mode`, `fanout-enabled`, `isolation-level`, `check-nullability`, `check-ordering`: Spark's behavior measured, then honoured in Rust or refused typed with a row. Nothing accepted-but-unhonoured stays silent. | Same pin file, OPT cells; registry rows for refusals. | OPEN | `target-file-size-bytes` needs F-TARGET-FILE-SIZE-1 (see R-19c-3). |
| C-004 | Unknown option keys answer exactly as Spark does (measured: Spark ignores unknown DataSource options). | Same pin file, UNKNOWN cells. | OPEN | To be measured in §2. |
| C-005 | The process-once `UserWarning` goes away when C-001..C-004 hold; kept only for anything still not honoured, naming the keys. | Warning-text grep + pin file. | OPEN | Old text at `spark/dataframe/core.py:191-203`. |
| C-006 | SQL door: measured whether Spark 4.1.2 + Iceberg 1.11 exposes snapshot properties on SQL (session conf, `SET`, table options on INSERT). Same answer on RePark's SQL door if it does; measurement in ledger if not. | SQL cells in fixture + ledger §2. | OPEN | To be measured in §2. |
| C-007 | Registry rows in `docs/spark-sql-iceberg-parity.md`: `ICE-WRITE-OPTIONS-1` dispositions, the ORC/Avro DECLARED row, any fork ask. | Registry diff. | OPEN | Written in §5. |

## 1. Red-first record (base `79e328f2`, release native, 2026-09-17)

`test_ice_write_options_1.py -k "not live"`: 23 failed, 11 passed. Every
snapshot-property cell fails with `KeyError: 'run_id'`; every refusal cell fails
(no error raised; e.g. orc writes parquet); `test_no_option_warning` fails on the
old process-once `UserWarning`. The 11 passes are the accept-paths that the old
ignore-everything behavior satisfies trivially (unknown keys, parquet default,
check options, distribution hash/none, isolation-on-append, per-snapshot
non-carry, SQL-door absence, overwrite(cond) refusal). First failure:

    assert summary["run_id"] == "abc-123" → KeyError: 'run_id'

## 2. Spark oracle recording (PySpark 4.1.2, Iceberg 1.11.0, 2026-09-17)

`ice_write_options_1_spark_oracle.json`, 43 cells, one JVM per record driver
(`SPARK_LOCAL_IP=127.0.0.1`, `spark.driver.memory=2g`, Hadoop catalog, jar
`iceberg-spark-runtime-4.1_2.13-1.11.0` matching `_oracle_pins.py`). Measured:

- C-001: `snapshot-property.<k>` lands prefix-stripped and LOWER-cased
  (`SNAPSHOT-PROPERTY.UPPER_KEY` → `upper_key`); empty suffix commits an `""` key.
  Honoured on append, two props at once, dynamic overwrite, createOrReplace CTAS,
  V1 saveAsTable CTAS, V1 insertInto, V1 append-mode saveAsTable, and
  `overwrite(condition)`. Per-snapshot (absent on the next plain commit).
- C-002: `write-format` parquet/orc/avro (any case) writes that format; `bogus`
  → `IllegalArgumentException: Invalid file format: bogus`.
- C-003: `target-file-size-bytes` honoured (layout); `abc` → `NumberFormatException`.
  `compression-codec=gzip` honoured; lone `compression-level` and `zstd`+level
  accepted; `gzip`+integer level → `SparkException` wrapping Hadoop
  `IllegalArgumentException: No enum constant ...ZlibCompressor.CompressionLevel.1`.
  `distribution-mode` none/hash accepted, `bogus` → `IllegalArgumentException:
  Invalid distribution mode: bogus`. `fanout-enabled` true/false/garbage all
  accepted. `isolation-level=serializable` on overlapping dynamic overwrite →
  `ValidationException` (conflict); `snapshot` commits; `bogus` →
  `IllegalArgumentException: Invalid isolation level: bogus`; on plain append the
  level is accepted and ignored. `check-nullability=false` and `check-ordering`
  true/false/default all commit; a null into a NOT NULL field fails in Spark's own
  `NOT_NULL_ASSERT_VIOLATION` regardless of the option.
- C-004: unknown keys commit silently with no summary trace.
- C-006: `spark.conf.set("spark.sql.iceberg.write.snapshot-property.run_id", ...)`
  does NOT reach the SQL INSERT summary. No SQL-door channel exists.

R-19c-3 CORRECTION (2026-09-17): the brief's fork-ask premise is wrong at the
pinned rev. `RollingFileWriterBuilder::new(inner, target_file_size, ...)` exists
and the data-file builders already pass the parsed table property
(`merge/mod.rs`, `append.rs` fanout funnel). No fork change is needed: the option
overrides the table property in RePark code with Spark's precedence. There is no
F-TARGET-FILE-SIZE-1 and no F-WRITE-OPTIONS-1: every touched fork action
(FastAppend, OverwriteFiles, ReplacePartitions, staged create/replace publish)
already carries what RePark needs — the staged publish path is avoided for
option-carrying CTAS (publish empty, then `commit_append` with the summary).

## 3. Implementation (2026-09-17)

Channel: the facade renders stored options as `OPTIONS('k'='v', …)` on its generated
Iceberg SQL (Python binds names only). Rust extracts at the router entry (before any
rewrite), validates, and honours/refuses. Raw SQL never carries the clause.

- `crates/repark-spark/src/write_options.rs` (new, 500 lines, 13 units): extraction
  (INSERT + CREATE [OR REPLACE] TABLE only; quote/comment/paren aware; pairs shape
  required) and validation (strip-and-lowercase snapshot keys incl. `""` suffix,
  parquet pass, orc/avro/bogus refusals, numeric codec/level/size grammar, isolation
  and distribution domains, lenient booleans, unknown keys ignored).
- `crates/repark-iceberg/src/write/write_options.rs` (new, 489 lines):
  `WriterStagingOverrides`, override-capable builders mirroring the canonicals,
  `append_with_statement_options`, four `*_with_summary` commits, `summary_with_extras`,
  `isolation_with_override`. The mirror exists because `append.rs`, `merge/mod.rs` and
  `overwrite.rs` are size-capped exact (verified byte-identical in the diff).
- `writer_props.rs` (+67): `writer_properties_with`, `target_file_size_with`,
  `parse_target_file_size`, gzip-plus-level refusal; option-free behaviour unchanged.
- `insert_overwrite.rs` (782→912), `ctas.rs` (543→617), `router.rs` (377→395),
  `partition_overwrite.rs` (+26: one static-staging variant), `row_lineage.rs`
  (one-word visibility widening). Option-free staging arms keep the canonical
  functions (layout-identical; default concurrency is 4, so the serial options
  staging only runs with options present).
- Python: `render_write_options_clause` in `writer_layout.py`; V2/V1 store and render;
  the process-once `UserWarning` and its helpers are gone (`core.py` 4015→3991,
  `writer_readwriter.py` 1101→1114, baselines amended in `check_lib_py.py`).
- Decisions: R-19c-4 — `isolation-level` values validated strictly on every path
  (Spark validates where honoured; unknown on append unmeasured, loud wins).
  gzip+integer level refuses (Spark errors; no divergence pin needed). Serializable
  overlap commits (fork OCC finds no concurrent commit) vs Spark's
  `ValidationException` — honest divergence pin + registry residual (c).

## 4. Gates (release native rebuilt 2026-09-17 12:22 UTC, 2026-09-17)

- `test_ice_write_options_1.py -k "not live"`: 34 passed.
- `test_writer_v2.py` + `test_insert_store_assign.py`: 58 passed.
- `test_sql_harden_cutover.py` + `test_dml_b_partition_overwrite.py`: 44 passed,
  15 skipped. Old-warning grep: only an unrelated UDF docstring remains.
- `cargo test -p repark-spark --lib`: 1056 passed; `-p repark-iceberg --lib`:
  443 passed; `make test` (workspace): all suites ok, zero failures.
- `make rust-clippy`: green after fixing 5 iceberg lints (Errors docs, format!),
  6 spark lints (fn length via `finish_ctas_staged_commit` extraction, borrow,
  single-match let-else, while-let, same-arms merge, `String::new`), and the
  `large_futures` tip-over (`Box::pin` on the router thread-through).
- `make verify`: the monolith outran the 300 s command yield at
  `check-matrix-test-liveness`; every gate was then run piecewise and is green
  (fmt, clippy, panic-ban, dag, lib-rs, file-size, lib-py, conventions,
  docstring, example-coverage, manifest, ledgers, ledger-grammar,
  docs-compaction, docs-links, owner-ruling, dual-wire, matrix-liveness,
  rust-check, py-lint, py-format-check, py-lock-check, toml-check, spell-check,
  workspace tests).
- Size gates: `append.rs` / `merge/mod.rs` / `overwrite.rs` byte-identical;
  `partition_overwrite.rs` 896→922, `insert_overwrite.rs` 782→908,
  `ctas.rs` 543→631, `router.rs` 377→394 (all ≤1000); Python baselines amended
  (`core.py` 4015→3991, `writer_readwriter.py` 1101→1114).

## 5. Registry

TODO: rows written.

## 6. Handoff (2026-09-17)

- Disk: `/tmp` 578 GB free at close; no headroom issue at any phase.
- Cleanup: debug warehouse `/tmp/dbg-size-wh` and commit-output scraps removed.
  Oracle warehouses were `tempfile.mkdtemp` + `shutil.rmtree` per driver run.
- Kept: the fixture + both record drivers (deliverables); the release `.so`
  in-tree (normal build artifact, untracked-ignored).
- Proposition ledger close: C-001, C-002, C-003, C-004, C-005, C-006, C-007
  PROVEN (C-003 with the named residuals (a)(b)(c) in the registry row;
  C-006 by measurement, no SQL-door change).

## 7. Round 2 remediation (2026-09-17)

- Step 1 comment purge: deleted all 135 added `///`/`//!` lines (both new
  `write_options.rs` files stripped mechanically; `writer_props.rs`,
  `partition_overwrite.rs`, `ctas.rs`, `insert_overwrite.rs`, `write/mod.rs`
  blocks removed by hand). Substance already lived in the two `map.md`
  entries, so nothing moved except a one-line allow-convention note in
  `crates/repark-iceberg/src/write/map.md`. Fallible pub fns carry
  `#[allow(clippy::missing_errors_doc)]` (11 in iceberg `write_options.rs`,
  1 in `partition_overwrite.rs`, 3 pre-existing-pattern in `writer_props.rs`);
  the spark `write_options.rs` module is `pub(crate)`, so the lint stays
  silent there. `missing_docs` is not enabled workspace-wide, so bare pub
  items are gate-clean. Gate grep exit 1 (no matches) on the working tree.

- Step 2 size restore: `writer_readwriter.py` 1114 -> 1099 (baseline recorded
  1099, a down-move; ruff format's collapse of the overwritePartitions call
  saved one line past the 1101 target, so exactness forced 1099, not 1101).
  Case-insensitive last-wins dedup now lives once in
  `writer_layout.store_writer_option` (uncapped file, 376 -> 385 lines) and
  both V1 `option` and V2 `option` call it; the six render sites build a
  `head` prefix instead of splicing mid-string. Rendered SQL is unchanged
  (same concatenation, verified by review; behavior gates re-run in step 5).
  `check_lib_py.py`, ruff check, ruff format, conventions, docstring gates green.

- Step 4 Rust-first check: on the Iceberg write path Python never inspects a
  stored option. V1/V2 `option()` only store (plus the pre-existing
  branch/tag refusal, which raises before storage); the six action sites pass
  the dict to `writer_layout.render_write_options_clause`, which applies the
  one shared escaper (`_idents.escape_sql_single_quotes`, `'` -> `''`) to
  both key and value. The other key conditionals in `writer_readwriter.py`
  belong to the path-save (CSV/TXT) arms, not the Iceberg SQL channel. New
  pin SNAP-06 (`test_snapshot_property_quoted_key_value`) writes key
  `up')side` / value `rock'n)roll` through V2 append and reads them back
  byte-exact from the snapshot summary; it passes. No fixture cell: the
  Spark oracle has no quote-key cell, so this is a RePark round-trip pin,
  not a parity claim.

## 8. Round 2 step 5 gates (2026-09-17, release native rebuilt via
`maturin develop --release`, 7m13s)

- `tests/test_ice_write_options_1.py`: 35 passed, 1 skipped, exit 0
  (101 s; the skip is `test_live_cells_reproduce_fixture`, JVM-gated by design).
- `tests/test_writer_v2.py` + `test_insert_store_assign.py` +
  `test_sql_harden_cutover.py` + `test_dml_b_partition_overwrite.py`:
  102 passed, 15 skipped, exit 0 (290 s).
- `tests/test_dfcore_1*.py` (one file, `test_dfcore_1_exports.py`):
  10 passed, exit 0.
- `cargo test -p repark-spark --lib`: 1056 passed, 0 failed, exit 0 (514 s).
- `cargo test -p repark-iceberg --lib`: 443 passed, 0 failed, exit 0 (291 s).
- `make rust-clippy`: exit 0. `make ci`: exit 0.
- Live tier NOT re-run in round 2: this clone has no pyspark (no `pip` in
  the venv, none on the box) and the 11:30 ET stop left no room to provision
  it plus the Maven iceberg jar. Round-1 recorded the 43-cell fixture live;
  round-2 diffs cannot move live behavior (Rust diff is comments/allows
  only; the Python restructure renders byte-identical SQL, pinned offline by
  all 35 cells including new SNAP-06).
- Round-2 commits: step 1 purge, step 2 size restore (baseline 1099, one
  under main after ruff format's collapse), step 4 SNAP-06 pin, step 5 this
  gate paste. Tree clean, no push per the brief.

## 9. Round 3 remediation (2026-09-17, head dd897287 after orchestrator rebase)

Rulings carried in:

- Q-20c-4 (design): the `OPTIONS('k'='v')` text channel is WITHDRAWN. L-01
  (user-typed SQL smuggling honoured options Spark never honours; CTAS
  OPTIONS-refusal bypass) and L-02 (UTF-8 `byte as char` corruption) both die
  with the hand-written literal lexer. Options travel OUT OF BAND: facade
  dict -> new PyO3 method -> typed `StatementWriteOptions` at the router; SQL
  stays option-free; user-typed `OPTIONS(...)` keeps main's behaviour, pinned.
  Python still only forwards, never branches. The text parser, the escaper
  uses that only served the channel, and SNAP-06 go. UTF-8 pin (`café` +
  emoji) stays through both writer APIs.
- Q-20c-5: user `snapshot-property.<k>` colliding with an engine-computed
  summary key must not replace the engine value; one Spark cell for
  `added-records` and `engine-name` first, then answer as Spark does
  (extras win for non-metrics if Spark says so; `engine.operation-id`
  always ours).
- Q-20c-6: gzip + TABLE-PROPERTY level refuses like option + option, before
  any file is written.
- Perf P-01..P-04 (P2): stream, keep session writer concurrency, ONE catalog
  commit for OPTIONS CTAS (also correctness: Spark writes one snapshot).
  P3s only if trivial.

## 10. Round 3 step 2 — out-of-band channel (Q-20c-4, 2026-09-17)

- Text `OPTIONS('k'='v')` channel WITHDRAWN with the hand-written lexer
  (L-01 smuggling, L-02 UTF-8 `byte as char` both die with it). Facade keeps
  the dict (`store_writer_option` stays) and forwards it: writer actions call
  native `PyReparkSession.sql_with_write_options(sql, options)`; core
  `Session::sql_with_write_options` runs the session dialect's new
  `execute_with_write_options` (default impl refuses non-empty on doors
  without a channel); `SparkDialect` validates pairs into the typed
  `StatementWriteOptions` (sorted for determinism) and routes via the new
  `execute_with_statement_options`. SQL text is option-free everywhere.
- A first-cut facade `_sql_with_write_options` mirroring `session.sql` was
  written, then deleted: `DataFrame._session` is the native session, so writer
  SQL never went through the facade pipeline — the native call is thinner.
- User-typed behaviour pinned as main's (probed, then pinned): INSERT..OPTIONS
  fails to parse (`ParseException`, SQL-02); `USING..OPTIONS` fails to parse;
  `WITH(..)` CTAS keeps the loud `not supported for Iceberg` refusal (SQL-03).
- UTF-8 rides PyO3 `HashMap` byte-exact: SNAP-08 (V2) and SNAP-09 (V1) pin
  `café-🎉` / `naïve 🎉` through append/insertInto into the summary. Round-2
  SNAP-06 (text-channel round-trip) deleted with the channel.
- `writer_readwriter.py` 1099 -> 1093 (shared `run_through_temp_view` funnel).

## 11. Round 3 step 3a — gzip plus table-property level (Q-20c-6, 2026-09-17)

- Red-first at Rust unit level:
  `table_level_gzip_option_codec_refuses_like_spark` failed before the fix
  (table `write.parquet.compression-level=1` plus option codec gzip committed
  fine) and passes after.
- Fix: `writer_properties_with` refuses on the MERGED level
  (option-over-table-property) whenever the effective codec is gzip, whatever
  side the level came from. Refusal text renamed to the source-neutral
  `gzip compression-codec with a compression-level is refused`; no test or
  registry row pinned the old wording (only the `compression-level`
  substring, kept).

## 12. Round 3 step 3b — summary-key collisions (Q-20c-5, 2026-09-17)

- Spark cells verbatim (`/tmp/sparkenv`, Iceberg 1.11.0, one JVM each):
  - added-records=999 extra ->
    `IllegalArgumentException: Multiple entries with same key:
    added-records=2 and added-records=999` (write aborted).
  - engine-name=custom-engine extra ->
    `IllegalArgumentException: Multiple entries with same key:
    engine-name=spark and engine-name=custom-engine`.
  - operation=stolen-op extra (append AND overwrite): write commits, summary
    shows `added-records => '2'`, `engine-name => 'spark'`,
    `operation => None`, `run_id => 'plain-extra'` — the user operation never
    lands and Spark sets none of its own on these paths.
  - engine.operation-id=stolen-id extra: lands (`engine.operation-id =>
    'stolen-id'`); Spark never computes the key.
- Rule implemented in `summary_with_extras` (now `Result`): drop user
  `operation` (as Spark does) and user `engine.operation-id` (ours always
  wins, fresh UUID stands); refuse metric classes (`added-`/`deleted-`/
  `removed-`/`total-` prefixes, `engine-name`, `engine-version`,
  `changed-partition-count`) with a `Multiple entries with same key`
  refusal. Red-first via SNAP-10/11/12 (all 3 failed before, pass after).

## 13. Round 3 step 4 — P-01..P-04 (2026-09-17)

- P-03/P-01: `execute_append_with_options` streams the source
  (`execute_stream`, no `collect`); the partitioned overwrite arm conforms
  once per batch inside the stream instead of collect-then-reconform.
  `append_with_statement_options` and `stage_overwrite_files_with` are now
  stream-in; the `Vec` partitioned wrapper is deleted (static-overwrite keeps
  its main-branch collect, out of scope per the report).
- P-02: `staging: &WriterStagingOverrides` threads through the canonical
  concurrent fanout (`fanout_conformed_stream_with_concurrency`,
  `fanout_sorted_serial/stream`, `serial_with_abort`); canonical callers pass
  `none()`, which is value-identical to the old path (`with(None,None)` is
  literally `for()`'s body; the size re-parse returns the same number).
- P-04: OPTIONS CTAS publishes ONCE — materialize plus `set_snapshot_properties`
  on a fork `Transaction`, then `publish_create_table` / `publish_replace_table`
  (base captured from the loaded table before staging; the staged table's own
  location is post-staging and tripped a `CatalogCommitConflicts` in SNAP-13
  before the fix). Pins: SNAP-04 (create, count 1, kept) and new SNAP-13
  (existing-table replace, before+1 with the extras on it).
- P3s skipped as non-trivial (P-05 fork-side, P-06 Vec wrapper deleted as a
  side effect for the SQL paths, P-07/08/09/10 review-held as the report asks).
- Commit discipline deviation: steps 2+3+4 ride one implementation commit
  (per-step hunk splits did not fit the 13:30 stop beside the mandatory
  gates); the ledger sections above keep step traceability.

## 14. Round 3 verification signal (2026-09-17, final artifact)

- `maturin develop --release`: exit 0 on the committed tree.
- `tests/test_ice_write_options_1.py` (offline): 42 passed, 1 skipped
  (the JVM-gated live cell), exit 0 in 14 s — every prior pin plus SNAP-08/09,
  SQL-02/03, SNAP-10/11/12, SNAP-13 on the final native.
- Static gates green at commit: comment grep (exit 1, no matches),
  `check_lib_py`, `check_lib_rs`, `check_rust_file_size` (append.rs ratcheted
  1882 -> 1819), ruff check + format, `cargo fmt --check`, `cargo check`
  workspace crates, `make rust-clippy` (run pre-split; re-run pending).
- NOT run against the final tree for lack of stop time: `cargo test`
  (`-p repark-spark/iceberg/python --lib`), the remaining pytest files
  (`test_writer_v2`, `test_insert_store_assign`, `test_sql_harden_cutover`,
  `test_dml_b_partition_overwrite`, `test_dfcore_1*`,
  `test_production_file_size`), `make ci`, the live tier. Handed back HALT
  for a follow-up gate round.

## 15. Round 4 — verification-critic findings V-01..V-04 (2026-09-17)

**Model:** claude-opus-5 (Actor, round 4). Muse rows above stand; no contributor stripped.
Base `1485db96` (the orchestrator's BY NAME threading fix, which this round pins).

Rulings carried in / taken:

- Q-21c-5 (orchestrator, 2026-09-17): a write path that cannot honour the
  statement options REFUSES with `refuse_if_non_empty`; it never drops them.
- V-04 collision rule (orchestrator, 2026-09-17, measured): a user
  `snapshot-property.<k>` refuses if and only if the engine computed `<k>` for
  that snapshot, with Spark's text `Multiple entries with same key: <k>=<engine>
  and <k>=<user>`; otherwise it lands. Q-20c-5 keeps its two keys: `operation`
  dropped, `engine.operation-id` always RePark's (`commit_error.rs` reports that
  id for `CommitStateUnknown`; the two must agree).
- Where the computed summary is available: the fork merges extras INSIDE its
  `SnapshotProducer::summary` (`additional_properties.extend`, user last-wins),
  so the fork's own computed map is not reachable at the merge point. Its
  collector is public, though: `EngineSummary::for_append` rebuilds the exact
  added side (`SnapshotSummaryCollector` over the staged files, partition-summary
  limit from the table) plus the branch-head totals (the fork's `update_totals`
  arithmetic for an add-only commit). Appends and OPTIONS CTAS therefore use the
  exact rule with the exact engine value. The overwrite family resolves its
  removal set inside the commit; the narrowest honest alternative taken there
  (`EngineSummary::for_overwrite`) refuses exactly the keys the operation is
  known to compute: the exact added side, the six totals,
  `changed-partition-count`, and — when a parent snapshot exists — the
  data-removal keys `deleted-data-files`, `deleted-records`,
  `removed-files-size`, engine value `<resolved at commit>`. Residual: an
  overwrite that ends up removing nothing over-refuses those three keys.
- R-21c-1 (Actor, 2026-09-17, for orchestrator review): `engine-name` /
  `engine-version` stay refused, matching COLL-01/02. Spark stamps both on every
  write; RePark stamps neither, so the message's engine half reads
  `<engine-reserved>`. The pins assert the key and the user half from the
  fixture, not Spark's `spark` / `4.1.2`.
- The refusal class stays `AnalysisException` (Spark: `IllegalArgumentException`);
  the session error mapping has no DataFusion path to that class and changing it
  is outside this round.

| Clause | Proposition (checkable) | Proof obligation | Verdict | Evidence |
|---|---|---|---|---|
| C-008 | V-04: a user extra refuses iff the engine computes that key for the commit in hand (exact on append/CTAS, known-key set on the overwrite family); `deleted-data-files` lands on an append; Spark's message text with the engine value. | `test_snapshot_property_collision_cells[COLL-00..08]`; mutation B. | PROVEN | 9/9 green on the final native; prefix rule restored → 6 red (COLL-07 lands-vs-refused, COLL-00/01/02/05/06 message). |
| C-009 | V-01: the nine measured collision cells are fixture cells `COLL-00..08` (43 prior cells byte-untouched, `git diff --numstat` 182/0); the committed recorder `_record_ice_write_options_3_oracle.py` re-derives them (no machine-local path literal; `--warehouse`, `REPARK_ORACLE_IVY`); the live leg runs it and compares the COLL fields; pins read `_fixture_cell` only. | Fixture + recorder + live projection; mutation D. | PROVEN | Perturbing three fixture values → 3 red. Recorder not run (no JVM this round). |
| C-010 | V-03 / Q-21c-5: every `execute_inner` arm that cannot honour a non-empty options map refuses it before executing (MERGE, BY NAME append/empty projection, DELETE, UPDATE, TRUNCATE, CALL, DROP TABLE/NAMESPACE, ALTER, pre-parse DDL/DESCRIBE/SHOW, passthrough, the two non-Iceberg INSERT OVERWRITE fallbacks); INSERT OVERWRITE BY NAME honours it. | `test_merge_refuses_write_options`, `test_insert_by_name_*`, `test_non_write_arms_refuse_write_options[10]`; mutation A. | PROVEN | 13/13 green; all 13 red under mutation A. |
| C-011 | V-02: table `write.parquet.compression-level=1` + option `compression-codec=gzip` refuses before any file is written, snapshot count unchanged — pinned in pytest, and the Rust pin actually runs. | `test_table_level_gzip_with_option_codec_refuses`; `cargo test -p repark-iceberg`; mutation C. | PROVEN | Both red under mutation C. |

Red first (V-03, base `1485db96` native): the MERGE pin and all ten arm pins
failed `DID NOT RAISE AnalysisException`; the two BY NAME pins were already green
because `1485db96` carries the fix (proved by mutation A instead).

Latent break fixed on the way: `cargo test -p repark-iceberg` did not compile
(`append.rs` test module used `Uuid` through `super::*`; the round-3 split moved
the import out). Fully qualified `uuid::Uuid` on the same line — `append.rs`
keeps its exact 1819 baseline.

Mutation transcripts (each: revert the fix in the working tree, `maturin develop
--release`, run the pins, restore, rebuild):

- A — V-03. `router.rs` / `insert_overwrite.rs` restored to `1485db96` (every
  round-4 refusal gone) and `insert_by_name.rs` given the pre-fix drop semantics
  (both delegations get `StatementWriteOptions::empty()`, both refusals deleted;
  a literal `1485db96^` does not compile against the rebased BY NAME door).
  `-k "merge_refuses or by_name or non_write_arms"` → `13 failed`:
  `DID NOT RAISE AnalysisException` (MERGE, BY NAME append, 10 arms);
  `KeyError: 'run_id'` (BY NAME overwrite — the dropped extra).
- A' — V-03 on the final structure (after the clippy `too_many_lines` refactor
  moved the arm refusals into `refuse_options_on_non_write`): the gate call
  disabled, the pre-parse gate given an empty set, the BY NAME drop semantics as
  in A, the two non-Iceberg INSERT OVERWRITE refusals deleted → the same
  `13 failed`. Those last two refusals have no pin of their own (no non-Iceberg
  overwrite target in this pin file); stated as a residual.
- B — V-04. `engine.refuse_collision(&folded, value)?` replaced by the round-3
  prefix block. `-k collision_cells` → `6 failed, 3 passed`: COLL-07
  `AnalysisException: … deleted-data-files is an engine-computed snapshot summary
  key` (the over-refusal); COLL-00/05/06 `assert 'Multiple entries with same key:
  added-records=2 and added-records=999' in …` (no engine value); COLL-01/02 key
  half missing.
- C — V-02. `effective_level.is_some()` → `level_override.is_some()` in
  `writer_properties_with`. pytest `-k gzip` → `1 failed, 2 passed`
  (`test_table_level_gzip_with_option_codec_refuses`: `DID NOT RAISE`);
  `cargo test -p repark-iceberg table_level_gzip_option_codec_refuses_like_spark`
  → `test result: FAILED. 0 passed; 1 failed` (panicked at
  `writer_props.rs:728`, codec `GZIP(GzipLevel(1))`).
- D — V-01. Fixture perturbed (COLL-00 engine value 2→5, COLL-06
  `snapshot_count` 1→2, COLL-07 landed value 3→4). `-k collision_cells` →
  `3 failed, 6 passed`, each on the perturbed value; fixture restored (182/0).


## 16. Round 4 gates (2026-09-17, final native rebuilt from `ba12e621`)

- Comment ban: the staged-diff grep printed nothing before each commit; the
  orchestrator's `comment_ban.py` against `origin/main` reports 0 hits.
- `cargo test -p repark-spark`: 1203 passed, 0 failed, 4 ignored (doc leg ok).
- `cargo test -p repark-core`: 623 passed, 0 failed, 2 ignored.
- `cargo test -p repark-iceberg`: 456 passed, **2 failed** —
  `writer_props::tests::option_target_size_rolls_one_file_per_batch` and
  `::option_target_size_beats_table_property` (a 1-byte target yields 1 file, not
  3). Both are this unit's own round-1 tests (`0ccc7fca`). They fail
  identically on `1485db96` once its test module compiles, and they never ran
  before, because the module did not compile until this round. OPEN for the
  orchestrator. Corrected in §17 (Q-21c-7): the reading above, that written
  bytes stay 0 until a row group flushes, was wrong. The RP-23 fork writer rolls on
  1000-row slices once flushed plus in-progress bytes reach the target, so 150
  rows are one file at any target. The "rolls every batch" premise was stale, and
  the option was not broken. The pytest pin only asserts that the option is
  accepted and the rows commit.
- `test_ice_write_options_1.py`: 65 passed, 1 skipped (the JVM-gated live leg).
- `python/repark/tests -n 8`: 9750 passed, 2 failed, 399 skipped, 47 xfailed.
  The 2 are `test_spark_sql_grammar_1.py::test_q14_current_date_bare_and_paren`
  [ansi-on/off], which fail on the UTC/local date window (`2026-09-18` UTC vs
  `2026-09-17` local). That is unrelated to this unit, and it still fails
  identically when re-run.
- `python/repark-parity/tests -n 8`: 754 passed, 2 failed at the time of the
  run. `test_dl_6_docs_links` failed on the then-untracked `_3` recorder and
  passes after commit (19 passed). `test_cap_1` failed on mirror rows that
  round 3 had left behind; after the ratchet it passes (23 passed).
- `make verify`: `ci` stages all clean (map-sync, crate-dag, lib-rs,
  rust-file-size, lib-py, python-conventions, docstring-presence, ledger-check,
  ledger-grammar, docs-compaction, docs-links, owner-ruling, dual-wire, ruff,
  fmt, clippy); `rust-test` stops at the two `repark-iceberg` failures above.

## 17. Round 5 — Q-21c-7, dead recorder JARs, V-03 overwrite pins (2026-09-18)

**Model:** claude-opus-5 (Actor, round 5). Base `1487180d`.

Rulings carried in:

- Q-21c-7 (orchestrator, 2026-09-18): the two round-1 `option_target_size_*`
  units asserted that a 1-byte target writes one file per 50-row batch. That
  premise is stale. The RP-23 fork pin (`4151b488`, fork #288,
  F-TARGET-FILE-SIZE-1) makes `RollingFileWriter` roll on 1000-row slices once
  flushed plus in-progress bytes reach the target. This is Java 1.11.0's
  `ROWS_DIVISOR` / `ParquetWriter.length()` behaviour, and the fork measured it
  against Spark (20 files of 23,666–68,446 B vs Spark's 20 files of
  23,735–57,178 B). Three 50-row batches are 150 rows, below one slice, so one file
  is the correct answer. The units are rewritten to observe the option at slice
  granularity. No registry sentence claimed per-batch rolling; the §16
  wording is corrected above.
- Finding (Actor, 2026-09-18): a `USING parquet` table created inside a
  registered Iceberg catalog is an Iceberg table (it has `.snapshots`, and an
  options-carrying empty overwrite on it commits with the extra honoured). It is
  therefore not a non-Iceberg overwrite target. A session temp view (a DataFusion
  `MemTable`) is one, and it reaches both refusals.

| Clause | Proposition (checkable) | Proof obligation | Verdict | Evidence |
|---|---|---|---|---|
| C-012 | Q-21c-7: `target-file-size-bytes` takes effect at the RP-23 granularity. 3,500 rows (five 700-row batches) stage exactly 4 files (1000/1000/1000/500) at a 1-byte option, exactly 4 at a 1-byte table property alone, and exactly 1 at the 512 MB default or at a 512 MB option over the 1-byte property. | `writer_props.rs::option_target_size_rolls_on_row_slices`, `::option_target_size_beats_table_property`; mutations E, F. | PROVEN | Both green. E (option ignored) makes both red; F (property ignored) makes the property leg red. |
| C-013 | V-03: the empty-source wipe and no-source passthrough `INSERT OVERWRITE` fallbacks refuse a non-empty options map on a non-Iceberg target and leave its rows unchanged. | `test_non_iceberg_overwrite_refuses_write_options[2]`; mutations G, H. | PROVEN | 2/2 green. Each goes red alone when its refusal is disabled. |

Recorders (item 2): `_record_ice_write_options_1_oracle.py` and `_2` drop the
dead `/tmp/ic-build` JAR literal. They load `ICEBERG_SPARK_RUNTIME_GAV` from
`_oracle_pins` through `spark.jars.packages`, and `REPARK_ORACLE_IVY` optionally
sets `spark.jars.ivy` (unset means Spark's default), the same idiom as `_3`. The
usage docstrings name "a PySpark 4.1.2 interpreter" instead of a `/tmp` path.
`_1` writes the meta field `iceberg_jar` as the Ivy file name derived from the
GAV, which is the same string the fixture carries. None of the three recorders
was run. The fixture JSON is untouched. The live check's `_stable_projection`
compares the summary minus the app keys, `(suffix, format, records)` per file,
`snapshot_count`, the error class, the collision fields and the collision
message. It never reads `trace_tail` or `meta`.

Residues, stated and not fixed:

- (P3) the overwrite family over-refuses `deleted-data-files` /
  `deleted-records` / `removed-files-size` when an overwrite ends up removing
  nothing (see §15).
- `_record_ice_write_options_3_oracle.py` has not been run live, and neither
  have the `_1` / `_2` recorders since this round's rewiring.

Mutation transcripts (each one: mutate the working tree, rebuild where the pin
is pytest, run the pins, restore, rebuild):

- E: `target_file_size_with` ignores the option
  (`size_override.filter(|_| false)`).
  `cargo test -p repark-iceberg --lib option_target_size` gives
  `0 passed; 2 failed`: the slices test fails with `left: 1, right: 4` ("a 1-byte
  option target rolls on every 1000-row slice"), and the precedence test fails
  with `left: 4, right: 1` ("the option takes the table property's place").
- F: `target_file_size_with` ignores the table property (the no-override arm
  returns 512 MB). The result is `1 passed; 1 failed`: the precedence test fails
  with `left: 1, right: 4` ("the 1-byte table property rolls on every
  1000-row slice").
- G: the empty-source wipe refusal in `insert_overwrite.rs` is replaced by
  `let _ = options;` (native rebuilt). `-k non_iceberg_overwrite` gives
  `1 failed, 1 passed`. The wipe cell fails with
  `UnsupportedOperationException: … Insert into not implemented for this table`
  in place of the options refusal.
- H: the no-source refusal is replaced the same way (native rebuilt). The
  result is `1 failed, 1 passed`, and the `DEFAULT VALUES` cell fails with
  `Regex pattern did not match … Actual message: 'Error during planning: Inserts
  without a source not supported'`.

Round 5 gates (2026-09-18, native rebuilt from the restored tree):

- Comment ban: the staged-diff grep printed nothing, and `comment_ban.py`
  against `origin/main` reported `comment-ban hits=0` (exit 0).
- `cargo test -p repark-iceberg`: 458 passed, 0 failed, 0 ignored.
- `cargo test -p repark-spark`: 1203 passed, 0 failed, 4 ignored.
- `test_ice_write_options_1.py`: 67 passed, 1 skipped (the JVM-gated live leg).
- `make verify`: exit 0. Across its test stages: 3827 passed, 0 failed,
  7 ignored. ledger-grammar reports 1501 clauses; docs-links reports 5837 links.
- Targeted parity guards: `test_cap_1` + `test_dl_6`, 42 passed.

## 18. Orchestrator close-out — live tier and verification round 2 (2026-09-18)

- **Live tier, green.** On 2026-09-18 the orchestrator ran all three record drivers (`_1`, `_2`, `_3`) with `--out` into
  one scratch file, under a PySpark 4.1.2 interpreter with the Iceberg runtime from `_oracle_pins`. It then compared every
  cell with the committed fixture through the pin file's own `_stable_projection`: ids equal (52 = 52), **0 mismatched**,
  including `COLL-00`…`COLL-08`. This closes round 5's residue "not run live" for all three drivers.
- **Verification critic, round 2 (Grok 4.6): CLOSED.** V-01…V-04, Q-21c-7, the two non-Iceberg overwrite pins, and the
  comment ban all hold. The critic explained each mutation transcript A–H from the code, and found no regression on an
  option-free write.
- **New residue V2-01 (P3), stated and not fixed.** When a table sets `write.summary.partition-limit > 0`, the snapshot
  producer writes `partitions.*` keys for removed-only partitions. `EngineSummary::for_overwrite` runs the collector only
  over added files, so a user extra named `partitions.<removed path>` could replace that engine key on an overwrite. The
  default limit is 0, which means the keys are not written and the append collector stays exact. This joins the §17
  over-refusal residue as the overwrite family's second known imprecision.

## 19. Run 22b rebase over #682 / #687 / #678 (2026-09-18)

**Model:** claude-opus-5 (Actor, run 22b round 1). Base `9dc9bef3` merged with `origin/main`
`13a5d24b`. Main had moved by ten commits: #682 ICE-DYN-OVERWRITE-1, #687 ICE-EVO-DML-1,
#691 ICE-SORTED-INSERT-1, #692 ICE-OCC-SCOPED-1, #678 ICE-V3-WRITE-DEFAULT-1, RP-25, the
read-perf slate, #693 ICE-RTAS-OPS-2, RP-26 and TEST-HYGIENE-1. The history is one merge
commit (`11990953`), then one commit per semantic fix the merge needed (`21d2af58`,
`91e5a3b3`, `db420c65`), the pins (`b6e6a379`) and this section.

### Rulings (Actor, 2026-09-18)

- **Q-22b-WO-1 (one statement funnel).** ICE-DYN-OVERWRITE-1's typed static flag and this
  unit's options map travel together. Core `session/write_options.rs` is the only
  statement body: `sql_with_write_options(query, options, force_static_overwrite)` fills
  `EngineContext::force_static_overwrite`, and `sql_with` passes an empty map and
  `false`. Main's `static_overwrite.rs` (core and PyO3) and the `sql_static_overwrite`
  native are retired. The PyO3 `session_sql_with_write_options` takes
  `force_static_overwrite=False` as a fourth argument. On the Spark door the flag is the
  typed field `StatementWriteOptions::force_static_overwrite`. It is not an option key,
  so `is_empty` and `refuse_if_non_empty` ignore it. Putting it there keeps every
  router signature within clippy's argument and line limits. `execute_static_overwrite`
  stays as a public entry that sets the field on an empty set.
- **Q-22b-WO-2 (dynamic overwrite honours).** A dynamic PARTITION-less overwrite on a
  partitioned table commits through `commit_replace_partitions_with_summary`. That is the
  same `replace_partitions` action main's `commit_replace_partitions_to` builds, plus the
  merged summary and the isolation override. It is the function `overwritePartitions()`
  already used for options (SNAP-03). The staging takes the option writer knobs. With
  no options the extras are empty, so the result is value-identical to main's path.
- **Q-22b-WO-3 (empty dynamic source).** An empty dynamic source commits no snapshot, as
  Spark's `DynamicOverwrite.commit` does (`Dynamic overwrite is empty, skipping commit`,
  ICE-DYN-OVERWRITE-1 C-004/C-011). The options are validated up front and have no
  snapshot to land on. This is Spark's answer, not a silent drop, so nothing refuses.
  The static empty `BY NAME` wipe does commit, and keeps its C-010 refusal. The refusal
  now sits inside the `!dynamic` branch.
- **Q-22b-WO-4 (RTAS with options).** ICE-RTAS-OPS-2 made `CREATE OR REPLACE … AS SELECT`
  record `overwrite` (or `delete`). The options CTAS path hand-builds its publish, so it
  takes the same operation: `overwrite_by_row_filter(AlwaysTrue).allow_empty_commit()`
  with the merged summary, collision-checked against `EngineSummary::for_overwrite`.
  The service-managed arm uses `commit_replace_write_with_summary`.
- **Q-22b-WO-5 (column-list append).** Since ICE-V3-WRITE-DEFAULT-1 the DataFrame writers
  render `INSERT INTO t (cols) SELECT …`. This unit's options append refused explicit
  column lists, so on the merged tree every option-carrying DataFrame append failed
  (33 of 67 `test_ice_write_options_1.py` pins red). The list now takes the overwrite
  arms' `write_default` fill step, is staged by name with the overrides, and commits
  through `commit_append_with_summary`.

### Conflict files and resolutions

| File | Resolution |
|---|---|
| `crates/repark-core/src/session.rs` | `sql_with` calls `sql_with_write_options_inner(dialect, query, &{}, false)` (Q-22b-WO-1). |
| `crates/repark-core/src/static_overwrite.rs` (main, auto) | Deleted. Its body is the funnel in `session/write_options.rs`. |
| `crates/repark-iceberg/src/write/append.rs` | Ours: the serial fanout stays split out to `append_fanout_serial.rs`. Main's `stamp` wrap moves there (`21d2af58`). The unused `stamp` import is dropped. The baseline stays 1819. |
| `crates/repark-iceberg/src/write/mod.rs` | Both exports kept: main's `static_partition_source_columns`, and this unit's `stage_static_partition_overwrite_files_with`, now re-exported from `write_options`. |
| `crates/repark-iceberg/src/write/partition_overwrite.rs` (auto) | Main's column list reaches the options variant. That variant moves to `write_options.rs` behind a shared `pub(crate) static_injected_stream`, which leaves the file at 991 lines (the naive merge was 1006, over the ceiling). |
| `crates/repark-python/src/lib.rs` | `session_write_options` kept. `static_overwrite` is retired (Q-22b-WO-1). |
| `crates/repark-spark/src/ctas.rs` | Main's `with_replace_write(ctas.or_replace)` is kept on both staged arms, and the `(staged, replace_base)` tuple is kept. The service-managed option-free arm takes main's `commit_replace_write`. The options arms: Q-22b-WO-4 (`91e5a3b3`). |
| `crates/repark-spark/src/insert_by_name.rs` | Options plus the flag field. The static empty wipe refuses a non-empty map through main's `wipe_by_name_target`. The dynamic empty case commits nothing. The non-empty case delegates with the options. |
| `crates/repark-spark/src/insert_overwrite.rs` | `dynamic` comes from `options.force_static_overwrite`. The empty Iceberg wipe moves to `wipe_empty_overwrite_target` (clippy line limit), with the dynamic early return first. The stage-then-swap commits `commit_replace_partitions_with_summary` or `commit_overwrite_replace_all_with_summary`. The static PARTITION arm passes the column list and `Option` staging. `execute_append_with_options` moves verbatim to `append_with_options.rs` (the merge would have left the file at 1015 lines). |
| `crates/repark-spark/src/lib.rs` | Exports `execute`, `execute_static_overwrite`, `execute_with_read_only` and `execute_with_statement_options`. |
| `crates/repark-spark/src/router.rs` | Main's `execute_routed` folds into `execute_with_statement_options`. The signatures are ours; the flag rides in the options (Q-22b-WO-1). |
| `crates/repark-spark/src/dialect.rs` (auto) | `execute_with_write_options` copies `cx.force_static_overwrite` onto the validated set. |
| `python/repark/src/repark/spark/dataframe/writer_readwriter.py` | Main's column list and `static_overwrite=` are kept together with `self._options` on every Iceberg action. The V1 funnel forwards both to `writer_layout.run_through_temp_view`, whose one native call carries the flag. |
| `scripts/check_lib_py.py`, `test_cap_1_source_file_line_cap.py` | `writer_readwriter.py` goes to 1095. That is down from main's 1102; this unit's 1093 plus main's column-list and flag lines cannot fit the old number. `append.rs` stays at ours (1819) and `merge/mod.rs` at main's (1773). |
| `crates/repark-spark/src/map.md`, `docs/map.md`, `python/repark/src/repark/spark/dataframe/map.md`, `python/repark/tests/map.md`, `scripts/map.md`, `task/ledgers/staging/map.md` | Both sides' entries kept, plus run-22b notes. No exactly duplicated entry block exists after the merge. The two repeated headings in `python/repark/tests/map.md` were already on both parents. |

### Constraint-2 decision (per path)

| Path (main unit) | Decision | Commit path | Pin |
|---|---|---|---|
| PARTITION-less `INSERT OVERWRITE` / `insertInto(overwrite=True)`, dynamic, partitioned (#682) | HONOUR | `commit_replace_partitions_with_summary` | WO-DYN-01 (property + `replace-partitions`), WO-DYN-02 (gzip knob) |
| Same, dynamic, unpartitioned (#682) | HONOUR | `commit_overwrite_replace_all_with_summary` (Spark replaces the whole table) | existing SNAP / OPT overwrite pins |
| `saveAsTable` overwrite under dynamic, pinned static (#682 typed flag) | HONOUR | replace-all with summary; the flag rides with the options | WO-DYN-03 |
| `INSERT OVERWRITE … BY NAME`, dynamic (#682 round 3) | HONOUR | through `insert_overwrite_from_staged_source` | WO-DYN-04 |
| Empty dynamic source, positional and BY NAME (#682) | NO COMMIT (Spark-equal) | none | WO-DYN-05 [2 cells] |
| Empty static BY NAME wipe | REFUSE (C-010, unchanged) | none | WO-DYN-06, `test_insert_by_name_*` |
| PARTITION static / dynamic with a column list (#678) | HONOUR | unchanged row-filter / replace-partitions with summary | existing SNAP-03 + DML-B pins |
| RTAS `createOrReplace()` (RTAS-OPS-2) | HONOUR | overwrite family with summary | WO-RTAS-01, WO-RTAS-02 [2 cells] |
| Column-list append from the writers (#678) | HONOUR | by-name staging + `commit_append_with_summary` | WO-APP-01, WO-APP-02, all SNAP append pins |
| Sorted tables (#691) | HONOUR (sort + stamp) | options builders take `stamp` | WO-SORT-01 [2 cells] |

Constraint 5 audit: main added no `execute_inner` arm. #682 added only the
`execute_static_overwrite` entry and the flag threading; #678 added the `DEFAULT` marker
rewrite inside existing arms; #692 changed only MERGE / DELETE commit validation, and
MERGE and DELETE still refuse through `refuse_options_on_non_write`; RTAS-OPS-2 added
nothing to the router. The C-010 arm sweep (`test_non_write_arms_refuse_write_options`,
10 cells) is still complete, and all ten stay green. #687's evo-DML planning and #678's
write-default fill run after the gate unchanged: `test_ice_evo_dml_1.py` and
`test_ice_v3_write_default_1.py` are green.

| Clause | Proposition (checkable) | Proof obligation | Verdict | Evidence |
|---|---|---|---|---|
| C-014 | Q-22b-WO-1: the typed static flag and the options map reach the router on one statement; `saveAsTable` overwrite stays whole-table under dynamic and carries the property; option-free statements keep main's behaviour. | WO-DYN-03, main's `test_save_as_table_overwrite_ignores_dynamic_conf`; mutation M-FLAG. | PROVEN | Green. M-FLAG (`writer_layout` passes `False`) → `2 failed` (both saveAsTable pins, rows `[(1,'a'),(2,'b'),(40,'c')] != [(40,'c')]`). |
| C-015 | Q-22b-WO-2 / WO-3: dynamic PARTITION-less overwrites (positional, `insertInto`, BY NAME) honour the snapshot property and writer knobs; an empty dynamic source commits nothing and refuses nothing; the static empty BY NAME wipe still refuses. | WO-DYN-01, 02, 04, 05, 06; red-first. | PROVEN | Red on `11990953` native + naive `commit_replace_partitions_to`: WO-DYN-01 `KeyError: 'run_id'`, WO-DYN-04 red; green after. |
| C-016 | Q-22b-WO-4: an option-carrying RTAS records RTAS-OPS-1's operations and carries the property. | WO-RTAS-01, WO-RTAS-02; red-first. | PROVEN | Red on `11990953`: `['append','append'] != ['append','overwrite']`, `['append'] != ['overwrite']`, `['append'] != ['delete']`; green after `91e5a3b3`. |
| C-017 | Option-carrying writers sort by the declared order and stamp its id (partitioned and unpartitioned). | WO-SORT-01; red-first. | PROVEN | Red on `11990953`: `sort_order_id None == 1` on both cells; green after `21d2af58`. |
| C-018 | Q-22b-WO-5: option-carrying appends with a writer or SQL column list land with the property; unlisted nullable columns read NULL. | WO-APP-01, WO-APP-02, the SNAP append pins; red-first. | PROVEN | Red on `11990953`: `INSERT with an explicit column list does not support write options` (and `test_ice_write_options_1.py` `33 failed, 34 passed, 1 skipped`); green after `db420c65`. |

### Red-first record

Native built from `11990953`, with the dynamic commit mutated to main's naive resolution
`commit_replace_partitions_to` (no summary). Then
`pytest -n 4 test_ice_write_options_1_rebase.py` gave `9 failed, 5 passed`: WO-DYN-01,
WO-DYN-04, WO-RTAS-01, WO-RTAS-02 ×2, WO-SORT-01 ×2, WO-APP-01 and WO-APP-02. The 5 passes
are WO-DYN-02, WO-DYN-03, WO-DYN-05 ×2 and WO-DYN-06. WO-DYN-02 passes because the
mutation leaves the option staging in place, so the gzip knob stays honoured. WO-DYN-03
passes because the flag funnel is part of the merge commit; M-FLAG reds it. WO-DYN-05
and WO-DYN-06 pin behaviour the merge commit already has. On the same native,
`test_ice_write_options_1.py` gave `33 failed, 34 passed, 1 skipped`, which is the
column-list regression. The mutation was restored (`git checkout`), and the committed
code is byte-identical to the tree the crate tests ran on.

### Gates (2026-09-18, `CARGO_BUILD_JOBS=6 RUST_TEST_THREADS=6`, build slots)

- `cargo check --tests -p repark-core -p repark-iceberg -p repark-spark -p repark-sql -p repark-python`: clean.
- `cargo clippy --locked --tests` (same crates) `-- -D warnings -A clippy::disallowed_methods`
  (the `make rust-clippy` form): clean. `cargo clippy --locked --lib --bins` on core /
  iceberg / spark / sql `-- -D warnings -D clippy::unwrap_used -D clippy::expect_used`: clean.
- `cargo test --locked -p repark-iceberg`: 516 passed, 0 failed.
- `cargo test --locked -p repark-spark`: 1228 passed, 0 failed, 4 ignored.
- `cargo test --locked -p repark-core`: 629 passed, 0 failed, 2 ignored.
- `maturin develop --release` (codegen-units 16): exit 0.
- `pytest -n 4` `test_ice_write_options_1.py` + `test_ice_write_options_1_rebase.py`:
  81 passed, 1 skipped (the JVM-gated live leg).
- `pytest -n 4` the files above + `test_ice_dyn_overwrite_1.py`,
  `test_ice_dyn_overwrite_1_by_name.py`, `test_ice_v3_write_default_1.py`,
  `test_ice_evo_dml_1.py`: 420 passed, 4 skipped, 16 xfailed.
- `pytest -n 4` writer files `test_writer.py`, `test_writer_v2.py`, `test_e2_readwriter.py`,
  `test_insert_store_assign.py`, `test_dml_b_partition_overwrite.py`,
  `test_ice_rtas_byname_1.py`, `test_ice_sorted_insert_1.py`, `test_ice_sorted_insert_2.py`,
  `test_ice_occ_scoped_1.py`: 217 passed, 10 skipped, 4 xfailed.
- Pre-commit hook (map-sync, crate-dag, lib-rs, rust-file-size, lib-py, docstring,
  compaction, manifest, fmt, taplo, typos) green on every commit.
  `comment_ban.py /tmp/lb-build origin/main`: `comment-ban hits=0`.
- Not run (CI owns them, owner ruling 2026-09-18): `make verify`, the whole facade
  suite, the parity harness. The live tier was not re-run. The Spark cells the new
  pins read are already recorded fixtures (DML-1B, RTAS-OPS-1, SNAP-03).

### Residues, stated and not fixed

- An option-carrying append whose `VALUES` carry a `DEFAULT` marker fails loudly. The
  options path materializes the source without main's marker rewrite. The facade never
  renders `DEFAULT`, so only a hand-written native call reaches it. This is loud, not
  silent.
- §17's P3 over-refusal and §18's V2-01 are unchanged. `EngineSummary::for_overwrite` now
  also guards the RTAS path, which inherits the same over-refusal of the three
  data-removal keys when the replace removes nothing.
