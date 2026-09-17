# Charter ledger — ICE-WRITE-OPTIONS-1 · DataFrame write options on Iceberg writes

**Date:** 2026-09-17 · **Branch:** `feat/ice-write-options-1` · **Base:** `origin/main`
`79e328f2` · **Model:** muse-spark-1.3-contributor · **Policy:** [../../../AGENTS.md](../../../AGENTS.md).
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
