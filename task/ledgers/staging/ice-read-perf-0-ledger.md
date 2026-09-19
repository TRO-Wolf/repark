# Unit ledger — ICE-READ-PERF-0 · Iceberg I/O counting layer and the read bench bed

**Date:** 2026-09-19 · **Branch:** `perf/ice-read-perf-0` · **Base:** `b0fb6feb` (`main`)
**Model:** claude-opus-5 (round 1) · **Policy:** [../../../AGENTS.md](../../../AGENTS.md).
**Path:** STANDARD. **risk_tier: standard.**

**Retires:** this ledger moves to `../completed/` when the unit's last commit lands.

**Why now.** Unit 0 of the v1.5.0 read-performance slate
([../../roadmap/mid-term/ice-read-perf-slate-2026-09-18.md](../../roadmap/mid-term/ice-read-perf-slate-2026-09-18.md)):
every later unit is ranked by a number from this bed, not by the source review. Owner ruling
R-3 (2026-09-18) makes the 3 GB table-size flag a hard clause of this unit.

**What it is.** No product behaviour changes. Two deliverables, Rust only: a counting
`StorageFactory` wrapper in repark-iceberg whose counters the session owns, and a
harness-free bench in repark-spark (`benches/ice_read_perf/`). No `unsafe`, no new package
(`bytes` and `serde` were already in the lockfile and are now declared by repark-iceberg; the
fork's `typetag` storage traits need both), no Cargo feature.

**Not in this unit:** `STATUS.md`; the metadata and manifest caches for Glue and S3 Tables
(ICE-CATALOG-CACHE-1); the AWS leg of `aws-acceptance.yml` and its Slack note (a later PR);
the 200-file baseline numbers (the orchestrator records them in
[../../../docs/perf/ice-read-perf-baseline-2026-09-19.md](../../../docs/perf/ice-read-perf-baseline-2026-09-19.md)).

## PROPOSITION LEDGER — ICE-READ-PERF-0 — 2026-09-19

| Clause | Proposition (checkable) | Proof obligation | Verdict | Evidence / open question |
|---|---|---|---|---|
| C-001 | The counting storage delegates every `Storage` call and counts one request per call with the bytes it returned or sent, per op kind (exists, metadata, read, ranged read, footer read, write, delete, list); a ranged read through `reader()` counts the RETURNED length, never the requested span; `InputFile` / `OutputFile` / `writer()` obtained through the wrapper still count; the file class is computed once per opened reader / writer. | `catalog::tests::io_stats` op, range and wrapper pins; `a_ranged_read_counts_the_returned_length_not_the_requested_span` (a stub `FileRead` whose `read(0..1000)` returns 10 bytes counts 1 / 10). | PROVEN | `every_op_kind_counts_once_with_its_bytes`, `ranged_reads_count_the_range_length`, `input_and_output_files_from_the_wrapper_still_count` green; the mutation `new_input`/`new_output` over the inner storage reds five pins (recorded 2026-09-19). Round 2 (F-MUT-5A): the mutation "count `range.end − range.start`" reds the stub pin; round 1's pins could not see it (local in-bounds reads return the requested span). |
| C-002 | The path classifier names table metadata JSON, manifest list, manifest, data file, delete file, and `other`, on literal paths and on every file a RePark INSERT writes, and on the names RePark's `pos-del` and the fork's `dv` generators produce. | `the_classifier_reads_literal_iceberg_paths`, `the_classifier_reads_the_paths_repark_writes`. | PROVEN | Both green; no written file classifies as `other`. |
| C-003 | The counters are lock-free atomics in one `Arc` per session, owned by `CatalogCaches`, cumulative, with a snapshot and a reset; `ReparkSession::iceberg_io_stats()` / `reset_iceberg_io_stats()` expose them, and configured Glue / S3 Tables catalogs count into the same set. | `caches_share_one_counter_set_across_clones`; repark-core `session::tests::io_stats`. | PROVEN | Both green; `register_catalog_spec` passes `iceberg_io_counters()` to the counted builders. |
| C-004 | The Glue and S3 Tables catalogs receive `with_storage_factory(<counting wrapper over exactly the fork default>)`: OpenDAL S3 with `configured_scheme` `s3a` (Glue) and `s3` (S3 Tables), no custom credential loader, so their storage behaviour is byte-identical to the fork's default; neither receives the metadata or manifest cache. | `glue_and_s3tables_factories_are_the_fork_defaults`, `the_counting_factory_serializes_over_its_inner_factory`, `glue_and_s3tables_counted_builders_keep_their_prop_errors`; diff read of `builders.rs`. | PROVEN | Pins green offline; the defaults restate fork `43fcd243` `catalog/glue/src/catalog.rs:253` and `catalog/s3tables/src/catalog.rs:235` (re-read after the round-2 rebase onto the RP-34 pin; unchanged in value). No AWS call was made (unit rule). |
| C-005 | A scan of a small local table reads data-file ranges through the counter, and a second `load_table` with the metadata cache on reads fewer metadata JSON documents than with it off. | `a_scan_reads_data_file_ranges_through_the_counter`, `the_metadata_cache_cuts_metadata_json_reads_on_a_second_load`. | PROVEN | Both green. |
| C-006 | The bench drives `ReparkSession` + `SparkExtension` + `SparkDialect`: `setup` writes exactly N data files deterministically and a bed manifest; `run --mode cold|warm|concurrent` measures Q1–Q6 (planning, first batch, total, rows, requests and bytes by op kind and file class, metadata-cache deltas, peak RSS) and writes one JSON document with the H-3 environment header plus a markdown table; `--catalog glue|s3tables` compile and fail loud on missing props. | `ice_read_perf_pins` (`setup_writes_exactly_n_files_and_every_mode_runs_on_them`, the parser pin, `aws_catalogs_fail_loud_on_missing_props_without_a_call`); the step-3 release smoke. | PROVEN | Pins green; smoke on 20 files × 50,000 rows (142,896,230 B) ran all three modes on the default release profile (tables in the hand-back, not in the repo). |
| C-007 | R-3: the limit is the constant 3 × 1024³ = 3221225472, never an option or an override; a table above it (not at it) raises the flag, which prints `R3-SIZE-FLAG table=… bytes=… limit=3221225472`, appends it to `$GITHUB_STEP_SUMMARY` when set, and exits 3 (distinct from 0, 1, 2); every `run` checks before its first read and `setup` checks at its end. | `the_r3_limit_is_three_gibibytes_and_flags_only_above_it` (limit − 1, limit, limit + 1), `the_r3_exit_code_is_distinct`, `a_flag_appends_one_line_to_the_step_summary_and_a_pass_appends_none`, `the_limit_is_not_a_cli_option`. | PROVEN | All green. |
| C-008 | With an injected size above the limit, the bench's pre-scan path on a real local table stops with the flag and the counting layer shows zero data-file and zero delete-file requests. | `a_fake_size_above_the_limit_stops_the_run_before_any_data_file_read`. | PROVEN | Green; the gate session's counters show manifest reads (the counter was live) and no data-file read; the same path at exactly the limit runs Q2. |
| C-009 | RePark's `files` metadata table lists delete files, so the R-3 sum covers data and delete files. | `the_files_table_counts_delete_files_in_the_footprint`. | PROVEN | A merge-on-read DELETE leaves 2 files, 1 delete file; the byte sum equals the two files on disk; no data-file read. |
| C-010 | No product behaviour changes: the wrapper only delegates and counts, the memory catalog keeps its scheme-selected factory underneath, and the existing catalog and session suites stay green. | `cargo test -p repark-iceberg --lib catalog`, `cargo test -p repark-core --lib session`. | PROVEN | See §Gates. |
| C-011 | A ranged read on a data or delete file whose returned bytes end with the Parquet tail magic `PAR1` or the Puffin tail magic `PFA1` counts as `footer_read`; every other ranged read counts as `ranged_read` (the page bucket on data and delete files); reads on any other class never count as `footer_read`. | `a_ranged_read_ending_on_the_tail_magic_of_a_data_or_delete_file_is_a_footer_read`; `a_scan_reads_data_file_ranges_through_the_counter` (one footer read for the one data file, page reads beside it). | PROVEN | Both green; the mutation "every ranged read is `ranged_read`" reds both (2026-09-19, round 2). |
| C-012 | Every counter cell is 64-byte aligned, so no two cells share a cache line; the per-read path neither classifies a path nor allocates. | `every_counter_cell_owns_its_cache_line` (`align_of` = 64, `size_of` = cells × 64); diff read of `CountingFileRead` / `CountingFileWrite` (they hold an `IcebergFileClass`, not a path). | PROVEN | Green; dropping `#[repr(align(64))]` reds the pin (2026-09-19, round 2). |
| C-013 | Q3 stays as users write it (`CAST(<seconds> AS TIMESTAMP)` window) and its Iceberg scan predicate is empty today; Q7 asks for the same rows as an `id` window, and its scan predicate is that `id` range; every run records each query's scan predicate in the JSON. | `q3_never_reaches_the_scan_today_and_q7_does_with_the_same_rows` (three-file bed: Q3 `""`, Q7 `(id >= 540) AND (id < 552)`, identical 12 ids; the plan-line parser). | PROVEN | Green. Seven timestamp spellings were probed by EXPLAIN (2026-09-19); none reaches the scan (map.md lists them). The Q3 half is a sentinel for F-TS-PUSHDOWN-1. |
| C-014 | On the bed, Q1 reads only data-file footers (one per data file, no page read) and Q2 reads one footer per data file plus page reads, in both cold and warm. | `setup_writes_exactly_n_files_and_every_mode_runs_on_them` (the `data_file_ranged` assertions). | PROVEN | Green on the three-file bed; the 20-file smoke is in the hand-back. |
| C-015 | `run` has four modes: cold (fresh session per sample, same process), warm, concurrent (four warmed queries joined on one task), and concurrent-cold (a fresh session per round, no warm-up, four at once). `--repeat N` measures every query or round N times; the JSON keeps every sample, gives timing and RSS medians, and flags (never averages) any sample whose I/O or rows differ from sample 1's. The environment header carries `cold_kind`, `concurrent_kind`, `os_page_cache: "not_dropped"`, `rustc_version`. Each sample carries `execute_to_first_batch_ms` and `rss_at_reset_kib`, and the run carries `run_io_total`. | `setup_writes_exactly_n_files_and_every_mode_runs_on_them` (four modes × `--repeat 2`; the concurrent-cold group reads manifests, the warm concurrent group none); `the_parser_reads_the_repeat_and_remote_setup_flags`. | PROVEN | Green; adding a warm-up to concurrent-cold reds the mode pin (2026-09-19). |
| C-016 | `run --catalog glue|s3tables` needs no local manifest: its query constants derive from `--files` / `--rows-per-file` and equal those a local manifest of the same counts records; every run refuses a table whose data files, rows or delete files differ from the expected bed. | `the_counts_derive_the_local_manifests_query_constants`; `aws_catalogs_fail_loud_on_missing_props_without_a_call` (`--manifest` on AWS is refused). | PROVEN | Green. |
| C-017 | `setup --catalog glue|s3tables --phase create` is idempotent. It creates the namespace by the acceptance module's rule (Glue: location `<warehouse>/<ns>`, verified after the create; S3 Tables: no location) and then the empty bed table, or it reports that a table with the bed schema exists. `--phase write` writes the N files only into a table with no file, skips at exactly N data files with N × rows rows and no delete file, and in any other state fails loud before writing. Both phases end with the R-3 check. Every failure path is pinned offline. | `the_setup_phases_write_only_into_an_empty_table_and_skip_only_an_exact_one`, `the_create_phase_refuses_a_glue_namespace_at_another_location`, `both_setup_phases_end_with_the_r3_check`, `aws_catalogs_fail_loud_on_missing_props_without_a_call` (memory-catalog stand-in, no AWS call). | PROVEN | Green; the mutation "write whatever the state" reds the phase pin (2026-09-19). |

## Measured observations (not clauses — inputs to the slate's ranking)

From the step-3 smoke (20 files, release, a loaded box: load average about 21):

- **C-9 (count folding).** `SELECT count(*)` reads data files today: one ranged read of
  512 KiB per data file (20 requests, 10,485,760 B, warm and cold), i.e. the footer size hint,
  and decodes no column page. It does not fold to manifest metadata alone.
- **The 1% `ts` window prunes nothing.** `EXPLAIN` shows `IcebergTableScan … predicate:[]`
  for `ts >= CAST(<n> AS TIMESTAMP) AND ts < …` — the `TimestampMicrosecond(…, "UTC")`
  literal does not convert to an Iceberg predicate, so Q3 reads the same 60 requests /
  27.5 MB as the unprunable Q5. The manifests do carry `ts` bounds. ICE-PAGE-PRUNE-1 measures
  "selective timestamp filters", so this must be fixed or the query re-spelled first.
- **The `id` point lookup reaches the scan** (`predicate:[id = 500017]`) and reads 28 requests
  / 14.6 MB against Q2's 60 / 23.2 MB.
- **Bare Rust Spark door:** `SELECT … FROM cat.ns.tbl.files` fails with `Expected: end of
  statement, found: $files`; the metadata-table rewrite emits an unquoted `tbl$files` that the
  Databricks tokenizer splits. The Python facade quotes names first and is not affected. The
  bench spells the name with backticks. A product defect outside this unit.
- **Cold mode:** `register_table` fills the metadata cache, so a cold query itself reads no
  metadata JSON; the registration's one read is in `register_io`.

## Gates

Round 1, on `344ade2a`, `CARGO_BUILD_JOBS=6 RUST_TEST_THREADS=6` through the build slot:

- `comment_ban.py /tmp/pa-build origin/main` — `comment-ban hits=0`.
- `cargo test -p repark-iceberg --lib catalog` — 73 passed (the 11 `catalog::tests::io_stats`
  pins among them).
- `cargo test -p repark-core --lib session` — 163 passed (`session::tests::io_stats` among them).
- `cargo test -p repark-spark --test ice_read_perf_pins` — 9 passed.
- `cargo clippy -p repark-iceberg -p repark-core -p repark-spark --all-targets -- -D warnings
  -A clippy::disallowed_methods` (the `make rust-clippy` form) — clean. Without the `-A`, the
  command reds only on `disallowed_methods` in test code (unwrap / expect), pre-existing across
  the three crates and followed by the new test files; `make rust-clippy` allows it on purpose.
- `cargo clippy … --lib --bins -- -D clippy::disallowed_methods -D clippy::unwrap_used
  -D clippy::expect_used -D clippy::panic -D clippy::todo -D clippy::unimplemented
  -D clippy::unreachable` (the `make rust-panic-ban` form, three crates) — clean.
- `cargo fmt --all -- --check` — clean.
- `make check-map-sync check-rust-file-size check-ledgers check-ledger-grammar
  check-docs-links` — all clean.
- Release smoke (`cargo bench -p repark-spark --bench ice_read_perf`, default release profile):
  `setup --files 20 --rows-per-file 50000` (11.6 s, 142,896,230 B, R-3 pass) and one `run` per
  mode — exit 0 each; numbers in the hand-back only.

## Coverage attestation

```yaml
COVERAGE_ATTESTATION:
  pr_unit: ice-read-perf-0
  categories:
    - id: AT-1
      status: N/A
      justification: No Spark-parity claim. The unit counts I/O and measures; it asserts no Spark answer.
    - id: AT-2
      status: ATTACKED
      evidence: The InputFile/OutputFile escape was mutated in (new_input and
        new_output over the inner storage) and five counting pins went red. The
        R-3 decision is pinned on both sides of the limit and at it.
      artifacts: [crates/repark-iceberg/src/catalog/tests/io_stats.rs, crates/repark-spark/benches/ice_read_perf/pins.rs]
    - id: AT-3
      status: ATTACKED
      evidence: Every op kind and every file class is pinned. The R-3 path is pinned
        flagged, at the limit and within it, with a delete file present, and
        through the real pre-scan path on a real table.
      artifacts: [crates/repark-iceberg/src/catalog/tests/io_stats.rs, crates/repark-spark/benches/ice_read_perf/pins.rs]
    - id: AT-4
      status: ATTACKED
      evidence: The counters are relaxed AtomicU64 in one Arc. A snapshot taken during
        a reset is not atomic across cells; the bench resets only between queries.
        The concurrent mode joins futures on one task, so the tokio::spawn ban holds.
      artifacts: [crates/repark-iceberg/src/catalog/io_stats.rs, crates/repark-spark/benches/ice_read_perf/run.rs]
    - id: AT-5
      status: ATTACKED
      evidence: The counting storage never logs paths or props. The Glue and S3 Tables
        spans still record property key names only. The bench reads no secret and
        passes --prop pairs to the catalog builder unchanged. No unsafe code.
      artifacts: [crates/repark-iceberg/src/catalog/builders.rs, crates/repark-iceberg/src/catalog/counting_storage.rs]
    - id: AT-6
      status: ATTACKED
      evidence: A failed request still counts one request with zero bytes. The
        classifier falls back to other rather than guessing. The bench fails loud
        on a missing manifest, table, prop, or a bed that is not exactly N data files.
      artifacts: [crates/repark-iceberg/src/catalog/counting_storage.rs, crates/repark-spark/benches/ice_read_perf/bed.rs]
    - id: AT-7
      status: ATTACKED
      evidence: The unit measures but claims no speed-up. Its smoke numbers stay out of
        the repo; the baseline file carries only the H-3 header skeleton for the
        orchestrator's 200-file run.
      artifacts: [docs/perf/ice-read-perf-baseline-2026-09-19.md]
    - id: AT-8
      status: ATTACKED
      evidence: No STATUS.md edit, no new package in Cargo.lock (two dependency edges
        onto crates already locked), no feature, no crate-DAG edge. All files stay
        under the 1000-line ceiling; session.rs is 982 lines.
      artifacts: [Cargo.toml, crates/repark-iceberg/Cargo.toml, crates/repark-spark/Cargo.toml]
    - id: AT-9
      status: ATTACKED
      evidence: Every touched directory's map.md carries the change with pins; the
        bench map records the layout, queries, modes, metrics, exit codes and the R-3
        contract; docs/perf/map.md reserves the baseline row.
      artifacts: [crates/repark-spark/benches/ice_read_perf/map.md, crates/repark-iceberg/src/catalog/map.md, docs/perf/map.md]
    - id: AT-10
      status: ATTACKED
      evidence: The existing repark-iceberg catalog and repark-core session suites run
        green with the wrapper in the memory catalog path, and clippy is clean on
        all targets of the three crates.
      artifacts: [crates/repark-iceberg/src/catalog/tests/catalog.rs]
  complete: true
```
