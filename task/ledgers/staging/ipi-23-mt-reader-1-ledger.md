# Charter ledger — IPI-23-MT-READER-1 · the DataFrame reader loads Iceberg metadata tables

**Date:** 2026-09-22 · **Branch:** `fix/ipi-23-reader-metadata-tables` · **Base:** `743f1be9` (`origin/main`) · **Model:** muse-spark-1.3-contributor · **Policy:** [../../../AGENTS.md](../../../AGENTS.md).
**Path:** STANDARD. **risk_tier: standard.** **Registry:** row `MT-1` in `docs/spark-sql-iceberg-parity.md` gains the served reader leg (rd-r3).

**Retires:** in flight.

**Scope:** `spark.read.format("iceberg").load("<cat>.<ns>.<t>.<meta>")` (and `.table(...)`)
answers what SQL `SELECT * FROM <cat>.<ns>.<t>.<meta>` answers, and with
`.option("versionAsOf", v)` / `.option("timestampAsOf", ts)` what SQL
`SELECT * FROM …​.<meta> VERSION AS OF v` / `TIMESTAMP AS OF ts` answers. New file
`crates/repark-core/src/time_travel/metadata_at.rs` (the ONE metadata AS OF decision:
`provider_for_spec` is the #802 `prepare_metadata_as_of` body moved down from
`repark-spark`; `read_metadata_path_at` routes four-part reader names past the
three-part loader; `read_sql` quotes the un-pinned read); one-line-each wiring in
`crates/repark-core/src/session.rs` (holds 1000 lines) and
`crates/repark-core/src/time_travel.rs`; net deletion in
`crates/repark-spark/src/time_travel.rs`; the Rust pin in
`crates/repark-spark/src/tests/metadata_tables_asof.rs`; the facade pins in
`python/repark/tests/test_ice_mt_reader_1.py`; the MT-1 reader paragraph; five `map.md`
files; and this ledger. The fork, every `Cargo.toml`, `Cargo.lock`, `STATUS.md`,
`crates/repark-spark/src/wap.rs` and `crates/repark-sql/**` are untouched; no code
comment added anywhere.

## Measurements (decide-then-build evidence)

**M-1 — the oracle is recorded, not re-derived.** The two replay cells were recorded
against live PySpark 4.1.2 + Iceberg 1.11.0 and carried in
`/tmp/xo-xo-opus55/sb-mt/out/spark-core.json`: `R-DF-LOAD-META`
(`load(t.snapshots).select("operation")` rows `append, append, overwrite`) and
`R-DF-LOAD-META-FILES-VERSIONASOF` (`versionAsOf` on `load(t.files)` rows `[2]`).
On RePark main the first fails with
`ParseException: SQL error: ParserError("Expected: end of statement, found: $snapshots …")`
and the second with
`AnalysisException: Error during planning: time travel requires a three-part
catalog.namespace.table identifier, got …​.files`.

**M-2 — why SQL answers and the reader does not.** Python `spark.sql` rewrites FROM
references through `_sql_table_ref`, so the engine sees the quoted
`SELECT * FROM "sc"."ns"."t"."snapshots"`; the metadata rewrite preserves the quoting
(`"t$snapshots"`) and the Databricks-dialect parse succeeds. The reader's internal
`self.sql("SELECT * FROM sc.ns.t.snapshots")` is unquoted, so the rewrite emits a bare
`t$snapshots` the Databricks tokenizer splits at `$`. The un-pinned reader arm now
quotes the same shape and rides the identical router path; probed on main before the
fix (`SELECT *` == `SELECT operation` == quoted form on `spark.sql`, reader red).

**M-3 — the red-first pins.** Six facade pins fail on main with the M-1 texts
(`C-001`/`C-002` the `$snapshots`/`$files` ParserError; `C-004`/`C-005`/`C-006`/`C-007`
the three-part error); the Rust pin fails with
`Plan("time travel requires a three-part … got ice.sales.m.files")`. The near-miss
pins (`C-003`, `C-008`–`C-012`) pass on main and after. The unknown-suffix text pinned
is the reader's own (`'…​.nope'. Expected 1, 2 or 3 parts, got 4`), which differs from
the SQL door's backticked spelling by construction (unquoted internal SQL, unchanged).

## PROPOSITION LEDGER — IPI-23-MT-READER-1 — 2026-09-22

| Clause | Proposition (checkable) | Proof obligation | Verdict | Evidence / open question |
|---|---|---|---|---|
| C-001 | `load("mt.ns.t.snapshots")` answers the same rows and schema field names as `SELECT * FROM mt.ns.t.snapshots`, and the operations read `append, append, overwrite`. | `test_load_snapshots_equals_sql` green. | **PROVEN** | Recorded cell `R-DF-LOAD-META`: reader/SQL equality on rows and columns plus the absolute operation list. pins: ipi-23-mt-reader-1/C-001 |
| C-002 | `load("mt.ns.t.<meta>")` equals the SQL door (rows and columns) for `files`, `history` and `refs`. | `test_load_files_history_refs_equal_sql` green. | **PROVEN** | Same seed; per-suffix reader/SQL equality. pins: ipi-23-mt-reader-1/C-002 |
| C-003 | `read.table("mt.ns.t.snapshots")` equals the SQL door (rows and columns). | `test_table_api_snapshots_equals_sql` green. | **PROVEN** | Already green on main via `session.table`; pinned against regressions. pins: ipi-23-mt-reader-1/C-003 |
| C-004 | `.option("versionAsOf", <first id>).load("mt.ns.t.<meta>")` equals SQL `VERSION AS OF <first id>` (rows and columns) for `files`, `snapshots`, `entries` and `manifests`. | `test_load_version_as_of_equals_sql` green. | **PROVEN** | Recorded cell `R-DF-LOAD-META-FILES-VERSIONASOF` plus the sibling scoped/current types. pins: ipi-23-mt-reader-1/C-004 |
| C-005 | `.option("timestampAsOf", <ts>).load("mt.ns.t.files")` equals SQL `TIMESTAMP AS OF '<ts>'` (rows and columns). | `test_load_timestamp_as_of_equals_sql` green. | **PROVEN** | Same commit instant on both doors; reader/SQL equality. pins: ipi-23-mt-reader-1/C-005 |
| C-006 | Reader AS OF refusals carry the SQL door's class and exact text: `versionAsOf 'nope'` on `.files` → `IllegalArgumentException("Cannot find matching snapshot ID or reference name for version nope")`; `timestampAsOf` before the first snapshot → `IllegalArgumentException("Cannot find a snapshot older than 2000-01-01T00:00:00+00:00")`; `versionAsOf` on `.all_files` → the SQL door's `AnalysisException` text containing `Cannot select snapshot in table: ALL_FILES`. | `test_reader_as_of_refusals_match_sql` green. | **PROVEN** | Each reader refusal asserted equal to the SQL door's class and text; the #802 sentences verbatim. pins: ipi-23-mt-reader-1/C-006 |
| C-007 | Unknown numeric `versionAsOf` on `.files` answers empty with the SQL door's schema field names. | `test_reader_unknown_numeric_as_of_answers_empty` green. | **PROVEN** | `try_new_empty` through the reader; zero rows, equal columns. pins: ipi-23-mt-reader-1/C-007 |
| C-008 | Near miss: `load("mt.ns.t")` with and without `versionAsOf` keeps its rows (`[[2,"b","y"],[3,"c","x"]]` current, `[[1,"a","x"],[2,"b","y"]]` at the first snapshot). | `test_load_plain_and_version_as_of_unchanged` green. | **PROVEN** | Absolute rows plus reader/SQL equality; green on main and after. pins: ipi-23-mt-reader-1/C-008 |
| C-009 | Near miss: `load("mt.ns.t.branch_b0")`, `.tag_t0` and `.snapshot_id_<id>` equal the SQL door as they do today. | `test_load_branch_tag_snapshot_id_selectors_unchanged` green. | **PROVEN** | Selector routing untouched; reader/SQL equality per suffix. pins: ipi-23-mt-reader-1/C-009 |
| C-010 | Near miss: a real table `mt.ns.snapshots` reads its own rows (`[[1],[2]]`) under `load`. | `test_load_table_named_snapshots_reads_real_table` green. | **PROVEN** | Three-part names never route to metadata. pins: ipi-23-mt-reader-1/C-010 |
| C-011 | Near miss: `load("mt.ns.t.nope")` keeps today's `AnalysisException` text (`Unsupported compound identifier 'mt.ns.t_load_nope.nope'. Expected 1, 2 or 3 parts, got 4`). | `test_load_unknown_suffix_keeps_error_text` green. | **PROVEN** | The reader's own literal, measured on main (M-3). pins: ipi-23-mt-reader-1/C-011 |
| C-012 | Near miss: the legacy `snapshot-id` / `as-of-timestamp` / `tag` options on `load("mt.ns.t.files")` refuse with the #800 `IllegalArgumentException` texts. | `test_legacy_options_on_metadata_keep_refusal_texts` green. | **PROVEN** | Python-layer refusals fire before any engine routing. pins: ipi-23-mt-reader-1/C-012 |
| C-013 | Live leg: on Spark 4.1.2 itself the reader answers what SQL answers for `load(t.snapshots)` and `versionAsOf` on `load(t.files)`. | `test_live_spark_reader_matches_sql` green under `REPARK_PARITY_LIVE=1`. | **PROVEN** | Goal premise re-measured on the live oracle in the gate's live leg. pins: ipi-23-mt-reader-1/C-013 |

## Gates

| Command | Result |
|---|---|
| `build-slot.sh local-gate.sh xo55-rd "repark-core:--lib+time_travel,…" <6 test files>` | `CB=0 R=0 T=0 U=0 L=0` — rust 13+29+22 passed; unit 264 passed 2 skipped; live 266 passed |
| `make rust-clippy` | exit 0 (workspace, `-D warnings`) |
| `make rust-panic-ban` | exit 0 |
| `make check-rust-file-size` | exit 0 (808 files clean; `session.rs` holds 1000) |
| `bash scripts/check_map_md.sh --base origin/main` | exit 0 at the final head |
| `python3 /tmp/oc-worker/_lib/comment_ban.py /tmp/xo55-rd origin/main HEAD` | exit 0 (`hits=0`) after every commit |
| `cargo fmt --check` + `uvx ruff@0.15.22 check/format` on touched files | exit 0 |
| sb-mt replay `--only R-DF-LOAD-META,R-DF-LOAD-META-FILES-VERSIONASOF` | both cells equal `spark-core.json` rows and columns |
| `python3 scripts/check_ledger_grammar.py` | exit 0 |

## COVERAGE_ATTESTATION

```
COVERAGE_ATTESTATION:
  pr_unit: ipi-23-mt-reader-1
  complete: true
  categories:
    - id: AT-1
      status: ATTACKED
      evidence: Each answering clause walks the reader to the SQL door on the same table — rows plus schema field names on snapshots/files/history/refs (C-001/C-002), the table API (C-003), versionAsOf on four types (C-004), timestampAsOf (C-005), and the two recorded Spark cells replayed equal (rows append/append/overwrite, record_count [2]).
      artifacts: [python/repark/tests/test_ice_mt_reader_1.py]
    - id: AT-2
      status: ATTACKED
      evidence: The seed exercises a two-append history with a delete so pins sit mid-history; the Rust pin drives the moved provider_for_spec through read_table_at and the SQL door for one scoped case (one row, rendered batches equal) and one empty case (zero rows, schema names equal); refusals pin class plus full text on both doors.
      artifacts: [python/repark/tests/test_ice_mt_reader_1.py, crates/repark-spark/src/tests/metadata_tables_asof.rs]
    - id: AT-3
      status: ATTACKED
      evidence: The 'nope'/too-old/all_* refusals assert the reader's class and text equal the SQL door's, with the #802 sentences verbatim; legacy options keep the #800 IllegalArgumentException texts; the unknown suffix keeps the reader's measured Analysis text.
      artifacts: [python/repark/tests/test_ice_mt_reader_1.py]
    - id: AT-4
      status: ATTACKED
      evidence: Resolution runs per query through Session state; the metadata provider registers one temp view per read with no shared mutable state or global registration, mirroring the existing read_table_at tail.
      artifacts: [crates/repark-core/src/time_travel/metadata_at.rs]
    - id: AT-5
      status: N/A
      justification: Read-only metadata scans over the session's own catalog; no auth, secret, or injection surface added — the quoted internal SQL only reorders identifier segments the parser already accepted.
    - id: AT-6
      status: ATTACKED
      evidence: Registry row MT-1 gains the served reader leg; every previously green behavior in scope (plain loads, selectors, real metadata-named tables, unknown suffix, legacy refusals) is pinned unchanged, and no existing test's expected value moved.
      artifacts: [docs/spark-sql-iceberg-parity.md, python/repark/tests/test_ice_mt_reader_1.py]
    - id: AT-7
      status: N/A
      justification: The scan streams through the existing provider and temp-view tail with two catalog existence probes per pinned read; no added materialization, unbounded growth, or hot-loop pattern.
    - id: AT-8
      status: ATTACKED
      evidence: The moved decision calls the same fork inspect constructors at the pinned rev through the same repark-iceberg provider the SQL door uses; no dependency or pin move, no Cargo.toml change.
      artifacts: [crates/repark-core/src/time_travel/metadata_at.rs, crates/repark-spark/src/time_travel.rs]
    - id: AT-9
      status: ATTACKED
      evidence: Reader errors are the SQL door's errors verbatim (same functions, same texts); a mis-scope surfaces the router's or resolver's loud refusal, never a silent wrong table — real-table-wins and missing-parent fall through to today's texts.
      artifacts: [crates/repark-core/src/time_travel/metadata_at.rs, python/repark/tests/test_ice_mt_reader_1.py]
    - id: AT-10
      status: ATTACKED
      evidence: Two recorded cells replayed verbatim plus thirteen facade pins and one Rust pin, every clause carrying a pins: citation in the test file and the touched map.md rows; C-001 asserts schema field names alongside values.
      artifacts: [python/repark/tests/test_ice_mt_reader_1.py, python/repark/tests/map.md, crates/repark-core/src/time_travel/map.md]
```

Every clause above is PROVEN against the SQL door on the same table, and the two
replay cells against recorded PySpark 4.1.2 rows; no clause is OPEN. Touched files
per the Scope paragraph; the fork, `Cargo.toml`/`Cargo.lock`, `STATUS.md`, `wap.rs`
and the ANSI door are untouched. `make verify` and the full facade suite were not
run per the work order's gate list; the gates table above is the proof.
