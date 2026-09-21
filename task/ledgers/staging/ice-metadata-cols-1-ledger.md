# Charter ledger — ICE-METADATA-COLS-1 · IPI-20 PR-1, `_file` and `_pos` on the Spark door

**Date:** 2026-09-20 · **Branch:** `fix/ice-metadata-cols-1` · **Base:** `ed15699b` (`origin/main`) · **Model:** muse-spark-1.3-contributor · **Policy:** [../../../AGENTS.md](../../../AGENTS.md).
**Path:** STANDARD. **risk_tier: standard.** **Registry:** row `ICE-MC-FILEPOS-1` filed BACKLOG in `docs/spark-sql-iceberg-parity.md` §7 this unit.

**Retires:** ready for the departure move to `../completed/`.

**Scope:** PR-1 narrows the five-column draft to the two columns the fork pin
serves: `crates/repark-iceberg/src/catalog/metadata_columns.rs` (the served set
is the hard-coded `METADATA_COLUMN_NAMES = [_file, _pos]`; `_spec_id`,
`_partition`, `_deleted` move to the hard-coded `UNSERVED_METADATA_COLUMN_NAMES`),
`crates/repark-core/src/metadata_columns.rs` (the Spark-door rewrite plus the
typed `[ICE-MC-1]` refusal of the three unserved names), the router hook and the
`repark-core` re-export, `crates/repark-spark/src/tests/metadata_columns.rs`,
`python/repark/tests/test_ice_metadata_cols_1.py`, the registry row, six
`map.md` files, the `repark-core` lib.rs `154` EXCEPTIONS row (sanctioned out
(2): the root sat exactly at the 150 default), and this ledger. No code comment
added anywhere (`comment-ban hits=0`); Python docstrings only where the presence
gate needs them. The fork, every `Cargo.toml`, `Cargo.lock`, the pin,
`STATUS.md`, `metadata_tables.rs`, `time_travel.rs`, `describe_show.rs`, the
lineage path and the ANSI door are untouched.

## Measurements (decide-then-build evidence)

**M-1 — the tree pins fork `886b94c1`, and every measurement below was taken
there.** `Cargo.toml:162-166` (all five `[patch.crates-io]` `iceberg*` entries)
and `Cargo.lock` agree on `886b94c1bbc68f43b791221761b8360dec4767d0`, matching
`origin/main`; `.cargo/config.toml` carries no `[patch]` override. Pin truth
comes from two independent legs: reading the pinned source in the cargo git
cache, and running the suite against the pinned tree. Values below are
pin-measured unless noted. (r2: supersedes the `3ed905c6` claim.)

**M-2 — R-MC-POS-MOR at the pin is `[[2,0],[3,0],[4,1]]`, Spark-equal, no
divergence.** Pin run: `pos_is_the_file_position_after_a_merge_on_read_delete`
green. The survivor keeps its file ordinal; `_pos` rides the fork's
`RowPosition` at the pin (`record_batch_transformer.rs` in the pinned source).
Not a residue.

**M-3 — the brief's ruling holds verbatim at the pin.** Pinned source:
`MetadataScope` is `CurrentSnapshot | AllSnapshots` only
(`inspect/manifest_source.rs`); `include_deleted` appears nowhere;
`unified_partition_type` exists (`spec/partitioning.rs:176`), which is why the
five-column draft compiled. Pin run: the draft's `_deleted`, `_partition`,
`_partition`-union and `_spec_id` assertions all fail at the pin while `_file`
(`R-MC-FILE` legs) and `_pos` pass. Narrowing to two is correct at the pin.

**M-4 — a bare `count(*)` plans an empty projection, and the pin serves it.**
The draft refused `Some([])` with `metadata-column scan needs at least one
projected column`, which reddened `R-MC-FILE-FILTER` at both forks. The pin's
own fork test asserts `select_empty()` returns 0 columns with the full row
count, so the layer now passes the empty select through and rebuilds a
0-column batch with the scanned row count (`RecordBatchOptions::with_row_count`).
Filters stay `Inexact`, so a pushed-down predicate still applies above the scan
and the fix cannot silently over-count.

**M-5 — user columns named `pos` / `file_path` are unreadable at the pin, so
the A-6 test is deferred, not kept.** The pin's `TableScanBuilder::build`
checks the fork's `is_metadata_column_name` BEFORE the table schema
(`scan/mod.rs`), and the delete-file vocabulary contains `pos` and
`file_path` — so any select naming them (even `SELECT *`, even
`select_all`, which replays the schema's own names through the same check)
resolves to the delete-file reserved field ids and dies with `field not found`
in the transformer. Measured on the base build (no lane code) and at the pin:
`SELECT pos`, `SELECT file_path` and `SELECT *` over such tables all fail
identically. No RePark-side select spelling can reach those columns — the fork
offers no id-based select — so serving them needs a fork fix (schema-first
name resolution). The draft's `plain_columns_...` test waits on that fix as the
fifth `deferred_to_pr2` entry; the layer itself never claims those names (its
served set is the hard-coded two).

**M-6 — the ANSI door is unwired.** `crates/repark-sql/src/router.rs` calls
`prepare_lineage_sql` but has no metadata-columns hook; per the brief it stays
that way. Residue, verbatim: "the ANSI door does not serve metadata columns in
this unit".

**M-7 — name folding on the Spark door.** Unquoted `_POS` folds to `_pos`;
backtick `` `_pos` `` resolves exact; double-quoted `"_pos"` is a string
literal (Spark semantics, base path, untouched by the layer). All three
behaviors are pinned or probed; the compound `x._pos` through an alias resolves
through the temp view.

## PROPOSITION LEDGER — ICE-METADATA-COLS-1 — 2026-09-20

| Clause | Proposition (checkable) | Proof obligation | Verdict | Evidence / open question |
|---|---|---|---|---|
| C-001 | `SELECT id, _file LIKE '%.parquet', _file LIKE '%/data/%' FROM t` answers `[[2,true,true],[3,true,true],[4,true,true]]` with `_file: string`. | `file_and_pos_answer_spark` (R-MC-FILE leg) and `test_file_predicates_answer_spark` green. | **PROVEN** | Exact recorded `R-MC-FILE` values, Arrow `utf8` via downcast and `df.schema` `string`. pins: ice-metadata-cols-1/C-001 |
| C-002 | `SELECT count(DISTINCT _file) FROM t` answers `[[3]]`. | `file_and_pos_answer_spark` (R-MC-FILE-DISTINCT leg) and `test_file_distinct_counts_live_files` green. | **PROVEN** | Exact recorded `R-MC-FILE-DISTINCT` value: three live files after the CoW rewrite. pins: ice-metadata-cols-1/C-002 |
| C-003 | `SELECT count(*) FROM t WHERE _file IS NOT NULL` answers `[[3]]`. | `file_and_pos_answer_spark` (R-MC-FILE-FILTER leg) and `test_file_filter_counts_non_null` green. | **PROVEN** | Exact recorded `R-MC-FILE-FILTER` value through the M-4 empty-projection path. pins: ice-metadata-cols-1/C-003 |
| C-004 | `SELECT id, _pos FROM t` answers `[[2,0],[3,0],[4,0]]` with `_pos: bigint`. | `file_and_pos_answer_spark` (R-MC-POS leg) and `test_pos_is_zero_based_file_ordinal` green. | **PROVEN** | Exact recorded `R-MC-POS` values, Arrow `int64` via downcast and `df.schema` `bigint`. pins: ice-metadata-cols-1/C-004 |
| C-005 | `SELECT id, _pos FROM t` after a merge-on-read DELETE answers `[[2,0],[3,0],[4,1]]`. | `pos_is_the_file_position_after_a_merge_on_read_delete` and `test_pos_survives_merge_on_read_delete` green. | **PROVEN** | Exact recorded `R-MC-POS-MOR` values, pin-measured per M-2; no divergence. pins: ice-metadata-cols-1/C-005 |
| C-006 | `SELECT *` answers user columns only; `*, _file` and `*, _pos` compose. | `select_star_excludes_every_served_metadata_column` and `test_star_excludes_served_metadata_columns` green. | **PROVEN** | Field names `[id, data, cat]` / `+ [_file]` / `+ [_pos]` on both doors of the pin. pins: ice-metadata-cols-1/C-006 |
| C-007 | `_spec_id`, `_partition`, `_deleted` refuse typed `[ICE-MC-1]`, never raw; served names fold; composed shapes refuse. | `unserved_metadata_columns_refuse_with_a_typed_error`, `served_names_fold_and_composed_shapes_refuse`, `test_unserved_metadata_columns_refuse_typed` green. | **PROVEN** | Each unserved name (bare and backtick) raises `[ICE-MC-1]` naming it, with no `No field named` leak; `_POS` folds, `` `_pos` `` and `x._pos` resolve, and a `DELETE` naming `_file` plus a two-relation `*` refuse `[ICE-MC-1]`. pins: ice-metadata-cols-1/C-007 |
| C-008 | Registry row `ICE-MC-FILEPOS-1` is filed BACKLOG for IPI-20. | The row in `docs/spark-sql-iceberg-parity.md` §7. | **PROVEN** | Row states the served two, the refused three with their `[ICE-MC-1]` pin, the inventory oracle, and the PR-2 intent; the refusal pins red on purpose when served. pins: ice-metadata-cols-1/C-008 |

## Gates

| Command | Result |
|---|---|
| lane gate `build-slot.sh local-gate.sh rc-meta repark-core,repark-iceberg,repark-spark python/repark/tests/test_ice_metadata_cols_1.py` | `CB=0 R=0 T=0 U=0 L=0` |
| `cargo test -p repark-core` (gate tier T) | `651 passed; 0 failed; 1 ignored` lib + `37` + `8` bins |
| `cargo test -p repark-iceberg` (gate tier T) | `630 passed; 0 failed` |
| `cargo test -p repark-spark` (gate tier T) | `1344 passed; 0 failed; 5 ignored` lib + integration bins green |
| pytest offline (gate tier U) | `7 passed in 28.20s` |
| pytest under `REPARK_PARITY_LIVE=1` (gate tier L) | `7 passed in 29.84s` |
| pin-source `cargo test -p repark-spark --lib tests::metadata_columns::` (M-1 leg) | narrowed suite `5 passed; 0 failed` (discovery: 3/8 draft tests green at pin) |
| `cargo fmt --all --check` | clean |
| `cargo clippy --locked --workspace --all-targets -- -D warnings -A clippy::disallowed_methods` (Makefile `rust-clippy` flags) | clean |
| `comment_ban.py /tmp/rc-meta origin/main HEAD` | `hits=0` |
| `scripts/check_lib_rs.py` | clean (`repark-core` at 154 of 154) |
| `scripts/check_rust_file_size.py`, `scripts/check_lib_py.py` | clean (new files under the 1000 default) |
| `ruff check` + `ruff format --check` on the new test file and `check_lib_rs.py` | `All checks passed!`, already formatted |

## COVERAGE_ATTESTATION

```
COVERAGE_ATTESTATION:
  pr_unit: ice-metadata-cols-1
  complete: true
  categories:
    - id: AT-1
      status: ATTACKED
      evidence: Each clause walked against a run — five recorded-value pins on the Arrow path value AND type (C-001..C-005), the star pin (C-006), the refusal plus fold plus composed-shape pins (C-007), the registry row (C-008).
      artifacts: [python/repark/tests/test_ice_metadata_cols_1.py, crates/repark-spark/src/tests/metadata_columns.rs]
    - id: AT-2
      status: ATTACKED
      evidence: Boundaries — empty projection (bare count), quoted vs folded vs compound names, non-query and two-relation shapes, backtick unserved names, the MoR delete twin; every added branch has a nameable input that changes the output.
      artifacts: [crates/repark-spark/src/tests/metadata_columns.rs, crates/repark-core/src/metadata_columns.rs, crates/repark-iceberg/src/catalog/metadata_columns.rs]
    - id: AT-3
      status: ATTACKED
      evidence: Refusal texts pinned verbatim ([ICE-MC-1] naming the column, no raw-leak assertion); temp pins release on every router path including the refusal return; the empty-projection rebuild cannot over-count (Inexact filters re-apply above the scan).
      artifacts: [crates/repark-spark/src/tests/metadata_columns.rs, crates/repark-spark/src/router.rs]
    - id: AT-4
      status: ATTACKED
      evidence: Temp-view minting is the draft's relaxed atomic counter, unchanged; the rewrite runs before lineage in one router pass and releases with the other pins; no lock, no shared mutation beyond registration.
      artifacts: [crates/repark-core/src/metadata_columns.rs, crates/repark-spark/src/router.rs]
    - id: AT-5
      status: N/A
      justification: Read-only scan path over caller-owned catalogs; no auth, secret, or injection surface added (names compared, never interpolated into commands).
    - id: AT-6
      status: ATTACKED
      evidence: Read-only provider (TableType::Base, no write arm); the narrowing only removes advertised columns; the refusal pins red on purpose when PR-2 serves the names.
      artifacts: [crates/repark-iceberg/src/catalog/metadata_columns.rs, crates/repark-spark/src/tests/metadata_columns.rs]
    - id: AT-7
      status: N/A
      justification: One temp-view registration per statement naming the columns, released after planning; the scan streams; no added materialization beyond the fork read.
    - id: AT-8
      status: ATTACKED
      evidence: Served set keyed off the hard-coded two-name const, never the fork's broad is_metadata_column_name; every fork behavior the layer needs verified at the pin by source and by run (M-1..M-4); no new dependency, no pin move.
      artifacts: [crates/repark-iceberg/src/catalog/metadata_columns.rs, task/ledgers/staging/ice-metadata-cols-1-ledger.md]
    - id: AT-9
      status: ATTACKED
      evidence: Every refusal carries the [ICE-MC-1] tag naming the column and the served set; internal scan failures name the missing column.
      artifacts: [crates/repark-core/src/metadata_columns.rs, crates/repark-iceberg/src/catalog/metadata_columns.rs]
    - id: AT-10
      status: ATTACKED
      evidence: Five recorded cells replayed verbatim on two harnesses (Rust lib + facade pytest); the refusal mutation (drop the unserved check) reds C-007, the empty-projection mutation reds C-003, and each added branch has a nameable input per AT-2.
      artifacts: [python/repark/tests/test_ice_metadata_cols_1.py, crates/repark-spark/src/tests/metadata_columns.rs]
```

Every clause above is PROVEN with a quoted command and its output; no clause is
OPEN. Touched files per clause: C-001..C-007
`crates/repark-core/src/metadata_columns.rs` +
`crates/repark-iceberg/src/catalog/metadata_columns.rs` +
`crates/repark-spark/src/tests/metadata_columns.rs` +
`python/repark/tests/test_ice_metadata_cols_1.py` plus the five code-dir
`map.md` files; C-008 `docs/spark-sql-iceberg-parity.md`; the lib.rs ceiling row
`scripts/check_lib_rs.py` + `scripts/map.md`; plus this ledger's own entry in
`task/ledgers/staging/map.md`. `Cargo.lock`, every `Cargo.toml`, the fork pin,
`STATUS.md`, the ANSI door, lineage, metadata tables, time travel and
describe/show: untouched. `make verify`, the full facade suite and the parity
harness not run per the brief's machine rule; the lane gate is the proof.
