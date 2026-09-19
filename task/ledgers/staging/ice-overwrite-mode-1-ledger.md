# Unit ledger — ICE-OVERWRITE-MODE-1 · Spark's overwrite partition set on every overwrite door (IPI-03)

**Date:** 2026-09-19 · **Branch:** `fix/ice-overwrite-mode-1` · **Base:** `5ceeb2cc` (`main`)
**Model:** claude-opus-5 (round 1, steps 1–5; round 2, steps 1–5; round 3, steps 1–6) · **Policy:** [../../../AGENTS.md](../../../AGENTS.md).
**Path:** STANDARD. **risk_tier: standard** (planner/DML scope decision on the write path; no
table-format semantics change).

**Retires:** this ledger moves to `../completed/` when the unit's last commit lands.

**Why now.** Owner direction 2026-09-18: 1:1 parity with Spark's Iceberg integration; a
declared refusal is no longer an end state when Spark answers. Registry row DML-1 read FIXED
but carried a "DECLARED residue" that is a silent wrong result: static-mode
`INSERT OVERWRITE t PARTITION (k)` kept the partitions absent from the source (stale rows) and
`overwrite-mode=dynamic` on `insertInto` replaced the whole table (data loss).

**Measured (the fixture).** Spark 4.1.2 + Iceberg 1.11.0, 30 shapes × format v2/v3 = 60
cells, each holding the rows and the snapshot history (operation plus seven summary counters)
or the refusal class, condition and SQLSTATE. The orchestrator's harness recording
(2026-09-18) and this unit's recorder (2026-09-19) agree on all 60 cells. On `main` 24 cells
differed.

**Spark's rule (read from its source, confirmed by the cells).** A PARTITION clause in static
mode is `OverwriteByExpression` with a filter of the static values only; no static value means
`true`, the whole table. Dynamic mode and `writeTo.overwritePartitions` are
`OverwritePartitionsDynamic`. Iceberg's `SparkWriteBuilder.overwrite` turns an `alwaysTrue`
filter into a dynamic overwrite when the `overwrite-mode` writer option lower-cases to
`dynamic`; any other value, and any filter with static values, is left alone. A PARTITION key
that is not an identity partition column refuses `NON_PARTITION_COLUMN`.

**Decision table implemented** (`repark_iceberg::write::overwrite_scope`):

| intent | session mode | static values | `overwrite-mode=dynamic` | scope |
|---|---|---|---|---|
| `Session` (SQL, `insertInto`) | dynamic | any | any | replace partitions (static values injected) |
| `Session` | static | yes | any | row filter over the static values |
| `Session` | static | no | no | whole table |
| `Session` | static | no | yes | replace partitions |
| `Static` (`saveAsTable`) | any | none | any | whole table |
| `Dynamic` (`writeTo.overwritePartitions`) | any | any | any | replace partitions |

An empty source wipes its scope (row filter or whole table stamps `delete`); an empty dynamic
source commits nothing (round 2, `replace_partitions_is_noop`; residue R-2 closed).

**Not in this unit:** `STATUS.md`, `Cargo.toml`, `Cargo.lock`, `saveAsTable(overwrite)`
snapshot history (Spark's RTAS table replace), the engine-wide `getCondition()` of native
errors.

## PROPOSITION LEDGER — ICE-OVERWRITE-MODE-1 — 2026-09-19

| Clause | Proposition (checkable) | Proof obligation | Verdict | Evidence / open question |
|---|---|---|---|---|
| C-001 | The 60-cell Spark oracle is recorded from live PySpark 4.1.2 + Iceberg 1.11.0 and committed with provenance, SHA-256 and a recorder whose `check` mode exits non-zero on drift; the live tier runs it. | `ice_overwrite_mode_1_spark_oracle.json`, `_record_ice_overwrite_mode_1_oracle.py`, `test_live_oracle_fixture_reproduces`. | PROVEN | SHA-256 `e4e41177…a917`; equal to the orchestrator's harness recording on 60/60 cells; the live leg's `check` passes (§Gates). |
| C-002 | Static mode with a PARTITION clause holding no static value — `PARTITION (cat)`, `PARTITION (cat, sub)`, `INSERT OVERWRITE TABLE t PARTITION (cat)` — replaces the whole table (Spark's rows and summaries), in default and explicit static mode. | Cells `PDYN-STATIC`, `PDYN-STATIC-EXPL`, `PDYN-TABLE-KW`, `TWO-BOTH-DYN-STATIC` (v2, v3); Rust `static_mode_partition_clause_without_values_replaces_the_whole_table` on both doors. | PROVEN | Red on `main` (kept `y`); green after the fix. |
| C-003 | An empty source in static mode wipes its scope: no static value → the whole table (`delete`, every file); static values → that filter only (`delete`). | Cells `PDYN-EMPTY-STATIC`, `PSTATIC-EMPTY`, `EMPTY-STATIC`; Rust `static_mode_empty_partition_clause_without_values_wipes_the_table`. | PROVEN | `PDYN-EMPTY-STATIC` refused on `main`; now `delete` with Spark's counters. |
| C-004 | A mixed list `PARTITION (cat='x', sub)` runs: static mode replaces every `sub` under `cat='x'` (row filter), dynamic mode replaces only the source's `(x, p)`. | Cells `TWO-MIXED-STATIC`, `TWO-MIXED-DYNMODE`; Rust `mixed_static_and_dynamic_keys_follow_the_session_mode` (Spark door) and `mixed_partition_keys_follow_the_session_mode` (native door); `plan_scopes_mixed_and_dynamic_clauses_by_mode`. | PROVEN | Refused on `main` (`cannot mix`); now Spark's rows and summaries. |
| C-005 | A PARTITION key that binds no partition field refuses `[NON_PARTITION_COLUMN] PARTITION clause cannot contain the non-partition column: \`k\`. SQLSTATE: 42000` as `AnalysisException`, and the table is untouched. | Cell `UNPART-PDYN-ERR`; `test_non_partition_column_refusal_keeps_every_row`; Rust `plan_refuses_a_non_partition_column_like_spark` and the two door pins. | PROVEN | `main` silently replaced everything. |
| C-006 | The `overwrite-mode` writer option: `dynamic` in any key or value case turns a whole-table overwrite into a dynamic one on `insertInto(overwrite=True)` and `mode("overwrite").insertInto`; `static`, `bogus` and the `partitionOverwriteMode` writer option change nothing; static values and the `saveAsTable` intent win over it. | Cells `OPT-OWMODE-DYN`, `OPT-OWMODE-DYN-MODE-OW`, `OPT-OWMODE-STATIC-DYNSESSION`, `OPT-OWMODE-BAD`, `OPT-PARTOWMODE-DYN`, `SAVEASTABLE-OW-OPT` rows; Rust `overwrite_mode_writer_option_turns_a_whole_table_overwrite_dynamic`, `overwrite_mode_option_keeps_static_values_and_the_static_intent`, `overwrite_mode_option_reads_dynamic_in_any_case_only`. | PROVEN | The two `OPT-OWMODE-DYN*` shapes lost `y` on `main`; the facade only forwards the option string. |
| C-007 | The typed intents hold: `writeTo.overwritePartitions` replaces only the source partitions in every session mode (the facade passes `force_dynamic_overwrite`), `saveAsTable(overwrite)` replaces the whole table in every mode (`force_static_overwrite`), and both flags at once raise `ValueError`. | Cells `WRITETO-OVERWRITEPARTS`, `WRITETO-OVERWRITEPARTS-OPT`, `SAVEASTABLE-*` rows; Rust `dynamic_intent_replaces_partitions_in_static_session_mode`; `test_writer_v2.py`, `test_examples_window_catalog.py`. | PROVEN | Without the dynamic intent the static-mode rule of C-002 would have widened `overwritePartitions` to the whole table. |
| C-008 | Every cell already equal on `main` stays equal (rows and summaries): dynamic-mode `PARTITION (cat)`, static values in both modes, the new-partition static value, no-clause static/dynamic, empty dynamic no-clause, the bucket table in both modes, `insertInto` in both modes. | The 40 control cells of `test_ice_overwrite_mode_1.py`. | PROVEN | Green before and after. |
| C-009 | The native SQL door plans through the same decision with the session mode: static-mode `PARTITION (k)` replaces the whole table (empty wipes), mixed lists run in both modes, `NON_PARTITION_COLUMN` refuses; whole-table no-clause stays Q9. | `crates/repark-sql/src/partition_overwrite.rs` (three new cells, two pins moved to dynamic mode), `crates/repark-sql/tests/ansi_write_defaults.rs`. | PROVEN | Python's native `repark.sql` door has no Iceberg `PARTITION` shape ("Partitioned inserts not yet supported"), so the native cells are Rust pins. |
| C-010 | Registry DML-1 reads true (FIXED 2026-09-19 for the static-mode wipe, mixed lists, empty static and `NON_PARTITION_COLUMN`; residue O5, dynamic-empty refuse and `BY NAME` mixed named), row ICE-OVERWRITE-MODE-1 reads FIXED 2026-09-19 with before/after, and ICE-WRITE-OPTIONS-1 no longer implies `overwrite-mode` is ignored. | `docs/spark-sql-iceberg-parity.md` diff; `check_docs_links`. | PROVEN | See the three rows. |
| C-011 | An empty source in dynamic mode commits nothing — `PARTITION (cat)` and the mixed `PARTITION (cat='x', sub)` under session mode `dynamic`, and `writeTo.overwritePartitions` of an empty frame: rows and snapshot list unchanged. The mixed empty source in static mode still deletes `cat='x'` (`delete`). | Cells `OW2-R2-PDYN-EMPTY-DYN`, `OW2-R2-MIXED-EMPTY-DYN`, `OW2-R2-MIXED-EMPTY-STA`; the empty-frame `overwritePartitions` arm moved to the recorded cells of C-016 in round 3. | PROVEN | Green after round-2 step 2: the three `OW2-R2-*` cells (v2, v3) and the empty-frame pin; Rust `empty_mixed_source_commits_nothing_in_dynamic_mode_and_deletes_in_static_mode`, `empty_dynamic_partition_overwrite_commits_nothing` (both doors), `an_empty_dynamic_stage_skips_the_commit`. The empty-frame `writeTo.overwritePartitions` cell is not in the fixture (no Spark JVM run in this round); its no-op is the Spark-consistent reading of registry row EX-W2-2 (measured 2026-09-04) and is named for the orchestrator to record. |
| C-012 | `INSERT OVERWRITE … PARTITION (cat='x', sub) BY NAME` runs through the same decision: static mode replaces all of `cat='x'`, dynamic mode only `(x, p)`. | Cells `OW2-R3-BYNAME-MIXED-STA`, `OW2-R3-BYNAME-MIXED-DYN`, `OW2-R3-BYNAME-STATIC`. | PROVEN | Green after round-2 step 2: `OW2-R3-*` cells; Rust `mixed_static_and_dynamic_keys_follow_the_session_mode` runs the `BY NAME` form in both modes. |
| C-013 | A static value is cast to its partition source type with the engine's cast: `PARTITION (d = '2024-01-01')` on a `DATE` partition replaces that partition; a value the cast rejects refuses with the engine's own `CAST` refusal text and the table is untouched. | Cell `OW2-CAST-STATIC-DATE`; `test_invalid_static_date_refuses_like_the_engine_cast`. | PROVEN | Green after round-2 step 2: `OW2-CAST-STATIC-DATE` cells; Rust `plan_casts_a_static_value_to_the_partition_source_type`, `static_value_is_cast_to_a_date_partition_and_an_invalid_value_refuses`. The refusal text is the engine's Arrow cast text (`Cast error: Cannot cast string '2024-13-45' to value of Date32 type`), not Spark's `CAST_INVALID_INPUT`; both refuse. |
| C-014 | A PARTITION key naming the source of a non-identity partition field (`bucket`, `days`) refuses `[NON_PARTITION_COLUMN]` as `AnalysisException`, static or dynamic, before the value is read; `NULL`, upper-case keys and upper-case session mode values keep Spark's answer. | Cells `OW2-R1-BUCKET-STATIC`, `OW2-R1-BUCKET-DYN`, `OW2-R1-DAYS-STATIC`, `OW2-R1-IDENT-AND-BUCKET`, `OW2-NULL-STATIC`, `OW2-CASE-STATIC`, `OW2-OPT-BOTH-SQL`. | PROVEN | Green after round-2 step 2: `OW2-R1-*`, `OW2-NULL-STATIC`, `OW2-CASE-STATIC`, `OW2-OPT-BOTH-SQL` cells; Rust `plan_refuses_a_transform_source_key_before_reading_its_value`; PIN O5 flipped. |
| C-015 | Registry rows read true after round 2: DML-1 and ICE-OVERWRITE-MODE-1 carry the round-2 before/after and a residue list without R-1..R-3; EX-W2-2 and ICE-V3-WRITE-DEFAULT-1-MIX-PARTITION read FIXED with their renamed pins. | `docs/spark-sql-iceberg-parity.md` diff; `check_docs_links`. | PROVEN | Round-2 step 3. |
| C-016 | An empty frame answers Spark on the DataFrame overwrite doors: `writeTo.overwritePartitions()` in static and in dynamic session mode and `insertInto(overwrite=True)` with `overwrite-mode=dynamic` commit nothing (rows and snapshot list unchanged); `insertInto(overwrite=True)` in static mode commits a `delete` of every file and leaves the table empty. The recorder re-derives the 8 cells. | Cells `OW3-WRITETO-OWP-EMPTY`, `OW3-WRITETO-OWP-EMPTY-DYN`, `OW3-INSERTINTO-OW-EMPTY-DYNOPT`, `OW3-INSERTINTO-OW-EMPTY-STATIC` (v2, v3); recorder `check` live. | PROVEN | Folded round-3 step 2 (96 cells); all 8 green on the round-2 tree, no Rust change. |
| C-017 | The typed overwrite intent is pinned at the PyO3 binding seam: `session_sql_with_write_options` with `force_dynamic_overwrite` keeps the untouched partition of `PARTITION (cat)` in a static session, with `force_static_overwrite` replaces the table in a static and a dynamic session, with neither follows the session mode (static replaces, dynamic keeps), and with both refuses `ValueError`. Forcing the entry's intent to `Session` turns the tests red. | `crates/repark-python/src/tests.rs` `binding_*` (four tests). | PROVEN | Round-3 step 3 (P2-PY-INTENT). Mutations: intent forced to `Session` in the entry → the Dynamic and Static tests red; `Static` mapped to `Session` → the Static test red; both restored green. |

## Pins flipped (each with its reason)

- `crates/repark-iceberg/src/write/partition_overwrite.rs::mixed_static_dynamic_refuses` →
  `mixed_static_dynamic_parses`: Spark answers mixed lists (C-004); the request is now a struct.
  `request_static_and_dynamic_shapes` / `string_and_null_literals` read the struct fields.
- `crates/repark-spark/src/tests/partition_overwrite.rs::dynamic_partition_overwrite_replaces_source_partitions_only`
  and `::empty_dynamic_partition_overwrite_refuses`: now under `setup_dynamic`; in the default
  static mode `PARTITION (id)` is Spark's whole-table replace (C-002, C-003).
- `crates/repark-sql/src/partition_overwrite.rs::dynamic_partition_overwrite_keeps_absent_partitions`
  and `::empty_dynamic_partition_overwrite_refuses`: now under `door_with_mode(Dynamic)` (C-009).
- `python/repark/tests/test_dml_b_partition_overwrite.py::test_sql_dynamic_partition_overwrite_keeps_absent_partitions`
  and `::test_sql_empty_dynamic_partition_overwrite_refuses`: now on a `dynamic` fixture that
  restores the mode; the static-mode twin
  `test_sql_static_mode_partition_without_values_replaces_whole_table` pins C-002/C-003.
- `python/repark/tests/test_ice_v3_write_default_1.py::test_dynamic_partition_named_list_fills_write_default`:
  compared the table to seed + written rows (dynamic reading) and only checked the Spark cell by
  containment; Spark's recorded table is the written row alone (whole-table replace). Now it
  equals the recorded cells exactly.
- `python/repark/tests/test_ice_v3_write_default_1.py::test_mixed_static_dynamic_partition_refuses`
  → `test_mixed_static_dynamic_partition_matches_spark`: answers the recorded `MIX_*` rows on
  `(id, cat, payload)`; the probe's table carried a `c` write-default RePark cannot create
  (`CREATE TABLE … DEFAULT` is unsupported), so `c` is out of the compare.

`rg -n "PARTITION \(" crates/*/src/tests python/repark/tests | rg -i overwrite` found no other
pin asserting the old refusal or the old dynamic reading; the PIN O5 transform refusal
(`transform_overwrite.rs`) is kept on purpose (R-1).

## Round 2 (2026-09-19) — pins flipped, each with its reason

- `crates/repark-iceberg/src/write/partition_overwrite.rs::empty_dynamic_guard_refuses` —
  deleted with `refuse_empty_dynamic_overwrite` and `EMPTY_DYNAMIC_OVERWRITE_NEEDLE`; the
  empty-dynamic rule is now `overwrite_scope::replace_partitions_is_noop`, pinned by
  `tests/overwrite_scope.rs::an_empty_dynamic_stage_skips_the_commit` (C-011).
- `crates/repark-spark/src/tests/partition_overwrite.rs::empty_dynamic_partition_overwrite_refuses`
  and `crates/repark-sql/src/partition_overwrite.rs::empty_dynamic_partition_overwrite_refuses`
  → `…_commits_nothing`: Spark commits nothing (cell `OW2-R2-PDYN-EMPTY-DYN`); both now
  assert the snapshot count and every row unchanged (C-011).
- `python/repark/tests/test_dml_b_partition_overwrite.py::test_sql_empty_dynamic_partition_overwrite_refuses`
  → `…_commits_nothing`; `test_writer_v2.py::test_write_to_overwrite_partitions_replaces_source_partitions_only`'s
  empty arm; `test_examples_window_catalog.py::test_writerv2_overwrite_partitions_empty_refuses`
  → `…_empty_commits_nothing` (registry EX-W2-2, Spark measured the no-op 2026-09-04) (C-011).
- `crates/repark-spark/src/tests/transform_overwrite.rs::overwrite_partition_clause_on_transform_table_still_rejected`
  (PIN O5) → `…_on_transform_source_refuses_non_partition_column`: Spark refuses
  `NON_PARTITION_COLUMN` for `PARTITION (id = 1)` on `bucket(4, id)` (cell `OW2-R1-BUCKET-STATIC`);
  the untouched-files and row-count assertions are kept (C-014).
- `crates/repark-spark/src/insert_by_name.rs`'s mixed-list refusal had no pin of its own; the
  `OW2-R3-*` cells were red on it (C-012).

`rg -n "dynamically overwrite|EMPTY_DYNAMIC|cannot mix static|NotImplemented.*PARTITION"`
over `crates/` and `python/` finds nothing left after the flips.

## Residue (named, not fixed here)

Round 2 (2026-09-19) closed R-1, R-2 and R-3 (C-011, C-012, C-014) and the cast refusal
it found (C-013). Still open: R-4, R-5, and two named by round 2 — **R-6** an invalid static
value refuses with the engine's Arrow `Cast error` text where Spark raises
`CAST_INVALID_INPUT` (both refuse, table untouched); **R-7** a typed literal
(`PARTITION (d = DATE'2024-01-01')`) on an identity key still refuses (after the key check),
unmeasured on Spark.

- **R-1** — transform-field static `PARTITION (id = 1)` on a bucket table keeps its typed
  `NotImplemented` refusal (PIN O5). Spark's `partitionColumnNames` lists identity columns only,
  so Spark refuses `NON_PARTITION_COLUMN` there (read from source, not measured).
- **R-2** — an empty source under dynamic mode (`PARTITION (k)`, `writeTo.overwritePartitions`)
  refuses `Cannot dynamically overwrite partitions with no data`; Spark skips the commit.
- **R-3** — a mixed static/dynamic list on `INSERT … BY NAME` still refuses.
- **R-4** — `saveAsTable(overwrite)` history: Spark writes an RTAS table replace (a fresh
  `overwrite` snapshot with no deleted files); RePark overwrites in place. Rows equal; pinned as a
  strict xfail.
- **R-5** — native engine errors answer `getCondition() is None` engine-wide; the refusal's class
  and message carry Spark's condition token.

## Step 4 (2026-09-19) — one more flipped pin, and the gates

**Flipped in step 4.** The `rg` sweep of step 2 missed a pin that runs its statement from a
helper module: V3-COV's `insert-overwrite-partition-dynamic`
(`_v3_statement_coverage_programs.py`, static-mode `PARTITION (part)`). Its measured RePark
half kept the other partitions; the recorded Spark half is the single written row. The RePark
half now equals the Spark half and the verdict flips DIVERGES → EQUAL
(`_v3_statement_coverage_repark.py`, `_v3_statement_coverage_golden.py`,
`docs/design/v3-statement-coverage.md`). Evidence for C-002.

**Gates** (release native rebuilt from the final tree):

- Harness replay (`cells_ow.py`, 60 cells): rows equal on 60/60; rows plus snapshot summaries
  plus refusal class and message equal on 56/60 — the 4 misses are the `saveAsTable(overwrite)`
  RTAS histories (R-4). Strict key including `getCondition()`: 54/60 (R-5 adds the two
  `NON_PARTITION_COLUMN` cells).
- Offline `-n 4`: the new file plus every file `rg` finds for `INSERT OVERWRITE|insertInto|
  overwritePartitions|overwrite-mode` (29 test files) — 1098 passed, 37 skipped, 20 xfailed;
  the 30 further test files naming `overwrite` or the V3-COV / promote-read programs — 1346
  passed, 93 skipped, 1 xfailed (after the V3-COV flip).
- Live (`REPARK_PARITY_LIVE=1`, PySpark 4.1.2): `test_ice_overwrite_mode_1.py` 62 passed,
  4 xfailed; the recorder's `check` reproduces the fixture.
- `cargo test -p repark-spark --lib` (overwrite, partition, by-name, write-option, dialect,
  transform filters) 158 passed; `-p repark-sql --lib` 365 passed; `--test
  ansi_write_defaults` 8 passed; `-p repark-iceberg --lib` (overwrite filters) 36 passed;
  `-p repark-core --lib` (dialect, mode, write-option, session filters) 171 passed.
- `cargo fmt --all --check` clean; `cargo clippy -p <crate> --all-targets -- -D warnings -A
  clippy::disallowed_methods` clean on repark-iceberg, -core, -spark, -sql, -python; the
  panic-ban form (`--workspace --lib --bins --exclude repark-python -- -D warnings`) clean.
- `ruff check .` clean; `ruff format --check` on the changed Python clean; lib-py,
  rust-file-size, python-conventions, docstring-presence, ledger-grammar, docs-links and
  map-sync clean. Comment ban: 0 hits on every commit.

## Round 2 step 4 (2026-09-19) — the gates

Release native rebuilt from the round-2 step-2 tree (no Rust change after it).

- Harness replay (`cells_pc3.py`, 88 cells: the 60 base cells re-recorded plus 28 `OW2`):
  rows, snapshot summaries and error presence equal on 84/88 — the 4 misses are the
  `saveAsTable(overwrite)` RTAS histories (R-4). Every refusing cell matches Spark's class
  and carries its `[NON_PARTITION_COLUMN]` token; `getCondition()` stays `None` (R-5).
- Offline `-n 4`: the 29 test files `rg` finds for `INSERT OVERWRITE|insertInto|
  overwritePartitions|overwrite_partitions|overwrite-mode` — 1128 passed, 37 skipped,
  20 xfailed; the 32 further files naming `overwrite` or the V3-COV / promote-read programs —
  1403 passed, 93 skipped, 1 xfailed.
- Live (`REPARK_PARITY_LIVE=1`, PySpark 4.1.2, JVM lock): `test_ice_overwrite_mode_1.py`
  92 passed, 4 xfailed; the recorder's `check` re-derives all 88 cells, so the 28 folded
  `OW2` cells are reproduced by this unit's recorder on live Spark.
- `cargo test -p repark-iceberg --lib` (overwrite filters) 38 passed; `-p repark-spark --lib`
  (overwrite, partition, by-name, write-option, dialect, transform filters) 160 passed and
  (`overwrite partition insert`) 277 passed; `-p repark-sql --lib` 365 passed, `--test
  ansi_write_defaults` 8 passed; `-p repark-core --lib` (dialect, mode, write-option, session)
  171 passed.
- `cargo fmt --all --check` clean; `cargo clippy --locked -p <crate> --all-targets -- -D
  warnings -A clippy::disallowed_methods` clean on repark-iceberg, -spark, -sql; the
  panic-ban form (`--workspace --lib --bins --exclude repark-python -- -D warnings`) clean.
- `ruff check .` clean; `ruff format --check` on the changed Python clean; rust-file-size,
  lib-py, python-conventions, docstring-presence, ledger-grammar, docs-links and map-sync
  clean. `test_cap_1_source_file_line_cap.py` was red on round 1's tree: round 1 lowered
  `writer_readwriter.py`'s `check_lib_py.py` row 1095 → 1093 without its mirror; the mirror
  now reads 1093 (23 passed). Comment ban: 0 hits after every commit.

## Round 3 (2026-09-19) — the empty-frame cells, the binding seam, and the gates

- Main merged at round-3 step 1 (RP-35 fork pin `7bd2fea3`, ICE-DROP-NS-1, ICE-READ-PERF-0);
  three `map.md` conflicts kept both sides. `repark-core/src/session.rs` came to 1001 lines
  under the default ceiling of 1000: the `OverwriteIntent` import joined the crate `use` list
  and the `sql_with` call fits one line (996).
- The 8 `OW3` empty-frame cells (C-016) joined the fixture (96 cells, SHA-256
  `df60a901…0b90`); the recorder gained an `empty_frame` shape flag. All 8 were already
  green; `test_empty_frame_overwrite_partitions_commits_nothing` was dropped because the
  `OW3-WRITETO-OWP-EMPTY-*` cells pin the same answer with the full snapshot history.
- P2-PY-INTENT (C-017): four Rust tests drive the binding entry itself.

## Coverage attestation

```yaml
COVERAGE_ATTESTATION:
  pr_unit: ice-overwrite-mode-1
  categories:
    - id: AT-1
      status: ATTACKED
      evidence: Every expectation reads from the committed Spark 4.1.2 + Iceberg 1.11.0
        oracle, which equals the orchestrator's independent harness recording on 60/60
        cells; the live leg re-derives it. The decision table cites Spark's and
        Iceberg's source rules and each row is exercised by a recorded cell.
      artifacts: [python/repark/tests/ice_overwrite_mode_1_spark_oracle.json, python/repark/tests/_record_ice_overwrite_mode_1_oracle.py]
    - id: AT-2
      status: ATTACKED
      evidence: Red on main for exactly the 20 differing non-RTAS cells plus the
        refusal test (40 controls green, 4 RTAS xfail); green after the fix. The
        flipped pins asserted the old dynamic reading or the old refusal, and the
        v3 write-default pin had hidden the divergence behind a containment check.
      artifacts: [python/repark/tests/test_ice_overwrite_mode_1.py, python/repark/tests/test_ice_v3_write_default_1.py]
    - id: AT-3
      status: ATTACKED
      evidence: Every cell on v2 and v3, both SQL doors, SQL and DataFrame writer
        spellings, empty and non-empty sources, one- and two-level and bucket and
        unpartitioned tables; the decision table covers every intent, mode, option
        and static-value combination.
      artifacts: [crates/repark-iceberg/src/tests/overwrite_scope.rs, crates/repark-spark/src/tests/overwrite_mode.rs, crates/repark-sql/src/partition_overwrite.rs]
    - id: AT-4
      status: ATTACKED
      evidence: The decision is a pure function of per-statement inputs; the session
        mode is read from the statement's context and the intent travels on the
        statement's options, so no state is shared across writes. Commit isolation
        and validation paths are the existing ones.
      artifacts: [crates/repark-iceberg/src/write/overwrite_scope.rs, crates/repark-spark/src/write_options.rs]
    - id: AT-5
      status: ATTACKED
      evidence: A data-destroying scope (whole table) is chosen only where Spark
        chooses it; the NON_PARTITION_COLUMN check runs before any staging or
        commit and leaves the table untouched (pinned). Both typed flags at once
        refuse instead of picking one.
      artifacts: [python/repark/tests/test_ice_overwrite_mode_1.py, crates/repark-python/src/session_write_options.rs]
    - id: AT-6
      status: ATTACKED
      evidence: Rows, Arrow types and the seven snapshot counters are pinned per cell;
        residues R-1..R-5 are named in the registry and here rather than hidden.
      artifacts: [docs/spark-sql-iceberg-parity.md, task/ledgers/staging/ice-overwrite-mode-1-ledger.md]
    - id: AT-7
      status: N/A
      justification: No performance or wall-clock claim; the decision adds one match per overwrite statement.
    - id: AT-8
      status: ATTACKED
      evidence: No STATUS.md, Cargo.toml or Cargo.lock edit and no new crate edge;
        repark-core re-exports OverwriteIntent so the binding keeps its DAG. The
        facade change shrinks writer_readwriter.py 1095 -> 1093 (ceiling ratcheted
        down); partition_overwrite.rs shrinks under the default ceiling; new code
        sits in new files. No code comments added.
      artifacts: [scripts/check_lib_py.py, crates/repark-iceberg/src/write/overwrite_scope.rs]
    - id: AT-9
      status: ATTACKED
      evidence: The failure paths (NON_PARTITION_COLUMN, O5, the dynamic-empty
        refusal, conflicting typed flags) refuse before any catalog mutation and
        are pinned; registry rows DML-1, ICE-OVERWRITE-MODE-1 and ICE-WRITE-OPTIONS-1
        read true and every touched map.md carries the change with pins.
      artifacts: [docs/spark-sql-iceberg-parity.md, crates/repark-spark/src/tests/overwrite_mode.rs]
    - id: AT-10
      status: ATTACKED
      evidence: Every Python file that runs INSERT OVERWRITE, insertInto,
        overwritePartitions or overwrite-mode ran green offline, the harness replay
        matches Spark on every non-RTAS cell, and the per-crate Rust suites around
        overwrite, partition, by-name, insert, write-option and dialect pass.
      artifacts: [python/repark/tests/test_dml_b_partition_overwrite.py, python/repark/tests/test_writer_v2.py, crates/repark-spark/src/tests/partition_overwrite.rs]
  complete: true
```
