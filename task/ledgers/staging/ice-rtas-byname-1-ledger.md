# Unit ledger — ICE-RTAS-BYNAME-1 · `INSERT … BY NAME` + RTAS snapshot operations

## Round 1 (2026-09-17) — RePark side

Rating report 2026-09-16 row V2-24 (worker-findings
`/tmp/oc-worker/ice-rating/worker-findings.md` §V2-24; probes `p_sort_overwrite.py`,
`p_append_resolution.py` under `/tmp/oc-worker/ice-rating/scratch/probes/`):
V2-24-1 `INSERT INTO t BY NAME SELECT …` is a RePark parse error where Spark 4
accepts it; V2-24-2 RTAS over an existing table records `append, append` where
Spark records `append, overwrite`.

Measured Spark oracle (orchestrator-recorded):
`/tmp/oc-worker/ic-build/write_fidelity_spark.json` (probe
`write_fidelity_probe.py` beside it), cells `insert_by_name` and `rtas_ops`.
Committed here as `python/repark/tests/ice_rtas_byname_1_spark_oracle.json` with
generator `python/repark/tests/_record_ice_rtas_byname_1_oracle.py`.

**Date:** 2026-09-17 · **Branch:** `feat/ice-rtas-byname-1` · **Base:** `origin/main`
· **Model:** muse-spark-1.3-contributor · **Policy:** [../../../AGENTS.md](../../../AGENTS.md).

**Retires:** this ledger moves to `../completed/` when the unit's last commit lands.

**Why now.** Two V2-24 findings on the write path: the Spark door cannot parse
the `BY NAME` column-resolution modifier at all, and the staged
create/replace commit records the wrong snapshot operation on replace.

**Not in this unit:** the fork change (orchestrator fork lane, ask
F-RTAS-OPS-1); `STATUS.md`; size ceilings; anything outside Home files plus
ledger, registry doc, tests and maps.

**Writable paths:** `crates/repark-spark/src/` (BY NAME intercept + rewrite),
`crates/repark-sql/src/sniff.rs` (native-door steer), `python/repark/tests/`
(oracle, generator, pins), `docs/spark-sql-iceberg-parity.md` (rows),
`task/ledgers/staging/ice-rtas-byname-1-ledger.md`,
touched `map.md` files.

### Charter

- C-001 `INSERT INTO … BY NAME SELECT`: by name, case-insensitive, missing
  nullable source column NULL-filled, Spark's errors for extra/duplicate.
- C-002 `INSERT OVERWRITE … BY NAME`.
- C-003 `BY NAME VALUES` error.
- C-004 BY NAME on the other table kinds the SQL door writes.
- C-005 RTAS operations (BLOCKED-ON-FORK, xfail pins + exact fork change).
- C-006 registry rows.

### Door decision

`BY NAME` is Spark-dialect syntax with no ANSI spelling, so the Spark door
(`ReparkSession.sql`, `crates/repark-spark`) parses and executes it, and the
native door (`repark.sql()`, `crates/repark-sql`) steers it the way it already
steers `INSERT OVERWRITE`, `LATERAL VIEW` and the other Spark-isms in
`sniff.rs`. A blended native parser would break ADR-0002. The DataFrame door
already resolves by name (`writeTo().append()`); a pin holds it on the same
table shape while `insertInto` stays positional.

The resolver is a Rust kernel in `crates/repark-spark`
(`insert_by_name.rs`): the SQL door reaches it, and the Python facade binds
names and argument shapes only. The `writeTo` by-name projection it might have
reused lives in Python
(`python/repark/src/repark/spark/dataframe/writer_readwriter.py::_by_name_projection`),
so there is no Rust resolver to reuse; this unit adds the first one.

### Red-first (unfixed tree, release native 2026-09-17)

BY NAME parse (probe `/tmp/bn_red.py`, memory catalog, target
`(first_name STRING, last_name STRING, n INT)`, source `(last_name STRING,
first_name STRING, n INT)`):

```text
ERR: INSERT INTO mem.ns.bn BY NAME SELECT * FROM mem.ns.sw -> ParseException SQL error:
  ParserError("Expected: SELECT, VALUES, or a subquery in the query body, found: BY at Line: 1, Column: 29")
ERR: INSERT OVERWRITE mem.ns.bn BY NAME SELECT 'O' AS last_name, 'P' AS first_name, 8 AS n ->
  ParseException SQL error: ParserError("Expected: SELECT, VALUES, or a subquery in the query
  body, found: BY at Line: 1, Column: 34")
```

RTAS operations (probe `/tmp/rtas_red.py`):

```text
ctas_then_rtas: ['append', 'append']   # Spark: ['append', 'overwrite']
rtas_new: ['append']                   # Spark: ['overwrite']
rtas_empty_new: []                     # Spark: ['delete']
rtas_empty_twice: []                   # Spark: ['delete', 'delete']
```

### C-004 scope (measured 2026-09-17)

PROVEN — `USING parquet` takes the same rewrite with one different error
cell. The first scope note was wrong: the Spark door DOES create parquet
tables and positional `INSERT INTO` a `USING parquet` table commits (probed
2026-09-17 on the unfixed tree). The extra Spark probes (`/tmp/pq_probe.py`,
`/tmp/pq_probe2.py`, `/tmp/pq_probe3.py`, folded into the generator as the
`parquet_by_name` section) show the parquet matrix equals the Iceberg matrix
except one cell: a SHORTER source with an unmapped name answers
`INCOMPATIBLE_DATA_FOR_TABLE.EXTRA_COLUMNS` on parquet where the longer
Iceberg `by_name_extra` answers `TOO_MANY_DATA_COLUMNS`. The `/tmp/pq_probe3.py`
disambiguator (`first_name, bogus1, bogus2, bogus3` into three columns →
TOO_MANY) proves the rule is provider-independent and count-first:

1. more source columns than target columns → `TOO_MANY_DATA_COLUMNS`
   (all source names listed);
2. else a case-insensitive duplicate source name →
   `INCOMPATIBLE_DATA_FOR_TABLE.AMBIGUOUS_COLUMN_NAME` (names the first
   spelling; measured case-insensitive by the new `by_name_case_dup` cell);
3. else an unmapped source name → `INCOMPATIBLE_DATA_FOR_TABLE.EXTRA_COLUMNS`
   (unmapped names listed).

`INSERT INTO t (columns) BY NAME` is a Spark `PARSE_SYNTAX_ERROR`
(`Syntax error at or near 'BY'`, SQLSTATE 42601 — new `by_name_column_list`
cell), so the rewrite must refuse that combination at parse altitude, never
strip-and-run it positionally.

### C-005 — the RTAS operation is fork-owned

RePark's CTAS/RTAS path (`crates/repark-spark/src/ctas.rs::execute_ctas`)
stages the SELECT into data files, then commits through the fork's
`StagedTableTransaction` (`crates/iceberg/src/transaction/staged_table.rs` at
`~/.cargo/git/checkouts/iceberg-rust-da38c1c1a42143bd/edc38c6/`):
`begin_create` for a new table, `begin_replace(&existing, …)` for a replace,
then `add_data_files(files).commit(catalog)`. `commit` calls
`materialize_pending`, which runs `tx.fast_append()` unconditionally — hence
`Operation::Append` on every non-empty staged commit — and returns the staged
table unchanged when `pending_data_files` is empty, hence NO snapshot at all
on an empty RTAS. Both halves of the divergence live in that one fork
function; no RePark-side patch can change the recorded operation, so per
`docs/fork-sync.md` rule 3 this unit does not touch it.

Exact fork change Spark's answer needs (ask F-RTAS-OPS-1), all inside
`StagedTableTransaction::materialize_pending`:

- Replace mode with files stages an `overwrite` commit, not `fast_append`.
  Summary keys per the fixture `rtas_ops.ctas_then_rtas[1]` /
  `rtas_new_table[0]`: the added/total/manifest keys Java writes
  (`added-data-files`, `added-records`, `total-records`, `total-data-files`,
  `total-files-size`, `added-files-size`, `changed-partition-count`,
  `manifests-created`, engine keys) with NO `deleted-*` keys — a replace
  snapshot carries no `deleted-data-files`, `deleted-records` or
  `removed-files-size`.
- Replace mode with no files still commits one snapshot with operation
  `delete` (fixture `rtas_empty_new` / `rtas_empty_twice`): total keys at
  zero, no added keys.
- Create mode (plain CTAS) keeps `append` (fixture `ctas_then_rtas[0]`).
  Plain-CTAS-empty is unmeasured — no fixture cell covers it — and stays
  unclaimed.

RePark-side routing note for the fork lane: RTAS over a MISSING table goes
through `CtasMode::StagedCreate` (`begin_create`) today, but Spark records
`overwrite` for it (fixture `rtas_new_table`), so the RTAS replace path must
stay on the replace commit even when the table does not exist yet, or the
fork's replace entry must tolerate a missing base. The xfail pins below hold
all four shapes.

pins: ice-rtas-byname-1/C-005a, C-005b, C-005c, C-005d
(`xfail(strict=True, reason="BLOCKED-ON-FORK F-RTAS-OPS-1")`).

### Oracle cells (fixture `ice_rtas_byname_1_spark_oracle.json`)

`insert_by_name` (table `bn (first_name STRING, last_name STRING, n INT)`,
view `swapped` = `SELECT 'Smith' AS last_name, 'Ann' AS first_name, 1 AS n`):
`by_name` → `[('Ann','Smith',1)]`; `by_name_subset`
(`SELECT 'Bo' AS first_name, 2 AS n`) → missing `last_name` NULL-filled;
`by_name_extra` → `[INSERT_COLUMN_ARITY_MISMATCH.TOO_MANY_DATA_COLUMNS] …
SQLSTATE 21S01`; `by_name_case` (`FIRST_NAME`, `Last_Name`, `N`) →
`[('Di','Y',5)]`; `by_name_dup` (two `first_name`, four source columns) → same
TOO_MANY_DATA_COLUMNS; `by_name_case_dup` (`first_name` + `FIRST_NAME`, three
source columns) → `INCOMPATIBLE_DATA_FOR_TABLE.AMBIGUOUS_COLUMN_NAME`;
`positional` stays positional (`[('Smith','Ann',1)]`); `by_name_values` →
`[INCOMPATIBLE_DATA_FOR_TABLE.EXTRA_COLUMNS] … extra columns
col1, col2, col3 … SQLSTATE KD000`; `by_name_column_list`
(`(n, first_name) BY NAME`) → Spark `PARSE_SYNTAX_ERROR` at `BY`;
`overwrite_by_name` → `INSERT OVERWRITE … BY NAME …` leaves `[('P','O',8)]`.
`parquet_by_name` repeats the matrix on a `USING parquet` table (same rows;
`pq_extra` answers EXTRA_COLUMNS by the count-first rule).
`rtas_ops`: `ctas_then_rtas` → `[append, overwrite]`; `rtas_new_table` →
`[overwrite]`; `rtas_empty_new` → `[delete]`; `rtas_empty_twice` →
`[delete, delete]`, with the full Java summaries in the fixture. Spark basis:
PySpark 4.1.2 + iceberg-spark-runtime-4.1_2.13:1.11.0 (GAV from
`python/repark/tests/_oracle_pins.py`, never restated), recorded 2026-09-16
by the orchestrator probe and re-derived by the committed generator.

### Implementation (round 1)

`BY NAME` executes on the Spark door in `crates/repark-spark/src/insert_by_name.rs`
(Rust kernel, both SQL-facing doors reach it: the Spark door executes, the
native door steers). `strip_insert_by_name` excises the modifier at the token
level (sqlparser 0.59 has no `BY NAME` on `Insert`), erroring
`PARSE_SYNTAX_ERROR` when an explicit column list precedes it (measured Spark
behavior). `match_source_to_target` applies the count-first rule from the
C-004 probes (count → case-insensitive duplicate → unmapped, with the three
byte-exact Spark texts through `DataFusionError::Plan` → `AnalysisException`).
Source names come from planning `SELECT * FROM (<source>) … LIMIT 0` (VALUES
sources synthesize Spark's `col1…colN` from the AST arity); the missing
nullable fill is a bare `NULL AS target` (the store-assignment matrix admits
`Null`, probed 2026-09-17).

Why the staged route, not a SQL rewrite into the DML passthrough: the
probes `/tmp/wt_debug*.py` prove `INSERT INTO t SELECT <reordered> FROM s`
silently writes scan-order values under target-order names on the current
tree. Root cause, measured to the file bytes: the physical plan is correct
but the fork's `apply_write_defaults`
(`crates/iceberg/src/writer/write_defaults.rs`) matches batch columns to the
target by `PARQUET:field_id` first, and a scan-sourced batch carries the
SOURCE table's ids. Same-shape tables share id sequences, which is why the
existing suite never saw it. A logical-plan id strip cannot propagate (the
physical `ProjectionExec` rebuilds field metadata from its input), and CAST
armor preserves the stale ids (probed). So `INSERT … BY NAME` stages through
RePark's own name-based conform (`conform_batch` inside the existing stream
stagers, which rebuild batches with target ids) and commits `fast_append`
(`commit_append_to`, new, mirrors `commit_overwrite_replace_all_to` for the
branch); `INSERT OVERWRITE … BY NAME` resolves names, rewrites the source to
a positional projection, and delegates to the existing
`insert_overwrite_from_staged_source` (whose positional mapper already
re-stamps target ids). Store-assignment refusals flow from the same matrix
(the staged conform labels the op `append`; the DML gate labels it
`INSERT INTO`). Consequences recorded: the pre-existing reordered-positional
DML skew stays open for the fork lane (second ask below); the new surface
never routes through it.

Fork asks: F-RTAS-OPS-1 (RTAS operations, C-005) and F-DML-FIELD-ID-1 (the
fork's id-first `batch_column_index` misroutes any DML batch whose field ids
come from a differently-ordered source table; engine-side evidence in this
ledger).

Stack-overflow lesson (2026-09-17): adding the `execute_insert_by_name` arm
to the Spark router overflowed the 2 MiB test-thread stack in
`refs_and_wap::ref_selector_on_the_read_side_of_dml_is_a_read` — gdb showed
pure DataFusion planner recursion with no RePark cycle. The arm's future
(`Table`, streams, name vectors) rides inside `execute_inner`'s future on the
polling thread's stack during deep planning, so the router now
`Box::pin`s the arm onto the heap. Any future router arm carrying table-sized
state needs the same treatment.

### Gates

Gates for step 5 (`test_ice_rtas_byname_1.py`, `test_insert_store_assign.py`,
`test_sql_harden_cutover.py`, `test_writer_v2.py`,
`cargo test -p repark-sql --lib`, `cargo test -p repark-spark --lib`,
clippy on both crates, `make verify`) are recorded below on the release
native with per-command summaries.

### C-006 registry rows

`docs/spark-sql-iceberg-parity.md`: BY NAME row (FIXED with pins) and the
RTAS operation row (BACKLOG, fork ask F-RTAS-OPS-1, pins named).
