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

PROVEN — scoped to Iceberg. The Spark door creates only Iceberg tables:
`CREATE TABLE … USING parquet` is refused before any write path, and a
parquet file registered with `read.parquet` is a read-only view, so there is
no non-Iceberg table kind the SQL door can INSERT into. The BY NAME rewrite
resolves through the target's planned schema, so a future writable table kind
inherits it; until one exists there is nothing else to pin.

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
`[('Di','Y',5)]`; `by_name_dup` (two `first_name`) → same
TOO_MANY_DATA_COLUMNS; `positional` stays positional
(`[('Smith','Ann',1)]`); `by_name_values` →
`[INCOMPATIBLE_DATA_FOR_TABLE.EXTRA_COLUMNS] … extra columns
col1, col2, col3 … SQLSTATE KD000`; `overwrite_by_name` →
`INSERT OVERWRITE … BY NAME …` leaves `[('P','O',8)]`.
`rtas_ops`: `ctas_then_rtas` → `[append, overwrite]`; `rtas_new_table` →
`[overwrite]`; `rtas_empty_new` → `[delete]`; `rtas_empty_twice` →
`[delete, delete]`, with the full Java summaries in the fixture. Spark basis:
PySpark 4.1.2 + iceberg-spark-runtime-4.1_2.13:1.11.0 (GAV from
`python/repark/tests/_oracle_pins.py`, never restated), recorded 2026-09-16
by the orchestrator probe and re-derived by the committed generator.

### Gates

Gates for step 5 (`test_ice_rtas_byname_1.py`, `test_insert_store_assign.py`,
`test_sql_harden_cutover.py`, `test_writer_v2.py`,
`cargo test -p repark-sql --lib`, `cargo test -p repark-spark --lib`,
clippy on both crates, `make verify`) are recorded below on the release
native with per-command summaries.

### C-006 registry rows

`docs/spark-sql-iceberg-parity.md`: BY NAME row (FIXED with pins) and the
RTAS operation row (BACKLOG, fork ask F-RTAS-OPS-1, pins named).
