# Unit ledger — ICE-V3-WRITE-DEFAULT-1 · omitted columns fill from `write_default` on every write path

**Unit:** ICE-V3-WRITE-DEFAULT-1 round 1 · **Date:** 2026-09-17 · **Branch:** `fix/ice-v3-write-default-1` · **Base:** `chore/fork-pin-ice-19b` head (fork pin `75da2b58`, RP-21 / PR #665)
**Model:** muse-spark-1.3-contributor
**Policy:** [AGENTS.md](../../../AGENTS.md). **Path:** STANDARD. **risk_tier: standard.**

**Why now.** A format-v3 table created by Spark, with column `c INT` added through
the Java API carrying `initial-default: 5` and `write-default: 5`, fills 5 on
every Spark write door when the column is omitted. RePark writes NULL (INSERT /
MERGE column lists) or refuses (`DataFrame` append: `missing from the DataFrame:
['c']`). Every RePark write path that builds rows for an Iceberg table must fill
an omitted column from the current schema field's `write_default` in Rust, in the
shared write-projection step both SQL doors and the DataFrame writers reach.

**Retires:** this ledger moves to `../completed/` when the unit's last commit
lands.

**Not in this step:** conflict detection, the DML planner's scan (run 19a owns
those); any fork pin move; `STATUS.md`; any JVM outside the one fixture-recording
step.

## Rulings carried in

- Q-19b-4: fix scope is every RePark write path that builds rows for an Iceberg
  table, filling from the field's `write_default`, in Rust, in the shared
  write-projection step.
- Owner, jb-common.md: no comments in code; Rust first; branch commits carry
  `Authored-By: Muse Spark (muse-spark-1.3-contributor) <noreply@meta.ai>` as the
  last line; work COMMITTED by 13:30 EDT 2026-09-17; JVM only through
  `/tmp/oc-worker/jb-jvm.sh`, one at a time.

## PROPOSITION LEDGER — ICE-V3-WRITE-DEFAULT-1 — 2026-09-17

| Clause | Proposition (checkable) | Proof obligation | Verdict | Evidence |
|---|---|---|---|---|
| C-001 | `repro_before_fix`: `p_write_default.py` on the unfixed tree shows INSERT / MERGE column lists writing NULL for omitted `c` and `DataFrame` append refusing `missing from the DataFrame: ['c']`. | Regenerate the source table with `p_v3_types_defaults.py` via `/tmp/ib-scratch/run-probe.sh`; run `p_write_default.py`; paste the output. | **PROVEN** | Source table regenerated (`/tmp/oc-worker/jb-jvm.sh /tmp/ib-scratch/run-probe.sh p_v3_types_defaults.py`, exit 0; schema carries `initial-default: 5, write-default: 5` on `c`; Spark reads pre-add rows as 5; RePark omitted-`c` insert reads back `(6, 'f', None)`). Repro on this tree's release native (`.venv/bin/python /tmp/ib-scratch/probes/p_write_default.py`, no JVM): `INSERT (id, name)` writes `(7, 'g', None)`; MERGE NOT MATCHED writes `(13, 'm', None)`; `writeTo`/`saveAsTable` append refuse `missing from the DataFrame: ['c']`; positional-short and SELECT-short refuse at planning (`Inconsistent data length`, `Column count doesn't match`); `DEFAULT` keyword refuses (`No field named default`). |
| C-002 | `spark_oracle_recorded`: the Java-API-created v3 table ships as a checked-in fixture with relative-path-safe adoption, plus a truth JSON of Spark's answer for every cell, recorded by a script checked in beside it. | Fixture directory + truth JSON + recording script, following the `ice_spark_table_1` / `test_ice_spark_table_1.py` copy-and-register pattern. | **OPEN** | — |
| C-003 | `pins_red_first`: `python/repark/tests/test_ice_v3_write_default_1.py` (offline tier against the fixture, live tier under `REPARK_PARITY_LIVE=1`) and the Rust fill unit tests fail on the unfixed tree. | Run the new pins on the unfixed tree; paste failures. | **OPEN** | — |
| C-004 | `insert_column_list_fills`: `INSERT INTO t (id, name)` on both SQL doors fills omitted `c` from `write_default` (NULL only when the field has none); a required column with no write-default keeps Spark's error. | Offline + live pins green; Rust unit tests for the fill green. | **OPEN** | — |
| C-005 | `merge_not_matched_fills`: `MERGE … WHEN NOT MATCHED THEN INSERT (id, name)` fills omitted `c` from `write_default` on both SQL doors. | Offline + live pins green. | **OPEN** | — |
| C-006 | `dataframe_writers_fill`: `writeTo(t).append()`, `saveAsTable(append)`, and `insertInto` with `c` missing fill from `write_default` instead of refusing. | Offline + live pins green. | **OPEN** | — |
| C-007 | `other_shapes_match_spark`: positional-short `INSERT INTO t VALUES (8, 'h')`, `INSERT INTO t SELECT 9, 'i'`, `VALUES (10, 'j', DEFAULT)`, `saveAsTable` append, and `df.write.insertInto` each give Spark's measured answer (fill or error). | Truth JSON cell per shape; pins assert each. | **OPEN** | — |
| C-008 | `type_fidelity`: the default literal casts to the field type exactly; coverage is int, string, and one decimal (plus date, timestamp, binary defaults if the spec allows them). | Pins per covered type. | **OPEN** | — |
| C-009 | `nested_struct_default`: one nested struct-field default measured against Spark; filled, or DECLARED with a registry row if the fix is not small. | Measurement pasted; fix or DECLARED row. | **OPEN** | — |
| C-010 | `no_default_unchanged`: a table without any defaults behaves exactly as before. | Pin green. | **OPEN** | — |
| C-011 | `v2_unchanged`: a v2 table behaves exactly as before. | Pin green. | **OPEN** | — |
| C-012 | `write_default_differs`: write-default 7 with initial-default 5 (Java API `updateColumnDefault` / `UpdateSchema`) — old rows read 5, new omitted writes fill 7. | Pin green. | **OPEN** | — |
| C-013 | `registry_rewritten`: the V3-6 / write-default rows in `docs/spark-sql-iceberg-parity.md` state the measured truth (dated 2026-09-16, ICE-V3-WRITE-DEFAULT-1, FIXED with the pins; DECLARED rows for anything left, with Spark's shape); `test_rp3_c009_write_default.py` still describes the contract. | Registry diff; guard test disposition recorded. | **OPEN** | — |
| C-014 | `gates_green`: the new test file offline and live, `cargo test -p repark-iceberg --lib`, `cargo test -p repark-spark --lib`, `make verify`, the whole facade suite, and the whole parity suite are green on the release native. | Counts in this ledger. | **OPEN** | — |

VERDICT: 14 clauses, 0 PROVEN, 14 OPEN, 0 REJECTED.

## Red first

C-001 red is the matrix in `## Evidence` below (unfixed tree, release native
`.venv`, 2026-09-17): every FILL cell reads NULL or refuses where Spark fills.

## Evidence

Spark oracle (live PySpark 4.1.2 + Iceberg 1.11.0, banner `spark=4.1.2 tz=UTC`,
`/tmp/ib-scratch/wh/p_wd_oracle` + `p_wd_oracle3.py`, truth at
`/tmp/ib-scratch/wh/wd_oracle/truth.json`, 20 cells). RePark column is this
tree unfixed (`/tmp/wd_repark_matrix.py`). `FILL(x)` = writes x; both refuse =
parity of refusal with different text.

| Shape | Spark 4.1.2 | RePark unfixed |
|---|---|---|
| INSERT column list omits `c` | FILL(5): `(3, 'c', 5)` | NULL: `(103, 'rc', None)` |
| MERGE NOT MATCHED omits `c` | FILL(5): `(4, 'd', 5)` | NULL: `(104, 'rd', None)` |
| `writeTo(t).append()` missing `c` | FILL(5): `(5, 'e', 5)` | refuses `missing from the DataFrame: ['c']` |
| `saveAsTable` append missing `c` | FILL(5): `(12, 'l', 5)` | refuses `missing from the DataFrame: ['c']` |
| `df.write.insertInto` missing `c` | refuses `INSERT_COLUMN_ARITY_MISMATCH.NOT_ENOUGH_DATA_COLUMNS` | refuses (`Column count doesn't match`) |
| positional-short `VALUES (8, 'h')` | refuses `NOT_ENOUGH_DATA_COLUMNS` | refuses (`Inconsistent data length`) |
| `INSERT SELECT 9, 'i'` short | refuses `NOT_ENOUGH_DATA_COLUMNS` | refuses (`Column count doesn't match`) |
| `VALUES (10, 'j', DEFAULT)` | FILL(5) | refuses (`No field named default`) |
| explicit NULL full-width | NULL (`(14, 'n', None)`) | NULL |
| no-default nullable omitted (SQL + writeTo) | NULL | SQL NULL; writeTo refuses `missing` |
| string default `'hi'` | FILL(`'hi'`, incl. pre-add row) | NULL on new row |
| decimal default `3.14` | write OK; Spark read of the filled row ERRORS (`Cannot cast default value to long: 3.14`; physical parquet carries `3.14`) | NULL on new row |
| date `2024-10-04` + timestamptz defaults | FILL both | NULL, NULL on new row |
| write-default 7 / initial-default 5 | old row reads 5, new omitted write 7 | new row NULL |
| required `req INT NOT NULL`, no default | refuses both doors (`INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_FIND_DATA`, `Cannot find data for the output column`) | INSERT refuses (Arrow `non-nullable but contains null`, pre-commit); writeTo refuses `missing` |
| v2 table + Java `addColumn` default | refused (`Invalid schema for v2: non-null default not supported until v3`) | n/a (no v2 table can carry a default) |
| Java `addRequiredColumn` no default | refused (`cannot add required column without a default value`) | n/a |

Plan ground truth (EXPLAIN on this tree): column-list omit plans
`Projection: column1 AS id, column2 AS name, Int32(NULL) AS c`; explicit
full-width NULL plans `column3 AS c` (source ref); full-width
`SELECT …, NULL` plans a bare `Int32(NULL) AS c` — identical to the
synthesized fill, so the fix must know the statement column list (AST), a bare
plan literal is not enough. Plain INSERT executes fork `IcebergWriteExec`
(DataFusion `insert_to_plan` → fork `insert_into`); the fork's
`apply_write_defaults` fills only MISSING columns, so an explicit NULL never
fills there. MERGE NOT MATCHED null-fills in RePark-owned
`write/merge/insert.rs::insert_projection` (`NULL AS c`).

## Open questions

—
