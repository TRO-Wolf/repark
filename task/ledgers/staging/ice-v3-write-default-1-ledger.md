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
| C-001 | `repro_before_fix`: `p_write_default.py` on the unfixed tree shows INSERT / MERGE column lists writing NULL for omitted `c` and `DataFrame` append refusing `missing from the DataFrame: ['c']`. | Regenerate the source table with `p_v3_types_defaults.py` via `/tmp/ib-scratch/run-probe.sh`; run `p_write_default.py`; paste the output. | **PROVEN** | Source table regenerated (`/tmp/oc-worker/jb-jvm.sh /tmp/ib-scratch/run-probe.sh p_v3_types_defaults.py`, exit 0; schema carries `initial-default: 5, write-default: 5` on `c`; Spark reads pre-add rows as 5; RePark omitted-`c` insert reads back `(6, 'f', None)`). Repro on this tree's release native (`.venv/bin/python /tmp/ib-scratch/probes/p_write_default.py`, no JVM): `INSERT (id, name)` writes `(7, 'g', None)`; MERGE NOT MATCHED writes `(13, 'm', None)`; `writeTo`/`saveAsTable` append refuse `missing from the DataFrame: ['c']`; positional-short and SELECT-short refuse at planning (`Inconsistent data length`, `Column count doesn't match`); `DEFAULT` keyword refuses (`No field named default`). pins: ice-v3-write-default-1/C-001. |
| C-002 | `spark_oracle_recorded`: the Java-API-created v3 table ships as a checked-in fixture with relative-path-safe adoption, plus a truth JSON of Spark's answer for every cell, recorded by a script checked in beside it. | Fixture directory + truth JSON + recording script, following the `ice_spark_table_1` / `test_ice_spark_table_1.py` copy-and-register pattern. | **PROVEN** | `python/repark-parity/fixtures/torture/data/ice_v3_write_default_1/`: seven v3 tables (225,094 bytes, `.crc` stripped), `truth.json` (banner `spark=4.1.2 tz=UTC`, per-table schema plus seed outcome, 22 cells), `record.py` (Spark-only, pins C-002), `map.md`. Recorded 2026-09-17 on live PySpark 4.1.2 + Iceberg 1.11.0; adoption copies to the baked-in canonical `/tmp/repark-ice-v3-write-default-1/ns/<table>` under a lock. pins: ice-v3-write-default-1/C-002. |
| C-003 | `pins_red_first`: `python/repark/tests/test_ice_v3_write_default_1.py` (offline tier against the fixture, live tier under `REPARK_PARITY_LIVE=1`) and the Rust fill unit tests fail on the unfixed tree. | Run the new pins on the unfixed tree; paste failures. | **PROVEN** | `## Red first` below (8 failed on the unfixed tree); the live cell never ran red-first (it was skipped offline) and its hand-written two-row expectation was repaired to the measured three rows in round 2 (audit class: stale pin, `150a820a`). |
| C-004 | `insert_column_list_fills`: `INSERT INTO t (id, name)` on both SQL doors fills omitted `c` from `write_default` (NULL only when the field has none); a required column with no write-default keeps Spark's error. | Offline + live pins green; Rust unit tests for the fill green. | **PROVEN** | Offline `14 passed, 1 skipped`; live `15 passed` (round 2, release native); ANSI-door Rust pins `crates/repark-sql/tests/ansi_write_defaults.rs` `2 passed` (the ANSI fill had code but no pin until round 2 — found in step 5). Required-missing refuses `non-nullable but contains null` on both doors. |
| C-005 | `merge_not_matched_fills`: `MERGE … WHEN NOT MATCHED THEN INSERT (id, name)` fills omitted `c` from `write_default` on both SQL doors. | Offline + live pins green. | **PROVEN** | Offline + live green (same runs as C-004); Rust `insert_fill.rs` fill + required pins green (`cargo test -p repark-iceberg --lib` in step 7). |
| C-006 | `dataframe_writers_fill`: `writeTo(t).append()` and `saveAsTable(append)` with `c` missing fill from `write_default` instead of refusing; `insertInto` stays positional (no column list) and refuses a short frame, as Spark does — corrected 2026-09-17 per the logic review (L-02): the registry and this clause wrongly listed `insertInto` as a fill door while Spark (`insertInto_missing`: `INSERT_COLUMN_ARITY_MISMATCH.NOT_ENOUGH_DATA_COLUMNS`) and the pin (`test_insert_into_and_extra_column_refuse`: `Column count doesn't match`) refuse. | Offline + live pins green. | **PROVEN** | Offline + live green (same runs as C-004); `check_lib_py.py` ratchets `writer_readwriter.py` 1101 → 1095. |
| C-007 | `other_shapes_match_spark`: positional-short `INSERT INTO t VALUES (8, 'h')`, `INSERT INTO t SELECT 9, 'i'`, `VALUES (10, 'j', DEFAULT)`, `saveAsTable` append, and `df.write.insertInto` each give Spark's measured answer (fill or error). | Truth JSON cell per shape; pins assert each. | **PROVEN** | Offline + live green (same runs as C-004), including the Spark-door `overwrite_column_list` cell. |
| C-008 | `type_fidelity`: the default literal casts to the field type exactly; coverage is int, string, and one decimal (plus date, timestamp, binary defaults if the spec allows them). | Pins per covered type. | **PROVEN** | `string_default_insert`, `decimal_default_insert`, `temporal_default_insert` cells green offline (no binary-default cell exists — the registry NESTED row claims int/string/decimal/temporal only). |
| C-009 | `nested_struct_default`: one nested struct-field default measured against Spark; filled, or DECLARED with a registry row if the fix is not small. | Measurement pasted; fix or DECLARED row. | **PROVEN** | DECLARED arm taken: `ICE-V3-WRITE-DEFAULT-1-NESTED` row (2026-09-17) — only primitive literals fill; struct-field defaults unpinned and unmeasured on both engines. pins: ice-v3-write-default-1/C-009. |
| C-010 | `no_default_unchanged`: a table without any defaults behaves exactly as before. | Pin green. | **PROVEN** | Offline green (same run as C-004). |
| C-011 | `v2_unchanged`: a v2 table behaves exactly as before. | Pin green. | **PROVEN** | Green in the offline run, the live run, and the whole-facade run (no `test_ice_v3_write_default_1` failure there). |
| C-012 | `write_default_differs`: write-default 7 with initial-default 5 (Java API `updateColumnDefault` / `UpdateSchema`) — old rows read 5, new omitted writes fill 7. | Pin green. | **PROVEN** | Green in the offline run, the live run, and the whole-facade run (no `test_ice_v3_write_default_1` failure there). |
| C-013 | `registry_rewritten`: the V3-6 / write-default rows in `docs/spark-sql-iceberg-parity.md` state the measured truth (dated 2026-09-16, ICE-V3-WRITE-DEFAULT-1, FIXED with the pins; DECLARED rows for anything left, with Spark's shape); `test_rp3_c009_write_default.py` still describes the contract. | Registry diff; guard test disposition recorded. | **PROVEN** | Four rows landed 2026-09-17 (§7): FIXED `ICE-V3-WRITE-DEFAULT-1`, DECLARED `ICE-V3-WRITE-DEFAULT-1-OVERWRITE-PART` (with Spark's measured partition shapes) and `ICE-V3-WRITE-DEFAULT-1-NESTED`, BACKLOG `F-001`. No V3-6/write-default row existed before — these are new. The C-009 guard still describes the contract: the new code only READS defaults (setter needles `with_write_default` / `write_default(` absent from all touched files, grepped 2026-09-17); full guard run in step 7. |
| C-014 | `gates_green`: the new test file offline and live, `cargo test -p repark-iceberg --lib`, `cargo test -p repark-spark --lib`, `make verify`, the whole facade suite, and the whole parity suite are green on the release native. | Counts in this ledger. | **PROVEN** | Unit pins offline `14 passed, 1 skipped`, live `15 passed` (release native); `repark-iceberg --lib` 442 passed; `repark-spark --lib` 1051 passed, 4 ignored; `repark-sql` all targets exit 0 (342 lib + integration incl. 2 new ANSI pins); `insert_fill` struct pin green; `uvx ruff check .` + `format --check .` clean; `make verify` exit 0. Whole facade (`/tmp/oc-worker/jb-wd/facade-r2.log`): 9342 passed, 369 skipped, 26 xfailed, 3 failed — each dispositioned: the stale saveAsTable-missing-column refusal retired to the measured NULL fill (`0d273199`, green on rerun), the insertInto-missing-table `TableNotFound` leak fixed at the root (`rewrite_insert_markers` passthrough, `test_missing_table_text` green on rerun), the sort-pool OOM is load-induced (passes alone and 18/18 as a file). Whole parity (`/tmp/oc-worker/jb-wd/parity-r2.log`): 756 passed, 2 skipped, 12 xfailed, 1 failed — the CAP-1 mirror row for `writer_readwriter.py` ratcheted 1101 → 1095, `23 passed` on rerun. |

VERDICT: 14 clauses, 14 PROVEN, 0 OPEN, 0 REJECTED.

## Red first

C-001 red is the matrix in `## Evidence` below (unfixed tree, release native
`.venv`, 2026-09-17): every FILL cell reads NULL or refuses where Spark fills.

C-003 red (2026-09-17, unfixed tree, `.venv/bin/python -m pytest
python/repark/tests/test_ice_v3_write_default_1.py -q -p no:cacheprovider`):
8 failed, 6 passed, 1 skipped in 1.22s. The 8 failures are exactly the FILL
cells (`insert_column_list`, `merge_not_matched`, `dataframe_writers`,
`default_keyword`, `no_default` DEFAULT-keyword leg, `string_temporal_differ`,
`decimal`, `overwrite_column_list` — NULL or `No field named default` /
`missing from the DataFrame` where Spark fills). The 6 passes are the
already-correct shapes (seed, short-insert refusals, `insertInto`/extra-column
refusals, explicit NULL, required refusal, v2 control). Live skipped
(`REPARK_PARITY_LIVE` unset). pins: ice-v3-write-default-1/C-003.

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

## Findings (out of scope, observed)

- F-001 (2026-09-17, read path, fork-or-adoption attribution open): a table
  whose head is a schema-only commit (Java `addColumn` with defaults, no
  snapshot after) reads pre-add rows as NULL on this engine while Spark 4.1.2
  reads the initial default. One RePark write (new snapshot on the post-add
  schema) flips the old rows to the default. Measured on the `defaults`
  fixture: adopted at v3 (snapshot schema 0) reads `(1, 'a', None)`; after
  `INSERT INTO t VALUES (99, 'z', 7)` the same rows read `(1, 'a', 5)`. The
  fixture works around it with one post-add Spark seed row per defaulted
  table. Untouched by this unit (write paths only); needs attribution before
  any fix. pins: none (finding, no clause).
- F-002 (2026-09-17, Spark behavior): `df.write.mode("overwrite").saveAsTable`
  on an existing table REPLACES it (2-column result), while this engine
  generates `INSERT OVERWRITE` keeping the schema — pre-existing divergence,
  untouched. pins: none (finding, no clause).
- F-003 (2026-09-17, round 2 step 5, DECLARED as
  `ICE-V3-WRITE-DEFAULT-1-OVERWRITE-PART`): partition-overwrite shapes do not
  fill omitted defaulted columns. Spark measured (banner `spark=4.1.2`,
  partitioned v3 table, Java-API default 5, probe `/tmp/ib-scratch/probes/p_write_default_ow.py`):
  `INSERT OVERWRITE t (id, name) VALUES (10, 'x')` replaces the table with
  `(10, 'x', 5)`; `df.select("id", "name").writeTo(t).overwritePartitions()`
  replaces only source partitions, filling `c = 5`
  (`[[10, 'x', 5], [20, 'y', 5]]`). RePark measured (release native, adopted
  fixture): `overwritePartitions` raises `ParseException: Expected: an
  expression, found: )` from the generated text — loud but wrong-shaped; both
  doors' PARTITION arms stage positionally with no fill (`*_overwrite.rs`
  `execute_partition_overwrite` has no `insert_defaults` call). ANSI
  whole-table `INSERT OVERWRITE` stays Q9-omitted (row DML-1, by ruling).
  Filling needs all four partition arms plus a partitioned defaulted fixture:
  not small, so DECLARED, not fixed.
- F-004 (2026-09-17, round 2 step 5): the ANSI-door `INSERT` fill had code
  (`router.rs` `delegate` → `fill_insert_plan`) but no pin on either level —
  found by reading the dispatch, fixed the same morning with
  `crates/repark-sql/tests/ansi_write_defaults.rs` (`2 passed`: fill
  `(4, 'd', 5)` plus the Spark-door-identical required refusal). The facade
  pin file drives the Spark door only (`ReparkSession.sql` is the Spark door;
  the ANSI door is top-level `repark.sql()` on the native session, which
  cannot adopt a defaulted table in-probe).

## Round-4 notes (2026-09-17, run 20b)

- Ruling Q-20b-7: this unit goes up as a DRAFT PR today; the branch stays in
  the cleanest committable state (one slice per commit, gates green).
- Remediated this round: the 14 added `///` lines are gone (exported `Result`
  entry points carry `#[allow(clippy::missing_errors_doc)]`, error contract in
  `write/map.md`); L-02 corrected (registry FIXED row + C-006 name `insertInto`
  with Spark's `insertInto_missing` refusal); R-01/R-02 fixed
  (`query_has_default_marker` probes the AST before any catalog load,
  `MarkerRewrite` threads the loaded table into `fill_insert_plan`, both doors
  updated, load-count pins in `insert_defaults/tests.rs`, tests split to
  `insert_defaults/tests.rs` + `insert_defaults/map.md` for the file-size
  ceiling).
- Open review items with dispositions (no code change this round):
  - L-01 OPEN: the dynamic PARTITION arm with a column list maps by name and
    null-fills unlisted nullable fields where Spark fills the write-default;
    the Spark door shape is a `ParserError` from the generated text.
    Disposition: needs re-scoping — parser support with a Spark cell
    (F-003 shapes), or a declared row.
  - L-03 OPEN: Spark-door `INSERT OVERWRITE … DEFAULT` never reaches
    `rewrite_insert_markers` (overwrite intercepts first) and the overwrite
    fill only appends omitted columns, so a present `DEFAULT` token fails.
    Disposition: fold `DEFAULT` into `overwrite_source_with_default_fills`
    (or call the marker pass from the overwrite path) and pin both shapes.
  - L-04 OPEN: `saveAsTable(mode=overwrite)` now fills omitted defaults with
    no Spark cell; the unit's own F-002 measurement says Spark replaces.
    Disposition: record a Spark Iceberg cell (fill vs replace) and pin it, or
    DECLARED next to F-002.
  - R-03 OPEN (P3): MERGE `table_projection` reconverts a schema
    `execute_merge` already holds. Disposition: pass the existing
    `write_schema` plus a once-per-MERGE `ColumnDefaults` into
    `insert_projection_with_defaults`.
  - R-04 OPEN (P3): `overwrite_source_with_default_fills` walks the schema on
    every column-list OVERWRITE even when it adds no fills. Disposition: a
    `write_default.is_none()` pre-scan before the Arrow conversion, as R-02.

## Round-2 notes (2026-09-17, run 20b)

- Step-1 comment grep over `git diff --cached` shows only the gate-required
  `/// # Errors` docstrings on the new `pub` fns (workspace clippy pedantic
  `missing_errors_doc`; AGENTS.md keeps required docstrings). Zero
  explanatory comments. Kept by contract precedence, disclosed here.
- Live-cell repair (`150a820a`, audit class stale pin): the live Spark table
  carries three rows after its own column-list insert fills `c = 5`; the
  adoption assertion now expects all three and the RePark replay inserts
  `(4, 'd')` for the fill. The two-row text was hand-written and never ran
  live (the cell skips without `REPARK_PARITY_LIVE=1`).
- Rebase onto `origin/main` (`225f68ee`): clean, `a003f9f5` skipped as
  predicted, no conflicts, no markers.
- Step-5 probes: `/tmp/ib-scratch/probes/p_write_default_ow.py` (live Spark)
  and `/tmp/p_repark_ow_probe.py` (release native, offline). Fixture metadata
  embeds absolute canonical paths — a probe must materialize at
  `/tmp/repark-ice-v3-write-default-1/ns`, never a private path.

## Build notes (2026-09-17, implementation round)

- Recovered the pre-compaction design from the working-tree diff after the
  compacted summary's file text proved stale: `insert_defaults` exposes
  `column_defaults`/`ColumnDefaults` (MERGE), `insert_column_list`,
  `rewrite_insert_markers` (DEFAULT keyword), `fill_insert_plan` (NULL-pad
  replacement). Reimplemented against DF54 (`Expr::Literal(_, None)`,
  `insert_to_plan` pads omitted columns as `Cast(Literal(Null))`) and the fork
  at `75da2b58` (`PrimitiveLiteral`, `write_default: Option<Literal>`).
- DAG adaptation: `repark-iceberg` cannot take `repark-core` (`check_crate_dag`
  allows core→iceberg only), so the doors resolve `(catalog, TableIdent)` via
  new pure helpers `insert_target` / `dml_target` and pass them in. ANSI door
  plans from SQL text, so `rewrite_insert_markers` returns the rewritten SQL
  for `delegate`; the Spark door plans the mutated statement.
- DataFrame writers funnel through generated SQL: `_by_name_projection` now
  emits a target column list omitting missing columns, so
  `INSERT INTO t (id, name) SELECT ...` reaches `fill_insert_plan`; `insertInto`
  stays positional and refuses; extra columns still refuse in Python.
- C-009 guard (`test_rp3_c009_write_default.py`) scans non-test `.rs` for
  `with_write_default` / `write_default(`: product code reads the field via
  `match &field.write_default` (no needle); schema-building tests live in
  `merge/tests/` (exempt path). Disposition: guard untouched, still green.
- Size gates: `merge/mod.rs` 1792 and `merge/tests/merge.rs` 1065 held exact by
  keeping the `insert_sql(index, table)` shape and moving the fill pins to new
  `merge/tests/insert_fill.rs`; `writer_readwriter.py` 1101→1095 via ordinary
  ratchet-down (dead missing-refusal detail removed).
