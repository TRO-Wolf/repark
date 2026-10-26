# NS-TRUTHUP-1 — the four nanosecond registry rows re-measured on today's main, re-dated, and DESCRIBE shows the spec names     grade: C   engine band: Muse MAX measures and builds, Haiku 5.5 clerk writes the rows, the orchestrator reproduces one cell per row, Muse MAX verifies   release: 1.5.6

**Status:** order, adopted by the owner 2026-10-26 with the v3 label plan (decisions D1, D3, D4). Written by the
orchestrator (Claude Fable 5.1) on 2026-10-26 from the plan, the 2026-10-20 priorities row 8 and the registry rows
named below, read on main `ab1b9bc8`. Measurement runs on the WRITE-SPINE-1 step 2 head and is reproduced on main
after that PR merges; the rows cite the merged sha.

## 0. Why, and what is out of scope

The v3 core label (Tue 11-17) needs the nanosecond rows to leave EXPERIMENTAL. Four rows were measured 2026-10-09,
before the type planner (U-1, PR #1082, 10-16) and the floor (U-3) landed, and still read as measured then. This unit
measures each again on today's code, re-dates it with the result, and, by owner decision D3, makes `DESCRIBE` show
the Iceberg spec type names for nanosecond columns. Spark cannot read or write these columns, so no Spark value exists
for any row; the oracle column stays "none" or "documented".

Out of scope, by name: any row not listed; the array floor (U-2, 1.5.7); the create door (#1048, 1.5.7); R-012 (U-4,
its own order); R-024; anything that changes a stored value.

## 1. Rulings already made

- Owner 2026-10-26 (v3 label plan): D1 the unit runs now, beside U-11; D3 `DESCRIBE` shows `timestamp_ns` /
  `timestamptz_ns`; D4 R-018 part 1, if still red, folds into NESTED-LITERAL-SEAM-1 and gets no unit here.
- Measure before ruling: no row changes until the orchestrator has re-run one of its cells on merged main.
- A silent wrong value disqualifies a label; a refusal with a hint does not (2026-10-20 priorities).
- Ruling 76(a): no hint or refusal text names a data-deleting action.

## 2. The rows and their cells

| Row | Claim as measured 2026-10-09 | Cells to run on today's head | Outcome mapping |
|---|---|---|---|
| ICE-TSNS-SQL-1-R-010 | `CAST(… AS timestamp_ns)` and `CAST(… AS timestamptz_ns)` inside `UPDATE … SET`, `UPDATE … WHERE` and `DELETE … WHERE` refuse `Unsupported SQL type` | both type names × the three positions × format v3 × zones UTC and America/New_York; the in-range value `'2026-01-02 03:04:05.123456789'` | stores or answers on every cell → FIXED (planner), pin = the `r010_answers` cells of `test_ice_tsns_merge_wall_1.py` or a new facade cell; any refusal → stays OPEN, card ICE-TSNS-CAST-SPELLING-2 |
| ICE-TSNS-SQL-1-R-013 | the strict-mode session lists `__repark_cast_timestamp_ns__` in `information_schema.routines` and resolves it by name | the `SELECT` of the name with one literal; the routines listing filtered to the `__repark_` prefix; `SHOW FUNCTIONS` | lists and resolves → DECLARED, dated row: harmless, reserved prefix, same as `__repark_cast_timestamp_ntz__` and `__repark_cast_map__`; the row states which other reserved names list beside it |
| ICE-TSNS-SQL-1-R-018 part 1 | a `TIMESTAMP '…'` literal inside `array(…)`, `named_struct(…)` or `map(…)` in a `VALUES` list fails with the raw Arrow `column types must match schema types` before any table is touched | the bare `VALUES` of each container; `INSERT INTO … VALUES` and `INSERT OVERWRITE … VALUES` of each into a v3 table with a nanosecond leaf; UTC and America/New_York | all store → FIXED with a facade pin; any raw Arrow text → stays OPEN with the dated note "folds into NESTED-LITERAL-SEAM-1 (D4)" and the exact cells listed for that unit |
| ICE-TSNS-SQL-1-R-001 | `DESCRIBE TABLE` on `(ts timestamp_ns, tz timestamptz_ns)` shows `timestamp_ntz` / `timestamp` | `DESCRIBE TABLE`, `DESCRIBE EXTENDED`, `SHOW COLUMNS`, `df.schema`, `df.printSchema()`, `spark.table(...).dtypes` on the same table, format v3 | measurement first; then step 3 changes the `DESCRIBE` family to show `timestamp_ns` / `timestamptz_ns`; the DataFrame schema surfaces are recorded, not changed (they carry the Arrow type, the row says which name they show) |

## 3. Files

- Measurement: a probe script beside this order, `task/wo/ns-truthup-1/probe_ns_truthup_1.py` (no comments, one-line
  docstrings), and its record `task/wo/ns-truthup-1/evidence/cells-2026-10-26.json` (one object per cell: row, cell
  id, statement, mode, zone, format, outcome, class, text, value).
- Step 3: the `DESCRIBE` renderer for Iceberg type names, found from `crates/repark-sql` and `crates/repark-iceberg`
  maps (the builder names the file in the sketch line of the hand-back; one file, one seam); Rust pin beside the
  nearest `DESCRIBE` test; facade pin in `python/repark/tests/timestamps/test_ice_tsns_sql_1.py`.
- Docs (clerk): the four rows in `docs/spark-sql-iceberg-parity.md` re-dated 2026-10-26 with the merged main sha;
  the 1.5.6 release-note draft lines; the card for any OPEN residue; `map.md` rows; a ledger
  `task/ledgers/staging/ns-truthup-1-ledger.md` with one row per registry row (PROVEN rows cite the pin).

## 4. Steps

0. **Measure** (Muse MAX, no commit): run every cell of §2 on the step-2 head, write the record, hand back the table.
1. **Reproduce** (orchestrator): one cell per row on merged main; any difference from step 0 re-runs step 0 on main.
2. **Rows and pins** (Haiku 5.5 clerk + Muse MAX for the pins): the outcome mapping of §2 applied row by row; red-first
   where a pin is new (the pin fails on the 10-09 claim's text, passes on the measured answer).
3. **DESCRIBE names** (Muse MAX, one commit): the renderer shows `timestamp_ns` / `timestamptz_ns`; microsecond columns
   unchanged (control cells on a v2 table and on `timestamp` / `timestamp_ntz` columns); pins red first.
4. **Records** (clerk): rows, ledger, maps, release-note lines, card for residue.

## 5. Gates

`make rust-clippy rust-panic-ban check-ledger-grammar check-docs-links`, `python3 scripts/sync_map_md.py --check`,
`scripts/check_map_md.sh --base`, the comment grep empty, file sizes clean, `make py-lint py-format-check`; the
facade files touched; CI green before the verifier; one Muse MAX verifier round on the cells and the DESCRIBE pins.

## 6. Halt rules

Stop and hand back on: a cell where today's head stores a value different from the 10-09 record (a value moving is
the owner's first); the DESCRIBE change needing more than one file or touching the DataFrame schema surfaces; a
refusal text that names a data-deleting action; a cost median above 1.05 on the read set if the renderer sits on a
hot path.

## 7. Hand-back

One JSON: `model_id`, `status`, `head`, `commits`, `cells` (path, counts per row and outcome), `rows` (id → outcome
proposed), `describe` (file, seam, pins), `gates`, `halts`, `questions`, `out_of_scope_observed`, `hours_used`.

## 8. Steps 1 to 3 outcome (2026-10-26)

- **Builder commits** (`muse-spark-1.3-contributor`): `a9c990cc` (the probe and its record), `312757ca` (the R-010 pin: the twelve cells of `test_ns_truthup_1.py`), `543c9cbc` (the DESCRIBE names).
- **Re-run tally** (orchestrator, on code equal to main `9fd60149`): 39 cells, identical to the builder's run on head `056b9058`: 12 `stores` (R-010), 13 `answers` (R-013: 1; R-018: 6; R-001: 6), 12 `refuses` (R-018, each the named R-015 class), 2 `error` (R-013).
- **Step 1, scrub** (clerk `7a2661de`): the recorder reads the repo-relative probe path, the warehouse prefix reads `<warehouse>`, the owner reads `<owner>`. The probe needed no change: its recorder was already repo-relative and it carries no local path or owner text.
- **Step 2, rows** (clerk `09bcd81f`, with the card `ee99ae54`): R-001 FIXED and R-010 FIXED, both re-dated 2026-10-26; R-013 OPEN, re-dated, blocked by card SHOW-FUNCTIONS-1; R-018 part 1 CLOSED under R-015, part 2 not re-measured. The builder's notes lean (R-018 stays OPEN) is superseded by the orchestrator's ruling of the same day.
- **Step 3, ledger** (clerk `f9ed1afb`): C-001 (R-010) PROVEN; C-002 (R-013) OPEN; C-003 (R-018 part 1) PROVEN; C-004 (R-001) PROVEN. Pins sit in the map of each pin's directory: `python/repark/tests/timestamps` (C-001, C-004), `crates/repark-spark/src/describe_show` (C-004), `crates/repark-spark/tests` (C-003, R-015's refusal test).
- **DESCRIBE renderer** (`543c9cbc`): one file, `crates/repark-spark/src/describe_show.rs`, one wrapper `describe_ddl_type_name` over the shared `spark_ddl_type_name`. The two DESCRIBE TABLE and DESCRIBE EXTENDED call sites route through it. The shared helper stays for its other callers (the nested-column and INVALID_EXTRACT texts, the partition append cast, `SHOW CREATE TABLE`, the view DDL, the void-type refusals), so `df.schema`, `dtypes`, `printSchema`, `SHOW CREATE TABLE` and every error text are unchanged.
- **Residue** (dated, one line in the R-001 row, no new unit): `DESCRIBE <table> <column>` and the DESCRIBE partition section keep the microsecond names.
- **Unpinned:** the six bare `VALUES` answers of R-018 part 1 have no test in the tree; C-003 cites R-015's refusal test only.
