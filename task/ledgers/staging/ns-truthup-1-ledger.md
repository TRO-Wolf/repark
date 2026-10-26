# Unit ledger — NS-TRUTHUP-1 · the four nanosecond registry rows re-measured on main, re-dated, and `DESCRIBE` shows the Iceberg spec names

**Date:** 2026-10-26 · **Branch:** `feat/ns-truthup-1` · **Base:** main `9fd60149`, merged at `0621df3d` · **Release:** 1.5.6.
**Order:** [task/wo/ns-truthup-1/order.md](../../wo/ns-truthup-1/order.md), grade C (adopted by the owner 2026-10-26 with the v3 label plan; decisions D1, D3 and D4). **Policy:** [../../../AGENTS.md](../../../AGENTS.md).
**Models:** builder `muse-spark-1.3-contributor` (probe, pins and the DESCRIBE renderer); clerk `claude-haiku-5-5` (registry rows, this ledger, maps); measurement re-run by the orchestrator.

**Scope.** The four registry rows the order's §2 names: R-010, R-013, R-018 part 1 and R-001. Out of scope: R-018 part 2, the array floor, the create door, R-012, R-024, and any change to a stored value. **This ledger closes when the unit's pull request merges.**

**Why.** The v3 core label (Tue 11-17) needs the nanosecond rows to leave EXPERIMENTAL. Four rows were measured on 2026-10-09, before the type planner and the floor landed; this unit measures them again on today's code, re-dates each one with the result, and, by owner decision D3, makes `DESCRIBE` show `timestamp_ns` and `timestamptz_ns`.

## PROPOSITION LEDGER — NS-TRUTHUP-1 — 2026-10-26

| Clause | Proposition (checkable) | Proof obligation | Verdict | Evidence / open question |
|---|---|---|---|---|
| C-001 | R-010: `UPDATE … SET`, `UPDATE … WHERE` and `DELETE … WHERE` lower `CAST(… AS timestamp_ns)` and `CAST(… AS timestamptz_ns)`. All twelve cells (two type names, three positions, UTC and America/New_York, format v3) store or match on main `9fd60149`; the example stored value is `1767323045123456789`. | The twelve `r010.*` cells of the probe, measured on head `056b9058` and re-run on main; the pin `test_update_and_delete_lower_the_nanosecond_cast_spellings` (twelve parameters). | **PROVEN** | §1 and §2. Evidence: `task/wo/ns-truthup-1/evidence/cells-2026-10-26.json`, twelve `stores`. Fixed by the type planner (U-1, #1082). |
| C-002 | R-013: the wall kernel `__repark_cast_timestamp_ns__` resolves in a `SELECT`, but the routine list does not answer on either Python door, so the row cannot be declared or closed. | The `r013.select` cell (answers `timestamp[ns]`); the `r013.routines` and `r013.show` cells (error: `information_schema.routines` and bare `SHOW FUNCTIONS` not found). | **OPEN** | Question: can the routine list be read on both doors, so the reserved names beside the kernel can be compared? Card SHOW-FUNCTIONS-1, [task/roadmap/mid-term/show-functions-1-card-2026-10-26.md](../../roadmap/mid-term/show-functions-1-card-2026-10-26.md). |
| C-003 | R-018 part 1: a `TIMESTAMP` literal inside `array(…)`, `named_struct(…)` or `map(…)` in a `VALUES` list answers as a bare statement; every `INSERT INTO … VALUES` and `INSERT OVERWRITE … VALUES` of it into a v3 table with a nanosecond leaf refuses with R-015's named class `INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST`, and no cell shows raw Arrow text. The six bare `VALUES` answers are the row's controls, recorded in the evidence file (cells `r018.bare.*`), not a claim; the claim is that every `INSERT` and `OVERWRITE` spelling takes R-015's named refusal, which R-015's test pins. | The eighteen `r018.*` cells (six `answers`, twelve `refuses`, zero raw Arrow text); R-015's Rust pin `every_door_refuses_a_nested_nanosecond_leaf_and_writes_nothing` on its `insert values` and `insert overwrite values` doors. | **PROVEN** | Orchestrator ruling 2026-10-26, measure before ruling: closed under R-015, no fold into NESTED-LITERAL-SEAM-1 (D4 not triggered). The six bare answers are pinned by no test (§4). |
| C-004 | R-001: `DESCRIBE TABLE` and `DESCRIBE TABLE EXTENDED` on a v3 table show `timestamp_ns` and `timestamptz_ns` for nanosecond columns; a v2 microsecond control keeps `timestamp_ntz` and `timestamp`. | The `r001.describe` and `r001.describe_extended` cells as measured before the fix (`timestamp_ntz`, `timestamp`); the pins `test_describe_shows_the_spec_names_for_nanosecond_columns` (two spellings, with the control) and `describe_shows_timestamp_ns_and_timestamptz_ns`. | **PROVEN** | Commit `543c9cbc`: `describe_ddl_type_name` in `crates/repark-spark/src/describe_show.rs`, one seam. Owner decision D3 (2026-10-26). Dated residue in §4. |

## 1. Measurement (C-001 to C-004)

- 39 cells, one object per cell, in `task/wo/ns-truthup-1/evidence/cells-2026-10-26.json`; the probe that records them is `task/wo/ns-truthup-1/probe_ns_truthup_1.py`.
- The builder's Muse MAX run on head `056b9058` and the orchestrator's re-run on code equal to main `9fd60149` give identical tallies: 12 `stores` (R-010); 13 `answers` (R-013: 1, R-018: 6, R-001: 6); 12 `refuses` (R-018, each the named R-015 class); 2 `error` (R-013).

## 2. Outcome per row (order §2 mapping)

- **R-010 FIXED** by the type planner (U-1, #1082): every spelling stores; the pin is the twelve cells of `test_ns_truthup_1.py`.
- **R-013 stays OPEN**, re-dated 2026-10-26: the kernel answers in a `SELECT`, the listing does not answer. The order's DECLARED branch is not reached; card SHOW-FUNCTIONS-1 is filed.
- **R-018 part 1 CLOSED under R-015** (orchestrator ruling 2026-10-26): the bare `VALUES` answers; every `INSERT` and `INSERT OVERWRITE` spelling refuses by R-015's named class; no raw Arrow text.
- **R-001 FIXED** for `DESCRIBE TABLE` and `DESCRIBE TABLE EXTENDED` (owner decision D3): the Iceberg spec names; the microsecond controls are unchanged.

## 3. Commits

- Builder: `a9c990cc` (probe and record), `312757ca` (the R-010 pin), `543c9cbc` (the DESCRIBE names).
- Clerk: `7a2661de` (the record scrubbed of local paths and the owner name), `ee99ae54` (card SHOW-FUNCTIONS-1), `09bcd81f` (the four registry rows), and the commit that files this ledger with its maps and pins.

## 4. Residue (dated, no new unit)

- `DESCRIBE <table> <column>` and the DESCRIBE partition section keep the microsecond names. `SHOW CREATE TABLE`, `df.schema`, `dtypes` and `printSchema` are unchanged by design: they carry the Arrow type.
- The six bare `VALUES` answers of C-003 are pinned by no test in the tree. The citation for C-003 covers the twelve refusals only.
- R-018 part 2 (a multi-row `map(…)` mixing a `TIMESTAMP_NTZ` literal and `to_timestamp_ntz(…)`, which panics in Arrow) was not re-measured; it keeps its 2026-10-09 reading.
