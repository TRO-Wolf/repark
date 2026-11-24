# TEMP-VIEW-SESSION-SETTINGS-1 part 1 — the bar (head against main and Spark 4.1.2)

**Head:** product commit `12c0680c` on `fix/temp-view-session-settings-1-part1`; its product tree equals the
scratch commit `c7fc64c6` the measured package was built from (`git diff --name-only c7fc64c6 12c0680c -- crates
python/repark/src Cargo.toml Cargo.lock` lists one `map.md`). **Main:** `6f80793b`, measured with the package of
`27d6149f` (product tree equal). **Spark:** 4.1.2, Zulu 17, the Iceberg runtime jar as `spark.jars` only. Measured
2026-11-18 between 00:19 and 00:42 UTC (inside the window where the two test zones' dates differ), every run under
the shared measure lock with `TZ=UTC`, no `-I`, each RePark run printing the package it loaded.

## What ran

- **The step 0's domain table, with its own probes** (`../probes/run.py` both start modes, `../probes/rev.py`) on main
  and the head, paired with the step 0's Spark files for `run.py` and a fresh Spark run for `rev.py` (its current-time
  cells depend on the hour). **The re-critic's cells** (`probes/rc.py`, its 44 filter cells and the rest) on all three,
  fresh.
- **2,772 paired cells:** 2,495 `run.py` (start ANSI on), 32 `run.py` unset, 156 `rev.py`, 89 `rc.py`.
  [probes/bar.py](probes/bar.py) pairs them, gives each a scope by door and the step 0's verdict classes, and lists
  every move; [probes/target.py](probes/target.py) checks part 1's 375 view gaps. Raw: [raw/](raw/).

## Counts per verdict

| build | EQUAL | BOTH_RAISE | BOTH_RAISE_OTHER | VALUE | REPARK_ANSWERS | REPARK_RAISES |
|---|---|---|---|---|---|---|
| main | 1,260 | 169 | 416 | 407 | 199 | 321 |
| head | 1,563 | 225 | 437 | 206 | 122 | 219 |

| scope | stay equal | WRONG → EQUAL | EQUAL → RAISE (ruled class) | EQUAL → WRONG | stay non-equal |
|---|---|---|---|---|---|
| part 1 (SQL door, view over view, DataFrame over SQL, `spark.table` over SQL, rename, writes, nested inner SQL) | 487 | **363** | **4** | **0** | 366 |
| part 2 (DataFrame door, SQL over DataFrame, held writes, derivations, DataFrame caches) | 448 | 0 | 0 | 0 | 430 |
| held frames | 272 | 0 | 0 | 0 | 119 |
| Iceberg catalog views | 96 | 0 | 0 | 0 | 32 |
| direct queries and fixtures | 122 | 0 | 0 | 0 | 33 |

**Hard bar: held.** 0 EQUAL → WRONG; the 4 EQUAL → RAISE are the ruled class below; no cell moves in part 2, the held
door, the catalog views or the direct queries; no cell whose created and read settings match moves (966 such cells).
The native door has no cell in the table: it does not build a `ReplanningTempView`, and its code is unchanged.

## The 4 EQUAL → RAISE (the ruled class: a settings-sensitive reader conjunct kept above a raising body)

| cell | Spark | head |
|---|---|---|
| `filter/or_mixed/sql/fdiv/on_off` (`b <> 0 OR b + 0 > 100`) | 5.0 | `DIVIDE_BY_ZERO` |
| `filter/agg_key_sens/sql/fagg/on_off` (`b + 0 <> 0` over the aggregate) | 5.0 | `DIVIDE_BY_ZERO` |
| `filter/and_free/sql/fdiv/on_off` (`NOT (b = 0) AND m > 1`; also AND-GUARD-ANSI-1) | 5.0 | `DIVIDE_BY_ZERO` |
| `filter_sensitive/sql/fdiv/on_off` (`b + 0 <> 0`, the step 0's own residual cell) | 5.0 | `DIVIDE_BY_ZERO` |

The first three are the SQL-door half of the critic's 6 ruled cells (the other 3 are DataFrame-door cells, part 2).
The fourth is the revision's `filter_sensitive` cell that question 11's default names ("the residual is loud only (2
`filter_sensitive` cells raise where Spark answers `5.0`)"); its off→on twin stays equal.

## Part 1's 375 view gaps

| outcome | cells |
|---|---|
| now Spark's answer | **348** (325 `run.py`, 23 `rev.py`) |
| now raise as Spark does, with RePark's condition text (`10 % 0` answers `DIVIDE_BY_ZERO`, Spark `REMAINDER_BY_ZERO`; `CAST(true AS DECIMAL(1,1))` lacks `.WITH_SUGGESTION`: direct gaps of domain table §6) | 20 |
| raise as Spark does; Spark's own PySpark error lost its condition (`'NoneType' object has no attribute 'contextType'`), `nest/read/sqlsql/off/m` | 1 |
| `CREATE TEMP VIEW … AS SELECT CAST('abc' AS DOUBLE)` refused at creation with ANSI on (the literal folds at analysis; direct gap, §6) | 7 creation steps (no read cell) |
| `current_timezone()` on a fresh `spark.table` frame (HELD-FRAME-CURRENT-TIMEZONE-1) | 2 |
| `caseSensitive`, created insensitive and read sensitive (`ci_upper`, the SQL door and `spark.table`; see below) | 3 |
| the outer DataFrame level of `nest-sql-df` (`m2`, part 2's) | 1 |

**`caseSensitive` is not captured.** Captured, it moved 4 cells from EQUAL to WRONG: a view `SELECT A AS m FROM cs2`
over a Parquet file with twin columns `a`, `A`, created case-sensitive and read case-insensitive, answers `2`/`1` where
Spark refuses `AMBIGUOUS_REFERENCE`. Left out, the 3 `ci_upper` cells stay the loud refusals they are on main.
Corrected 2026-11-18 (Delta 1, Fable's measurement): Spark does capture `spark.sql.caseSensitive` into the view's
`viewSQLConfigs`, and the body's names resolve under the captured value. The refusal is the file relation's, under the
reader's setting (`AMBIGUOUS_REFERENCE` from `LogicalPlan.resolve(StructType)`, `COLUMN_ALREADY_EXISTS` from
`DataSource.resolveRelation`), not a general twin-column check: a twin pair from a SQL view, an inline subquery or
`VALUES … AS t(a, A)` answers under the captured value. The refusal holds at every door, a held frame included, for
this file fixture only. RePark has no reader-side twin-column refusal on a file relation (the step 0's §6 direct gap);
capturing before it exists is what turned the 4 loud cells silent. The card TEMP-VIEW-CASE-SENSITIVE-CAPTURE-1 stays
proposed (S3, loud, not label-blocking): the file relation's reader-side refusal first, or in the same unit, then the
capture.

## Every move

363 WRONG → EQUAL, 23 WRONG → WRONG with a changed answer (the 20 condition-text cells, `nest/read/sqlsql/off/m`,
and the two `numcast` cells whose Arrow text changes from the `big` cast to the `s` cast because the body is no longer
pruned the same way), the 4 above. Cell by cell: [raw/bar-summary.md](raw/bar-summary.md) "Every move".

## Every non-EQUAL cell left in part 1's scope, by class

| class | cells |
|---|---|
| direct gap (a direct query of the body already differs from Spark) | 174 |
| direct gap, settings match (created and read under one value) | 117 |
| direct gap, condition text only | 64 |
| named view classes: HELD-FRAME-CURRENT-TIMEZONE-1 (2), `caseSensitive` (3), part 2's outer level (1), the twin-column held `spark.table` frames (2, unchanged), the ruled class (4), the string-to-integer cast gap (2), AND-GUARD-ANSI-1 control `on_on` (1) | 15 |

Cell by cell: [raw/bar-summary.md](raw/bar-summary.md) "Every non-EQUAL cell left in part 1's scope".

## Pins

`python/repark/tests/session/test_temp_view_session_settings_1.py`: 772 passed on the head; 606 failed and 166
passed on main (both ANSI modes and unset; 357 of 383 cells per mode equal Spark, the other 26 pinned by class). Every
position has a main-red cell, per ANSI mode: the part 1 doors (229), the filter cells (8), nested `current_timezone()`
and the nested ANSI outer level (2), unset (1), `CREATE OR REPLACE` (2), `RESET` (1 with ANSI off, 2 on) and
`spark.conf.set` (4), the self-join (2), the writes (30), the SQL-view caches (5).

## Delta 1 (2026-11-18): the inlined-frame check, closed by class

**Why.** A Fable medium consult under the owner's overnight delegation (verbatim: "Proceed with the plans, use
defaults, when it comes to any roadblocks or questions spin up a Fable medium session to ask for guidance. Merge any
PRs needed as long as they get us to our end goal") judged part 1's three builder calls. Its reply ("Fable reply — view
part 1 builder calls 2026-11-18") is guidance under that delegation, not an owner ruling. It declined the inlined-frame
fallback as built on one measured cell (S1, silent, the zone half, ANSI on): a Parquet view `fx` of
`TIMESTAMP '2024-01-01 03:00:00'` and `'2024-01-01 12:00:00'`; under `America/Los_Angeles`,
`spark.table("fx").filter("ts >= '2024-01-01 00:00:00'")` registered as `dv2`; under `UTC`,
`CREATE OR REPLACE TEMP VIEW sv2 AS SELECT ts FROM dv2`; under `America/Los_Angeles`, `SELECT count(*) FROM sv2`:
Spark 1, main 1, part 1 **2**. It adopted the `caseSensitive` exclusion (corrected above) and confirmed
`filter_sensitive/sql/fdiv/on_off` inside question 11's ruled class: the SQL door's accepted EQUAL → RAISE count is 4.

**Cause, measured.** The SQL door analyses a view body eagerly (`analyze_eagerly` in `spark_ast.rs`), so the check
read the inlined frame after the overlay's analyzer had run: the coercion of `'2024-01-01 00:00:00'` to a timestamp had
been folded into a timestamp literal under the SQL view's zone (`SparkLtzTimestampCast`, `spark_string_literal`), and
`column >= literal` passed the allow-list. Running the check after analysis is where it already ran, and a rule that
requires equal operand types cannot tell a folded literal from one the user wrote, so neither of those two options
closes the class. A scratch build of the equal-types rule left all 20 sweep regressions in place.

**The new rule** (`crates/repark-spark/src/view_ddl/temp_settings.rs`, `column_only_node` and `bare_column`): an
inlined frame reads settings unless every node of its analysed plan is in a closed list (projection, filter, subquery
alias, sort, distinct, aggregate, empty relation, a scan, a scan whose source carries a plan being checked the same
way, a union whose inputs have the union's types, a join whose keys have equal types) and every expression in it is a
bare column reference or an alias of one. A literal, a comparison, a cast, a function, `CASE`, `VALUES` and `LIMIT`
all count as reading; a reading frame keeps today's path, so its answer is main's. A plain DataFrame view over Parquet
(the cost rows' `k`) stays on the captured-settings path.

**The sweep** ([`_temp_view_session_settings_1_sweep.py`](../../../../../python/repark/tests/session/_temp_view_session_settings_1_sweep.py),
run by [probes/sweep_drive.py](probes/sweep_drive.py), compared by [probes/sweep_cmp.py](probes/sweep_cmp.py), raw
[raw/delta1-sweep.json](raw/delta1-sweep.json)): a DataFrame frame made under one setting, inlined into a SQL view
made under the other, read under each, both orders, in each ANSI mode: 116 cells per mode, 232 in all, on Spark 4.1.2,
main (`pkg-avg-27d6149f`), part 1 as built (`pkg-view1-12c0680c`) and the head (`pkg-view1-92a55871`, a scratch commit
whose product tree equals `c54f5fc7`'s; the final `pkg-view1-c54f5fc7` answers all 232 identically). Each shape has 8
cells.

| shape | equal to Spark: main / part 1 / head | part 1 EQUAL → WRONG | head EQUAL → WRONG | head's non-equal cells, by class |
|---|---|---|---|---|
| zone: `ts >= '…'` (filter string), `ts >= '…'` (Column API), `ts = '…'`, `ts IN (…)`, `ts BETWEEN …` | 4 / 0 / 4 each | **4 each (20)** | 0 | Part 2, 4 each |
| zone: `d < ts`, `hour(ts) = 3`, `CASE … THEN ts ELSE '…'` | 4 / 4 / 4 each | 0 | 0 | Part 2, 4 each |
| zone: `date` against a string (`>=`, `=`, `IN`, `BETWEEN`), the `spark.sql` frame, two levels | 8 / 8 / 8 each | 0 | 0 | — |
| zone: `date_add(ts, 0)` (select and filter), `hour('…Z')` | 0 / 0 / 0 each | 0 | 0 | frame gap, 8 each |
| ANSI: mixed `CASE` | 8 / 8 / 8 | 0 | 0 | — |
| ANSI: `s = 1`, `s + n`, `'1' + n`, `CAST(s AS INT) > 0`, `d = 'x'`, `date_add(d, '1')`, two levels | 0 / 0 / 0 each | 0 | 0 | frame gap, 8 each |
| decimal switch: `x0 = x10`, mixed `CASE`, two levels | 8 / 8 / 8 each | 0 | 0 | — |
| decimal switch: a `union` of `DECIMAL(38,0)` and `DECIMAL(38,10)` | 0 / 0 / 0 | 0 | 0 | frame gap, 8 |

- **The head answers every sweep cell exactly as main does** (232 of 232, rows, types and conditions); part 1 differs
  from main on the 20 cells above, every one read under the frame's own zone, in both ANSI modes. `sv2` is
  `ts >= '…'`, `la>utc`, read `la`.
- **Part 2** (16 per mode): read under the SQL view's own setting, which is the matching path; the frame follows the
  reader on main as on the head (TEMP-VIEW-SESSION-SETTINGS-1 part 2).
- **Frame gap** (44 per mode): the frame answers unlike Spark under its own setting, on main as on the head: RePark's
  DataFrame door raises Arrow's text for `'x'` in both ANSI modes, refuses `'1' + n` and `date_add` on a timestamp or a
  string count at planning, parses `hour('…Z')` differently, and refuses the decimal union with
  `CANNOT_UP_CAST_DATATYPE` under one switch value. So no ANSI-reading coercion that answers differently by mode could
  be measured on this door: the ANSI twin Fable assumed open is not reachable here, and its frames keep today's path.

**The bar, re-run** (04:41 to 04:43 UTC, inside the window where the two zones' dates differ): the step 0's domain
table with its own probes and the re-critic's `rc.py`, 2,772 paired cells: `run.py` (both start modes), `rev.py` and
`rc.py` fresh on the head (`pkg-view1-92a55871`); `rev.py` and `rc.py` fresh on main, part 1 as built and Spark; main's and part 1's `run.py`
from part 1's own run, the step 0's Spark files for `run.py`. Raw: [raw/delta1-rev-*.jsonl,
raw/delta1-rc-*.jsonl](raw/); moves by [probes/bar_moves.py](probes/bar_moves.py).

| build | EQUAL | BOTH_RAISE | BOTH_RAISE_OTHER | VALUE | REPARK_ANSWERS | REPARK_RAISES |
|---|---|---|---|---|---|---|
| main | 1,260 | 169 | 416 | 407 | 199 | 321 |
| part 1 as built | 1,563 | 225 | 437 | 206 | 122 | 219 |
| head | 1,563 | 225 | 437 | 206 | 122 | 219 |

- **Against part 1 as built: 0 cells move** (every answer identical). Against main: the moves of part 1 (363 WRONG →
  EQUAL, 23 WRONG → WRONG with a changed answer, the 4 ruled EQUAL → RAISE), 0 EQUAL → WRONG, no move in part 2 (878),
  the held door (391), the catalog views (128) or the direct queries (155), no move where the settings match. The pairing's
  summary equals [raw/bar-summary.md](raw/bar-summary.md) but for the four `now_fn` cells' hour values.
- **Hard bar: held,** with the sweep: 0 EQUAL → WRONG, 0 EQUAL → RAISE outside the 4 ruled cells.

**Pins.** The pin file now holds 499 cells per ANSI mode, the sweep's 116 included (Spark's answers in the oracle; the
Part 2 and frame-gap cells pinned as they answer, by class, in `temp_view_session_settings_1_sweep_standing.json`):
1,004 pass on the head (`pkg-view1-c54f5fc7`); on part 1 as built 20 fail, the 20 regressions above, `sv2` and its ANSI
off twin among them; on main 606 fail (part 1's positions; the sweep passes there). Rust:
`view_ddl::temp_settings::tests::a_coercing_frame_comparison_is_never_called_settings_free` fails on part 1's check and
passes on the head: it asserts the analysed `ts >= '…'` frame differs by zone and is found, as are the unanalysed one,
mixed `IN` and `BETWEEN`, string against number, a computed column, a coercing union and join and `VALUES`, while bare
columns, a boolean-column filter, an equal-typed union and join are not.
