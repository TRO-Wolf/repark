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
  every move; [probes/target.py](probes/target.py) checks part 1's 375 view gaps. Raw: [raw/](raw/map.md).

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
over a relation with twin columns `a`, `A`, created case-sensitive and read case-insensitive, answers `2`/`1` where
Spark refuses `AMBIGUOUS_REFERENCE` at every door, a held frame included (the reader-time twin-column check, the step 0's
§6 gap). Left out, the 3 `ci_upper` cells stay the loud refusals they are on main. A card is proposed:
TEMP-VIEW-CASE-SENSITIVE-CAPTURE-1.

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

Up: [map.md](map.md).
