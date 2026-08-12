# map — repark/dataframe/

## Purpose

DataFrame facade package. r26 T1 package-split the former monolith; **r27 T0** made the
region split real (technique A: nested-class extract + owned helpers).

## Contents

- `core.py` — `DataFrame` class + plan/export helpers; re-exports nested classes and
  moved private helpers (Q7 freeze: package + `core` + `_` binds). **G4b:** `DataFrame.join`'s
  `how_aliases` carries the semi family (`semi`/`leftsemi`, `anti`/`leftanti` — `left_semi` /
  `left_anti` fold in via the existing `.replace("_", "")`), routed to the engine tokens listed
  in the module-level `_SEMI_JOIN_HOWS`. Those tokens take two side paths: `_join_on_condition_h1`
  emits the LEFT side only and spells `LEFT SEMI` / `LEFT ANTI` (a semi join contributes no
  right-hand columns, so projecting them would be an unresolvable reference), and a conditionless
  semi/anti join (`on=None` or `on=[]`) refuses loud instead of falling through to the Cartesian
  path — a cross join is a different result set. See `task/g4b-join-widening-ledger.md`.
- `joins_columns.py` — `GroupedData` + pivot helpers (real body; technique A).
- `writer_readwriter.py` — `DataFrameWriter`, `DataFrameWriterV2`, `DataFrameStatFunctions`
  + write helpers (real body; technique A).
- `actions_export.py` — `DataFrameNaFunctions` (real body; technique A).
- `__init__.py` — frozen public imports (star-bind of core for private parity).

## I want to…

| Task | Go to |
|---|---|
| Change DataFrame methods / plan glue | `core.py` |
| Change `join` how-aliases / semi-family routing | `core.py` (`DataFrame.join` + `_SEMI_JOIN_HOWS`) |
| Change groupBy / pivot / agg grouping | `joins_columns.py` |
| Change write / save / V2 writer / stat | `writer_readwriter.py` |
| Change na.fill / drop / replace | `actions_export.py` |
| Public import surface | `__init__.py` |

## Pointers

Up: [../map.md](../map.md). Tests: `python/repark/tests/`. MOVE MAP: `task/t0-df-regions-ledger.md`.

## Debug

- Import path breaks → check core re-exports (Q7) and package `__init__` star-bind.
- Circular import → region modules import `DataFrame`/helpers from `core`; `core` imports
  classes only at file end (after helpers defined).
- Census / collect identity regressions → move-only regression; restore from base and re-slice.
