# map — repark/dataframe/

## Purpose

DataFrame facade package. r26 T1 package-split the former monolith; **r27 T0** made the
region split real (technique A: nested-class extract + owned helpers).

## Contents

- `core.py` — `DataFrame` class + plan/export helpers; re-exports nested classes and
  moved private helpers (Q7 freeze: package + `core` + `_` binds).
- `joins_columns.py` — `GroupedData` + pivot helpers (real body; technique A).
- `writer_readwriter.py` — `DataFrameWriter`, `DataFrameWriterV2`, `DataFrameStatFunctions`
  + write helpers (real body; technique A).
- `actions_export.py` — `DataFrameNaFunctions` (real body; technique A).
- `__init__.py` — frozen public imports (star-bind of core for private parity).

## I want to…

| Task | Go to |
|---|---|
| Change DataFrame methods / plan glue | `core.py` |
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
