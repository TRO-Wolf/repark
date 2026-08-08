# map — repark/session/

## Purpose

Package split of monolithic `session.py` (r26 T1 MOVE-ONLY).

## Contents

- `_funcs.py` — free functions (shared name binding for class modules)
- `builder_conf.py` — SparkContext, RuntimeConfig
- `session_core.py` — ReparkSession (sql/catalog methods stay here)
- `reader.py` — DataFrameReader (**`smartCsv` method body** — Q7 MOVE MAP destination)
- `sql_udf.py` — UDFRegistration
- `create_dataframe.py` — region marker + SparkSession/ReParkSession aliases
- `catalog.py` — re-export binding region note (r27 T1 no-stub mark)
- `__init__.py` — frozen public re-exports

## MOVE MAP (Q7)

| Symbol | Pre-split | Post-split |
|---|---|---|
| `DataFrameReader.smartCsv` | `session.py` | `session/reader.py` |

- r26 morning: T2 smartCsv samplingRows body re-seated into reader.py

- octo C1: smartCsv reads samplingRows from option map

- octo C2: samplingRows must be integral int > 0
