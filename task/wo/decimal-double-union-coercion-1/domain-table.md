# DECIMAL-DOUBLE-UNION-COERCION-1 — the domain table against Spark 4.1.2

RePark main `04d84ab9` (package `pkg-04d84ab9`, Spark door, DataFrame API, native door) against Spark
4.1.2 (`local[1]`, session zone UTC), ANSI on and off, Iceberg 1.11.0 runtime for the writes. Raw
cells, probes and provenance: [evidence/map.md](evidence/map.md). Every cell compares the result type
and the values; a written cell compares the rows **Spark reads back** from the Iceberg table each engine
wrote.

## Size and headline

| set | cells | root cells (a DECIMAL beside a FLOAT/DOUBLE) | WRONG value | RePark raises | type only | EQUAL |
|---|---|---|---|---|---|---|
| SQL `SELECT`, per ANSI mode | 1,985 | 1,193 | 306 | 368 overflow + 61 other | 200 | 256 (+2 both raise) |
| DataFrame API, per ANSI mode | 203 | 131 | 38 | 27 overflow + 36 other | 24 | 6 |
| Iceberg writes, per ANSI mode | 30 | 28 | **14 wrong writes** | 7 | — | 5 (+1 both raise) |
| #888 nvl-family table, replayed (ANSI on) | 612 | 2 | 0 | 0 | 0 | 2 |
| native door (`repark.sql()`), informational | 1,985 | — | see the native matrix | | | |

**Wrong reads (root, both modes): 688** (344 per mode). **Wrong writes (root, both modes): 28** (14 per
mode). The root cells are **identical in both ANSI modes** (every root cell has the same verdict on and
off): the fault is not mode-dependent, so the fix is not either. Spark types every root cell `double`
(or `array<double>`, `map<string,double>`, `boolean` for predicates), except `nullif(<decimal>,
<double>)`, which Spark types as its first argument's decimal.

Verdicts: `=` EQUAL; `T` type only (RePark's type differs, the values are the same numbers); `W` WRONG
value (a different number, a lost sign of zero, a different count); `X` RePark raises an overflow or a
`simplify_expressions` cast failure where Spark answers; `R` RePark raises for another reason (`IN
(subquery)` in a select list, the DataFrame `intersectAll` / `exceptAll` refusals); `B` both raise; `A`
Spark raises and RePark answers; `.` not run (a `VALUES` cell cannot hold a column).

## Operands and constructs

- **Decimal operands (left, `L`):** `0.1` (decimal(1,1)), `1.5`, `123456789.123456789` (decimal(18,9)),
  `0.001`, `CAST(0.5 AS DECIMAL(38,18))`, a DECIMAL(10,2) column `dc` (1.25).
- **Controls (left):** `1`, `CAST(2 AS BIGINT)`, `NULL`, `'3'`.
- **Float operands (right, `R`):** DOUBLE `1e-20`, `1e30`, `-0.0`, NaN, Infinity, a DOUBLE column `dd`
  (1e-20); FLOAT `1.5`, FLOAT `1e-20`.
- **Constructs:** `UNION ALL` both orders, `UNION`, `INTERSECT`, `EXCEPT`, two-row `VALUES` both orders,
  `CASE`, `IF`, `coalesce`, `nvl`, `ifnull`, `nvl2`, `nullif` both orders, `greatest`, `least`, `IN`
  list both ways, `IN (subquery)` both ways, `array`, `map` values, and two controls (`=`, `+`). The
  templates are in [evidence/probes/cells.py](evidence/probes/cells.py); each matrix cell below is six
  letters, one per decimal operand in the order listed, or four for the controls.

## The SQL matrices (Spark door)
ANSI true, decimal operands in the order dec_0.1, dec_1.5, dec_18_9, dec_0.001, dec_38_18, dec_col

| construct | dbl_1e-20 | dbl_1e30 | dbl_neg0 | dbl_nan | dbl_inf | dbl_col | flt_1.5 | flt_1e-20 |
|---|---|---|---|---|---|---|---|---|
| `union_all` | `WWWWWW` | `XXXXXX` | `WWWWWW` | `XXXXXX` | `XXXXXX` | `WWWWWW` | `TTTTTT` | `WWWWWW` |
| `union_all_rev` | `WWWWWW` | `XXXXXX` | `WWWWWW` | `XXXXXX` | `XXXXXX` | `WWWWWW` | `TTTTTT` | `WWWWWW` |
| `union` | `WWWWWW` | `XXXXXX` | `WTTTWT` | `XXXXXX` | `XXXXXX` | `WWWWWW` | `TTTTTT` | `WWWWWW` |
| `intersect` | `TTTTTT` | `XXXXXX` | `TTTTTT` | `XXXXXX` | `XXXXXX` | `TTTTTT` | `TTTTTT` | `TTTTTT` |
| `except` | `TTTTTT` | `XXXXXX` | `TTTTTT` | `XXXXXX` | `XXXXXX` | `TTTTTT` | `TTTTTT` | `TTTTTT` |
| `values` | `WWWWW.` | `XXXXX.` | `WWWWW.` | `XXXXX.` | `XXXXX.` | `......` | `TTTTT.` | `WWWWW.` |
| `values_rev` | `WWWWW.` | `XXXXX.` | `WWWWW.` | `XXXXX.` | `XXXXX.` | `......` | `TTTTT.` | `WWWWW.` |
| `case` | `WWWWWW` | `XXXXXX` | `WWWWWW` | `XXXXXX` | `XXXXXX` | `WWWWWW` | `TTTTTT` | `WWWWWW` |
| `if` | `WWWWWW` | `XXXXXX` | `WWWWWW` | `XXXXXX` | `XXXXXX` | `WWWWWW` | `TTTTTT` | `WWWWWW` |
| `coalesce` | `WWWWWW` | `XXXXXX` | `WWWWWW` | `XXXXXX` | `XXXXXX` | `WWWWWW` | `TTTTTT` | `WWWWWW` |
| `nvl` | `======` | `======` | `======` | `======` | `======` | `======` | `======` | `======` |
| `ifnull` | `======` | `======` | `======` | `======` | `======` | `======` | `======` | `======` |
| `nvl2` | `WWWWWW` | `XXXXXX` | `WWWWWW` | `XXXXXX` | `XXXXXX` | `WWWWWW` | `TTTTTT` | `WWWWWW` |
| `nullif_rl` | `WWWWWW` | `XXXXXX` | `WWWWWW` | `XXXXXX` | `XXXXXX` | `WWWWWW` | `TTTTTT` | `WWWWWW` |
| `nullif_lr` | `TTTT=T` | `XXXXXX` | `TTTT=T` | `XXXXXX` | `XXXXXX` | `TTTT=T` | `TT=T=T` | `TT=T=T` |
| `greatest` | `TTTTTT` | `XXXXXX` | `TTTTTT` | `XXXXXX` | `XXXXXX` | `TTTTTT` | `TTTTTT` | `TTTTTT` |
| `least` | `WWWWWW` | `XXXXXX` | `WWWWWW` | `XXXXXX` | `XXXXXX` | `WWWWWW` | `TTTTTT` | `WWWWWW` |
| `in_rl` | `======` | `====XX` | `======` | `====XX` | `====XX` | `======` | `======` | `======` |
| `in_lr` | `======` | `XXXXXX` | `======` | `XXXXXX` | `XXXXXX` | `======` | `======` | `======` |
| `insub_rl` | `RRRRRR` | `XXXXXX` | `RRRRRR` | `XXXXXX` | `XXXXXX` | `RRRRRR` | `RRRRRR` | `RRRRRR` |
| `insub_lr` | `RRRRRR` | `XXXXXX` | `RRRRRR` | `XXXXXX` | `XXXXXX` | `RRRRRR` | `RRRRRR` | `RRRRRR` |
| `array` | `WWWWWW` | `XXXXXX` | `WWWWWW` | `XXXXXX` | `XXXXXX` | `WWWWWW` | `TTTTTT` | `WWWWWW` |
| `map_value` | `WWWWWW` | `XXXXXX` | `WWWWWW` | `XXXXXX` | `XXXXXX` | `WWWWWW` | `TTTTTT` | `WWWWWW` |
| `ctl_eq` | `======` | `====XX` | `======` | `====XX` | `====XX` | `======` | `======` | `======` |
| `ctl_add` | `======` | `======` | `======` | `======` | `======` | `======` | `WTWWTT` | `WTWWTT` |

ANSI true, control operands in the order int, bigint, null, str_digit

| construct | dbl_1e-20 | dbl_1e30 | dbl_neg0 | dbl_nan | dbl_inf | dbl_col | flt_1.5 | flt_1e-20 |
|---|---|---|---|---|---|---|---|---|
| `union_all` | `===W` | `===W` | `===W` | `===W` | `===W` | `===W` | `TT=W` | `TT=W` |
| `union_all_rev` | `===W` | `===W` | `===W` | `===W` | `===W` | `===W` | `TT=W` | `TT=W` |
| `union` | `===W` | `===W` | `WWWW` | `===W` | `===W` | `===W` | `TT=W` | `TT=W` |
| `intersect` | `TTTT` | `TTTT` | `TTTT` | `TTTT` | `TTTT` | `TTTT` | `TTTT` | `TTTT` |
| `except` | `WWTW` | `WWTW` | `WWTW` | `WWTW` | `WWTW` | `WWTW` | `WWTW` | `WWTW` |
| `values` | `===A` | `===A` | `===A` | `===A` | `===A` | `....` | `===A` | `===A` |
| `values_rev` | `===A` | `===A` | `===A` | `===A` | `===A` | `....` | `===A` | `===A` |
| `case` | `====` | `====` | `====` | `====` | `====` | `====` | `TT=T` | `TT=T` |
| `if` | `====` | `====` | `====` | `====` | `====` | `====` | `TT=T` | `TT=T` |
| `coalesce` | `====` | `====` | `====` | `====` | `====` | `====` | `TT=T` | `TT=T` |
| `nvl` | `===W` | `===W` | `===W` | `===W` | `===W` | `===W` | `TT=W` | `TT=W` |
| `ifnull` | `===W` | `===W` | `===W` | `===W` | `===W` | `===W` | `TT=W` | `TT=W` |
| `nvl2` | `===W` | `===W` | `===W` | `===W` | `===W` | `===W` | `TT=W` | `TT=W` |
| `nullif_rl` | `====` | `====` | `====` | `====` | `====` | `====` | `====` | `====` |
| `nullif_lr` | `WWTW` | `WWTW` | `WWTW` | `WWTW` | `WWTW` | `WWTW` | `WWTW` | `WWTW` |
| `greatest` | `===A` | `===A` | `===A` | `===A` | `===A` | `===A` | `TT=A` | `TT=A` |
| `least` | `===A` | `===A` | `===A` | `===A` | `===A` | `===A` | `TT=A` | `TT=A` |
| `in_rl` | `====` | `====` | `====` | `====` | `====` | `====` | `====` | `====` |
| `in_lr` | `====` | `====` | `====` | `====` | `====` | `====` | `====` | `====` |
| `insub_rl` | `RRRR` | `RRRR` | `RRRR` | `RRRR` | `RRRR` | `RRRR` | `RRRR` | `RRRR` |
| `insub_lr` | `RRRR` | `RRRR` | `RRRR` | `RRRR` | `RRRR` | `RRRR` | `RRRR` | `RRRR` |
| `array` | `====` | `====` | `====` | `====` | `====` | `====` | `TT=T` | `TT=T` |
| `map_value` | `====` | `====` | `====` | `====` | `====` | `====` | `TT=T` | `TT=T` |
| `ctl_eq` | `====` | `====` | `====` | `====` | `====` | `====` | `====` | `====` |
| `ctl_add` | `===R` | `===R` | `===R` | `===R` | `===R` | `===R` | `TT=R` | `TT=R` |

ANSI false, decimal operands in the order dec_0.1, dec_1.5, dec_18_9, dec_0.001, dec_38_18, dec_col

| construct | dbl_1e-20 | dbl_1e30 | dbl_neg0 | dbl_nan | dbl_inf | dbl_col | flt_1.5 | flt_1e-20 |
|---|---|---|---|---|---|---|---|---|
| `union_all` | `WWWWWW` | `XXXXXX` | `WWWWWW` | `XXXXXX` | `XXXXXX` | `WWWWWW` | `TTTTTT` | `WWWWWW` |
| `union_all_rev` | `WWWWWW` | `XXXXXX` | `WWWWWW` | `XXXXXX` | `XXXXXX` | `WWWWWW` | `TTTTTT` | `WWWWWW` |
| `union` | `WWWWWW` | `XXXXXX` | `WTTTWT` | `XXXXXX` | `XXXXXX` | `WWWWWW` | `TTTTTT` | `WWWWWW` |
| `intersect` | `TTTTTT` | `XXXXXX` | `TTTTTT` | `XXXXXX` | `XXXXXX` | `TTTTTT` | `TTTTTT` | `TTTTTT` |
| `except` | `TTTTTT` | `XXXXXX` | `TTTTTT` | `XXXXXX` | `XXXXXX` | `TTTTTT` | `TTTTTT` | `TTTTTT` |
| `values` | `WWWWW.` | `XXXXX.` | `WWWWW.` | `XXXXX.` | `XXXXX.` | `......` | `TTTTT.` | `WWWWW.` |
| `values_rev` | `WWWWW.` | `XXXXX.` | `WWWWW.` | `XXXXX.` | `XXXXX.` | `......` | `TTTTT.` | `WWWWW.` |
| `case` | `WWWWWW` | `XXXXXX` | `WWWWWW` | `XXXXXX` | `XXXXXX` | `WWWWWW` | `TTTTTT` | `WWWWWW` |
| `if` | `WWWWWW` | `XXXXXX` | `WWWWWW` | `XXXXXX` | `XXXXXX` | `WWWWWW` | `TTTTTT` | `WWWWWW` |
| `coalesce` | `WWWWWW` | `XXXXXX` | `WWWWWW` | `XXXXXX` | `XXXXXX` | `WWWWWW` | `TTTTTT` | `WWWWWW` |
| `nvl` | `======` | `======` | `======` | `======` | `======` | `======` | `======` | `======` |
| `ifnull` | `======` | `======` | `======` | `======` | `======` | `======` | `======` | `======` |
| `nvl2` | `WWWWWW` | `XXXXXX` | `WWWWWW` | `XXXXXX` | `XXXXXX` | `WWWWWW` | `TTTTTT` | `WWWWWW` |
| `nullif_rl` | `WWWWWW` | `XXXXXX` | `WWWWWW` | `XXXXXX` | `XXXXXX` | `WWWWWW` | `TTTTTT` | `WWWWWW` |
| `nullif_lr` | `TTTT=T` | `XXXXXX` | `TTTT=T` | `XXXXXX` | `XXXXXX` | `TTTT=T` | `TT=T=T` | `TT=T=T` |
| `greatest` | `TTTTTT` | `XXXXXX` | `TTTTTT` | `XXXXXX` | `XXXXXX` | `TTTTTT` | `TTTTTT` | `TTTTTT` |
| `least` | `WWWWWW` | `XXXXXX` | `WWWWWW` | `XXXXXX` | `XXXXXX` | `WWWWWW` | `TTTTTT` | `WWWWWW` |
| `in_rl` | `======` | `====XX` | `======` | `====XX` | `====XX` | `======` | `======` | `======` |
| `in_lr` | `======` | `XXXXXX` | `======` | `XXXXXX` | `XXXXXX` | `======` | `======` | `======` |
| `insub_rl` | `RRRRRR` | `XXXXXX` | `RRRRRR` | `XXXXXX` | `XXXXXX` | `RRRRRR` | `RRRRRR` | `RRRRRR` |
| `insub_lr` | `RRRRRR` | `XXXXXX` | `RRRRRR` | `XXXXXX` | `XXXXXX` | `RRRRRR` | `RRRRRR` | `RRRRRR` |
| `array` | `WWWWWW` | `XXXXXX` | `WWWWWW` | `XXXXXX` | `XXXXXX` | `WWWWWW` | `TTTTTT` | `WWWWWW` |
| `map_value` | `WWWWWW` | `XXXXXX` | `WWWWWW` | `XXXXXX` | `XXXXXX` | `WWWWWW` | `TTTTTT` | `WWWWWW` |
| `ctl_eq` | `======` | `====XX` | `======` | `====XX` | `====XX` | `======` | `======` | `======` |
| `ctl_add` | `======` | `======` | `======` | `======` | `======` | `======` | `WTWWTT` | `WTWWTT` |

ANSI false, control operands in the order int, bigint, null, str_digit

| construct | dbl_1e-20 | dbl_1e30 | dbl_neg0 | dbl_nan | dbl_inf | dbl_col | flt_1.5 | flt_1e-20 |
|---|---|---|---|---|---|---|---|---|
| `union_all` | `====` | `====` | `====` | `====` | `====` | `====` | `====` | `====` |
| `union_all_rev` | `====` | `====` | `====` | `====` | `====` | `====` | `====` | `====` |
| `union` | `====` | `====` | `WWW=` | `====` | `====` | `====` | `====` | `====` |
| `intersect` | `TTT=` | `TTT=` | `TTT=` | `TTT=` | `TTT=` | `TTT=` | `TTT=` | `TTT=` |
| `except` | `WWT=` | `WWT=` | `WWT=` | `WWT=` | `WWT=` | `WWT=` | `WWT=` | `WWT=` |
| `values` | `===A` | `===A` | `===A` | `===A` | `===A` | `....` | `===A` | `===A` |
| `values_rev` | `===A` | `===A` | `===A` | `===A` | `===A` | `....` | `===A` | `===A` |
| `case` | `===W` | `===W` | `===W` | `===W` | `===W` | `===W` | `===W` | `===W` |
| `if` | `===W` | `===W` | `===W` | `===W` | `===W` | `===W` | `===W` | `===W` |
| `coalesce` | `===W` | `===W` | `===W` | `===W` | `===W` | `===W` | `===W` | `===W` |
| `nvl` | `====` | `====` | `====` | `====` | `====` | `====` | `====` | `====` |
| `ifnull` | `====` | `====` | `====` | `====` | `====` | `====` | `====` | `====` |
| `nvl2` | `====` | `====` | `====` | `====` | `====` | `====` | `====` | `====` |
| `nullif_rl` | `====` | `====` | `====` | `====` | `====` | `====` | `====` | `====` |
| `nullif_lr` | `WWTW` | `WWTW` | `WWTW` | `WWTW` | `WWTW` | `WWTW` | `WWTW` | `WWTW` |
| `greatest` | `===A` | `===A` | `===A` | `===A` | `===A` | `===A` | `===A` | `===A` |
| `least` | `===A` | `===A` | `===A` | `===A` | `===A` | `===A` | `===A` | `===A` |
| `in_rl` | `====` | `====` | `====` | `====` | `====` | `====` | `====` | `====` |
| `in_lr` | `====` | `====` | `====` | `====` | `====` | `====` | `====` | `====` |
| `insub_rl` | `RRRR` | `RRRR` | `RRRR` | `RRRR` | `RRRR` | `RRRR` | `RRRR` | `RRRR` |
| `insub_lr` | `RRRR` | `RRRR` | `RRRR` | `RRRR` | `RRRR` | `RRRR` | `RRRR` | `RRRR` |
| `array` | `===W` | `===W` | `===W` | `===W` | `===W` | `===W` | `===W` | `===W` |
| `map_value` | `===W` | `===W` | `===W` | `===W` | `===W` | `===W` | `===W` | `===W` |
| `ctl_eq` | `====` | `====` | `====` | `====` | `====` | `====` | `====` | `====` |
| `ctl_add` | `===R` | `===R` | `===R` | `===R` | `===R` | `===R` | `===R` | `===R` |

native door against Spark ANSI true, decimal operands

| construct | dbl_1e-20 | dbl_1e30 | dbl_neg0 | dbl_nan | dbl_inf | dbl_col | flt_1.5 | flt_1e-20 |
|---|---|---|---|---|---|---|---|---|
| `union_all` | `====WW` | `====XX` | `====WW` | `====XX` | `====XX` | `====WW` | `====TT` | `====WW` |
| `union_all_rev` | `====WW` | `====XX` | `====WW` | `====XX` | `====XX` | `====WW` | `====TT` | `====WW` |
| `union` | `====WW` | `====XX` | `WWWWWT` | `====XX` | `====XX` | `====WW` | `====TT` | `====WW` |
| `intersect` | `====TT` | `====XX` | `====TT` | `====XX` | `====XX` | `====TT` | `====TT` | `====TT` |
| `except` | `====TT` | `====XX` | `====TT` | `====XX` | `====XX` | `====TT` | `====TT` | `====TT` |
| `values` | `====W.` | `====X.` | `====W.` | `====X.` | `====X.` | `......` | `====T.` | `====W.` |
| `values_rev` | `====W.` | `====X.` | `====W.` | `====X.` | `====X.` | `......` | `====T.` | `====W.` |
| `case` | `====WW` | `====XX` | `====WW` | `====XX` | `====XX` | `====WW` | `====TT` | `====WW` |
| `if` | `RRRRRR` | `RRRRRR` | `RRRRRR` | `RRRRRR` | `RRRRRR` | `RRRRRR` | `RRRRRR` | `RRRRRR` |
| `coalesce` | `====WW` | `====XX` | `====WW` | `====XX` | `====XX` | `====WW` | `====TT` | `====WW` |
| `nvl` | `======` | `======` | `======` | `======` | `======` | `======` | `======` | `======` |
| `ifnull` | `======` | `======` | `======` | `======` | `======` | `======` | `======` | `======` |
| `nvl2` | `====WW` | `====XX` | `====WW` | `====XX` | `====XX` | `====WW` | `====TT` | `====WW` |
| `nullif_rl` | `====WW` | `====XX` | `====WW` | `====XX` | `====XX` | `====WW` | `TTTTTT` | `TTTTWW` |
| `nullif_lr` | `TTTT=T` | `TTTTXX` | `TTTT=T` | `TTTTXX` | `TTTTXX` | `TTTT=T` | `TTTT=T` | `TTTT=T` |
| `greatest` | `====TT` | `====XX` | `====TT` | `====XX` | `====XX` | `====TT` | `====TT` | `====TT` |
| `least` | `====WW` | `====XX` | `====WW` | `====XX` | `====XX` | `====WW` | `====TT` | `====WW` |
| `in_rl` | `======` | `====XX` | `======` | `====XX` | `====XX` | `======` | `======` | `======` |
| `in_lr` | `======` | `====XX` | `======` | `====XX` | `====XX` | `======` | `======` | `======` |
| `insub_rl` | `RRRRRR` | `RRRRXX` | `RRRRRR` | `RRRRXX` | `RRRRXX` | `RRRRRR` | `RRRRRR` | `RRRRRR` |
| `insub_lr` | `RRRRRR` | `RRRRXX` | `RRRRRR` | `RRRRXX` | `RRRRXX` | `RRRRRR` | `RRRRRR` | `RRRRRR` |
| `array` | `RRRRRR` | `RRRRRR` | `RRRRRR` | `RRRRRR` | `RRRRRR` | `RRRRRR` | `RRRRRR` | `RRRRRR` |
| `map_value` | `====WW` | `====XX` | `====WW` | `====XX` | `====XX` | `====WW` | `====TT` | `====WW` |
| `ctl_eq` | `======` | `====XX` | `======` | `====XX` | `====XX` | `======` | `======` | `======` |
| `ctl_add` | `======` | `======` | `======` | `======` | `======` | `======` | `====TT` | `====TT` |


## The Iceberg writes (rows Spark reads back)

Target tables `(id INT, v <target>)`; UPDATE and MERGE cells seed `(1, 9), (2, 9)` first. Probe [evidence/probes/run_write.py](evidence/probes/run_write.py), cells [evidence/probes/write_cells.py](evidence/probes/write_cells.py), reader [evidence/probes/read_back.py](evidence/probes/read_back.py).

| cell | target | ANSI | Spark writes; Spark reads | RePark writes; Spark reads | verdict |
|---|---|---|---|---|---|
| `ins_union_tiny` | DOUBLE | true | ok; double: 1=0.1, 2=1e-20 | ok; double: 1=0.1, 2=0.0 | WRONG-WRITE |
| `ins_union_big` | DOUBLE | true | ok; double: 1=0.1, 2=1e+30 | raises PySparkException: datafusion engine error: Optimizer rule 'simplify_expressions' failed; double:  | REPARK-RAISES |
| `ins_union_neg0` | DOUBLE | true | ok; double: 1=0.1, 2=-0.0 | ok; double: 1=0.1, 2=0.0 | WRONG-WRITE |
| `ins_union_nan` | DOUBLE | true | ok; double: 1=0.1, 2=nan | raises PySparkException: datafusion engine error: Optimizer rule 'simplify_expressions' failed; double:  | REPARK-RAISES |
| `ins_union_rev_tiny` | DOUBLE | true | ok; double: 1=1e-20, 2=0.1 | raises PySparkException: datafusion engine error: Internal error: cannot convert text Decimal128(30, 15) into byte ; double:  | REPARK-RAISES |
| `ins_union_distinct_tiny` | DOUBLE | true | ok; double: 1=0.1, 2=1e-20 | ok; double: 1=0.1, 2=0.0 | WRONG-WRITE |
| `ins_values_tiny` | DOUBLE | true | ok; double: 1=0.1, 2=1e-20 | ok; double: 1=0.1, 2=1e-20 | EQUAL |
| `ins_values_big` | DOUBLE | true | ok; double: 1=0.1, 2=1e+30 | ok; double: 1=0.1, 2=1e+30 | EQUAL |
| `ins_select_values_tiny` | DOUBLE | true | ok; double: 1=0.1, 2=1e-20 | ok; double: 1=0.1, 2=1e-20 | EQUAL |
| `ins_case_tiny` | DOUBLE | true | ok; double: 1=0.1, 2=1e-20 | raises AnalysisException: Error during planning: Inconsistent data length across values list: got 1 values in row 0 ; double:  | REPARK-RAISES |
| `ins_coalesce_tiny` | DOUBLE | true | ok; double: 2=1e-20 | ok; double: 2=0.0 | WRONG-WRITE |
| `ins_greatest_big` | DOUBLE | true | ok; double: 2=1e+30 | raises PySparkException: datafusion engine error: Optimizer rule 'simplify_expressions' failed; double:  | REPARK-RAISES |
| `ins_array_elem_tiny` | DOUBLE | true | ok; double: 2=1e-20 | ok; double: 2=0.0 | WRONG-WRITE |
| `ins_union_float_tiny` | FLOAT | true | ok; float: 1=0.10000000149011612, 2=9.999999682655225e-21 | ok; float: 1=0.10000000149011612, 2=0.0 | WRONG-WRITE |
| `ins_union_float_big` | FLOAT | true | ok; float: 1=0.10000000149011612, 2=1.0000000150474662e+30 | raises PySparkException: datafusion engine error: Optimizer rule 'simplify_expressions' failed; float:  | REPARK-RAISES |
| `ins_union_dec_round` | DECIMAL(10,2) | true | ok; decimal(10,2): 1=0.10, 2=1.26 | ok; decimal(10,2): 1=0.10, 2=1.26 | EQUAL |
| `ins_union_dec_tiny` | DECIMAL(10,2) | true | ok; decimal(10,2): 1=0.10, 2=0.00 | ok; decimal(10,2): 1=0.10, 2=0.00 | EQUAL |
| `ins_union_dec_big` | DECIMAL(10,2) | true | raises CAST_OVERFLOW_IN_TABLE_INSERT: [CAST_OVERFLOW_IN_TABLE_INSERT] Fail to assign a value of "DOUBLE" type to the "DECIMAL(10; decimal(10,2):  | raises PySparkException: datafusion engine error: Optimizer rule 'simplify_expressions' failed; decimal(10,2):  | BOTH-RAISE |
| `ins_values_dec_round` | DECIMAL(10,2) | true | ok; decimal(10,2): 1=0.10, 2=1.26 | ok; decimal(10,2): 1=0.10, 2=1.25 | WRONG-WRITE |
| `upd_case_tiny` | DOUBLE | true | ok; double: 1=0.1, 2=1e-20 | ok; double: 1=0.1, 2=0.0 | WRONG-WRITE |
| `upd_coalesce_tiny` | DOUBLE | true | ok; double: 1=9.0, 2=1e-20 | ok; double: 1=9.0, 2=0.0 | WRONG-WRITE |
| `upd_if_big` | DOUBLE | true | ok; double: 1=0.1, 2=1e+30 | raises PySparkException: datafusion engine error: Optimizer rule 'simplify_expressions' failed; double: 1=9.0, 2=9.0 | REPARK-RAISES |
| `upd_where_in_union` | DOUBLE | true | ok; double: 1=1.0, 2=9.0 | raises AnalysisException: Error during planning: UPDATE with a subquery in its WHERE clause is refused on ``lc`.`ns`; double: 1=9.0, 2=9.0 | REPARK-RAISES |
| `merge_union_src` | DOUBLE | true | ok; double: 1=0.1, 2=1e-20, 3=-1e-20 | ok; double: 1=0.1, 2=0.0, 3=0.0 | WRONG-WRITE |
| `merge_union_big` | DOUBLE | true | ok; double: 1=0.1, 2=1e+30, 3=-1e+30 | raises PySparkException: datafusion engine error: Optimizer rule 'simplify_expressions' failed; double: 1=9.0, 2=9.0 | REPARK-RAISES |
| `merge_values_src` | DOUBLE | true | ok; double: 1=0.1, 2=1e-20, 3=-1e-20 | ok; double: 1=0.1, 2=0.0, 3=0.0 | WRONG-WRITE |
| `merge_set_case` | DOUBLE | true | ok; double: 1=9.0, 2=1e-20 | ok; double: 1=9.0, 2=0.0 | WRONG-WRITE |
| `merge_insert_coalesce` | DOUBLE | true | ok; double: 1=9.0, 2=9.0, 3=1e-20 | ok; double: 1=9.0, 2=9.0, 3=0.0 | WRONG-WRITE |
| `ctas_union_tiny` | None | true | ok; double: 1=0.1, 2=1e-20 | ok; decimal(30,15): 1=0.100000000000000, 2=0E-15 | WRONG-WRITE |
| `df_union_append` | DOUBLE | true | ok; double: 1=0.1, 2=1e-20 | ok; double: 1=0.1, 2=0.0 | WRONG-WRITE |
| `ins_union_tiny` | DOUBLE | false | ok; double: 1=0.1, 2=1e-20 | ok; double: 1=0.1, 2=0.0 | WRONG-WRITE |
| `ins_union_big` | DOUBLE | false | ok; double: 1=0.1, 2=1e+30 | raises PySparkException: datafusion engine error: Optimizer rule 'simplify_expressions' failed; double:  | REPARK-RAISES |
| `ins_union_neg0` | DOUBLE | false | ok; double: 1=0.1, 2=-0.0 | ok; double: 1=0.1, 2=0.0 | WRONG-WRITE |
| `ins_union_nan` | DOUBLE | false | ok; double: 1=0.1, 2=nan | raises PySparkException: datafusion engine error: Optimizer rule 'simplify_expressions' failed; double:  | REPARK-RAISES |
| `ins_union_rev_tiny` | DOUBLE | false | ok; double: 1=1e-20, 2=0.1 | raises PySparkException: datafusion engine error: Internal error: cannot convert text Decimal128(30, 15) into byte ; double:  | REPARK-RAISES |
| `ins_union_distinct_tiny` | DOUBLE | false | ok; double: 1=0.1, 2=1e-20 | ok; double: 1=0.1, 2=0.0 | WRONG-WRITE |
| `ins_values_tiny` | DOUBLE | false | ok; double: 1=0.1, 2=1e-20 | ok; double: 1=0.1, 2=1e-20 | EQUAL |
| `ins_values_big` | DOUBLE | false | ok; double: 1=0.1, 2=1e+30 | ok; double: 1=0.1, 2=1e+30 | EQUAL |
| `ins_select_values_tiny` | DOUBLE | false | ok; double: 1=0.1, 2=1e-20 | ok; double: 1=0.1, 2=1e-20 | EQUAL |
| `ins_case_tiny` | DOUBLE | false | ok; double: 1=0.1, 2=1e-20 | raises AnalysisException: Error during planning: Inconsistent data length across values list: got 1 values in row 0 ; double:  | REPARK-RAISES |
| `ins_coalesce_tiny` | DOUBLE | false | ok; double: 2=1e-20 | ok; double: 2=0.0 | WRONG-WRITE |
| `ins_greatest_big` | DOUBLE | false | ok; double: 2=1e+30 | raises PySparkException: datafusion engine error: Optimizer rule 'simplify_expressions' failed; double:  | REPARK-RAISES |
| `ins_array_elem_tiny` | DOUBLE | false | ok; double: 2=1e-20 | ok; double: 2=0.0 | WRONG-WRITE |
| `ins_union_float_tiny` | FLOAT | false | ok; float: 1=0.10000000149011612, 2=9.999999682655225e-21 | ok; float: 1=0.10000000149011612, 2=0.0 | WRONG-WRITE |
| `ins_union_float_big` | FLOAT | false | ok; float: 1=0.10000000149011612, 2=1.0000000150474662e+30 | raises PySparkException: datafusion engine error: Optimizer rule 'simplify_expressions' failed; float:  | REPARK-RAISES |
| `ins_union_dec_round` | DECIMAL(10,2) | false | ok; decimal(10,2): 1=0.10, 2=1.26 | ok; decimal(10,2): 1=0.10, 2=1.26 | EQUAL |
| `ins_union_dec_tiny` | DECIMAL(10,2) | false | ok; decimal(10,2): 1=0.10, 2=0.00 | ok; decimal(10,2): 1=0.10, 2=0.00 | EQUAL |
| `ins_union_dec_big` | DECIMAL(10,2) | false | raises CAST_OVERFLOW_IN_TABLE_INSERT: [CAST_OVERFLOW_IN_TABLE_INSERT] Fail to assign a value of "DOUBLE" type to the "DECIMAL(10; decimal(10,2):  | raises PySparkException: datafusion engine error: Optimizer rule 'simplify_expressions' failed; decimal(10,2):  | BOTH-RAISE |
| `ins_values_dec_round` | DECIMAL(10,2) | false | ok; decimal(10,2): 1=0.10, 2=1.26 | ok; decimal(10,2): 1=0.10, 2=1.25 | WRONG-WRITE |
| `upd_case_tiny` | DOUBLE | false | ok; double: 1=0.1, 2=1e-20 | ok; double: 1=0.1, 2=0.0 | WRONG-WRITE |
| `upd_coalesce_tiny` | DOUBLE | false | ok; double: 1=9.0, 2=1e-20 | ok; double: 1=9.0, 2=0.0 | WRONG-WRITE |
| `upd_if_big` | DOUBLE | false | ok; double: 1=0.1, 2=1e+30 | raises PySparkException: datafusion engine error: Optimizer rule 'simplify_expressions' failed; double: 1=9.0, 2=9.0 | REPARK-RAISES |
| `upd_where_in_union` | DOUBLE | false | ok; double: 1=1.0, 2=9.0 | raises AnalysisException: Error during planning: UPDATE with a subquery in its WHERE clause is refused on ``lc`.`ns`; double: 1=9.0, 2=9.0 | REPARK-RAISES |
| `merge_union_src` | DOUBLE | false | ok; double: 1=0.1, 2=1e-20, 3=-1e-20 | ok; double: 1=0.1, 2=0.0, 3=0.0 | WRONG-WRITE |
| `merge_union_big` | DOUBLE | false | ok; double: 1=0.1, 2=1e+30, 3=-1e+30 | raises PySparkException: datafusion engine error: Optimizer rule 'simplify_expressions' failed; double: 1=9.0, 2=9.0 | REPARK-RAISES |
| `merge_values_src` | DOUBLE | false | ok; double: 1=0.1, 2=1e-20, 3=-1e-20 | ok; double: 1=0.1, 2=0.0, 3=0.0 | WRONG-WRITE |
| `merge_set_case` | DOUBLE | false | ok; double: 1=9.0, 2=1e-20 | ok; double: 1=9.0, 2=0.0 | WRONG-WRITE |
| `merge_insert_coalesce` | DOUBLE | false | ok; double: 1=9.0, 2=9.0, 3=1e-20 | ok; double: 1=9.0, 2=9.0, 3=0.0 | WRONG-WRITE |
| `ctas_union_tiny` | None | false | ok; double: 1=0.1, 2=1e-20 | ok; decimal(30,15): 1=0.100000000000000, 2=0E-15 | WRONG-WRITE |
| `df_union_append` | DOUBLE | false | ok; double: 1=0.1, 2=1e-20 | ok; double: 1=0.1, 2=0.0 | WRONG-WRITE |

Outside the root: `ins_values_dec_round` (RePark rounds the double 1.255 to 1.25 in a DECIMAL(10,2) store, Spark to 1.26), `ins_case_tiny` (a one-column `VALUES` inside an `INSERT` source takes the two-column target schema and raises), `upd_where_in_union` (the ruling-106 refusal of an `UPDATE` with a subquery `WHERE`).

## Extra shapes and DataFrame column functions (ANSI on; ANSI off has the same verdicts)

| cell | ANSI | Spark 4.1.2 | RePark | verdict |
|---|---|---|---|---|
| `dfcol/array_dc_dd` | true | `array<double>` [1.25 | `array<decimal(30,15)>` [dec:1.250000000000000, dec:0E-15] | wrong-value |
| `dfcol/coalesce_dbig_dc` | true | `double` 1e+30 | raises Arrow error: Cast error: Cannot cast to Decimal128(30, 15). Overflowing on 1e30 | repark-raises-overflow |
| `dfcol/coalesce_dd_dc` | true | `double` 1e-20 | `decimal(30,15)` dec:0E-15 | wrong-value |
| `dfcol/coalesce_dd_litdec` | true | `double` 1e-20 | `decimal(30,15)` dec:0E-15 | wrong-value |
| `dfcol/fexpr_array` | true | `array<double>` [0.1 | `array<double>` [0.1 | equal |
| `dfcol/fexpr_case` | true | `double` 1e-20 | `double` 1e-20 | equal |
| `dfcol/fexpr_coalesce` | true | `double` 1e-20 | `double` 1e-20 | equal |
| `dfcol/fexpr_greatest_big` | true | `double` 1e+30 | `double` 1e+30 | equal |
| `dfcol/filter_dd_eq_zero` | true | `bigint` 0 | `bigint` 0 | equal |
| `dfcol/filter_zero_in_dd` | true | `bigint` 0 | `bigint` 1 | wrong-value |
| `dfcol/greatest_dc_dbig` | true | `double` 1e+30 | raises Arrow error: Cast error: Cannot cast to Decimal128(30, 15). Overflowing on 1e30 | repark-raises-overflow |
| `dfcol/ifnull_dd_dc` | true | `double` 1e-20 | `decimal(30,15)` dec:0E-15 | wrong-value |
| `dfcol/isin_dc_dd` | true | `boolean` false | `boolean` false | equal |
| `dfcol/isin_dd_litdec` | true | `boolean` false | `boolean` true | wrong-value |
| `dfcol/least_dc_dd` | true | `double` 1e-20 | `decimal(30,15)` dec:0E-15 | wrong-value |
| `dfcol/map_dc_dd` | true | `map<string,double>` {a: 1.25 | `map<string,decimal(30,15)>` {a: dec:1.250000000000000, b: dec:0E-15} | wrong-value |
| `dfcol/nullif_dd_zero` | true | `double` 1e-20 | `decimal(30,15)` None | wrong-value |
| `dfcol/nvl2_id_dd_dc` | true | `double` 1e-20 | `decimal(30,15)` dec:0E-15 | wrong-value |
| `dfcol/nvl_dd_dc` | true | `double` 1e-20 | `decimal(30,15)` dec:0E-15 | wrong-value |
| `dfcol/selectexpr_if` | true | `double` 1e-20 | `decimal(30,15)` dec:0E-15 | wrong-value |
| `dfcol/when_dc_dbig` | true | `double` 1e+30 | raises Arrow error: Cast error: Cannot cast to Decimal128(30, 15). Overflowing on 1e30 | repark-raises-overflow |
| `dfcol/when_dc_dd` | true | `double` 1e-20 | `decimal(30,15)` dec:0E-15 | wrong-value |
| `dfcol/withcolumn_union_view` | true | `double` 0.5, 1e-20 | `decimal(30,15)` dec:0.500000000000000 | wrong-value |
| `extra/case_simple_when` | true | `string` 'other' | `string` 'other' | equal |
| `extra/coalesce_nan_card` | true | `double` 1.5, NaN | raises datafusion engine error: Optimizer rule 'simplify_expressions' failed | repark-raises-overflow |
| `extra/cte_union` | true | `double` 0.5, 1e-20 | `decimal(30,15)` dec:0.500000000000000, dec:0E-15 | wrong-value |
| `extra/join_on_mixed` | true | `bigint` 0 | `bigint` 1 | wrong-value |
| `extra/rcte_dbl_anchor` | true | `double` 0.5, 1e-20 | raises Join Error | repark-raises-other |
| `extra/rcte_dec_anchor` | true | raises Py4JJavaError | raises Join Error | both-raise |
| `extra/rcte_dec_anchor_big` | true | raises Py4JJavaError | raises datafusion engine error: Optimizer rule 'simplify_expressions' failed | both-raise |
| `extra/sort_union` | true | `double` -1e-20, 0.0 | `decimal(30,15)` dec:0E-15, dec:0E-15 | wrong-value |
| `extra/subquery_union_filter` | true | `double` 1e-20 | `decimal(30,15)`  | wrong-value |
| `extra/union_nested_cast` | true | `string` '0.1', '1.0E-20' | `string` '0.000000000000000', '0.100000000000000' | wrong-value |
| `extra/union_sum` | true | `double` 0.1 | `decimal(38,15)` dec:0.100000000000000 | type-only |
| `extra/union_three_way` | true | `double` 0.1, 1.0, 1e-20 | `decimal(30,15)` dec:0.100000000000000, dec:0E-15, dec:1.000000000000000 | wrong-value |
| `extra/values_nan` | true | `double` 1.5, NaN | raises datafusion engine error: Optimizer rule 'simplify_expressions' failed | repark-raises-overflow |
| `extra/values_three_rows` | true | `double` 0.1, 1.0, 1e-20 | `decimal(35,15)` dec:0.100000000000000, dec:0E-15, dec:1.000000000000000 | wrong-value |
| `extra/values_two_cols` | true | `double` 0.1, 1e-20 | `decimal(30,15)` dec:0.100000000000000, dec:0E-15 | wrong-value |
| `extra/view_union` | true | `double` 0.5, 1e-20 | `decimal(30,15)` dec:0.500000000000000, dec:0E-15 | wrong-value |
| `extra/where_dec_in_dbl` | true | `bigint` 0 | `bigint` 1 | wrong-value |
| `extra/where_eq_literal` | true | `bigint` 0 | `bigint` 0 | equal |
| `extra/where_in_literal` | true | `bigint` 0 | `bigint` 0 | equal |

## #888's table, folded in

The Spark 4.1.2 recording from #888 (`python/repark/tests/test_nvl_type_coercion_1_spark.json` at the
round-10 fold `54bc9ac2`, ANSI on, `select_utc` 392 cells and `select_ny` 220 cells) was replayed on
main `04d84ab9` ([evidence/raw/replay-888-repark.jsonl](evidence/raw/replay-888-repark.jsonl), probe
[evidence/probes/replay_888.py](evidence/probes/replay_888.py)). 612 cells: 157 EQUAL, 192 type
differences, 102 RePark raises, 46 Spark raises and RePark answers, 115 both raise.

- **Share this root (decimal beside a float):** 2 cells, `nvl(CAST(1.5 AS FLOAT), CAST(1.5 AS
  DECIMAL(10,2)))` and `nvl(CAST(1.5 AS DOUBLE), CAST(1.5 AS DECIMAL(10,2)))`. Both are **EQUAL on main**
  (`double`, `1.5`): DataFusion's `nvl` signature (`Signature::uniform` over a type list with no decimal,
  `nvl.rs:65-95`) casts the decimal to `double`. They become regression pins for this unit.
- **Stay with NVL-TYPE-COERCION-2 (every other divergent cell, 455):** the temporal and string families
  (`timestamp`, `timestamp_ntz`, `date`, `bigint`, arrays typed `string` by the same `nvl` signature,
  about 140 cells); the inverse decimal fault, `nvl` / `ifnull` of a decimal beside an integral or another
  decimal answering `double` where Spark keeps a decimal (26 cells: `decimal(10,2)`, `decimal(12,2)`,
  `decimal(10,4)` → `double`), which sits in the `nvl` signature, not in `decimal_coercion`; the ANSI
  string-cast and `nullif` / `nvl2` families. None of them involves a DECIMAL beside a FLOAT or DOUBLE.
- **Consequence for the build:** the unit must leave `nvl` / `ifnull` on DataFusion's signature (they
  are EQUAL for this root), and NVL-TYPE-COERCION-2's mode-aware rewrite, when it lands, must route the
  decimal-beside-float case to this unit's rule so the two never disagree.

Up: [map.md](map.md).
