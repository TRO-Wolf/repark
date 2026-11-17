# Card MAP-FILTER-ORDER-1: a map built in the select list meets a row a filter removes, and over-raises where Spark answers

**Date:** 2026-11-17. **Filed by:** the records clerk (`claude-haiku-5-5`), under the owner's ruling "Defaults." (2026-11-17, about 10:40 UTC), ruling 4 ([rulings](../../../wo/ice-v2-flat-aws-label-1/design/rulings-2026-11-17.md)). **Source:** the DECIMAL-COMMON-TYPE-1 build's [bar](../../../wo/decimal-common-type-1/evidence/built/bar.md), its [bar table](../../../wo/decimal-common-type-1/evidence/built/raw/bar-table.md), and the failing cells in [run_revision.py](../../../wo/decimal-common-type-1/evidence/probes/run_revision.py) (lines 22 to 35). Fable (`claude-fable-5-2`), consult reply of 2026-11-17, section 3, Q4.

**Status:** filed 2026-11-17, not dispatched.

**Severity: S2, not label-blocking** (ruled 2026-11-17). The bar names this finding MAP-FILTER-ORDER, without the `-1` suffix. This card is that finding.

## Finding

In both ANSI modes, RePark over-raises where Spark answers on three of the four cells below. On the fourth, both engines raise, with different texts: RePark `NULL_MAP_KEY`, Spark `DUPLICATED_MAP_KEY`. Row 6 of the builder's table holds a NULL map key. The builder's reading is that the physical plan evaluates the map constructor above a filter that moved after it, so the constructor meets that NULL key on a row Spark's filter removes. That reading is not confirmed, and the cause is not diagnosed.

Cause not diagnosed.

The same raise happens on main with no common type involved: `map(a, 'x', CAST(gg AS DECIMAL(38,0)) + 5, 'y')` raises the same way (bar.md).

## Failing cells

The SQL is from [run_revision.py](../../../wo/decimal-common-type-1/evidence/probes/run_revision.py), lines 22 to 35. Every cell is `FROM t WHERE id <= 5`, and row 6 holds the value that would raise.

| cell | SQL | ANSI off | ANSI on |
|---|---|---|---|
| `rev/m1/create_map_keys_apart` | `SELECT id, map(a, 'x', c + 5, 'y') AS r` | over-raise | over-raise |
| `rev/m1/map_concat_keys_apart` | `SELECT id, map_concat(map(a, 'x'), map(c + 5, 'y')) AS r` | over-raise | over-raise |
| `rev/m1/map_keys_of_concat` | `SELECT id, map_keys(map_concat(map(a, 'x'), map(c + 5, 'y'))) AS r` | over-raise | over-raise |
| `rev/m1/map_concat_keys_all` | `SELECT id, map_concat(map(a, 'x'), map(c, 'y')) AS r` | both raise: RePark `NULL_MAP_KEY`, Spark `DUPLICATED_MAP_KEY` | both raise, the same texts |

Modes and the 4-cell count are from [bar-table.md](../../../wo/decimal-common-type-1/evidence/built/raw/bar-table.md).

## Shapes that did not reproduce

Fable's table (Fable (`claude-fable-5-2`), consult reply of 2026-11-17, section 3, Q4). Every cell is `SELECT id, <expr> FROM t WHERE id <= 5`, where row 6 holds the value that would raise. Shape V is a `createDataFrame` view, P a Parquet table, and R the builder's `range` view with `CASE` columns. Shapes V and P ran on main before #1341. Shape R ran on main and on #1350's head.

| expression | shapes | Spark | RePark |
|---|---|---|---|
| `10 / b` (b = 0 on row 6), also inside `array(…)` | V, P, R | answers | answers |
| `10 % b`, `coalesce(10 / b, 0)` | R | answers | answers |
| `CAST(s AS INT)` (s = 'abc' on row 6) | V, P, R | answers | answers |
| `CAST(big AS INT)` (overflow on row 6) | V, P, R | answers | answers |
| `map(a, 'x')`, `map(a, 'x', a + 5, 'y')` (NULL key on row 6) | V, P, R | answers | answers |
| `map_from_arrays(array(a), array('x'))` | V, P | answers | answers |
| `map(a, 'x', c + 5, 'y')` and the `map_concat` form, `a` INT and `c` DECIMAL(10,2) | R | answers | answers |

Fable's readings, as readings:

- No raising expression was evaluated on the removed row, on main or on #1350's head.
- Fable "could not reproduce the builder's raise with an `INT` key beside a `DECIMAL(10,2)` key. So a map above a filter is not enough, and the builder's reading of the cause is not confirmed. The trigger is narrower; the wide decimal key in the builder's cell is the first place I would look."
- "It is not a general class (measured)." Fable's worry, that any raising select-list expression over removed rows would be affected, was tested with the shapes above, and every one of them answered on both engines.

## Build seat

Not diagnosed. The builder's reading names the map projection evaluated above the filter. Fable's reading names the wide decimal key as the first place to look.

Up: [map.md](map.md).
