# Card STRING-DECIMAL-EXPONENT-1: a STRING in E-notation does not cast to DECIMAL

**Date:** 2026-11-15. **Filed by:** the DECIMAL-DOUBLE-UNION-COERCION-1 part 1 builder (`claude-opus-5-5`), from
its step 0′ finding F1. The orchestrator ruled it a card on 2026-11-15.

**Status:** filed; not dispatched. Pre-existing: main `dc6b4c94` and part 1's head answer alike.

**Severity: S2, a silent wrong value on read.**
- `TRY_CAST` answers NULL where Spark answers the number.
- `CAST` refuses where Spark answers. That half is a refusal.
- No store door is affected: an implicit STRING→DECIMAL store refuses on both engines.
- A NULL from `TRY_CAST` that feeds an `INSERT … SELECT` writes NULL, but only through an explicit `TRY_CAST`.

## Measured (Spark 4.1.2 on JDK 17 against main `dc6b4c94` and the part-1 head, both ANSI modes, 2026-11-15)

| text | Spark `CAST` / `TRY_CAST` to DECIMAL(10,2) | RePark `CAST` | RePark `TRY_CAST` |
|---|---|---|---|
| `1.0E-4` | 0.00 / 0.00 | refuses (`Optimizer rule 'simplify_expressions' failed`) | NULL |
| `1E2` | 100.00 / 100.00 | refuses | NULL |
| `1.5e1` | 15.00 / 15.00 | refuses | NULL |
| `-2.5E-3` | 0.00 / 0.00 | refuses | NULL |
| `1.0E-20` | 0.00 / 0.00 | refuses | NULL |
| `1e40` | raises `NUMERIC_OUT_OF_SUPPORTED_RANGE` with ANSI on, NULL with it off / NULL | refuses | NULL |

- `INSERT INTO t(d DECIMAL(10,2)) SELECT 1, '<text>'` refuses on both engines: Spark with
  `INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST`, RePark with `repark_insert_store_assignment`.
- It also shows on RePark's own `CAST(CAST(v AS STRING) AS DECIMAL(p,s))` for any double below 1e-3, whose JDK 17
  text is in E-notation: 332 of 5,455 DOUBLE values and 329 FLOAT values at every type
  (`task/wo/decimal-double-union-coercion-1/evidence/step0b/table.md`, section 6).

Up: [map.md](map.md).
