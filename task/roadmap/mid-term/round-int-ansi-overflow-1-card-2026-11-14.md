# Card ROUND-INT-ANSI-OVERFLOW-1: `ROUND(2147483647, -1)` wraps where Spark with ANSI on raises

**Date:** 2026-11-14. **Filed by:** the ROUND-DOUBLE-EXACT-1 builder (`claude-opus-5-5`), on the
orchestrator's ruling at that unit's step 0 (question 5, "a separate card").
**Status:** filed, not dispatched.
**Severity:** S2. With ANSI on, an overflowing integer `round` returns a wrapped value instead of raising.

## Measured (main `954423e0`, Spark 4.1.2 on JDK 17)

The cells are the INT controls of ROUND-DOUBLE-EXACT-1's step 0 table
([domain-table.md](../../wo/round-double-exact-1/evidence/step0/domain-table.md), the misc `ctl_int` rows).
Eight cells are VALUE-FOR-ERR with ANSI on on the Spark-door SQL and `F.expr` doors; with ANSI off, all
eight match Spark.

| cell | Spark, ANSI on | RePark, ANSI on |
|---|---|---|
| `ROUND(CAST(127 AS TINYINT), -1)` | `ARITHMETIC_OVERFLOW` | -126 |
| `ROUND(CAST(32767 AS SMALLINT), -1)` | `ARITHMETIC_OVERFLOW` | -32766 |
| `ROUND(CAST(2147483647 AS INT), -1)` | `ARITHMETIC_OVERFLOW` | -2147483646 |
| `ROUND(CAST(9223372036854775807 AS BIGINT), -1)` | `ARITHMETIC_OVERFLOW` | -9223372036854775806 |

The four negative extremes behave the same way. `bround` is RePark's own kernel, and it raises as Spark
does.

## Where it is decided (read)

- The integer arms of `round` are datafusion-spark's `SparkRound`
  (`datafusion-spark-54.1.0/src/function/math/round.rs`, `round_integer`).
- That function reads ANSI from DataFusion's `execution.enable_ansi_mode` (`:123`), which RePark never
  sets. RePark's own carrier is `spark.sql.ansi.enabled`, read through
  `repark_functions::ansi::spark_ansi_enabled_from_options`.
- Since ROUND-DOUBLE-EXACT-1, `F.round` reaches the same integer arm, so all three Spark doors wrap.

## Close

`round` of an integral type with ANSI on raises `ARITHMETIC_OVERFLOW` when the rounded value leaves the
type, on every Spark door, and still wraps with ANSI off. The eight step 0 cells are the pin.

Up: [map.md](map.md).
