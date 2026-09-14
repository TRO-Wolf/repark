# Card ANSI-DOOR-1 — the SQL door honours `spark.sql.ansi.enabled`

**Date:** 2026-09-13 · **Filed by:** run 12b (card only, no implementation) · **Source:** ABS-EXPR-1's door-parity
finding ([abs-expr-1-ledger.md](../../ledgers/staging/abs-expr-1-ledger.md), C-001 evidence "Door-parity finding") and
the `("abs", Kernel(1), …)` row of `EXPECTED_DIVERGENCES` in `crates/repark-python/src/column/door_parity_tests.rs`.

## Why

Since ABS-EXPR-1 the facade's `F.abs` lowers to DataFusion's core `abs` (`checked_abs`), which raises on a signed
minimum, matching Spark 4.1.2 with ANSI on (the default): `[ARITHMETIC_OVERFLOW] integer overflow`. The SQL door
resolves `abs` to datafusion-spark's `SparkAbs`, which reads `execution.enable_ansi_mode`. repark never sets that
option, so the door wraps: `spark.sql("SELECT abs(x) FROM v")` over tinyint −128, smallint −32768, int −2147483648 and
bigint −9223372036854775808 returns the same minimum, and `abs(CAST(-2147483648 AS INT))` and the untyped literal
`abs(-2147483648)` wrap too. The facade and the door give different answers for the same query, and the door's answer
is wrong for Spark with ANSI on. The same flag gates datafusion-spark's other ANSI-aware kernels (arithmetic overflow,
casts), so the gap is wider than `abs`.

## Decisions (proposed)

| id | decision |
|---|---|
| D-1 | Session build sets DataFusion's `execution.enable_ansi_mode` from `spark.sql.ansi.enabled` (default `true`, as in Spark 4.x). A runtime `spark.conf.set("spark.sql.ansi.enabled", …)` updates the session config so the next query plans under it. |
| D-2 | Measure first. Enumerate every datafusion-spark function registered on the SQL door that reads `enable_ansi_mode`, and pin each one's ANSI-on and ANSI-off answers against the live PySpark 4.1.2 oracle before wiring. A kernel whose ANSI-on answer still diverges from Spark stays in `EXPECTED_DIVERGENCES` with its measured reason. |
| D-3 | The `abs` row leaves `EXPECTED_DIVERGENCES` (table 22 → 21) and `test_abs_door_parity_integer_min` flips to "both doors raise". |
| D-4 | ANSI-off (`spark.sql.ansi.enabled=false`) behaviour on the facade is out of scope; if the facade ignores the flag, record that as residue, not a fix here. |

## Steps

| step | worker | what |
|---|---|---|
| 0 | Devin | Measure (live Spark, box alone, `JAVA_HOME=/usr/lib/jvm/zulu-17-amd64`): the D-2 roster and the oracle cells, ANSI on and off. No product code. |
| 1 | Devin | D-1 wiring at session build and on runtime conf set; D-3 flip; pins per door. |

**Home:** the session-build config seam in `crates/repark-core/src/session/` (the `SessionConfig` build), the runtime
conf path in `python/repark/src/repark/spark/session/session_configuration.py`, `door_parity_tests.rs`,
`python/repark/tests/test_abs_expr_1.py`, a new oracle pin file. **Gates:** `make verify`, `make preflight`, the parity
suite, the door-parity ratchet; S2-21 Rust reviewer (per-query config read cost) and Grok critic-logic before the PR.

## Pointers

- Up: [map.md](map.md)
