# Small cells: `ROUND` and `TRY_CAST` of a TIMESTAMP (2026-11-14)

Probe [small_cells.py](small_cells.py), session zone UTC; main `d843d37e`, the stores head `d2b517c6`, Spark 4.1.2 on
JDK 17.0.15. `ts` is `2024-01-01 00:00:00.123455`. Each cell answered the same in both ANSI modes (checked).

| cell | main | head | Spark |
|---|---|---|---|
| `round_1.005_2` | 1.0 | 1.0 | 1.01 |
| `round_0.285_2` | 0.28 | 0.28 | 0.29 |
| `round_1.255_2` | 1.25 | 1.25 | 1.26 |
| `round_2.675_2` | 2.68 | 2.68 | 2.68 |
| `round_-22.129547339983247_18` | -22.12954733998325 | -22.12954733998325 | -22.129547339983247 |
| `round_float_1.255_2` | 1.2599999904632568 | 1.2599999904632568 | 1.25 |
| `trycast_ts_dec20_0` | 1794584523005000 | 1794584523005000 | 1794584523 |
| `trycast_ts_dec38_18` | 1704067200123455.000000000000000000 | 1704067200123455.000000000000000000 | 1704067200.123455000000000000 |
| `trycast_ts_double` | 1704067200123455.0 | 1704067200123455.0 | 1704067200.123455 |
| `trycast_ts_bigint` | 1704067200 | 1704067200 | 1704067200 |
| `trycast_ts_int` | None | None | 1704067200 |
| `cast_ts_dec20_0` | 1794584523 | 1794584523 | 1794584523 |
| `cast_ts_dec38_18` | 1704067200.123454970761576448 | 1704067200.123454970761576448 | 1704067200.123455000000000000 |
| `cast_ts_double` | 1704067200.123455 | 1704067200.123455 | 1704067200.123455 |
| `cast_ts_bigint` | 1704067200 | 1704067200 | 1704067200 |

The `ROUND` rows are [ROUND-DOUBLE-EXACT-1](../../../../roadmap/mid-term/round-double-exact-1-card-2026-11-14.md);
the `trycast_ts_*` rows are [TRY-CAST-TIMESTAMP-NUMERIC-1](../../../../roadmap/mid-term/try-cast-timestamp-numeric-1-card-2026-11-14.md).
`cast_ts_dec38_18` is a written cast: it answers main's binary value on the head until step 4 of the split plan.

Up: [map.md](map.md).
