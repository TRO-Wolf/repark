# The doors outside the card (sketch §4, seat A risk 1)

What `hour(c)`, `minute(c)` and `second(c)` answer over a `STRING` on the doors that might run DataFusion's
coercion without `SparkLtzTimestampCast` after it, on main `81ae984e` and the head `58b2725e`. Not fixed in this
unit.

## The Rust `AnsiDialect` door

A throwaway `repark-sql` integration test (not committed; `N/string-hms-out/rustprobe/zz_shms_door_probe.rs`, run in
a detached worktree of each commit with `cargo test -p repark-sql --test zz_shms_door_probe`): one-row `t(c STRING)`,
`SELECT hour(c), minute(c), second(c) FROM t`, the same over the text as a literal, and the written
`CAST(c AS TIMESTAMP)` control, in `UTC` and `America/Los_Angeles`, on three sessions.

| session | door | main | head |
|---|---|---|---|
| `SparkExtension` + `SparkDialect` | `sql_with(SparkDialect)` (the Spark door) | `hour(CAST(t.c AS Date32))`: `0,0,0` for `full`, `frac`, `off`, `zulu`; `Cast error` for `lead`, `nosec`, `bad` | `hour(to_timestamp(t.c))`: Spark's answers (Los Angeles `full` `3,4,5`, `off` `13,34,5`, `zulu` `19,4,5`, `nosec` `3,4,0`; UTC `off` `21,34,5`); `bad` `CAST_INVALID_INPUT` |
| the same session | `sql_with(AnsiDialect)` | the same as the Spark door | the same as the Spark door: the session's analyzer rules, `SparkLtzTimestampCast` included, run whatever the statement dialect |
| `AnsiDialect` only, no extension | `sql_with(AnsiDialect)` | `[UNRESOLVED_ROUTINE] Cannot resolve routine hour` | the same |

The literal form answers as the column on every row; the `CAST(c AS TIMESTAMP)` control answers alike on both
commits. Raw: `N/string-hms-out/rustprobe/head.log`, `main.log`.

## The native `repark.sql()` door

`N/string-hms-out/native_door.py` on each package (`sweep/base/native.jsonl`, `sweep/head/native.jsonl`): `hour`,
`minute`, `second` over a `VALUES` string column, over the literal, and `hour(CAST(c AS TIMESTAMP))`, for `date`,
`full`, `frac`, `off`, `zulu`, `lead`, `nosec`, `bad`: every query answers
`[UNRESOLVED_ROUTINE] Cannot resolve routine hour` (SQLSTATE 42883) on both packages. The native door registers no
`hour`; seat A cannot reach it.

## Reading

Risk 1 does not materialize on the head: a session with the Spark extension plans `hour(to_timestamp(c))` on both
dialects, and a session without it has no `hour`. No door answers a zoneless text read as UTC through seat A's cast.
