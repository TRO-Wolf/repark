# map — repark-spark/src/extension

## Purpose

File-backed tests for `SparkExtension` (`../extension.rs`): the `configure` hook's
`repark.sql.*` `ConfigExtension` install (r24 SB1 re-home, incl. the fail-loud unparsable-value
contract and **V3-2** `allowCreateFormatVersion3`), **the Spark-door `spark.sql.ansi.enabled` carrier** (U5 / Q10=A — default TRUE;
`notabool` fail-louds; ANSI door never calls this hook), **the Spark-door
`parse_float_as_decimal=true` default** (DEC-1 / U2 — bare
floating-point SQL literals infer DECIMAL, matching Spark; ANSI door never calls this hook),
**and its session-timezone carrier install** (H-1a split B — this hook is the ONE place
`repark-core`'s resolved zone reaches `repark-functions`' extractors, because it is the only crate
that depends on both and the reverse edge is forbidden), and the `register` hook's
function-registry + analyzer-rule installation + the composed
`repark_ta::TaExtension` (the PR-2 TA-omission rider's discharge, pinned bit-exact against the
`repark_ta` kernel).

## Contents

- `tests.rs` — `#[cfg(test)] mod tests;` in `../extension.rs`. U2 pins:
  `configure_defaults_parse_float_as_decimal` (the option is on) and
  `configure_makes_bare_1_23_decimal128_3_2` (collect path, i128=123). U5 pins:
  `configure_defaults_ansi_enabled_true`, `configure_honors_ansi_enabled_false`,
  `configure_refuses_ansi_notabool`. **Q10:** `configure_defaults_timestamp_type_ltz`,
  `configure_honors_timestamp_type_ntz`, `configure_refuses_invalid_timestamp_type`.

## Pointers

- Up: [../map.md](../map.md)

## Debug

| Symptom | First check |
|---|---|
| A `repark.sql.*` key silently ignored | `configure` parses the builder conf map — key spelling vs `repark_functions::cardinality` consts |
| `year`/`hour`/`date_trunc` ignore the session timezone | `configure` must install the carrier (`repark_functions::session_time_zone::with_session_time_zone`) from `SessionBuildConf::session_time_zone`. Pinned by `configure_installs_the_resolved_session_time_zone_carrier`; the end-to-end behavior is `../../tests/session_timezone.rs` |
| `ta_ema`/`ta_adx`/… unknown on a Spark-doored session | `register` must reach `TaExtension.register(ctx)`; see [../../../repark-ta/src/extension/map.md](../../../repark-ta/src/extension/map.md) |
| Bare `1.23` is still Float64 on a Spark-doored session | `configure` must call `apply_spark_float_as_decimal`. Pinned by `configure_defaults_parse_float_as_decimal` + `configure_makes_bare_1_23_decimal128_3_2`. The ANSI door is supposed to stay Float64. |

First checks: `cargo test -p repark-spark extension::`. Escalate to: [../map.md#debug](../map.md).
