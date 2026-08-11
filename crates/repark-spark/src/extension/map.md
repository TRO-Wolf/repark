# map — repark-spark/src/extension

## Purpose

File-backed tests for `SparkExtension` (`../extension.rs`): the `configure` hook's
`repark.sql.*` `ConfigExtension` install (r24 SB1 re-home, incl. the fail-loud unparsable-value
contract) **and its session-timezone carrier install** (H-1a split B — this hook is the ONE place
`repark-core`'s resolved zone reaches `repark-functions`' extractors, because it is the only crate
that depends on both and the reverse edge is forbidden), and the `register` hook's
function-registry + analyzer-rule installation + the composed
`repark_ta::TaExtension` (the PR-2 TA-omission rider's discharge, pinned bit-exact against the
`repark_ta` kernel).

## Contents

- `tests.rs` — `#[cfg(test)] mod tests;` in `../extension.rs`.

## Pointers

- Up: [../map.md](../map.md)

## Debug

| Symptom | First check |
|---|---|
| A `repark.sql.*` key silently ignored | `configure` parses the builder conf map — key spelling vs `repark_functions::cardinality` consts |
| `year`/`hour`/`date_trunc` ignore the session timezone | `configure` must install the carrier (`repark_functions::session_time_zone::with_session_time_zone`) from `SessionBuildConf::session_time_zone`. Pinned by `configure_installs_the_resolved_session_time_zone_carrier`; the end-to-end behavior is `../../tests/session_timezone.rs` |
| `ta_ema`/`ta_adx`/… unknown on a Spark-doored session | `register` must reach `TaExtension.register(ctx)`; see [../../../repark-ta/src/extension/map.md](../../../repark-ta/src/extension/map.md) |

First checks: `cargo test -p repark-spark extension::`. Escalate to: [../map.md#debug](../map.md).
