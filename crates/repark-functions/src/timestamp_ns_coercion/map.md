# map — repark-functions/src/timestamp_ns_coercion

## Purpose

ICE-TSNS-COERCION-1 slice 1 round 1 (2026-10-12). The gate behind the Spark door's first
`type_coercion` seat: the parent `timestamp_ns_coercion.rs` holds the wrapper, the one-walk
gate, the two process-wide counters and the fallback log-line builder. This round the wrapper
only counts; no statement is rewritten. The line builder exists because no logging crate is
reachable from `repark-functions` without a manifest edit, so emission wires in a later slice.
The counters move onto the session metrics facade when one exists.

Round 2 (2026-10-12) wires the emission without a manifest edit: the wrapper takes an
optional `Arc<dyn Fn(&str)>` sink and calls it once per fallback, and the seat passes a
`tracing::debug!` closure. The parent also gains the all-or-nothing fallback check; the site
rewriter lives in the sibling `../timestamp_ns_coercion_sites.rs`.

- `tests.rs` — the module's `#[cfg(test)]` suite: the gate truth table (a microsecond-only
  table closed; `timestamp_ns`, `timestamptz_ns`, and nanosecond leaves inside a struct, a
  list and a map open; a written cast and a nanosecond literal closed; both cast kernels
  open), the gate-open counter firing once per open analysis and never for a closed one with
  the fallback counter at zero, the reset, the log-line shape, and the closed gate handing
  the inner rule the plan it was given (same table-source `Arc`, byte-identical plan).
  Round 2 adds: the `coalesce`/`array`/`CASE`/`if`/`UNION ALL` kernel-wrap pins for `ns`
  and `tzns` (output type, kernel present, no narrow kernel; `array` asserts a list of
  `ns`), idempotence, one fallback pin per trigger (top-level written cast, try-cast,
  typed NULL, live literal, `to_timestamp` call; a container-of-timestamps cast target
  does not trigger), the leave-alone pins (µs, DATE, STRING via a union
  column because `coalesce(ns, string)` fails plan-build coercion, mixed zones, `nvl`) and
  the never-a-site pins (comparison, `IN`, join keys), plus the sink pins (one line per
  fallback, quiet on rewrite and closed gate, the `has_fallback_sink` accessor).
  pins: ice-tsns-coercion-1/C-005

Fold 1 (2026-10-12) asks DataFusion before it wraps a NULL: each candidate node is run
through `TypeCoercionRewriter` on a clone, and an untyped NULL is wrapped only when the
answer brings it to the site's `Timestamp(ns, _)` together with the nanosecond operand,
so a NULL the answer leaves alone or brings to another type (`Utf8` timezone and
delimiter positions, window offsets, struct fields) is never wrapped and a cast the
answer inserts is never applied, only read. `SiteRewrite::Zoned` carries the zone so the
membership test compares full types. Union columns ask `coerce_union` for the common
schema and wrap a NULL branch only when it equals the gate's target. A rejected rewrite
retries on the original plan with one fallback and the `rewrite was rejected` line. New
pins: each of the twelve shapes' classes matches main's rule alone (`from_utc_timestamp`,
`string_agg`, `lag`, struct UNION, struct CASE, `struct(ns, NULL)` keeping a void second
field), the retry pin, and the four surviving-mutant pins (NULL beside a microsecond
sibling, beside mixed zones, as a CASE WHEN condition, union kernel on the NULL branch
only). pins: ice-tsns-coercion-1/C-005

## Pointers

- Up: [../map.md](../map.md)
