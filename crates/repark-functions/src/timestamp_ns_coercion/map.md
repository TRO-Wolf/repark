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
  `ns`), idempotence, one fallback pin per trigger (a surviving top-level written cast,
  try-cast, typed NULL, live literal, `to_timestamp` call; a container-of-timestamps
  cast target does not trigger, and neither do written `TIMESTAMP_NTZ` and nanosecond
  casts, which reach the wrapper as kernel calls), the leave-alone pins (µs, DATE, STRING via a union
  column because `coalesce(ns, string)` fails plan-build coercion, mixed zones, `nvl`) and
  the never-a-site pins (comparison, `IN`, join keys), plus the sink pins (one line per
  fallback, quiet on rewrite and closed gate, the `has_fallback_sink` accessor).
  pins: ice-tsns-coercion-1/C-005

Narrow fold (2026-10-13): `feed_tests.rs` pins the feed rule — a read site is left to
main, an open gate without a feed hands the inner rule the plan it was given, a `Dml`
position feed and a kernel-argument feed are rewritten, feeds behind alias/subquery,
bare projection, sort and filter are rewritten, reading filters/sorts, double reads
and every other plan node stop the walk, a consumer at the feed root is left to main,
a site left inside a rewritten root refuses once, a microsecond target and a raw
planner cast are not anchors. pins: ice-tsns-coercion-1/C-005

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
field), the retry pin, and four pins aimed at the first verifier's surviving mutants (NULL
beside a microsecond sibling, beside mixed zones, as a CASE WHEN condition, union kernel on
the NULL branch only). Of those four only the union pin killed its mutant; fold 2 re-pinned
the other three. pins: ice-tsns-coercion-1/C-005

**Slice 1 fold 2 (2026-10-13), the three re-pins.** Each was shown red on its mutant and
green after the restore (mutant diffs: the second verifier's `m4`, `m5`, `m7`).
- `null_beside_a_microsecond_sibling_is_not_wrapped` (m4, a microsecond operand classed as
  a nanosecond one): `coalesce(ns, µs, NULL)` and `coalesce(µs, NULL)` equal main's rule
  alone and hold no kernel, and so does a call whose signature takes two nanosecond
  timestamps over `(µs, NULL)`. Only the last form kills m4: DataFusion's common type of
  `ns` and `µs` is `µs`, so under `coalesce`, `array`, `greatest`, `if` and `CASE` the ask
  never answers the site target and the mutant changes no plan. A fixed nanosecond
  signature makes the ask answer `Timestamp(ns)` for the NULL, and then only the operand
  class keeps the NULL unwrapped.
- `null_beside_mixed_ns_and_tzns_follows_the_mixed_rule` (m5, mixed zones treated as one
  type): both operand orders, main's rule alone, neither kernel. The order
  `(tzns, ns, NULL)` is the one that kills m5; with `ns` first the mutant's target is the
  naive type, the ask answers the zoned one, and nothing is wrapped.
- `null_case_when_condition_is_not_a_site` (m7): `CASE WHEN NULL THEN ns ELSE NULL END`
  keeps the WHEN operand a NULL literal and the THEN operand the column, and wraps only the
  ELSE branch.
- `aggregate_with_a_non_timestamp_null_argument_matches_main` now calls
  `string_agg(ns, NULL)` and `string_agg(tzns, NULL)` and asserts no kernel; it dies on the
  ask-skipped mutant (`n1`), which the old string-column form survived.
pins: ice-tsns-coercion-1/C-005

## Pointers

- Up: [../map.md](../map.md)
