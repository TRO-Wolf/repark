# map — repark-core/src/temp_view

## Purpose

File-backed tests for the temp-view NAME choke point (`../temp_view.rs`): the ONE place a
caller's temp-view name becomes a `TableReference` (SQM round 6, R6-1).

## Contents

- `tests.rs` — the four properties the choke point owes its callers
  (`#[cfg(test)] mod tests;` in `../temp_view.rs`): a one-part name is pinned `Full` against the
  build-time `TempViewHome` (not left `Bare` for the live default catalog to resolve);
  identifier normalization still comes from DataFusion's own `TableReference::parse_str`, so
  `MyView` folds and `"MyView"` does not; and a qualified name refuses at every arity —
  including FOUR parts, which `parse_str` hands back as `Bare { table: "a.b.c.d" }`, so an
  arity check alone would let a qualified spelling through as one oddly-named view. Plus the
  segment overload (`temp_view_ref_from_segment`), the `table_exists` path whose segments arrive
  already quote-stripped: it must NOT re-parse (that refused the allowed `"a.b"` as qualified)
  and must still fold an unquoted `MyView`, which BASE got free from `parse_str`.

The home is TWO things, not one (round-6 critic S1): the build-time `catalog.schema` NAME and the
schema PROVIDER that sat under it. The name alone is not a session-local home —
`datafusion.catalog.default_catalog` is a first-class BUILD-time key, so a session built with
`default_catalog = ice` pins the home name `ice.sales`, and a later `register_memory_catalog("ice")`
replaces the provider it resolves to. MEASURED with the name-only fix: a `required: true` tighten
payload PERSISTED into the Iceberg catalog through `register_record_batches_as_temp_view`. So
`assert_home_intact` re-checks provider identity (`Arc::ptr_eq`) at every entry point and refuses
loud when a catalog has taken the home over — the API can neither write it nor answer for it.

## Pointers

- Up: [../map.md](../map.md)
- The callers that must all resolve through it: `../session/temp_views.rs`.
- The other choke point (planned statements, not names): `../pre_execute.rs`.

## Debug

| Symptom | First check |
|---|---|
| `createOrReplaceTempView` persisted an Iceberg table | A temp-view path bypassed `temp_view_ref` and handed a raw `&str` to `register_table`. Route it through the seam; do not add a per-caller name check. |
| A one-part temp view vanished after `SET datafusion.catalog.default_catalog` | Registration or lookup read the LIVE default instead of `TempViewHome`. Both sides must use the pinned home. |
| Every temp-view call refuses "this session has no session-local temp-view home" | A catalog is registered under the name of `datafusion.catalog.default_catalog` (usually a build-time `default_catalog = <catalog name>`). Point `default_catalog` away from any registered catalog; the API will not write a catalog. |
| A legitimate name is refused | Only qualified spellings refuse. A dot inside a QUOTED name (`"a.b"`) is one identifier and is allowed — check the caller is not double-quoting. |
| Reading `SELECT * FROM v` fails while `tableExists("v")` is true | Known, measured, and a real CHANGE from BASE, not "current behaviour" (round-6 ledger, R6-1 disclosure; pinned by `set_to_a_plain_catalog_keeps_the_write_home_and_moves_only_the_read`): the WRITE is pinned to the home, the READ is still DataFusion's live-default resolution. Name the home (`datafusion.public.v`) or clear the `SET`. |

First checks: `cargo test -p repark-core temp_view`. Escalate to: [../map.md#debug](../map.md).
