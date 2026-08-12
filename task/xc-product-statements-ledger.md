# XC — product statements G3-E3 / G3-E4 / G3-E7 (docs)

**Lane:** XC (overnight conductor #3) · **Branch:** `grok/xc-product-statements` ·
**Freeze:** `9acb566` · **Path:** direct (no ACC/octo) · **Date:** 2026-08-11

## Charter

- A6: new focused doc under `docs/design/` + `docs/design/map.md` + `docs/map.md` lockstep.
- Source rows (read-only): `planning/hardening/G3-engine-intake.md` §Product statements.
- B6: no AGENTS/CLAUDE/STATUS/registry/locks/.github/planning-hardening edits.
- Every claim cites a real test or pinned refusal (re-derived against the tree at freeze).

## Delivered

| Artifact | Path |
|---|---|
| Product contract | [`docs/design/product-contract.md`](../docs/design/product-contract.md) |
| design map | [`docs/design/map.md`](../docs/design/map.md) |
| docs map | [`docs/map.md`](../docs/map.md) |

**Placement rationale:** `docs/design/product-contract.md` — settled consumer-facing product
behavior (not a phase design pass, not the divergence registry). Sibling of the phase designs
because callers and intake rows need one citeable home; ST-1 semantics stay in the registry and
are linked, not copied.

## Cite inventory (re-derived)

### G3-E3

- `test_catalog_surface.py::test_show_tables_in_not_implemented_divergence` (ST-1)
- `test_catalog_surface.py::test_list_tables_*` / `test_show_namespaces_lists_registered_namespaces`
- `test_catalog_staleness.py::test_list_tables_sees_oob_create` (+ siblings)
- `repark-sql/tests/introspection.rs::{show_tables_and_describe_…, introspection_still_refuses_…, information_schema_enumerates_…}`
- `repark-core/src/session/tests.rs::{information_schema_enumerates_…, show_tables_still_refuses_…}`
- Registry ST-1 (link only)

### G3-E4

- `repark-spark/src/tests/router.rs::{bug010_multi_statement_…, bug010_trailing_…}`
- `repark-sql/src/guards/tests.rs::{two_statements_…, single_statement_…, semicolon_inside_…, unparsable_…, multi_statement_refuses_first_…}`
- `repark-spark/tests/dml_sessions.rs::session_sql_bare_dml_applies_eagerly`
- `test_sql_dml_eager.py::test_bare_sql_{insert,delete,update}_applies_*` + exactly-once + failing

### G3-E7

- `test_catalog_staleness.py` (list-on-access + refresh hatch)
- `repark-iceberg/src/catalog/tests.rs::{live_list_sees_oob_…, invalidate_adds_…, invalidate_after_live_table_drop_…, oob_namespace_drop_phantoms_…}`
- `test_catalog_flow.py::test_silver_publish_flow`
- Honest residual: free-SQL OOB staleness — **not** guaranteed; stated plainly

## AGENTS / CLAUDE proposals (ledger only — B6)

None required. Optional morning note: a one-line pointer from AGENTS.md "What repark is" or
STATUS consumer notes to `docs/design/product-contract.md` would improve discoverability; not
blocking.

## docs/testing.md exemption

Pure docs + map lockstep — no testable surface. Exempt under docs/testing.md hard rule 1
("pure docs"). Declared in the PR body.

## Verify

Docs-only; `make verify` map-md / manifest checks as available. No code/tests changed.

## Landing note (L-1, 2026-08-12)

Classified **ALREADY-LANDED** — `docs/design/product-contract.md` is on `origin/main`. No
registry surface (product statements are not Spark divergences). AGENTS pointer remains an
optional morning note.
