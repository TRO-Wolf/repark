# Card NATIVE-CREATE-SCHEMA-NOT-DURABLE-1: a native `CREATE SCHEMA` under an Iceberg catalog makes a schema that is never written

**Date:** 2026-11-13. **Filed by:** the records clerk (`claude-opus-5-5`). **Source:** NATIVE-CTAS-ICEBERG-REFUSE-1
(PR #1317) builder, 2026-11-13 ("found, not carded"); verifier 1, 2026-11-13, finding F5; verifier 2, 2026-11-13,
finding F5.

**Status:** filed 2026-11-13, not dispatched. Severity **S2**: a reported success whose tables are not durable.

## Measured

- **Builder:** a native `CREATE SCHEMA <iceberg catalog>.<name>` answers success and ignores `WITH (location = …)`.
  DataFusion registers a `MemorySchemaProvider` for it (datafusion 54.1.0 `execution/context/mod.rs:1023-1024`), and
  `ReparkCatalogProvider::register_schema` (`crates/repark-iceberg/src/catalog/provider.rs:114`) stores it as given.
  Tables created there live in memory and write no file.
- **Verifier 1:** `CREATE SCHEMA hd.x`, where namespace `x` exists on disk but not yet in the session's provider,
  succeeds and shadows it. A CTAS there stores its rows in memory only (no file written).
- **Verifier 2:** `CREATE SCHEMA hd.real`, over a namespace a second session created, shadows it. A CTAS there
  stores its rows in memory only, and the second session reads `table 'hd.real.k' not found`.
- `hd` is a `type=hadoop` catalog.

**Builds:** #1317's head `ed9f678c` and its base, main `a3f8c7d9`, identical.

## Why it matters beyond the door

#1317's first round failed 26 facade tests (`test_ice_struct_pairing_1_exception.py` 24,
`test_update_set_subquery_walk_1.py` 1, `test_write_body_carriers_1.py` 1), because they seed native tables after
`CREATE SCHEMA <cat>.ns`. #1317's refusal therefore leaves `MemorySchemaProvider` targets alone
([NATIVE-CTAS-ICEBERG-REFUSE-1](native-ctas-iceberg-refuse-1-card-2026-11-12.md)). A fix here moves those seeds.

## Fix direction

None given by the source.

## Close

A native `CREATE SCHEMA` under an Iceberg catalog either creates a durable namespace or refuses, never shadows a
namespace on disk, and is pinned on the memory and hadoop catalogs, with the seeding tests above moved.
