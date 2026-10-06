# map — repark-connect

## Purpose

Database connectivity for repark (crate-DAG **tier 1**, role `table service`, peer of
`repark-iceberg`; card 1.6 of
[the design plan](../../task/roadmap/epic-term/roadmap-design-plan-2026-08-29.md), layout of record
[crate-layout-1-8-2026-10-01.md](../../task/roadmap/epic-term/crate-layout-1-8-2026-10-01.md) §1).
C-1 (2026-10-05) created it on its pre-declared footprint (CL-8): the workspace member, this map,
the `repo-manifest.toml` row flipped `planned` → `delivered`, and the one edge
`repark-connect → repark-common` (`normal`) that `scripts/check_crate_dag.py` pre-declared.
The `repark-core → repark-connect` edge arrives with C-2, its first caller (the providers mount).
C-2 also restores its policy row: R-14 took that row out of `ALLOWED_EDGES`, because the DAG
gate's drift rule reds a declared edge with no dependency behind it once both crates are
members. C-1 lands no provider, no read path, no pool and no driver dependency.
pins: c-1/C-001

## Contents

- `Cargo.toml` — workspace inheritance for edition, version, license, rust-version, repository,
  publish and lints (the `repark-iceberg` precedent, so `unsafe_code = "forbid"` rides the
  workspace). Dependencies: `repark-common`, `arrow`, `thiserror`, all at workspace versions.
  `tokio-postgres`, `sqlx` and `tiberius` wait for the read and write paths (C-2 onward).
  pins: c-1/C-011
- `src/` — [src/map.md](src/map.md): `settings.rs` (the connection settings and the reserved
  auth-method field) and `types/postgres.rs` (the Postgres ↔ Arrow type map).
- `tests/` — [tests/map.md](tests/map.md): the one integration binary `tests/it/main.rs`.

## Design bars (R-10)

The 1.6 release row's standing instruction (owner, 2026-09-13) asks every unit to name which of
the external bars shaped it. **Arrow ADBC** shaped C-1's type map: the Arrow side of each mapped
row is the type the ADBC PostgreSQL driver's documented type-mapping table names for that
Postgres type (a documented basis, not a run of the driver; C-2's live cells against the C-0
container are the first measurement), and a row where ADBC and another reading disagree (or where Postgres has values the Arrow type cannot hold) is
declared, not chosen. **ConnectorX** shapes the partitioned reads; nothing in C-1 reads, so it is
recorded as not yet (C-2, C-3). pins: c-1/C-010

## I want to...

| ...do this | go to |
|---|---|
| Interpret a new connection key | `src/settings.rs`, in the unit that first consumes it (CC-3: no key is defined ahead of its unit) |
| Map a declared Postgres type | `src/types/postgres.rs`: flip the row, add its codec arm and a round-trip pin named in the row, retire its registry row |
| Add the SQL Server map | `src/types/mssql.rs` beside `postgres.rs` (C-5) |

## Component contract

- **Owns:** connection settings (`ConnectionSettings`, `AuthMethod`) and database-specific type
  conversion (`postgres::POSTGRES_TYPES` and its wire codec). CC-2's split puts the source
  identity in `repark-common`, loading and the user-facing handles in `repark-core`.
- **Does not own:** profile loading, precedence, registration (`repark-core`); the source
  identity (`repark-common::SourceIdentity`); lineage, offsets and capture state (`repark-cdc`,
  1.7); endpoint keys (C-2, CC-3).
- **Public inputs:** one source's prop map (`BTreeMap<String, String>`, as the core loader
  produces it); Arrow arrays and Postgres binary wire values.
- **Public outputs:** `ConnectionSettings` or a `SettingsError`; `PostgresTypeRow::encode` /
  `decode`, or a `TypeMapError`. `SettingsError` folds into `repark_common::Error`:
  `InvalidAuthMethod` → `Config` (IllegalArgument class, CC-4's invalid specification),
  `DeclaredAuthMethod` → `NotImplemented` (Unsupported class).
- **State & lifecycle:** stateless value types and one const table.
- **Allowed internal deps:** `repark-common` only (declared `normal` in the DAG gate).
- **Failure model:** typed `thiserror` enums; no panics in product code.
- **Extension points:** new auth methods (flip a declared refusal), new type rows, new backends.
- **Test strategy:** `tests/it/` pins every settings branch and one round trip per mapped type.
- **Known limitations:** the declared rows (registry `CONNECT-DECL-*`) and no connector yet.

## Pointers

- Up: [../map.md](../map.md)
- Ledger: [c-1-ledger.md](../../task/ledgers/staging/c-1-ledger.md)
- Registry rows: [docs/spark-sql-iceberg-parity.md](../../docs/spark-sql-iceberg-parity.md) §5,
  the `CONNECT-DECL-*` family beside `SES-DECL`. pins: c-1/C-009

## Debug

| Symptom | First check |
|---|---|
| `crate-dag` red on a new edge | the edge must be pre-declared in `scripts/check_crate_dag.py`; a tier-1 service never reaches up to `repark-core` |
| A row's pin is "not a test here" | `type_map_has_one_row_per_type_and_a_live_pin_per_row`: the row's `pin` must name a `fn` in `tests/it/postgres_types.rs` |

First checks: `cargo test -p repark-connect`. Escalate to: [../map.md#debug](../map.md).
