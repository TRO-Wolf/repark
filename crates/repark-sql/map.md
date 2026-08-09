# map — repark-sql

## Purpose

The **ANSI/Trino-flavoured SQL door** (tier 3) — NEW code, not a port. `AnsiDialect` implements
the frozen phase-1 `repark_core::SqlDialect` seam and routes one statement at a time. The door
**delegates** wherever DataFusion is already right (reads, `information_schema`, temp views, the
fork `TableProvider`'s DML) and intercepts only the Iceberg catalog DDL DataFusion cannot
express. There is deliberately **no `SessionExtension`**: native/ANSI semantics ARE stock
DataFusion, which is why cross-door equivalence must run two sessions (extensions are
session-scoped).

Design SSOT: [../../docs/design/sql-doors.md](../../docs/design/sql-doors.md) §2 (Q1–Q15).
Milestone ledgers: [p2f-ansi-m1-ledger.md](../../docs/history/port-v2/p2f-ansi-m1-ledger.md) (M1 /
PR-5) and [p2g-ansi-m2-ledger.md](../../docs/history/port-v2/p2g-ansi-m2-ledger.md) (M2 / PR-6).

**The door is CLOSED as of M2 (PR-6).** M1 landed the crate spine + `AnsiDialect`, the guard set
(multi-statement FIRST, P11 read-only-catalog DML, write-to-branch, SEC-02 local-filesystem),
the error-path wrong-door sniff, the `CREATE TABLE` family (CTAS + column-def) with the curated
`WITH (…)` vocabulary, `extra_properties`, partitioning and Q15 loud-refuse routing,
`CREATE`/`DROP SCHEMA` and `DROP TABLE`. M2 added ALTER (+ `SET PROPERTIES`, `RENAME TO`), the
MERGE lowering, `FOR … AS OF` time travel, the ALTER-scoped branch/tag DDL, the completed refuse
set, Q8 introspection and the two-session cross-door rows. `src/matrix.rs` now reads 39 tested /
4 deliberately absent, and every remaining absence is a standing design ruling, not a deferral.

## Contents

- `Cargo.toml` — deps: `repark-core`, `repark-iceberg`, `repark-common`, `datafusion`, plus
  `iceberg` (staged create/replace types) and `async-trait` (the seam is an async trait).
  **No direct `sqlparser`** (types come only through `datafusion::sql::sqlparser`) and **no
  `datafusion-spark`** — the design's hard constraint, so this door cannot reach Spark semantics
  through a crate edge. **Dev-dependencies only:** `repark-spark` (the two-session cross-door
  protocol needs both doors in one test binary) and `repark-ta` (the Q11 toll). Both are
  **declared in the dependency policy as `dev` edges** (`scripts/check_crate_dag.py`
  `ALLOWED_EDGES`): visible and reasoned about, exempt from the layering rule, and RED the moment
  either is promoted to `normal` — `repark-sql → repark-spark` as a product edge is precisely the
  forbidden door→door edge. Nothing in `src/` may name them.
- [src/map.md](src/map.md) — module-by-module navigation.
- [tests/map.md](tests/map.md) — integration tests: the R1 parser-production pins, the
  two-session `cross_door.rs` rows, Q8 `introspection.rs`, the Q11 `ta_toll.rs`.

## I want to...

| ...do this | go to |
|---|---|
| Follow a statement through the door | [src/map.md](src/map.md) → `router.rs` |
| Change what `WITH (…)` accepts on CREATE TABLE | `src/properties.rs` |
| Change partition-transform parsing / validation | `src/partitioning.rs` |
| Understand why an unqualified CREATE TABLE refuses | `src/create_table.rs` (Q15 routing) |
| Add or adjust a guard | `src/guards.rs` |
| Add a Spark-ism to the wrong-door steer | `src/sniff.rs` |
| See what this door does NOT do, and why | `src/matrix.rs` (typed absence rows) |

## Component contract

- **Owns:** the ANSI/Trino-flavoured SQL door (NEW code) — `AnsiDialect`, the guard set
  (multi-statement first, P11 read-only DML, write-to-branch, SEC-02 local-filesystem), the wrong-door
  sniff, the Iceberg catalog DDL it intercepts (CREATE TABLE family + curated `WITH (…)`, schema DDL,
  ALTER, MERGE lowering, `FOR … AS OF` time travel), and the typed-absence matrix.
- **Does not own:** Spark semantics (deliberately no `datafusion-spark`, no `SessionExtension` —
  native = stock DataFusion); the shared Iceberg machinery; the Spark door.
- **Public inputs:** a `SessionContext` + `CatalogRegistry` + ANSI SQL text (one statement at a time).
- **Public outputs:** DataFusion `DataFrame`s; intercepted Iceberg DDL commits; loud refusals for
  out-of-scope forms.
- **State & lifecycle:** per-call routing; `FOR … AS OF` registers ephemeral pinned relations released
  right after planning (they never accumulate, never appear in `SHOW TABLES`).
- **Allowed internal deps:** `repark-core`, `repark-iceberg`, `repark-common`. **No `sqlparser` /
  `datafusion-spark`.** Dev-only: `repark-spark` + `repark-ta` (cross-door tests + the Q11 toll) —
  not product edges; nothing in `src/` may name them.
- **Failure model:** `DataFusionError`; on a parse / plan failure the wrong-door sniff upgrades the
  error (the original stays the first line).
- **Extension points:** change the `WITH (…)` vocabulary (`properties.rs`); partition parsing
  (`partitioning.rs`); a guard (`guards.rs`); a wrong-door steer (`sniff.rs`).
- **Test strategy:** `cargo test -p repark-sql` — R1 parser-production pins, the two-session
  `cross_door.rs` rows, Q8 introspection, the Q11 ta-toll.
- **Known limitations:** `matrix.rs` reads 39 tested / 4 deliberately absent; every remaining absence
  is a standing design ruling, not a deferral.

## Pointers

- Up: [../map.md](../map.md). Sibling door: [../repark-spark/map.md](../repark-spark/map.md)
  (no door→door dependency edge, ever — design §1).
- Surface registry (shared, tier 0): `repark_common::surfaces`.

## Debug

| Symptom | First check |
|---|---|
| `CREATE TABLE … is not a qualified Iceberg table name` | Q15: the leading segment must be a REGISTERED Iceberg catalog; the message lists them |
| `unknown table property` on a create | The curated set is in `src/properties.rs`; dotted Iceberg keys go through `extra_properties = MAP(…)` |
| `cannot resolve a storage location` | The schema has no `location` property and the catalog is `RequireExplicitLocation` — set it on the schema or per-table |
| An error suddenly mentions Spark | The wrong-door sniff fired on the ERROR path (`src/sniff.rs`); the original error is still the first line |
| `SHOW TABLES` / `information_schema` empty or missing | Build the session with `.config("datafusion.catalog.information_schema", "true")` — the builder now carries `datafusion.*` keys through to `SessionConfig` (PR-6 R2 fix); without the conf the refusal is DataFusion's own |
| A `FOR … AS OF` query fails to parse | The scanner runs AFTER the multi-statement refuse and BEFORE the parse (`src/time_travel.rs`); bare `VERSION AS OF` (no `FOR`) is the Spark spelling and steers |

First checks: `cargo test -p repark-sql`. Escalate to: [../map.md#debug](../map.md).
