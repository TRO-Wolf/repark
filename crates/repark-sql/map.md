# map — repark-sql

CC-4 (2026-08-30): remaining banner files condensed to the one-line rule
(pins: cc-3-comment-condensation/C-009).

## Purpose

The **ANSI/Trino-flavoured SQL door** (tier 3) — NEW code, not a port. `AnsiDialect` implements
the frozen `repark_core::SqlDialect` seam and routes one statement at a time. The door
**delegates** wherever DataFusion is already right (reads, `information_schema`, temp views, the
fork `TableProvider`'s DML) and intercepts only the Iceberg catalog DDL DataFusion cannot
express. There is deliberately **no `SessionExtension`**: native/ANSI semantics ARE stock
DataFusion, which is why cross-door equivalence must run two sessions (extensions are
session-scoped).

Design SSOT: [../../docs/design/sql-doors.md](../../docs/design/sql-doors.md) §2 (Q1–Q15).
Historical delivery records live in the [archived port-v2 ledgers](../../docs/history/port-v2/map.md);
current behavior is described below and in the source and test maps.

The crate owns `AnsiDialect`, its guards, wrong-door sniff,
Iceberg DDL handlers, MERGE lowering, time travel, branch/tag DDL, refusals, and the surface
matrix. Stock DataFusion handles delegated reads and DML.

## Contents

- `Cargo.toml` — deps: `repark-core`, `repark-iceberg`, `repark-common`, `repark-functions`
  (F-Y10-1: `AnsiDialect.on_session_built` installs checked integer overflow), `datafusion`, plus
  `iceberg` (staged create/replace types) and `async-trait` (the seam is an async trait).
  **No direct `sqlparser`** (types come only through `datafusion::sql::sqlparser`) and **no
  `datafusion-spark`** — SparkExprSemantics stays on SparkExtension. **Dev-dependencies only:** `repark-spark` (the two-session cross-door
  protocol needs both doors in one test binary) and `repark-ta` (the Q11 toll). Both are
  **declared in the dependency policy as `dev` edges** (`scripts/check_crate_dag.py`
  `ALLOWED_EDGES`): visible and reasoned about, exempt from the layering rule, and RED the moment
  either is promoted to `normal` — `repark-sql → repark-spark` as a product edge is precisely the
  forbidden door→door edge. Nothing in `src/` may name them.
- [src/map.md](src/map.md) — module-by-module navigation.
- [tests/map.md](tests/map.md) — integration tests: the R1 parser-production pins, the
  two-session `cross_door.rs` rows (incl. G11 intended divergences), F-Y10-1
  `cross_door_int_overflow.rs`, Q8 `introspection.rs`,
  the Q11 `ta_toll.rs`, the G11 ANSI-door value pins (`ansi_door_values.rs`), the
  Native-profile pins (`ansi_door_join_null_keys.rs`, `ansi_door_window_frames.rs`,
  `ansi_door_float_agg.rs`).

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
- **State & lifecycle:** per-call routing; successful time-travel rewrites release both ephemeral
  names after planning. A core name can remain if post-registration lookup fails before the frame returns.
- **Allowed internal deps:** `repark-core`, `repark-iceberg`, `repark-common`. **No `sqlparser` /
  `datafusion-spark`.** Dev-only: `repark-spark` + `repark-ta` (cross-door tests + the Q11 toll) —
  not product edges; nothing in `src/` may name them.
- **Failure model:** `DataFusionError`; on a parse / plan failure the wrong-door sniff upgrades the
  error (the original stays the first line).
- **Extension points:** change the `WITH (…)` vocabulary (`properties.rs`); partition parsing
  (`partitioning.rs`); a guard (`guards.rs`); a wrong-door steer (`sniff.rs`).
- **Test strategy:** `cargo test -p repark-sql` — R1 parser-production pins, the two-session
  `cross_door.rs` rows (incl. G11 intended divergences), Q8 introspection, the Q11 ta-toll,
  G11 ANSI-door value pins (`tests/ansi_door_values.rs`), and Native-profile pins
  (`tests/ansi_door_join_null_keys.rs`, `tests/ansi_door_window_frames.rs`,
  `tests/ansi_door_float_agg.rs`).
- **Known limitations:** `matrix.rs` reads 46 tested / 4 deliberately absent; the four
  statement-surface absences are standing design rulings. The three `SEMANTICS_*`
  pin-absences (window frames, JOIN NULL keys, float determinism) are tested.
  **A11:** column-def `CREATE TABLE` refuses nanosecond `TIMESTAMP` / `TIMESTAMP(9)`
  (DDL needle). `TIMESTAMP(6)` is the supported spelling. CTAS / ALTER / Spark door
  are not this refuse. Base audit findings, 2026-08-29:
  - `BF-CC2-SQL-001` (S1): quoted catalog identifiers bypass the text-level read-only DML guard.
  - `BF-CC2-SQL-002` (S1): service-managed CREATE can commit, then return an invalidation error
    without undoing the table.
  - `BF-CC2-SQL-003` (S2): quoted branch targets bypass the text guard, but the current planner
    rejects four-part names before execution.
  - `BF-CC2-SQL-004` (S2): a user table that squats on a reserved time-travel name can be removed
    during registration or cleanup.

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
| `SHOW TABLES` / `information_schema` empty or missing | Build the session with `.config("datafusion.catalog.information_schema", "true")`; without the setting the refusal is DataFusion's own |
| A `FOR … AS OF` query fails to parse | The scanner runs AFTER the multi-statement refuse and BEFORE the parse (`src/time_travel.rs`); bare `VERSION AS OF` (no `FOR`) is the Spark spelling and steers |

First checks: `cargo test -p repark-sql`. Escalate to: [../map.md#debug](../map.md).
