# map — repark-common/src

CC-4 (2026-08-30): remaining banner files condensed to the one-line rule
(pins: cc-3-comment-condensation/C-009).

## Purpose

Source for `repark-common` — shared types, the `Error` enum, and concise API contracts. See [../map.md](../map.md).

## Contents
- `tests.rs` — unit tests for Error / ErrorClass

- `surfaces.rs` — the dialect-neutral **SQL surface registry** (design
  `docs/design/sql-doors.md` §2 Q13, graft G2): 50 capability IDs (`CTAS`, `MERGE`,
  `TABLE_OPTION_PARTITIONING`, `GUARD_MULTI_STATEMENT`, `SEMANTICS_NULL_ORDERING`, …)
  named by CAPABILITY rather than by spelling, so one ID covers the ANSI `WITH (…)` form
  and the Spark `TBLPROPERTIES` form; plus
  `ALL` (the audit's universe), `Row { Tested { test, profile } | DeliberatelyAbsent { reason,
  adr } }`, `SessionProfile { Unit, Native, SparkExtended, TwoSession }` (graft G5 — evidence
  is only meaningful when the session profile is explicit) and `audit()`, which each door's
  `matrix.rs` calls from a `#[test]`. It lives here, at tier 0, because both tier-3 doors must
  reach it without a door→door edge (design §1). Tests: [surfaces/map.md](surfaces/map.md).

- `spark_error.rs` — the Spark error-condition catalogue (IPI-51 PR2): the
  closed `Condition` enum plus one screaming-cap constant per row, `message()`
  rendering `[CONDITION] … SQLSTATE: XXXXX`, and the `analysis` / `parse` /
  `unsupported` / `illegal_argument` constructors returning `Error`. The
  no-suggestion unresolved-column row preserves Spark's two spaces before
  `SQLSTATE`. Unit tests
  at the bottom of the module pin every row's name, SQLSTATE, and template. Clippy-clean sqlstate() match (merged same-SQLSTATE arms), if-let in substitute, and a test-module Row alias. pins: ice-error-conditions-1/C-010
  **ICE-VIEWS-1 (2026-09-20):** `VIEW_ALREADY_EXISTS` / `VIEW_NOT_FOUND` /
  `CREATE_VIEW_COLUMN_ARITY_MISMATCH` (both directions) rows with unit pins.
  **IPI-40 ALTER VIEW (2026-09-23):** `UNSUPPORTED_FEATURE.CATALOG_OPERATION` refuses view property changes on catalogs without view support with SQLSTATE `0A000`.
  pins: ice-views-1/C-010
  **IPI-40 PR6 (2026-09-24):** the temporary-view rows measured on Spark 4.1.2 —
  `TEMP_TABLE_OR_VIEW_ALREADY_EXISTS` (42P07), `TEMP_VIEW_NAME_TOO_MANY_NAME_PARTS` (428EK),
  `IDENTIFIER_TOO_MANY_NAME_PARTS` (42601), `RECURSIVE_VIEW` (42K0H),
  `INCOMPATIBLE_VIEW_SCHEMA_CHANGE` (51024), `CANNOT_UP_CAST_DATATYPE` (42846) and
  `VIEW_EXCEED_MAX_NESTED_DEPTH` (54K00) — with
  byte-exact unit pins; `template()` carries `#[allow(clippy::too_many_lines)]` as a lookup
  table. pins: ice-views-1/C-018
  **CASESENS-1 S3 (2026-09-27):** the `UNRESOLVED_USING_COLUMN_FOR_JOIN` row
  (42703), measured on live Spark (`USING column … on the left side …`), with
  catalogue + template-param pins (43 conditions). pins: casesens-1/C-009
  **CAST-OVERFLOW-INSERT-1 (2026-09-29):** the `CAST_OVERFLOW_IN_TABLE_INSERT` row
  (22003), measured byte-exact on live Spark 4.1.2 for DOUBLE and DECIMAL(38,0)
  sources, with catalogue + template pins (44 conditions).

- `source.rs` — **C-1 (2026-10-05), the CC-2 identity move:** `SourceKind`
  (`Postgres`, `SqlServer`, `Trino`; `from_spelling` / `spelling` over the exact loader
  spellings `postgres | sqlserver | trino`) moved here unchanged from
  `repark-core/src/config_file/sources.rs`, all three variants (CFG-2 parses `trino`; its
  connector moved to 1.10 without dropping the spelling), and the minimal source identity
  `SourceIdentity { name, kind, generation }`. `generation` is an `Option<NonZeroU64>`, `None`
  meaning not yet assigned (R-12 as amended, owner, 2026-10-05: "generation u64/0-unassigned accepted, with 0 only in the serialized form and an Option or NonZero type in Rust"). `0` belongs only to a serialized form,
  mapped `0` ↔ `None` at that boundary; C-1 serializes no identity, so no `0` exists in Rust.
  1.7's capture assigns it on first use, and re-pointing a configured name at another database
  is detected there (CC-2, CC-9); equality is the only operation C-1 needs. `lib.rs`
  re-exports both. `repark-connect` owns the settings and conversions, `repark-core` the
  loading and handles, `repark-cdc` lineage and offsets.
  pins: c-1/C-002
  **C-1b (2026-10-06), NS-14:** the generation is the named newtype
  `Generation(NonZeroU64)` (`Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord`), and
  `SourceIdentity.generation` is an `Option<Generation>`, `None` still meaning unassigned.
  `Generation::new(u64) -> Option<Generation>` maps `0` to `None`, `get()` returns the
  `NonZeroU64`; there is no `Default` and no `From<u64>`, so a `0` generation cannot be built
  (North Star NS-14: "never a bare `u64` across a function boundary … in Rust it is
  `Option<Generation>` or a non-zero type"; R-12 as amended, owner, 2026-10-05). `new` is the
  serialized `0` ↔ `None` boundary the first serializing unit calls. `lib.rs` re-exports it.
  pins: c-1/C-012

- `redaction.rs` — **SOURCE-URL-REDACT-1 (2026-10-06):** the shared property redactor. `prop_key_is_secret`
  (moved here unchanged from `repark-core/src/catalog_config.rs`, which re-exports it),
  `redact_value(key, value)` (a secret key gives `***`, any other value goes through
  `mask_value_credentials`) and `mask_value_credentials(value)`: URL userinfo of any
  `scheme://` keeps the user and host and masks the password; a userinfo with no colon is
  masked whole; an `@` after `://` with a `:` before it that does not parse cleanly masks the
  whole userinfo (fail closed); a secret-named query parameter or libpq / ODBC keyword
  (`prop_key_is_secret`, or a compact name `sig`, `…pwd`, `…signature`) masks its value,
  quoted, braced and unterminated forms included. Unit pins at the bottom of the module.
  pins: source-url-redact-1/C-001, C-002, C-003, C-004, C-005, C-006
  **SOURCE-URL-REDACT-1 fold 1 (2026-10-06):** userinfo is read only inside the RFC 3986 authority (it ends at the first
  `/`, `?` or `#`), so a path, query or fragment `@` never masks or hides the host; a userinfo
  whose user part carries an `@` is masked whole. pins: source-url-redact-1/C-002, C-015
  **SOURCE-URL-REDACT-1 fold 2 (2026-10-06):** the legs run key=value (parenthesized pairs included), then quoted-JSON and
  YAML colon pairs, then URL userinfo (the authority ends at `/ ? # ;`, not at whitespace; an
  authority that is not a clean host fails closed up to a later `@host`), then Oracle
  `user/password@host` and bare `user:password@host`. `prop_key_is_secret` adds `account_key`,
  `authorization` and a final `pat`; `column_name_is_secret_shaped` keeps the previous rule for
  the secret-column read flag. Tests moved to [redaction/](redaction/map.md).
  pins: source-url-redact-1/C-021, C-022, C-024, C-025, C-026, C-027, C-034
  **SOURCE-URL-REDACT-1 fold 3 (2026-10-06):** the legs record masked spans on the original value (URL userinfo, then Oracle
  and bare logins, then key=value, then JSON/YAML) and a later leg skips what an earlier one
  masked, so key=value never fires inside a userinfo; a storage scheme whose userinfo has no
  `:` is shown as is; Oracle TNS descriptors, fail-closed logins and multi-line JSON/YAML are
  masked. pins: source-url-redact-1/C-041, C-043, C-044, C-045, C-046
- `names.rs` — **WO CASESENS-1 slice 2 (2026-09-27):** the one
  name-matching rule every binder calls. `NameRule { Exact, IgnoreCase }`
  (`from_case_sensitive`, `matches`, `lookup` → `NameHit::{One, Many,
  CaseOnly, None}`) with an inline table test over `["id", "Data", "ID"]`
  under both rules. It lives at tier 0 because every crate that binds names
  already depends on this crate. **WO CASESENS-1 slice 4 (2026-09-27):**
  `folded_duplicate` (the lower-cased name of the first later name equal
  ignoring case to an earlier one) and `column_already_exists` (Spark's
  42711 text) with the `folded_duplicate_reports_the_lower_cased_twin` pin.
  pins: casesens-1/C-005, C-006, C-007, C-008, C-012, C-017
- `lib.rs` — `Error` (variants: `NotImplemented(String)` — the deterministic scope-gate /
  unsupported-feature class (U4: no longer a scaffolding placeholder; `engine_err` folds
  `DataFusionError::NotImplemented` + iceberg `FeatureUnsupported` into it, verbatim `{0}`);
  `DataFusion(String)` — the catch-all engine bucket; `Parse(String)` / `Analysis(String)` — the
  syntax / analysis-plan sub-classes split out so the PyO3 boundary can raise
  `repark.errors.ParseException` / `AnalysisException` (both render the inner engine text
  verbatim, `#[error("{0}")]`, preserving the diagnostic in `str(exc)`);
  `IllegalArgument(String)` — **IO-TEXT-1 round 3 (2026-09-15, U-9):** the verbatim
  `{0}` invalid-argument class for `repark.errors.IllegalArgumentException`;
  `Config(String)` — the
  session/catalog config-mapping error for malformed `spark.sql.catalog.*` /
  `repark.sql.catalog.*` blocks and dual-prefix conflicts; messages name keys, not secret-bearing
  values; `Iceberg(String)` — the iceberg residual (commit conflicts, invalid data, unexpected —
  U4), verbatim `{0}` whose text leads with the structured iceberg kind name;
  `CommitStateUnknown { message, operation_id }` — **ICE-COMMIT-UNKNOWN-1 (2026-09-14):** the
  ambiguous-commit class, carrying the attempted commit's `engine.operation-id` when a RePark
  write path minted one) + `Result<T>`. **FNP-MATH-1 WO-6b R3 (2026-09-21):**
  `Arithmetic(String)` — the verbatim `{0}` ANSI arithmetic class. pins: fnp-math-1/C-004.
  **U7 PR1 round 2 (2026-09-24):** `NumberFormat(String)` — the verbatim `{0}` class for a
  string Java's `Integer.parseInt` refuses (`For input string: "<v>"`), routed
  `NumberFormat → NumberFormat` → `repark.errors.NumberFormatException`, the
  `IllegalArgumentException` leaf. pins: u7-write-df/C-011. Plus
  `ErrorClass { Parse, Analysis, Arithmetic, Unsupported, IllegalArgument, NumberFormat, CommitStateUnknown, Base }` + `Error::exception_class()`
  — the WG-3/U4/Group-X error-taxonomy routing (`NotImplemented → Unsupported` →
  `repark.errors.UnsupportedOperationException`, the PySpark class for a JVM
  `UnsupportedOperationException`; **Group X:** `Config → IllegalArgument` →
  `repark.errors.IllegalArgumentException`, what live pyspark 4.0.0 raises for an invalid
  `SQLConf` value; `Iceberg → Base`; **ICE-COMMIT-UNKNOWN-1:**
  `CommitStateUnknown → CommitStateUnknown` → `repark.errors.CommitStateUnknownException` with
  `operation_id` on the instance; **FNP-MATH-1 WO-6b R3:** `Arithmetic → Arithmetic` →
  `repark.errors.ArithmeticException`): an **exhaustive, no-`_`** match so a new
  variant fails to compile until explicitly routed to a Python exception partition (the "no
  silent default arm" guarantee). Stays at the bottom of the DAG: no heavy deps, so engine errors
  are carried as a formatted string and **classified** into the
  parse/analysis/unsupported/iceberg/base partition by the originating crate
  (`repark-session::engine_err` / `classify_iceberg_error`, which inspect the live
  `DataFusionError` / iceberg `ErrorKind`) before conversion. **Error-boundary honesty
  (C1-CRATE-001):** not every crate returns `repark_common::Error` end-to-end today —
  intermediate layers still surface `iceberg::Result` / `DataFusionError` and fold here.

## Pointers

- Up: [../map.md](../map.md)

## Debug

First checks: `cargo check -p repark-common`. Escalate to: [../map.md#debug](../map.md).
