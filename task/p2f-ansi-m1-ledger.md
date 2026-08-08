# Unit ledger — P2F: the ANSI door, milestone 1 (PR-5)

**Unit:** phase-2 PR-5 · **Brief:**
[../briefs/phase-2-sql-doors.md](../briefs/phase-2-sql-doors.md) §1 "PR-5" · **Design:**
[../docs/design/sql-doors.md](../docs/design/sql-doors.md) (LAW for this unit) · **Status:**
IN FLIGHT · **Parallel with:** phase-2 PR-4 (repark-ta)

**This is NEW code, not a port.** There is no census, no rename map, no pin to diff against.
The discipline that replaces them is [../docs/testing.md](../docs/testing.md)'s native rule:
test-per-change, in the same commit — every handler branch, every refusal, every property
mapping. Arrow-path assertions (value AND type), never `show` alone. The matrix audit
(design §2 Q13) is the structural backstop: a surface cannot go missing quietly.

## Scope

`crates/repark-sql` (tier 3, pre-declared in `scripts/check_crate_dag.py`), M1 half:

- `AnsiDialect: SqlDialect` over the FROZEN phase-1 seam (`execute(EngineContext<'_>, &str)` —
  design §3; no core-side hooks, guards are door-called).
- Router order, exactly as ruled: multi-statement refuse FIRST (quote-aware; double-quote
  strings, no backticks) → metadata/`$` passthrough → parse via stock DataFusion parser
  machinery (Generic dialect, Q14) → statement match → **on parse/plan FAILURE ONLY** the
  wrong-door sniff (Q10 / graft G3). `FOR …` time travel is PR-6 and is deliberately not
  scanned here.
- Guard set at the router head (Q12): multi-statement, P11 read-only-catalog DML refuse
  (generic message), SEC-02 local-filesystem plan refuse, write-to-branch sniff.
- CTAS / `CREATE TABLE … WITH (…)`: the curated vocabulary (`format`, `format_version`,
  `partitioning`, `location`) + the FUNCTIONAL `extra_properties = MAP(ARRAY[…], ARRAY[…])`
  hatch (graft G4) + the two reserved refusals (`sorted_by`, ORC/AVRO — graft G9) + the
  unknown-BARE-key refuse that lists the curated set.
- Q15 routing (graft G1): leading segment resolved against `EngineContext.catalogs`;
  registered Iceberg catalog → staged create/replace with the LocationPolicy 3-way (including
  `TempFallbackAllowed { root }`, exactly as the Spark door); ANYTHING else → LOUD refuse
  requiring qualification. **Never a silent MemTable fallthrough.**
- `CREATE`/`DROP SCHEMA WITH (location = …)` on the same catalog ops the Spark door uses;
  `DROP TABLE [IF EXISTS]`.
- The Q13 machinery, both doors (WS2): `repark_common::surfaces` + `matrix.rs` + audit.

Out of scope (M2 / PR-6, each row present in the matrix as a typed absence): ALTER handlers,
MERGE lowering, `FOR … AS OF` + the double-quote pin set, branch/tag ALTER DDL, the full refuse
set, DML delegation rows, the cross-door two-session equivalence rows, `session-api.md`
seam-freeze edits.

## Decisions applied (design § cited)

| Ruling | Where it lands | Cite |
|---|---|---|
| No direct `sqlparser`, no `datafusion-spark` in this crate | `crates/repark-sql/Cargo.toml` | §1 table, §2 Q14 |
| Stock DF parser, Generic dialect, no bespoke `Dialect` | router parse stage | §2 Q14 |
| Multi-statement refuse runs FIRST | router head | §0 defect fixes, §2 Q5 |
| SEC-02 IS in the ANSI guard set | guard set | §0 defect fixes, §2 Q12 |
| Wrong-door sniff is error-path ONLY; backticks refuse | router failure path | §2 Q10 / G3 |
| Curated `WITH` vocab + functional `extra_properties`; dotted keys ONLY via the hatch | create-option parsing | §2 Q1 / G4 |
| `sorted_by` + ORC/AVRO refuse loud, naming the trigger | create-option parsing | §2 Q1 / G9 |
| Unknown BARE key refuses loud listing the curated set | create-option parsing | §2 Q1 |
| Transform validation re-implemented as a small pure fn (no half-file move) | partitioning module | §2 Q2 |
| CTAS routing refuses loud on anything but a registered Iceberg catalog | Q15 routing | §2 Q15 / G1 |
| Typed surface registry + per-door matrix + compile-run audit | `repark-common` + both doors | §2 Q13 / G2 |
| Cross-door rows need TWO sessions — not faked at M1 | matrix absence rows | §2 Q13 / G5 |
| Every deferral names its equivalent + trigger | matrix absence rows | §6 R5 |

## Day-1 spikes (design §6 R1 / R2)

Recorded with evidence before the handlers were written, because both outcomes change the
build: R1 decides whether M2 needs the ~50-LOC pre-parse recognizer fallback, and R2 decides
whether an introspection gap is a core/fork issue rather than a door parser.

Evidence is checked in as reproducible test targets, not hand notes — R1 lives in
`crates/repark-sql/tests/parser_productions.rs`
(`cargo test -p repark-sql --test parser_productions -- --nocapture`) and R2 in
`crates/repark-sql/src/tests.rs::information_schema_enumerates_registered_iceberg_catalogs`.
Both run against the pinned DataFusion 54.1 re-export (`datafusion::sql::sqlparser`, Generic
dialect) and the pinned fork.

### R1 — sqlparser productions (design §6 R1)

**Everything PR-5 needs parses on the stock Generic dialect** — `CREATE SCHEMA … WITH
(location = …)` (`Statement::CreateSchema { with: Some(Vec<SqlOption>) }`), `DROP SCHEMA`,
`DROP TABLE`, CTAS + column-def `CREATE TABLE … WITH (…)`, `ARRAY['month(ts)', …]` as
`Expr::Array`, and the G4 hatch `MAP(ARRAY[…], ARRAY[…])` as `Expr::Function{ name: MAP }`.
`sorted_by` parses too, and therefore reaches the handler where it refuses loud with its
trigger named (G9) rather than being silently dropped. **The R1 fallback was NOT needed in
PR-5; no pre-parse recognizer was written.**

**PR-6 inherits three confirmed recognizer obligations** (the design predicted exactly this):

| production | parses? | consequence for M2 |
|---|---|---|
| `ALTER TABLE … SET PROPERTIES (…)` | **NO** — `Expected: (, found: PROPERTIES` | needs the v1-proven ~50-LOC pre-parse recognizer (sqlparser models `SET TBLPROPERTIES` / `SET (…)`, not the Trino spelling) |
| `ALTER TABLE … EXECUTE …` | **NO** | Q7's loud refuse must run pre-parse or on the error path; PR-5's sniff already carries the `EXECUTE` token, so M2 upgrades it from steer to a dedicated refuse |
| `SELECT … FOR VERSION\|TIMESTAMP AS OF …` | **NO** — `FOR` is consumed as `FOR UPDATE/SHARE` | confirms the Q5 token-scan rewrite: G7's quote-parameterized scanner is load-bearing, not optional |

Branch/tag ALTER DDL (Q6) is the same class — expect a recognizer.

### R2 — information_schema enumeration (design §2 Q8)

Two halves that disagree, which is the whole finding:

- **Through `ReparkSession` (the product path): NO.** `information_schema` is OFF and cannot be
  turned on — the builder's `.config(k, v)` map is repark/spark-shaped (consumed by
  `parse_catalog_specs`, the concurrency readers, the S3-region resolver, the extension
  `configure` hook) and never reaches the `SessionConfig`'s DataFusion options, so
  `datafusion.catalog.information_schema = true` is silently inert. `SHOW TABLES` fails with
  "not supported unless information_schema is enabled".
- **On a raw DataFusion context with it forced on: YES, fully.** Namespaces enumerate in
  `information_schema.schemata`; tables enumerate in `tables` / `SHOW TABLES`. The
  `ReparkCatalogProvider` / fork `SchemaProvider` are correct.

**Verdict — two core-side items, per Q8 ("gaps are core/fork fixes, not door parsers"):**

1. **CORE GAP (blocks Q8 delegation in BOTH doors):** `ReparkSession` must be able to enable
   `information_schema` — either flip it in the session defaults or thread `datafusion.*` keys
   from the builder map into `SessionConfig`. **This is why the ANSI matrix carries
   `INTROSPECTION` as a `DeliberatelyAbsent` row citing this spike instead of a Tested row.**
   Filed against core/PR-6.
2. **PRODUCT QUESTION (fork/core, low severity):** the fork's 16 `$`-suffixed metadata tables
   enumerate as `BASE TABLE` alongside the real table; Trino hides these from `SHOW TABLES`.
   Decide whether `repark-iceberg::catalog`'s `SchemaProvider::table_names` should filter them
   before Q8 is declared delivered. Recorded, not fixed here.

Neither spike changed the PR-5 plan: R1 retired its own risk for this PR and sharpened M2's
scope to three named recognizers; R2 converted an open design question into a filed core gap
plus one product question, with the enumeration machinery itself proved working.

## Surface matrix (Q13) — the row counts this PR lands

`repark_common::surfaces::ALL` = **43** capability IDs. Both doors map all 43; the audit test
(`matrix_maps_every_surface`) fails on any unmapped ID, any stale ID, any duplicate, and any
untraceable row (a `Tested` with no test name, a `DeliberatelyAbsent` with no reason/ADR).

| Door | Tested | DeliberatelyAbsent |
|---|---|---|
| `repark-spark` | 38 | 5 |
| `repark-sql` (M1) | 25 | 18 |

**The Spark door's 5 absences** — `TABLE_OPTION_SORT_ORDER` and
`TABLE_OPTION_UNKNOWN_KEY_REFUSE` (no Spark spelling exists to guard: `TBLPROPERTIES` is a raw
map), `WRONG_DOOR_SNIFF` (the sniff points AT this door), `TA_FUNCTIONS` (PR-4 composes
`TaExtension`; the row flips there — **sequencing, and the integrator should expect PR-4 to
change it**), `CROSS_DOOR_EQUIVALENCE` (PR-6's two-session protocol).

**The ANSI door's 18 absences** break down as 13 M2 deferrals, three standing decisions,
`TA_FUNCTIONS`, and one filed core gap. The 13 M2 deferrals are the 12 rows citing the `M2`
const plus `BRANCH_TAG_DDL`, whose reason is M2 but which cites §2 Q6 (the ruling that names it
the first deferral candidate if M2 overruns). The three standing decisions — absent by ruling,
not by sequencing — are `ALTER_TABLE_PARTITION_FIELDS` (Q3 — deferred from SQL entirely, the
callable op is the answer), `INSERT_OVERWRITE` (Q9 — omitted, Trino-faithful, dbt-trino evidence
per graft G10), and `MAINTENANCE_CALL` (Q7 — callable ops only, the ADR pin stands).
`TA_FUNCTIONS` (Q11) is neither: it is opt-in per session and its ANSI toll rides M2 once PR-4
lands the extension. The core gap is `INTROSPECTION` (Q8 — see R2 above).

Session profiles are recorded per row (graft G5). The ANSI matrix carries a test —
`ansi_rows_are_native_or_unit_only` — that forbids ANSI evidence gathered on a Spark-extended
session, because extensions are session-scoped: an extended session has Spark expression
semantics through EVERY door, so such evidence would describe the Spark analyzer, not this
door. Neither door may claim the `TwoSession` profile before PR-6.

## Gate table

| Gate | Command | Result |
|---|---|---|
| repark-common unit tests | `cargo test -p repark-common` | 13 passed (2 pre-existing + 11 new) |
| repark-common lints | `cargo clippy -p repark-common --all-targets` | clean |
| Spark-door matrix audit | `cargo test -p repark-spark --lib matrix` | 3 passed |
| ANSI-door matrix audit | `cargo test -p repark-sql --lib matrix` | 3 passed |
| repark-sql full battery | `cargo test -p repark-sql` | _WS1 / integrator_ |
| Workspace | `make verify` / `make preflight` | _integrator_ |
| map.md lockstep | `scripts/check_map_md.sh` | new dir `crates/repark-common/src/surfaces/` carries its `map.md` |

## Deviations

1. **`repark-common` is a DEV dependency of `repark-spark`, not a regular one.** Design §1
   lists it in that door's dep set, but the only consumer today is `src/matrix.rs`, which is
   `#[cfg(test)]`. A regular dep would be an unused edge in the shipped crate. It graduates the
   day product code reads the registry. (`repark-sql` takes it as a regular dep — WS1's
   handlers name `repark_common::Error` at seams.)
2. **`Row::Tested` carries a `profile` field** beyond the design's `Row::Tested { test }`. Q13
   also requires "each matrix row records its session profile"; folding it into the variant is
   what makes that mechanical rather than a comment convention.
3. **The `SurfaceId` newtype + `surface_ids!` macro were merged in from a parallel workstream's
   competing draft of `surfaces.rs`** (see the collision note below). Keeping the newtype means a
   matrix row cannot be keyed by an arbitrary string that merely looks like an ID; deriving the
   wire name via `stringify!` from the constant's own identifier also makes the copy-paste class
   (`pub const DELETE = SurfaceId("UPDATE")`) unrepresentable.
4. **`audit()` also rejects untraceable rows** (empty test name / empty reason or ADR) — not in
   the design's letter, but a row that cites nothing is indistinguishable from the oversight
   the registry exists to prevent.

## Collision note (for the integrator)

Two workstreams wrote `crates/repark-common/src/surfaces.rs` in the same worktree. WS2's version
(43 capability IDs, `SessionProfile`, file-backed tests) was overwritten mid-flight by a second
draft (28 coarser IDs, `SurfaceId` newtype + `surface_ids!` macro, inline tests, `audit_matrix`).
**Resolved by merge, not by reverting either:** the newtype + macro were adopted (strictly better
typing — see deviation 3), the 43-ID vocabulary + `SessionProfile` + the four-failure-mode
`audit(door, rows)` were kept, and the tests are file-backed per house style. Both doors' matrices
compile and pass against the merged API. If a reviewer prefers the coarser 28-ID vocabulary, the
change is mechanical but touches both matrices — say so before PR-6 builds on these IDs.
