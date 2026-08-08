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
  strings, no backticks) → the async BUG-001 MoR valve → parse via stock DataFusion parser
  machinery (Generic dialect, Q14) → statement match → **on parse/plan FAILURE ONLY** the
  wrong-door sniff (Q10 / graft G3). `FOR …` time travel is PR-6 and is deliberately not
  scanned here. The metadata/`$` stage the ruling names is **implemented as "nothing"**: the
  stock parser accepts `$` in identifiers, so a metadata reference reaches delegation through
  the ordinary `_ =>` arm. A pre-parse `$` passthrough was written first and REMOVED at the
  verify-panel fix pass — see "Verify-panel fixes" below.
- Guard set at the router head (Q12): multi-statement, P11 read-only-catalog DML refuse
  (generic message), the BUG-001 MoR valve over delegated DELETE/UPDATE, SEC-02
  local-filesystem plan refuse, write-to-branch sniff.
- Delegated DML: `INSERT` / `DELETE` / `UPDATE` reach the fork's `TableProvider` (ADR-0003)
  through the delegation core, so M1 SHIPS them — with round-trip rows and the MoR valve wired
  over them. `MERGE` lowering remains M2.
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
set, `MERGE` lowering, the cross-door two-session equivalence rows, `session-api.md`
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
| MoR valve (BUG-001) is in the ANSI guard set | `guards::refuse_mor_multi_spec_dml` | §2 Q12 |

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
| `repark-sql` (M1) | 29 | 14 |

**The Spark door's 5 absences** — `TABLE_OPTION_SORT_ORDER` and
`TABLE_OPTION_UNKNOWN_KEY_REFUSE` (no Spark spelling exists to guard: `TBLPROPERTIES` is a raw
map), `WRONG_DOOR_SNIFF` (the sniff points AT this door), `TA_FUNCTIONS` (PR-4 composes
`TaExtension`; the row FLIPPED to `Tested` in the PR-4→PR-5 sync merge — **sequencing done, and PR-4 was expected to
change it**), `CROSS_DOOR_EQUIVALENCE` (PR-6's two-session protocol).

**The ANSI door's 14 absences** break down as nine M2 deferrals, three standing decisions,
`TA_FUNCTIONS`, and one filed core gap. The nine M2 deferrals are the eight rows citing the `M2`
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
| repark-sql full battery | `cargo test -p repark-sql` | 144 lib + 3 `parser_productions` + 2 `session_wiring` passed, 0 failed, 0 ignored |
| Workspace tests | `cargo test --workspace` (NEVER `--all-features`) | EXIT=0 — **900 passed / 0 failed / 0 ignored** |
| Fast gate | `make ci` | EXIT=0 (fmt, clippy `-D warnings`, panic-ban, crate-dag, lib-rs, check, ruff, taplo, typos) |
| Full CI surface | `make preflight` | EXIT=0 — cargo-deny advisories/bans/licenses/sources ok, 7 workflows parse, zizmor clean |
| Crate-DAG layering | `./scripts/check_crate_dag.sh` | 9 internal edges clean across 6 of 7 mapped crates (`repark-sql` tier 3 → core/iceberg/common only) |
| `lib.rs` manifests | `./scripts/check_lib_rs.sh` | 6 crate roots clean |
| map.md lockstep | `scripts/check_map_md.sh` | clean; new dir `crates/repark-common/src/surfaces/` carries its `map.md` |
| Matrix audits (both doors) | `cargo test -p repark-spark --lib matrix` / `-p repark-sql --lib matrix` | 3 + 3 passed; mutation-verified (an unmapped ID REDs both) |

The **full panel** above ran on the post-fix tree — no §-substitution, no gate skipped. Every
commit on the branch is green under `make ci`.

## Verify-panel fixes (post-assembly, four lenses)

Four verification lenses ran against the assembled branch. What they found, and what changed —
each defect is pinned by a test that RED-ed before the fix.

1. **HIGH — the pre-parse `$` passthrough was a Q15/G1 hole (three lenses reproduced it).** The
   router short-circuited to `delegate()` whenever the SCRUBBED text contained a `$`, BEFORE the
   statement match. So `CREATE TABLE snapbak AS SELECT * FROM ice.sales.orders$snapshots` never
   reached `create_table`: DataFusion's own CTAS built a session-local `MemTable` that read back
   for the rest of the session and vanished with it — verbatim the failure the door's own
   refusal text forbids. The qualified form was broken differently (`register_table does not
   support tables with data`), and `DROP TABLE`/`CREATE SCHEMA` carrying a `$` were delegated
   past their handlers too. **Fix: the passthrough is deleted, not moved.** The stage was
   unnecessary from the start — the stock Generic dialect parses `$` inside an identifier
   (verified: `SELECT … FROM t$snapshots`, `CREATE TABLE … AS SELECT … FROM t$snapshots` and
   `DROP TABLE t$x` all parse), so metadata references reach delegation through the ordinary
   `_ =>` arm. Pin: `router::tests::metadata_reference_does_not_bypass_the_create_handler`.
2. **HIGH — `INSERT_INTO` / `DELETE` / `UPDATE` were false absence rows.** They were marked
   `DeliberatelyAbsent` ("lands with the M2 DML set") while the surfaces were LIVE and mutating:
   the delegation core hands them to the fork's `TableProvider` (ADR-0003), so `INSERT INTO …
   VALUES` committed a snapshot and `DELETE`/`UPDATE` rewrote rows — three Iceberg WRITE surfaces
   shipping with zero tests. That is precisely the failure Q13's typed absence exists to prevent.
   **Fix: the rows are `Tested`,** with round-trip evidence on the Arrow path (value AND type,
   plus a committed-snapshot assertion for INSERT).
3. **HIGH — the BUG-001 MoR valve was unwired (consequence of 2).** `GUARD_MOR_MULTI_SPEC_DML`'s
   deferral rested on "this door's DML rows are M2", which was false; Q12 lists the valve in this
   door's guard set, and M1's `extra_properties` hatch is exactly what makes a merge-on-read
   table creatable here. **Fix: `guards::refuse_mor_multi_spec_dml`** — the ANSI resolution
   wrapper over the tier-1 predicate, called at the router head (async, so it sits beside
   `run_text_guards` rather than inside it). Pinned end to end on a REAL hazard fixture
   (`tests::mor_unpartitioned_multi_spec_dml_refuses`: create merge-on-read + bucket-partitioned
   through this door, drop the partition field via the tier-1 evolution helper, then DELETE and
   UPDATE both refuse) plus every pass-through branch of the wrapper
   (`guards::tests::mor_valve_wrapper_passes_what_it_cannot_or_must_not_gate`).
4. **MEDIUM — the wrong-door sniff false-positived on ANSI-legal SQL (two lenses).** `USING` was
   a bare token, so `SELECT * FROM a JOIN b USING (id)` failing for an unrelated reason (missing
   table) was answered with "this looks like Spark SQL… Drop it" — advice that would break a
   standard ANSI join. Same class for `tag` / `branch` / `namespace` / `database` as ordinary
   column names. The module doc claimed "no false positives that matter" and the only pin was
   four hand-picked non-SQL strings. **Fix: leading-keyword scoping** (`Scope::Leading`) for every
   token with an ANSI reading — `USING` under `CREATE`, the namespace family and BRANCH/TAG under
   the DDL verbs, `CALL … system` only when the statement LEADS with `CALL`. The doc claim is
   downgraded to "bounded false positives" and pinned by
   `sniff::tests::ansi_legal_statements_are_never_steered_to_the_spark_door`, with
   `scoped_rules_still_fire_in_their_own_statement_shapes` proving the scoping did not disarm the
   rules.
5. **MEDIUM — `SQL_DIALECT_SEAM` was evidence that did not match its claim.** The registry defines
   that ID as "the door is reachable through a SESSION"; the cited test called
   `AnsiDialect.execute(EngineContext::new(…))` on a bare `SessionContext`, and nothing anywhere
   installed the dialect on a `ReparkSession` — the door was not shown wired into the product at
   all. **Fix: `crates/repark-sql/tests/session_wiring.rs`**, mirroring the Spark door's
   `*_sessions.rs` precedent: `ReparkSessionBuilder::with_sql_dialect(AnsiDialect)`, then schema
   DDL + CTAS + INSERT + a typed read through `session.sql`, plus a second test proving a door
   REFUSAL survives the session boundary. The matrix row now cites it.
6. **LOW — SEC-02 scope was unstated.** `refuse_local_filesystem_plan` runs only in `delegate()`
   and matches only `CreateExternalTable` / `Copy`, so an intercepted `CREATE TABLE … WITH
   (location = 'file:///…')` never reaches it. That is intended — an intercepted create makes an
   ICEBERG table, whose placement is governed by the catalog's `LocationPolicy` (a stricter,
   per-catalog rule) — but nothing said so. **Fix: documented** in `guards.rs` and
   `src/map.md`. No behavior change.

**Rejected, with reasons:**

- *"`audit()` should verify the named test EXISTS"* (two lenses; renaming a cited test leaves the
  audit green). Real decay risk, correctly described — but not fixable in-process: a Rust test
  binary cannot enumerate its own test names, so the check needs a harness-level gate
  (`cargo test -- --list` diffed against the matrix) living in the Makefile / `.github/`, which
  are orchestrator-only carve-outs for this PR. Recorded here as a PR-6/orchestrator item rather
  than half-built. All 63 `Tested` names across both doors reconcile against `--list` today (two
  lenses verified this independently).
- *"the workspace test count is 890, not 891"* — a counting artifact, not a defect. The current
  count is recorded in the gate table below.

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
