# The divergence registry — Spark SQL + Iceberg parity

**What this is.** The place this repository records *how repark differs from Apache Spark* and
why — the one home a divergence gets once it has been **disposed of**. Every entry is a **row**,
and a row is only admitted with a live test that pins it. It was **swept on 2026-08-10** (see
"Scope" below): reading the absence of a row is still not a claim that no such divergence exists
outside the method's stated blind spots.

**What this is not.** It is not a status page and it is not a backlog tracker.
[../STATUS.md](../STATUS.md) owns the **state** of each known issue (open / fixed / declared);
this file owns the **semantics** of every divergence that has been disposed of — what repark
does, what Spark does, what pins it, and why it was declared instead of fixed. The two never
restate each other: STATUS links here, and this file links back for state (§6).

---

## 1. How to read a row

Every row carries the same four things, in this order, plus its class:

| Field | What it means |
|---|---|
| **repark** | the behavior this engine actually has, stated as an observable result |
| **Apache Spark** | the behavior it differs from, with the oracle basis that established it |
| **Pin** | `path::test_name` of the live test that holds the row true |
| **Rationale** | why it is DECLARED rather than fixed, or the intent to fix that keeps it BACKLOG |

**Classes.** A **DECLARED** row is a deliberate, permanent-until-revisited difference (§2–§5). A
**BACKLOG** row is a difference we intend to close; its pin codifies today's behavior so the fix
*reds* it on purpose rather than passing unnoticed (§7). §8 is a narrower table: the drop-in
surface repark accepts for source compatibility without reproducing Spark's effect.

**Oracle basis.** Stated on every row's Spark half in §2–§7, because a hand-computed expectation
is not an oracle ([testing.md](testing.md) "Divergence-class claims"). §8 is a table and states
its basis once, in its preamble, because every row there shares it.

- *live* — re-derived from a running Spark by the live-oracle tier on every nightly run, so a
  silent convergence goes RED (see `live-mirror` below).
- *recorded* — recorded once from a live Spark session at authoring time and pinned; drift is
  detected only if the row also carries a live mirror.
- *documented* — taken from Spark's / Iceberg's documented **grammar** or documented
  **semantics**, and admitted **only where no value oracle exists yet**. The clean case is a row
  whose divergence is that repark **refuses a statement form Spark accepts**: the refusal is the
  whole claim and no value oracle is involved. Where the row instead makes a claim about a Spark
  *value* nobody in this repository has observed, the row **says so explicitly and names the unit
  expected to attach a real oracle** — a documented value claim is a placeholder with a deadline,
  not evidence, and it moves to *live* or *recorded* in the change that supplies the oracle.

**`live-mirror:`** — the machine-checked link between a row and the live-oracle tier. A row that
carries `live-mirror: <name>` promises that `python/repark/tests/_live_parity.py` holds a
`Disclosure` of exactly that name. The two sides are checked against each other by
`python/repark/tests/test_parity_live.py::test_disclosures_mirror_the_registry`, which reds if
either side gains or loses a name. The field has **one exact spelling**, given in §6 ("The exact
spelling this gate parses") — a near-miss is a loud failure, not a silent skip. Rows without the
field are pinned JVM-free only; that is a property of what the live tier can express, not a
lesser row.

**Scope — swept on 2026-08-10 (method-bounded, not exhaustive).** It opened the same day with
[ID-1](#id-1--a-quoted-identifier-resolves-case-sensitively) — quoted-identifier case folding,
campaign decision D3 — as the first row admitted at seeding, alongside the rows the sixteen live
citations forced, the cast-failure backlog row, and the four live-tier disclosures. Unit **G-5**
then swept the pre-registry disclosures: a wider inventory over `python/repark/` and `crates/`
(markers and looser phrasing: `divergen`, `disclos`, `differs from Spark`, `unlike Spark`,
`Spark would`, plus the original `DISCLOSED DIVERGENCE` / `KNOWN DIVERGENCE` / `DIVERGENCE-n`
spellings), triage of every hit, and a row for every confirmed queue candidate whose pin asserts
the claim. Full triage and dispositions live in
[../task/g5-sweep-ledger.md](../task/g5-sweep-ledger.md); the historical seed queue remains in
[../task/h1d-ledger.md](../task/h1d-ledger.md) "The sweep queue" with a dated G-5 closure line.
Closing an entry means *moving* its description here, never copying it. §6's
one-authoritative-description rule binds every disposition made **from 2026-08-10 forward**.

**Stated blind spots of the sweep method.** The inventory is a text search, not a semantic proof
of the tree: a disposed divergence described without any of the search terms would not appear; a
comment without a pin cannot become a row (rows require a pin); polars-or-fork-only differences
are out of this registry's Spark scope; and candidate **#1**
(`test_dogfood_gaps.py::test_divergence_timestamp_ltz_collect_passthrough` / DIVERGENCE-1) is
carved out for H-1a split B and was not rowed here.

---

## 2. Statement-surface gaps (DECLARED)

Rows where repark **refuses loud** a statement form Apache Spark's Iceberg extension accepts. The
refusal is deliberate: a silent partial implementation of these forms is the failure mode each row
exists to prevent, and every refusal message names the section it is recorded in.

### 2.1 Iceberg metadata tables

#### MT-1 — time travel composed with a metadata table

- **repark** — `SELECT … FROM cat.ns.t.snapshots VERSION AS OF n` (and the `TIMESTAMP AS OF` /
  `FOR SYSTEM_VERSION AS OF` / `FOR SYSTEM_TIME AS OF` spellings) refuses at planning time with an
  error naming this section. The base table with `AS OF`, and the metadata table without `AS OF`,
  both work — and a query that joins a metadata table to a time-travelled *base* table is
  deliberately **not** caught by this guard.
- **Apache Spark** — accepts the dotted metadata-table reference as a queryable relation.
  *(oracle: documented — the claim here is the refusal, not a value.)*
- **Pin** — `crates/repark-spark/src/tests.rs::metadata_tables_spark_dot_form_and_guards`
- **Rationale** — DECLARED. The composition would have to resolve a snapshot for a relation that
  is itself derived from the snapshot log; getting that wrong silently returns a *plausible* wrong
  history, which is worse than a refusal. Revisit when a workload needs it.

#### MT-2 — the read-only diagnostic on a write to a metadata table

This row is a **diagnostic** divergence, not an outcome one: both engines refuse the write. What
differs is *which* refusal the user sees.

- **repark** — all ten write forms — `INSERT`, `UPDATE`, `DELETE`, `MERGE`, CTAS, `CREATE OR
  REPLACE`, `TRUNCATE`, `CREATE VIEW`, `DROP`, `ALTER` — are caught by one guard *before* routing
  and refuse with a single repark-authored `metadata table … is read-only` error naming the
  offending path. Every one of the ten is exercised by the pin; the row asserts nothing the pin
  does not prove.
- **Apache Spark** — **also refuses**: metadata tables are read-only there too. Its refusal is the
  Spark/Iceberg extension's own diagnostic, in Spark's error-class vocabulary, raised at its own
  point in analysis. *(oracle: documented — Spark's and Iceberg's documented read-only treatment
  of metadata tables. The refusal, not a value, is the whole claim on the Spark side; this row
  makes no claim about Spark's message text.)*
- **Pin** — `crates/repark-spark/src/tests.rs::metadata_tables_spark_dot_form_and_guards`
- **Rationale** — DECLARED and intentionally permanent. It is recorded because the guard is
  load-bearing for MT-1's sibling paths and because a *silent* fall-through — a write to
  `t.snapshots` reaching the planner and failing with something opaque, or worse, rewriting to
  `t$snapshots` and being attempted — is the failure this row exists to keep impossible. The
  engines agree about the outcome; keeping the row means the agreement stays pinned.

> The separate question of whether `$`-suffixed metadata tables should *enumerate* in
> `SHOW TABLES` / `information_schema.tables` was ruled on **2026-08-10** (unit H-1c): **fixed, not
> declared — so no row here** (§6). The decision and its evidence:
> [adr/0006-hide-iceberg-metadata-tables-from-enumeration.md](adr/0006-hide-iceberg-metadata-tables-from-enumeration.md).

### 2.2 Snapshot-ref DDL (`BRANCH` / `TAG`)

Supported surface, for reference:
`ALTER TABLE t CREATE [OR REPLACE] | REPLACE | DROP BRANCH|TAG b [AS OF VERSION n] [RETAIN …]
[WITH SNAPSHOT RETENTION …]` and the top-level `CREATE|DROP BRANCH|TAG b IN t` forms.

> **A `BRANCH`/`TAG` shape the dedicated parser does not reach** is not a separate row, and this
> note says why. `crates/repark-spark/src/router.rs` carries a residual guard that refuses such a
> shape loud (naming the supported grammar and citing this section) rather than letting it fall
> through to a raw parser error — that citation is what brings a reader here. It has **no row**
> because it is unreachable defense-in-depth today: the router's recognizer
> (`normalize::starts_with_branch_or_tag_ddl`) and the ref-DDL parser
> (`ref_ddl::try_parse_ref_ddl`) accept the same shapes, and the parser answers every shape it
> declines with an error of its own — so a statement reaching the residual guard would mean the
> two drifted apart. The recognizer's own boundary is pinned by
> `crates/repark-spark/src/tests.rs::branch_sniff_skips_table_name_segments` (true positives and
> the table-name false positives); REF-2's pin holds the parser's answer for the trailing-token
> shapes. A row lands here the day the guard becomes reachable, with the pin that reaches it.

#### REF-1 — writing to a branch or tag

- **repark** — `INSERT` / `UPDATE` / `DELETE` / `MERGE` targeting `t.branch_<name>` (or the
  `t.branch_name` two-part spelling) refuses loud and names the upstream gap. The read side
  (`VERSION AS OF '<ref>'`) works, and `CREATE|REPLACE BRANCH` re-pinning is the supported write
  path for refs.
- **Apache Spark** — the Iceberg extension writes to the named ref.
  *(oracle: documented.)*
- **Pin** — `crates/repark-spark/src/tests.rs::write_to_branch_refuses_loud_naming_fork_gap`
- **Rationale** — DECLARED, and it is not RePark's fix to make: the pinned iceberg-rust fork's
  snapshot-producing actions always emit the ref update on the main branch, with no commit-target
  API to aim them elsewhere. The alternative to refusing is writing to `main` while the statement
  says otherwise — a silent wrong-target write. Closing it is fork work; capability status lives
  in the fork's own gap matrix, never here.

#### REF-2 — `IF EXISTS` / `IF NOT EXISTS`, and any other trailing clause

- **repark** — every ref-DDL form is parsed to its exact supported shape; a significant token left
  over refuses loud, naming the leftover token and the supported grammar.
  `IF EXISTS` / `IF NOT EXISTS` are the named, known-but-unsupported spellings and stay out.
- **Apache Spark** — accepts `IF NOT EXISTS` on `CREATE BRANCH|TAG` and `IF EXISTS` on
  `DROP BRANCH|TAG`. *(oracle: documented.)*
- **Pin** — `crates/repark-spark/src/tests.rs::ref_ddl_if_exists_spellings_and_trailing_clauses_refuse_loud`
- **Rationale** — DECLARED for now. Silently dropping a trailing clause is the fail-open class
  this door was built to avoid — an ignored `IF NOT EXISTS` turns a no-op into a hard failure,
  and an ignored `IF EXISTS` turns a tolerated miss into one. Refusing keeps the statement's meaning
  honest until the idempotent forms are implemented; the pin reds on purpose when they are.

### 2.3 DML statement forms

#### DML-1 — `INSERT OVERWRITE … PARTITION (…)`

- **repark** — **all** `PARTITION (…)` forms of `INSERT OVERWRITE` refuse, static and dynamic
  alike, and whether the source is empty or not. Whole-table `INSERT OVERWRITE` (no `PARTITION`
  clause) is supported and is a whole-table replace.
- **Apache Spark** — performs a partition-scoped overwrite.
  *(oracle: documented.)*
- **Pin** — `crates/repark-spark/src/tests.rs::empty_insert_overwrite_partition_refuses_full_wipe`
  and `crates/repark-spark/src/tests.rs::insert_overwrite_partition_nonempty_refuses_whole_table_replace`
- **Rationale** — DECLARED until a partition-scoped write path exists. Both degradations are
  destructive and neither is detectable from the result: an empty source would wipe sibling
  partitions, and a non-empty source would silently become a whole-table replace. The documented
  substitute is `DELETE` with a partition predicate followed by `INSERT INTO`.

#### DML-2 — `TRUNCATE TABLE`

- **repark** — refuses with a targeted message naming the substitutes
  (`INSERT OVERWRITE … SELECT … WHERE false`, or `DELETE FROM <table>` with no predicate).
- **Apache Spark** — truncates the table. *(oracle: documented.)*
- **Pin** — `crates/repark-spark/src/tests.rs::truncate_table_refuses_loud_naming_gap`
- **Rationale** — DECLARED until a dedicated truncate action lands. The refusal exists so the
  statement does not fall through to the planner's opaque "unsupported" diagnostic, which tells a
  migrating user nothing about what to write instead.

#### DML-3 — `MERGE INTO` forms outside the supported surface

- **repark** — a `MERGE INTO` shape the door cannot parse refuses with a targeted error pointing
  at this section, rather than surfacing a raw parser error.
- **Apache Spark** — parses the full `MERGE INTO` grammar. *(oracle: documented.)*
- **Pin** — `crates/repark-spark/src/tests.rs::merge_unparsable_form_gets_targeted_error`
- **Rationale** — DECLARED as a *boundary*, not as a permanent gap: `MERGE` is RePark-owned and
  its supported surface grows. The row exists so the boundary is a stated, tested thing instead
  of an accident of the parser.
- **Where the boundary moves next.** The door parses Spark SQL with stock sqlparser's
  `DatabricksDialect` plus token-level normalizers, which is why some Spark/Iceberg extension
  grammar is recognized by hand rather than parsed. A bespoke `ReParkDialect` + Iceberg-extension
  recognizer is the intended replacement, and it arrives with the `MERGE` / `CALL` / branch
  increments — the same increments that widen this row's supported surface. That is the whole of
  the "increment roadmap" `crates/repark-spark/src/normalize.rs` points here for; the schedule
  itself lives in [../STATUS.md](../STATUS.md) and the campaign briefs, never in this registry.

### 2.4 Namespace and table listing statements

#### NS-1 — `SHOW NAMESPACES` without `IN` / `FROM` requires an explicit catalog

- **repark** — bare `SHOW NAMESPACES` (no `IN` / `FROM` catalog) refuses at planning with an
  `AnalysisException` whose message requires an explicit catalog. There is no current-catalog
  fallback for this form.
- **Apache Spark** — uses the current catalog for the bare form. *(oracle: documented — the claim
  here is the refusal form, not a value.)*
- **Pin** — `python/repark/tests/test_show_namespaces.py::test_show_namespaces_disclosed_divergences_fail_loud`
  (the no-`IN`/`FROM` arm)
- **Rationale** — DECLARED. repark has no engine-side current catalog for free SQL; guessing one
  would silently list the wrong place. The facade's `listDatabases` path always supplies an
  explicit catalog. Revisit if engine `USE` / current-catalog state lands.

#### NS-2 — nested `SHOW NAMESPACES IN catalog.namespace` is refused

- **repark** — `SHOW NAMESPACES IN cat.ns` refuses with an `AnalysisException` naming the
  supported one-part `IN <catalog>` form. Nested namespace listing is not implemented.
- **Apache Spark** — lists children of the nested namespace. *(oracle: documented — the claim
  here is the refusal form, not a value.)*
- **Pin** — `python/repark/tests/test_show_namespaces.py::test_show_namespaces_disclosed_divergences_fail_loud`
  (the nested-`IN` arm)
- **Rationale** — DECLARED. repark namespaces are single-level today; an empty frame would read as
  "no children exist" rather than "nested listing is unsupported". Loud refusal keeps that
  ambiguity from laundering into a false empty result.

#### ST-1 — `SHOW TABLES IN …` is unimplemented

- **repark** — `SHOW TABLES IN <catalog>.…` refuses loud with
  `UnsupportedOperationException` naming `SHOW TABLES`. The implemented sibling is the Catalog
  facade method `listTables` (live Iceberg table names + session temp views + DF-schema
  permanents — **not** a global `information_schema.tables` walk), not this SQL form.
- **Apache Spark** — accepts `SHOW TABLES IN …` as a catalog SQL form. *(oracle: documented —
  the claim here is the refusal form, not a value.)*
- **Pin** — `python/repark/tests/test_catalog_surface.py::test_show_tables_in_not_implemented_divergence`
- **Rationale** — DECLARED. The facade path is the supported listing surface; a partial SQL
  implementation that listed the wrong set would be worse than a refusal. `SHOW NAMESPACES IN`
  remains the implemented SQL listing form for databases.

---

## 3. Identifier resolution (DECLARED)

### ID-1 — a quoted identifier resolves case-sensitively

**The first row admitted at seeding (campaign decision D3, 2026-08-10).** It is first by
*declaration*, not by position: §2's rows were back-filled from the sixteen citations that forced
them, and the document is ordered by surface, never by date.

- **repark** — a *quoted* identifier is matched case-**sensitively** through both SQL doors:
  neither the ANSI door's `"ID"` nor the Spark door's `` `ID` `` resolves against a column stored
  as `id`; both refuse. *Unquoted* identifiers agree with Spark — a mixed-case unquoted reference
  resolves to the same column through either door.
- **Apache Spark** — resolves the backticked form case-**insensitively** by default
  (`spark.sql.caseSensitive = false` applies to quoted names too), so `` `ID` `` finds `id`.
  *(oracle: documented.)*
- **Pin** — `crates/repark-sql/tests/cross_door.rs::cross_door_identifier_case_folding_agrees_unquoted_and_diverges_quoted`
- **Rationale** — DECLARED, not fixed. The behavior is inherited engine-wide from stock DataFusion
  resolution; it is not introduced by either door and the doors do not disagree with each other.
  Making quoted resolution case-insensitive means changing identifier resolution engine-wide for
  marginal migration value, which is a deliberate decision rather than a bug fix. Revisit only if
  a workload that actually depends on it turns up. The pin is a **declared-divergence test**: it
  names this section, and it reds if either half of the claim stops being true — including if the
  divergence silently disappears.

### ID-2 — the case-collision refusal covers the SQL-string form only

- **repark** — on a frame carrying both `id` and `ID`, the SQL-string predicate
  `filter("id > 1")` refuses with an ambiguity error, but two spellings bypass that refusal and
  return rows: the `Column` entry point (`df.filter(df["ID"] > 1)`) resolves exact-case-first, and
  an explicitly double-quoted `filter('"ID" > 1')` is a protected span DataFusion then resolves
  case-sensitively.
- **Apache Spark** — raises `AMBIGUOUS_REFERENCE` for the `Column` form, and reads a
  double-quoted span as a string **literal**, raising `CAST_INVALID_INPUT` under ANSI when it is
  compared to a number. *(oracle: live.)*
- **Pin** — `python/repark/tests/test_filter_predicate_rewrite.py::test_column_entry_point_bypasses_the_ambiguity_refusal`
  and `python/repark/tests/test_filter_predicate_rewrite.py::test_explicitly_double_quoted_ident_bypasses_the_ambiguity_refusal`,
  with the guarded half in the same module's
  `test_ambiguous_reference_raises_analysis_exception`
- `live-mirror: filter_case_collision_bypasses`
- **Rationale** — DECLARED. The refusal is deliberately scoped to the rewriter's own surface: the
  two bypassing spellings never reach the SQL-string rewriter at all, so covering them means
  intercepting `Column` resolution and DataFusion's quoted-identifier handling — the same
  engine-wide resolution change [ID-1](#id-1--a-quoted-identifier-resolves-case-sensitively)
  declines. Case-colliding frames are legal in both engines; what is recorded here is which
  spellings are guarded.

### ID-3 — exact duplicate column names are refused at construction

- **repark** — both `createDataFrame([(…)], ["id", "id"])` and `SELECT 1 AS id, 2 AS id` refuse
  at construction / planning with an `AnalysisException` matching `unique expression names`. No
  frame carrying exact-duplicate output names is ever materialised.
- **Apache Spark** — accepts both constructions (e.g. `Row(id=1, id=2)`); the ambiguity surfaces
  later as `AMBIGUOUS_REFERENCE` only when the duplicate name is *referenced*. *(oracle:
  documented — PySpark 4.1.2 API / analysis semantics for duplicate output names.)*
- **Pin** — `python/repark/tests/test_filter_predicate_rewrite.py::test_exact_duplicate_column_names_are_rejected_at_frame_construction`
- **Rationale** — DECLARED. The refusal is inherited from DataFusion's unique-output-name rule
  and is load-bearing for facade helpers that assume unique names (e.g. the filter rewriter's
  exact-duplicate defensive branch). Reproducing Spark's late raise would mean allowing illegal
  frames through the engine.

---

## 4. Type and value semantics (DECLARED)

### TY-1 — `union` of an integer and a string column

- **repark** — coerces to **string** and returns rows: `union(int, string)` has Arrow type
  `Utf8`, and the integer is widened losslessly (`1` → `'1'`). No error.
- **Apache Spark** — coerces to `bigint`, then raises `CAST_INVALID_INPUT` at materialization on
  the non-numeric string; no rows are ever produced. *(oracle: live.)*
- **Pin** — `python/repark/tests/test_union_distinct.py::test_union_int_string_coerces_to_string_diverges_from_ansi_spark`
- `live-mirror: int_union_string`
- **Rationale** — DECLARED. The coercion is DataFusion's common-type choice, and it is *lossless*
  where Spark's is lossy-then-fatal; forcing repark to raise would mean overriding type coercion
  engine-wide to reproduce an error. Recorded so the difference is never laundered into "parity":
  a query that returns two string rows here returns nothing at all on Spark.

### TY-2 — nullability after a scalar-numeric `fillna`

- **repark** — `fillna(0)` makes the filled **integer** column non-nullable (the fill lowers to a
  coalesce).
- **Apache Spark** — is internally inconsistent here: the filled integer column stays **nullable**
  while a filled double column becomes non-nullable. The divergence is on the integer column.
  *(oracle: live.)*
- **Pin** — `python/repark/tests/test_parity_live.py::test_live_disclosure_still_diverges`
  (parameter `fillna_scalar_numeric_nullability`), with the JVM-free value/type half in
  `python/repark/tests/test_na_rename.py::test_fillna_scalar_numeric_value_and_type`
- `live-mirror: fillna_scalar_numeric_nullability`
- **Rationale** — DECLARED. Values and Arrow types agree; only the nullability flag differs, and
  it differs from a behavior Spark is not self-consistent about. Reproducing it would mean
  encoding an inconsistency as a contract. The JVM-free pin deliberately asserts everything
  *except* nullability, so the divergence has exactly one home — this row and its live mirror.

### TY-3 — an inline SQL decimal literal

- **repark** — parses `2.5` in an inline `VALUES` as `double`, and marks inline-`VALUES` columns
  nullable, so `union(VALUES (1), VALUES (2.5))` yields `double` / nullable with `1.0`, `2.5`.
- **Apache Spark** — parses the literal as `DECIMAL(2,1)` and widens the integer into it, yielding
  `decimal128(11,1)` / non-null with `Decimal('1.0')`, `Decimal('2.5')`. *(oracle: recorded.)*
- **Pin** — `python/repark/tests/test_union_distinct.py::test_union_inline_decimal_literal_diverges_from_spark`
- **Rationale** — DECLARED, with narrow impact: stored Iceberg `DECIMAL` columns coerce
  faithfully; only inline decimal *literals* differ. The pin asserts repark's actual output **and**
  asserts that the recorded Spark golden still does not match, so a future convergence reds it.

### TY-4 — `createDataFrame` widens Arrow int32 to int64

- **repark** — a frame whose column is Arrow `int32` (e.g. `CAST(… AS INT)` via SQL) exports
  through `to_polars` as `Int32`, but `createDataFrame` on that polars frame re-infers Python
  `int` as **int64**: the round-trip Arrow type is `int64` / polars `Int64`. Values are preserved.
- **Apache Spark** — with an `IntegerType` schema, preserves int32 through the equivalent
  interchange. *(oracle: documented **value/type** claim under §1's exception — nobody in this
  repository pins Spark's int32 preservation on a live oracle here. The unit that attaches a real
  oracle is **H-2 gap G10** — named by the G-5 owner ruling for facade-boundary interchange
  shapes (G10's slate also budgets map/struct/array container pins); the basis moves to *live*
  or *recorded* in that change.)*
- **Pin** — `python/repark/tests/test_interchange_parity.py::test_to_polars_round_trip_int32_widens_to_int64_divergence`
- **Rationale** — DECLARED until StructType-schema `createDataFrame` (or an equivalent width-
  preserving path) lands. The SQL VALUES path cannot carry Arrow int32 width through Python
  inference; the pin holds both sides of the round-trip so a silent preservation reds it.

### TY-5 — `createDataFrame` widens `Decimal` precision and scale

- **repark** — a column of Arrow `decimal128(10, 2)` / polars `Decimal(10, 2)` round-trips through
  `createDataFrame` as `decimal128(38, 18)` / polars `Decimal(38, 18)`. Numeric values are
  preserved (scale padded).
- **Apache Spark** — through an equivalent schema-preserving interchange keeps a `Decimal(10, 2)`
  (or a precision that is not forced to `(38, 18)` by Python re-inference alone). *(oracle:
  documented **value/type** claim under §1's exception — no live Spark oracle in this repository
  asserts the Spark half yet. The unit that attaches a real oracle is **H-2 gap G10** — named by
  the G-5 owner ruling for facade-boundary interchange shapes; the basis moves to *live* or
  *recorded* in that change.)*
- **Pin** — `python/repark/tests/test_interchange_parity.py::test_to_polars_round_trip_decimal_precision_widens_divergence`
- **Rationale** — DECLARED until a schema-preserving create path exists. Values stay equal; only
  the container precision/scale widens. The pin asserts source and round-trip Arrow + polars
  dtypes so a silent preservation reds it.
### TZ-2 — the session-timezone default is `UTC`

- **repark** — `spark.conf.get("spark.sql.session.timeZone")` is `UTC` on a session that never set
  it; the default is a fixed constant, never the host zone.
- **Apache Spark** — defaults the key to the **JVM's local zone**, so the same job produces
  different wall-clock values on two hosts. *(oracle: documented — Spark's documented
  configuration default. Admitted under §1's exception deliberately: pinning the *live* default
  would pin the CI host's own zone, which is exactly the non-reproducibility this row declares
  against.)*
- **Pin** — `python/repark/tests/test_session_timezone_parity.py::test_session_timezone_conf_is_readable_back_and_defaults_to_utc`
  and `crates/repark-core/src/session_time_zone/tests.rs::absent_key_resolves_to_the_utc_default`
- **Rationale** — DECLARED. A reproducible default beats a host-dependent one, and reading the
  host zone would be the environment read
  [adr/0004-server-prep-disciplines.md](adr/0004-server-prep-disciplines.md) forbids. A job that
  wants host-local behavior sets the key explicitly.

### TZ-3 — a runtime `conf.set` of the session zone is accepted, neither validated nor applied

- **repark** — the call succeeds, warns once (`accepted for source compatibility but NOT
  applied … its value is NOT validated`) and stores nothing, so `conf.get` keeps reporting the
  engine's real zone; `conf.unset` behaves the same. The value is not checked at all —
  `conf.set(key, "Mars/Olympus_Mons")` is swallowed. The disclosure is once per **process**, not
  per session, so a second session in the same interpreter gets a fully silent no-op. The zone is
  resolved and validated exactly once, at session build.
- **Apache Spark** — applies the new zone to the live session immediately, and validates it:
  the same call raises `[INVALID_CONF_VALUE.TIME_ZONE] … SQLSTATE: 22022`. *(oracle: recorded —
  observed against live PySpark 4.1.2 while authoring the unit.)*
- **Pin** — `python/repark/tests/test_session_timezone_parity.py::test_runtime_conf_set_of_the_session_zone_is_accepted_but_not_applied`
  (valid leg, warning text, and the invalid leg under `simplefilter("error")`);
  `…::test_apache_sql_conf_context_manager_round_trips_the_session_zone`;
  `…::test_getorcreate_reuse_with_an_invalid_zone_warns_and_does_not_raise` (the same laxness on
  the reuse path)
- **Rationale** — DECLARED, and evidence-driven: refusing the call reds the pinned Apache drop-in
  test `test_create_dataframe_from_pandas_with_dst`, which sets this key through PySpark's own
  `sql_conf` helper. Accepting keeps drop-in source compatibility; not storing keeps `conf.get`
  honest. The unvalidated half is the accepted cost of keeping exactly **one** validator (the
  engine, at build) — repark is knowingly laxer than PySpark on this key at runtime, and the
  warning says so in as many words. Becomes fixable if the extraction unit routes the zone through
  DataFusion `ConfigOptions` (a live `SET` would then retire this row).

---

## 5. Facade drop-in semantics (DECLARED)

### FA-1 — lateral column aliases in `withColumns`

- **repark** — has no lateral-alias resolution: `withColumns({"x": col("a")+1, "y": col("x")})`
  raises, and so does the reverse dict order.
- **Apache Spark** — resolves a *later* new name against an *earlier* new name laterally, so the
  forward order yields `x=2, y=2`; the reverse order raises `AnalysisException`.
  *(oracle: recorded.)*
- **Pin** — `python/repark/tests/test_dropin_disclosure.py::test_with_columns_lateral_alias_divergence_disclosed`
- **Rationale** — DECLARED. Lateral aliasing is an analyzer feature, not a facade one; reproducing
  it means resolving new names against a projection under construction. The pin asserts **both**
  orders raise, so partial convergence (the forward order starting to work) reds it and forces the
  row and the `withColumns` docstring to be re-recorded together.

### FA-2 — `listDatabases` leaves `description` and `locationUri` as `None`

- **repark** — every `Database` returned by `spark.catalog.listDatabases()` has
  `description is None` and `locationUri is None`. The facade builds rows from
  `SHOW NAMESPACES`, which has no location or description column.
- **Apache Spark** — fills `locationUri` (and may fill `description`) from the catalog metadata
  for each database. *(oracle: documented — field shape / filled location from the Catalog API.)*
- **Pin** — `python/repark/tests/test_catalog_surface.py::test_list_databases_location_uri_none_divergence`
- **Rationale** — DECLARED. Inventing a location would be a silent lie; leaving the fields `None`
  is honest about what the SHOW primitive carries. Revisit if namespace metadata readback gains a
  real location source.

### FA-3 — Python-argument wrappers subclass `RuntimeError`

This row is a **diagnostic / exception-class** divergence (the [MT-2](#mt-2--the-read-only-diagnostic-on-a-write-to-a-metadata-table)
pattern): the claim is about the *error class hierarchy*, not a value.

- **repark** — `PySparkTypeError`, `PySparkValueError`, and `PySparkAttributeError` are
  subclasses of `RuntimeError` (because `PySparkException` subclasses `RuntimeError`). A
  facade arg error such as `df.select(123)` is therefore catchable by `except RuntimeError`.
- **Apache Spark** — in `pyspark.errors` those three wrappers are **not** `RuntimeError`
  subclasses (`PySparkException` subclasses `Exception` there); a broad `except RuntimeError`
  does not catch them. *(oracle: documented — the documented class hierarchy of
  `pyspark.errors`.)*
- **Pin** — `python/repark/tests/test_errors.py::test_python_arg_errors_runtime_error_divergence_is_deliberate`
- **Rationale** — DECLARED. The hierarchy is the near-drop-in decision that keeps
  `except RuntimeError` catching engine failures after migration. The consequence is a strict
  **superset**: everything PySpark catches, repark catches too; a broad `except RuntimeError`
  additionally catches facade arg errors. A future unit that decouples `PySparkException` from
  `RuntimeError` updates this pin — it records today's shape, not a forever contract.

---

## 6. How a row is added, mirrored and retired

**Where a fact lives.** [../STATUS.md](../STATUS.md) is the status SSOT and holds the **state** of
every known issue, including issues that have no disposition yet. This registry holds the
**semantics** of every divergence that *has* been disposed of. The boundary is mechanical:

- an issue with **no disposition** lives only in STATUS, as state **and** as whatever description
  it needs to be understood; it has no row here, and STATUS is its one authoritative home until it
  is disposed of;
- the moment it is disposed — DECLARED (a permanent difference) or BACKLOG (a difference we intend
  to close) — its semantics move here as a row, and STATUS keeps one line of state plus a link;
- an issue that is a **known defect with its fix already scheduled** is not "disposed of" in this
  sense: it is neither declared nor backlogged-as-a-difference, its semantics stay in STATUS, and
  the unit that fixes it removes them from STATUS rather than moving them here;
- nothing is stated in both places. For every disposition made **on or after 2026-08-10**, a
  parity grep must find exactly one authoritative description of the divergence, and it must be a
  row in this file. Pre-registry dispositions that were still described only in test docstrings
  were swept by unit **G-5** (triage and dispositions in
  [../task/g5-sweep-ledger.md](../task/g5-sweep-ledger.md)); closing an entry means moving its
  description here, not copying it. Residual pre-registry pins left deliberately unrowed (e.g.
  DIVERGENCE-1 for H-1a split B) are named in that ledger's carve-outs, not held as a second
  authoritative home.

**Adding a row.** A row lands with its pin, in the same change, or it does not land. There is no
TODO row and no "pin to follow": an unpinned divergence is prose, and prose is what this registry
exists to replace.

**Retiring a row.** A DECLARED row is retired when the decision that declared it is reversed by a
new dated decision; a BACKLOG row is retired when the fix lands. Either way the row's pin goes RED
**on purpose** in the same change that retires it — that RED is the evidence the row was real.

**The live mirror.** The live-oracle tier re-asserts recorded divergences the other way round: it
proves the Spark behavior *still differs*, so a silent convergence reds instead of being laundered
into "parity" ([testing.md](testing.md) "The live oracle tier"). A row that the tier can express
carries `live-mirror: <name>`, and
`python/repark/tests/test_parity_live.py::test_disclosures_mirror_the_registry` checks the two
sides against each other in both directions — a name on one side and not the other is RED. The
mirror keys on the **field**, not on the row's class: a BACKLOG row can be live-mirrored (BL-2 is),
and a DECLARED row need not be (TY-3 is not, because the live tier has no scenario for it).

**The exact spelling this gate parses.** The field is read out of this document by a regex, so its
form is a contract, and a row author edits *this* file rather than the test. A row opts in with a
**top-level list item and nothing else on the line**: a `- ` at column zero, then — inside one
pair of backticks — the field name, a colon, a single space, and the name; then end of line. The
name matches `[a-z0-9_]+` (lower-case, digits and underscores; **no hyphens**). The compiled form
is `python/repark/tests/test_parity_live.py::_LIVE_MIRROR_RE`, and the four live rows in this
document are its worked examples.

Anything else is a **near-miss** and reds loud, naming the offending line: an indented bullet, a
`*` bullet, bold or extra emphasis around the span, a hyphenated name, a trailing parenthetical,
a second sentence on the line. The gate is fail-closed on purpose — a near-miss that merely
*failed to match* would let a row advertise a drift detector nobody checks, which is the exact
condition the mirror exists to make impossible.

---

## 7. Known Spark-parity divergences (BACKLOG)

Differences we intend to close. Each pin **codifies today's behavior** so the fix reds it on
purpose; a pin here is a description, not a contract, and the unit that fixes the class *updates*
the pin rather than obeying it.

### BL-1 — a failing `CAST` raises where non-ANSI Spark yields NULL

- **repark** — a runtime `CAST` of a non-numeric or out-of-range string to a numeric type —
  `CAST('abc' AS INT)` — **raises** an execution-class error at collect, through both the raw
  `spark.sql()` entry point and the DataFrame `Column.cast` entry point.
- **Apache Spark** — in its default **non-ANSI** mode returns **NULL** for the same expression.
  *(oracle: documented — Spark's documented non-ANSI cast semantics. This is a documented **value**
  claim, admitted under §1's narrow exception because no oracle for it exists here yet: nobody in
  this repository has observed the NULL, and neither pin below asserts it. The unit that attaches a
  real oracle is **H-2 gap G6** — cast-failure semantics — whose slate already budgets the
  differential rows and the live-tier disclosures for this class; the basis moves to *live* in that
  same change.)*
- **Pin** — `python/repark/tests/test_errors.py::test_sql_execution_error_raises_base_exception`
  and `python/repark/tests/test_errors.py::test_dataframe_collect_execution_error_raises_base_exception`
- **Rationale** — BACKLOG, intent to FIX. This is a silently-wrong-result class in the migration
  direction that matters: a pipeline that relied on Spark's NULL-on-bad-cast gets a hard failure
  here instead of a null row. It is deliberately **not** in the adversarial SQL corpus
  (`test_sql_passthrough_parity.py`), because that corpus is green-only and repark does not match
  Spark on this class yet. A CAST-parity unit updates both pins to assert NULL.

### BL-2 — backtick-quoted identifiers in a filter string

- **repark** — backticks are not a protected span in the SQL-string filter rewriter, so the token
  inside them is rewritten and then re-quoted by DataFusion into a triple-double-quoted field name
  that resolves to nothing: a backticked identifier in a filter string fails with
  `No field named """x"""`.
- **Apache Spark** — backticks are its own quoting spelling; the predicate simply filters.
  *(oracle: live.)*
- **Pin** — `python/repark/tests/test_filter_predicate_rewrite.py::test_backtick_quoted_identifier_is_not_a_protected_span`
- `live-mirror: filter_backtick_identifier`
- **Rationale** — BACKLOG, intent to FIX. Pre-existing rather than introduced (the rewriter never
  handled backticks), and unrelated to the case-collision work that surfaced it — which is why it
  is a backlog row and not part of
  [ID-2](#id-2--the-case-collision-refusal-covers-the-sql-string-form-only).
  The fix is to treat a backticked span the way a double-quoted span is already treated.

### TZ-1 — timestamp extraction ignores the session zone

- **repark** — `year` / `month` / `dayofmonth` / `hour` / `date_trunc` / `date_format` over a
  TIMESTAMP resolve in the **stored (UTC) zone**; `spark.sql.session.timeZone` does not move
  them. Holds over scalar literals and over a tz-aware timestamp **column** alike.
- **Apache Spark** — resolves every one of them in the **session zone** (the census measured a
  four-hour silent offset in this class). *(oracle: recorded — goldens re-derivable inside the
  repo via `python/repark/tests/_record_session_timezone_goldens.py` against live PySpark 4.1.2.)*
- **Pin** — `python/repark/tests/test_session_timezone_parity.py::test_session_timezone_row_matches_spark_or_still_diverges`,
  the **15 extraction-class disclosure rows** (the module holds 18 disclosure rows + 2 equality
  controls; of the 18, two are TZ-4 and one is TZ-5) — e.g.
  `[hour_of_instant_under_new_york_session]`, `[dst_fall_back_repeated_local_hour]`,
  `[year_boundary_date_trunc_under_tokyo_session]`, and the two column-path rows
  `[column_extract_under_new_york_session]` / `[column_extract_under_tokyo_session]`.
- **Rationale** — BACKLOG, **fix in flight** (campaign decision D7; the extraction unit,
  H-1a split B). Recorded as disclosures so the CRITICAL class is measured while the fix lands;
  the fix flips every row to an equality assertion, and that flip is its revert-red evidence. A
  row that instead starts matching the recorded Spark half reds with a CONVERGED /
  flip-don't-delete message.

### TZ-4 — TIMESTAMP Arrow export is tz-naive

- **repark** — `to_arrow()` yields `timestamp[ns]` (or `timestamp[us]` after `date_trunc`) with
  **no timezone** on the Arrow type.
- **Apache Spark** — `toArrow()` yields `timestamp[us, tz=UTC]`. *(oracle: recorded — including
  the live `current_timestamp` type, `timestamp[us, tz=UTC]`, non-null.)*
- **Pin** — `[to_timestamp_of_zone_suffixed_string]`, `[tz_aware_to_naive_round_trip]` and
  `…::test_current_timestamp_type_and_zone_disclosure` in
  `python/repark/tests/test_session_timezone_parity.py`
- **Rationale** — BACKLOG, **fix in flight** — the export-**type** half of TZ-1's class. A
  consumer that localizes a tz-naive column silently shifts it, which is why the Arrow type is
  asserted and not only the value.

### TZ-5 — `CAST(TIMESTAMP AS BIGINT)` returns nanoseconds

- **repark** — epoch **nanoseconds**: `-1800000000000` for `1969-12-31T23:30:00Z`.
- **Apache Spark** — epoch **seconds**: `-1800` for the same instant. *(oracle: recorded.)*
- **Pin** — `python/repark/tests/test_session_timezone_parity.py::test_session_timezone_row_matches_spark_or_still_diverges[pre_1970_timestamp_cast_to_bigint]`
- **Rationale** — BACKLOG, intent to FIX. Found while authoring the timezone corpus but **not a
  zone bug** — a cast-unit bug, correctly signed before 1970. A 10⁹ factor on every
  timestamp→integer cast is a silently-wrong-result class in its own right; it gets its own unit
  rather than a fold into the extraction fix. State: [../STATUS.md](../STATUS.md) "Known
  correctness issues".

### Surfaced, awaiting pins — not yet rows

Five candidates surfaced by the session-timezone unit carry **no pin yet**, so under §6 they are
not admitted as rows; they are queued here so the surfacing is on the record, and each becomes a
row in the change that lands its pin (the unit ledger `task/h1a-ledger.md` §6 carries the full
observed behavior for each):

- **B-TZ-1** — `unix_timestamp` is not a Spark-door SQL function (the facade `F.unix_timestamp`
  exists; the SQL spelling does not plan).
- **B-TZ-2** — `timestamp_seconds` is not a Spark-door SQL function (same shape as B-TZ-1).
- **B-TZ-3** — `date_add(DATE, <integer literal>)` fails to coerce in the SQL door
  (`date_add(Date32, Int64)` refuses; the DataFrame spelling works).
- **B-TZ-4** — `CAST(TIMESTAMP AS STRING)` returns Arrow `string_view` with ISO-`T` formatting in
  the stored zone, where Spark returns `string` with space-separated formatting in the session
  zone.
- **B-TZ-5** — the SQL `SET` door does not reach the `spark.*` conf namespace at all
  (`Could not find config namespace "spark"`) — pre-existing for every `spark.*` key and wider
  than the session zone; it wants its own decision rather than a fold into the extraction unit.

---

## 8. Drop-in disclosure rationale

The narrow surface where the facade accepts a PySpark call **for source compatibility** without
reproducing Spark's effect — so a migrated script runs, and the difference is disclosed rather
than silent. Every row is pinned in
[`python/repark/tests/test_dropin_disclosure.py`](../python/repark/tests/test_dropin_disclosure.py);
the `Pin` column names the test.

**Oracle basis for this whole table: *documented*** — PySpark's documented API contract for each
named call (what `master(url)`, `setLogLevel`, `spark.version`, `clearCache`, `show(vertical=True)`
are specified to do). It is stated once here rather than per row because every row shares it, and
it is admissible for the same reason §1 gives: what each row claims about Spark is the documented
*effect of an API call*, and the divergence being recorded is repark's side — accepting the call
without reproducing that effect. No row in this table claims a Spark *value* nobody here has
observed; where a value appears (`4.1.2`) it is illustrative of the shape, marked "e.g.".

| Surface | repark | Apache Spark | Pin | Rationale |
|---|---|---|---|---|
| `SparkSession.builder.master(url)` | records the URL, ignores it; **warns once** per process | connects to the named master | `test_master_warns_once` | Single-node by design. A cluster URL must not be *silently* downgraded, so the first call warns; a second is silent, because a script that sets it in a loop should not drown its own logs. |
| `spark.sparkContext.setLogLevel(level)` | accepted, ignored, **no warning** | sets the JVM log4j level | `test_set_log_level_is_documented_silent_noop` | Engine logging is `tracing`-based; there is no JVM level to set. Silent because jobs call it every run — a warn-once here is noise on a call that cannot go wrong. |
| `spark.version` | returns `repark-<version>` | returns the Spark release, e.g. `4.1.2` | `test_spark_version_discloses_repark_not_spark_release` | Honesty over mimicry: returning a Spark release number would make this engine unidentifiable in a log. Scripts log the value; they must not *parse* it as a Spark release. |
| `spark.catalog.clearCache()` | a **real** drop of session cache tables; no warning | drops cached tables | `test_clear_cache_is_real_drop_without_warning` | No longer a disclosure — kept in this table because it *was* one, and the pin asserts the docstring no longer says "no-op". Behavior pins live with the cache tests. |
| `DataFrame.show(vertical=True)` | renders the real `-RECORD` vertical layout; no warning | same | `test_show_vertical_true_no_longer_warns` | No longer a disclosure — implemented. The pin holds the *absence* of the old warning, so a regression that reintroduces the no-op path reds here. |
| `DataFrame.show()` logging | prints to stdout like PySpark; logs only a row-count breadcrumb at INFO, full render at DEBUG | prints to stdout | `test_show_does_not_log_row_data_at_info` | A deliberate divergence in the *logging* dimension: `show()` is a display call, and row data reaching INFO logs is a data-exposure path a drop-in user would not expect. Opt in at DEBUG. |

Two of these rows describe surfaces that have **converged** (`clearCache`, `show(vertical=True)`).
They stay in the table with their pins because the pins now hold the convergence in place: each
asserts the absence of the old disclosure, so a regression to the no-op behavior reds here rather
than quietly restoring a divergence this table once documented.
