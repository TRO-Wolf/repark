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
[history/hardening-h1/g5-sweep-ledger.md](history/hardening-h1/g5-sweep-ledger.md); the historical seed queue remains in
[history/hardening-h1/h1d-ledger.md](history/hardening-h1/h1d-ledger.md) "The sweep queue" with a dated G-5 closure line.
Closing an entry means *moving* its description here, never copying it. §6's
one-authoritative-description rule binds every disposition made **from 2026-08-10 forward**.

**Stated blind spots of the sweep method.** The inventory is a text search, not a semantic proof
of the tree: a disposed divergence described without any of the search terms would not appear; a
comment without a pin cannot become a row (rows require a pin); polars-or-fork-only differences
are out of this registry's Spark scope; and candidate **#1**
(`test_dogfood_gaps.py::test_divergence_timestamp_ltz_collect_passthrough` / DIVERGENCE-1) was
carved out for H-1a split B, which **revisited it on 2026-08-10 and left it unrowed** (that
disposition also carries a dated closure line in the home §6 designates,
[history/hardening-h1/g5-sweep-ledger.md](history/hardening-h1/g5-sweep-ledger.md)) — its
subject is the `CAST(… AS TimestampType())` passthrough, not extraction, and the class that keeps
it open is already described by TZ-4 and TZ-6. Its docstring now says so instead of instructing a
reader not to build the session-tz machinery that has since been built.

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
- **Pin** — `crates/repark-spark/src/tests/metadata_tables.rs::metadata_tables_spark_dot_form_and_guards`
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
- **Pin** — `crates/repark-spark/src/tests/metadata_tables.rs::metadata_tables_spark_dot_form_and_guards`
- **Rationale** — DECLARED and intentionally permanent. It is recorded because the guard is
  load-bearing for MT-1's sibling paths and because a *silent* fall-through — a write to
  `t.snapshots` reaching the planner and failing with something opaque, or worse, rewriting to
  `t$snapshots` and being attempted — is the failure this row exists to keep impossible. The
  engines agree about the outcome; keeping the row means the agreement stays pinned.

> The separate question of whether `$`-suffixed metadata tables should *enumerate* in
> `SHOW TABLES` / `information_schema.tables` was ruled on **2026-08-10** (unit H-1c): **fixed, not
> declared — so no row here** (§6). The decision and its evidence:
> [adr/0006-hide-iceberg-metadata-tables-from-enumeration.md](adr/0006-hide-iceberg-metadata-tables-from-enumeration.md).

#### F-V4-1 — timestamptz identity partition metadata projection

This row is a **metadata-projection** capability gap, not a statement-form refuse:
the CTAS/INSERT succeeds. It lives here because the refuse is the Iceberg
`.files` / `.partitions` inspect, next to the other metadata-table rows.

- **repark** — identity-partition of an Iceberg `timestamptz` column writes and
  round-trips rows, but `table.files` / `table.partitions` refuse to project the
  partition struct (`FeatureUnsupported`: `Timestamptz is not supported` in the
  `data_file` metadata projection).
- **Apache Spark** — the same CTAS/INSERT projects
  `{"ts":"2024-01-01T04:30:00.000000Z"}` (and the companion instant) from
  `.files` / `.partitions`. *(oracle: recorded.)*
- **Pin** —
  `python/repark/tests/test_partition_value_audit.py::test_partition_value_row[carry_identity_timestamp_ctas]`
  and
  `python/repark/tests/test_partition_value_audit.py::test_partition_value_row[carry_identity_timestamp_insert]`
- **Rationale** — DECLARED, fork-wave-routed. Fork inspect (`data_file` metadata
  projection). Not a TZ-4 representation miss (data values match). Do not "fix" in
  repark by skipping the meta read.

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
> `crates/repark-spark/src/tests/ref_ddl.rs::branch_sniff_skips_table_name_segments` (true positives and
> the table-name false positives); REF-2's pin holds the parser's answer for the trailing-token
> shapes. A row lands here the day the guard becomes reachable, with the pin that reaches it.

#### REF-1 — writing to a branch or tag

- **repark** — `INSERT` / `UPDATE` / `DELETE` / `MERGE` targeting `t.branch_<name>` (or the
  `t.branch_name` two-part spelling) refuses loud and names the upstream gap. The read side
  (`VERSION AS OF '<ref>'`) works, and `CREATE|REPLACE BRANCH` re-pinning is the supported write
  path for refs.
- **Apache Spark** — the Iceberg extension writes to the named ref.
  *(oracle: documented.)*
- **Pin** — `crates/repark-spark/src/tests/ref_ddl.rs::write_to_branch_refuses_loud_naming_fork_gap`
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
- **Pin** — `crates/repark-spark/src/tests/ref_ddl.rs::ref_ddl_if_exists_spellings_and_trailing_clauses_refuse_loud`
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
- **Pin** — `crates/repark-spark/src/tests/insert_overwrite.rs::empty_insert_overwrite_partition_refuses_full_wipe`
  and `crates/repark-spark/src/tests/insert_overwrite.rs::insert_overwrite_partition_nonempty_refuses_whole_table_replace`
- **Rationale** — DECLARED until a partition-scoped write path exists. Both degradations are
  destructive and neither is detectable from the result: an empty source would wipe sibling
  partitions, and a non-empty source would silently become a whole-table replace. The documented
  substitute is `DELETE` with a partition predicate followed by `INSERT INTO`.

#### DML-2 — `TRUNCATE TABLE`

- **repark** — refuses with a targeted message naming the substitutes
  (`INSERT OVERWRITE … SELECT … WHERE false`, or `DELETE FROM <table>` with no predicate).
- **Apache Spark** — truncates the table. *(oracle: documented.)*
- **Pin** — `crates/repark-spark/src/tests/router.rs::truncate_table_refuses_loud_naming_gap`
- **Rationale** — DECLARED until a dedicated truncate action lands. The refusal exists so the
  statement does not fall through to the planner's opaque "unsupported" diagnostic, which tells a
  migrating user nothing about what to write instead.

#### DML-3 — `MERGE INTO` forms outside the supported surface

- **repark** — a `MERGE INTO` shape the door cannot parse refuses with a targeted error pointing
  at this section, rather than surfacing a raw parser error.
- **Apache Spark** — parses the full `MERGE INTO` grammar. *(oracle: documented.)*
- **Pin** — `crates/repark-spark/src/tests/merge.rs::merge_unparsable_form_gets_targeted_error`
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

#### DML-4 — insert-only `MERGE` snapshot operation stamp

- **repark** — an insert-only `MERGE` (no `WHEN MATCHED` arm) commits with operation `append`
  under copy-on-write and `overwrite` under merge-on-read, inherited from the Java-faithful
  commit classification (`commit_overwrite` vs the row-delta path).
- **Apache Spark** — the same operation classes for the same modes — but a table whose
  `write.merge.mode` flips changes which snapshots an `IncrementalAppendScan` (CDC-style
  consumer) sees, so the stamp is a visibility contract worth declaring. *(oracle: documented —
  audit M20.)*
- **Pin** — `crates/repark-iceberg/src/write/merge/occ_conflict_tests.rs`
  `merge_insert_only_cow_stamps_append_m20` / `merge_insert_only_mor_stamps_overwrite_m20`
  (plus the mixed/delete-only stamps in the same battery).
- **Rationale** — DECLARED. The stamps match Spark class-for-class; the row exists to warn CDC
  consumers that the mode knob moves insert-only commits between `append` and `overwrite`
  visibility. Landed with the 2026-08-15 OCC battery (#121).

#### DML-5 — serializable `MERGE` conflict-detection breadth

- **repark** — a serializable `MERGE` validates against **any** concurrent append
  (`AlwaysTrue` conflict filter): a concurrent insert into an unrelated partition aborts the
  MERGE with a conflict error.
- **Apache Spark** — scopes serializable validation to a filter derived from the scan, so the
  same unrelated-partition append commits. *(oracle: documented — audit M15.)*
- **Pin** — `crates/repark-iceberg/src/write/merge/occ_conflict_tests.rs`
  `commit_serializable_merge_rejects_concurrent_append_in_a_different_partition_m15` (and the
  snapshot-isolation contrast cases beside it; `write.merge.isolation-level = snapshot` (#117)
  is the user-facing relief valve).
- **Rationale** — DECLARED, fail-closed by design. Narrowing to the pushed-predicate residual
  would be UNSOUND for the shapes whose residual under-covers the scan (audit M15); the honest
  contract is over-rejection plus the documented `snapshot` opt-down.

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

> **G11 closed: not parity — correctness (2026-08-12, Y-10 / #67).** Spark is not the ANSI
> door's oracle (owner ruling 2026-08-12, Option A). The ANSI door serves standard SQL;
> matching Spark is the Spark door's job. Six INTENDED door-vs-door splits are pinned in
> `crates/repark-sql/tests/cross_door.rs` (`cross_door_integer_division_*`,
> `cross_door_*_div_by_zero_*`, `cross_door_order_by_*`). Six ANSI-door standard-SQL value
> pins live in `crates/repark-sql/tests/ansi_door_values.rs`. Identifier case folding remains
> this section's [ID-1](#id-1--a-quoted-identifier-resolves-case-sensitively) (cited, not
> duplicated).
>
> **F-Y10-1 / F-Y10-2 — routed, not invented as DEC rows (2026-08-13, Z-5).** F-Y10-1
> (`CAST(2147483647 AS INT) + CAST(1 AS INT)` wraps on **both** doors) is the integer analog
> of [DEC-6](#dec-6--max-decimal380--1-under-ansi-returns-a-corrupted-value). The 2026-08-13
> DEC addendum (Q11 = A) puts ANSI overflow on campaign U5 / G13; wrap is not pinned as
> intended and is not a new DEC row. F-Y10-2 (ANSI float `/ 0` is IEEE `+Inf` rather than a
> standard-SQL raise) is residual; the door-vs-door Inf-vs-NULL split is already an INTENDED
> pin (`cross_door_float_div_by_zero_is_infinity_on_ansi_null_on_spark`). It is not
> [DEC-7](#dec-7--decimal--0-under-ansi-returns-null) (decimal).

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

- **repark** — after U2, `VALUES (2.5)` is `DECIMAL(2,1)` and `VALUES (1)` is still Int64, so
  `union(VALUES (1), VALUES (2.5))` yields `decimal128(21,1)` / nullable with `Decimal('1.0')`,
  `Decimal('2.5')`.
- **Apache Spark** — parses the literal as `DECIMAL(2,1)` and widens the integer into it, yielding
  `decimal128(11,1)` / non-null with `Decimal('1.0')`, `Decimal('2.5')`. *(oracle: recorded.)*
- **Pin** — `python/repark/tests/test_union_distinct.py::test_union_inline_decimal_literal_diverges_from_spark`
- **Rationale** — DECLARED, with narrow impact: stored Iceberg `DECIMAL` columns coerce
  faithfully; only inline decimal *literals* differ. The pin asserts repark's actual output **and**
  asserts that the recorded Spark golden still does not match, so a future convergence reds it.
  **Dated 2026-08-13 (W-2 U2 / #84):** U2 landed. The declaration is revisited and **kept**:
  after `parse_float_as_decimal=true`, `VALUES (2.5)` is `DECIMAL(2,1)` and `VALUES (1)` is
  still Int64, so the union is `decimal128(21,1)` **nullable** vs Spark `decimal128(11,1)`
  **non-null**.
  **Dated 2026-08-14 (V-2 U3 / #91):** U3 landed. The declaration is revisited and **kept**:
  U3 `fromLiteral` applies to `+ − *` only. UNION set-op widening uses Spark
  `forType(INT) = DECIMAL(10,0)`, not digits-of-the-value. Applying `fromLiteral` here
  would yield `DECIMAL(1,0)` union `DECIMAL(2,1)` → `(3,1)`, which is neither today's `(21,1)`
  nor Spark's `(11,1)`. Observed type after U3 is still `decimal128(21,1)` **nullable**
  vs Spark `(11,1)` **non-null**. Residual is INT-literal-as-INT, not min-precision
  arithmetic.

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
  warning says so in as many words.
- **The `ConfigOptions` escape was evaluated in H-1a split B and declined, on measurement.** The
  earlier note here read "becomes fixable if the extraction unit routes the zone through
  DataFusion `ConfigOptions` (a live `SET` would then retire this row)". The extraction unit did
  route the zone through `ConfigOptions` — but through a **carrier** whose `set` refuses and whose
  `entries` are empty, not through `datafusion.execution.time_zone`, for three measured reasons:
  in DataFusion 54.1 that option drives `now()` / `current_date` / `current_time` and the SQL
  planner's `TIMESTAMP WITH TIME ZONE` mapping and **not** `date_part`, so it does not fix this
  class on its own; it is reachable as `.config("datafusion.execution.time_zone", …)` and as
  `SET datafusion.execution.time_zone`, which is a second live spelling of a knob whose acceptance
  gate is "exactly one"; and it would retype `current_timestamp` as
  `timestamp[ns, tz=<session zone>]`, moving it *away* from Spark's `timestamp[us, tz=UTC]`
  (TZ-4). This row therefore stands, and the working shown is in
  [history/hardening-h1/h1a-ledger.md](history/hardening-h1/h1a-ledger.md) decision D-B2 rather than restated here.

### F-V4-2 — timestamptz Arrow annotation after Iceberg read

- **repark** — Iceberg `timestamptz` columns export `timestamp[us, tz=+00:00]`.
- **Apache Spark** — `timestamp[us, tz=UTC]`. Instants match. *(oracle: recorded.)*
- **Pin** —
  `python/repark/tests/test_partition_value_audit.py::test_partition_value_row[carry_years_ts_ctas]`
  (type rider; every timestamp-carrying data half).
- **Rationale** — DECLARED, fork-wave-routed. Fork read mapping
  (`Timestamp(µs, "+00:00")`). Not a partition-VALUE divergence.

---

### RAND-1 — `randstr` refuses a length Spark accepts

- **repark** — `randstr(n, seed)` refuses `n` above **1,000,000** with a catchable
  `randstr length must be between 0 and 1000000, got …`, and refuses a request whose
  `length x batch rows` would exceed `i32::MAX` bytes with
  `randstr would build … characters x … rows, past the … byte limit of a string column`.
- **Apache Spark** — has no cap. `SELECT length(randstr(5000000, 1))` returns **5000000**.
  *(oracle: live — PySpark 4.1.2.)*
- **Pin** — `python/repark/tests/test_lrs3_registered_divergences.py::test_randstr_refuses_a_length_spark_accepts`
  and `::test_randstr_refuses_a_batch_that_would_overflow_string_offsets`
- **Rationale** — DECLARED, as a **safety limit rather than a parity claim**, which is why it needs
  a row: nothing else says it is deliberate. The failure mode without the per-row cap is not an
  error at all — `String::with_capacity(n)` per row aborts the process on a large constant, SIGABRT
  with no traceback and the session lost, where every other refusal in that module is catchable
  (F-CFS-1). The batch bound was added for the same reason at a different scale: a *legal* length
  times a large batch overflows the i32 offsets of an Arrow `StringArray` and panics inside
  arrow-rs. That panic is caught at the PyO3 boundary rather than aborting, but a caught panic is
  not a contract (round 2 F-R3-9). Raising either bound is a decision, not a bug fix.

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
  is honest about what the SHOW primitive carries. **Y-3 / #69 (2026-08-12):** `getDatabase` now
  returns the stored `locationUri` and `description` (DESCRIBE NAMESPACE; pin
  `python/repark/tests/test_catalog_surface.py::test_get_database_returns_location_and_comment`).
  This row's `listDatabases` half is unchanged (`locationUri` / `description` remain `None`).
  Revisit if list-side metadata readback gains a real location source.

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

### FA-4 — `inferNestedDictAsStruct` defaults to `true`

- **repark** — `spark.sql.pyspark.inferNestedDictAsStruct.enabled` defaults to **`"true"`**:
  dict-valued *cells* in `createDataFrame` python-object ingestion infer as `StructType`
  (field union + null-fill) with no conf set. Row-level dicts (key-union) are unaffected;
  explicit `schema=` wins either way.
- **Apache Spark** — the conf defaults to `false` (SPARK-35929): dict cells infer as
  `MapType` unless the user opts in. *(oracle: documented — the conf's documented default.)*
- **Pin** — `python/repark/tests/test_n1_nested_dict_struct.py::test_conf_default_true_in_sqlconf_defaults`
  and `::test_default_unset_dict_cell_infers_struct`; the conf-false leg stays pinned
  byte-identical to PySpark in the same file.
- **Rationale** — DECLARED, owner decision (2026-08-16). The dominant facade ingestion shape
  is nested dict rows headed for `dynamicFlatten`/struct addressing, where map inference is
  a silent no-op surprise; struct is the useful default and the honest one to declare. The
  PySpark-faithful behavior is one conf away (`"false"` restores byte-identity), the flip is
  visible (`conf.get` discloses it), and both directions stay under test so a drift in
  either inference path reds.

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
  [history/hardening-h1/g5-sweep-ledger.md](history/hardening-h1/g5-sweep-ledger.md)); closing an entry means moving its
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
is `python/repark/tests/test_parity_live.py::_LIVE_MIRROR_RE`, and the live-mirrored rows in this
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

### BL-1 — cast-failure class (G6) — oracle-backed pin home

- **repark** — under **ANSI ON** (Spark 4 default; the recorded oracle), a failing
  `CAST('abc' AS INT)` **raises** an execution-class error (`Cast error`) through both
  `spark.sql()` and DataFrame `Column.cast`. `try_cast` of a failing cast yields NULL at the
  target Arrow type. These two recipes **agree with ANSI Spark** (raise vs raise; NULL vs NULL).
- **Apache Spark** — under ANSI ON raises `CAST_INVALID_INPUT` / `NumberFormatException` for the
  malformed-string CAST and yields NULL for `try_cast`. The pre-G6 "non-ANSI Spark yields NULL"
  claim is retired: this repository has never recorded a non-ANSI NULL for this recipe, and the
  live session is ANSI ON. *(oracle: recorded — PySpark 4.1.2, ANSI on.)*
- **Pin** — the oracle-backed home is now
  `python/repark/tests/test_cast_failure_parity.py` (G6 corpus; 15 rows). The ONE remaining
  live divergence under ANSI ON is
  [G6-4](#g6-4--timestampint-nullability-only-after-tz-5), and it is nullability only. Equality
  pins: `…[malformed_string_to_int_both_raise]`, `…[df_cast_malformed_string_to_int_both_raise]`,
  `…[try_cast_malformed_string_to_int_null]`, `…[try_cast_overflow_tinyint_null]`.
- **Rationale** — rewritten 2026-08-12 (L-1) when the G6 corpus landed, and again 2026-08-15 when
  the DATE↔INT gate landed. BL-1 is no longer a documented-value placeholder; it is the pointer at
  the recorded corpus. The sentence this bullet used to carry — "the residual silently-wrong-result
  class is G6-3 (DATE→INT)" — became false at that commit: G6-3 and G6-5 are both CLOSED, the
  corpus grew 10 → 15 rows recording all five converged doors, and what is left under ANSI ON is
  G6-4's CAST nullability (value+type already agree after #64).

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

### BL-3 — `MERGE` cardinality check fires on a lone unconditional `DELETE`

> **CLOSED 2026-08-15 (#140).** The cardinality check now skips exactly Spark's
> `isCardinalityCheckNeeded` shape — a single unconditional `WHEN MATCHED THEN DELETE` —
> via a `skip_cardinality` flag threaded from the spec-owning execute path (never SQL
> re-parsing in the fold). Every other matched shape keeps the check (the file+pos grouping
> pin and the conditional-clause differential row stayed green untouched). The recorded
> differential row `dup_source_keys_unconditional_delete` flipped split → content in the
> same change: both engines now produce the recorded survivor table. Retired per §6.

### BL-4 — `UPDATE`-path store-assignment error shape in `MERGE`

> **CLOSED 2026-08-15 (#135).** `WHEN MATCHED UPDATE SET` assignments now route through the
> same `ansi_store_assignable` matrix as the INSERT path (`update_stream_checked` /
> `validate_update_store_assignment` in `merge/insert.rs`), refusing with the shared
> `not ANSI-store-assignable` needle on BOTH doors. The old CASE/type-coercion error shape is
> gone; the UPDATE trio in `test_merge_store_assign.py` plus native-door pins are the record.
> Retired per §6: the fix landed and the pins flipped in the same change.

### BL-5 — rejected `MERGE` commit leaves written files behind

> **CLOSED 2026-08-15 (#134, owner-ratified design A).** A commit-path `Err` now best-effort
> deletes every file the attempt wrote (paths threaded from writer results, never re-derived),
> then re-raises the original error; the battery-I characterization pins flipped to
> "files gone, error surfaces" in the same change. One deliberate carve-out:
> `CommitStateUnknown` errors SKIP cleanup (the catalog may have persisted — Java's
> `CommitStateUnknownException` rethrow-before-cleanup rule), leaving those files to
> orphan-file maintenance; an injection test for that path is a named residual.

### BL-6 — `bin` / `rint` over-accept BOOLEAN where Spark analysis-refuses

- **repark** — the facade lowers `F.bin(col)` as `bin(CAST(col AS BIGINT))` and `F.rint(col)` as
  `rint(CAST(col AS DOUBLE))` (the G5 unix_date mold), so a BOOLEAN input is silently cast:
  `F.bin(F.lit(True))` returns `"1"`, `F.rint(F.lit(True))` returns `1.0`.
- **Apache Spark** — analysis-refuses BOOLEAN for both with
  `DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE` (`bin` requires BIGINT; `rint` requires DOUBLE).
  *(oracle: live — PySpark 4.1.2, 2026-08-18.)*
- **Pin** — `python/repark/tests/test_functions_gt1.py::test_bin_bool_over_accepts_where_spark_refuses`
  (covers both names).
- **Rationale** — BACKLOG, intent to FIX (fail-loud on BOOLEAN before the cast). Deliberately kept
  out of the GT1-FIX PR (#180, round-2 ruling A4): wrong-answer classes outranked over-accepts in
  the 0.4.0 gate round, and an over-accept never corrupts a value a correct script produces.

### BL-7 — `bit_length` / `octet_length` stringify DOUBLE with Arrow float formatting

- **repark** — a DOUBLE input to the owned length kernel is stringified by the Arrow
  `float64 → utf8` cast: `CAST('Infinity' AS DOUBLE)` becomes `'inf'` (octet_length 3) and
  `1.0E21` becomes `'1e21'` (octet_length 4). Mainstream values agree with Spark (`1.0` → 3,
  `12.5` → 4).
- **Apache Spark** — stringifies via Java `Double.toString`: `'Infinity'` (octet_length 8),
  `'1.0E21'` (octet_length 6). *(oracle: live — PySpark 4.1.2, 2026-08-19.)*
- **Pin** — `python/repark/tests/test_functions_gt1.py::test_sql_door_double_infinity_stringify_is_named_divergence`
  (codifies today's `3`; the fix reds it on purpose).
- **Rationale** — BACKLOG, intent to FIX (Java-shaped double formatting in the decimal-style
  path, GT1-FIX round-3 ruling R3-4). The divergence is confined to the E-notation thresholds and
  the Infinity/NaN spellings; the common numeric range already matches.


> **TZ-1 — timestamp extraction ignores the session zone — was CLOSED IN PART and CONVERTED on
> 2026-08-10**, when H-1a split B landed the extraction fix (campaign decision D7). It does not
> simply retire, and the distinction is the point: what closed is the **instant-typed** half —
> `year` / `month` / `dayofmonth` / `hour` / `date_trunc` / `date_format`, and this repo's
> `trunc` / `add_months`, over a TIMESTAMP that already carries the right instant now resolve in
> `spark.sql.session.timeZone`, as Spark does, at all four entry points. The closure paid the
> price §6 charges: the row's disclosure pins went RED **on purpose** and were flipped to equality
> rows in the same change, which is what makes them the fix's revert-red evidence; the `date_trunc`
> rows whose value converged and whose type did not moved to TZ-4 below.
>
> **What did NOT close stayed a row rather than becoming a silence.** Two narrower successors carry
> the remainder, each measured against live Spark 4.1.2 and pinned:
> [TZ-7](#tz-7--a-zoneless-timestamp-input-is-read-as-utc-not-as-a-session-zone-wall-clock) (a
> zoneless timestamp INPUT is read as UTC, so its instant is wrong before any extractor sees it)
> and [TZ-8](#tz-8--timestampdate-outside-this-repos-coercion-path-reads-the-stored-zone)
> (`to_date` / `CAST(ts AS DATE)` / `datediff` take the date in the stored zone). A reader who
> arrives here from a wrong wall clock is routed to one of the two, never told the class is shut.
> The remaining state line is in [../STATUS.md](../STATUS.md); the full account, including the
> adversarial panel that forced this narrowing, is in
> [history/hardening-h1/h1a-ledger.md](history/hardening-h1/h1a-ledger.md) "§ Split B".

### TZ-4 — TIMESTAMP Arrow export is tz-naive

> **TZ-4 progress, not retired (2026-08-13, Z-2 / #79 + TZ-4 PR-2 / #85 + B-TZ-4 / #90).** Instant-typed
> producers export `timestamp[us, tz=UTC]`. Spark-door DDL `TIMESTAMP` maps to Iceberg
> `timestamptz` (live Spark 4.1.2 CREATE probe 2026-08-13). PR-2 localizes zoneless LTZ
> inputs in `spark.sql.session.timeZone` and distinguishes `TIMESTAMP` (µs+UTC / Iceberg
> `timestamptz`) from `TIMESTAMP_NTZ` (naive µs / Iceberg `timestamp`). Extraction is
> type-driven. **TZ-6 and TZ-7 are FIXED** (their registry sections, dated by #85 — not
> duplicated here). **B-TZ-4 string-cast landed (#90).** This row is **not** retired.
> Remaining: ANSI column-def `timestamp_ns` (A11).

- **repark** — instant-typed producers (`current_timestamp` / `now`, `to_timestamp` of a
  zone-suffixed string, `date_trunc` return, `CAST(<integer> AS TIMESTAMP)` type wrap) export
  `timestamp[us, tz=UTC]`. Spark-door `CREATE` / CTAS of those producers stores Iceberg
  `timestamptz`. `[tz_aware_to_naive_round_trip]` is now a content equality at
  `timestamp[us, tz=UTC]` (W-5's "still `timestamp[ns]`" is stale on this base). Native ANSI
  column-def `TIMESTAMP` still derives `timestamp_ns` and Iceberg v2 refuses it (A11).
  Zoneless-input and NTZ-indistinguishability classes are closed on TZ-7 / TZ-6 (FIXED
  notes), not restated here.
- **Apache Spark** — `toArrow()` yields `timestamp[us, tz=UTC]`, and does so *whatever* the
  session zone is — the session zone moves a `TIMESTAMP`'s calendar fields, never its export
  annotation. Spark-door Iceberg `TIMESTAMP` is `timestamptz`. *(oracle: recorded — including
  the live `current_timestamp` type, `timestamp[us, tz=UTC]`, non-null; CREATE probe
  2026-08-13.)*
- **Pin** — progress (now equalities):
  `python/repark/tests/test_session_timezone_parity.py::test_current_timestamp_type_and_zone_disclosure`,
  `…[to_timestamp_of_zone_suffixed_string]`,
  `…[tz_aware_to_naive_round_trip]` (now equality, PR-2),
  `…[date_trunc_day_across_a_zone_boundary]`,
  `…[dataframe_api_extract_under_new_york_session]`,
  `python/repark/tests/test_timestamp_cast_parity.py::…[bigint_to_timestamp_reads_seconds]`,
  `crates/repark-spark/tests/session_timezone.rs::date_trunc_truncates_on_the_session_zone_calendar`
  (now asserts `timestamp[us, tz=UTC]`, not naive),
  `crates/repark-spark/src/tests/create_table.rs::column_def_temporary_refuse_testing_create_ref_and_types`,
  `…::ctas_of_instant_producers_stores_timestamptz`.
  Residue: `crates/repark-sql/tests/session_wiring.rs::ansi_column_def_timestamp_still_rejects_ns_on_v2`
  (A11).
- **Rationale** — BACKLOG, **partial close**. It split from TZ-1 rather than closing with it
  (2026-08-10): TZ-1 was the extractor *coercion path*; TZ-4 is TIMESTAMP *representation*.
  PR-1 closed the instant-producer + Spark-door write-mapping half. PR-2 closed zoneless
  localization, NTZ distinction, and the Python `TimestampType` / `TimestampNTZType`
  mapping. PR-3 closed B-TZ-4 string-cast render (`#90`). Remaining: ANSI column-def
  `timestamp_ns`. Not retired.

> **TZ-5 — `CAST(TIMESTAMP AS <numeric>)` returns epoch seconds — FIXED (2026-08-12, #64).**
> repark returned epoch **nanoseconds** where Spark returns epoch **seconds** — a 10⁹ factor,
> correctly signed, on `CAST(ts AS BIGINT)`. The same wrong scaling reached `DOUBLE`, `FLOAT`
> and `DECIMAL(p,s)`; `INT` and `SMALLINT` were refused outright.
>
> repark now matches Spark 4.1.2 on the whole numeric-target family, **including the floor
> edge**: Spark uses `Math.floorDiv`, so `1969-12-31T23:59:59.5Z` is `-1` (not `0`). Float and
> decimal targets keep the fraction. The class is zone-independent on both engines.
>
> The **reverse** direction (`CAST(<integer> AS TIMESTAMP)`) was already correct (seconds).
> **2026-08-13 (Z-2 / #79):** its Arrow export type is now `timestamp[us, tz=UTC]` (pin
> `[bigint_to_timestamp_reads_seconds]`). Remaining TZ-4 residues stay on
> [TZ-4](#tz-4--timestamp-arrow-export-is-tz-naive), not this row.
>
> **Fix:** `repark_functions::timestamp_cast` driven by the `Expr::Cast` arm of
> `repark_functions::analyzer::SparkExprSemantics`.
> **Pins:** `crates/repark-spark/tests/timestamp_cast_seconds.rs`,
> `crates/repark-sql/tests/timestamp_cast_ansi_door.rs`,
> `python/repark/tests/test_timestamp_cast_parity.py`, and the flipped row
> `pre_1970_timestamp_cast_to_bigint` in
> `python/repark/tests/test_session_timezone_parity.py`.
> **Residuals** (all LOUD refusals, none silent): `TINYINT` overflow-message parity, the `LONG`
> keyword spelling, `unix_timestamp`, the `D` double-literal suffix, and `F.expr` over a
> column reference — `task/tz5-cast-seconds-ledger.md` §5.
> The X-1 TIMESTAMP→INT split flipped to a **nullability-only** disclosure; see
> [G6-4](#g6-4--timestampint-nullability-only-after-tz-5).

### TZ-6 — every TIMESTAMP is an instant; there is no `TIMESTAMP_NTZ`

> **TZ-6 — `TIMESTAMP` vs `TIMESTAMP_NTZ` are distinct — FIXED (2026-08-13, TZ-4 PR-2).**
> `TimestampType` / default `TIMESTAMP` is Arrow `timestamp[us, tz=UTC]` (Iceberg `timestamptz`);
> `TimestampNTZType` / `TIMESTAMP_NTZ` is naive `timestamp[us]` (Iceberg `timestamp`). Extraction
> is type-driven: a tz-annotated column is an instant resolved in `spark.sql.session.timeZone`; a
> naive column is NTZ and is not shifted. The same wall clock declared as both types under
> `America/New_York` exports `ltz = timestamp[us, tz=UTC] @ 16:00Z` and
> `ntz = timestamp[us] @ 12:00`, and `hour` reads `12` from both — matching Spark 4.1.2
> (recorded 2026-08-10).
>
> **Pins:**
> `python/repark/tests/test_session_timezone_parity.py::…[timestamp_ntz_is_indistinguishable_from_timestamp]`
> (now an equality row) and
> `crates/repark-spark/tests/session_timezone.rs::a_naive_ntz_timestamp_is_not_shifted_by_the_session_zone`.
> **Residual:** `spark.sql.timestampType` (opt-in default-NTZ) is not implemented (Q10).

### TZ-7 — a zoneless TIMESTAMP input is read as UTC, not as a session-zone wall clock

> **TZ-7 — zoneless LTZ input localizes in the session zone — FIXED (2026-08-13, TZ-4 PR-2).**
> `TIMESTAMP '2024-06-15 12:00:00'`, zoneless `to_timestamp`, `CAST(str AS TIMESTAMP)`, and a
> naive-`datetime` `createDataFrame` column (default `TIMESTAMP`) are session-zone wall clocks
> stored as µs+UTC. `hour` reads `12` in every zone; `year`/`dayofmonth` of
> `TIMESTAMP '2024-01-01 00:30:00'` under `America/New_York` stay `2024` / `1`. A zone-suffixed
> string is **not** localized (H-1a double-shift control).
>
> **Pins:** `python/repark/tests/test_session_timezone_parity.py`
> `[zoneless_timestamp_literal_under_new_york_session]`,
> `[zoneless_timestamp_input_spellings_under_tokyo_session]`,
> `[naive_datetime_column_under_new_york_session]` (now equality rows); Rust:
> `crates/repark-spark/tests/session_timezone.rs::a_zoneless_timestamp_input_localizes_in_the_session_zone`.
> **Residual:** extractor columns over a `TIMESTAMP '…'` literal stay Arrow-nullable (Spark
> types them non-null) — not the TZ-7 class. `F.lit(tz-aware)` under a non-UTC session still
> emits a zoneless `TIMESTAMP '…'` of the UTC wall (`functions.py` is out of this PR's grant).
> B-TZ-4 string-cast render landed in `#90`. `TimestampType.toInternal`/`fromInternal` use the session
> zone (Q12), not the host.

> **B-TZ-4 — `CAST(TIMESTAMP AS STRING)` is Spark's session-zone space-separated Arrow `string` —
> FIXED (2026-08-13, V-3 / #90).** repark now emits `Utf8` (`string`), space-separated wall in
> `spark.sql.session.timeZone` for LTZ; stored wall for NTZ; trailing-zero fractions stripped
> (`.123400` → `.1234`); year −1 is `-0001`, year 10000 is `+10000`. Same strings as Spark
> 4.1.2 (recorded 2026-08-13, PySpark 4.1.2, zulu-17, `local[2]`, ANSI on).
> **Pins:** `python/repark/tests/test_timestamp_cast_parity.py::test_timestamp_cast_row_matches_spark_or_still_diverges[timestamp_to_string_ltz_under_new_york]`
> (and the 11 sibling STRING rows in `ROWS`); Rust
> `crates/repark-functions/src/timestamp_cast.rs::spark_timestamp_string_trims_trailing_fraction_zeros`
> + `ltz_renders_in_the_session_zone_and_ntz_does_not`; analyzer
> `timestamp_cast_to_string_is_spark_utf8`. TZ-8 date-cast stays disclosed. A fixed defect
> gets this dated note, never a live divergence row.

### TZ-8 — TIMESTAMP→DATE outside this repo's coercion path reads the stored zone

- **repark** — `to_date(to_timestamp('2024-06-15T03:00:00Z'))` and
  `CAST(to_timestamp('2024-06-15T03:00:00Z') AS DATE)` both answer `2024-06-15`;
  `datediff(to_timestamp('2024-06-15T03:00:00Z'), DATE '2024-06-01')` answers `14`. `last_day` and
  `date_add` over a TIMESTAMP do not plan at all (`coercion from Timestamp(ns) … failed`).
  As an identity partition key under `America/New_York`, `CAST(ts AS DATE)` / `to_date(ts)`
  write the **UTC** calendar date (`2024-01-01` for `2024-01-01T04:30Z`).
- **Apache Spark** — takes the date in `spark.sql.session.timeZone`: `2024-06-14` (the instant is
  23:00 EDT on the 14th), `13`, and `last_day` / `date_add` accept a TIMESTAMP and answer
  `2024-05-31` / `2024-06-01`. The same NY session writes partition date `2023-12-31` for
  `2024-01-01T04:30Z`. *(oracle: recorded — live PySpark 4.1.2, 2026-08-10; V-3 recapture
  2026-08-13; V-4 partition audit 2026-08-13.)*
- **Pin** — `crates/repark-spark/tests/session_timezone.rs::timestamp_to_date_paths_outside_this_crate_still_read_the_stored_zone`
  (NY `2024-06-14` Spark / `2024-06-15` repark) and
  `python/repark/tests/test_partition_value_audit.py::test_partition_value_row[tz8_cast_ts_as_date_identity_new_york_ctas]`
  + `…[tz8_to_date_ts_identity_new_york_ctas]` (partition dates). One row, two citations.
- **Rationale** — BACKLOG, intent to FIX. **Not FIXED.** **Not a regression** — these behaved
  the same before H-1a split B — but a completeness gap that the class claim would otherwise
  paper over, and `CAST(ts AS DATE)` is the most common partition-key derivation in a
  migrated job. The boundary is ownership, not difficulty: this repo's own date-valued shims
  (`trunc`, `add_months`) reach the date through `repark-functions`' coercion path and were
  **fixed** in the same rework, while `to_date` / `datediff` come from `datafusion-spark` and
  `CAST(ts AS DATE)` is arrow's cast kernel, which reads the array's own annotation. UTC
  annotation (TZ-4 PR-2 / `#85`) did not close date-cast. Registry queue **B-TZ-3** is the
  `DATE`-argument sibling of the same `date_add` coercion hole.

> **Dated 2026-08-14 (V-3 handoff + V-4 / #88).** V-3 captured NY
> `CAST(to_timestamp('2024-06-15T03:00:00Z') AS DATE)` → Spark DATE `2024-06-14`. V-4's
> two partition-date diverge rows CROSS-CITE this row; they are not a second TZ-8.

> **The DEC family (DEC-1 … DEC-9)** landed on 2026-08-11 from the G-7 decimal128 differential
> corpus (hardening gaps **G2** and **G13**; unit ledger
> [history/hardening-h1/g7-decimal-ledger.md](history/hardening-h1/g7-decimal-ledger.md)). Oracle basis for every Spark half:
> **recorded** — live PySpark 4.1.2 (ANSI on), re-derivable in-repo via the committed driver
> `python/repark/tests/_record_decimal128_goldens.py`. Every pin below is a parametrized case of
> `python/repark/tests/test_decimal128_parity.py::test_decimal128_row_matches_spark_or_still_diverges`,
> written `[<case>]` for short; each asserts repark's pinned half AND classifies a drift as
> CONVERGED (flip, don't delete) vs regression.

> **DEC-1 — a bare SQL decimal literal infers `double` — FIXED (2026-08-13, W-2 U2 / #84).**
> Spark-door default `datafusion.sql_parser.parse_float_as_decimal=true` via
> `SparkExtension::configure` (`apply_spark_float_as_decimal`). Bare `1.23` / `0.1` /
> `123.456` are `decimal128(3,2)` / `(1,1)` / `(6,3)` non-null. ANSI door unchanged.
> The parametrized names
> `[literal_1_23_infers_decimal_in_spark_double_in_repark]` (and `0.1` / `123.456`
> siblings) are kept so citations still resolve; the rows are now content equalities
> (`repark is None`). Pins: those corpus cases; Rust
> `crates/repark-spark/src/tests/decimal.rs::pin_literal_1_23_infers_decimal128_3_2_i128` +
> `crates/repark-spark/src/extension/tests.rs::configure_makes_bare_1_23_decimal128_3_2`.
> A fixed defect gets this dated note, never a live divergence row.
> [TY-3](#ty-3--an-inline-sql-decimal-literal) (inline-`VALUES` union width) stays
> DECLARED — see its dated 2026-08-13 note.

### DEC-2 — `DECIMAL / DECIMAL` result precision and scale

- **repark** — `CAST(1.23 AS DECIMAL(10,2)) / CAST(4.56 AS DECIMAL(10,2))` yields
  `decimal128(16,6)` nullable with rounded value `0.269736`; repeating money division
  `(10.00)/(3.00)` → `(16,6)` `3.333333`; integer-scale `1/3` at `(10,0)` → `(14,4)` `0.3333`;
  exact half `5.00/2.00` keeps value `2.5` but lands `(16,6)`.
- **Apache Spark** — `(10,2)/(10,2)` yields `decimal128(23,13)` nullable `0.2697368421053`;
  repeating money stays `(23,13)`; integer-scale lands `(21,11)` `0.33333333333`; exact half is
  `(23,13)` `2.5000000000000`. *(oracle: recorded.)*
- **Pin** — `[div_same_precision_scale]`, `[div_repeating_money]`, `[div_integer_scales]`,
  `[div_exact_half_type_only]`
- **Rationale** — BACKLOG, intent to FIX (gap G2 — Spark-compatible division precision/scale
  rules). Value AND type diverge on three recipes; the fourth is type-only. A unit-price split is
  silently short under repark's six fractional digits.
  **Dated 2026-08-14 (V-2 U4a / #91):** U4a shipped add/sub/mul clamp only. `/` is declared
  **U4b**: CAST-after wrongs the *value* (exact-half agrees on 2.5 and still diverges on
  `(23,13)` vs `(16,6)`); needs a UDF / scaled division. `%` UNPROBED. Do not fold `/` into
  DEC-3's FIXED note.

> **DEC-3 — the 38-digit result-type clamp on multiply and add — FIXED (2026-08-13, V-2 U4a / #91).**
> Spark `adjustPrecisionScale` (`allowPrecisionLoss=true`) via `SparkDecimalPrecision`
> CAST-after: `(38,10)*(38,10)` → `decimal128(38,6)` `1.000000`; `(38,18)+(38,18)` →
> `(38,17)` `2.00000000000000000`; `(38,10)+(38,10)` → `(38,9)` `2.000000000`. The
> parametrized names `[mul_38_10_clamps_scale_in_spark]`, `[add_38_18_clamps_scale_in_spark]`,
> `[add_38_10_clamps_scale_in_spark]` are kept so the name-gated family pin still
> resolves; the rows are now content equalities (`repark is None`). Pins: those corpus
> cases; Rust `pin_mul_38_10_clamps_to_38_6_i128` + `pin_add_38_18_clamps_to_38_17_i128`;
> name-gated budget `test_decimal128_row_set_covers_gap_budgets` (≥ 3 `*clamps_scale_in_spark`).
> `/` is **EXCEPTED** (U4b — see DEC-2). Registry DEC-8 (plan-refuse at
> `BinaryExpr::get_type`) is a **different altitude** and stays BACKLOG. A fixed defect
> gets this dated note, never a live divergence row.

> **DEC-4 — `avg(DECIMAL)` promotes to `double` — FIXED (2026-08-13, Z-3 U1 / #76;
> campaign DEC-5).** Facade `avg(DECIMAL(p,s))` now returns Spark's
> `DECIMAL(min(38,p+4), min(38,s+4))` (group and sliding). The overwrite is
> `SparkAvgWithRetract` in `crates/repark-functions/src/aggregate.rs` (not `analyzer.rs`).
> Float sliding avg is unchanged (X-3). Corpus `(10,2) → (14,6)` nullable `1.650000`.
> The parametrized name `[avg_money_stays_decimal_in_spark_double_in_repark]` is kept so
> citations still resolve; the row is now a content equality (`repark is None`). Pins:
> that corpus case; Rust
> `crates/repark-functions/src/aggregate.rs::group_avg_decimal128_stays_decimal_14_6_i128`
> + `sliding_avg_decimal128_retracts`; G-7b
> `pin_avg_money_stays_decimal128_14_6_i128`. A fixed defect gets this dated note, never a
> live divergence row. Registry DEC-5 (`INT * DECIMAL`) is a different class and stays
> BACKLOG.

### DEC-5 — `INT * DECIMAL` result width and nullability

- **repark** — `5 * CAST(1.50 AS DECIMAL(10,2))` yields `decimal128(12,2)` **non-null** with
  value `7.50` (U3 `fromLiteral`: the integer literal `5` is `DECIMAL(1,0)`). Typed
  `CAST(5 AS INT) * …` stays `(21,2)`.
- **Apache Spark** — yields `decimal128(12,2)` **nullable** with the same value. *(oracle:
  recorded.)*
- **Pin** — `[int_times_decimal_promotes_wider_in_repark]` (still a disclosure — nullability)
  + Rust `pin_int_times_decimal_is_12_2_i128`
- **Rationale** — BACKLOG, **width closed**. Campaign DEC-8 / U3 (`#91`) closed the
  **width**. Nullability is DEC-9. Do not mark this row FIXED until both faces close.

> **Name collision.** Campaign DEC-8 is integer-literal min-precision (this width).
> Registry DEC-8 is `(38,20)*(38,20)` plan-refuse — a different class.

### DEC-6 — max `DECIMAL(38,0) + 1` under ANSI returns a corrupted value

- **repark** — `CAST(999…9 AS DECIMAL(38,0)) + CAST(1 AS DECIMAL(38,0))` stores `10^38` at
  declared `decimal128(38,0)` (no raise). U2 removed the prior float-residue photograph;
  leftover is wrap-not-residue.
- **Apache Spark** — under ANSI raises `ArithmeticException` / `NUMERIC_VALUE_OUT_OF_RANGE`.
  *(oracle: recorded.)*
- **Pin** — `[overflow_max_decimal38_plus_one_raises_in_spark]`; Rust
  `crates/repark-spark/src/tests/decimal.rs::pin_overflow_max_decimal38_plus_one_wrong_value_i128`
- **Rationale** — BACKLOG, intent to FIX (gap G13 — ANSI overflow raise, or honest NULL under
  non-ANSI). Silently-wrong-result class on the integrity path.
  **Dated 2026-08-13 (W-2 / #84):** photograph updated to the wrap. ANSI raise is campaign
  DEC U5 / G13 (not this increment).

### DEC-7 — `DECIMAL / 0` under ANSI returns NULL

- **repark** — returns NULL at a decimal result type: `(38,0)/(38,0)` → NULL at
  `decimal128(38,4)`; small `(2,0)/(2,0)` → NULL at `decimal128(6,4)`.
- **Apache Spark** — under ANSI raises `ArithmeticException` / `DIVIDE_BY_ZERO` for both recipes.
  *(oracle: recorded.)*
- **Pin** — `[div_by_zero_decimal38_raises_in_spark_null_in_repark]`,
  `[div_by_zero_small_decimal_raises_in_spark_null_in_repark]`
- **Rationale** — BACKLOG, intent to FIX (gap G13). NULL-vs-raise is an integrity divergence for
  any consumer that distinguishes error from missing.

### DEC-8 — `DECIMAL(38,20) * DECIMAL(38,20)` refuses at plan time

- **repark** — still refuses with `AnalysisException` (`Cannot get result type for decimal
  operation … 38,20 * 38,20`) at **plan construction** (`BinaryExpr::get_type` / Arrow
  `s>38`) before any `AnalyzerRule` runs.
- **Apache Spark** — clamps the product to `decimal128(38,6)` and succeeds. *(oracle: recorded.)*
- **Pin** — `[mul_38_20_plans_in_spark_refuses_in_repark]` + Rust
  `pin_mul_38_20_still_refuses_at_plan` +
  `repark_functions::decimal_precision::tests::mul_38_20_still_refuses_before_any_analyzer_rule`
- **Rationale** — BACKLOG, intent to FIX via an **ExprPlanner** (U4b-adjacent). V-2 U4a's
  analyzer cannot see a node that never plans. Do not fold this into DEC-3's FIXED note.
  Not campaign DEC-8 (U3 integer-literal min-precision — that closed DEC-5 **width**).

### DEC-9 — overflow-capable binary arithmetic is marked non-null

- **repark** — marks small mul/add results **non-null** while values and `(p,s)` agree with
  Spark: `9*9` → `(3,0)` non-null `81`; `9+9` → `(2,0)` non-null `18`; `999*999` → `(7,0)`
  non-null `998001`.
- **Apache Spark** — marks the same results **nullable** (overflow-capable binary arithmetic) at
  the same types and values. *(oracle: recorded.)*
- **Pin** — `[mul_single_digit_nullability_differs]`, `[add_single_digit_nullability_differs]`,
  `[mul_three_digit_capacity_nullability_differs]`
- **Rationale** — BACKLOG, intent to FIX (gap G13). Nullability-only pin; a schema-sensitive
  consumer that trusts non-null is wrong under repark's marking.

> **The 2026-08-12 landing-truth sweep (L-1)** pasted the overnight-wave §6 handoffs after
> re-verifying each against merged `main` (`baf6617`). Equalities and already-landed pins are
> classified in `task/l1-landing-truth-ledger.md`, not restated here.
>
> **The 2026-08-13 Y-wave increment (Z-5)** pasted the merged Y-wave §6 handoffs after
> re-verifying each against frozen `9b2dce3` (PRs #66–#72). Classification:
> [`task/z5-landing-increment-ledger.md`](../task/z5-landing-increment-ledger.md).
>
> **The 2026-08-13 Z-wave increment (W-5)** pasted the merged Z-wave §6 handoffs after
> re-verifying each against frozen `c7e6589` (PRs #75–#79). Classification:
> [`task/w5-z-landing-ledger.md`](../task/w5-z-landing-ledger.md). TZ-6 / TZ-7 sections
> were not touched (PR-2 owns those two headings). No new `live-mirror:` tokens.
>
> **The 2026-08-13 W-wave increment (V-5)** pasted the merged W-wave §6 handoffs after
> re-verifying each against frozen `8d325d4` (PRs #81–#85). Classification:
> [`task/v5-w-landing-ledger.md`](../task/v5-w-landing-ledger.md). TZ-6 / TZ-7 FIXED
> notes were already in-file from #85 (not duplicated). No new `live-mirror:` tokens.
>
> **The 2026-08-14 V-wave increment (S-5)** pasted the merged V-wave §6 handoffs after
> re-verifying each against frozen `d9a7391` (PRs #87–#91). Classification:
> [`task/s5-v-landing-ledger.md`](../task/s5-v-landing-ledger.md). TZ-6 / TZ-7 FIXED
> notes were already in-file from #85 (not duplicated). No new `live-mirror:` tokens.

### FN-1 — `element_at` out of range is NULL under ANSI

- **repark** — `element_at(array(1, 2), 5)` and `element_at(array(1, 2), -5)` are
  NULL at the element type on BOTH doors (Spark `.sql()` and the facade
  DataFrame API). Index `0` still raises `INVALID_INDEX_OF_ZERO`.
- **Apache Spark** — under ANSI (the Spark 4 default, and repark's) raises
  `INVALID_ARRAY_INDEX_IN_ELEMENT_AT`. *(oracle: documented Spark 4.1.2; not
  re-derived live — this needs **Spark**, and no pyspark is installed in this
  worktree's `.venv`. The repair round's JVM probe is `java.net.URI`, which has
  nothing to say about ANSI `element_at`.)*
- **Pin** — `python/repark/tests/test_functions_gt2.py::test_ansi_pair_is_null_not_a_raise`
- **Rationale** — BACKLOG. repark's `spark.sql.ansi.enabled` is TRUE by default
  but its implemented scope is `/` and `%` by zero
  (`crates/repark-functions/src/ansi.rs`; docs/guide/session-and-conf.md). NULL
  vs raise is an integrity divergence for any consumer that distinguishes an
  error from a missing value. Same class as DEC-6/DEC-7.

### FN-2 — `make_date` with an invalid Y-M-D is NULL under ANSI

- **repark** — `make_date(2024, 2, 31)` and `make_date(2024, 13, 1)` are NULL at
  `date32` on BOTH doors.
- **Apache Spark** — under ANSI raises `DATETIME_FIELD_OUT_OF_BOUNDS`.
  *(oracle: documented Spark 4.1.2; not re-derived live.)*
- **Pin** — `python/repark/tests/test_functions_gt2.py::test_ansi_pair_is_null_not_a_raise`
- **Rationale** — BACKLOG, same class and same rationale as FN-1.


### G6-3 — DATE→INT: Spark refuses; repark yields days-since-epoch

> **CLOSED 2026-08-15.** `CAST`/`TRY_CAST` between `DATE` and any signed integer width now
> refuses at ANALYSIS with Spark's own class — `[DATATYPE_MISMATCH.CAST_WITH_FUNC_SUGGESTION]`,
> naming `UNIX_DATE` as the remedy — from a deny matrix in
> `crates/repark-functions/src/analyzer/cast_legality.rs`, called at the head of
> `SparkExprSemantics`'s `Expr::Cast` arm and from a NEW `Expr::TryCast` arm. The gate must live
> at analysis, not the optimizer: `datafusion-spark`'s `unix_date` — the remedy the message
> names — lowers to a textually identical `CAST(a AS Int32)` in `simplify_expressions`, one
> stage later. Five recipes converged, not one: the SQL `INT` and `BIGINT` doors, SQL
> `try_cast`, `Column.cast` and `Column.try_cast`, all recorded as shared-raise equalities in
> `test_cast_failure_parity.py`. The `live-mirror: cast_date_to_int_spark_refuses` disclosure is
> retired with the row (a disclosure detects a DIVERGENCE; a converged pair belongs in the
> corpus). Design: `planning/hardening/G63-DATE-INT-DESIGN.md`. Retired per §6.

### G6-5 — INT→DATE: Spark refuses; repark yields a date

> **CLOSED 2026-08-15, in the same change as G6-3.** The reverse direction is the same Spark
> class with the reverse remedy (`DATE_FROM_UNIX_DATE`): `CAST(18262 AS DATE)` answered
> `2020-01-01` non-null in repark and refused in Spark. It was unpinned anywhere — not in the G6
> corpus, not in this registry — and closing G6-3 alone would have left the class half-shut,
> which §6 discipline forbids. Now pinned as a shared-raise equality:
> `python/repark/tests/test_cast_failure_parity.py::test_cast_failure_row[int_to_date_both_refuse]`.
> Retired per §6.

### G6-4 — TIMESTAMP→INT nullability only (after TZ-5)

- **repark** — `CAST(TIMESTAMP '2020-01-01 00:00:00' AS INT)` under UTC yields int32
  **non-null** `1577836800` (unix seconds).
- **Apache Spark** — same value and Arrow type; the CAST is typed **nullable**.
  *(oracle: recorded.)*
- **Pin** — `python/repark/tests/test_cast_failure_parity.py::test_cast_failure_row[timestamp_to_int_nullability]`
- `live-mirror: cast_timestamp_to_int_nullability`
- **Rationale** — BACKLOG, intent to FIX or DECLARE (same class as G12 null-safe-equal
  nullability). X-1 originally queued this as raise-vs-value; #64 un-refused the INT path
  and the residual is nullability only (`task/tz5-cast-seconds-ledger.md` §10).

### G12-1 — null-safe equal result nullability (SQL `<=>`)

- **repark** — `SELECT (NULL <=> NULL) AS nse` yields Arrow `bool` **nullable** (value TRUE).
- **Apache Spark** — same expression yields Arrow `bool` **non-nullable** (value TRUE).
  *(oracle: recorded.)*
- **Pin** —
  `python/repark/tests/test_three_valued_logic_parity.py::test_tvl_parity_row[null_eq_vs_null_safe_eq]`
- `live-mirror: null_safe_eq_sql_nullability`
- **Rationale** — BACKLOG, intent to FIX or DECLARE (gap G12). VALUE already matches; only
  schema nullability diverges.

### G12-2 — null-safe equal result nullability (DataFrame `eqNullSafe`)

- **repark** — `Column.eqNullSafe` select yields Arrow `bool` **nullable** (values match Spark).
- **Apache Spark** — same recipe yields Arrow `bool` **non-nullable**. *(oracle: recorded.)*
- **Pin** —
  `python/repark/tests/test_three_valued_logic_parity.py::test_tvl_parity_row[df_eq_null_safe_select]`
- `live-mirror: null_safe_eq_df_nullability`
- **Rationale** — BACKLOG, intent to FIX or DECLARE (gap G12 — DF door twin of G12-1).

### FLOAT-AGG-1 — sum of catastrophic-cancellation float vector

- **repark** — `sum(v)` over the G7 fixture lands **3.75** at
  `spark.sql.shuffle.partitions = 2` on a VALUES source. Type: Arrow `float64` nullable.
- **Apache Spark** — same recipe under `local[2]`, ANSI on, `spark.sql.shuffle.partitions=2`
  lands **2.25**. Type: Arrow `float64` nullable. *(oracle: recorded.)*
- **Pin** —
  `python/repark/tests/test_float_agg_parity.py::test_float_agg_parity_row[sum_catastrophic_cancellation_fixture]`
  and `crates/repark-spark/src/tests/float_agg.rs::pin_sum_f64_bits_at_target_partitions_2`
- `live-mirror: sum_catastrophic_cancellation_fixture`
- **Rationale** — accumulation-order sensitivity on a catastrophic-cancellation fixture;
  value diverges, type agrees. DECLARE candidacy until a G7 fix lands.

### FLOAT-AGG-2 — avg of the same fixture

- **repark** — `avg(v)` lands **0.46875**.
- **Apache Spark** — lands **0.28125**. *(oracle: recorded.)*
- **Pin** —
  `python/repark/tests/test_float_agg_parity.py::test_float_agg_parity_row[avg_catastrophic_cancellation_fixture]`
  and `crates/repark-spark/src/tests/float_agg.rs::pin_avg_f64_bits_at_target_partitions_2`
- `live-mirror: avg_catastrophic_cancellation_fixture`
- **Rationale** — follows FLOAT-AGG-1 (avg = sum/8); same accumulation-order class.

### G18-1 — array-column list value-field name (`item` vs `element`)

- **repark** — `createDataFrame` of an array column exports Arrow list value field named `item`.
- **Apache Spark** — same recipe exports list value field named `element`. Values match.
  *(oracle: recorded.)*
- **Pin** —
  `python/repark/tests/test_nested_container_parity.py::test_nested_row_matches_spark_or_still_diverges[array_column_roundtrip]`
- `live-mirror: nested_array_list_field_name`
- **Rationale** — TYPE disclosure (list field name). Unlocked by G18; fix is G10 / list-type
  follow-on.

### G18-2 — `collect_list` list nullability and value-field name

- **repark** — `groupBy.agg(collect_list)` exports `list<item: int64>` **nullable** (elements
  nullable).
- **Apache Spark** — exports `list<element: int64 not null>` **non-nullable**. Values match
  under the G18 order-insensitive comparator. *(oracle: recorded.)*
- **Pin** —
  `python/repark/tests/test_nested_container_parity.py::test_nested_row_matches_spark_or_still_diverges[collect_list_grouped]`
- `live-mirror: nested_collect_list_nullability`
- **Rationale** — TYPE disclosure (field name + collect_list nullability). Same G10 follow-on.

### G18-3 — array-of-struct list value-field name

- **repark** — array-of-struct `createDataFrame` wraps `struct<x, y>` in a list value field
  named `item`.
- **Apache Spark** — wraps the same struct in a list value field named `element`. Values match.
  *(oracle: recorded.)*
- **Pin** —
  `python/repark/tests/test_nested_container_parity.py::test_nested_row_matches_spark_or_still_diverges[array_of_struct_roundtrip]`
- `live-mirror: nested_array_of_struct_list_field_name`
- **Rationale** — TYPE disclosure; sibling of G18-1 on a nested list+struct shape.

### G10-1 — typed-map `toPandas` cells are list-of-pairs, not dict

- **repark** — `toPandas` of a typed map column yields object-dtype **list-of-pairs** cells
  (raw Arrow map → pandas).
- **Apache Spark** — the same recipe yields object-dtype **dict** cells. Values otherwise
  match. *(oracle: recorded.)*
- **Pin** —
  `python/repark/tests/test_boundary_shapes_parity.py::test_boundary_row_matches_spark_or_still_diverges[map_topandas_cell_shape]`
- **Rationale** — BACKLOG, interchange SHAPE (G10). Fix is a G10 follow-on, not silent
  absorption. No `live-mirror`: the live-oracle tier has no scenario for this pandas-cell
  SHAPE class (same as the G5-RANK-TYPE family).

### G10-2 — typed-struct Long field stays `int` where Spark's second row is `float`

- **repark** — `toPandas` of a typed struct Long field stays Python **int** `20` on the
  recorded second row.
- **Apache Spark** — the same recipe lands that Long as Python **float** `20.0` (row-0
  stays int `10`; recorded live-Spark fact under arrow-on toPandas). *(oracle: recorded.)*
- **Pin** —
  `python/repark/tests/test_boundary_shapes_parity.py::test_boundary_row_matches_spark_or_still_diverges[struct_topandas_cell_shape]`
- **Rationale** — BACKLOG, interchange SHAPE (G10). Type-sensitive compare so `20 == 20.0`
  cannot launder the row. No `live-mirror` (same SHAPE-class reason as G10-1).

### G10-3 — pandas-ingest object-list arrays export `item` vs `element`

- **repark** — `createDataFrame` from a pandas object-dtype list column exports Arrow
  `list<item: int64>`.
- **Apache Spark** — the same ingest exports `list<element: int64>`. Values match.
  *(oracle: recorded.)*
- **Pin** —
  `python/repark/tests/test_boundary_shapes_parity.py::test_boundary_row_matches_spark_or_still_diverges[array_from_pandas_object]`
- **Rationale** — BACKLOG, interchange SHAPE (G10 — pandas ingest twin of G18-1). No
  `live-mirror` (same SHAPE-class reason as G10-1). G18-1's live-mirror is the VALUES
  entry; this row is the pandas ingest twin.

### G10-4 — inbound `datetime64[us]` exports as `datetime64[us]`, not `[ns]`

- **repark** — `createDataFrame` from pandas `datetime64[us]` then `toPandas` keeps
  `datetime64[us]`. Wall-clock values match Spark.
- **Apache Spark** — the same recipe exports `datetime64[ns]`. *(oracle: recorded.)*
- **Pin** —
  `python/repark/tests/test_boundary_shapes_parity.py::test_boundary_row_matches_spark_or_still_diverges[pandas_timestamp_unit_from_pandas_us]`
- **Rationale** — BACKLOG, interchange SHAPE (G10 timestamp-unit family). The inbound
  `datetime64[ns]` twin is an equality control, not a row. No `live-mirror` (same
  SHAPE-class reason as G10-1).

> **REG-G4-1 / REG-G4-2 — DataFrame `leftsemi` / `leftanti` — FIXED (2026-08-11, G4b / #63).**
> W-3 queued both as BACKLOG surface gaps. G4b implemented the `how`-token widening and
> facade alias map; the corpus rows
> `test_join_parity.py::test_join_parity_row[df_left_semi_on_name]` and
> `…[df_left_anti_on_name]` are now content equalities. A
> fixed defect gets this dated note, never a live divergence row.

> **G4b D6 — H1 origin-map gap on semi/anti results — FIXED (2026-08-12, Y-5 / #70).** After
> `left.join(right, on, "leftsemi"|"leftanti")`, `select` / `filter` / `withColumn` of a
> right-parent Column raise `AnalysisException` carrying Spark 4.1.2's
> `MISSING_ATTRIBUTES.RESOLVED_ATTRIBUTE_APPEAR_IN_OPERATION` (same-name) or
> `MISSING_ATTRIBUTES.RESOLVED_ATTRIBUTE_MISSING_FROM_INPUT` (distinct-name).
> `drop(right[…])` is a no-op (left schema unchanged). Left-parent refs and inner-join origin
> resolution are unchanged. Pin:
> `python/repark/tests/test_g4b_semi_join.py::test_right_ref_select_raises_missing_attributes_same_key`
> (and the filter / withColumn / drop / left-ref / inner / distinct-name siblings). **2026-08-13
> (Z-4 / #77):** the Y-5 SAF-001 residual is closed — `F.abs(right[…])` after semi/anti now
> raises the same `MISSING_ATTRIBUTES` classes (`test_right_ref_abs_raises_missing_attributes_same_key`
> and the left / inner / distinct-name / `F.lower` siblings). The origin-thread rides
> `functions._scalar` (e.g. `F.lower`) and the other named Column wrappers in `functions.py`;
> aggregate builders (`F.sum` / `F.count` / …) were a named residual (Z-4 Q-002).
> **2026-08-13 (W-4 / #82):** Q-002 is closed — after
> `left.join(right, on, "leftsemi"|"leftanti")`,
> `select(F.sum|count|avg|min|max|count_distinct|first|last(right[…]))` raises
> `AnalysisException` carrying Spark 4.1.2's
> `MISSING_ATTRIBUTES.RESOLVED_ATTRIBUTE_APPEAR_IN_OPERATION` (same-name) or
> `MISSING_ATTRIBUTES.RESOLVED_ATTRIBUTE_MISSING_FROM_INPUT` (distinct-name). Left refs
> and inner-join refs still resolve. `count_distinct(left, right)` still raises on the
> unemitted right (`join_sql` QCOL scan, not first-origin-only). Pin:
> `python/repark/tests/test_g4b_semi_join.py::test_right_ref_agg_raises_missing_attributes_same_key`
> (and left / inner / distinct-name / `count_distinct` left-then-right siblings). No
> remaining D6 divergence to disclose; no `live-mirror`. Conditionless semi/anti refusal
> remains [G4-3](#g4-3--conditionless-dataframe-semiant-join-refuses). A fixed defect
> gets this dated note, never a live divergence row.

### G4-3 — conditionless DataFrame semi/anti join refuses

- **repark** — `df.join(other, how="leftsemi")` with no `on` (and `on=[]`) raises
  `AnalysisException`: join type `leftsemi`/`leftanti` requires an `on` condition; a
  conditionless semi/anti is not a Cartesian product.
- **Apache Spark** — `on=None` keeps every left row when the right side is non-empty and none
  when it is empty; the anti side is the complement. `on=[]` raises a PySpark `IndexError`.
  *(oracle: recorded live, PySpark 4.1.2.)*
- **Pin** — `python/repark/tests/test_g4b_semi_join.py::test_conditionless_semi_family_refuses_loud`
- `live-mirror: conditionless_semi_anti_refuses`
- **Rationale** — DELIBERATE refusal, low priority to fix. The facade's only fallback is the
  Cartesian path, which returns an m×n result set — a wrong answer, not a narrower one.

### G5-RANK-TYPE-1 — SQL-door `rank()` Arrow type

- **repark** — `rank() OVER (ORDER BY k)` yields Arrow `uint64` non-null (values match Spark).
- **Apache Spark** — yields `int32` non-null with the same values. *(oracle: recorded.)*
- **Pin** — `python/repark/tests/test_window_parity.py::test_window_row_matches_spark_or_still_diverges[rank_with_ties]`
- **Rationale** — BACKLOG, intent to FIX (gap G5). SQL door leaves DataFusion UInt64; DF-API
  door already casts row_number to IntegerType.

### G5-RANK-TYPE-2 — SQL-door `row_number()` Arrow type (total order)

- **repark** — `row_number() OVER (ORDER BY k, id)` → `uint64`.
- **Apache Spark** — `int32`. *(oracle: recorded.)*
- **Pin** — `python/repark/tests/test_window_parity.py::test_window_row_matches_spark_or_still_diverges[row_number_total_order]`
- **Rationale** — BACKLOG, intent to FIX (gap G5). Sibling of G5-RANK-TYPE-1.

### G5-RANK-TYPE-3 — SQL-door `ntile` Arrow type

- **repark** — `ntile(4) OVER (ORDER BY id)` → `uint64`.
- **Apache Spark** — `int32`. *(oracle: recorded.)*
- **Pin** — `python/repark/tests/test_window_parity.py::test_window_row_matches_spark_or_still_diverges[ntile_4_total_order]`
- **Rationale** — BACKLOG, intent to FIX (gap G5). Completes the ranking-family type class.

> **Temporal `RANGE` window frames — supported, with a corrected bare-offset envelope
> (G5b / #62, 2026-08-11).** A `RANGE` frame bounded by an interval over a `TIMESTAMP` or
> `DATE` order key matches Spark 4.1.2 on value and Arrow type through the facade `sql()`
> door. A **unit-less** offset over a datetime order key (`RANGE BETWEEN 1 PRECEDING`) no
> longer silently means one *month*: over a `TIMESTAMP` key the door refuses with Spark's
> `DATATYPE_MISMATCH.RANGE_FRAME_INVALID_TYPE`, and over a `DATE` key it means days.
> Pins: `crates/repark-spark/src/tests/window_temporal_range.rs` and the `temporal_range`
> family in `python/repark/tests/test_window_parity.py`. **G5b-R2 and Spark-door G5b-R3
> closed in Y-1 / #72** (FIXED notes below). **G5b-R1 and G5b-R5 closed in W-4 / #82**
> (FIXED notes below). **G5b-R4 stays OPEN.** This increment does not claim R4 closed.

> **G5b-R2 — `DAY TO SECOND` qualified interval as a frame bound — FIXED (2026-08-12,
> Y-1 / #72).** `INTERVAL '1 12:00:00' DAY TO SECOND` (and `'1 0:0:0'`) as a frame bound
> matches Spark 4.1.2 on value and Arrow type. The Spark door restates the qualified
> literal as an Arrow-accepted interval string (`1 days 12 hours 0 minutes 0 seconds`) and
> re-plans. Pins (now equalities):
> `crates/repark-spark/src/tests/window_temporal_range.rs::temporal_range_day_to_second_literal_matches_spark`
> and
> `python/repark/tests/test_window_parity.py::test_window_row_matches_spark_or_still_diverges[temporal_range_day_to_second_literal]`.
> A fixed defect gets this dated note, never a live divergence row.

> **G5b-R3 — negative interval offset wrap (`count(*)` = -1) — FIXED (2026-08-12, Y-1
> Half-B / #72).** Invert is kind **or** same-kind magnitude after sign-normalize. Kind
> invert vs CURRENT ROW (`INTERVAL '-1' DAY PRECEDING AND CURRENT ROW`) is Spark's empty
> frame (`count(*)` 0, `sum` NULL) via `FILTER (WHERE false)` over a current-row frame.
> Same-kind magnitude invert (`-2 PRECEDING AND -1 PRECEDING`, direct `2 FOLLOWING AND 1
> FOLLOWING`) refuses at classify with Spark's `SPECIFIED_WINDOW_FRAME_WRONG_COMPARISON`
> (live 4.1.2). The previous silent-wrong (`count(*)` = **-1** in release wheels; `sum`
> panics in debug) is gone on the **Spark door / facade `.sql()`**. The far-future
> `10000 YEAR FOLLOWING` pair is gone (not Spark-empty). DATE + negative already answered
> empty and is unchanged. The **ANSI door does not call this seam and still wraps** —
> named residual, not silently absorbed (no dedicated pin → no G5b-R3-ANSI row). A
> statement that mixes a negative TIMESTAMP interval with a numeric unit-less `RANGE`
> bound is refused (`UNSUPPORTED.NEGATIVE_RANGE_OFFSET`) so wrapping cannot ride the
> mixed-statement hole. Pins:
> `crates/repark-spark/src/tests/window_temporal_range.rs::temporal_range_negative_offset_is_spark_empty_frame`,
> `…::temporal_range_value_inverted_frames_do_not_wrap`,
> `python/repark/tests/test_window_parity.py::test_window_row_matches_spark_or_still_diverges[temporal_range_negative_offset_count]`
> and `…[temporal_range_negative_offset_sum]`.

> **G5b-R1 — unquoted `INTERVAL n UNIT` frame bound refused — FIXED (2026-08-13, W-4 / #82).**
> Unquoted `INTERVAL 1 DAY` is quoted to `INTERVAL '1' DAY` before first plan and matches
> the quoted table `[10, 30, 60, 90, 90]`. Pins:
> `crates/repark-spark/src/tests/window_temporal_range.rs::temporal_range_unquoted_interval_literal_matches_quoted`
> and
> `python/repark/tests/test_window_parity.py::test_window_row_matches_spark_or_still_diverges[temporal_range_unquoted_interval_literal]`
> (now equality). A fixed defect gets this dated note, never a live divergence row.

### G5b-R4 — FOLLOWING-to-FOLLOWING frame includes the current row

- **repark** — `INTERVAL '1' DAY FOLLOWING AND INTERVAL '2' DAY FOLLOWING` includes the
  current row (sums 120 where Spark sums 90 on the recorded seed).
- **Apache Spark** — the current row lies outside that frame. *(oracle: recorded.)*
- **Pin** — `python/repark/tests/test_window_parity.py::test_window_row_matches_spark_or_still_diverges[temporal_range_following_to_following_window]`
- **Rationale** — BACKLOG (G5b-R). Range-search boundary. Still OPEN.
  W-4 re-verified on this base: DF 54.1.0 still 120; Spark 4.1.2 is 90;
  `sqlparser` 0.62 `WindowFrame` is `// TBD: EXCLUDE`; no dependency bump. Rust pin
  `temporal_range_following_to_following_still_includes_current_row` still asserts 120.

> **G5b-R5 — interval bound over a numeric order key — FIXED (2026-08-13, W-4 / #82).**
> `INTERVAL 'n' UNIT` over a numeric order key is restated to unit-less `n` RANGE (unit
> ignored). Unique-key seed `[10, 20, 30, 40, 50]` is `n=1` on gaps of 10. Ties seed
> distinguishes magnitude (`1 DAY` → `[10, 40, 40, 61, 30]`; `10 DAY` →
> `[10, 50, 50, 61, 91]`; `0 DAY` → peer group). Y-1's "each unique key sees only
> itself" is the unique-`v` special case of numeric `n=1`, not a distinct class. Pins:
> `crates/repark-spark/src/tests/window_temporal_range.rs::temporal_range_interval_bound_over_int_key_is_numeric_n`,
> `…::temporal_range_interval_bound_over_int_key_uses_numeric_magnitude`,
> `python/repark/tests/test_window_parity.py::test_window_row_matches_spark_or_still_diverges[temporal_range_interval_bound_over_int_key]`
> (now equality), and facade
> `test_temporal_range_interval_over_int_uses_numeric_magnitude`. A fixed defect gets
> this dated note, never a live divergence row.

### G3-E8 — DELETE/UPDATE subquery predicate is refused (valve, not a fix)

- **repark** — `DELETE` / `UPDATE` with a subquery `WHERE` are still **refused** (needle
  `subquery predicates are silently mis-executed`) **except** uncorrelated
  `DELETE … WHERE col IN (SELECT col FROM …)`,
  `DELETE … WHERE col NOT IN (SELECT col FROM …)` (including ANY-NULL-in-subquery
  matches nothing, empty subquery matches all), and
  `DELETE … WHERE [NOT] EXISTS (SELECT …)` both uncorrelated (all-or-nothing) and
  correlated (per-row semi/anti-join, including NULL join keys and duplicate rows).
  Those execute on both doors via the A1-identity path (`execute_predicate_dml`) and
  match Spark (FROM and FROM-less). UPDATE IN / NOT IN / EXISTS, correlated IN,
  ANY/ALL, scalars, nested, mixed AND/OR remain refused. SET-assignment / `INSERT` /
  `MERGE` source still unaffected.
- **Apache Spark** — runs all of them, deleting/updating exactly the matching rows.
  *(oracle: recorded — PySpark 4.1.2 + Iceberg 1.11.0.)*
- **Pin** — `python/repark/tests/test_dml_subquery_parity.py::test_dml_subquery_row[delete_in_subquery]`,
  `…[delete_not_in_subquery]`, `…[delete_not_in_subquery_with_null_key]`,
  `…[delete_exists_correlated]`, `…[delete_not_exists_correlated]` (now **content**)
  plus the uncorrelated / none / all / empty / NULL-key / duplicate EXISTS content
  rows; residual splits `delete_correlated_in_subquery`, `update_in_subquery`,
  `update_not_in_subquery_with_null_key`. Rust:
  `crates/repark-spark/src/tests/dml.rs::g3e8_delete_in_subquery_deletes_exactly_the_matching_row`
  (and quoted / temp-view / FROM-less siblings);
  `…::g3e8_delete_not_in_subquery_deletes_non_matching_rows`,
  `…::g3e8_delete_not_in_subquery_with_null_key_deletes_nothing`,
  `…::g3e8_delete_not_in_empty_subquery_deletes_every_row`;
  `…::g3e8_delete_exists_uncorrelated_and_correlated_execute`;
  `…::g3e8_delete_subquery_family_all_refuse` (IN / NOT IN / EXISTS excluded — residual refuse);
  `…::g3e8_update_subquery_family_all_refuse`;
  ANSI `crates/repark-sql/src/guards/tests.rs::dml_subquery_in_delete_executes_and_deletes_exactly_the_match`,
  `…::dml_subquery_not_in_delete_executes_and_honors_three_valued_logic`,
  `…::dml_subquery_exists_delete_executes_uncorrelated_and_correlated`;
  ROW 9 `crates/repark-sql/tests/cross_door.rs::cross_door_g3e8_refusals_render_identically`
  restated over correlated IN / UPDATE IN / nested / scalar;
  `…::cross_door_g3e8_not_in_delete_executes_identically`;
  `…::cross_door_g3e8_exists_delete_executes_identically`.
- **Rationale** — DEFECT, **partial fix**. IN + NOT IN + `[NOT] EXISTS` ± correlation
  all execute both doors — the dbt-upgrade gate is MET. The family is **not**
  "fixed" while UPDATE IN + correlated IN / ANY / ALL stay valved. Delete this row
  only when the claimed surface is actually re-enabled.

### G3-E8-NULL — `NOT IN (SELECT …)` with a NULL key (3VL trap, keep after the fix)

- **repark** — `DELETE … NOT IN (SELECT …)` where the subquery contains `NULL` now
  matches Spark (identity SELECT reproduces 3VL: ANY NULL ⇒ match zero rows; table
  unchanged). `UPDATE … NOT IN (SELECT …)` with a NULL key stays refused (same
  G3-E8 valve).
- **Apache Spark** — `x NOT IN (…, NULL)` is UNKNOWN for every row, so Spark matches nothing
  and the table is unchanged. *(oracle: recorded.)*
- **Pin** — `python/repark/tests/test_dml_subquery_parity.py::test_dml_subquery_row[delete_not_in_subquery_with_null_key]`
  (now **content**, remaining ids `{1,2,3}`) and `…[update_not_in_subquery_with_null_key]`
  (still **split**). Rust:
  `crates/repark-spark/src/tests/dml.rs::g3e8_delete_not_in_subquery_with_null_key_deletes_nothing`.
- **Rationale** — keep after the DELETE fix landed: the 3VL trap is surprising enough
  to be re-broken. Flip the UPDATE half to "matches Spark" when that spelling ships.

### G15 — string collation is refused at first evaluation

- **repark** — string collation is **refused** at parse / first evaluation on the inventoried
  compare/order-changing paths. SQL `expr COLLATE name`, `ORDER BY col COLLATE name`,
  `CREATE TABLE … (col STRING COLLATE name)`, `CAST(x AS STRING COLLATE name)`,
  `SET`/`RESET spark.sql.collation.*`, `createDataFrame` with a non-`UTF8_BINARY`
  `StringType` (including Spark JSON `__COLLATIONS`), and `Column.cast`/`try_cast` to a
  collated string all raise `UnsupportedOperationException` (`NotImplemented` on the Rust
  doors) naming the requested collation and steering to binary/default ordering.
  `StringType(collation=…)` construction and `simpleString` display stay (schema metadata).
  `F.collate` / `F.collation` / `Column.collate` are not on the facade (`AttributeError`).
- **Apache Spark** — Spark 4.0+ applies the named collation to comparisons and `ORDER BY`.
  `createDataFrame([("Alice",), ("alice",)], StringType("UNICODE_CI")).distinct().count()`
  is **1**. `F.collate` / `F.collation` return a collated column / its name
  (`SYSTEM.BUILTIN.UNICODE`). *(oracle: recorded — Apache 4.1.2 tests
  `test_create_df_with_collation`, `test_collation`; live probe in
  `task/y7-collation-refuse-ledger.md` §0b.)*
- **Pin** — `python/repark/tests/test_collation_refuse.py::test_create_dataframe_unicode_ci_refuses`,
  `…::test_from_json_collations_metadata_constructs_and_create_refuses`,
  `…::test_sql_collate_expression_refuses`,
  `…::test_sql_order_by_collate_refuses`,
  `…::test_sql_cast_as_string_collate_refuses`,
  `…::test_sql_set_collation_key_refuses`,
  `…::test_cast_collated_string_type_refuses`,
  `…::test_conf_set_collation_key_refuses`,
  `crates/repark-spark/src/tests/collation.rs::select_collate_expression_refuses`,
  `…::order_by_collate_refuses`,
  `…::cast_as_string_collate_refuses`,
  `…::set_collation_session_key_refuses_via_execute`,
  `…::execute_passthrough_attaches_collation_valve`,
  `…::create_table_column_collate_refuses`,
  `crates/repark-sql/src/guards/tests.rs::collation_valve_fires_on_expression_collate`,
  `…::collation_valve_fires_on_cast_as_string_collate`,
  `…::collation_valve_refuses_end_to_end_and_default_select_is_untouched`.
- **Rationale** — DEFECT, refused pending a future implement-or-keep-absent decision.
  **History:** G15 (MEDIUM) — "collation is unimplemented and silently wrong-count." The
  census row `pyspark.sql.tests.test_dataframe.DataFrameTests.test_create_df_with_collation`
  was `FAIL-VALUE` / `2 != 1`: repark accepted `StringType("UNICODE_CI")`, stripped it to
  binary `STRING`, and counted Alice/alice as two values. SQL `COLLATE` was already a raw
  DataFusion unsupported-AST error (`test_collated_string` = `FAIL-MISSING`), not an
  actionable refusal. **Ruling provenance:** owner 2026-08-12, conductor-4 A5 (scope =
  compare/order-changing paths; constructor + simpleString stay; refuse at first
  evaluation; ABSENCE IS LOUD) and A10 (parse-altitude refuse on `spark_ast.rs` + repark-sql
  guard sites; G3-E8 lesson). Keep this row until collation is implemented or the product
  permanently documents absence without a silent path.

### BL-8 — SQL-door count-like aggregates return `UInt64`

- **repark** — the **facade** casts a count-like aggregate to signed `bigint`
  (`df.agg(F.regr_count("y", "x"))` → `int64`, `F.approx_count_distinct` likewise), taken from the
  aggregate's own declared return type rather than a name list. The **SQL door** does not:
  `SELECT regr_count(y, x)` and `SELECT approx_distinct(g)` hand back Arrow `UInt64`. So the two
  doors reach the same kernel and disagree on the result type.
- **Apache Spark** — `bigint` on both, and Spark has no unsigned type at all.
  *(oracle: live — PySpark 4.1.2: `regr_count` → `struct<r:bigint>`,
  `approx_count_distinct` → `struct<r:bigint>`.)*
- **Pin** — `python/repark/tests/test_fnp5_aggregates.py::test_regression_aggregates_agree_with_the_sql_door`,
  whose `DOOR_RETURNS_UNSIGNED` set is a **ratchet**: the pin asserts the door still returns
  unsigned, so closing this row turns it RED on purpose.
- **Rationale** — BACKLOG, split deliberately. The facade is the surface the parity campaign is
  about and it is now correct; correcting the door means moving the cast into the shared analyzer
  layer, where the rewrite must be idempotent across re-analysis and must not rename an `Aggregate`
  node's output field that a parent `Projection` refers to by name. That is an engine-semantics
  unit. Recorded rather than left as a STATUS promise, because a `UInt64` column written to
  Parquet or Iceberg is read back by Spark as `decimal(20,0)` and does not round-trip — the cost of
  the gap is on disk, not just in a schema string.

### RE-2 — a zero-width match at a mid-surrogate position

- **repark** — `regexp_extract_all('🎉ab', '', 0)` returns **4** empty strings and
  `regexp_extract_all('🎉ab', 'b*', 0)` returns 4 elements; `regexp_substr('🎉ab', '')` returns
  `''`. `regexp_count` on the same inputs returns **5**, so two functions in this repository
  disagree.
- **Apache Spark** — **5** in every case (`['','','','','']`, `['','','','b','']`), and
  `regexp_substr('🎉ab', '')` returns **NULL**. Java's `Matcher` finds an empty match at every
  UTF-16 code-unit index, including the one *inside* a surrogate pair.
  *(oracle: live — PySpark 4.1.2.)*
- **Pin** — `python/repark/tests/test_lrs6_regexp_divergences.py::test_re2_zero_width_matches_skip_the_mid_surrogate_position`
- **Rationale** — BACKLOG. `regexp_count` walks UTF-16 code units and is already right;
  `collect_matches` walks Unicode scalars, because a mid-surrogate offset is **not a byte boundary**
  and Rust's `&str` cannot address one — there is no `regex::Match` to build there. Closing this
  means running the collector in UTF-16 space and mapping back, which is a restructure of a hot
  path, not an edge-case patch. The row exists so the number 4 is a known, measured difference
  rather than an assumption that the two functions agree.

### LOG-1 — SQL-door `log` is base 10, Spark's is natural

- **repark** — on the **Spark facade's SQL door** (`SparkSession.sql`; the native ANSI door is a
  separate contract per [ADR-0002](adr/0002-two-sql-doors.md) and is not in this row). The facade's
  `F.log(8.0)` is right at `2.0794415416798357` — it lowers straight to `ln`, bypassing the registry
  entirely — while `SELECT log(8)` returns `0.9030899869919434`, DataFusion's base-10 `LogFunc`.
  **Corrected 2026-08-21:** this row first said "`log(2, 8)` gives `3.0` on both, so only the
  one-argument form diverges". That is **false**. The two-argument form agrees only on positive
  operands; on any non-positive one it diverges too, because DataFusion's `LogFunc` has no
  null-guard — `log(0, 8)` → `-0.0`, `log(-2, 8)` → `NaN`, `log(10, 0)` → `-inf`,
  `log(10, -1)` → `NaN`, and the one-argument `log(0)` → `-inf`, `log(-1)` → `NaN`.
- **Apache Spark** — `log(8)` → `2.0794415416798357`, `log(2, 8)` → `3.0`, and **NULL** for every
  one of the six non-positive cases above (`Logarithm.nullSafeEval`). `log(1, 8)` → `inf` on both.
  *(oracle: live — PySpark 4.1.2.)*
- **Pin** — `python/repark/tests/test_lrs4_door_domain.py::test_log1_sql_door_log_is_base_ten`
  and `::test_log1_the_two_argument_form_diverges_on_non_positive_operands`
- **Rationale** — BACKLOG, and it is a **silently wrong answer on a common function**: a query
  that reads `log(x)` through `spark.sql` gets a number that is off by a constant factor and looks
  perfectly plausible. Not closed here because it needs a Spark-semantics `log` kernel registered
  over DataFusion's — a new kernel and a changed answer, which is outside this campaign's
  invariant. The kernel must carry Spark's null-guard at **both** arities; redirecting the
  one-argument form to `ln` and leaving DataFusion's two-argument formula in place would close half
  the row and leave the other half silently open, which is the failure mode the correction above
  records. Found the day the C-012 guard's domain grew from 20 hand-listed names to the session's
  own 341, which is the argument for that change on its own. **Scope for closing it** (the kernel
  shape, the ratchet move it forces, and the adjacent missing `F.log` overload) is
  [task/sem-0-charter-ledger.md](../task/sem-0-charter-ledger.md), SEM-2 — queued, gate held.

### UNIX-1 — SQL-door `from_unixtime` returns TIMESTAMP, not STRING

- **repark** — the **facade** returns a STRING (`'1970-01-01 00:00:00'`); the **SQL door** returns
  a TIMESTAMP value for the same call.
- **Apache Spark** — returns a STRING: `SELECT from_unixtime(0)` has schema `struct<r:string>`.
  *(oracle: live — PySpark 4.1.2. Its value there is `'1969-12-31 19:00:00'` because the oracle's
  session zone is not UTC; repark's default zone is UTC by registry row
  [TZ-2](#tz-2--the-session-timezone-default-is-utc), so the instant is the same and the rendering
  differs by that already-declared row, not by this one.)*
- **Pin** — `python/repark/tests/test_lrs4_door_domain.py::test_unix1_sql_door_from_unixtime_is_a_timestamp`
- **Rationale** — BACKLOG. The **type** is the divergence, and it is the facade that matches Spark.
  A consumer that writes `SELECT from_unixtime(t)` to Parquet gets a timestamp column where Spark
  would have written a string. Not closed here for the same reason as `LOG-1`: it changes what a
  working query returns.

### Surfaced, awaiting pins — not yet rows

Four candidates surfaced by the session-timezone unit still carry **no pin yet**, so under §6 they are
not admitted as rows; they are queued here so the surfacing is on the record, and each becomes a
row in the change that lands its pin (the unit ledger `docs/history/hardening-h1/h1a-ledger.md` §6 carries the full
observed behavior for each). **B-TZ-4 left this queue as a dated FIXED note (V-3 / #90).**

- **B-TZ-1** — `unix_timestamp` is not a Spark-door SQL function (the facade `F.unix_timestamp`
  exists; the SQL spelling does not plan).
- **B-TZ-2** — `timestamp_seconds` is not a Spark-door SQL function (same shape as B-TZ-1).
- **B-TZ-3** — `date_add(DATE, <integer literal>)` fails to coerce in the SQL door
  (`date_add(Date32, Int64)` refuses; the DataFrame spelling works).
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
