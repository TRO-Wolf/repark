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
`ALTER TABLE t CREATE [OR REPLACE] | REPLACE | DROP BRANCH|TAG b [AS OF VERSION n] [RETAIN n
<unit>] [WITH SNAPSHOT RETENTION m SNAPSHOTS [k <unit>]]` and the top-level
`CREATE|DROP BRANCH|TAG b IN t` forms. Reads reach a ref through
`VERSION AS OF '<ref>'` on both doors and, on the Spark door, through the dotted selectors
`cat.ns.t.branch_<name>` / `cat.ns.t.tag_<name>` (REF-4).

> **Both `WITH SNAPSHOT RETENTION` halves are taken** since REF (2026-09-01): the snapshot count
> and the optional max-snapshot-age that follows it. Measured on live Spark 4.1.2 + Iceberg
> 1.11.0: `CREATE BRANCH b RETAIN 5 DAYS WITH SNAPSHOT RETENTION 3 SNAPSHOTS 7 DAYS` writes
> `max_reference_age_in_ms=432000000`, `min_snapshots_to_keep=3`,
> `max_snapshot_age_in_ms=604800000`. The reversed order is a **Spark parse error**
> (`mismatched input '3' expecting <EOF>`) and refuses on both doors. `WITH SNAPSHOT RETENTION`
> on a `TAG` is also a Spark parse error, and both doors refuse it. This is not a divergence
> row: it is the supported grammar, and the pins are
> `crates/repark-spark/src/tests/refs_and_wap.rs::branch_snapshot_retention_takes_both_count_and_age_halves`
> (+ its reversed-order sibling), `crates/repark-sql/src/ref_ddl/tests.rs::parses_both_snapshot_retention_halves`,
> and `python/repark/tests/test_ref_branch_tag_wap.py`.
> *(Incidental control, same measurement: Spark 4.1.2 **parse-fails** the top-level
> `CREATE BRANCH b IN t` spelling that this door accepts. RePark is a superset there, not a
> divergence, so no row — but do not cite that form as Spark-equal.)*

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

#### REF-1 — writing to a branch or tag — **FIXED 2026-09-01, RP-5**

- **repark** — `INSERT` / `UPDATE` / `DELETE` / `MERGE` / `INSERT OVERWRITE` whose **write
  target** is `t.branch_<name>` commit onto that branch, parented off the branch head, and
  leave `main` unmoved. A write naming a **tag** refuses Spark-shaped (`Cannot write to table
  with time travel` / `Cannot modify table with time travel` for UPDATE/DELETE/MERGE). A write
  naming a missing branch refuses `Cannot use branch (does not exist): <name>` and does not
  create the branch (Spark 4.1.2 + Iceberg 1.11.0, 2026-09-01). Fork-executed families
  (`INSERT`/`UPDATE`/`DELETE`) use `IcebergTableProvider::with_commit_branch`; RePark-owned
  families (`MERGE`/`INSERT OVERWRITE`/`TRUNCATE`) pass `.to_branch` and scan the branch head.
- **Apache Spark** — the Iceberg extension writes to the named **branch**: `INSERT INTO
  t.branch_b`, `UPDATE`/`DELETE`/`MERGE`/`INSERT OVERWRITE` on the same name commit onto `b`,
  parented off `b`'s head, and leave `main` unmoved. A write naming a **tag** refuses —
  `IllegalArgumentException: Cannot write to table with time travel` (`Cannot modify table with
  time travel` for `UPDATE`). A missing branch refuses at analysis for every family:
  `ValidationException: Cannot use branch (does not exist): nope`. *(oracle: live PySpark 4.1.2
  + Iceberg 1.11.0, 2026-09-01, `<pyspark-4.1.2-oracle>`.)*
- **Pin** — `crates/repark-spark/src/tests/write_to_branch.rs` (per family on a diverged branch;
  tag refuse; missing-branch refuse);
  `python/repark/tests/test_ref_branch_tag_wap.py`
- **Rationale** — FIXED (2026-09-01, RP-5). `df.writeTo("cat.ns.t.branch_b").append()` is not
  plumbed unless `writeTo` already funnels into this SQL path; BACKLOG if still missing.
  pins: rp-5-fork-repin/C-004

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

#### REF-3 — write-audit-publish (WAP)

- **repark** — there is no WAP surface, and every door into one is fail-closed. The publish
  procedures `CALL <cat>.system.fast_forward`, `publish_changes` and `cherrypick_snapshot` refuse
  loud, listing the seven procedures that do exist. The `spark.wap.branch` and `spark.wap.id`
  session confs cannot be set at all (`Invalid or Unsupported Configuration: Could not find
  config namespace "spark"`), so no write is silently redirected: a refused conf leaves both
  `main` and the branch where they were.
- **Apache Spark** — the full flow works. With `write.wap.enabled=true` on the table and
  `spark.wap.branch` set, a plain `INSERT INTO t` stages onto that branch and a plain `SELECT`
  in the same session reads the branch, while `main` stays put until publish;
  `CALL sys.fast_forward(table, branch, to)` returns `(branch_updated, previous_ref,
  updated_ref)` and moves `main`. `spark.wap.id` instead stamps `wap.id` into the snapshot
  summary and leaves the snapshot in the log unreferenced by `main`, which
  `CALL sys.publish_changes(table, wap_id)` then cherry-picks;
  `CALL sys.cherrypick_snapshot(table, snapshot_id)` replays a branch-only snapshot onto `main`.
  *(oracle: live PySpark 4.1.2 + Iceberg 1.11.0, 2026-09-01. Incidental control from the same
  run: with `spark.wap.branch` set but `write.wap.enabled` **absent**, Spark silently ignored the
  conf and wrote to `main`.)*
- **Pin** — `crates/repark-spark/src/tests/refs_and_wap.rs::wap_publish_procedures_and_session_conf_refuse_loud`;
  facade rows in `python/repark/tests/test_ref_branch_tag_wap.py`
- **Rationale** — BACKLOG (2026-09-01, RP-5). REF-1 is FIXED, so the "declared while REF-1 holds"
  reason is gone. The remaining gap is the engine's missing publish procedures and `spark.wap.*`
  session confs. Fork primitives exist: `ManageSnapshots::fast_forward`,
  `Transaction::cherry_pick` (`GAP_MATRIX` R98). Do not half-build WAP: a staged write that
  quietly lands on `main` is the failure mode this row keeps impossible.
  pins: ref-branch-tag-wap/C-005

#### REF-4 — reading a ref through the dotted selector — **FIXED 2026-09-01**

**The boundary is read-vs-write, not query-vs-DML.** `cat.ns.t.branch_b` is a ref selector
wherever the statement *reads* it — a standalone `SELECT`, a JOIN, a DML statement's source
relation, a `MERGE`'s `USING` operand, a predicate subquery, or a CTAS body — and all of those
work. A branch write target (`INSERT INTO x`, `INSERT OVERWRITE [TABLE] x`, `UPDATE x`,
`DELETE FROM x`, `MERGE INTO x`) commits onto that branch (REF-1 FIXED). A tag write target
or a missing branch refuses. One statement can do both: `INSERT INTO t.tag_v SELECT …
FROM t.branch_b` refuses on its tag write-target while the branch selector in its source is a
perfectly good read.

- **repark** — the selector reads the ref on the Spark door in every read position above; a
  branch write target commits onto the branch; a missing branch **or** a tag write refuses
  naming it. The ANSI door's spelling for the same read is
  `FOR VERSION AS OF '<ref>'`, which is delivered and pinned — the dotted selector is
  Spark-dialect sugar and stays Spark-only, like `t.snapshots` (§2.1) whose ANSI spelling is the
  fork's `t$snapshots`. On that door a dotted selector still answers DataFusion's generic 4-part
  planning error; its write guard is target-scoped and never claimed a read-side hit.
- **Apache Spark** — the same rows in every one of those positions, `MERGE … USING` and CTAS
  included; a missing ref raises
  `ValidationException: Cannot use branch (does not exist): nope`.
  *(oracle: live PySpark 4.1.2 + Iceberg 1.11.0, 2026-09-01.)*
- **Pin** — `crates/repark-spark/src/tests/refs_and_wap.rs::branch_and_tag_read_selectors_resolve_the_ref`,
  `…::ref_selector_on_the_read_side_of_dml_is_a_read`,
  `…::write_to_branch_refusal_claims_the_target_only`, and
  `…::ref_selector_does_not_claim_metadata_tables_or_real_table_names`;
  `python/repark/tests/test_ref_branch_tag_wap.py`
- **Rationale** — the row is kept after the fix because the boundary above is the interesting
  part. Before REF the selector answered DataFusion's opaque `Unsupported compound identifier …
  Expected 1, 2 or 3 parts, got 4`, which named neither the ref nor the door; worse, a selector
  on a DML statement's read side tripped the write-to-branch sniff and refused with a message
  that claimed a write target the statement did not have. A factually false refusal is worse than
  an opaque one, so the sniff now locates the target from the statement's own head keywords and
  examines only that. The read rewrite claims a four-or-more-part name whose last segment carries
  the `branch_`/`tag_` prefix and is not a metadata-table name, so a table literally named
  `branch_exp` still resolves as itself, and a selector overlapping an `AS OF` clause is left
  alone because Spark rejects that combination.
  pins: ref-branch-tag-wap/C-002, C-007

### 2.3 DML statement forms

#### DML-1 — `INSERT OVERWRITE … PARTITION (…)`

- **repark** — **FIXED 2026-08-30 (DML-B).** Identity-field static `PARTITION (k=v, …)` commits
  through `OverwriteFiles.overwrite_by_row_filter` (sibling partitions stay; nonempty stamps
  `overwrite`, empty stamps `delete`). Dynamic `PARTITION (k, …)` and
  `writeTo().overwritePartitions()` commit through `ReplacePartitions` (source partitions
  only; `replace-partitions=true`). Empty dynamic input refuses loud. The three empty-dynamic
  surfaces are distinct: Spark SQL default-STATIC empty `PARTITION (k)` wipes the table;
  Spark `writeTo().overwritePartitions()` empty is a no-op; RePark `PARTITION (k)` empty
  refuses. Whole-table `INSERT OVERWRITE` (no `PARTITION` clause) is unchanged.
  Transform-field static `PARTITION (id = 1)` on a bucket table still refuses (PIN O5).
  Mixed static/dynamic `PARTITION (p1=1, p2)` refuses. ANSI whole-table `INSERT OVERWRITE`
  stays Q9-omitted; PARTITION forms run on both SQL doors.
- **Apache Spark** — static `PARTITION (k=v)` is `OverwriteByExpression` (sibling files stay;
  empty stamps `delete`; Hive injects the partition columns). `writeTo().overwritePartitions()`
  and `spark.sql.sources.partitionOverwriteMode=dynamic` are `OverwritePartitionsDynamic`
  (`replace-partitions=true`). Empty `writeTo().overwritePartitions()` is a no-op. Default
  STATIC empty `INSERT OVERWRITE t PARTITION (k)` (names, no values) wipes the table.
  *(oracle: live PySpark 4.1.2 + Iceberg 1.11.0, 2026-08-30.)*
- **Pin** — `crates/repark-spark/src/tests/insert_overwrite.rs::empty_insert_overwrite_partition_drops_only_named_partition`
  and `crates/repark-spark/src/tests/insert_overwrite.rs::insert_overwrite_partition_nonempty_replaces_only_named_partition`
  (flipped to partition-scoped success); `crates/repark-spark/src/tests/partition_overwrite.rs`;
  `crates/repark-sql/src/partition_overwrite.rs`;
  `crates/repark-iceberg/src/write/partition_overwrite.rs::commit_rejects_added_file_outside_overwrite_filter`;
  `python/repark/tests/test_dml_b_partition_overwrite.py`;
  `python/repark/tests/test_writer_v2.py::test_write_to_overwrite_partitions_replaces_source_partitions_only`;
  PIN O5 remains the transform-static refuse.
- **Rationale** — identity static/dynamic closed. Remaining DECLARED residue: transform-field
  static assignments; mixed static/dynamic PARTITION lists; Spark default-STATIC
  `PARTITION (k)` (names) full-table wipe (repark always takes the dynamic path, matching
  `writeTo` / `partitionOverwriteMode=dynamic`). Empty-dynamic loud refuse is stricter than
  Spark writeTo no-op and safer than Spark SQL STATIC wipe. `partitionOverwriteMode=dynamic`
  on a PARTITION-less `INSERT OVERWRITE` stays out of this unit.

#### DML-2 — `TRUNCATE TABLE`

- **repark** — **FIXED (2026-08-30, DML-C).** Whole-table `TRUNCATE TABLE` is a first-class
  statement on both SQL doors and the facade. It commits a delete-only overwrite (`AlwaysTrue`
  filter, no added files). Live PySpark 4.1.2 + Iceberg 1.11.0 stamps `summary.operation =
  delete`, snapshot count +1, zero live data files; time travel to the prior snapshot still
  reads the old rows. Empty `INSERT OVERWRITE … WHERE false` remains its own statement and
  stamps the same operation class. `TRUNCATE TABLE … PARTITION (…)` refuses loud.
- **Apache Spark** — same snapshot shape (measured 2026-08-30). Error classes:
  `TABLE_OR_VIEW_NOT_FOUND`, `EXPECT_TABLE_NOT_VIEW.NO_ALTERNATIVE`,
  `INVALID_PARTITION_OPERATION.PARTITION_MANAGEMENT_IS_UNSUPPORTED`.
- **Pin** — `crates/repark-spark/src/tests/truncate.rs::truncate_table_wipes_rows_stamps_delete_and_preserves_history`
  (Spark door); `crates/repark-sql/src/truncate_tests.rs::truncate_table_wipes_rows_stamps_delete_and_preserves_history`
  (ANSI door); `python/repark/tests/test_dml_c_truncate.py` (facade).
- **Rationale** — Iceberg has no separate truncate action; Spark's statement is delete-only
  overwrite. The product spelling is `TRUNCATE TABLE`, not a documented empty-overwrite
  substitute. pins: dml-c-truncate/C-001, C-002, C-003, C-004, C-005, C-006, C-007, C-008

#### DML-3 — `MERGE INTO` forms outside the supported surface

- **repark** — a `MERGE INTO` shape the door cannot parse refuses with a targeted error pointing
  at this section, rather than surfacing a raw parser error. **DML-A (2026-08-30):**
  `WHEN NOT MATCHED BY SOURCE` DELETE and UPDATE are in the supported surface (COW and MOR;
  `UPDATE SET *` on that arm is Spark-parse-fail and stays refused).
- **Apache Spark** — parses the full `MERGE INTO` grammar. *(oracle: documented.)*
- **Pin** — `crates/repark-spark/src/tests/merge.rs::merge_unparsable_form_gets_targeted_error`;
  NMBS execute: `crates/repark-spark/src/tests/merge_nmbs.rs`
  (pins: dml-a-merge-not-matched-by-source/C-002, C-003, C-008)
- **Rationale** — DECLARED as a *boundary*, not as a permanent gap: `MERGE` is RePark-owned and
  its supported surface grows. The row exists so the boundary is a stated, tested thing instead
  of an accident of the parser.
- **Where the boundary moves next.** Remaining refuse-forms (Oracle sub-predicates, `INSERT ROW`,
  `OUTPUT`/`RETURNING`, NMBS `UPDATE SET *`) stay targeted refusals. The door parses Spark SQL
  with stock sqlparser's `DatabricksDialect` plus token-level normalizers. Schedule lives in
  [../STATUS.md](../STATUS.md), never in this registry.

#### DML-4 — insert-only `MERGE` snapshot operation stamp

- **repark** — an insert-only `MERGE` (no `WHEN MATCHED` arm) commits with operation `append`
  under copy-on-write and `overwrite` under merge-on-read, inherited from the Java-faithful
  commit classification (`commit_overwrite` vs the row-delta path).
- **Apache Spark** — the same operation classes for the same modes — but a table whose
  `write.merge.mode` flips changes which snapshots an `IncrementalAppendScan` (CDC-style
  consumer) sees, so the stamp is a visibility contract worth declaring. *(oracle: documented —
  audit M20.)*
- **Pin** — `crates/repark-iceberg/src/write/merge/tests/occ_conflict.rs`
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
- **Pin** — `crates/repark-iceberg/src/write/merge/tests/occ_conflict.rs`
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

#### ENC-1 — Iceberg table encryption keys are stored, never applied

- **repark** — `CREATE TABLE … TBLPROPERTIES ('encryption.key-id' = …)` on a format-v3 table
  succeeds. The property is stored. `INSERT` writes ordinary Parquet; `SELECT` returns the
  rows; table-metadata `encryption-keys` stays empty. There is no KMS client and no file
  encryption. Owner ruling 2026-08-24: dated DECLARED exclusion from the v1.0 gate.
- **Apache Spark** — with a configured Iceberg KMS, `encryption.key-id` encrypts data,
  delete, manifest, and manifest-list files (Iceberg table-encryption docs). Without a KMS
  the Spark session fails to write. *(oracle: documented — Iceberg table property
  `encryption.key-id`; this engine never talks to a KMS, so there is no value oracle.)*
- **Pin** —
  `crates/repark-spark/src/tests/v3_cow.rs::v3_create_with_encryption_key_id_still_scans_without_a_kms`
- **Rationale** — DECLARED exclusion, owner-dated 2026-08-24. Implementing envelope encryption
  is fork work (GAP_MATRIX R130) and is not on the v1.0 slate. The pin holds the honest
  current behavior so a later encryption landing reds it on purpose.

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
> **F-Y10-1 — integer arithmetic overflow raises where Spark raises — FIXED (2026-08-30).**
> Checked integer `+` / `-` / `*` (`crates/repark-functions/src/integer_spark.rs`) read
> `spark.sql.ansi.enabled` (default TRUE, DEC U5 shape). `CAST(2147483647 AS INT) + CAST(1 AS INT)`
> and `CAST(INT) + 1` raise `[ARITHMETIC_OVERFLOW]` under ANSI; `ansi=false` wraps at Int32
> `-2147483648`. BIGINT is the same with `long overflow`. The ANSI door raises per standard SQL
> (owner Option A). Spark `ansi=false` wrap vs ANSI raise is an INTENDED pin
> (`cross_door_int32_add_overflow_wraps_on_spark_ansi_false_raises_on_ansi`). Pins:
> `crates/repark-functions/src/integer_spark.rs` tests; `ansi_door_int32_add_overflow_raises`;
> `python/repark/tests/test_integer_overflow_parity.py`. G13's integer half is closed.
> Residue of that campaign body: G5b-R3-ANSI (window RANGE wrap), F-Y10-2, and
> SMALLINT/Int16 overflow (2026-08-30: `CAST(32767 AS SMALLINT) + CAST(1 AS SMALLINT)`
> still Arrow-wraps to Int16 `-32768` under default ANSI; charter partition was int32/int64).
> **Lambda bodies are unarmed (2026-08-31, FNP-4c interaction):** an operand containing a
> higher-order lambda variable keeps its provisional type, so `+`/`-`/`*` inside lambda
> bodies stay on DataFusion coercion (no overflow raise there yet — arming desynchronized
> the declared `LambdaVariable` field from the re-derived merge type). Pin:
> `lambda_variable_operands_do_not_arm`.
>
> **F-Y10-2 — routed, not invented as a DEC row (2026-08-13, Z-5).** ANSI float `/ 0` is IEEE
> `+Inf` rather than a standard-SQL raise. Residual. The door-vs-door Inf-vs-NULL split is
> already an INTENDED pin (`cross_door_float_div_by_zero_is_infinity_on_ansi_null_on_spark`).
> It is not DEC-7 (decimal `/0`, since FIXED — #99).

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

### V3-GEO-1 — the v3 `geometry` / `geography` types are not supported

- **repark** — no engine surface reaches either type. `CREATE TABLE … (g GEOMETRY)` (and
  `GEOGRAPHY`) refuses on both SQL doors at the column-type mapping, naming the type, and
  leaves no table behind; there is no fixture to measure a read and none is planned for v1.0.
  **Owner ruling 2026-08-25: dated DECLARED exclusion from the v1.0 gate** (north star §3,
  the types row).
- **Apache Spark** — the ratified v3 spec defines both types and Iceberg-Java models them
  (fork `GAP_MATRIX` row R89 tracks the fork-side gap); this engine never reaches a value, so
  there is no value oracle. *(oracle: documented — the v3 spec's type table; no live
  scenario.)*
- **Pin** —
  `crates/repark-spark/src/tests/create_table.rs::v3_type_columns_geometry_geography_variant_refuse_naming_the_type`
  (ANSI twin of the same name in `crates/repark-sql/src/v3/types.rs`; facade
  `python/repark/tests/test_v3_create_opt_in.py::test_v3_geometry_geography_variant_columns_refuse_naming_the_type`)
- **Rationale** — DECLARED, owner-dated 2026-08-25. Spatial types are fork work (F-15 → R89)
  with no consumer on the v1.0 path; the ruling keeps the gate honest instead of silent.
  Reversing it needs a new dated decision, and the landing reds the pin on purpose. `variant`
  is **not** this row: it stays V3-6 work — the same pin holds its CREATE refusal today so
  V3-6's landing reds it — with **shredded**-Parquet variant DECLARED out of the v1.0 gate by
  the same ruling (row below).

### V3-VARIANT-SHRED-1 — shredded-Parquet `variant` is out of the v1.0 gate; binary `variant` refuses end to end

- **repark** — a `variant` column refuses at CREATE on all three doors (`V3-GEO-1`'s pin),
  so no engine surface reaches a variant value. At the fork pin `33be9a0` the **binary**
  (unshredded) type is measured: schema-level it maps to Arrow
  `struct<metadata: Binary, value: Binary>`, parquet file write refuses
  (`FeatureUnsupported`, naming `variant`), and scanning a file that projects the column
  refuses (`reject_variant_projection`, naming `variant`). The scan-refusal pin's fixture
  parquet carries only an `id` column — the fork cannot write variant bytes — which is
  honest here because the guard is per-file-task projection and fires before any file
  bytes are read; the empty-table control streams cleanly, which is why the fixture is
  non-empty. **Shredded**-Parquet variant has
  no fork surface at all. Fork gap filed against `GAP_MATRIX` R88 (file-level variant I/O).
- **Apache Spark** — Spark 4.1.2 + Iceberg 1.11.0 writes the **unshredded** two-binary
  variant shape (`struct<value: binary, metadata: binary>` with `variant: 'true'` field
  metadata) and reads it back; it cannot produce `timestamp_ns`-style SQL spellings for
  `variant` either — the column arrives via `parse_json` / cast. *(oracle: live probe,
  V3-6 C-001 matrix, 2026-08-31.)*
- **Pin** —
  `crates/repark-iceberg/src/tests/v3_types.rs::fork_variant_arrow_maps_and_parquet_write_refuses`
  and `fork_variant_scan_refuses_naming_the_type` (the binary-vs-shredded distinction: the
  fork models and Arrow-maps the binary type while refusing its file I/O; shredded has no
  model at all). CREATE refusals stay on `V3-GEO-1`'s pins.
- **Rationale** — DECLARED, owner-dated 2026-08-25 (same ruling as `V3-GEO-1`). Shredded
  variant is fork work (F-15 → R88) with no v1.0 consumer; binary variant consumption is
  queued fork work, not an engine invention. Reversing needs a new dated decision; the fork
  I/O landing reds the scan/write pins on purpose.

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
> and TZ-8 (`to_date` / `CAST(ts AS DATE)` / `datediff` since FIXED — #100, session-zone; only
> `last_day` / `date_add` over a TIMESTAMP remain). A reader who
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

### TZ-8 — `last_day` / `date_add` over a TIMESTAMP refuse to plan

- **repark** — `last_day` and `date_add` over a TIMESTAMP do not plan at all
  (`coercion from Timestamp(ns) … failed` / `No function matches`). The `CAST(ts AS DATE)` /
  `to_date` / `datediff` half of this row is **FIXED** — see the dated note below; those read the
  session zone now.
- **Apache Spark** — `last_day` / `date_add` accept a TIMESTAMP and resolve its date in
  `spark.sql.session.timeZone`: under `America/New_York` the instant `2024-06-15T03:00:00Z`
  (23:00 EDT on the 14th) answers `last_day(ts) → 2024-06-30` and `date_add(ts, 1) → 2024-06-15`.
  *(oracle: recorded — live PySpark 4.1.2, 2026-08-10; V-3 recapture 2026-08-13.)*
- **Pin** — `crates/repark-spark/tests/session_timezone.rs::last_day_and_date_add_over_a_timestamp_still_refuse`
  (red-on-purpose: the recorded NY Spark values above are named in it, so adding the overload reds
  the pin instead of passing unnoticed).
- **Rationale** — BACKLOG, intent to FIX. The residual is the TIMESTAMP overload of `last_day` /
  `date_add`; registry queue **B-TZ-3** (`date_add(DATE, int literal)` coercion) is
  the `DATE`-argument sibling of the same hole. Engine work — a later unit. **Not a regression**:
  these behaved the same before H-1a split B; the completeness gap is what the class claim would
  otherwise paper over.

> **TZ-8 `CAST(ts AS DATE)` / `to_date` / `datediff` — FIXED (2026-08-14, #100).**
> `CAST(ts AS DATE)`, `to_date(ts)` and `datediff(ts, date)` now take the date in
> `spark.sql.session.timeZone` through the analyzer rewrite `rewrite_timestamp_to_date_cast`
> (`crates/repark-functions/src/analyzer.rs`; `datediff` rides the same CAST via
> `SparkDateDiff::simplify`). Under `America/New_York`, `2024-06-15T03:00:00Z` answers `2024-06-14`
> (CAST and `to_date`) and `datediff(ts, DATE '2024-06-01')` answers `13` — Spark's values, not the
> stored UTC date; a UTC control keeps `2024-06-15` and a Tokyo instant crosses forward. As an
> identity partition key the same class now writes the session-zone date (`2023-12-31` for
> `2024-01-01T04:30Z`, was `2024-01-01`). Pins:
> `…session_timezone.rs::timestamp_to_date_paths_read_the_session_zone`,
> `…::native_dataframe_api_cast_to_date_reads_the_session_zone`,
> `…::date_valued_shims_take_the_date_in_the_session_zone` (the `trunc` / `add_months` shims that
> share the kernel);
> `python/repark/tests/test_partition_value_audit.py::test_partition_value_row[tz8_cast_ts_as_date_identity_new_york_ctas]`
> + `…[tz8_to_date_ts_identity_new_york_ctas]` (flipped to equality — flip evidence). A fixed
> defect gets this dated note, never a live divergence row.

> **G13 integer overflow (F-Y10-1) — FIXED 2026-08-30.** The integer half of gap G13
> (`INT`/`BIGINT` `+` `-` `*` at the boundary) now raises under ANSI and wraps when
> `ansi=false`. Residue named on the F-Y10-1 FIXED note above: G5b-R3-ANSI window RANGE wrap,
> F-Y10-2 float `/0`, DEC-9 nullability, and SMALLINT/Int16 wrap (2026-08-30).
>
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

> **DEC-2 — `DECIMAL / DECIMAL` result precision and scale — FIXED (2026-08-14, V-2 U4b / #99).**
> `/` now takes Spark's `resultDecimalType` through a `repark-functions` UDF
> (`crates/repark-functions/src/decimal_spark.rs`; `SparkDecimalRewrite` runs before
> `SparkExprSemantics` so the rewrite sees a clean `decimal / decimal`) — a CAST-after had wronged
> the *value*. `(10,2)/(10,2)` → `decimal128(23,13)` `0.2697368421053`; repeating money
> `(10.00)/(3.00)` stays `(23,13)`; integer-scale `1/3` lands `(21,11)` `0.33333333333`; exact
> half `5.00/2.00` is `(23,13)` `2.5000000000000` — repark now equals Spark on all four (was
> `(16,6)` `0.269736` / `3.333333` / `(14,4)` `0.3333` / `(16,6)` `2.5`). The parametrized names
> `[div_same_precision_scale]`, `[div_repeating_money]`, `[div_integer_scales]`,
> `[div_exact_half_type_only]` are kept so citations still resolve; the rows are now content
> equalities (`repark is None`). Pins: those corpus cases; Rust
> `crates/repark-spark/src/tests/decimal.rs::pin_div_same_precision_scale_repark_i128`. `%`
> (`resultDecimalType` for modulo) stays closed — UNPROBED, a later unit. A fixed defect gets this
> dated note, never a live divergence row.

> **DEC-3 — the 38-digit result-type clamp on multiply and add — FIXED (2026-08-13, V-2 U4a / #91).**
> Spark `adjustPrecisionScale` (`allowPrecisionLoss=true`) via `SparkDecimalPrecision`
> CAST-after: `(38,10)*(38,10)` → `decimal128(38,6)` `1.000000`; `(38,18)+(38,18)` →
> `(38,17)` `2.00000000000000000`; `(38,10)+(38,10)` → `(38,9)` `2.000000000`. The
> parametrized names `[mul_38_10_clamps_scale_in_spark]`, `[add_38_18_clamps_scale_in_spark]`,
> `[add_38_10_clamps_scale_in_spark]` are kept so the name-gated family pin still
> resolves; the rows are now content equalities (`repark is None`). Pins: those corpus
> cases; Rust `pin_mul_38_10_clamps_to_38_6_i128` + `pin_add_38_18_clamps_to_38_17_i128`;
> name-gated budget `test_decimal128_row_set_covers_gap_budgets` (≥ 3 `*clamps_scale_in_spark`).
> `/` has since landed (U4b / #99 — see DEC-2's note). Registry DEC-8 (plan-refuse at
> `BinaryExpr::get_type`) is a **different altitude** and has since landed too (#99 — see its
> note). A fixed defect
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

> **DEC-6 — max `DECIMAL(38,0) + 1` under ANSI raises — FIXED (2026-08-14, DEC U5 / #99 on the
> ANSI door #94).** A checked `+` / `-` UDF (`crates/repark-functions/src/decimal_spark.rs`) reads
> the landed ANSI knob (`spark.sql.ansi.enabled`, default TRUE since U5 / #94):
> `CAST(999…9 AS DECIMAL(38,0)) + CAST(1 AS DECIMAL(38,0))` now raises
> `NUMERIC_VALUE_OUT_OF_RANGE` / `ArithmeticException` — the prior `10^38` wrap is gone;
> `ansi=false` yields NULL at `decimal128(38,0)`. The names
> `[overflow_max_decimal38_plus_one_raises_in_spark]` (a shared-raise equality) and
> `[overflow_max_decimal38_plus_one_null_when_ansi_false]` are kept. Pins: those corpus cases; Rust
> `crates/repark-spark/src/tests/decimal.rs::pin_overflow_max_decimal38_plus_one_wrong_value_i128`
> (name kept; now asserts the raise) + `…::pin_overflow_max_decimal38_plus_one_null_when_ansi_false`.
> A fixed defect gets this dated note, never a live divergence row.

> **DEC-7 — `DECIMAL / 0` under ANSI raises — FIXED (2026-08-14, U5 / #94 + U4b / #99).** Default
> ANSI ON (#94) plus the U4b division UDF that owns `/0` (#99): `(38,0)/(38,0)` and small
> `(2,0)/(2,0)` now raise `DIVIDE_BY_ZERO` / `ArithmeticException` on both engines; `ansi=false`
> restores NULL at Spark's division type (`decimal128(38,6)` / `(8,6)`). The names
> `[div_by_zero_decimal38_raises_in_spark_null_in_repark]`,
> `[div_by_zero_small_decimal_raises_in_spark_null_in_repark]` (shared-raise equalities) and the two
> `…_null_when_ansi_false` twins are kept. Pins: those corpus cases; Rust
> `crates/repark-spark/src/tests/decimal.rs::pin_div_by_zero_decimal38_raises_under_default_ansi` +
> `…::pin_div_by_zero_decimal38_returns_null_at_38_4_when_ansi_false`. A fixed defect gets this
> dated note, never a live divergence row.

> **DEC-8 — `DECIMAL(38,20) * DECIMAL(38,20)` — FIXED (2026-08-14, DEC-8 planner / #99).** An
> `ExprPlanner` (`plan_binary_op`) rewrites the Arrow-refusing `(38,20)*(38,20)` — which used to
> fail with `AnalysisException` at `BinaryExpr::get_type` before any `AnalyzerRule` ran — into
> Spark's default-true clamp `decimal128(38,6)` `1.000000`, which now succeeds. The name
> `[mul_38_20_plans_in_spark_refuses_in_repark]` is kept; the row is now a content equality
> (`repark is None`). Pins: that corpus case; Rust
> `crates/repark-spark/src/tests/decimal.rs::pin_mul_38_20_still_refuses_at_plan` (name kept; now
> asserts the clamp) +
> `repark_functions::decimal_precision::tests::mul_38_20_plans_via_the_expr_planner`. Not campaign
> DEC-8 (U3 integer-literal min-precision — that closed DEC-5 **width**). A fixed defect gets this
> dated note, never a live divergence row.

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
> [`task/z5-landing-increment-ledger.md`](../task/ledgers/archive/2026-08/2026-08-13-z5-landing-increment-ledger.md).
>
> **The 2026-08-13 Z-wave increment (W-5)** pasted the merged Z-wave §6 handoffs after
> re-verifying each against frozen `c7e6589` (PRs #75–#79). Classification:
> [`task/w5-z-landing-ledger.md`](../task/ledgers/archive/2026-08/2026-08-13-w5-z-landing-ledger.md). TZ-6 / TZ-7 sections
> were not touched (PR-2 owns those two headings). No new `live-mirror:` tokens.
>
> **The 2026-08-13 W-wave increment (V-5)** pasted the merged W-wave §6 handoffs after
> re-verifying each against frozen `8d325d4` (PRs #81–#85). Classification:
> [`task/v5-w-landing-ledger.md`](../task/ledgers/archive/2026-08/2026-08-13-v5-w-landing-ledger.md). TZ-6 / TZ-7 FIXED
> notes were already in-file from #85 (not duplicated). No new `live-mirror:` tokens.
>
> **The 2026-08-14 V-wave increment (S-5)** pasted the merged V-wave §6 handoffs after
> re-verifying each against frozen `d9a7391` (PRs #87–#91). Classification:
> [`task/s5-v-landing-ledger.md`](../task/ledgers/archive/2026-08/2026-08-13-s5-v-landing-ledger.md). TZ-6 / TZ-7 FIXED
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

### FN-ISNAN-1 — `isnan(NULL)` is NULL where Spark is false

- **repark** — `isnan(CAST(NULL AS DOUBLE))` on `[1.0, NULL]` yields `[False, None]`
  (nullable `bool`); lowers to DataFusion `isnan`, which null-propagates.
  `crates/repark-python/src/column/function_dispatch.rs:282`.
- **Apache Spark** — `isnan(CAST(NULL AS DOUBLE))` on `[1.0, NULL]` yields
  `[False, False]` (non-nullable `bool`, Spark `IsNaN`); NULL is not NaN.
  *(oracle: live — PySpark 4.1.2, `local[1]`, ANSI on, 2026-09-03.)*
- **Pin** — `python/repark/tests/test_fn_batch1.py::test_isnan_null_divergence_is_pinned`
- **Rationale** — BACKLOG, silently wrong divergence. `isnan` on a nullable
  double is nullability-sensitive; Spark's `IsNaN` is non-nullable. The fix
  flips the pin to `[False, False]`.


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
> remains [G4-3](#g4-3--conditionless-dataframe-semianti-join-refuses). A fixed defect
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
  `subquery predicates are silently mis-executed`) **except**: uncorrelated
  `DELETE … WHERE col IN (SELECT col FROM …)` / `NOT IN (SELECT col FROM …)` (including
  ANY-NULL-in-subquery matches nothing, empty subquery matches all), `DELETE … WHERE
  [NOT] EXISTS (SELECT …)` both uncorrelated (all-or-nothing) and correlated (per-row
  semi/anti-join, including NULL join keys and duplicate rows), **correlated**
  `DELETE … WHERE col IN (SELECT s.col FROM s WHERE s.k = t.k)`, and identity
  **uncorrelated** `UPDATE … SET <scalar> WHERE col IN (SELECT …)`. Those execute on both
  doors via the A1-identity path (`execute_predicate_dml` / `try_allowed_update_in`) and
  match Spark (FROM and FROM-less). `UPDATE NOT IN` / `[NOT] EXISTS`, **correlated** UPDATE
  IN, every `ANY` / `ALL` spelling, scalars, nested, mixed AND/OR remain refused.
  SET-assignment / `INSERT` / `MERGE` source still unaffected.
- **Apache Spark** — runs all of them, deleting/updating exactly the matching rows.
  *(oracle: recorded — PySpark 4.1.2 + Iceberg 1.11.0.)*
- **Pin** — `python/repark/tests/test_dml_subquery_parity.py::test_dml_subquery_row[delete_in_subquery]`,
  `…[delete_not_in_subquery]`, `…[delete_not_in_subquery_with_null_key]`,
  `…[delete_correlated_in_subquery]`, `…[delete_exists_correlated]`,
  `…[delete_not_exists_correlated]`, `…[update_in_subquery]` (all now **content**)
  plus the uncorrelated / none / all / empty / NULL-key / duplicate EXISTS content
  rows and the `update_in_subquery_multi_set` / `_expr` / `_empty` siblings; the one
  residual split is `…[update_not_in_subquery_with_null_key]`. Rust:
  `crates/repark-spark/src/tests/dml.rs::g3e8_delete_in_subquery_deletes_exactly_the_matching_row`
  (and quoted / temp-view / FROM-less siblings);
  `…::g3e8_delete_not_in_subquery_deletes_non_matching_rows`,
  `…::g3e8_delete_not_in_subquery_with_null_key_deletes_nothing`,
  `…::g3e8_delete_not_in_empty_subquery_deletes_every_row`;
  `…::g3e8_delete_exists_uncorrelated_and_correlated_execute`;
  `…::g3e8_delete_correlated_in_deletes_exactly_the_matching_row`;
  `…::g3e8_update_in_subquery_rewrites_only_the_matching_row`;
  `…::g3e8_delete_subquery_family_all_refuse` + `…::g3e8_update_subquery_family_all_refuse`
  (the residual refuse set: `UPDATE NOT IN` / `[NOT] EXISTS`, correlated UPDATE IN, ANY / ALL);
  ANSI `crates/repark-sql/src/guards/tests.rs::dml_subquery_in_delete_executes_and_deletes_exactly_the_match`,
  `…::dml_subquery_not_in_delete_executes_and_honors_three_valued_logic`,
  `…::dml_subquery_exists_delete_executes_uncorrelated_and_correlated`;
  ROW 9 `crates/repark-sql/tests/cross_door.rs::cross_door_g3e8_refusals_render_identically`
  restated over nested / scalar / mixed-AND / `ANY` / `UPDATE NOT IN`;
  `…::cross_door_g3e8_not_in_delete_executes_identically`;
  `…::cross_door_g3e8_exists_delete_executes_identically`.
- **Rationale** — DEFECT, **partial fix**. Uncorrelated DELETE IN / NOT IN, `[NOT] EXISTS`
  ± correlation, correlated DELETE IN, and uncorrelated identity UPDATE IN all execute both
  doors — the dbt-upgrade gate is MET. The family is **not** "fixed" while `UPDATE NOT IN` /
  `[NOT] EXISTS`, correlated UPDATE IN and every `ANY` / `ALL` stay valved (`ANY` / `ALL` are a
  permanent v1 valve — Spark 4.1.2 parse-fails quantified comparisons). Delete this row only when
  the claimed surface is actually re-enabled.

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
  `regexp_extract_all('🎉ab', 'b*', 0)` returns 4 elements. `regexp_count` on the same inputs
  returns **5**, so two functions in this repository disagree.
- **Apache Spark** — **5** in every case (`['','','','','']`, `['','','','b','']`). Java's
  `Matcher` finds an empty match at every UTF-16 code-unit index, including the one *inside* a
  surrogate pair. *(oracle: live — PySpark 4.1.2.)*
- **Narrowed 2026-08-21 (SEM-5).** This row used to also carry `regexp_substr('🎉ab', '')` → `''`
  vs Spark's NULL, which put a **general** difference under a surrogate-shaped heading. Measurement
  shows `regexp_substr` returned `''` for a zero-width match on **plain ASCII** too, so that half
  was not about surrogates at all and moved to its own row, `RE-3` — **which SEM-6 then closed the
  same day**, so it is gone from this registry and `regexp_substr` now returns NULL there. What
  remains here is genuinely surrogate-bound: the count.
- **Pin** — `python/repark/tests/test_lrs6_regexp_divergences.py::test_re2_zero_width_matches_skip_the_mid_surrogate_position`
- **Rationale** — BACKLOG. `regexp_count` walks UTF-16 code units and is already right;
  `collect_matches` walks Unicode scalars, because a mid-surrogate offset is **not a byte boundary**
  and Rust's `&str` cannot address one — there is no `regex::Match` to build there. Closing this
  means running the collector in UTF-16 space and mapping back, which is a restructure of a hot
  path, not an edge-case patch. The row exists so the number 4 is a known, measured difference
  rather than an assumption that the two functions agree.

### LOG-1 — SQL-door `log` is base 10, Spark's is natural

> **FIXED (2026-08-31, SEM-1).** Owner ruling 2026-08-31: fix to Spark semantics. Spark-door
> `log(expr)` is the natural log (`log(8)` → `2.0794415416798357`); `log(base, expr)` is log at
> that base (`log(2, 8)` → `3.0`). Both arities return NULL on zero, negative, and null operands;
> `log(1, 8)` stays `inf`. Native ANSI `repark.sql()` is unchanged (base 10, ADR-0002). `F.log`
> accepts `log(arg1, arg2=None)`. Kernel: `crates/repark-functions/src/spark_log.rs`. Pins:
> `python/repark/tests/test_lrs4_door_domain.py::test_log1_sql_door_log_is_natural`,
> `::test_log1_both_arities_null_on_non_positive_operands`,
> `python/repark/tests/test_sem1_spark_log.py`. Oracle: live PySpark 4.1.2, 2026-08-31.

### ORPHAN-1 — `remove_orphan_files` requires `older_than`; Spark defaults it

- **repark** — `CALL <catalog>.system.remove_orphan_files(table => …)` with no `older_than`
  **refuses** at plan time and names the argument. Nothing is listed and nothing is deleted.
- **Apache Spark** — runs, defaulting `older_than` to `now - 3 days`, and **deletes** the orphans
  it finds. Measured: two planted orphans aged ten days were listed and removed from disk by a
  bare `CALL … remove_orphan_files(table => 't')`.
  *(oracle: live — PySpark 4.1.2 + Iceberg 1.11.0, Hadoop catalog, re-measured 2026-09-02 by
  V3-11: the bare call listed both planted ten-day-old orphans and both were gone from the data
  directory afterwards, the answer the 4.0.1 run recorded;
  the `DataSourceV2Relation` note this row used to carry is retired — see
  [MOR-1](#mor-1--rewrite_position_delete_files-compacts-below-sparks-min-input-files-floor).)*
- **Pin** — `crates/repark-spark/src/tests/call.rs::call_orphan1_requires_an_explicit_older_than`
  and `python/repark/tests/test_maintenance_call.py::test_remove_orphan_files_requires_an_explicit_older_than`
- **Rationale** — DECLARED, and deliberately stricter than Spark (owner decision OD-2). This is
  the only procedure on the surface that destroys data with no rollback: a bad compaction is
  compacted again, deleted files are gone. A defaulted cutoff makes the single most dangerous
  argument the one the caller never typed, and the default is not conservative — three days is
  short enough to catch a long-running write. The refusal costs a migrating job one argument and
  is the cheapest possible place to spend that. **Not to be confused with the 24-hour floor**,
  which repark also enforces and which is *parity* with Spark, not a stricter posture.

### ORPHAN-2 — `remove_orphan_files` defaults to a dry run; Spark defaults to deleting

- **repark** — `dry_run` defaults to **true**. The default call LISTS every orphan and removes
  nothing; deleting requires `dry_run => false` explicitly. `dry_run` accepts a boolean literal
  only — a quoted `'false'` refuses rather than being coerced, so a typo cannot arm the deletion.
- **Apache Spark** — `dry_run` defaults to **false**: the default call deletes. Measured on the
  same fixture — the bare call left three data files where five had been; `dry_run => true`
  returned the same two rows and left all five in place.
  *(oracle: recorded — live PySpark 4.0.1 + Iceberg 1.10.0, same basis as ORPHAN-1.)*
- **Pin** — `crates/repark-spark/src/tests/call.rs::call_remove_orphan_files_dry_run_lists_without_deleting`,
  `::call_remove_orphan_files_armed_deletes_orphans_and_nothing_else`,
  `::call_remove_orphan_files_refuses_a_quoted_dry_run`, and
  `python/repark/tests/test_maintenance_call.py::test_remove_orphan_files_dry_run_is_the_default`
- **Rationale** — DECLARED, deliberately stricter than Spark (owner decision OD-2). The **result
  shape is identical either way** — one row per orphan, `orphan_file_location`, exactly Spark's
  schema — so the dry run is not a second surface bolted on beside the real one; it is Spark's own
  result with the deletion withheld. A caller who reads the listing and re-runs with
  `dry_run => false` gets Spark's behaviour exactly. What changes is which of the two a caller
  gets by typing nothing, and on an unrecoverable operation that default should be the safe one.

### MOR-1 — `rewrite_position_delete_files` compacts below Spark's `min-input-files` floor

> **FIXED 2026-08-23 (RP-1 / fork F-1).** The position-delete planner now shares
> `MIN_INPUT_FILES_DEFAULT = 5` with `RewriteDataFiles`. The pin below flipped from
> `rewritten = 4` to `rewritten = 0` on a 4-file group — that RED-then-GREEN is the
> retirement evidence. This is no longer a divergence.

- **repark** — a `(spec, partition)` group of 4 position-delete files returns all four
  counts as `0` and leaves the files in place.
- **Apache Spark** — the same zeros. Spark's planner extends `SizeBasedFileRewritePlanner`,
  whose `MIN_INPUT_FILES_DEFAULT` is 5. Re-measured 2026-09-02 (V3-11) on the pinned
  PySpark 4.1.2 + Iceberg 1.11.0 oracle: a v2 merge-on-read table with four single-file
  parquet position deletes in one group answered
  `rewritten_delete_files_count = 0, added_delete_files_count = 0, rewritten_bytes_count = 0,
  added_bytes_count = 0` and left all four delete files in place.
  **Re-measured 2026-09-02 (V3-11): the pinned 4.1.2 + 1.11.0 oracle executes all five
  maintenance procedures** — `rewrite_data_files`, `rewrite_manifests`,
  `rewrite_position_delete_files`, `expire_snapshots` and `remove_orphan_files`. The
  4.0.1/1.10.0 `DataSourceV2Relation` note this registry carried on six rows applies nowhere;
  this row is its single home.
- **Pin** —
  `crates/repark-spark/src/tests/call.rs::call_mor1_compacts_below_sparks_min_input_files_floor`
- **Rationale** — retired. The owned fork closed the planner gap; this engine consumed it
  at pin `5e7b2e4`.

### MOR-2 — merge-on-read delete files are partition-granularity, where Spark's default is per file

- **repark** — one `MERGE` touching six distinct data files writes **six** position-delete files
  when the property is unset (Spark's `SparkWriteConf` default is `file`). Explicit
  `'partition'` writes one file per `(spec, partition)` group. Iceberg-core's
  `TableProperties.DELETE_GRANULARITY_DEFAULT` is `partition`; this engine matches Spark, not
  that core default (fork ENGINE_CONTRACT §7).
- **Apache Spark** — `MERGE` and `DELETE` both write **one delete file per data file** with
  the property unset. Confirmed on the oracle: eight `DELETE`s across eight data files
  produced eight delete files; `'partition'` produced one per partition.
  *(oracle: recorded — live PySpark 4.0.1 + Iceberg 1.10.0, same basis as MOR-1.)*
- **Pin** —
  `crates/repark-spark/src/tests/call.rs::call_mor2_merge_writes_one_position_delete_per_data_file_by_default`
  (MERGE writer). Residual: Spark SQL `DELETE`/`UPDATE` that hit the fork `TableProvider`
  still group by partition (`fork_table_provider_delete_is_not_this_writer`).
- **Rationale** — FIXED (MW-9) **for RePark-owned MERGE** (`write_position_deletes`).
  Heading kept as the historical anchor. SQL `DELETE`/`UPDATE` via iceberg-datafusion
  have no granularity knob (fork ENGINE_CONTRACT §7). Contents are unaffected.

### RDF-1 — `rewrite_data_files` never selects a delete-laden file, so its dead rows are retained forever

- **repark** — **FIXED 2026-09-02** for a position-delete file that names ONE data file; the
  heading and the first paragraph below are the dated history this row was opened on, kept as
  the anchor. Read the 2026-09-02 entry for the current claim.
  F-16r (`#248`, pin `00cdde0`) wired `tooHighDeleteRatio`, but the ratio
  clause counts only **file-scoped** position deletes (`referenced_data_file` present, or
  equal file-path bounds). Partition-granularity deletes and bounds-absent position deletes
  are invisible to it (F-16 residue 2). The MW-7 2,500-row pin writes with
  `write.delete.granularity = 'partition'` (`python/repark-parity/bench/mw7/measure.py`), so
  those deletes never raise the ratio and a correctly sized 100 %-dead file stays unselected.
  A data file is otherwise a rewrite candidate only when it is outside the size band or
  carries at least `delete_file_threshold` delete files (`usize::MAX` by default). So a
  **correctly sized** file whose rows are 100 % deleted under partition granularity is
  invisible to compaction. It is kept, its dead rows
  with it, and the position-delete file covering it survives too, **still naming a LIVE data
  file** — it survives because its data file was never selected, not because of the
  `removed_delete_files_count` constant (that counter and fork ask F-3 belong to the other
  half: a delete file whose referent WAS rewritten). Measured on a 2,500-row v2 merge-on-read
  fixture: one 68,523 B data file, inside the band for a 64 KiB target, and one `MERGE` deleting
  all 2,500 of its rows. After the COMPLETE maintenance sequence
  (`rewrite_position_delete_files` → `rewrite_data_files` → `rewrite_manifests` →
  `expire_snapshots` → `remove_orphan_files`) the file is still live with 2,500 dead rows, and
  one 8,240 B delete file still names it. At 1e7 rows × 50 MERGEs the same shape ended the
  sequence with **8 delete files holding 10,000,000 delete records** (MW-7 §4.4).
  **RP-3 C-006 (2026-08-30, fork `d408da42` / F-16):** the same 1e7×50 MOR driver still ends
  at **8 delete files / 10,000,000 delete records** after the full maintenance sequence
  (`rewrite_position_delete_files` folds 400 → 8; `rewrite_data_files` leaves those 8). The
  2,500-row pin still holds. F-16 did not close this shape.
  **RP-5 C-005 (2026-09-01, fork `00cdde0` / F-16r `#248`):** the 2,500-row pin
  `test_delete_laden_in_band_file_survives_the_runbook` stayed GREEN. F-16r did not close
  that shape. The MW-8 partitioned 6,000-row runbook pin reded: those in-band seed files
  were rewritten. The 1e7 × 50 driver was not re-run. RDF-1 stays BACKLOG.
  **RP-6 C-004 (2026-09-01, fork `fb0cacfa`):** F-16 residue 2 is not in this pin. RDF-1
  stayed BACKLOG.
  **RDF-1 (2026-09-02) — FIXED for the single-referent shape.** The cause was RePark's own
  writer, not the fork's clause. `write_position_deletes` built its Parquet
  `WriterProperties` from the table's compression alone, so parquet-rs's default
  `DEFAULT_STATISTICS_TRUNCATE_LENGTH = Some(64)` truncated the `file_path` statistic. A
  truncated statistic is not `min_is_exact` / `max_is_exact`, and the fork's
  `MinMaxColAggregator` drops an inexact bound — so the delete file reached the manifest with
  **no** `file_path` bound at all. `referenced_data_file_location` then returned `None`, the
  delete was never file-scoped, and `tooHighDeleteRatio` could not see it. The writer now takes
  the fork's `position_delete_writer_properties()` truncation setting
  (`set_statistics_truncate_length(None)`) alongside the table's codec.
  Measured on the 2,500-row v2 merge-on-read fixture (`write.delete.granularity = 'partition'`,
  one 68,523 B in-band data file for a 64 KiB target, one MERGE deleting all 2,500 rows):

  | | `file_path` bounds (field `2147483546`) | `rewrite_data_files` | after the full sequence |
  |---|---|---|---|
  | before | ABSENT (parquet stats truncated at 64 B: min `…/2dfaeaae-`, max `…/2dfaeaae.`) | rewritten 4, `removed_delete_files_count` 0 | 3 data files, 1 delete file, 2,500 delete records, the 100 %-dead seed still live |
  | after | exact; `lower == upper ==` the full 103-byte seeded path | rewritten 5, `removed_delete_files_count` 1 | 2 data files, **0 delete files, 0 delete records**, 2,500 rows, the seed gone |

  SQL `DELETE` / `UPDATE` never carried the defect: they run through iceberg-datafusion, which
  already used the fork's properties.
  **Residue (F-16 residue 2, still open):** a delete file naming **two or more** data files has
  unequal `file_path` bounds, so it is still not file-scoped and the ratio clause still cannot
  see it. That shape is pinned as the incidental control, not repaired here.
- **Apache Spark** — the same sequence on the same shape ends with **zero** delete files and
  **zero** delete records, at **both** `write.delete.granularity` settings, with
  `removed_delete_files_count` reported as 0 and `remove-dangling-deletes` OFF (jar default
  `false`, javap-verified). `BinPackRewriteFilePlanner` carries
  `DELETE_RATIO_THRESHOLD_DEFAULT = 0.3` and a live `tooHighDeleteRatio` clause: a delete-laden
  file is a candidate **regardless of size**, the rewrite physically drops its deleted rows, and
  the delete files covering it die in the rewrite commit.
  *(oracle: recorded — live PySpark 4.0.1 + Iceberg 1.10.0, 200,000-row v2 merge-on-read
  fixtures, tiling and 30 %-deleted shapes; measured 2026-08-24 during MW-7's Critic pass.)*
  **Re-measured 2026-09-02 on the 2,500-row fixture above (live PySpark 4.1.2 + Iceberg 1.11.0,
  single-file CTAS then `write.target-file-size-bytes` 64 KiB):** Spark's delete file carries
  exact, untruncated `file_path` bounds with `lower == upper` at BOTH granularities, and
  `rewrite_data_files` does rewrite the 100 %-dead in-band file (3 data files → 1). The delete
  file itself **survives** all five steps with its 2,500 records and
  `removed_delete_files_count = 0` — dangling, because `remove-dangling-deletes` is off. The
  DATA-file reclaim reproduces; the 2026-08-24 "zero delete files" reading does not, at this
  version. RePark now goes one step past this oracle: its rewrite attributes the file-scoped
  delete to the data file it named and drops it.
- **Pin** —
  `python/repark/tests/test_mw7_scale_smoke.py::test_delete_laden_in_band_file_is_rewritten_and_its_delete_file_dies`
  (the flipped runbook pin), and on the Spark door
  `crates/repark-spark/src/tests/call_rewrite_dangling.rs::call_rewrite_data_files_drops_the_merge_delete_that_names_one_data_file`
  with its incidental control
  `…::call_rewrite_data_files_keeps_a_partition_delete_that_names_two_data_files`.
- **Rationale** — FIXED for a position-delete file that names ONE data file, which is every
  `file` granularity write and every `partition` granularity write whose partition holds one
  data file. The bounds are the routing key: the fork's `referenced_data_file_location` reads
  `referenced_data_file` first and falls back to equal `file_path` bounds, and v2 parquet
  deletes carry no `referenced_data_file`. The remaining miss is a delete file spanning two or
  more data files — F-16 residue 2 in
  [../task/roadmap/mid-term/iceberg-rust-handoff-2026-08-23.md](../task/roadmap/mid-term/iceberg-rust-handoff-2026-08-23.md),
  fork work, and unchanged by this row. Heading kept as the historical anchor (MOR-2
  precedent), so the guide's and MW-8's `#rdf-1` links keep resolving.
  **Contents are unaffected.**

### RDF-SORT-1 — `rewrite_data_files` refuses `sort` / `sort_order`; Spark runs a sort rewrite

- **repark** — `CALL … rewrite_data_files(strategy => 'sort')` and a named `sort_order` refuse
  loud. The fork at `d408da42` ports bin-pack only (GAP_MATRIX R135: sort and z-order deferred).
  A requested option that cannot be honored is never a silent binpack.
- **Apache Spark** — `strategy => 'sort'` with `sort_order => 'id ASC'` rewrites; a six-file v2
  table compacted to one with id min/max `{1..6}`. `sort_order` without `strategy` still
  binpacks (warns that rewritten files are not marked sorted). Missing sort column is
  `ValidationException: Cannot find field '…' in struct`. Unknown strategy is
  `unsupported strategy: {name}. Only binpack or sort is supported`.
  *(oracle: live — PySpark 4.1.2 + Iceberg 1.11.0, 2026-08-31.)*
- **Pin** —
  `crates/repark-spark/src/tests/call.rs::call_rewrite_sort_strategy_refuses_loud`
  and
  `crates/repark-spark/src/tests/call_rewrite_options.rs::call_rewrite_sort_order_refuses_and_does_not_compact`
- **Rationale** — DECLARED fork ceiling until a later iceberg-rust rev ports sort rewrite.
  `where` and `binpack` are honerable on this rev and are not this row.

### MANIFEST-1 — `rewrite_manifests` rewrites data manifests only; Spark rewrites delete manifests too

- **repark** — `CALL <catalog>.system.rewrite_manifests(table => …)` re-groups the **data**
  manifests of the current partition spec and reports only that leg. On a merge-on-read table
  with four data manifests and three delete manifests it answers
  `rewritten_manifests_count = 4`, `added_manifests_count = 1`, and the three delete manifests
  are carried forward untouched. When the data leg has nothing to do **and** two or more delete
  manifests are present, the call **refuses** rather than answering two zeros.
- **Apache Spark** — runs two legs in one procedure and sums them. Measured on the same shapes:
  five data manifests plus three delete manifests answered `8, 2` (both legs compacted, manifests
  8 → 2); one data manifest plus two delete manifests answered `2, 1`; one data manifest plus one
  delete manifest answered `0, 0`, because a single matching manifest per leg is already at
  Spark's target.
  *(oracle: live — PySpark 4.1.2 + Iceberg 1.11.0, 2026-09-02, re-measured by V3-11 at a
  `coalesce(1)` single-file layout: five data plus three delete manifests answered
  `rewritten_manifests_count = 8, added_manifests_count = 2` and left one manifest per leg;
  one data plus two delete manifests answered `2, 1`; one data plus one delete manifest
  answered `0, 0`;
  the `DataSourceV2Relation` note this row used to carry is retired — see
  [MOR-1](#mor-1--rewrite_position_delete_files-compacts-below-sparks-min-input-files-floor).)*
- **Pin** —
  `crates/repark-spark/src/tests/call_manifests.rs::call_rewrite_manifests_reports_the_data_leg_and_leaves_delete_manifests`
  and `::call_rewrite_manifests_refuses_zeros_while_delete_manifests_stay`
- **Rationale** — BACKLOG, and it is fork work. The owned fork's `RewriteManifestsAction` keeps
  every `Deletes`-content manifest byte-identical by design, so outstanding merge-on-read deletes
  still apply after the rewrite; there is no delete leg to call. **Contents are unaffected** — the
  live row set is identical either way, and this is manifest layout. The refusal covers the one
  shape where the divergence would be invisible: two zeros read as "nothing to compact", so an
  operator would run the procedure forever on a table that never compacts. Closing the row means a
  delete-manifest rewrite in the fork.

### MANIFEST-2 — `rewrite_manifests` refuses `spec_id`; `use_caching` is accepted and does nothing

- **repark** — `spec_id` refuses loud, named or positional. The procedure always rewrites the
  manifests of the table's **current** partition spec, which is Spark's default, and older specs'
  manifests are kept. `use_caching` is accepted, type-checked as a boolean **literal**, and
  changes nothing: a quoted `use_caching => 'true'` refuses here.
- **Apache Spark** — takes both (`RewriteManifestsProcedure.PARAMETERS`: `table` STRING required,
  `use_caching` BOOLEAN optional, `spec_id` INTEGER optional). `spec_id` selects the spec whose
  manifests are rewritten and refuses an id the table does not have (`Invalid spec id 7`);
  `use_caching` sets the action's `use-caching` option, which caches Spark's own manifest
  DataFrame. Measured: `use_caching => true`, `use_caching => false` and the bare call all
  answered `5, 1` on the same five-manifest table, and `spec_id => 0` on a spec-0 table answered
  `5, 1` as well. Spark also **accepts a STRING literal** for it — `use_caching => 'true'`,
  `'yes'` and `'no'` each executed and answered `5, 1`, because the procedure's typed parameter
  casts the string — and refuses only a non-castable type: `use_caching => 1` fails analysis with
  `[DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE] … requires the "BOOLEAN" type, however "1" has the
  type "INT"`. So a migrating job written `use_caching => 'true'` runs on Spark and refuses here.
  *(oracle: recorded — live PySpark 4.0.1 + Iceberg 1.10.0, same basis as MANIFEST-1.)*
- **Pin** —
  `crates/repark-spark/src/tests/call_manifests.rs::call_rewrite_manifests_argument_surface_is_sparks`
  and `python/repark/tests/test_maintenance_call.py::test_rewrite_manifests_spec_id_refuses_and_use_caching_is_accepted`
- **Rationale** — DECLARED. `use_caching` is a Spark-side execution option with no counterpart
  here, and accepting it keeps a migrating maintenance job's SQL unchanged while the type check
  keeps a typo loud. The stricter literal rule is kept deliberately, and it is the same rule
  `remove_orphan_files`' `dry_run` already carries: on this surface a quoted boolean is far more
  likely a typo than an intent, and Spark's own cast would read `'yes'` as true and an
  unrecognized string as null. The cost is one edit in a migrating job, and the refusal names the
  argument. `spec_id` is a *behaviour* selector, so accepting it and ignoring it would
  silently rewrite the wrong spec's manifests; refusing names what the engine actually does. The
  fork exposes `RewriteManifestsAction::rewrite_if`, which this engine already uses to pin Spark's
  default (current spec), so wiring the argument is possible — it is a scope decision, not a
  capability gap.

### MANIFEST-3 — above the manifest target size, `rewrite_manifests` writes a different number of manifests

- **repark** — five over-target data manifests (17,777 B total at
  `commit.manifest.target-size-bytes = 4096`) answer `rewritten_manifests_count = 5`,
  `added_manifests_count = 3`, and the table holds 3 manifests afterwards. Twelve (42,682 B)
  answer `12, 6`. The fork's action opens one writer per cluster key and rolls to a new manifest
  when a RUNNING ESTIMATE of the open writer's size reaches the target — the estimate is the
  source manifest's average per-entry size, because the Rust `ManifestWriter` buffers its entries
  and exposes no incremental on-disk length.
- **Apache Spark** — the same two fixtures answer `5, 5` and `12, 12`, leaving the manifest count
  where it started. `RewriteManifestsSparkAction` computes
  `targetNumManifests = ceil(total / target)` (9 and 21 here) and repartitions the manifest-entry
  DataFrame into that many groups, so with more groups than entries every entry lands in its own
  manifest.
  *(oracle: recorded — live PySpark 4.0.1 + Iceberg 1.10.0, same basis as MANIFEST-1.)*
- **Pin** —
  `crates/repark-spark/src/tests/call_manifests.rs::call_rewrite_manifests_added_count_diverges_above_the_target_size`
- **Rationale** — BACKLOG, and narrow: **only `added_manifests_count` moves.**
  `rewritten_manifests_count` agreed with Spark on every shape measured, at both target sizes, and
  the live row set is identical either way — this is how many files the entries are spread over,
  not which entries are live. It does not appear at the default 8 MB target, where both engines
  write one manifest and the counts agree; it needs a table whose manifests are individually over
  target. Disclosed rather than refused, because the rewrite is correct and useful there: refusing
  would deny manifest compaction to exactly the large tables that need it most, and the number the
  engine reports is an honest count of what it wrote. Closing the row means giving the fork Java's
  `ceil(total / target)` sizing, which is fork work.

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
  would have written a string. Not closed here because it changes what a working query
  returns (the same class of break `LOG-1` needed a dated ruling to take).

### V3-LINEAGE-1 — `rewrite_data_files` carries row lineage through format-v3 compaction

> **FIXED 2026-08-31 (RP-4 / fork #243 F-7 slice 1).** `CALL system.rewrite_data_files` on a
> twelve-file v3 table rewrites 12→1 and PySpark 4.1.2 + Iceberg 1.11.0 reads the same
> `(id, _row_id, _last_updated_sequence_number)` and Arrow types (`int64`) before and after.
> Residue: `B-MOR-3` (position-delete rewrite still refuses live DVs). `V3-DANGLE-1` FIXED by V3-5.

- **repark** — `CALL <catalog>.system.rewrite_data_files(table => …)` compacts a format-v3
  table and carries `_row_id` / `_last_updated_sequence_number` through unchanged. The 2026-08-21
  pre-guard measurement reassigned lineage (`id=5099` moved `_row_id` 599→691); RP-2/RP-3
  re-measures at `ce92a7bf` / `d408da42` still reassigned 0..11 → 12..23, seq → 13. Fork #243
  closed that.
- **Apache Spark** — performs the rewrite and carries lineage through unchanged: the same row
  reads `_row_id = 599, seq = 6` on both sides of the CALL, and the result is
  `rewritten_data_files_count = 6`, `added_data_files_count = 1`,
  `removed_delete_files_count = 6` (the six deletion vectors die with the files they were scoped
  to). Round-tripped through the Spark reader afterwards to confirm the lineage columns, not
  inferred from metadata.
  *(oracle: live — PySpark 4.0.1 + Iceberg 1.10.0, format-v3 Hadoop-catalog fixture;
  the `DataSourceV2Relation` note this row used to carry is retired — see
  [MOR-1](#mor-1--rewrite_position_delete_files-compacts-below-sparks-min-input-files-floor).)*
- **Pin** —
  `crates/repark-spark/src/tests/call_v3.rs::call_rewrite_data_files_on_v3_preserves_row_lineage`
- **Rationale** — FIXED. The refusal was stricter than Spark on purpose while the fork
  reassigned lineage. Fork #243 carries `_row_id` / seq through `RewriteDataFiles`; the public
  CALL is Spark-equal on values and Arrow types (RP-4, 2026-08-31). Default `CREATE` is still
  v2 (`the_engine_still_cannot_produce_a_v3_table`). Opt-in CREATE now reaches the CALL
  (`opt_in_create_produces_v3_and_rewrite_runs`). The RP-4 twelve-file fixture had no
  live DVs (`removed_delete_files_count = 0`); V3-5 pinned the DV-drop half.

### V3-DANGLE-1 — v3 `rewrite_data_files` drops deletion vectors scoped to rewritten files

> **FIXED 2026-08-31 (V3-5 / fork `33be9a0`).** `CALL system.rewrite_data_files` on a
> six-file v3 MOR table with one Puffin DV per data file rewrites 6→1, reports
> `removed_delete_files_count = 6`, leaves zero live DVs, and keeps live rows plus
> `_row_id` / seq. The RP-4 twelve-file fixture had no live DVs so this count was
> unmeasured there. Spark's six-file Hadoop fixture reported `6` with no option set
> (V3-0, PySpark 4.0.1 + Iceberg 1.10.0).

- **repark** — a v3 compact drops every deletion vector whose `referenced_data_file`
  was rewritten, in the same commit, without `'remove-dangling-deletes'`. The
  result column is the fork's true count. A `where => 'part = 0'` compact drops
  only that partition's vector and keeps the sibling live (shared-Puffin rewrite).
  The V3E-3 partitioned fixture rewrites both delete-laden files (Java delete-ratio
  0.3) and reports `removed_delete_files_count = 2`.
- **Apache Spark** — the same compact removes the file-scoped vectors and reports
  `removed_delete_files_count = 6` on the six-file fixture, with no option set.
  Removal is an ordinary consequence of v3 compaction, not an opt-in sub-action.
  *(oracle: live — PySpark 4.1.2 + Iceberg 1.11.0, 2026-09-02: a six-file v3 table with one
  Puffin DV per file answered `rewritten_data_files_count = 6, added_data_files_count = 1,
  removed_delete_files_count = 6` and left zero live delete files;
  the `DataSourceV2Relation` note this row used to carry is retired — see
  [MOR-1](#mor-1--rewrite_position_delete_files-compacts-below-sparks-min-input-files-floor).)*
- **Pin** —
  `crates/repark-spark/src/tests/call_v3_dv.rs::call_rewrite_data_files_on_v3_drops_scoped_deletion_vectors`;
  `crates/repark-spark/src/tests/v3e3.rs::partitioned_v3_dv_rewrite_data_files_drops_both_vectors`
  and `partitioned_v3_dv_rewrite_where_part0_keeps_the_sibling_vector`;
  facade `python/repark/tests/test_v3_dv_compaction.py::test_facade_rewrite_data_files_drops_scoped_v3_deletion_vectors`.
  Pins: v3-5-dv-compaction/C-001, C-002, C-004.
- **Rationale** — FIXED. Fork `plan_dv_removal` / `rewrite_siblings_for_dropped_references`
  is wired in `RewriteDataFiles::rewrite_group` at `33be9a0`. The engine CALL already
  forwarded `removed_delete_files_count`; V3-5 measured the public path on live DVs.

### B-MOR-3 — `rewrite_position_delete_files` refuses live Puffin deletion vectors

- **repark** — `CALL <catalog>.system.rewrite_position_delete_files(table => …)` refuses when the
  current snapshot holds any live Puffin deletion vector, naming the count. A Spark-written
  format-v3 table with three vectors returns
  `found 3 live Puffin deletion vector(s)` rather than four zeros.
- **Apache Spark** — returns all four counts as `0` and does nothing. Re-measured 2026-09-02
  (V3-11) on the pinned oracle: three MoR `DELETE`s on a v3 table produced three `PUFFIN`
  delete files, the procedure answered
  `rewritten_delete_files_count = 0, added_delete_files_count = 0, rewritten_bytes_count = 0,
  added_bytes_count = 0`, all three vectors stayed live, the surviving rows were unchanged, and
  a second run answered the same four zeros. Spark's own answer on v3 is the silent no-op this
  engine refuses to give.
  *(oracle: live — PySpark 4.1.2 + Iceberg 1.11.0, Hadoop-catalog fixture, 2026-09-02;
  the `DataSourceV2Relation` note this row used to carry is retired — see
  [MOR-1](#mor-1--rewrite_position_delete_files-compacts-below-sparks-min-input-files-floor).)*
- **Pin** —
  `crates/repark-spark/src/tests/call_register.rs::call_rewrite_position_delete_files_refuses_spark_written_puffin_vectors`
  (CI-runnable Spark-written fixture; 37 live rows after the vectors apply, pinned beside it by
  `call_register_table_adopts_a_spark_written_v3_table_with_puffin_vectors`);
  `crates/repark-spark/src/tests/v3e3.rs::partitioned_v3_dv_rewrite_position_delete_files_still_refuses`
  and `partitioned_v3_dv_fork_rewrite_position_delete_files_measurement` (RP-3 C-007);
  facade `python/repark/tests/test_v3e3_fixtures.py::test_facade_partitioned_v3_dv_matches_spark_live_rows`;
  V3-5 `call_v3_dv.rs::call_rewrite_position_delete_files_still_refuses_engine_written_v3_dvs`
  and `test_v3_dv_compaction.py` (engine-written six-DV refuse). Pins: v3-5-dv-compaction/C-003.
- **Rationale** — DELIBERATE, stricter than Spark on purpose (owner decision OD-2). **RP-3
  C-007 (2026-08-30, fork `d408da42` / R136 F-7 U3):** the v3 arm *runs* and is read-identity
  on the V3E-3 partitioned-DV fixture, but it **converts parquet position deletes into DVs**.
  On a DV-only table it returns four zeros (`rewritten=0 added=0`, two DVs stay two) and a
  second run converges. Zeros still read as already-clean, so `B-MOR-3` stays (OD-2).
  **V3-5 (2026-08-31):** DV compaction lands through `rewrite_data_files` (`V3-DANGLE-1`
  FIXED). This procedure does not compact live DVs; Spark also returns four zeros on a
  DV-only table. The CALL refuse remains so those zeros cannot mean already-clean.

### V3-ADOPT-1 — Hadoop `vN.metadata.json` pointers register, read, and write `v(N+1)`

> **FIXED 2026-08-30 (RP-3 / fork #235 F-14).** A table registered from a Hadoop
> `vN.metadata.json` pointer now takes a write; the next pointer is uncompressed
> `v(N+1).metadata.json`. The pin flipped from "the refusal names the convention" to
> "the write succeeds". Residue on fork row R167: no `version-hint.text` writer, no
> exists-fail rename.

- **repark** — `CALL system.register_table` of a metadata file named `vN.metadata.json` (the
  Hadoop catalog convention) succeeds and subsequent reads return the adopted rows. A later
  write — Spark-door `INSERT` and the ANSI-door `INSERT` after `Catalog::register_table` —
  commits `v(N+1).metadata.json` and serves the new rows. Glue is unaffected: it already
  writes version-uuid pointers.
- **Apache Spark** — registers the Hadoop-named pointer the same way; reads work; writes
  also work, because Spark's Hadoop catalog writes the next `v(N+1).metadata.json` itself.
  *(oracle: recorded — V3-0 isolated the cause by copying the identical file to a version-uuid
  name, after which `INSERT` and `expire_snapshots` both succeeded on this engine. RP-3
  remeasured the Hadoop name itself at fork `d408da42`.)*
- **Pin** —
  `crates/repark-spark/src/tests/call_register.rs::call_register_table_of_hadoop_named_metadata_writes_name_the_convention`
  (Spark door),
  `crates/repark-sql/src/v3/cow.rs::ansi_hadoop_named_metadata_write_bumps_to_the_next_hadoop_pointer`
  (ANSI door)
- **Rationale** — retired. The owned fork closed Hadoop pointer math; this engine consumed
  it at `d408da42`.

### S3T-1 — S3 Tables `register_table` is a dated service gap (fork R126)

- **repark** — `CALL <catalog>.system.register_table` against an S3 Tables catalog refuses
  before any AWS call, naming the missing register-by-metadata-location operation, the
  Iceberg REST register endpoint, `UpdateTableMetadataLocation`, and fork GAP_MATRIX row
  **R126**. Glue implements the same CALL.
- **Apache Spark** — S3 Tables has no register-by-metadata-location API either; Spark cannot
  adopt an existing metadata file into a table bucket. The service mapping does not include
  the Iceberg REST `register` endpoint.
  *(oracle: documented — Iceberg REST register vs S3 Tables `UpdateTableMetadataLocation`;
  fork #233 dated the gap as R126.)*
- **Pin** —
  `crates/repark-spark/src/tests/call_register.rs::call_register_table_on_s3_tables_names_the_dated_service_gap`
- **Rationale** — DECLARED, dated service gap, not an engine stub. The fork returns
  `FeatureUnsupported` before any AWS call so the engine can cite R126 instead of "not
  supported yet". Pins: rp-3-fork-repin/C-008.

### S3T-V3-1 — FIXED (LIVE-v3-M, 2026-09-02): both live v3 legs are green; S3 Tables accepts `format-version = 3` at CREATE

- **repark** — `test_v3_dv_dml_maintenance_against_glue` and
  `test_v3_dv_dml_maintenance_against_s3tables` both passed on `aws-acceptance`
  [run 33635288918](https://github.com/TRO-Wolf/repark/actions/runs/33635288918), dispatched on
  merged `main` `8c4bc55` (2026-09-02; the acceptance module reported `6 passed in 122.13s` — the
  four pre-existing legs plus these two). They drive the shared `run_v3_acceptance` body: opt-in
  v3 CTAS (`format-version 3`, merge-on-read delete/update/merge, identity `part`), single-row
  appends to five files per partition, row-scoped `DELETE` (one Puffin DV), `MERGE`
  matched-UPDATE + NOT MATCHED INSERT (two DVs), a `_row_id` /
  `_last_updated_sequence_number` read, `rewrite_data_files` (12 rewritten, 2 added, 2 delete
  files removed, 0 DVs left), `expire_snapshots` (14 snapshots → 1), and on Glue only a
  `register_table` of the final metadata location on a second session. **Glue reproduced the
  local engine's numbers exactly** — that leg asserts with `exact_commit_counts=True`, so every
  count above is the measured Glue answer. **The S3 Tables leg took the decision table's accepted
  branch**: the service took `format-version = 3` at CREATE, so the full sequence ran with
  `exact_commit_counts=False` — row sets, `_row_id` values and every data- and delete-file count
  exact, only the service's own commit counts (sequence numbers, snapshot totals) relaxed,
  because S3 Tables commits maintenance on its own (MW-10). History, in one line: the other
  branch — a classified `format-version 3` refusal at CREATE asserts no table was left behind,
  records the masked refusal text and passes — stays wired and did not run; the log carries no
  `S3T-V3-1 refused-at-create` record.
- **Apache Spark** — no divergence to record. Glue is a metadata catalog and imposes no Iceberg
  format version, so the Glue leg behaves as the local engine does. AWS publishes no statement
  either way about `format-version = 3` on S3 Tables; the measurement above is the answer.
  `register_table` on an S3 Tables catalog remains the dated service gap `S3T-1` / fork R126 and
  is not attempted there.
  *(oracle: live — `aws-acceptance` run 33635288918 on merged `main` `8c4bc55`, 2026-09-02, job
  "tier-2 live AWS acceptance (Glue + S3 Tables, scratch-only)", `6 passed in 122.13s`.)*
- **Pin** —
  `python/repark/tests/test_v3_acceptance_local.py::test_v3_acceptance_leg_body_against_the_local_catalog`,
  `python/repark/tests/test_v3_acceptance_local.py::test_v3_create_refusal_classification_is_the_s3_tables_decision_table`,
  `python/repark/tests/test_acceptance_v3_helpers.py::test_v3_legs_are_twins_of_the_mor_legs`
- **Tightened 2026-09-02 (V3-11), confirmed live 2026-09-03.** The shared leg body's
  `assert_v3_lineage` now demands the inserted row's exact `_row_id = 11` on both legs
  (`exact_commit_counts` does not gate it) where it demanded only a fresh unused id. The counts
  above are unchanged. Both legs have now been re-dispatched under the tightened assertion:
  `aws-acceptance` [run 33699342417](https://github.com/TRO-Wolf/repark/actions/runs/33699342417)
  on merged `main` `a0fe83a`, 2026-09-03 00:48 UTC, `6 passed in 230.67s` — Glue and S3 Tables
  each read the exact inserted `_row_id`, so the confirmation this row was waiting on is
  recorded and no re-dispatch is outstanding.
- **Rationale** — FIXED by measurement, not by an engine change: the service question this row
  was opened on is answered, so the row is no longer BACKLOG. It is kept as the single home of
  the answer rather than retired, and §6's retire-with-a-RED-pin path does not apply — the local
  pins above stay green precisely because the live run reproduced them. A future S3 Tables
  refusal of `format-version = 3` would red the S3 Tables leg against this row, not silently
  re-open it. Pins: live-v3-aws-legs/C-001, C-002, C-003, C-004;
  live-v3-first-measurement/C-001.

### V3-COW-1 — FIXED (V3-8, 2026-09-02): v3 row-DML keeps row lineage on every served shape

- **repark** — V3-8 (2026-09-02) carries stored `_row_id` / `_last_updated_sequence_number`
  through the subquery-`WHERE` copy-on-write rewrite, the last shape that reassigned them.
  Single-file seed `(id,_row_id,seq) = (1,a,0,1),(2,b,1,1),(3,c,2,1)`, next-row-id 3, 1 data
  file: `DELETE … WHERE id IN (SELECT …)` and `EXISTS` leave `(1,0,1),(3,2,1)` at
  next-row-id 5 (first-row-id 3, added 2, 1 data file); `NOT IN` and `NOT EXISTS` leave
  `(2,1,1)` at next-row-id 4 (added 1, 1 data file); `UPDATE … SET name='m' WHERE id IN
  (SELECT …)` leaves `(1,0,1),(2,1,2),(3,2,1)` at next-row-id 6 (added 3). A
  correlated-to-target `DELETE … AS tgt WHERE tgt.id IN (SELECT s.id FROM src s WHERE
  s.id = tgt.id)` is served too, at the same values as the uncorrelated `IN` cell; its
  `s.id = tgt.id + 1` variant matches nothing and leaves the table at the seed. The refusal seat
  `crates/repark-iceberg/src/write/row_lineage_guard.rs` is deleted — it has no caller left.
  RP-6 (2026-09-01) lifted plain-`WHERE` UPDATE and sequential COW DELETE; V3-7 (2026-09-02)
  lifted MERGE on COW and merge-on-read.
- **Apache Spark** — COW `DELETE`/`UPDATE` on v3 **preserve** `_row_id` /
  `_last_updated_sequence_number` under a subquery `WHERE` exactly as under a plain one, on
  `IN`, `NOT IN`, `EXISTS` and `NOT EXISTS`; merge-on-read serves the same shapes with a
  deletion vector (`DELETE` next-row-id 3, added 0; `UPDATE` next-row-id 4, added 1).
  Seed `(1,a,0,1),(2,b,1,1),(3,c,2,1)`, next-row-id 3. Correlated-to-target `IN` behaves as
  the uncorrelated cell on COW (next-row-id 5, added 2, 1 data file); the `+ 1` variant
  matches nothing and commits an **empty** `overwrite` snapshot (next-row-id 3, added 0,
  1 data file, 1 manifest). Spark never reassigned a stored id on any of the eighteen cells.
  *(oracle: live — PySpark 4.1.2 + Iceberg 1.11.0, Hadoop catalog, `local[1]`, `coalesce(1)`,
  2026-09-02 V3-8 transcript)*.
- **Pin** —
  `crates/repark-spark/src/tests/v3_subquery_dml.rs` (five shapes × created and adopted, plus
  `created_v3_cow_correlated_subquery_delete_keeps_row_lineage` /
  `adopted_v3_cow_correlated_subquery_delete_keeps_row_lineage`,
  `created_v3_cow_correlated_subquery_delete_matching_nothing_leaves_the_table_unmoved` and the
  merge-on-read lift control
  `created_v3_merge_on_read_subquery_dml_commits_deletion_vectors`);
  Spark-door residual control
  `crates/repark-spark/src/tests/v3_cow.rs::adopted_v3_subquery_update_outside_the_hole_still_refuses`;
  ANSI twins `crates/repark-sql/src/v3/cow.rs::adopted_v3_cow_subquery_where_dml_keeps_row_lineage`
  and `ansi_door_still_refuses_subquery_predicate_shapes_outside_the_hole`; facade
  `python/repark/tests/test_v3_cow_dml.py::test_facade_adopted_v3_cow_subquery_where_dml_keeps_row_lineage`
  and `test_facade_v3_subquery_update_outside_the_hole_still_refuses`; live cell
  `python/repark/tests/test_v3_live_oracle.py::test_v3_subquery_where_dml_live_cow`.
  Earlier shapes stay pinned by
  `crates/repark-spark/src/tests/v3_cow.rs::adopted_v3_cow_delete_carries_survivor_row_lineage`,
  `adopted_v3_cow_second_delete_keeps_survivor_row_id`,
  `adopted_v3_cow_update_keeps_row_id_and_bumps_matched_seq`,
  `adopted_v3_cow_merge_matched_update_keeps_row_id` and
  `crates/repark-spark/src/tests/v3_cow_lift.rs`.
  Pins: v3-8-subquery-where-lineage/C-002; v3-7-merge-lineage/C-002; rp-6-fork-repin/C-002, C-003.
- **Rationale** — FIXED; the **owner ruling 2026-08-25** that held this row BACKLOG is
  discharged. One residual refusal remains and it is not about lineage: subquery spellings
  outside the allow-listed hole (`UPDATE … NOT IN` / `EXISTS`, correlated-to-target **on
  `UPDATE` only** — the correlated `DELETE` is served — nested, aggregate) refuse on the
  pre-existing `refuse_dml_subquery_predicate` guard (`G3-E8`). V3-8's other residual, the
  V2-only delete-file gate on merge-on-read predicate DML, is **FIXED by V3-9 (2026-09-02)** —
  see `V3-MOR-1` below.
  F-rp3-c7 is consumed as a layout artefact. F-v3-7-mor-mixed is a layout artefact: mixed
  MERGE is lineage Spark-equal; Spark writes 1 (COW) / 2 (MoR) data files, the engine writes
  2 (COW) / 3 (MoR). **F-v3-8-update-files** is a layout artefact: subquery-`WHERE` COW
  UPDATE is lineage and next-row-id Spark-equal; Spark writes 1 data file, the engine writes
  2 (survivors and updated rows come from the two arms of a `UNION ALL`).
  **F-v3-8-empty-delete-snapshot** is a commit-semantics artefact, pre-dating this unit: a
  subquery-`WHERE` DELETE that matches no row commits nothing on the engine, where Spark
  commits an empty `overwrite` snapshot. Rows, lineage, next-row-id and data-file count are
  equal on both sides.

### V3-MOR-1 — FIXED (V3-9, 2026-09-02): merge-on-read predicate DML writes deletion vectors on v3

- **repark** — `resolve_write_mode` gated merge-on-read `DELETE … WHERE` / `UPDATE … WHERE`
  to `format_version == V2`; V3-9 lifts it to `< V2`, so v3 predicate DML reuses the DV path
  MERGE has used since V3-7 (`prepare_row_delta_deletes` → `close_touched_dv_containers`).
  Single-file seed `(id,name,_row_id,seq) = (1,a,0,1),(2,b,1,1),(3,c,2,1)`, next-row-id 3, 1
  data file: `DELETE` on `IN` / `EXISTS` / plain leaves `(1,a,0,1),(3,c,2,1)` at next-row-id 3
  (first-row-id 3, added 0, 1 data file, one file-scoped Puffin DV of 1 record); `NOT IN` /
  `NOT EXISTS` leave `(2,b,1,1)` at the same counters with a 2-record DV; `UPDATE … SET
  name='m'` on `IN` and plain leaves `(1,a,0,1),(2,m,1,2),(3,c,2,1)` at next-row-id 4
  (added 1, 2 data files, one 1-record DV). `write.delete.granularity` changes nothing on v3.
  Plain-`WHERE` merge-on-read DML never sat on this gate — only allow-listed subquery shapes
  reach `execute_predicate_dml` — and has written DVs since RP-6; it is pinned here as a
  control. Format v2 merge-on-read predicate DML still writes Parquet position deletes with a
  NULL `referenced_data_file`.
- **Apache Spark** — merge-on-read `DELETE`/`UPDATE` on v3 write one deletion vector per
  touched data file: content `POSITION_DELETES`, format `PUFFIN`, `referenced_data_file` set,
  `content_offset` 4; the snapshot summary carries `added-dvs 1` and `added-delete-files 1`.
  `DELETE` adds no records (next-row-id stays 3); `UPDATE` appends the changed row as one data
  file and advances next-row-id by that appended count only, keeping the stored `_row_id` and
  moving `_last_updated_sequence_number` to the new sequence number. Granularity `file` and
  `partition` produce the identical result.
  *(oracle: live — PySpark 4.1.2 + Iceberg 1.11.0, Hadoop catalog, `local[1]`, `coalesce(1)`,
  2026-09-02 V3-9 transcript; nine cells)*.
- **Pin** — `crates/repark-spark/src/tests/v3_mor_dml.rs` (seven shapes × created and adopted,
  the granularity cell, the no-match cell and the v2 Parquet-position-delete control);
  `crates/repark-spark/src/tests/v3_subquery_dml.rs::created_v3_merge_on_read_subquery_dml_commits_deletion_vectors`;
  shared-Puffin sibling
  `crates/repark-spark/src/tests/v3e4.rs::subquery_delete_on_the_shared_puffin_v3_table_keeps_the_untouched_sibling`;
  ANSI twin
  `crates/repark-sql/src/v3/cow.rs::adopted_v3_mor_subquery_where_dml_writes_file_scoped_deletion_vectors`;
  facade
  `python/repark/tests/test_v3_cow_dml.py::test_facade_adopted_v3_mor_subquery_where_dml_writes_deletion_vectors`;
  live cell `python/repark/tests/test_v3_live_oracle.py::test_v3_mor_subquery_where_dml_live`.
  Pins: v3-9-mor-predicate-dml-dv/C-002, C-003, C-004.
- **Rationale** — FIXED for rows, lineage, next-row-id and delete-entry content. The one dated
  residual it carried was a Puffin **container-packing** difference, not a row or lineage
  difference; `V3-DV-1` below is FIXED too (RP-7, 2026-09-02). No new deletion-vector code: the lift is one format-version
  comparison, and the write path was already V3-7's. The v3 create opt-in message dropped its
  now-false parenthetical ("v3 tables cannot yet do merge-on-read row-level writes") in the
  same unit; it names only the conf and the v2 default. Pins:
  v3-9-mor-predicate-dml-dv/C-006.

### V3-DV-1 — FIXED (RP-7, 2026-09-02): the shared-Puffin container close is Spark-equal

- **repark** — after the fork repin to `ff4764d3` (fork PR `#260`, F-18)
  `close_touched_dv_containers_with_partitions` rewrites **only the touched blob**, into one new
  container per statement, and leaves every untouched sibling entry byte-identical. Re-measured
  on the Spark-written partitioned fixture (2 data files, 2 DVs packed in one Puffin at offsets 4
  and 46): after `DELETE … WHERE id IN (SELECT …)` touching only the `part = 0` file there are
  **two containers** — the touched file's DV in a new one at offset 4 with 2 records, and the
  sibling entry still at its **original** container and **original** offset with 1 record (which
  of the two blobs lands at offset 4 and which at 46 follows the seed writer's ordering and is not
  stable run to run, so the pins compare the sibling to its own before-value). Snapshot
  summary `removed-delete-files 1` / `removed-dvs 1` / `removed-position-deletes 1` /
  `added-delete-files 1` / `added-dvs 1` / `added-position-deletes 2`. Removal is keyed by Java's
  `DeleteFileSet` triple `(location, content_offset, content_size_in_bytes)`, not by path.
- **Apache Spark** — the same statement, same layout, same summary
  *(oracle: live — PySpark 4.1.2 + Iceberg 1.11.0, Hadoop catalog, `local[1]`, ANSI on,
  partitioned v3 merge-on-read, re-measured 2026-09-02 for RP-7 by the committed live cell)*.
  Both readings are now equal.
- **Cost** — the write amplification is closed, measured in this tree
  (`v3e4.rs::measure_later_single_row_delete_bytes`, one blob per data file, later single-row
  `DELETE`):

  | blobs in the container | containers after / bytes written, pin `fb0cacfa` | pin `ff4764d3` |
  |---|---|---|
  | 16 | 1 / 4,830 B | 2 / 377 B |
  | 64 | 1 / 19,126 B | 2 / 377 B |

  The data-file walk is closed for ANY table, partitioned or not: RePark hands the close the
  `(spec_id, partition)` its own target scan already planned for every file the statement
  touched, so a v3 `DELETE` reads **no** data manifest whether or not the touched files already
  carry DVs. Measured on a 192-partition fresh-path `DELETE`: 2,176 → 761 ms.
- **Pin** —
  `crates/repark-spark/src/tests/v3e4.rs::subquery_delete_on_the_shared_puffin_v3_table_keeps_both_file_scoped_deletion_vectors`
  now pins Spark's layout: two containers, the sibling `(container, offset, record_count)` tuple
  unchanged, the touched blob at offset 4, and the six summary counts above.
  `v3e4.rs::a_later_single_row_delete_writes_one_blob_not_the_whole_container` holds the byte
  budget at 16 blobs;
  `crates/repark-iceberg/src/write/merge/dv_close.rs::shared_puffin_row_delta_keeps_the_untouched_sibling`
  keeps the semantic assertion and gains the layout one;
  `dv_close.rs::a_supplied_partition_map_closes_a_fresh_partitioned_delete_with_no_data_manifest`
  holds the manifest-read budget for a fresh PARTITIONED delete, and
  `merge/tests/partition_sink.rs::the_target_scan_records_every_planned_file_partition` holds the
  scan side of it; `dv_close.rs::closing_a_covered_v3_delete_reads_no_data_manifest` records the
  fork's own laziness for an already-covered path. Live cell
  `python/repark/tests/test_v3_dv_container_close.py::test_v3_shared_puffin_container_close_live`.
- **Rationale** — FIXED. The packing lived in the fork and was fixed there (F-18); RePark
  consumed it in repin **RP-7**, re-aimed the narrowed V3-9 pin at Spark's exact layout, and
  re-measured the oracle at the matched layout rather than trusting the V3-9 transcript. Pins:
  rp-7-f18-repin/C-001, C-002, C-003.

### V3-ROWID-3 — FIXED (V3-11, 2026-09-02): the merge-on-read MERGE insert's `_row_id`

> **FIXED 2026-09-02 (V3-11).** One commit's new data files are handed to the manifest in
> ascending partition-value order, so `first_row_id` assignment is deterministic. The
> LIVE-v3 cell now reads `_row_id = 11` in **10 of 10** consecutive runs, Spark's value.

- **repark** — on a v3 merge-on-read table, `MERGE … WHEN NOT MATCHED THEN INSERT` gives the new
  row `_row_id = 11` over **10 identical runs** of the LIVE-v3 sequence (one CTAS row, nine
  single-row appends to ids 1–10, `DELETE … WHERE id = 3`, then the MERGE inserting id 11), with
  `_last_updated_sequence_number = 12` and the survivor triples `(1,0,1) (2,1,12) (4,3,4)
  (5,4,5) (6,5,6) (7,6,7) (8,7,8) (9,8,9) (10,9,10)`. Before the fix the same ten runs read
  `11` six times and `10` four times: the MERGE writes the matched-UPDATE row into partition
  `part = 0` and the inserted row into `part = 1`, two files in one commit, and the fanout
  writer closed them in `HashMap` order, so whichever file was numbered first took
  `first_row_id = 10`. Three consecutive ten-run batteries measured in this tree before the
  fix read 6, 4 and 2 correct answers out of ten (24 red of 30).
- **Apache Spark** — deterministic at `_row_id = 11`, the table's `next-row-id` at that commit:
  **10 of 10** runs (two JVMs × five fresh warehouses) on the identical statement sequence gave
  `(11, 11, 12)`, with the same nine survivor triples and the same single Puffin deletion vector
  after the DELETE.
  *(oracle: live PySpark 4.1.2 + `iceberg-spark-runtime-4.1_2.13:1.11.0`,
  `JAVA_HOME=/usr/lib/jvm/zulu-17-amd64`, `local[2]`, Hadoop catalog, measured 2026-09-02 by
  unit LIVE-v3 and re-measured by V3-11.)*
- **Why it moved** — the MERGE writes the matched-UPDATE row into `part = 0` and the inserted
  row into `part = 1`, two files in one commit, and the fanout writer closed them in `HashMap`
  order. V3-11 orders one commit's files by ascending partition value before the manifest sees
  them, which for this two-value identity-int set is exactly Spark's order. The general rule and
  the sets where the two engines disagree are
  [V3-FILEORDER-1](#v3-fileorder-1--declared-v3-11-2026-09-02-same-commit-data-file-order-is-ascending-partition-value-not-sparks-hash-bucket-order).
- **Pin** — `crates/repark-spark/src/tests/v3_row_order.rs` (Spark door): the ten-run
  determinism pin `mor_merge_insert_takes_sparks_row_id_in_ten_consecutive_runs`, plus
  `mor_merge_across_three_partitions_numbers_files_ascending_by_partition_value` and
  `partitioned_ctas_numbers_files_ascending_by_partition_value`; ANSI twins
  `crates/repark-sql/src/v3/cow.rs::ansi_mor_merge_across_three_partitions_numbers_files_ascending_by_partition_value`
  and `::ansi_partitioned_ctas_numbers_files_ascending_by_partition_value`; facade
  `python/repark/tests/test_v3_acceptance_local.py::test_v3_acceptance_leg_body_against_the_local_catalog`
  via `_acceptance_v3.assert_v3_lineage`, which now pins `V3_EXPECTED_INSERTED_ROW_ID = 11`
  exactly where it used to assert only a fresh-id floor.
- **Rationale** — FIXED. The engine's ordering rule is ascending partition value, spec-field
  order, nulls first, a stable sort over the already-written `Vec<DataFile>` — file-count work,
  not per-row work (1e6 rows across eight partitions: 2.810 / 2.850 / 2.875 s with the sort
  against 2.973 / 2.943 / 3.010 s without it). The cell this row owns is fixed. The rule is
  **not** Spark's rule, and where the two part company is the dated residual
  [V3-FILEORDER-1](#v3-fileorder-1--declared-v3-11-2026-09-02-same-commit-data-file-order-is-ascending-partition-value-not-sparks-hash-bucket-order).
  Pins: v3-11-row-id-determinism/C-003, C-004.

### V3-FILEORDER-1 — DECLARED (V3-11, 2026-09-02): same-commit data-file order is ascending partition value, not Spark's hash-bucket order

- **repark** — the engine hands one commit's data files to the manifest ordered by **ascending
  partition value**: spec-field order, a null slot before every non-null, primitive literals
  ascending, a stable sort so files sharing a partition keep the order their writer produced.
  It is `write/file_order.rs::ascending_partition_order`, applied once per commit to the
  already-written `Vec<DataFile>`. Because `first_row_id` is assigned in manifest-entry order,
  this order decides every derived `_row_id` in the commit.
- **Apache Spark** — orders the same files by the **`java.util.HashMap` bucket index** of the
  partition struct, which is not a value ordering at all. Decoded 2026-09-02 with
  `javap -p -c` over `iceberg-spark-runtime-4.1_2.13-1.11.0.jar`:

  | Step | Instruction |
  |---|---|
  | writer | `org.apache.iceberg.io.FanoutWriter.writers : Map<Integer, StructLikeMap<FileWriter>>`; `closeWriters()` walks `values()` |
  | map | `StructLikeMap.wrapperMap` is a `java.util.HashMap` keyed by `StructLikeWrapper` |
  | struct hash | `JavaHashes$StructLikeHash.hash`: `r = 97`; `r = 41*r + nFields`; per field `r = 41*r + fieldHash` |
  | 1-field spec | `H = 41*(41*97 + 1) + fieldHash = 163098 + fieldHash` |
  | int field | `JavaHash.forType` default arm, `Objects::hashCode` — the value itself |
  | string field | `JavaHashes.hashCode(CharSequence)`: `r = 177`; `r = 31*r + charAt(i)` |
  | bucket | `(H ^ (H >>> 16)) & (capacity - 1)`, capacity 16 while ≤ 12 distinct partitions |
  | collisions | keys sharing a bucket fall back to **insertion order**, so a colliding set is arrival-**dependent** |

- **Where they agree and where they part** — measured cell by cell on the pinned oracle
  (`local[1]`, `spark.sql.shuffle.partitions = 1`, one task, so no task-order term), each cell a
  single-commit partitioned v3 write, `id -> _row_id`:

  | Cell | Spark | repark |
  |---|---|---|
  | identity int `{0,1}` | ascending | same |
  | identity int `{0,1,2}` | ascending | same |
  | identity int `{0,1,2,3}` | ascending | same |
  | identity int `{0..4}` | file order `0,1,4,2,3` | ascending — **differs** |
  | identity int `{0..9}` | file order `8,9,6,7,0,1,4,5,2,3` | ascending — **differs** |
  | identity string `{a..e}` | file order `a,b,e,c,d` | ascending — **differs** |
  | two-field `(a,b)` over `(0,1),(0,0),(1,1),(1,0),(2,0)` | `1→2 2→4 3→1 4→0 5→3` | `1→1 2→0 3→3 4→2 5→4` — **differs** |
  | `truncate(1, part)` over `aa..ee` | `1→0 2→1 3→3 4→4 5→2` | `1→0 2→1 3→2 4→3 5→4` — **differs** |
  | `bucket(4, part)` over `0..7` | `1→0 2→1 3→2 4→5 5→4 6→6 7→3 8→7` | **identical** (buckets are `{0,1,2,3}`) |
  | `days(d)` over five consecutive dates | `1→2 2→3 3→0 4→1 5→4` | `1→0 2→1 3→2 4→3 5→4` — **differs** |
  | `{0, NULL, 1}` in that arrival order | `0, NULL, 1` | `NULL, 0, 1` — **differs** |
  | `{NULL, 0, 1}` | `NULL, 0, 1` | same |
  | `{1, NULL, 0}` | `NULL, 0, 1` | same |

  The last three rows are the collision caveat made concrete: a null slot and integer `0` hash to
  the same bucket (`fieldHash` 0 either way), so Spark's answer for that pair is decided by which
  arrived first. Spark's order is arrival-**independent** only while no two partitions collide.
  The engine's is arrival-independent always.
  *(oracle: live — PySpark 4.1.2 + `iceberg-spark-runtime-4.1_2.13:1.11.0`,
  `JAVA_HOME=/usr/lib/jvm/zulu-17-amd64`, Hadoop catalog, 2026-09-02.)*
- **Pin** — engine-behaviour pins, not Spark-parity pins:
  `crates/repark-spark/src/tests/v3_row_order.rs::a_null_partition_slot_is_numbered_first_whatever_order_it_arrives_in`
  (all three arrival orders), `::a_two_field_spec_orders_lexicographically_in_spec_field_order`,
  `::transform_partitions_order_by_the_transformed_value_ascending` (`truncate`, `bucket`,
  `days`). The two cells where the rules coincide are pinned against Spark instead —
  `::partitioned_ctas_numbers_files_ascending_by_partition_value`,
  `::mor_merge_across_three_partitions_numbers_files_ascending_by_partition_value` and their
  ANSI twins in `crates/repark-sql/src/v3/cow.rs`.
- **Rationale** — DECLARED, and deliberately not fixed. Reproducing Spark's order means
  reimplementing `java.util.HashMap` iteration — the seeded 41/97 struct hash, the `h ^ h>>>16`
  spread, the capacity ladder (the order changes again at the thirteenth partition), bucket
  treeification and the insertion-order fallback on collisions — inside a Rust writer, and then
  keeping it in step with a JDK internal. That is an unmaintainable anti-feature, and the value
  ordering is the one a reader can predict. The consequence is stated rather than hidden:
  **on a commit whose partition set is not a collision-free monotonic run, this engine's derived
  `_row_id` values differ from Spark's.** Row sets, `next-row-id`, the id **sets** and every
  sequence number are equal on both sides in every cell above. Revisiting this needs a new dated
  decision. Pins: v3-11-row-id-determinism/C-007.

### V3-UPGRADE-1 — FIXED (V3-10, 2026-09-02): `ALTER … format-version = '3'` upgrades v2 to v3 in place

- **repark** — with `repark.sql.allowCreateFormatVersion3 = true`, `ALTER TABLE … SET
  TBLPROPERTIES ('format-version' = '3')` (Spark door, facade) and `ALTER TABLE … SET
  PROPERTIES (format_version = 3)` (ANSI door) upgrade the table through the fork's
  `UpgradeFormatVersionAction`: metadata-only, **no new snapshot**, `next-row-id = 0`, and the
  reserved key is not persisted into the property map. Combined with another key it is ONE
  metadata commit; requesting the version the table already has writes no metadata file at
  all. Without the opt-in the ALTER refuses naming `repark.sql.allowCreateFormatVersion3`.
  A downgrade or an unsupported version refuses naming the key and both versions. After the
  upgrade the v3 paths are Spark-equal at a matched single-file layout: seed `(1,a),(2,b),(3,c)`
  reads `_row_id`/`seq` NULL, one 2-row append leaves `(1,2,1),(2,3,1),(3,4,1),(4,0,2),(5,1,2)`
  at next-row-id 5; COW `DELETE id=2` leaves `(1,0,2),(3,1,2),(4,2,2)` next-row-id 3 and the
  following `UPDATE id=3` leaves `(1,0,2),(3,1,3),(4,2,2)` next-row-id 6; merge-on-read MERGE
  `WHEN MATCHED THEN DELETE` writes ONE Puffin DV leaving `(1,0,1),(3,2,1),(4,3,1)` at
  next-row-id 4; `rewrite_data_files` over six single-row files leaves six distinct row ids
  0–5 at sequence 7, next-row-id 6, one data file; `register_table` of the upgraded table on a
  fresh catalog reads v3 with the same lineage.
- **Apache Spark** — identical on every cell above: the ALTER is metadata-only (snapshot count
  unchanged, `next-row-id` 0, the reserved key filtered out of `properties`), pre-upgrade rows
  read NULL lineage until a later v3 commit assigns `first_row_id` to their manifest, and the
  same append / COW / MoR / rewrite / adopt values follow.
  *(oracle: live — PySpark 4.1.2 + Iceberg 1.11.0, Hadoop catalog, `local[1]`–`local[2]`,
  `coalesce(1)`, 2026-09-02 V3-10 transcript)*.
- **Pin** — `crates/repark-spark/src/tests/v3_upgrade.rs` (twelve cells, Spark door),
  `crates/repark-sql/src/v3/create.rs::alter_set_properties_*` (ANSI door),
  `python/repark/tests/test_v3_upgrade.py` (facade),
  `python/repark/tests/test_v3_live_oracle.py::test_v3_upgrade_v2_to_v3_live_matches_spark`
  (live triple),
  `crates/repark-functions/src/format_version.rs` unit pins.
  Pins: v3-10-upgrade-v2-to-v3/C-002, C-003, C-004.
- **Also measured (V3-10, 2026-09-02).** A **v1** table upgrades straight to v3 behind the same
  opt-in: metadata-only, `next-row-id` 0, pre-upgrade rows NULL, and one 2-row append leaves
  `(1,2,0),(2,3,0),(3,4,0),(4,0,1),(5,1,1)` at next-row-id 5 — the v1 rows carry
  `_last_updated_sequence_number` 0 because v1 has no sequence numbers. A **partitioned** v2
  table (identity `part`, 2+1 rows) upgrades the same way and the append leaves next-row-id 5
  with the pre-upgrade rows on `{2,3,4}` at sequence 1 and the appended rows on `{0,1}` at
  sequence 2.
- **Residuals (dated 2026-09-02, `F-v3-10-partition-file-order` re-measured 2026-09-02 by
  V3-11).** On a **partitioned** table a plain `INSERT INTO` hands the per-partition files
  their `first_row_id` in a different order than Spark. Spark leaves `1→2, 2→3, 3→4, 4→0, 5→1`
  in 10 of 10 runs. The engine **flaps**, measured over ten runs of the identical cell: the
  seed manifest read Spark's `1→2, 2→3, 3→4` five times and `1→3, 2→4, 3→2` five times, the
  append manifest read Spark's `4→0, 5→1` four times and `4→1, 5→0` six times, and the two
  halves moved independently — both were Spark's on 3 of the 10 runs. Rows, `next-row-id`, the
  id **sets** and every sequence number are equal on every run, so the pin asserts the sets.
  The unpartitioned cell matches Spark exactly. **Owner: the fork.** `INSERT INTO` on a
  partitioned table is planned by `iceberg-datafusion`'s `IcebergTableProvider::insert_into` —
  `IcebergWriteExec` → `TaskWriter` → `FanoutWriter`, whose
  `partition_writers: HashMap<Struct, _>` is drained in Rust hash order at `close()` — and
  `IcebergCommitExec` commits those files without repark ever holding them, so the ordering
  V3-11 applied to the engine's own writers (CTAS, MERGE, append) cannot reach this path. The
  fork ask is **F-20**: `FanoutWriter::close` drains its partition map in ascending
  partition-value order. F-20 matches **RePark's** rule, not Spark's — Spark's order is the
  hash-bucket artefact decoded in
  [V3-FILEORDER-1](#v3-fileorder-1--declared-v3-11-2026-09-02-same-commit-data-file-order-is-ascending-partition-value-not-sparks-hash-bucket-order),
  which ascending cannot reproduce beyond four identity-int partitions; F-20 buys determinism
  and one rule across every writer this engine owns, not parity.
  `F-v3-10-eqdel-upgrade`: upgrading a table that
  carries **equality deletes** is unmeasured — the engine has no equality-delete write surface,
  so the cell could not be built from either door; the upgrade path itself is delete-file
  agnostic and the DV interaction is `V3-UPGRADE-DV-1`.
- **Rationale** — FIXED; the **owner ruling 2026-08-25** ("build it, behind
  `repark.sql.allowCreateFormatVersion3`, after V3-3") is discharged. Two residuals are filed
  as their own dated rows below.

### V3-UPGRADE-V4-1 — DECLARED (V3-10, 2026-09-02): `format-version = '4'` upgrades on Spark and refuses here

- **repark** — `'format-version' = '4'` (and any value above 3) refuses on all three doors,
  naming the key, the value and the current version: "this engine writes Iceberg format v1
  through v3, so a v2 table upgrades only to '3'". The table is left untouched. `'1'` on a v2
  table refuses as a downgrade; `'x'` and `''` refuse as unparsable.
- **Apache Spark** — Iceberg 1.11.0 **accepts** `'4'` on both `CREATE` and `ALTER`: the ALTER
  writes `"format-version": 4` with `next-row-id: 0` and no new snapshot, and the v4 table is
  then readable. `'1'` on a v2 table raises `Unsupported table change: Cannot downgrade v2
  table to v1`; `'x'` and `''` raise `Unsupported table change: For input string: "x"` / `""`.
  *(oracle: live — PySpark 4.1.2 + Iceberg 1.11.0, 2026-09-02 V3-10 transcript)*.
- **Pin** — `crates/repark-spark/src/tests/v3_upgrade.rs::alter_downgrade_and_unsupported_versions_refuse_naming_both_versions`,
  `crates/repark-sql/src/v3/create.rs::alter_set_properties_downgrade_and_unsupported_versions_refuse`,
  `python/repark/tests/test_v3_upgrade.py::test_alter_downgrade_and_unsupported_versions_refuse`.
  Pins: v3-10-upgrade-v2-to-v3/C-002, C-005.
- **Rationale** — DECLARED and dated. The owned fork's `FormatVersion` enum stops at `V3` and
  its spec support ends there; format v4 is still in development upstream. Refusing loudly is
  the honest answer — writing v4-labelled metadata this engine cannot read back would be the
  silent one. TRIGGER for lifting: a fork `FormatVersion::V4` with spec support behind it.

### V3-UPGRADE-DV-1 — FIXED (V3-12, 2026-09-02): a v3 DV write merges a legacy parquet position delete

- **repark** — on a table upgraded to v3 that still carries **v2 parquet position deletes**, the
  next merge-on-read write loads their positions back off the delete files, unions them into the
  new deletion vector, and passes the superseded delete files to `RowDelta::remove_deletes_many`
  in the SAME commit. A four-row v2 MoR table with one parquet position delete (id 2), upgraded
  and then MERGE-deleted at id 3, ends with ONE Puffin DV of `record_count = 2` referencing the
  data file, no parquet delete file, `next-row-id` 4, rows/lineage `(1,0,1),(4,3,1)`; a later
  one-row append leaves `(1,0,1),(4,3,1),(9,4,4)` at `next-row-id` 5. The UPDATE and
  `DELETE … WHERE id IN (SELECT …)` arms merge identically. **Two** live parquet deletes for one
  data file (this engine's v2 arm leaves two where Spark rewrites one) both merge and both leave
  in the one `RowDelta` — DV `record_count` 3. A data file this commit does NOT touch keeps its
  own legacy delete live. Copy-on-write writes no DV, so a legacy delete is left exactly as it was.
- **Apache Spark** — the same, in the same statements: Java `BaseDVFileWriter.loadPreviousDeletes`
  unions the previous positions and `RowDelta.removeDeletes` drops the superseded file. Two
  different tests govern the two halves, and conflating them is wrong. **APPLICABILITY governs
  the merge:** a live position delete's positions are merged into a touched data file's DV when
  it applies to that file — path-scoped and naming it, or partition-scoped and sharing its
  `(spec_id, partition)` — and its sequence number is at least the data file's. A partition-scoped
  delete carries NO `file_path` bounds and so names no file at all, yet Spark still merges it
  (`V3-UPGRADE-DV-PART-1` P2). **FILE SCOPE governs only REMOVAL:** only a delete that covers
  exactly one data file is dropped from the commit, because removing one that covers more would
  resurrect the rows it deletes in the files this commit did not touch.
  `referenced_data_file` is NULL on every Spark-written position delete, so file scope comes from
  equal `file_path` lower/upper bounds (`ContentFileUtil.isFileScoped`). At the default `write.delete.granularity = 'file'` a
  partitioned table with two data files in one partition gets one delete file per data file, and
  only the touched one is merged and removed, leaving the sibling's live. Summary counts on the merging commit: `added-dvs 1`, `added-position-deletes 2`,
  `added-delete-files 1`, `removed-delete-files 1`, `removed-position-delete-files 1`,
  `removed-position-deletes 1`.
  *(oracle: live — PySpark 4.1.2 + Iceberg 1.11.0, 2026-09-02 V3-12 transcript)*.
- **Pin** — `crates/repark-spark/src/tests/v3_legacy_delete.rs` (eleven cells: merge, append
  after, UPDATE, subquery DELETE, two legacy deletes, untouched sibling, copy-on-write, two
  refusals and two diverged-branch cells, plus one `#[ignore]`d measurement), `crates/repark-sql/src/v3/create.rs::upgraded_v3_merge_delete_merges_a_legacy_parquet_position_delete_into_the_dv`,
  `python/repark/tests/test_v3_legacy_delete_merge.py` (facade + a live Spark cell at matched layout).
  Pins: v3-12-legacy-delete-merge/C-002, C-003, C-004.
- **Rationale** — FIXED. The merge is engine-side and needs no fork change: the fork's
  `validate_fresh_dvs_only` already lets a DV through when the same commit removes the delete it
  would supersede. Two residuals stay dated DECLARED rows below.

### V3-UPGRADE-DV-PLAIN-1 — DECLARED (V3-12, 2026-09-02): the plain-`WHERE` MoR arm still refuses over a legacy delete

- **repark** — `DELETE FROM t WHERE id = 3` and `UPDATE t SET … WHERE id = 3` (a non-subquery
  predicate) do not reach repark's own merge-on-read commit path: only the `IN (SELECT …)` /
  `EXISTS` hole is claimed engine-side, so those spellings plan through the fork's own
  `IcebergDeleteExec`. Over a live parquet position delete it refuses before any IO: "deletion-vector:
  data file `…` is still covered by a Parquet position-delete file. Merging those positions into a
  new DV is not yet ported (Java `BaseDVFileWriter.loadPreviousDeletes` does it)". The table is
  left exactly as the upgrade left it and no Puffin is written.
- **Apache Spark** — merges, exactly as `V3-UPGRADE-DV-1` records; the statement spelling makes no
  difference to Spark. *(oracle: live — PySpark 4.1.2 + Iceberg 1.11.0, 2026-09-02 V3-12 transcript)*.
- **Pin** — `crates/repark-spark/src/tests/v3_legacy_delete.rs::a_plain_where_merge_on_read_delete_over_a_legacy_delete_still_refuses_loudly`,
  `crates/repark-sql/src/v3/create.rs::ansi_plain_where_mor_delete_over_a_legacy_parquet_delete_refuses_loudly`,
  `python/repark/tests/test_v3_legacy_delete_merge.py::test_plain_where_mor_delete_over_a_legacy_parquet_delete_refuses_loudly`.
  Pins: v3-12-legacy-delete-merge/C-003, C-004.
- **Rationale** — DECLARED and dated; a loud refusal, never a silent supersede. The refusal lives in
  the pinned fork's delete exec, and the fork pin is fixed for this unit. TRIGGER for lifting:
  either a fork `write_deletion_vectors` that merges previous deletes, or the engine widening its
  predicate-DML hole so a plain-`WHERE` MoR statement commits through `prepare_row_delta_deletes`.

### V3-UPGRADE-DV-PART-1 — DECLARED (V3-12, 2026-09-02): a position delete covering two data files refuses; Spark commits

- **repark** — a position delete whose `file_path` bounds are absent or unequal covers more than
  one data file (this engine and Spark both write one under
  `write.delete.granularity = 'partition'`). The fork's commit door refuses: "Cannot commit
  deletion vector for `…`: live position delete file `…` still applies to that data file and
  would be silently superseded by the DV at read time". The table is left untouched and no
  Puffin is written.
- **Apache Spark** — **commits.** Measured at the matched layout: a v2 partitioned merge-on-read
  table at `write.delete.granularity = 'partition'` with two data files in `part = 7`;
  `DELETE … WHERE id IN (1, 3)` writes ONE parquet delete file (`record_count` 2,
  `referenced_data_file` NULL, `file_path` bounds **absent** from the manifest), upgraded to v3;
  `MERGE … WHEN MATCHED THEN DELETE` at id 2 (which touches the first data file only) commits to
  `.delete_files` = [PARQUET `record_count` 2 **still live**, PUFFIN `record_count` 2 referencing
  the touched data file], summary `added-dvs 1` / `added-position-deletes 2` /
  `added-delete-files 1` / `total-delete-files 2` / `total-position-deletes 4` and **no**
  `removed-delete-files`; rows `[(4,'d',7)]`, `next-row-id` 4, lineage `[(4,1,2)]`. A following
  append leaves `[(4,1,2),(9,4,5)]` at `next-row-id` 5. A second `MERGE … DELETE` at id 4, on the
  **other** data file, likewise commits a second DV and the parquet delete is **still live** —
  Spark never removes it, even once every data file it covers carries a DV.
  *(oracle: live — PySpark 4.1.2 + Iceberg 1.11.0, 2026-09-02 V3-12 transcript.)*
- **Pin** — `crates/repark-spark/src/tests/v3_legacy_delete.rs::a_partition_scoped_legacy_delete_still_refuses_loudly`,
  `crates/repark-sql/src/v3/create.rs::ansi_partition_scoped_legacy_delete_refuses_loudly`,
  `python/repark/tests/test_v3_legacy_delete_merge.py::test_partition_scoped_legacy_delete_refuses_loudly`.
  Pins: v3-12-legacy-delete-merge/C-002, C-003, C-004.
- **Rationale** — DECLARED and dated; a loud refusal, never a silent supersede. The merge itself
  is engine-side and cheap (the per-row `file_path` filter already reads only the touched file's
  positions), but the **commit** is not reachable at the pinned fork: `validate_fresh_dvs_only`
  admits an added DV over a live position delete only when the same commit **removes** that
  delete, and removing this one is measurably wrong — it deletes the rows the untouched sibling
  data file still needs deleted, resurrecting id 3 in the cell above. Merging without removing
  therefore cannot be expressed, and the engine refuses rather than corrupt. TRIGGER for lifting:
  a fork `validate_fresh_dvs_only` that admits a non-file-scoped position delete whose positions
  the committed DV provably carries (or a fork port of `BaseDVFileWriter.loadPreviousDeletes`
  that owns the merge). **Correction (2026-09-02):** the row this replaces called the shape
  UNMEASURED and cited Java's `validatePreviousDeletes` as consistent with refusing. Both were
  wrong — the cell is measurable from Spark SQL and Spark commits it.

### V3-DV-BRANCH-1 — FIXED (V3-12, 2026-09-02): a second merge-on-read DELETE on a diverged branch merged the wrong snapshot's deletion vectors

- **repark** — the v3 deletion-vector container close now runs against the snapshot the statement
  SCANNED, which for a `to_branch` write is the branch head, not `main`. Before the fix it always
  closed against the current snapshot: a second merge-on-read DELETE on a branch whose head had
  moved past `main` found no existing DV, wrote a FRESH one, and the fork's commit door then
  refused with "the current snapshot already carries a live deletion vector for that data file",
  leaving the branch un-writable after its first DV. A v3 MoR table, `CREATE BRANCH b`, then two
  `MERGE … WHEN MATCHED THEN DELETE` statements on `t.branch_b` now leave ONE branch DV of
  `record_count` 2, the branch reading `[1, 4]`, and `main` unmoved at `[1, 2, 3, 4]`.
- **Apache Spark** — commits both statements; the second merges into the branch's own DV. The
  branch reads `[1, 4]` and `main` stays at `[1, 2, 3, 4]` on distinct snapshot ids, with the
  branch's superseded 1-position DV replaced by a 2-position one.
  *(oracle: live — PySpark 4.1.2 + Iceberg 1.11.0, 2026-09-02; the branch cell is recorded in the
  V3-12 ledger's dated errata note, not the original transcript.)*
- **Pin** — `crates/repark-spark/src/tests/v3_legacy_delete.rs::a_second_merge_on_read_delete_on_a_diverged_branch_merges_the_branch_only_dv`
  and `::a_legacy_parquet_delete_that_exists_only_on_a_branch_merges_on_that_branch`.
  Pins: v3-12-legacy-delete-merge/C-006.
- **Rationale** — FIXED. Latent since RP-7 gave the close its partition map: the close took a
  `snapshot_id` the engine always passed as `None`. `prepare_row_delta_deletes` now takes the
  `snapshot_id` that `commit_target::snapshot_id_for_commit` already resolved for
  `validate_from_snapshot` and the target scan, so the scan, the legacy-delete collection, the
  container close and the commit validation all read ONE snapshot.

### BL-9 — a double-quoted string literal is an identifier on the SQL door

- **repark** — the Spark SQL door reads `"abc"` as a double-quoted **identifier**, so
  `spark.sql('SELECT "abc"')` fails with `No field named abc`, and `"a\"b"` never becomes the
  string `a"b`. SQP-1 canonicalises **single**-quoted literals only; double-quoted text is left
  exactly as written.
- **Apache Spark** — `"abc"` is a STRING literal (`spark.sql.ansi.doubleQuotedIdentifiers` is off
  by default): `SELECT "abc"` is `abc`, `length("a\nb")` is 3, `"it\'s"` is `it's`.
  *(oracle: `<pyspark-4.1.2-oracle>` — E17 / E18 / U16.)*
- **Pin** — `python/repark/tests/test_sqp_1_string_literals.py::test_double_quoted_literal_is_an_identifier`
- **Rationale** — BACKLOG, intent to FIX with **FNP-4b**. The fix is to wire the Spark parser
  dialect so `"…"` lexes as a STRING, which cannot land until repark's own internally-generated
  SQL stops quoting identifiers with ANSI double quotes (`extension.rs::apply_spark_parser_dialect`
  is measured-blocked on exactly this). Not this unit's — SQP-1 touches the single-quoted lexer at
  the front door, and a double-quoted change belongs to the write path that owns the internal SQL.

### BL-10 — `spark.sql.parser.escapedStringLiterals=true` has no carrier

- **repark** — there is no builder or runtime carrier for the flag, so every session processes
  escapes (the `false` behavior SQP-1 implements): `spark.sql("SELECT '\\d'")` is `d`, always.
- **Apache Spark** — with `escapedStringLiterals=true`, the lexer keeps the backslash verbatim:
  `'\d'` is `\d` (length 2) and `'\''` is `\'`. The default is `false`, which SQP-1 matches.
  *(oracle: `<pyspark-4.1.2-oracle>` — E20 / E21.)*
- **Pin** — `python/repark/tests/test_sqp_1_string_literals.py::test_escaped_string_literals_flag_has_no_carrier`
- **Rationale** — BACKLOG. The default (`false`) is the migrated-job default and the only measured
  contract SQP-1 was scoped to; the `true` legacy mode needs a config carrier and a second lexer
  path. Recorded so the carrier lands with its behavior rather than as a silent surprise.

### BL-11 — numeric → `BINARY` under `spark.sql.ansi.enabled=false` refuses rather than encodes

- **repark** — `CAST(1 AS BINARY)` refuses with `DATATYPE_MISMATCH` in every mode (SQP-1 / C-009);
  there is no ANSI-off big-endian encoding path.
- **Apache Spark** — under `spark.sql.ansi.enabled=false`, `CAST(1 AS BINARY)` is the value's
  big-endian bytes: `hex(CAST(1 AS BINARY))` is `00000001` (4 bytes). Under ANSI on (the default,
  and repark's) Spark refuses the same cast — which repark matches.
  *(oracle: `<pyspark-4.1.2-oracle>` — B11.)*
- **Pin** — `python/repark/tests/test_sqp_1_string_literals.py::test_numeric_to_binary_refuses`
- **Rationale** — BACKLOG, fail-loud direction. The refuse is safe (a loud stop, never a wrong
  answer), the ANSI-off default is not repark's, and the big-endian encoding is a narrow legacy
  path. Recorded so the encoding lands behind an ANSI-off carrier with its own pin.

### BL-12 — an out-of-range `\U` escape becomes one `?` where Spark emits a 2-char Java artifact

- **repark** — a `\UXXXXXXXX` escape whose value is not a Unicode scalar (past `U+10FFFF`) becomes
  a single `?` (`UNREPRESENTABLE`, U+003F): `spark.sql("SELECT '\U00110000'")` is one character and
  `length('\U00110000')` = 1. The single home of the rule is `push_code_point` in
  `crates/repark-spark/src/spark_literals.rs`; SQP-1 chose `?` so the result stays sane and
  single-homed rather than reproducing a Java `char[]` artifact. The in-scope valid-scalar `\U`
  (U5) and the lone-surrogate → `?` case (`hex('\ud83d')` = `3F`) already match Spark.
- **Apache Spark** — Spark keeps the raw code units and its Java UTF-8 encoder replaces each
  unpaired/oversized unit with `?`, so an out-of-range `\U` yields **two** characters:
  `length('\U00110000')` = 2 and `hex('\U00110000')` = `3F3F` (and `hex('\UFFFFFFFF')` = `ED9EBF3F`,
  a longer artifact). *(oracle: `<pyspark-4.1.2-oracle>`.)*
- **Pin** — `python/repark/tests/test_sqp_1_string_literals.py::test_out_of_range_unicode_escape_is_one_replacement`
- **Rationale** — BACKLOG, cosmetic-artifact direction. An out-of-range `\U` is a malformed escape
  a migrated job effectively never writes; both engines produce a replacement, and repark's single
  `?` is a saner, single-homed choice than a 2-char Java artifact. Recorded so the exact artifact
  lands with its own pin if a job ever depends on it. This is the single home of the divergence
  the `spark_literals` module doc previously only mentioned.

### BL-13 — `try_avg(INTERVAL)` refuses pending FNP-11

- **repark** — `try_avg(INTERVAL 1 DAY)` is a plan error naming `[FNP-11]` and the date
  2026-08-31. The function is registered (not an absent name) and the refuse is loud. `avg`
  of an interval stays the pre-existing Decimal/Float64 signature miss. DATE/TIMESTAMP ±
  INTERVAL and INTERVAL / numeric on `try_add` / `try_divide` compute.
- **Apache Spark** — `try_avg(INTERVAL)` returns interval day to second. A huge interval
  overflows `INTERVAL_ARITHMETIC_OVERFLOW` on both `avg` and `try_avg` (2026-08-31 oracle
  4.1.2; `try_avg` is not NULL-on-interval-overflow).
  *(oracle: live PySpark 4.1.2, 2026-08-31.)*
- **Pin** — `python/repark/tests/test_fnp7_try_inversions.py::test_try_avg_interval_refuses_fnp11`
- **Rationale** — BACKLOG, intent to FIX with **FNP-11**. Averaging intervals is temporal
  entanglement (month vs day-time fields, overflow class), not a try_* inversion. Silent NULL
  is not acceptable; the dated message is the holding contract until FNP-11 lands.

### BL-14 — `DATE + INTERVAL 0 HOUR` stays date (a zero sub-day interval loses its unit)

- **repark** — `try_add(DATE '2024-01-01', INTERVAL 0 HOUR)` returns Date32 `2024-01-01`. The
  FNP-7 promotion rule inspects the MonthDayNano value (nanos ≠ 0 promotes to timestamp), and
  `INTERVAL 0 HOUR` arrives as `{0,0,0}` — indistinguishable from `0 DAY` — so the sub-day
  unit is lost before the kernel can see it.
- **Apache Spark** — types the same cell `timestamp` (midnight `2024-01-01 00:00:00`): the
  literal's HOUR unit forces the promotion even at zero. The calendar day is equal on both
  engines; the divergence is the result type and a midnight clock, never a wrong instant.
  *(oracle: live PySpark 4.1.2, 2026-08-31.)*
- **Pin** — `python/repark/tests/test_fnp7_try_inversions.py::test_try_add_date_plus_zero_hour_stays_date_bl14`
  (asserts the current Date32 behavior so a silent change is loud).
- **Rationale** — BACKLOG, filed 2026-08-31 from the FNP-7 final Critic pass (finding L-008).
  A fix needs the literal's unit to survive planning — upstream of the MonthDayNano
  representation — which is out of proportion for a `try_*` unit. Recorded so the promotion
  lands with its own pin when the unit information is retained.

### BL-15 — `expm1` composes `exp(x) - 1`, losing the tiny-`x` precision the name exists for

> **FIXED (2026-09-02, LOG1P-1).** `F.expm1` / SQL `expm1` call `f64::exp_m1` (NULL-propagating,
> numeric coerce to Float64) on both SQL doors and the facade. `expm1(1e-16)` is `1e-16`;
> `expm1(1e-08)` is `math.expm1(1e-08)`, not `exp(x)-1`. Sibling `log1p` is the matching
> `f64::ln_1p` kernel (Spark SQL NULLs `x <= -1`; tiny-arg `log1p(1e-16)` is `1e-16`). Kernel:
> `crates/repark-functions/src/spark_log1p.rs`. Pins:
> `python/repark/tests/test_bl15_bl16_math_divergences.py::test_bl15_expm1_matches_spark_precise_kernel`,
> `python/repark/tests/test_log1p_1.py`. Oracle: live PySpark 4.1.2, 2026-09-02.

### BL-16 — `hypot` squares before the root, overflowing where Spark rescales

- **repark** — `F.hypot(1e200, 1e200)` → `inf` (the naive `sqrt(a*a + b*b)` overflows at
  `a*a`). Ordinary-magnitude behavior is exact.
- **Apache Spark** — `java.lang.Math.hypot` rescales: `1.4142135623730951e+200`.
  *(measured 2026-09-01, EX-2 pilot; the example demonstrates ordinary input only.)*
- **Pin** — `python/repark/tests/test_bl15_bl16_math_divergences.py::test_bl16_hypot_overflows_to_inf_today`.
- **Rationale** — BACKLOG, filed 2026-09-01. Same FNP numerics unit as BL-15; overflow-safe
  hypot is a standard rescale.

### WIN-SLIDE — non-retractable aggregates over a sliding frame (W-0, 2026-08-31)

Spark evaluates an aggregate over `ROWS BETWEEN n PRECEDING AND CURRENT ROW` even when the
aggregate has no inverse (it re-scans the frame). DataFusion 54.1 refuses at execution:
`Aggregate can not be used as a sliding accumulator because retract_batch is not implemented`.
W-0 measured the Spark 4.1.2 built-in aggregate roster; names that do not plan at all are
**absent** (not these rows). Names that plan and then refuse are the thirteen headings below.
`approx_count_distinct` is probed on int64; on Float64 it fails earlier with a type gap.
W-1 picks the fallback (Spark re-scan vs segment tree). *(oracle: live RePark probe, 2026-08-31;
Spark half is documented SlidingWindowFunctionFrame plus the W-0 PySpark 4.1.2 cell.)*

Shared pin for every heading:
`python/repark/tests/test_w0_window_bench_smoke.py::test_sliding_refuse_set_matches_the_frozen_roster`
and `python/repark-parity/tests/test_w0_window_bench.py::test_registry_has_a_heading_per_sliding_refuse`.

### WIN-SLIDE-approx_count_distinct — `approx_count_distinct` over a sliding frame refuses

- **repark** — `approx_count_distinct(vi)` over `ORDER BY id ROWS BETWEEN 10 PRECEDING AND CURRENT ROW` plans, then raises the sliding-accumulator `retract_batch` refusal. On Float64 the same name fails earlier (`approx_distinct` not implemented for that type) and is not this row.
- **Apache Spark** — accepts the aggregate as a window function and re-scans the frame. *(oracle: documented.)*
- **Pin** — `python/repark/tests/test_w0_window_bench_smoke.py::test_sliding_refuse_set_matches_the_frozen_roster`
- **Rationale** — BACKLOG. W-1.

### WIN-SLIDE-approx_percentile — `approx_percentile` over a sliding frame refuses

- **repark** — `approx_percentile(v, 0.5) OVER (ORDER BY id ROWS BETWEEN 10 PRECEDING AND CURRENT ROW)` raises the sliding-accumulator `retract_batch` refusal.
- **Apache Spark** — accepts the aggregate as a window function and re-scans the frame. *(oracle: documented.)*
- **Pin** — `python/repark/tests/test_w0_window_bench_smoke.py::test_sliding_refuse_set_matches_the_frozen_roster`
- **Rationale** — BACKLOG. Functional parity gap, not a perf gap. W-1.

### WIN-SLIDE-bit_and — `bit_and` over a sliding frame refuses

- **repark** — `bit_and(vi)` over the same sliding frame raises the sliding-accumulator refusal.
- **Apache Spark** — accepts the aggregate as a window function and re-scans the frame. *(oracle: documented.)*
- **Pin** — `python/repark/tests/test_w0_window_bench_smoke.py::test_sliding_refuse_set_matches_the_frozen_roster`
- **Rationale** — BACKLOG. W-1.

### WIN-SLIDE-bit_or — `bit_or` over a sliding frame refuses

- **repark** — `bit_or(vi)` over the same sliding frame raises the sliding-accumulator refusal.
- **Apache Spark** — accepts the aggregate as a window function and re-scans the frame. *(oracle: documented.)*
- **Pin** — `python/repark/tests/test_w0_window_bench_smoke.py::test_sliding_refuse_set_matches_the_frozen_roster`
- **Rationale** — BACKLOG. W-1.

### WIN-SLIDE-bool_and — `bool_and` over a sliding frame refuses

- **repark** — `bool_and(vi <> 0)` over the same sliding frame raises the sliding-accumulator refusal.
- **Apache Spark** — accepts the aggregate as a window function and re-scans the frame. *(oracle: documented.)*
- **Pin** — `python/repark/tests/test_w0_window_bench_smoke.py::test_sliding_refuse_set_matches_the_frozen_roster`
- **Rationale** — BACKLOG. W-1.

### WIN-SLIDE-bool_or — `bool_or` over a sliding frame refuses

- **repark** — `bool_or(vi <> 0)` over the same sliding frame raises the sliding-accumulator refusal.
- **Apache Spark** — accepts the aggregate as a window function and re-scans the frame. *(oracle: documented.)*
- **Pin** — `python/repark/tests/test_w0_window_bench_smoke.py::test_sliding_refuse_set_matches_the_frozen_roster`
- **Rationale** — BACKLOG. W-1.

### WIN-SLIDE-collect_list — `collect_list` over a sliding frame refuses

- **repark** — `collect_list(v)` over the same sliding frame raises the sliding-accumulator refusal.
- **Apache Spark** — accepts the aggregate as a window function and re-scans the frame. *(oracle: documented.)*
- **Pin** — `python/repark/tests/test_w0_window_bench_smoke.py::test_sliding_refuse_set_matches_the_frozen_roster`
- **Rationale** — BACKLOG. W-1. The intake named this class first.

### WIN-SLIDE-collect_set — `collect_set` over a sliding frame refuses

- **repark** — `collect_set(v)` over the same sliding frame raises the sliding-accumulator refusal.
- **Apache Spark** — accepts the aggregate as a window function and re-scans the frame. *(oracle: documented.)*
- **Pin** — `python/repark/tests/test_w0_window_bench_smoke.py::test_sliding_refuse_set_matches_the_frozen_roster`
- **Rationale** — BACKLOG. W-1.

### WIN-SLIDE-corr — `corr` over a sliding frame refuses

- **repark** — `corr(v, v2)` over the same sliding frame raises the sliding-accumulator refusal.
- **Apache Spark** — accepts the aggregate as a window function and re-scans the frame. *(oracle: documented.)*
- **Pin** — `python/repark/tests/test_w0_window_bench_smoke.py::test_sliding_refuse_set_matches_the_frozen_roster`
- **Rationale** — BACKLOG. W-1.

### WIN-SLIDE-covar_pop — `covar_pop` over a sliding frame refuses

- **repark** — `covar_pop(v, v2)` over the same sliding frame raises the sliding-accumulator refusal.
- **Apache Spark** — accepts the aggregate as a window function and re-scans the frame. *(oracle: documented.)*
- **Pin** — `python/repark/tests/test_w0_window_bench_smoke.py::test_sliding_refuse_set_matches_the_frozen_roster`
- **Rationale** — BACKLOG. W-1.

### WIN-SLIDE-covar_samp — `covar_samp` over a sliding frame refuses

- **repark** — `covar_samp(v, v2)` over the same sliding frame raises the sliding-accumulator refusal.
- **Apache Spark** — accepts the aggregate as a window function and re-scans the frame. *(oracle: documented.)*
- **Pin** — `python/repark/tests/test_w0_window_bench_smoke.py::test_sliding_refuse_set_matches_the_frozen_roster`
- **Rationale** — BACKLOG. W-1.

### WIN-SLIDE-percentile_approx — `percentile_approx` over a sliding frame refuses

- **repark** — `percentile_approx(v, 0.5)` over the same sliding frame raises the sliding-accumulator refusal.
- **Apache Spark** — accepts the aggregate as a window function and re-scans the frame. *(oracle: documented.)*
- **Pin** — `python/repark/tests/test_w0_window_bench_smoke.py::test_sliding_refuse_set_matches_the_frozen_roster`
- **Rationale** — BACKLOG. W-1. The intake named this class.

### WIN-SLIDE-try_sum — `try_sum` over a sliding frame refuses

- **repark** — `try_sum(v)` over the same sliding frame raises the sliding-accumulator refusal (group `try_sum` plans; sliding does not).
- **Apache Spark** — accepts the aggregate as a window function and re-scans the frame. *(oracle: documented.)*
- **Pin** — `python/repark/tests/test_w0_window_bench_smoke.py::test_sliding_refuse_set_matches_the_frozen_roster`
- **Rationale** — BACKLOG. W-1.

### Surfaced, awaiting pins — not yet rows

Candidates that carry **no pin yet**, so under §6 they are not admitted as rows; they are queued
here so the surfacing is on the record, and each becomes a
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

- **V3-DANGLE-1** — **FIXED (V3-5, 2026-08-31).** See the row above. RP-2 took the v2
  F-3 `'remove-dangling-deletes'` half; v3 file-scoped DV drop is this row.

- **V3-ROWID-1** — **FIXED (V3-4, 2026-08-31).** `_row_id` and
  `_last_updated_sequence_number` are served on **single-table** v3 reads, Spark-equal
  (nullable int64; stored value else `first_row_id +` position / file sequence; `SELECT *`
  hides them; unquoted identifiers fold). v1/v2 raise the engine Schema error
  `No field named _row_id` (Spark raises `UNRESOLVED_COLUMN.WITH_SUGGESTION` / SQLSTATE
  `42703`; mapping Spark's class is residual). Preserve across plain-`WHERE` UPDATE /
  DELETE / MERGE and subquery-`WHERE` DELETE / UPDATE is Spark-equal (RP-6 / V3-7 / V3-8;
  `V3-COW-1` FIXED). Pins:
  `crates/repark-spark/src/tests/v3_lineage.rs`,
  `crates/repark-sql/src/v3/partitioned_equality_deletes.rs` ANSI lineage tests,
  `python/repark/tests/test_v3_lineage_columns.py`.

- **V3-ROWID-2** — **DECLARED (V3-4, 2026-08-31).** Lineage projection over joins, CTEs,
  subqueries, and `VERSION AS OF` / time-travel refuses loud:
  `[V3-ROWID-2] lineage projection over {joins|CTEs|subqueries|time-travel} is not yet
  served; single-table reads are`. Spark serves those forms. Follow-up: snapshot-pinned
  lineage scan (`table.scan().snapshot_id`) for time-travel; join/CTE serving. The
  unused `try_new_with_snapshot` constructor was removed rather than left as an unwired
  promise. Pins: join / CTE / subquery / `VERSION AS OF` tests in the three V3-ROWID-1
  files.

- **V3-COW-1** — measured 2026-08-24, admitted as a BACKLOG row (see §7), and **FIXED
  (V3-8, 2026-09-02)** in place there. Left this queue. Its merge-on-read residual is
  **V3-MOR-1**, FIXED (V3-9, 2026-09-02).

- **V3-DV-1** — filed BACKLOG (V3-9, 2026-09-02) and **FIXED (RP-7, 2026-09-02)** in place in
  its row above: the fork repin to `ff4764d3` (F-18) makes the shared-Puffin close Spark-equal —
  only the touched blob is rewritten, two containers, the sibling entry byte-identical.

- **V3-ROWID-3** — measured 2026-09-02 (LIVE-v3) and **FIXED (V3-11, 2026-09-02)** in its
  §7 row: one commit's data files are ordered by ascending partition value, so the
  merge-on-read MERGE insert reads Spark's `_row_id = 11` in 10 of 10 runs. Left this queue.
  Its residual is **`F-v3-10-partition-file-order`**, below.

- **`F-v3-11-rewrite-row-order`** — **DECLARED (V3-11, 2026-09-02).** `rewrite_data_files`
  over six single-row files of an upgraded v3 table assigns the six ids `0..5` at sequence 7 on
  both engines, but the id→`_row_id` map is **nondeterministic on Spark itself**: ten runs (two
  JVMs × five fresh Hadoop warehouses, PySpark 4.1.2 + Iceberg 1.11.0) produced **ten distinct
  maps** — `[2,3,5,1,4,0] [4,1,3,5,0,2] [1,3,2,4,5,0] [3,0,1,2,4,5] [5,0,4,1,3,2] [5,2,1,4,0,3]
  [0,4,5,1,3,2] [4,3,1,2,0,5] [2,1,5,0,4,3] [2,5,0,3,1,4]`. This engine is nondeterministic too
  (ten runs, six distinct maps, `[5,4,3,2,1,0]` four times). There is therefore **no value to
  pin**: `v3_upgrade.rs::rewrite_data_files_after_an_engine_upgrade_assigns_lineage_like_spark`
  keeps asserting the id set and the sequence number, which are equal and stable on both sides.
  The compaction row order is the fork's `rewrite_data_files` scan order, not repark's.

- **`F-v3-10-partition-file-order`** — **DECLARED (V3-10, 2026-09-02; re-measured V3-11).**
  See the residual under `V3-UPGRADE-1` in §4. Owner: fork ask **F-20** — `iceberg-datafusion`'s
  `TaskWriter` closes its `FanoutWriter` in Rust `HashMap` order, which repark cannot reorder;
  F-20 asks the fork to drain it in ascending partition-value order, RePark's rule.

- **`V3-FILEORDER-1`** — **DECLARED (V3-11, 2026-09-02).** See the row above. This engine orders
  one commit's data files by ascending partition value; Spark orders them by Java `HashMap`
  bucket index. The two agree only on collision-free monotonic partition sets, so derived
  `_row_id` values differ on wider sets. Not to be fixed: replicating a JDK map's iteration
  order is an unmaintainable anti-feature.

- **S3T-V3-1** — measured and **FIXED (LIVE-v3-M, 2026-09-02)** in its §4 row: both live v3
  acceptance legs are green and S3 Tables accepts `format-version = 3` at CREATE. Left this
  queue.

- **V3-VARIANT-SHRED-1** — landed as a §4 row (2026-09-01, V3-6): shredded-Parquet `variant`
  stays **DECLARED out of the v1.0 gate (owner ruling 2026-08-25)**; binary variant is
  measured refusing end to end at the fork. See the row in §4 for the pins and the R88
  filing.

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

## 9. Declared-absent Spark functions (FNP-15 / FNP-16)

These names are exported and refuse. FNP-15 names are **unreachable** in a no-JVM engine.
FNP-16 families are **reachable without a JVM and deferred by cost**. The two claims are not
the same. Each row is DECLARED. All four FNP-16 family sections (sketches, CSV/XML/XPath,
VARIANT, geospatial) have landed.

Oracle basis for this section: *documented* — Spark 4.1.2 `pyspark.sql.functions` exports
the name; the divergence is that repark refuses the call Spark would evaluate. No value
oracle is involved.

### FNP-15-java_method — JVM class-load reflection is unreachable

- **repark** — `F.java_method`, Spark SQL `java_method(...)`, and ANSI SQL `java_method(...)`
  raise `UnsupportedOperationException` / `NotImplemented` stating the name is **unreachable**:
  it loads a Java class by name and invokes a static method by reflection, which needs a live
  JVM. repark has no JVM.
- **Apache Spark** — loads the named class and invokes the static method.
  *(oracle: documented — Spark `CallMethodViaReflection` / `java_method`.)*
- **Pin** — `python/repark/tests/test_fnp15_16_declared_refuse.py::test_facade_attribute_refuses_with_registry_reason[java_method]`,
  `…::test_spark_sql_door_refuses[java_method]`,
  `…::test_ansi_sql_door_refuses[java_method]`;
  `crates/repark-spark/src/tests/declared_refuse.rs::java_method_refuses`.
- **Rationale** — DECLARED unreachable. Register, do not build.

### FNP-15-reflect — CallMethodViaReflection is unreachable

- **repark** — `reflect` is the other spelling of `java_method` and refuses as **unreachable**
  (`CallMethodViaReflection`).
- **Apache Spark** — same JVM reflection as `java_method`.
  *(oracle: documented.)*
- **Pin** — `python/repark/tests/test_fnp15_16_declared_refuse.py::test_facade_attribute_refuses_with_registry_reason[reflect]`,
  `…::test_spark_sql_door_refuses[reflect]`,
  `…::test_ansi_sql_door_refuses[reflect]`;
  `crates/repark-spark/src/tests/declared_refuse.rs::reflect_refuses`.
- **Rationale** — DECLARED unreachable. Register, do not build.

### FNP-15-try_reflect — exception-to-NULL reflection is unreachable

- **repark** — `try_reflect` is `reflect` with exception-to-NULL and still **unreachable**;
  it needs a live JVM.
- **Apache Spark** — JVM reflection; exceptions become NULL.
  *(oracle: documented.)*
- **Pin** — `python/repark/tests/test_fnp15_16_declared_refuse.py::test_facade_attribute_refuses_with_registry_reason[try_reflect]`,
  `…::test_spark_sql_door_refuses[try_reflect]`,
  `…::test_ansi_sql_door_refuses[try_reflect]`;
  `crates/repark-spark/src/tests/declared_refuse.rs::try_reflect_refuses`.
- **Rationale** — DECLARED unreachable. Register, do not build.

### FNP-15-unwrap_udt — Spark UDT unwrap is unreachable

- **repark** — `unwrap_udt` is **unreachable**: Spark `UserDefinedType` unwrap walks the JVM
  UDT registry; with no JVM there is no UDT system to unwrap from.
- **Apache Spark** — unwraps a `UserDefinedType` to its SQL type.
  *(oracle: documented.)*
- **Pin** — `python/repark/tests/test_fnp15_16_declared_refuse.py::test_facade_attribute_refuses_with_registry_reason[unwrap_udt]`,
  `…::test_spark_sql_door_refuses[unwrap_udt]`,
  `…::test_ansi_sql_door_refuses[unwrap_udt]`;
  `crates/repark-spark/src/tests/declared_refuse.rs::unwrap_udt_refuses`.
- **Rationale** — DECLARED unreachable. Register, do not build.

### FNP-15-input_file_block_start — InputFileBlockHolder start is unreachable

- **repark** — `input_file_block_start` is **unreachable**: it reads Spark's
  `InputFileBlockHolder` thread-local, populated by `HadoopRDD`/`FileScanRDD` as a split is
  handed to a task. DataFusion has no equivalent surface, and repark's `input_file_name` is
  itself still a stub.
- **Apache Spark** — returns the start offset of the current file split.
  *(oracle: documented.)*
- **Pin** — `python/repark/tests/test_fnp15_16_declared_refuse.py::test_facade_attribute_refuses_with_registry_reason[input_file_block_start]`,
  `…::test_spark_sql_door_refuses[input_file_block_start]`,
  `…::test_ansi_sql_door_refuses[input_file_block_start]`;
  `crates/repark-spark/src/tests/declared_refuse.rs::input_file_block_start_refuses`.
- **Rationale** — DECLARED unreachable until `input_file_name` is destubbed. Register, do not
  invent a different mechanism here.

### FNP-15-input_file_block_length — InputFileBlockHolder length is unreachable

- **repark** — `input_file_block_length` is **unreachable** by the same `InputFileBlockHolder`
  thread-local mechanism as `input_file_block_start`. DataFusion has no equivalent surface, and
  repark's `input_file_name` is itself still a stub.
- **Apache Spark** — returns the length of the current file split.
  *(oracle: documented.)*
- **Pin** — `python/repark/tests/test_fnp15_16_declared_refuse.py::test_facade_attribute_refuses_with_registry_reason[input_file_block_length]`,
  `…::test_spark_sql_door_refuses[input_file_block_length]`,
  `…::test_ansi_sql_door_refuses[input_file_block_length]`;
  `crates/repark-spark/src/tests/declared_refuse.rs::input_file_block_length_refuses`.
- **Rationale** — DECLARED unreachable until `input_file_name` is destubbed. Register, do not
  invent a different mechanism here.

### FNP-16-sketches — HLL / theta / KLL are reachable, deferred by cost

- **repark** — the 32 sketch names (`hll_*` 4, `theta_*` 7, `kll_*` 21) are exported and refuse
  as **reachable without a JVM and deferred by cost**. Spark sketch columns are Apache
  DataSketches binary blobs. DataFusion's internal `hyperloglog.rs` is a different format and
  cannot serve the blob even for the HLL subset.
- **Apache Spark** — evaluates the DataSketches-backed aggregates and scalars.
  *(oracle: documented — Spark 4.1.2 `pyspark.sql.functions` sketch family.)*
- **Pin** — `python/repark/tests/test_fnp15_16_declared_refuse.py::test_sketch_facade_refuses_deferred_by_cost`,
  `…::test_sketch_spark_sql_door_refuses`,
  `…::test_sketch_ansi_sql_door_refuses`;
  `crates/repark-functions/src/declared_refuse.rs::sketches_are_deferred_by_cost_and_sorted`.
- **Rationale** — DECLARED deferred-by-cost (design D-7 / §8). This is a cost deferral, not a
  JVM-only gap. A DataSketches port is a sub-project; do not silently alias DataFusion HLL.

### FNP-16-csv-xml-xpath — CSV / XML / XPath are reachable, deferred by cost

- **repark** — `to_csv`, `to_xml`, and the nine `xpath_*` names are exported and refuse as
  **reachable without a JVM and deferred by cost**. The nine `xpath_*` functions need an XPath
  1.0 engine matching `javax.xml.xpath`. `datafusion-spark`'s `csv` and `xml` modules are
  empty. (`from_csv` / `from_xml` / `schema_of_*` already refuse as E1 stubs.)
- **Apache Spark** — parses CSV/XML and evaluates XPath 1.0 over XML strings.
  *(oracle: documented.)*
- **Pin** — `python/repark/tests/test_fnp15_16_declared_refuse.py::test_csv_xml_xpath_facade_refuses_deferred_by_cost`,
  `…::test_csv_xml_xpath_spark_sql_door_refuses`,
  `…::test_csv_xml_xpath_ansi_sql_door_refuses`;
  `crates/repark-functions/src/declared_refuse.rs::csv_xml_xpath_are_deferred_by_cost_and_sorted`.
- **Rationale** — DECLARED deferred-by-cost (design D-7 / §8). This is a cost deferral, not a
  JVM-only gap. An XPath 1.0 dependency is a sub-project.

### FNP-16-variant — Spark VARIANT is reachable, deferred by cost

- **repark** — the eight VARIANT names (`parse_json`, `try_parse_json`, `is_variant_null`,
  `variant_get`, `try_variant_get`, `schema_of_variant`, `schema_of_variant_agg`,
  `to_variant_object`) are exported and refuse as **reachable without a JVM and deferred by
  cost**. Spark VARIANT is a specific value/metadata binary encoding. RePark's `VariantType` is
  a shell with nothing behind it.
- **Apache Spark** — parses and extracts Spark VARIANT values.
  *(oracle: documented.)*
- **Pin** — `python/repark/tests/test_fnp15_16_declared_refuse.py::test_variant_facade_refuses_deferred_by_cost`,
  `…::test_variant_spark_sql_door_refuses`,
  `…::test_variant_ansi_sql_door_refuses`;
  `crates/repark-functions/src/declared_refuse.rs::variant_is_deferred_by_cost_and_sorted`.
- **Rationale** — DECLARED deferred-by-cost (design D-7 / §8). This is a cost deferral, not a
  JVM-only gap. Implementing the binary encoding is a sub-project.

### FNP-16-geospatial — GEOGRAPHY/GEOMETRY are reachable, deferred by cost

- **repark** — `st_asbinary`, `st_geogfromwkb`, `st_geomfromwkb`, `st_setsrid`, and `st_srid`
  are exported and refuse as **reachable without a JVM and deferred by cost**. Spark
  GEOGRAPHY/GEOMETRY have no Arrow representation and no vendored WKB codec.
- **Apache Spark** — constructs and inspects GEOGRAPHY/GEOMETRY values.
  *(oracle: documented.)*
- **Pin** — `python/repark/tests/test_fnp15_16_declared_refuse.py::test_geospatial_facade_refuses_deferred_by_cost`,
  `…::test_geospatial_spark_sql_door_refuses`,
  `…::test_geospatial_ansi_sql_door_refuses`;
  `crates/repark-functions/src/declared_refuse.rs::geospatial_is_deferred_by_cost_and_sorted`.
- **Rationale** — DECLARED deferred-by-cost (design D-7 / §8). This is a cost deferral, not a
  JVM-only gap. A WKB codec plus Arrow representation is a sub-project.
