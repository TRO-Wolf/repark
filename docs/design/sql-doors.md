# The two SQL doors — the phase-2 design (settled 2026-08-07)

The deliberate design pass ADR-0002 requires before the phase-2 doors land. Provenance: a
three-design adversarial review (three independent design attempts scored by a three-judge
panel) over a phase-2 recon corpus. **The delegate-first architecture anchors the synthesis**
(judged the only realistic one-phase scope, with the strongest port-fidelity score), with ten
grafts from the other two attempts recorded below where they land. Companion execution brief:
[../../briefs/phase-2-sql-doors.md](../../briefs/phase-2-sql-doors.md). Builds on the phase-1
design in [session-api.md](session-api.md); deferred-test obligations live in
[../../task/port/deferred-tests.md](../../task/port/deferred-tests.md).

## 0. Verdict shape

- **Architecture = delegate-first: NO shared-lowering crate in phase 2.** Shared machinery
  already sits legally below the doors (tier 1 repark-iceberg: merge execution, staged CTAS
  transaction, overwrite commit, UpdateSchema/UpdatePartitionSpec/ManageSnapshots; tier 2
  repark-core: CatalogRegistry, LocationPolicy, TimeTravelSpec/read_table_at). The Spark door
  ports **verbatim** — no parse/exec half-file surgery (the review found identity-diff fracture
  plus a test vacuum to be the shared-core alternative's fatal risk: v1's exec halves are pinned
  by SQL-text tests that cannot ride a text-free crate).
- **Grafts adopted** (judged best-in-field regardless of winner):
  G1 loud-refuse CTAS routing (never a silent MemTable) — unanimous across judges;
  G2 typed surface registry + per-door `matrix.rs` + compile-run audit test;
  G3 error-path-only wrong-door sniff with the token + native-equivalent + steer message;
  G4 `extra_properties` escape hatch (Trino's own raw-key spelling) — keeps
     `write.merge.mode` (MoR creation) reachable without freezing dotted keys as bare API;
  G5 two-session cross-door equivalence protocol (all three attempts had this wrong);
  G6 branch DDL is precedent-copying, not invention (v1's `ref_ddl` module ships the
     Iceberg-Spark-extensions ALTER-scoped grammar — verified at the pin);
  G7 quote-parameterized time-travel scanner + the v1 span/comment/string-ref pin set as
     double-quote ANSI variants;
  G8 DF 54.1 subquery guard placement → repark-core (extension-less native sessions must
     keep it);
  G9 reserved-refusal properties (`sorted_by`, ORC/AVRO) — hold the spelling, refuse loud,
     name the trigger;
  G10 dbt-trino evidence on Q9 (dbt-trino ships no insert_overwrite strategy) recorded in the
     absence row.
- **Defect fixes mandated by the judges**: SEC-02 (`refuse_local_filesystem_plan`) IS in the
  ANSI guard set; multi-statement refuse runs BEFORE the time-travel scanner (a known v1
  ordering-defect class); the PR-3 split (3a DDL / 3b DML+refs) is mandatory, not optional.

## 1. Crate layout + DAG tiers

`scripts/check_crate_dag.py` TIERS gains four rows, all **tier 3**:

| crate | tier | deps (workspace) | contents |
|---|---|---|---|
| `repark-functions` | 3 | repark-common | v1 repark-functions ported VERBATIM (name kept — zero census rename) |
| `repark-ta` | 3 | repark-common | v1 kernels + goldens; NEW thin `TaExtension: SessionExtension` |
| `repark-spark` | 3 | repark-core, repark-iceberg, repark-common, repark-functions, repark-ta (same-tier edges) | v1 repark-sql ported: router + all handlers; `SparkDialect: SqlDialect` + `SparkExtension: SessionExtension` |
| `repark-sql` | 3 | repark-core, repark-iceberg, repark-common | NEW ANSI door: `AnsiDialect: SqlDialect`; no extension; no datafusion-spark; sqlparser types ONLY via `datafusion::sql::sqlparser` re-export |

No door→door edge, ever. Small hoists (tier-legal, declared-rename units): the
MoR-unpartitioned-multi-spec valve predicate → repark-iceberg (metadata-shaped, both doors
call it); DF 54.1 subquery guard → repark-core session defaults (G8); a `stamp_read_only`
helper beside CatalogRegistry (both doors do v1's clone+stamp dance through it). The
`surfaces` const ID list (G2) lives in repark-common (tier 0, dialect-neutral vocabulary).

> **Delivery record (2026-08-08).** The ANSI door closed at phase-2 PR-6. The per-Q "what
> actually shipped, and what it cost" record has ONE home — [../../task/p2g-ansi-m2-ledger.md](../../task/p2g-ansi-m2-ledger.md)
> (PR-6) and [../../task/p2f-ansi-m1-ledger.md](../../task/p2f-ansi-m1-ledger.md) (PR-5). This
> file stays the DESIGN: the rulings below are what was decided, not a changelog of what was
> built. Read them together; do not annotate this file with delivery notes.

## 2. Q1–Q15 rulings (the ANSI door)

- **Q1 properties**: curated Trino names — `format` ('PARQUET'; ORC/AVRO refuse-loud w/
  trigger, G9), `format_version`, `partitioning`, `location`, `sorted_by` (refuse-loud w/
  trigger) — PLUS **functional `extra_properties = MAP(ARRAY[…], ARRAY[…])`** (G4) for raw
  Iceberg keys (typo-guard: unknown BARE key refuses loud listing the curated set; dotted keys
  go through extra_properties only). MoR table creation is reachable in phase 2.
- **Q2 partitioning**: `WITH (partitioning = ARRAY['month(ts)','bucket(16,id)'])` only.
  Transform validation re-implemented ANSI-side as a small pure function (no half-file move);
  cross-door rows pin identical accept/reject behavior against the Spark door's ported
  validator.
- **Q3 spec evolution**: DEFERRED from SQL. Callable op (fork `UpdatePartitionSpec` via
  repark-iceberg) is the phase-2 answer; designated future spelling `ALTER TABLE t SET
  PROPERTIES partitioning = ARRAY[…]` (replace-spec, Trino semantics). Trigger: dbt-repark or
  first user need. `DeliberatelyAbsent` row.
- **Q4 MERGE**: per-door thin lowering (~150 LOC ANSI `Statement::Merge` → `MergeSpec`);
  execution shared at tier 1 (`repark_iceberg::write::merge::execute_merge`, RePark-owned).
  Spark door's merge.rs (star-sentinel machinery) ports verbatim. Output clauses refuse loud.
  Drift guard: cross-door differential rows on shared fixtures (§4).
- **Q5 time travel**: token-scan rewrite, `FOR` mandatory — `FOR VERSION AS OF <n|'ref'>`,
  `FOR TIMESTAMP AS OF <ts>`; string version = branch/tag ref. Fresh ~150-line
  quote-parameterized scanner (G7) + the already-hoisted core resolution half. Ordering:
  multi-statement refuse FIRST, then metadata/TT rewrites, then parse (v1's order; fixes the
  ordering-inversion defect class the review flagged). No bespoke sqlparser Dialect (Q14).
  Spark door keeps its v1 scanner verbatim.
- **Q6 branch/tag DDL**: **ADOPT, as precedent-copying** (G6). The ANSI door takes exactly
  v1's ALTER-scoped grammar: `ALTER TABLE t CREATE [OR REPLACE] BRANCH|TAG b [AS OF VERSION n]
  [RETAIN …] [WITH SNAPSHOT RETENTION …]` / `ALTER TABLE t DROP BRANCH|TAG b` — own thin
  recognizer, same tier-1 `ManageSnapshots` executor path. The Spark-only top-level
  `CREATE BRANCH b IN t` forms stay Spark-door. Write-to-branch refuses in both doors (v1
  valve). Scope note: this is the designated first deferral if PR-6 overruns — and if
  deferred, the recorded rationale is SCOPE, never "no precedent".
- **Q7 maintenance**: callable-ops only (the ADR pin stands). `Statement::Call` and
  `ALTER TABLE … EXECUTE` shapes refuse loud, steering to the ops; `EXECUTE proc(arg => v)` is
  the pre-designated future spelling. Trigger: dbt-repark post-hooks demonstrating a
  statement-shaped need → superseding ADR note first, then the surface.
- **Q8 introspection**: delegate — DF information_schema + stock `SHOW TABLES`/`DESCRIBE t`.
  PR-5 runs the enumeration spike (registered Iceberg catalogs visible through
  information_schema) and records the result; gaps are core/fork fixes, not door parsers.
  Trino `SHOW SCHEMAS FROM` / `SHOW CREATE TABLE`: deferred with triggers.
- **Q9 INSERT OVERWRITE**: omit, Trino-faithful; loud refuse steering to MERGE /
  DELETE+INSERT / `CREATE OR REPLACE TABLE … AS SELECT`. Absence row cites the dbt-trino
  evidence (G10). OV1 machinery stays reachable (Spark door + callable op).
- **Q10 wrong-door ergonomics**: **error-path-only** sniff (G3) — on parse/plan FAILURE, scan
  for Spark-isms (USING, TBLPROPERTIES, PARTITIONED BY, bare VERSION/TIMESTAMP AS OF,
  SYSTEM_*, INSERT OVERWRITE, CALL …system…, backticks, NAMESPACE/DATABASE, LATERAL VIEW,
  top-level CREATE BRANCH) and upgrade the error: name the token, the native equivalent, and
  the Spark door. Zero happy-path cost; immune to string-literal/comment false positives.
  Backticks refuse (never tokenize as quotes). Case rules: stock DF ANSI folding; divergence
  from Spark documented, one doc-test row per door.
- **Q11 TA functions**: `repark-ta` ports with a thin `TaExtension` (register-only), owned by
  neither door; Spark extension composes it (v1 parity; deferred tests #8–#14); native
  sessions opt in. ANSI toll: one smoke row (f64::to_bits vs golden) + the non-literal-period
  refuse row.
- **Q12 guard rails**: per-door explicit calls, seam untouched. ANSI set: multi-statement
  refuse (quote-aware, FIRST), P11 read-only-catalog DML refuse (generic message), MoR valve
  (hoisted, tier 1), write-to-branch sniff, **SEC-02 local-filesystem plan refuse** (judge
  fix — the delegation path would otherwise plan `CREATE EXTERNAL TABLE 'file:///…'`/`COPY TO`
  freely). Spark door keeps all v1 guards verbatim.
- **Q13 test matrix**: `repark-common::surfaces` const ID list; each door carries `matrix.rs`
  mapping every ID → `Row::Tested { test }` | `Row::DeliberatelyAbsent { reason, adr }` with a
  compile-run audit test failing on unmapped IDs (G2) — absence is typed and build-enforced.
  **Cross-door equivalence rows run TWO sessions** (native/no-extension vs Spark-extended),
  each through its OWN door, comparing Arrow results (value AND type); `sql_with`
  single-session is legal only for surfaces the analyzer/UDF layer cannot touch (pure
  DDL/catalog ops); each matrix row records its session profile (G5).
  [session-api.md](session-api.md) gains the seam-freeze doc line: **extensions are
  session-scoped, not dialect-scoped** — a Spark-extended session has Spark expression
  semantics through every door.
- **Q14 parser**: stock DataFusion parser machinery, Generic dialect, both router-parse and
  delegation (NOT DF "ansi" — untested regression risk on the phase-1 baseline). No
  `ReparkAnsiDialect` in phase 2; trigger: a surface inexpressible as pre-parse scan + stock
  parse.
- **Q15 CTAS routing** (G1): resolve the target's leading segment against
  `EngineContext.catalogs`. Registered Iceberg catalog → staged create/replace (tier-1
  `StagedTableTransaction`, one publish, LocationPolicy 3-way incl. E-4
  `TempFallbackAllowed { root }`). ANYTHING else — unqualified, unregistered — **refuses
  loud requiring qualification**. Never a silent MemTable (the error-path sniff + fallthrough
  would otherwise compose into session-end data loss on a dbt-style 2-part name). Temp views
  never shadow a CTAS target. Default-catalog resolution: future non-breaking relaxation.

## 3. Seam freeze

Phase-2 start freezes `SqlDialect::execute(EngineContext<'_>, &str)` and
`SessionExtension` as shipped in phase 1 ([session-api.md](session-api.md) flips UNSTABLE →
frozen, plus the G5 extension-scope line). No core-side pre-execution hooks; guards are
door-called.

## 4. Testing discipline

Spark door + repark-functions + repark-ta: full port census — declared-rename units from pin
`fc3f48102`, rename maps GENERATED from the `cargo test -- --list` ground truth at the pin
(342 repark-sql + 62 repark-functions; repark-ta's list generated at its PR), prefix rule
`repark_sql::` → `repark_spark::`, `repark_functions::`/`repark_ta::` unchanged; empty
sorted-diff acceptance; relocation discipline for the three hoists
([../testing.md](../testing.md)). ANSI door: NEW code — [../testing.md](../testing.md) native
rules (test-per-change; every refusal is a behavior with a test row; divergence-class claims
pinned on the Arrow path, value AND type), plus the matrix audit. Deferred manifest
([../../task/port/deferred-tests.md](../../task/port/deferred-tests.md)): 14 of 18 rows zero in
phase 2 (7 Spark-door, 7 TA); the 3 postgres + 1 excel rows carry forward under an explicit
scheduling decision recorded in [../../task/todo.md](../../task/todo.md).

## 5. Sequencing rule (fidelity gate)

Any shared/hoisted unit an ANSI consumer relies on must have the ported Spark battery (or the
unit's own ported tests) green in the SAME PR that moves it, before an ANSI PR consumes it.
With the delegate-first shape this reduces to: the three hoists ride PR-2/PR-3b with their
tests; PR-5/PR-6 consume only phase-1 core + those landed hoists.

## 6. Top risks

R1 sqlparser 0.59 productions (`SET PROPERTIES`, `CREATE SCHEMA … WITH`) — spike PR-5 day 1;
fallback = v1-proven ~50-LOC pre-parse recognizers. R2 information_schema enumeration — spike
PR-5; fix lands core/fork-side if needed. R3 duplicated thin lowerings drift — cross-door
differential rows + shared target types (`MergeSpec`, `TimeTravelSpec`) turn drift into test
failure. R4 curated WITH vocabulary too small — extra_properties hatch + refuse-lists-the-set
+ additions are non-breaking. R5 dbt starvation from deferrals — every deferral names its
callable-op equivalent + trigger; absence rows keep the gaps auditable.
