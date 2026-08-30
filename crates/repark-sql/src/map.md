# map — repark-sql/src

CC-4 (2026-08-30): remaining banner files condensed to the one-line rule
(pins: cc-3-comment-condensation/C-009).

CC-2 closing-critic remediation: review-round label narration swept from prose; safety and
accuracy contracts restored in condensed form (see the unit ledger's findings dispositions).

## Purpose

Source for the ANSI SQL door. `lib.rs` is a manifest and `router.rs` owns routing. Every behavior,
including each refusal, has a test in the same change.
Source documentation may retain model provenance; code-quality grade tags stay outside code.

The router order is load-bearing: text guards first; pre-parse; parse; G15, FNP-15/16 declared-function refuse, and statement match;
parsed DML G3-E8, then async MoR/V3 valves; delegation applies SEC-02 after planning and before
execution. Multi-statement refusal precedes every rewrite. The wrong-door sniff runs on errors only.
There is no `$` pre-parse bypass; stock parsing handles metadata references.

## Contents

- `lib.rs` — manifest: module list, `pub use dialect::AnsiDialect`, `pub use router::execute`.
- `declared_refuse.rs` — FNP-15/16 ANSI-door parse valve (G15 dual-wire: Spark's copy lives in
  `repark-functions`). Sketches (32), CSV/XML/XPath (11), VARIANT (8), and geospatial (5) are
  deferred-by-cost. `armed_names()` is the 62-name roster. The crate door pin is
  `router/tests.rs::execute_refuses_every_armed_declared_name` (`execute`, not the helper).
  pins: fnp-15-16/C-001, C-008, C-009, C-010, C-011, C-013
- [v3/](v3/map.md) — format-v3 test modules (`#[cfg(test)] mod v3`).
- `delete_granularity.rs` — **test-only:** ANSI `write.delete.granularity`
  (`file` default / explicit `partition` / refuse unknown / SET PROPERTIES then MERGE)
  on MERGE (`Model: Grok 4.6 xHigh`).
- `a13_fallback.rs` — **A13 (test-only):** `register_memory_catalog` + location-less ANSI
  CREATE lands under `{warehouse}/repark_ansi_ctas/…`, not the process temp dir.
- `router.rs` — the statement router (text guards → pre-parse stage → parse → G15 collation
  valve → match → the two DML valves → delegate) and the delegation path that carries the SEC-02
  guard. Delegation
  covers reads, the fork's metadata tables, and `INSERT`/`DELETE`/`UPDATE` via the fork's
  `TableProvider` (ADR-0003). `DELETE`/`UPDATE` share a NAMED arm on the way to delegation:
  allow-listed uncorrelated `DELETE … IN` / `NOT IN (SELECT …)`, `[NOT] EXISTS` ±
  correlation, correlated IN, and identity `UPDATE … IN` call `execute_predicate_dml`; every other
  subquery `WHERE` still hits G3-E8 then BUG-001 (cheap-first). Tests: [router/map.md](router/map.md).
- `dialect.rs` — `AnsiDialect: repark_core::SqlDialect` (the frozen seam adapter; a one-liner
  onto the router, deliberately; `#[async_trait(?Send)]` matches the core trait). In-module tests.
- `guards.rs` — the guard set: multi-statement refuse (quote-aware, FIRST), P11 read-only
  catalog DML (generic message), write-to-branch, the BUG-001 MoR valve (async wrapper over the
  tier-1 predicate, gating delegated DELETE/UPDATE), the SEC-02 local-filesystem plan gate, and
  the **G15 collation valve** (`refuse_collation_in_statement`, called immediately after the
  stock parse so `COLLATE` / column collation / session collation conf refuse at parse
  altitude; type-position `CAST AS STRING COLLATE` on parse-fail;
  `RESET` of a collation key before delegate),
  the **G3-E8 subquery-predicate DML valve** (`refuse_dml_subquery_predicate`, called from the
  router's named `DELETE`/`UPDATE` arm because it needs the PARSED statement — it reads both the
  `WHERE` expression and the target off the parse tree, so a quoted target renders usably).
  SEC-02 scope: it gates DataFusion's own filesystem-as-data DDL (`CREATE EXTERNAL TABLE`,
  `COPY TO`), NOT an intercepted `CREATE TABLE … WITH (location = …)`, which the catalog's
  `LocationPolicy` governs instead.
  The last two are reimplemented from the Spark door's contract. They remain private, with no
  door-to-door product edge or `repark-functions` dependency. They use the same conf key and
  grandfather rule through `ConfigOptions::entries()`.
  **V3-2** reads `repark.sql.allow_create_format_version_3` the same way.
  Tests: [guards/map.md](guards/map.md). V3-COW-1 tests cover the delegated `DELETE | UPDATE`
  branch; `dml_target_ident` reads the AST and completes short names.
- `sniff.rs` — the error-path wrong-door sniff (Q10/G3): on parse/plan FAILURE, name the token,
  the native equivalent, and the Spark door. Tests: [sniff/map.md](sniff/map.md).
- `scan.rs` — ANSI-quoting-aware SQL text scanning: the one place the door reads raw text.
  Blanks string-literal / quoted-identifier / comment CONTENT so the guards and the sniff cannot
  false-positive. Backticks are deliberately NOT treated as quoting (they are the Spark-ism the
  sniff reports). In-module tests.
- `create_table.rs` — CTAS + column-def `CREATE TABLE`: Q15 target routing (registered Iceberg
  catalog or LOUD refuse — never a silent `MemTable`), clause refusals, **V3-2** `format_version`
  resolved at execute against `repark.sql.allowCreateFormatVersion3` (default false; entries()
  reader, no `repark-functions` product edge; `execute_staged_create` /
  `iceberg_table_creation` / `iceberg_create_format_version` carry `Model: Grok 4.6 xHigh`), A11 nanosecond-timestamp
  refuse on the column-def path (column + precision 9 + `TIMESTAMP(6)`; CTAS untouched), the
  three-way `LocationPolicy` resolution (**A13:** `TempFallbackAllowed` root is the memory
  catalog warehouse on the `register_memory_catalog` path), staged create/replace, and the service-managed
  create-first path. Refuses Iceberg CREATE when any `TableScan`
  source (including expression subqueries) is tighten-derived AND the
  output has a non-nullable field (R-D), or the output schema still carries
  the tag. The write boundary uses the same source walk. `router.rs::delegate` additionally calls
  `repark_core::refuse_iceberg_create_of_tightened_ddl` on the planned DDL, so the
  `CREATE VIEW cat.ns.v` / `SELECT … INTO cat.ns.t` sinks that fall through the `_ =>` arm
  cannot persist a required column. Both paths use the shared belt: `router.rs::delegate` is
  `PreExecute::plan` → SEC-02 → `PreExecute::guard` → `PreExecute::execute`; CTAS derivation
  plans and guards before target creation or publication, then returns the lazy SELECT frame.
  Later execution performs the SELECT. Tests:
  [create_table/map.md](create_table/map.md).
- `properties.rs` — the curated `WITH (…)` vocabulary (Q1/G4/G9): `format`, `format_version`
  (V3-2: `'2'` and `'3'` stored at parse; execute applies the session opt-in),
  `location`, `partitioning`, the `extra_properties = MAP(ARRAY[…], ARRAY[…])` raw-key hatch,
  and the reserved refusals (`sorted_by`, ORC/AVRO) that name their triggers.
  Tests: [properties/map.md](properties/map.md).
- `partitioning.rs` — partition-transform parsing (a small pure function, per Q2 — deliberately
  NOT shared with the Spark door's `PARTITIONED BY` validator) and Iceberg spec building with
  Java-parity field names. Tests: [partitioning/map.md](partitioning/map.md).
- `schema_ddl.rs` — `CREATE SCHEMA … WITH (location = …)`, `DROP SCHEMA`, `DROP TABLE`, plus the
  shared catalog-handle / name-parts / identifier-hygiene helpers.
  `IF NOT EXISTS` runs the same location-conflict predicate as the Spark door
  (matching / no-location idempotent; contradictory location fails loud).
  Tests: [schema_ddl/map.md](schema_ddl/map.md).
- `alter.rs` — `ALTER TABLE` schema evolution (ADD/DROP/RENAME COLUMN, `ALTER COLUMN … SET DATA
  TYPE`, `RENAME TO`) through the tier-1 `repark_iceberg::write::alter` seams, plus Trino
  `SET PROPERTIES` and its ONE pre-parse recognizer (blank the word `PROPERTIES`, let the stock
  parser read `SET (…)`). Curated vocabulary; `partitioning` is the pre-designated future
  spelling and refuses citing Q3. Tests: [alter/map.md](alter/map.md).
- `merge.rs` — `MERGE INTO` → `repark_iceberg::write::merge::MergeSpec` (~200 lines of mapping).
  Execution is the shared RePark-owned executor, never the fork `TableProvider`. No star forms
  (parse-level absent here); OUTPUT/RETURNING refuses. Oracle sub-predicates,
  assignment-target qualification, non-last unconditional clauses refuse at this door.
  Lone unconditional MATCHED DELETE cardinality exemption is execute-path (shared executor);
  pins in [merge/cardinality_tests.rs](merge/cardinality_tests.rs).
  Tests: [merge/map.md](merge/map.md).
- `time_travel.rs` — the `FOR VERSION|TIMESTAMP AS OF` token-scan rewrite (Q5/G7): recognize,
  resolve through the hoisted `repark_core` half (`TimeTravelSpec` / `read_table_at`), register
  an ephemeral pinned view, splice its name in, THEN parse. `FOR` is mandatory; `"` quotes
  identifiers and `'` quotes strings. **`PinnedViews` records two names per relation:** the
  `__repark_ansi_tt_<n>` view this door mints AND the `__repark_tt_<n>` `read_table_at` registers
  underneath it. For a returned frame, SQL records the core name before consuming it and the ANSI
  name before its registration attempt; the router releases both after planning. If core
  registration succeeds but `ctx.table` lookup fails, no frame returns and SQL cannot record the
  core name. Reader-options registrations remain because they back the returned frame.
  Tests: [time_travel/map.md](time_travel/map.md) + the both-prefix leak pin in
  [../tests/map.md](../tests/map.md).
- `ref_ddl.rs` — the ALTER-scoped branch/tag grammar (Q6/G6, copied from the Spark door's
  precedent) over the tier-1 `ManageSnapshots` seams. The top-level `CREATE BRANCH b IN t`
  spelling stays Spark-only. Tests: [ref_ddl/map.md](ref_ddl/map.md).
- `refusals.rs` — the completed refuse set (Q7/Q9 + TRUNCATE): `INSERT OVERWRITE`, `CALL`,
  `ALTER TABLE … EXECUTE` (pre-parse recognizer), `TRUNCATE TABLE`. Every message names a
  replacement and, where the design gives one, a trigger. Tests: [refusals/map.md](refusals/map.md).
- `matrix.rs` (`#[cfg(test)]`) — this door's disposition of every `repark_common::surfaces` ID,
  with the compile-run audit that fails on an unmapped surface (Q13/G2). **46 tested / 4
  deliberately absent** (Q3 partitioning, Q9
  `INSERT OVERWRITE`, `TRUNCATE`, Q7 maintenance). The three semantic pin absences are tested
  (window frames, JOIN NULL keys, float determinism). Each row also carries its
  session profile, and a test forbids this door from ever claiming `SparkExtended` evidence.
- `tests.rs` (`#[cfg(test)]`) — the end-to-end door battery on a native session, asserted on the
  Arrow path with value and type checks. The helper uses its warehouse as the temporary fallback
  root; the memory-catalog location pin lives in `a13_fallback.rs`.

## I want to...

| ...do this | go to |
|---|---|
| Change routing order | `router.rs` (the order is the design's — read the module doc first) |
| Add a curated table property | `properties.rs` + a row in `properties/tests.rs` + an e2e row in `tests.rs` |
| Add a partition transform | `partitioning.rs` + `partitioning/tests.rs` |
| Add a guard | `guards.rs` + `guards/tests.rs` + a `surfaces` ID if it is a claimed surface (a guard needing the PARSED statement instead of scrubbed text is called from a named `router.rs` arm; unit AND end-to-end pins still live in `guards/tests.rs` — that is what G3-E8 does) |
| Add an `ALTER TABLE` operation | `alter.rs` `execute_alter_table` + `alter/tests.rs` + an e2e row in `tests.rs` |
| Add a `SET PROPERTIES` key | `alter.rs` `parse_set_properties` (curated only — dotted keys go through `extra_properties`) |
| Change what MERGE lowers to | `merge.rs` — the target type is shared with the Spark door, so a change there is a cross-door contract change |
| Change the time-travel grammar | `time_travel.rs` `clause_kind_at` / `parse_as_of_value`, then the pin set in `time_travel/tests.rs` |
| Add a refusal | `refusals.rs` + `refusals/tests.rs` + a `DeliberatelyAbsent` matrix row citing the ruling |
| Record a surface this door will not have | `matrix.rs` (`DeliberatelyAbsent` with reason + ADR) |

## Pointers

- Up: [../map.md](../map.md). Design: `../../../docs/design/sql-doors.md`.

## Debug

| Symptom | First check |
|---|---|
| A `DELETE`/`UPDATE` with a subquery `WHERE` was refused | By design (G3-E8): `guards::refuse_dml_subquery_predicate`, the twin of the Spark door's valve, same refusal text. Re-implemented rather than shared — door→door product edges are banned |
| A guard fired on text inside a string literal | It cannot — the guards read `scan::blank_out_quoted_and_comments` output; check the scrubber's tests |
| A statement was delegated that should have been intercepted | `router.rs` match arms — no text path bypasses the statement match, including `$` metadata references |
| The wrong-door sniff fired on ANSI-legal SQL | `sniff.rs` `scope_for` / `Scope::Leading` — tokens with an ANSI reading (`USING`, `NAMESPACE`, `BRANCH`/`TAG`) fire only under their leading keyword |
| The matrix audit RED after adding a surface ID | Add a `Tested` or `DeliberatelyAbsent` row in `matrix.rs`; the failure names the ID |
| The matrix audit RED | A surface changed disposition — update its row and accompanying test in the same change |
| `FOR … AS OF` produced a raw parser error | The scanner only claims `FOR VERSION\|TIMESTAMP AS OF`; `SYSTEM_*` and the FOR-less forms are sniff steers by design (`time_travel.rs` module doc) |
| A time-travel read saw CURRENT rows | The rewrite must have been skipped — check `find_time_travel_spans` produced a span; a resolved span always registers a snapshot-pinned provider |
| `SET PROPERTIES` reached the parser unrewritten | `alter::rewrite_set_properties` only fires on a statement LEADING with `ALTER TABLE`; a `PROPERTIES` inside a literal is invisible by design |
| Branch DDL was not recognized | `ref_ddl::try_parse_ref_ddl` takes ALTER-scoped forms only; the top-level `CREATE BRANCH b IN t` is Spark-door surface |
| A legal `ALTER TABLE` was refused as `… EXECUTE …` | The recognizer is ANCHORED to the verb slot after the table name (`refusals::verb_slot_after_table_name`); a column may legally be named `execute` |
| A branch/tag statement ran with a clause we did not read | It cannot — `ref_ddl::reject_trailing` refuses on ANY leftover token, numbers and punctuation included (only a trailing `;` is dropped) |
| `SHOW TABLES` listed a `__repark_ansi_tt_*` relation | The router releases every `time_travel::PinnedViews` name after planning; a leak means a `?` / `return` path was added that skips `pinned.release` (unwind / future-drop bypass it by design — no `Drop` impl, and no cancellation source exists today) |
| `SHOW TABLES` listed a `__repark_tt_*` relation (no `ansi`) | The core half of the pinned view leaked. Each `FOR … AS OF` composes this door's view over `repark_core::read_table_at`, which registers its own `__repark_tt_<n>`. `register_pinned_view` records both names; check that record. A leftover after no `FOR … AS OF` is the reader-options residual (`spark.read.option("snapshot-id"…)`), which remains registered by design |

First checks: `cargo test -p repark-sql --lib`. Escalate to: [../map.md#debug](../map.md).
