# map — repark-sql/src

## Purpose

Source for the ANSI SQL door. `lib.rs` is a manifest (check_lib_rs); the router body lives in
`router.rs`. All NEW code — there is no port census here, so every behavior (including every
refusal) carries its own test in the same change.

The router's ORDER is the design's, and each position is load-bearing: text guards run first
(multi-statement before anything else), then the async BUG-001 MoR valve, then the PRE-PARSE
stage (the `ALTER … EXECUTE` refusal, the branch/tag recognizer, the `SET PROPERTIES` rewrite,
the `FOR … AS OF` rewrite), then a stock parse, then the statement match, then delegation with
the SEC-02 plan guard between planning and execution. The pre-parse stage sits AFTER the
multi-statement refuse on purpose — that is the BUG-010 ordering rule: no recognizer may ever
see, or rewrite, the second statement of a script. The wrong-door sniff runs on the ERROR path only. There is deliberately NO pre-parse
`$` passthrough — one existed and routed `CREATE TABLE … AS SELECT … FROM x$snapshots` past the
Q15 target check into a silent `MemTable`; the stock parser handles `$`, so metadata references
reach delegation through the ordinary arm.

## Contents

- `lib.rs` — manifest: module list, `pub use dialect::AnsiDialect`, `pub use router::execute`.
- `router.rs` — the statement router (text guards → pre-parse stage → parse → G15 collation
  valve → match → the two DML valves → delegate) and the delegation path that carries the SEC-02
  guard. Delegation
  covers reads, the fork's metadata tables, and `INSERT`/`DELETE`/`UPDATE` via the fork's
  `TableProvider` (ADR-0003). `DELETE`/`UPDATE` share a NAMED arm on the way to delegation:
  allow-listed uncorrelated `DELETE … IN` / `NOT IN (SELECT …)` and `[NOT] EXISTS` ±
  correlation call `execute_predicate_dml`; every other
  subquery `WHERE` still hits G3-E8 then BUG-001 (cheap-first). The BUG-001 valve used to sit at the router head, i.e.
  before the parse G3-E8 needs, which made the two doors disagree about which refusal a
  doubly-hazardous statement gets. Tests: [router/map.md](router/map.md).
- `dialect.rs` — `AnsiDialect: repark_core::SqlDialect` (the frozen seam adapter; a one-liner
  onto the router, deliberately). In-module tests.
- `guards.rs` — the guard set: multi-statement refuse (quote-aware, FIRST), P11 read-only
  catalog DML (generic message), write-to-branch, the BUG-001 MoR valve (async wrapper over the
  tier-1 predicate, gating delegated DELETE/UPDATE), the SEC-02 local-filesystem plan gate, and
  the **G15 collation valve** (`refuse_collation_in_statement`, called immediately after the
  stock parse so `COLLATE` / column collation / session collation conf refuse at parse
  altitude — G3-E8 lesson; type-position `CAST AS STRING COLLATE` on parse-fail;
  `RESET` of a collation key before delegate),
  the **G3-E8 subquery-predicate DML valve** (`refuse_dml_subquery_predicate`, called from the
  router's named `DELETE`/`UPDATE` arm because it needs the PARSED statement — it reads both the
  `WHERE` expression and the target off the parse tree, so a quoted target renders usably).
  SEC-02 scope: it gates DataFusion's own filesystem-as-data DDL (`CREATE EXTERNAL TABLE`,
  `COPY TO`), NOT an intercepted `CREATE TABLE … WITH (location = …)`, which the catalog's
  `LocationPolicy` governs instead.
  The last two are RE-IMPLEMENTED from the Spark door's contract (not shared): both live behind
  private modules in `repark-spark`, and `repark-sql` must not take a door→door edge, nor the
  `repark-functions` edge the Spark gate uses to read its conf. Same conf key, same grandfather
  rule, same refusal class — read via `ConfigOptions::entries()`.
  Tests: [guards/map.md](guards/map.md).
- `sniff.rs` — the error-path wrong-door sniff (Q10/G3): on parse/plan FAILURE, name the token,
  the native equivalent, and the Spark door. Tests: [sniff/map.md](sniff/map.md).
- `scan.rs` — ANSI-quoting-aware SQL text scanning: the one place the door reads raw text.
  Blanks string-literal / quoted-identifier / comment CONTENT so the guards and the sniff cannot
  false-positive. Backticks are deliberately NOT treated as quoting (they are the Spark-ism the
  sniff reports). In-module tests.
- `create_table.rs` — CTAS + column-def `CREATE TABLE`: Q15 target routing (registered Iceberg
  catalog or LOUD refuse — never a silent `MemTable`), clause refusals, the three-way
  `LocationPolicy` resolution, staged create/replace, and the service-managed create-first path.
  Tests: [create_table/map.md](create_table/map.md).
- `properties.rs` — the curated `WITH (…)` vocabulary (Q1/G4/G9): `format`, `format_version`,
  `location`, `partitioning`, the `extra_properties = MAP(ARRAY[…], ARRAY[…])` raw-key hatch,
  and the reserved refusals (`sorted_by`, ORC/AVRO) that name their triggers.
  Tests: [properties/map.md](properties/map.md).
- `partitioning.rs` — partition-transform parsing (a small pure function, per Q2 — deliberately
  NOT shared with the Spark door's `PARTITIONED BY` validator) and Iceberg spec building with
  Java-parity field names. Tests: [partitioning/map.md](partitioning/map.md).
- `schema_ddl.rs` — `CREATE SCHEMA … WITH (location = …)`, `DROP SCHEMA`, `DROP TABLE`, plus the
  shared catalog-handle / name-parts / identifier-hygiene helpers. **R-6 / G-6 Q1:**
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
  (parse-level absent here); OUTPUT/RETURNING refuses. Tests: [merge/map.md](merge/map.md).
- `time_travel.rs` — the `FOR VERSION|TIMESTAMP AS OF` token-scan rewrite (Q5/G7): recognize,
  resolve through the hoisted `repark_core` half (`TimeTravelSpec` / `read_table_at`), register
  an ephemeral pinned view, splice its name in, THEN parse. `FOR` is mandatory; `"` quotes
  identifiers and `'` quotes strings, which is the whole difference from the Spark door's
  scanner. **`PinnedViews` records TWO names per relation (H-1b, 2026-08-11):** the
  `__repark_ansi_tt_<n>` view this door mints AND the `__repark_tt_<n>` `read_table_at` registers
  underneath it. The second escaped the original ledger — it is minted in repark-core, under a
  different prefix — and leaked on the door whose fix declared the leak closed; the correction is
  recorded against the original claim in
  `docs/history/port-v2/p2g-ansi-m2-ledger.md` (finding 3). The reader-options caller of
  `read_table_at` is untouched: its registration must survive (it backs the returned frame).
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
  deliberately absent** as of R-3 (the four PR-6 standing rulings — Q3 partitioning, Q9
  `INSERT OVERWRITE`, `TRUNCATE`, Q7 maintenance). The three G8 pin-absences flipped to
  Tested (window frames, JOIN NULL keys, float determinism). Each row also carries its
  session profile, and a test forbids this door from ever claiming `SparkExtended` evidence.
- `tests.rs` (`#[cfg(test)]`) — the end-to-end door battery on a NATIVE session (no extension),
  asserted on the Arrow path, value AND type.

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
| A statement was delegated that should have been intercepted | `router.rs` match arms — and check nothing short-circuits BEFORE the statement match (a `$`-text passthrough once did) |
| The wrong-door sniff fired on ANSI-legal SQL | `sniff.rs` `scope_for` / `Scope::Leading` — tokens with an ANSI reading (`USING`, `NAMESPACE`, `BRANCH`/`TAG`) fire only under their leading keyword |
| The matrix audit RED after adding a surface ID | Add a `Tested` or `DeliberatelyAbsent` row in `matrix.rs`; the failure names the ID |
| `m1_ships_the_briefed_scope` RED | A surface changed disposition — update the pin AND the ledger, in the same change |
| `FOR … AS OF` produced a raw parser error | The scanner only claims `FOR VERSION\|TIMESTAMP AS OF`; `SYSTEM_*` and the FOR-less forms are sniff steers by design (`time_travel.rs` module doc) |
| A time-travel read saw CURRENT rows | The rewrite must have been skipped — check `find_time_travel_spans` produced a span; a resolved span always registers a snapshot-pinned provider |
| `SET PROPERTIES` reached the parser unrewritten | `alter::rewrite_set_properties` only fires on a statement LEADING with `ALTER TABLE`; a `PROPERTIES` inside a literal is invisible by design |
| Branch DDL was not recognized | `ref_ddl::try_parse_ref_ddl` takes ALTER-scoped forms only; the top-level `CREATE BRANCH b IN t` is Spark-door surface |
| A legal `ALTER TABLE` was refused as `… EXECUTE …` | The recognizer is ANCHORED to the verb slot after the table name (`refusals::verb_slot_after_table_name`); a column may legally be named `execute` |
| A branch/tag statement ran with a clause we did not read | It cannot — `ref_ddl::reject_trailing` refuses on ANY leftover token, numbers and punctuation included (only a trailing `;` is dropped) |
| `SHOW TABLES` listed a `__repark_ansi_tt_*` relation | The router releases every `time_travel::PinnedViews` name after planning; a leak means a `?` / `return` path was added that skips `pinned.release` (unwind / future-drop bypass it by design — no `Drop` impl, and no cancellation source exists today) |
| `SHOW TABLES` listed a `__repark_tt_*` relation (no `ansi`) | The CORE half. Each `FOR … AS OF` composes this door's view over `repark_core::read_table_at`, which registers its own `__repark_tt_<n>` — untracked until H-1b, so it escaped a ledger that only knew the `ansi` prefix. `register_pinned_view` now records both names; if one survives, check that record. A leftover on a session that ran NO `FOR … AS OF` is the reader-options residual instead (`spark.read.option("snapshot-id"…)`), which keeps its registration by design |

First checks: `cargo test -p repark-sql --lib`. Escalate to: [../map.md#debug](../map.md).
