# map — repark-spark/src/insert_by_name

## Purpose

File-backed tests for `../insert_by_name.rs` (ICE-RTAS-BYNAME-1, 2026-09-17):
the token-level `BY NAME` strip and the name-resolution error rules. Each
recognized form has a row; the executor pins live in the Python suite
(`test_ice_rtas_byname_1.py`).

## Contents

- `tests.rs` — the `#[cfg(test)] mod tests;` declared in `../insert_by_name.rs`.
  Strip pins (plain / overwrite / `TABLE` keyword / case, `PARTITION` kept),
  absence pins (no `BY NAME`, `ORDER BY name` after the source, quoted `"BY"`),
  and the resolution-rule pins (count-first arity, case-insensitive match,
  duplicate source ambiguity, extra-column refusal). Round 2 (2026-09-17):
  case-sensitive exact match + `EXTRA_COLUMNS`, `STATIC_PARTITION_COLUMN_IN_INSERT_COLUMN_LIST`
  and `CANNOT_FIND_DATA` texts, partition-literal rendering, verbatim
  syntactic names under the flag.
  pins: ice-rtas-byname-1/C-007, C-009, C-010

## Round 2 (2026-09-17)

`PARTITION (…)` routes through the positional arm: static overwrite delegates
to `execute_partition_overwrite` after name projection (the source must not
name a static column); dynamic overwrite is whole-table replace-all, which is
Spark's default-mode answer for a valueless spec (`partitionOverwriteMode`
is unread, same residue class as the positional always-dynamic path);
static append injects the clause literals into the projection; dynamic
append matches the full target list. Empty unpartitioned overwrite wipes
via `commit_overwrite_replace_all_to` after the empty-source type guard.
A missing required target refuses `CANNOT_FIND_DATA` before any write.
`spark.sql.caseSensitive=true` matches exact
(carrier `repark_functions::case_sensitive`). `plan_name_projection` owns
matching plus projection building; `execute_insert_by_name` owns routing
plus the two commits.

## Pointers

- Up: [../map.md](../map.md). Design: `../../../../docs/design/sql-doors.md`.
- Oracle: `../../../../python/repark/tests/ice_rtas_byname_1_spark_oracle.json`.
- Ledger: `../../../../task/ledgers/staging/ice-rtas-byname-1-ledger.md`.

## Debug

| Symptom | First check |
|---|---|
| A `BY NAME` after the source stopped stripping | `find_by_name_span` in `../insert_by_name.rs` — the span must precede the source start and survive the paren-depth walk |
| A quoted `"BY NAME"` strips | the tokenizer must mark quoted spans so the word walk skips them |

## IPI-19 + IPI-37 (2026-09-20) — the schema-evolving by-name append

- `evolution.rs` — `columns_to_add` applies the `write.spark.accept-any-schema`
  gate and the merge-schema flag to the source columns the table does not have,
  returning the names to add (empty means the existing refusal arms answer).
  `append_with_evolution` plans the projection, drops NULL-typed fill columns
  from the union input (a `NULL AS col` fill carries no type, and unioning it
  would refuse) while refusing outright if an *added* column is itself typeless,
  commits the union through `repark_iceberg::write::evolve_schema`, and writes
  the data files against the table that returns. `INSERT OVERWRITE … BY NAME`
  never evolves — no cell measures it and silently widening a schema on an
  overwrite is the wrong default.
  *Correction (2026-09-24, U6 WRITE-REFUSALS critic r1):* the names returned were
  the case-folded resolved names, so `1 AS NewC` added `newc` where Spark adds
  `NewC`. An added column now keeps the source spelling (u6-write-refusals/C-010),
  and `INSERT OVERWRITE … BY NAME` now evolves as Spark measures it
  (u6-write-refusals/C-011). `append_with_evolution` is gone; see the U6 section
  below.
  pins: ipi-19-56-37-schema-evolution-write/C-002, C-011

## U6 WRITE-REFUSALS (2026-09-24)

- `evolution.rs` — `columns_to_add` returns `None` when nothing evolves and
  `Some(SourceUnion)` whenever the table has the property and merge-schema is
  on, even with no new column. Spark's union runs on every such write, so a
  wider source widens and a non-promotable type refuses `Cannot change column
  type`. Without merge-schema, the first unknown source column refuses
  `Field <display name> not found in source schema`. `evolve_before_write`
  unions the source schema into the table and re-registers the catalog. The
  normal by-name append or overwrite then runs against the evolved table,
  including `INSERT OVERWRITE … BY NAME` (measured, C-011).
  `append_with_evolution` is gone. `routes_positional_by_name` is the router's
  gate for a positional `INSERT`.
  pins: u6-write-refusals/C-002, C-005, C-006
- Critic r1 remediation (2026-09-24), all in `evolution.rs`:
  - `SourceUnion` names each source column for the union: a matched column
    takes the table's spelling, and a new column takes the source's display
    spelling (`NewC`, not the folded `newc`). Matching stays case-insensitive.
    *Narrowed (2026-09-24, critic r2 V-001):* the display spelling is Spark's
    name only for the shapes `source_names.rs` names from the statement text
    (U6 round 3 below, C-015). A column behind a star over a RePark view keeps
    the planner's lowercase spelling (R-6).
  - `refuse_duplicate_sources` runs before any evolution. An exact repeat
    refuses with Iceberg's `Invalid schema: multiple fields for name …`. A
    case-folded repeat of a table column refuses `Multiple entries with same
    key: …`. Under the conf, `refuse_lower_case_collision` refuses two new
    names that differ only in case. All three are Spark's measured texts
    (C-013).
  - `routes_positional_by_name` returns `Result<bool>`. A tag selector or a
    missing table or namespace keeps the positional path; any other load
    failure surfaces (C-014).
  pins: u6-write-refusals/C-010, C-011, C-012, C-013, C-014
- `source_names.rs` (superseded by the round-3 entry below) — `syntactic_source_names` and `normalize_ident`, moved out
  of `../insert_by_name.rs` to keep it under the size ceiling. An unaliased
  integer or single-quoted literal is named by its text, as Spark names it.
  The names come from the leftmost `SELECT` of a positional set operation
  (Spark names a `UNION` by its first arm); `UNION … BY NAME` still goes to the
  planner probe. `aliased_source` rewrites that `SELECT`'s items so each carries its resolved
  name as a backquoted alias, and the projection can address every source column
  by that name.
  pins: u6-write-refusals/C-002, C-010
- `../insert_by_name.rs` — `source_from_clause` aliases a `VALUES` source's
  columns `col1..colN` with a derived-table column list. Every other source
  goes through `aliased_source`, because DataFusion's column-list rename looks
  the inner column up through `col(name)`, which folds `NewC` to `newc` and
  fails.
- `tests.rs` — pins for the literal spelling, the aliased `FROM` clause and the
  `UNION` arm naming.
  pins: u6-write-refusals/C-002, C-005, C-006, C-010

## U6 WRITE-REFUSALS round 3 (2026-09-24, critic r2 V-001..V-003)

Before this round, one projection item that was not a bare identifier, alias or
literal sent the whole list to the planner. DataFusion's names then reached the
refusal text and the Iceberg metadata (`upper(datafusion.public.src.data)`,
`Int64(-1)`, `column1`, and `newc` for an alias `NewC`). Names are now derived
per item.

- `source_names.rs` — `query_items` names each item of the leftmost `SELECT`
  (or `col1..colN` for a leftmost `VALUES`, also inside a positional set
  operation):
  - an alias, an identifier, and `CAST(<column> AS <type>)` or a parenthesized
    column take the column's own spelling;
  - any other expression takes `spark_names::expression_name`;
  - a plain `*` or `<alias>.*` over one derived table or CTE takes the inner
    statement's names, or its column-alias list when it has one;
  - any other star stays open. The planner fills it, and its columns count as
    Spark's only when every `FROM` relation is a named table or view.
  `probe_source_names` skips the planner when every item is named. Otherwise
  `place` lays the planner's columns under the items. That needs at most one open
  star; with more, every column is planner-named. Each `SourceName` keeps
  `column`, the planner-side name the projection addresses, apart from
  `resolved`, the match key. When RePark cannot derive Spark's name for a
  column, the column carries `underived`. `refuse_underived` then refuses it
  `NotImplemented` before any evolution, unless the table already has a column
  of that name. `aliased_source` aliases only the non-star items, so a star
  keeps the planner's columns.
  pins: u6-write-refusals/C-015, C-016, C-017
- `spark_names.rs` — Spark's rendering of an unaliased expression, measured
  item by item (`target/probe-u6-r2fix/spark*.out`):
  - an integer or plain decimal keeps its text, and a negative literal folds
    (`-1`; `-0.0` renders `0.0`);
  - strings are unquoted, booleans lowercase, and `NULL` stays `NULL`;
  - a nested column drops its qualifier only when that qualifier is a `FROM`
    relation;
  - binary operators render as `(l op r)`, `<>` as `(NOT (l = r))`, and `||` as
    `concat(l, r)`;
  - unary minus renders `(- x)`, and `NOT` / `IS [NOT] NULL` are parenthesized;
  - ten functions whose Spark name is the lower-cased call render as
    `name(args)`.
  Anything else stays underived: `CASE`, `substr` (Spark adds default
  arguments), `ceil` (`CEIL`), an exponent literal, or a nested `CAST`. Those
  are residue R-9.
  pins: u6-write-refusals/C-015, C-016
- `../insert_by_name.rs` — calls `refuse_underived` before `columns_to_add`. The
  projection addresses `SourceName::column`, and any leftmost `VALUES` takes the
  derived-table column list. The empty-overwrite type guard passes
  `null_assignable = true`: a NULL-typed column (a by-name fill or the
  source's own `NULL`) is assignable, as Spark's is. `INSERT OVERWRITE t SELECT
  9 AS id WHERE false` wipes, and under the conf it adds the new column first.
  pins: u6-write-refusals/C-018
- `tests.rs` — the rendering, underived, star-placement and derived-star pins.
  pins: u6-write-refusals/C-015, C-017

## U6 WRITE-REFUSALS round 4 (2026-09-24, critic r3 V-001..V-004)

- `source_names.rs` — `probe_source_names` takes `accepts_any`. When the
  planner probe fails on an accept-any table, `repeated_names` derives the
  per-item list from the statement. `star_names` resolves each star that
  would count as Spark's, one star at a time, with its own `LIMIT 0` probe.
  If that list repeats a name, it goes back without the whole-source probe, and
  `refuse_duplicate_sources` answers Iceberg's text. `SELECT *, * FROM src`
  refuses `Invalid schema: multiple fields for name id: 0 and 2`, where the
  planner would have leaked DataFusion's `Projections require unique expression
  names`. With no repeat, the probe's own error surfaces unchanged. A table
  without the property keeps the old path.
  pins: u6-write-refusals/C-013
- `source_names.rs` — `user_text` renders an opaque item, a star and the
  whole-source fallback with the router's `__repark_suffix_literal__(x)` marker
  unwrapped to `x`. A refusal therefore never quotes the internal marker. The
  quoted text is still the canonicalized form: `9L` reads `CAST(9 AS BIGINT)`,
  and `2BD` reads `CAST(2 AS DECIMAL(1,0))`. The router rewrites typed-suffix
  literals before this path sees the SQL. The same cast written out is a
  different Spark name (`CAST(9 AS BIGINT)`), so Spark's `9` is not recoverable
  from the AST, and those shapes stay residue R-9.
  pins: u6-write-refusals/C-017

U8 WRITE-SQL PR1 (2026-09-24): `evolution.rs` `routes_positional_by_name` keeps a positional
`INSERT INTO … PARTITION (…)` on the by-name door when the table carries
`write.spark.accept-any-schema`; only the `PARTITION` overwrite stays excluded. Spark answers
`Field 9 not found in source schema` there (u6-write-refusals residue R-3 part b, appends).
`spark_names.rs` `expression_name` is crate-visible for the PARTITION arity message.
pins: u8-write-sql/C-008, C-010
