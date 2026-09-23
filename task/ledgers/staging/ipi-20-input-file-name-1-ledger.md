# Charter ledger — IPI-20-INPUT-FILE-NAME-1 · `input_file_name()` over one Iceberg scan, Spark door

**Date:** 2026-09-23 · **Branch:** `fix/ipi-20-input-file-name` · **Base:** `f411192d` (`origin/main`) · **Model:** swe-2-high (R1) · **Policy:** [../../../AGENTS.md](../../../AGENTS.md).
**Path:** STANDARD. **risk_tier: standard.** **Registry:** new row `ICE-MC-IFN-1` in `docs/spark-sql-iceberg-parity.md` — `input_file_name()` served over one Iceberg relation; other shapes unresolved.

**Retires:** in flight.

**Scope:** `crates/repark-core/src/metadata_columns.rs` — a second trigger into
`prepare_metadata_column_sql` (an unquoted `input_file_name` word followed by
`(`) plus the `RewriteMetadataColumns::rewrite_input_file_names` /
`sole_relation_qualifier` helpers and the `InputFileNameCalls` sub-visitor that
rewrite a zero-argument `input_file_name()` to `<alias>._file` inside a
single-relation SELECT's projection and WHERE; the Rust battery
`crates/repark-spark/src/tests/input_file_name.rs` plus `tests/mod.rs`; the new
`ICE-MC-IFN-1` registry row; the `crates/repark-core/src/map.md` and
`crates/repark-spark/src/tests/map.md` lockstep rows; the `staging/map.md` row;
and this ledger. Round 3 adds the private `sole_input_file_name_relation`
helper behind `sole_relation_qualifier`: it accepts the SELECT's single
`TableFactor::Table` only when the written name is a rewrite's `original` — an
alias match against the statement-wide rewrite list no longer authorizes the
rewrite (the wildcard path's `sole_rewritten_relation` keeps main's alias
fallback untouched). Round 4 makes collection itself CTE-aware: a
`CollectCteNames` pass gathers every `With` clause's alias names in the
statement (nested ones included), and `CollectTables` skips a one-part
`TableFactor::Table` whose identifier matches a CTE name on the planner's own
fold (`normalized_ident_value` — unquoted folds case-insensitively, quoted
stays exact, so the skip predicts the CTE resolution the planner will make);
qualified names are never CTE references and stay collected. Round 4 also
replaces the `substr(input_file_name(), 1, 7)` scalar-argument pin with
full-value `concat('p:', …)` comparisons and adds the `#[cfg(test)]` module in
`metadata_columns.rs` that pins the collector seam directly. The fork, every
`Cargo.toml`, `Cargo.lock`, `STATUS.md`,
`crates/repark-functions/`, `crates/repark-sql/` and the ANSI door are
untouched; no code comment added anywhere.

## Measurements (decide-then-build evidence)

**M-1 — the oracle is recorded, not re-derived.** The WO carries probe 55l
(`/tmp/xo-xo-opus55/sb/probe/spark-probe55l.json`, queries in
`cells_probe55l.py`), live Spark 4.1.2 + iceberg-spark-runtime-4.1_2.13:1.11.0,
2026-09-22: `input_file_name() = _file` is `true` on every row, the upper-case
spelling folds, WHERE and scalar-argument (`substr`) shapes answer, the inner
SELECT of a derived table answers `f = _f`, the bare projection column is named
`input_file_name()`, and the residues are recorded — `count(DISTINCT …)` →
`AGGREGATE_FUNCTION_WITH_NONDETERMINISTIC_EXPRESSION` 42845, `input_file_name(1)`
→ `WRONG_NUM_ARGS.WITHOUT_SUGGESTION` 42605, self join → one side's path,
`VALUES`/no FROM → `''`, `t.snapshots` → the metadata.json path, UNION ALL →
per-row path. RePark main answers every shape `UNRESOLVED_ROUTINE` 42883.

**M-2 — the seed shape behind the battery.** A format-v2
`PARTITIONED BY (cat)` table with two appends
(`(1,'a','x'),(2,'b','y'),(4,'d','x')` then `(3,'c','x')`) and one delete
(`id = 1`) — three live rows over two data files, so per-row
`input_file_name() = _file` crosses files. The scoreboard cell's own `base3`
seeds `(1,'a','x'),(2,'b','y')` then `(3,'c','x')` with the same delete — two
live rows over two data files; Spark's recorded answer is `[[2,true],[3,true]]`.

**M-3 — the r4 reproducer lives at the collector seam, not `USE`.** The WO's
premise was `USE ice.ns` making a one-part `t` resolve to `ice.ns.t`.
Measured: `USE` updates the `CatalogRegistry` defaults and the
`SessionDefaults` carrier only; `resolve_table_ident` reads the DataFusion
planner options `catalog.default_catalog`/`default_schema`, which `USE` never
touches — a bare `FROM t` after `USE ice.ns` still resolves
`datafusion.public.t` and fails `table not found`, so the ordered `FROM t`
near-misses are unexecutable as spelled (they are pinned qualified, see C-013
note) and the `USE` CTE pin passes on semantics alone. The honest
leak-exercising pin sits at `prepare_metadata_column_sql` itself: a
`CatalogRegistry` keyed `datafusion` holding an Iceberg `public.t` under a
plain `SessionContext` makes the one-part `t` resolve to a registered Iceberg
table under untouched planner defaults — red → green and the mutation check
run on that seam. The only session-level reproducer,
`SET datafusion.catalog.default_catalog = 'ice'` +
`default_schema = 'ns'`, does reproduce the collection — and then fails
earlier at `ctx.register_table`, which writes the metadata temp view into the
live default `ice.ns` schema (`register_table does not support tables with
data`; the iceberg `SchemaProvider` attempts a real `create_table`). That is a
second, pre-existing defect — sibling `lineage_columns.rs` registers under
`datafusion.public` explicitly — filed below as a follow-up gap; the WO's
ruling scopes this round to the collector.

**M-4 — r4 red-first and mutation lines (starting head `5aabeae6`, unfixed
collector).** `cargo test -p repark-core --lib metadata_columns`: the eight
collector pins written before the fix all red (the two-part-name guard pin
landed with the fix); the V-001 line —
`a_one_part_cte_name_is_never_collected_as_the_table` panics `the outer
one-part CTE reference must not become the physical table: WITH t AS (SELECT
id, 'x' AS _file FROM __repark_mc_0 t) SELECT t._file AS `input_file_name()`
FROM __repark_mc_1 t` — the CTE reference collected and served from the
physical table, the critic's wrong answer verbatim. Mutation (the
`!self.is_cte_reference(name)` condition removed, everything else kept): the
same pin reds with the same rewritten string; reverted uncommitted.

**M-5 — r4 near misses, `_file` leg, and the temp-view measurement.** After
`USE ice.ns`: `WITH t AS (SELECT id, 'x' AS _file FROM ice.ns.t WHERE id = 2)
SELECT _file FROM t` answers exactly one row `'x'` (the CTE column wins);
`SELECT input_file_name() FROM ice.ns.t`,
`WITH c AS (SELECT 1 AS one) SELECT input_file_name() FROM ice.ns.t` and
`WITH t AS (SELECT 1 AS one) SELECT input_file_name() FROM ice.ns.t` all
answer `_file` row by row. Temp view named like the table: `CREATE TEMPORARY
VIEW t AS SELECT 1 AS id` refuses `NotImplemented("Temporary views not
supported")`; a programmatic `create_or_replace_temp_view_from("t", …)` then
`SELECT input_file_name() FROM t` refuses `[UNRESOLVED_ROUTINE]` naming
`input_file_name`, and `SELECT _file FROM t` refuses on the missing column —
the physical table is never answered, so no follow-up gap is filed for the
temp-view shape.

## PROPOSITION LEDGER — IPI-20-INPUT-FILE-NAME-1 — 2026-09-23

| Clause | Proposition (checkable) | Proof obligation | Verdict | Evidence / open question |
|---|---|---|---|---|
| C-001 | `SELECT id, input_file_name() LIKE '%.parquet' FROM ice.ns.t` answers `[[2,true],[3,true],[4,true]]`, and a scalar argument compares the full value: `concat('p:', input_file_name()) = concat('p:', _file)` is true per row and `concat('p:', input_file_name())` equals `concat('p:', _file)` per id. | `input_file_name_like_parquet_answers_true_on_every_row` green. | **PROVEN** | Cell R-INPUT-FILE-NAME plus the probe-55l function-argument shape; both collect on the Arrow path. Round 4 replaced the `substr(…,1,7)` prefix legs with full-value `concat` comparisons (V-002). Red on base: `UNRESOLVED_ROUTINE` 42883. pins: ipi-20-input-file-name-1/C-001 |
| C-002 | `SELECT id, input_file_name() = _file FROM ice.ns.t` answers `true` on every row. | `input_file_name_equals_file_on_every_row` green. | **PROVEN** | Probe 55l: `true` on every row. Red on base: `UNRESOLVED_ROUTINE`. pins: ipi-20-input-file-name-1/C-002 |
| C-003 | `INPUT_FILE_NAME()` upper case answers identically. | `input_file_name_upper_case_folds` green. | **PROVEN** | Probe 55l: same as lower case. Red on base: `UNRESOLVED_ROUTINE`. pins: ipi-20-input-file-name-1/C-003 |
| C-004 | `WHERE input_file_name() LIKE '%.parquet'` keeps all three rows, and `WHERE input_file_name() = _file` keeps all three rows. | `input_file_name_in_where_keeps_every_row` green. | **PROVEN** | Probe 55l: all rows; the `= _file` leg fails for a constant-path rewrite. Red on base: `UNRESOLVED_ROUTINE`. pins: ipi-20-input-file-name-1/C-004 |
| C-005 | The inner single-relation SELECT of a derived table rewrites: `SELECT id, f = _f FROM (SELECT id, input_file_name() f, _file _f FROM ice.ns.t) dt` answers `true` per row. | `input_file_name_in_a_derived_table_equals_file` green. | **PROVEN** | Probe 55l: `f = _f`. Red on base: `UNRESOLVED_ROUTINE`. pins: ipi-20-input-file-name-1/C-005 |
| C-006 | A bare `SELECT input_file_name() FROM ice.ns.t` projects a column literally named `input_file_name()` whose values equal `_file` row by row under `ORDER BY id`, over at least two distinct paths. | `bare_input_file_name_projection_names_the_column` green. | **PROVEN** | Probe 55l names the bare projection `input_file_name()`; the field name is asserted on the Arrow schema and the ordered values against `SELECT _file … ORDER BY id`, so a constant-path rewrite reds. Red on base: `UNRESOLVED_ROUTINE`. pins: ipi-20-input-file-name-1/C-006 |
| C-007 | The recorded residues keep today's `UNRESOLVED_ROUTINE`-class error whose text names `input_file_name`: `count(DISTINCT input_file_name())`, `input_file_name(1)`, self join `t a JOIN t b`, `FROM VALUES (1) AS v(a)`, no FROM, `` `ice`.`ns`.`t`.`snapshots` ``, and an outer SELECT over a UNION ALL derived table. | The seven `*_falls_through` tests green on base and after the change. | **PROVEN** | Probe 55l residues stay refused at this layer; every pin asserts the `[UNRESOLVED_ROUTINE]` class and text naming `input_file_name` (measured on HEAD: all seven answer `[UNRESOLVED_ROUTINE] Cannot resolve routine `input_file_name` … SQLSTATE: 42883`). Green on base (they assert today's error) and still green. pins: ipi-20-input-file-name-1/C-007 |
| C-008 | A real user column named `input_file_name` (`CREATE TABLE … (id INT, input_file_name STRING)`) is read unchanged — the trigger requires the following `(`. | `a_real_column_named_input_file_name_reads_unchanged` green. | **PROVEN** | The bare word never enters the rewrite path; the column answers `x`. Green on base and after. pins: ipi-20-input-file-name-1/C-008 |
| C-009 | With only the `input_file_name(` trigger fired, a non-query statement returns `Ok(None)` rather than the `[ICE-MC-1]` refusal: `INSERT INTO t2 SELECT id, 'x' FROM t` still lands three rows and `INSERT INTO t2 SELECT id, input_file_name() FROM t` fails with `[UNRESOLVED_ROUTINE]` naming `input_file_name`, never `[ICE-MC-1]`. | `insert_around_the_trigger_keeps_todays_answers` green. | **PROVEN** | The WO's trigger ruling for the non-query shape; measured on HEAD the second leg answers `[UNRESOLVED_ROUTINE] … SQLSTATE: 42883`. Green on base (the second leg already errors `UNRESOLVED_ROUTINE`) and still green. pins: ipi-20-input-file-name-1/C-009 |
| C-010 | An outer SELECT whose single relation merely carries the Iceberg table's name as its alias does not authorize the rewrite: `WITH c AS (SELECT id, 'x' AS _file FROM ice.ns.t) SELECT input_file_name() FROM c AS t` fails `[UNRESOLVED_ROUTINE]` naming `input_file_name` (projection leg and `WHERE input_file_name() = 'x'` leg), as do `WITH c AS (SELECT * FROM ice.ns.t) SELECT input_file_name() FROM c` and `WITH t AS (…) SELECT input_file_name() FROM t`; the near misses `FROM ice.ns.t`, `… AS t`, `… AS x` and the derived-table inner SELECT still serve `_file` row by row. | `input_file_name_over_a_cte_sharing_the_table_alias_falls_through` plus the `AS t` leg of `input_file_name_equals_file_on_every_row` green. | **PROVEN** | Round-3 critic finding V-001: on the pre-fix head the collision query answered the CTE's `'x'` (`a refused query must fail: [RecordBatch { … StringArray ["x"] … }]`); the fix matches the relation's written name to a rewrite's `original` — measured at `pre_visit_select` the name is always the original (the temp `replacement` is stamped later at `pre_visit_table_factor`; a `replacement`-only match reds all six served tests). The no-alias and CTE-named-`t` legs refused on the pre-fix head too (no alias → no `by_alias` hit; `find("t")` ≠ `ice.ns.t`). pins: ipi-20-input-file-name-1/C-010 |
| C-011 | A one-part `TableFactor::Table` matching any CTE alias in the statement — every `With` clause, nested ones included — is never collected as the physical Iceberg table: `WITH t AS (… datafusion.public.t …) SELECT input_file_name() FROM t` leaves the outer `FROM t` untouched (the qualified inner reference still rewrites), as do a `_file` outer projection, a nested CTE body (`u AS (SELECT * FROM t)`), a CTE reference inside a derived table, and a quoted `` `t` `` reference against unquoted CTE `T`; a quoted CTE `` `T` `` does not shield unquoted `t`. | The nine `repark_core::metadata_columns::tests` pins plus `input_file_name_after_use_over_a_cte_named_like_the_table_falls_through` green. | **PROVEN** | Red-first at the unfixed head (M-4): the outer CTE reference was collected and rewritten to `__repark_mc_N`, serving `t._file` from the physical table — the critic's V-001 wrong answer. The collector seam carries the red → green because `USE` does not move planner defaults in this harness (M-3). Mutation (skip removed) reds the V-001 pin identically. pins: ipi-20-input-file-name-1/C-011 |
| C-012 | Multi-part names are never CTE references: `WITH t AS (…) SELECT input_file_name() FROM ns.t` and `… FROM datafusion.public.t` still collect and rewrite the physical table, and a bare `t` with only an unrelated CTE (`WITH c AS …`) still collects. | `a_two_part_name_matching_a_cte_is_still_collected`, `a_qualified_name_matching_a_cte_is_still_collected`, `a_one_part_name_matching_no_cte_is_still_collected`, and `input_file_name_after_use_still_serves_the_table_by_name` green. | **PROVEN** | The near misses answer `_file` row by row after `USE ice.ns`; the ordered `FROM t` spellings are unexecutable in this harness (planner defaults unmoved, M-3) and are pinned qualified. pins: ipi-20-input-file-name-1/C-012 |
| C-013 | A temp view named like the table after `USE ice.ns` is not served as the physical table: `CREATE TEMPORARY VIEW t AS SELECT 1 AS id` refuses `NotImplemented`, and a programmatic temp view `t` leaves `input_file_name()` `[UNRESOLVED_ROUTINE]` and `_file` refusing on the missing column. | `input_file_name_over_a_temp_view_named_like_the_table_falls_through` green. | **PROVEN** | Measure-only per the WO; the physical table is never answered, so no follow-up gap is filed for the temp-view shape. pins: ipi-20-input-file-name-1/C-013 |

**Follow-up gap (outside this PR, per the WO ruling):** main's wildcard
expansion shares the alias-scope fallback — `sole_rewritten_relation` and
`select_touches_rewrite` accept a relation whose alias matches a rewrite
entry's alias (`by_alias`), so `WITH c AS (SELECT id AS x FROM ice.ns.t)
SELECT * FROM c AS t` expands `*` to the entry's user columns (`t.id`,
`t.data`, `t.cat`) rather than the CTE's own columns. Filed for a later unit;
`sole_rewritten_relation`, `select_touches_rewrite`, `by_alias` and
`expand_wildcards` are explicitly untouched here and the `metadata_columns.rs`
test expectations stand.

**Follow-up gap (r4, measured):** the metadata-column temp-view registration
follows the live planner defaults — `ctx.register_table(__repark_mc_N, …)`
writes into whatever `catalog.default_catalog`/`default_schema` say, so with
`SET datafusion.catalog.default_catalog = 'ice'` + `default_schema = 'ns'`
the rewrite dies at registration (`register_table does not support tables
with data` — the iceberg `SchemaProvider` treats registration as a real
`create_table` on an empty-provider contract). Sibling `lineage_columns.rs`
registers explicitly under `datafusion.public`; aligning this path is a
separate change the WO's collector ruling does not cover. Also measured:
`USE <cat>.<ns>` never updates the planner options, so `USE`-scoped
one-part names resolve against `datafusion.public` regardless — the gap
between session defaults and planner defaults is a named follow-up for
whoever owns `use_ddl.rs` / `router.rs`.

**Class A sweep — relation-scope leak (r4):**

| Shape | Result |
|---|---|
| `WITH t AS (…) SELECT input_file_name() FROM t` | collected pre-fix → served the physical table (wrong answer); fixed — skipped, falls through. pin `a_one_part_cte_name_is_never_collected_as_the_table` |
| `WITH t AS (…) SELECT _file FROM t` | same leak on the `_file` path; fixed — CTE column wins. pin `a_metadata_column_projection_over_a_cte_is_never_served_from_the_table` |
| Nested CTE `u AS (SELECT * FROM t)` | inner `t` collected pre-fix; fixed — never collected. pin `a_nested_cte_reference_inside_another_cte_is_never_collected` |
| CTE inside derived table `(SELECT * FROM t) dt` | collected pre-fix; fixed — never collected. pin `a_cte_reference_inside_a_subquery_is_never_collected` |
| `WITH t … FROM ns.t` / `FROM datafusion.public.t` | still collected — qualified names are never CTE references. pins `a_two_part_name_matching_a_cte_is_still_collected`, `a_qualified_name_matching_a_cte_is_still_collected` |
| Quoted `` `t` `` ref vs unquoted CTE `T` | folded match — skipped. pin `a_quoted_reference_matches_the_unquoted_cte_after_folding` |
| Quoted CTE `` `T` `` vs unquoted ref `t` | no match — still collected. pin `a_quoted_cte_alias_does_not_shield_a_differently_folded_table` |
| Temp view `t` after `USE ice.ns` | measure-only: `CREATE TEMPORARY VIEW` refuses `NotImplemented`; programmatic view → `[UNRESOLVED_ROUTINE]` / missing-column — never the physical table. pin `input_file_name_over_a_temp_view_named_like_the_table_falls_through` |

**Class B sweep — partial-value pin (r4):**

| Assertion | Disposition |
|---|---|
| `substr(input_file_name(), 1, 7) = substr(_file, 1, 7)` (scalar argument) | replaced — `concat('p:', input_file_name()) = concat('p:', _file)` per row plus the `concat('p:', …)` string equality leg (V-002) |
| `input_file_name() LIKE '%.parquet'` (projection) | kept — asserts the full boolean per row beside the full-value concat legs over the same relation |
| `WHERE input_file_name() LIKE '%.parquet'` | kept — full id set asserted, beside the `WHERE input_file_name() = _file` leg |
| `count(*) = 3` in the INSERT guard | full value of that query — unchanged |
| `distinct.len() >= 2` | fixture precondition (the seed spans ≥2 data files), not a value pin — unchanged |
| every `id_bool_pairs` / `strings` / `field_names` assertion | full-value already — unchanged |
| the PR's pytest files | none exist on this branch — the sweep is vacuous |

## Gates

| Command | Result |
|---|---|
| `cargo test -p repark-spark --lib input_file_name` (base `f411192d`, tests-only commit `557f2029`) | 6 red (served shapes, `UNRESOLVED_ROUTINE`), 9 green (fall-through + guards) |
| `cargo test -p repark-spark --lib input_file_name` (head) | 15 passed |
| `cargo test -p repark-spark --lib metadata_columns` (head) | 14 passed, expectations unchanged |
| `python3 /tmp/oc-worker/_lib/comment_ban.py /tmp/xo55-ifn origin/main HEAD` | exit 0 (`hits=0`) |
| `bash /tmp/oc-worker/_lib/build-slot.sh /tmp/oc-worker/_lib/local-gate.sh xo55-ifn …` | `CB=0 R=0 T=0 U=0 L=0` — release rc=0; `input_file_name` 15/15, `metadata_columns` 14/14, `repark-core --lib` 683 passed; facade unit 55 passed; parity live 55 passed |
| `make rust-clippy` | exit 0 — workspace `--all-targets`, `-D warnings`, clean |
| `make rust-panic-ban` | exit 0 — both clippy invocations (`--lib --bins --exclude repark-python`, then `repark-python --lib`) clean |
| `make check-rust-file-size` | `rust-file-size: 822 files clean (default ceiling 1000; 38 exceptions)` |
| `bash scripts/check_map_md.sh --base origin/main` | exit 0 |
| `python3 scripts/check_ledger_grammar.py` | `ledger-grammar: 251 live ledgers clean (2330 clauses, 2896 pinned clause ids, 2 exception rows)` |
| scoreboard replay `--only R-INPUT-FILE-NAME` | RePark `[[2,true],[3,true]]` = Spark `[[2,true],[3,true]]` (`out/repark-ifn1.json`, `out/spark-core.json`) |
| r2fix: `cargo test -p repark-spark --lib input_file_name` (head) | 15 passed — the tightened pins stay green |
| r2fix: `cargo test -p repark-spark --lib metadata_columns` (head) | 14 passed, expectations unchanged |
| r2fix: `make rust-clippy`; `make rust-panic-ban` | both exit 0 |
| r2fix: `bash scripts/check_map_md.sh --base origin/main`; `python3 scripts/check_ledger_grammar.py`; `comment_ban.py` | all exit 0 (`hits=0`) |
| r2fix mutation check | bare-projection rewrite forced to `'x.parquet'` → `bare_input_file_name_projection_names_the_column` red (`left: ["x.parquet", "x.parquet", "x.parquet"]` vs the real `_file` paths); reverted uncommitted |
| r3fix: `cargo test -p repark-spark --lib input_file_name` (pre-fix, tests-only commit) | `input_file_name_over_a_cte_sharing_the_table_alias_falls_through` RED — `a refused query must fail: [RecordBatch { … StringArray ["x"] … }]`, the CTE value instead of a refusal; 15 green |
| r3fix: `cargo test -p repark-spark --lib input_file_name` (head) | 16 passed |
| r3fix: `cargo test -p repark-spark --lib metadata_columns` (head) | 14 passed, expectations unchanged |
| r3fix mutation check | alias fallback restored in `sole_input_file_name_relation` → the CTE collision test red again (`a refused query must fail: [RecordBatch { … StringArray ["x"] … }]`); reverted uncommitted |
| r3fix name-at-visit measurement | `replacement`-only match in `sole_input_file_name_relation` → all six served tests red (`UNRESOLVED_ROUTINE`); the SELECT carries the rewrite's `original` name at `pre_visit_select`, so only `find(name)` is kept |
| r3fix: `make rust-clippy`; `make rust-panic-ban` | both exit 0 |
| r3fix: `bash scripts/check_map_md.sh --base origin/main`; `python3 scripts/check_ledger_grammar.py`; `comment_ban.py` | all exit 0 (`hits=0`; `251 live ledgers clean, 2331 clauses`) |
| r4fix: `cargo test -p repark-core --lib metadata_columns` (pre-fix, tests written first) | the eight collector pins RED (M-4); the wrong answer is the rewritten SQL itself: outer `FROM t` → `__repark_mc_1`, projection → `t._file` |
| r4fix: `cargo test -p repark-core --lib metadata_columns` (head) | 9 passed |
| r4fix: `cargo test -p repark-spark --lib input_file_name` (head) | 19 passed |
| r4fix: `cargo test -p repark-spark --lib metadata_columns` (head) | 14 passed, expectations unchanged |
| r4fix mutation check | `!self.is_cte_reference(name)` removed → `a_one_part_cte_name_is_never_collected_as_the_table` red, identical rewritten-SQL line (M-4); reverted uncommitted |
| r4fix: `make rust-clippy`; `make rust-panic-ban` | both exit 0 |
| r4fix: `bash scripts/check_map_md.sh --base origin/main`; `python3 scripts/check_ledger_grammar.py`; `comment_ban.py` | all exit 0 (`hits=0`; `252 live ledgers clean, 2350 clauses`) |

## COVERAGE_ATTESTATION

```
COVERAGE_ATTESTATION:
  pr_unit: ipi-20-input-file-name-1
  complete: true
  categories:
    - id: AT-1
      status: ATTACKED
      evidence: Each clause walked to its recorded Spark 4.1.2 probe-55l answer — value pins on the Arrow path for the six served shapes (C-001..C-006) and [UNRESOLVED_ROUTINE]-class pins naming input_file_name for the seven residues (C-007); red-first run on the base commit shows the six served tests failing UNRESOLVED_ROUTINE and the nine fall-through tests passing.
      artifacts: [crates/repark-spark/src/tests/input_file_name.rs]
    - id: AT-2
      status: ATTACKED
      evidence: The shared seed is the metadata-columns two-append plus one-delete fixture, so input_file_name() is asserted equal to _file per row across two data files rather than a single constant path; the upper-case, WHERE, function-argument and derived-table spellings each carry their own pin.
      artifacts: [crates/repark-spark/src/tests/input_file_name.rs]
    - id: AT-3
      status: ATTACKED
      evidence: The rewrite requires the SELECT's own single relation to be the Iceberg table by written name (C-010 — an alias match alone does not qualify), and collection itself is CTE-aware — a one-part name matching any CTE alias in the statement (nested With clauses included, on the planner's own fold) is never treated as the physical table (C-011) while qualified names stay collected (C-012); a zero-argument unquoted call, no FILTER and no OVER; the per-SELECT call visitor does not descend into a blocking aggregate's arguments or a query nested inside a projection/WHERE expression, and every nested SELECT is visited on its own and rewritten against its own sole relation; a real column of the same name (C-008) and a non-query statement (C-009) prove the trigger stays narrow.
      artifacts: [crates/repark-core/src/metadata_columns.rs]
    - id: AT-4
      status: ATTACKED
      evidence: Resolution runs per query through the existing CatalogRegistry load and temp-view pin/release path; no global mutable state beyond the pre-existing TEMP_VIEW_SEQ counter and no env reads added.
      artifacts: [crates/repark-core/src/metadata_columns.rs]
    - id: AT-5
      status: N/A
      justification: Read-only rewrite over the session's own catalogs; no auth, secret, or injection surface added.
    - id: AT-6
      status: ATTACKED
      evidence: The served shapes were refused on main and are now pinned to the recorded Spark answers; the fall-through pins assert the unchanged UNRESOLVED_ROUTINE-class error so no prior answer is silently altered.
      artifacts: [docs/spark-sql-iceberg-parity.md, crates/repark-spark/src/tests/input_file_name.rs]
    - id: AT-7
      status: N/A
      justification: A token scan plus an AST rewrite of an already-parsed statement; no added materialization or hot-loop allocation.
    - id: AT-8
      status: ATTACKED
      evidence: The change reuses the existing MetadataColumnsTableProvider rewrite rather than a parallel mechanism; _file is the same column the ICE-METADATA-COLS-1 pins already prove; no dependency, pin or manifest move.
      artifacts: [crates/repark-core/src/metadata_columns.rs, crates/repark-spark/src/tests/metadata_columns.rs]
    - id: AT-9
      status: ATTACKED
      evidence: Mis-scoped shapes surface the unchanged planner error naming input_file_name (UNRESOLVED_ROUTINE 42883), so a missed rewrite is diagnosable from the error rather than silent.
      artifacts: [crates/repark-spark/src/tests/input_file_name.rs]
    - id: AT-10
      status: ATTACKED
      evidence: Every clause carries a pins: citation in crates/repark-spark/src/tests/map.md and crates/repark-core/src/map.md; the registry row ICE-MC-IFN-1 states the served and unresolved shapes; C-006 asserts the Arrow field name, not rendered text.
      artifacts: [crates/repark-spark/src/tests/map.md, crates/repark-core/src/map.md, docs/spark-sql-iceberg-parity.md]
```

Every clause above is PROVEN against the recorded Spark 4.1.2 probe-55l answers
or the unchanged pre-existing refusal; no clause is OPEN. Round 4 pins the
collector seam at `prepare_metadata_column_sql` because `USE` does not move
planner defaults in this harness (M-3) — the spark-level `USE` pins stand as
semantic pins beside it, and the `SET`-defaults registration failure is filed
as a follow-up gap rather than widened into this change. Touched files per the
Scope paragraph; the fork, `Cargo.toml`/`Cargo.lock`, `STATUS.md`,
`crates/repark-functions/` and `crates/repark-sql/` are untouched.
`make verify`, the full facade suite and the whole-workspace test were not run
per the work order's gate list; the gates table above is the proof.
