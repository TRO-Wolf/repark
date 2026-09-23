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
and this ledger. The fork, every `Cargo.toml`, `Cargo.lock`, `STATUS.md`,
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

## PROPOSITION LEDGER — IPI-20-INPUT-FILE-NAME-1 — 2026-09-23

| Clause | Proposition (checkable) | Proof obligation | Verdict | Evidence / open question |
|---|---|---|---|---|
| C-001 | `SELECT id, input_file_name() LIKE '%.parquet' FROM ice.ns.t` answers `[[2,true],[3,true],[4,true]]`, and `substr(input_file_name(),1,7)` inside a scalar argument equals `substr(_file,1,7)` per row. | `input_file_name_like_parquet_answers_true_on_every_row` green. | **PROVEN** | Cell R-INPUT-FILE-NAME plus the probe-55l function-argument shape; both collect on the Arrow path. Red on base: `UNRESOLVED_ROUTINE` 42883. pins: ipi-20-input-file-name-1/C-001 |
| C-002 | `SELECT id, input_file_name() = _file FROM ice.ns.t` answers `true` on every row. | `input_file_name_equals_file_on_every_row` green. | **PROVEN** | Probe 55l: `true` on every row. Red on base: `UNRESOLVED_ROUTINE`. pins: ipi-20-input-file-name-1/C-002 |
| C-003 | `INPUT_FILE_NAME()` upper case answers identically. | `input_file_name_upper_case_folds` green. | **PROVEN** | Probe 55l: same as lower case. Red on base: `UNRESOLVED_ROUTINE`. pins: ipi-20-input-file-name-1/C-003 |
| C-004 | `WHERE input_file_name() LIKE '%.parquet'` keeps all three rows. | `input_file_name_in_where_keeps_every_row` green. | **PROVEN** | Probe 55l: all rows. Red on base: `UNRESOLVED_ROUTINE`. pins: ipi-20-input-file-name-1/C-004 |
| C-005 | The inner single-relation SELECT of a derived table rewrites: `SELECT id, f = _f FROM (SELECT id, input_file_name() f, _file _f FROM ice.ns.t) dt` answers `true` per row. | `input_file_name_in_a_derived_table_equals_file` green. | **PROVEN** | Probe 55l: `f = _f`. Red on base: `UNRESOLVED_ROUTINE`. pins: ipi-20-input-file-name-1/C-005 |
| C-006 | A bare `SELECT input_file_name() FROM ice.ns.t` projects a column literally named `input_file_name()` whose values are the parquet paths. | `bare_input_file_name_projection_names_the_column` green. | **PROVEN** | Probe 55l names the bare projection `input_file_name()`; the field name is asserted on the Arrow schema. Red on base: `UNRESOLVED_ROUTINE`. pins: ipi-20-input-file-name-1/C-006 |
| C-007 | The recorded residues keep today's `UNRESOLVED_ROUTINE`-class error whose text names `input_file_name`: `count(DISTINCT input_file_name())`, `input_file_name(1)`, self join `t a JOIN t b`, `FROM VALUES (1) AS v(a)`, no FROM, `` `ice`.`ns`.`t`.`snapshots` ``, and an outer SELECT over a UNION ALL derived table. | The seven `*_falls_through` tests green on base and after the change. | **PROVEN** | Probe 55l residues stay refused at this layer; every pin asserts the error text contains `input_file_name`. Green on base (they assert today's error) and still green. pins: ipi-20-input-file-name-1/C-007 |
| C-008 | A real user column named `input_file_name` (`CREATE TABLE … (id INT, input_file_name STRING)`) is read unchanged — the trigger requires the following `(`. | `a_real_column_named_input_file_name_reads_unchanged` green. | **PROVEN** | The bare word never enters the rewrite path; the column answers `x`. Green on base and after. pins: ipi-20-input-file-name-1/C-008 |
| C-009 | With only the `input_file_name(` trigger fired, a non-query statement returns `Ok(None)` rather than the `[ICE-MC-1]` refusal: `INSERT INTO t2 SELECT id, 'x' FROM t` still lands three rows and `INSERT INTO t2 SELECT id, input_file_name() FROM t` fails with an error naming `input_file_name`, never `[ICE-MC-1]`. | `insert_around_the_trigger_keeps_todays_answers` green. | **PROVEN** | The WO's trigger ruling for the non-query shape. Green on base (the second leg already errors `UNRESOLVED_ROUTINE`) and still green. pins: ipi-20-input-file-name-1/C-009 |

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

## COVERAGE_ATTESTATION

```
COVERAGE_ATTESTATION:
  pr_unit: ipi-20-input-file-name-1
  complete: true
  categories:
    - id: AT-1
      status: ATTACKED
      evidence: Each clause walked to its recorded Spark 4.1.2 probe-55l answer — value pins on the Arrow path for the six served shapes (C-001..C-006) and error-text pins naming input_file_name for the seven residues (C-007); red-first run on the base commit shows the six served tests failing UNRESOLVED_ROUTINE and the nine fall-through tests passing.
      artifacts: [crates/repark-spark/src/tests/input_file_name.rs]
    - id: AT-2
      status: ATTACKED
      evidence: The shared seed is the metadata-columns two-append plus one-delete fixture, so input_file_name() is asserted equal to _file per row across two data files rather than a single constant path; the upper-case, WHERE, function-argument and derived-table spellings each carry their own pin.
      artifacts: [crates/repark-spark/src/tests/input_file_name.rs]
    - id: AT-3
      status: ATTACKED
      evidence: The rewrite requires a sole rewritten Iceberg relation, a zero-argument unquoted call, no FILTER and no OVER, and never descends into aggregate arguments or nested subqueries; a real column of the same name (C-008) and a non-query statement (C-009) prove the trigger stays narrow.
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
or the unchanged pre-existing refusal; no clause is OPEN. Touched files per the
Scope paragraph; the fork, `Cargo.toml`/`Cargo.lock`, `STATUS.md`,
`crates/repark-functions/` and `crates/repark-sql/` are untouched.
`make verify`, the full facade suite and the whole-workspace test were not run
per the work order's gate list; the gates table above is the proof.
