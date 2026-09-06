# Unit ledger — FNP-8 · the eleven Spark higher-order functions with Python lambdas, both doors

**Retires:** this ledger moves to `../completed/` in the unit's last commit.
This file closes when FNP-8 merges, or when the owner closes the slate row.

**Unit:** FNP-8 · **Date:** 2026-09-06 · **Executor:** Muse Spark, Actor ·
**Branch:** `feat/fnp-8` · **Base:** `e100f72d`
**Model:** muse-spark-1.3
**risk_tier:** standard.

Spark is the oracle. Live PySpark 4.1.2, zulu-17, `TZ=UTC`, ANSI on and off, 2026-09-06.
The campaign charter is [fnp-0-charter-ledger.md](fnp-0-charter-ledger.md); the unit rows are
the charter's C-003 (Column entry point with a Python lambda), C-004 (Spark SQL door and
`F.expr` with `x -> y` syntax) and C-012 (both doors resolve the same kernel). The kernels
and the Column-door lambda tracing are FNP-4c's delivered work; this unit's mechanism is the
SQL-door executing parse, and its proof is the oracle-first differential matrix across both
doors. Clauses discharged from the campaign charter: C-001 (Rust-owned expression per name),
C-002 (no Python row compute), C-003/C-004 (this unit), C-010 (no raised ceiling), C-012
(same kernel both doors).

## C-001 — the roster, measured first

Every name below was re-derived from `pyspark.sql.functions` 4.1.2 (`__all__` membership and
signatures, 2026-09-06) and measured on repark (release module) **before** any code was
written. The live-Spark column is transcribed into "Oracle" below once the JVM runs.

| Name | PySpark 4.1.2 signature | repark Column door, before code | repark SQL door, before code |
|---|---|---|---|
| `transform` | `(col, f: 1- or 2-arg)` | answers (FNP-4c) | refuses: `No field named x` |
| `filter` | `(col, f: 1- or 2-arg)` | answers (FNP-4c) | refuses: `No field named x` |
| `exists` | `(col, f: 1-arg)` | answers (FNP-4c) | refuses: `ParserError` (`EXISTS` keyword under Generic) |
| `forall` | `(col, f: 1-arg)` | answers (FNP-4c) | refuses: `No field named x` |
| `aggregate` | `(col, initialValue, merge: 2-arg, finish: 1-arg = None)` | answers (FNP-4c) | refuses: `No field named acc` |
| `reduce` | alias of `aggregate`, byte-identical signature | answers (FNP-4c) | refuses: `No field named acc` |
| `zip_with` | `(left, right, f: 2-arg)` | answers (FNP-4c) | refuses: `No field named x` |
| `transform_keys` | `(col, f: 2-arg)` | answers (FNP-4c) | refuses: `No field named k` |
| `transform_values` | `(col, f: 2-arg)` | answers (FNP-4c) | refuses: `No field named k` |
| `map_filter` | `(col, f: 2-arg)` | answers (FNP-4c) | refuses: `No field named k` |
| `map_zip_with` | `(col1, col2, f: 3-arg)` | answers (FNP-4c) | refuses: `No field named k` |

`F.expr` with a lambda, before code: a column-free spelling
(`F.expr("transform(array(1,2), x -> x + 1)")`) refuses — the throwaway context parses Generic,
so `->` is the JSON arrow, not a lambda. A column-referencing spelling refuses under the
already-filed §7 `EX-FN-4` (no DataFrame-bound expr path), which this unit does not own.

The before-code SQL-door refusal shape is the Generic-dialect misparse, not a loud valve:
`x -> x + 1` parses as the JSON `->` operator applied to columns named `x`, and only the
plan step refuses (`No field named x`). The fix keeps that exact behavior for every query
without `->` and parses lambda queries the way Spark does.

## Proposition ledger

| ID | Clause | Proof obligation | Verdict |
|---|---|---|---|
| C-001 | The roster above is the exact set this unit takes, each name re-derived from `pyspark.sql.functions` 4.1.2 and measured on repark before any code was written and on live Spark after; every name reaches both doors and no size ceiling is raised to get it there. | The roster table, the oracle below, `test_fnp_8_sql_door.py`, and the unchanged ceiling tables in `check_lib_py.py` / `check_rust_file_size.py`. | **OPEN** |
| C-002 | No facade function performs row-level computation in Python. | The census `PY_COMPUTE` bucket stays 2 (both the sanctioned UDF path); this unit adds no facade Python that touches rows. | **OPEN** |
| C-003 | The eleven answer the oracle matrix through the Column entry point with a Python lambda — every arity Spark accepts, traced once at plan time, loud on an untraceable lambda. | FNP-4c's mechanism plus this unit's Column-door matrix cells in `test_fnp_8_sql_door.py`; the arity pins stay in `test_fnp4c_higher_order.py`. | **OPEN** |
| C-004 | The same eleven answer through the Spark SQL door and column-free `F.expr` with `x -> y` / `(x, i) -> y` syntax, resolving the same kernel the Column door resolves. | The arrow-gated executing parse plus per-name SQL-door pins and the `by_name` same-table test. | **OPEN** |
| C-005 | Every divergence this unit measured is filed as a §7 row with a pin, and no row claims parity it does not have. | §7 rows below; each carries its pin. | **OPEN** |
| C-006 | Every new pin is invertible and the gates are green on real exit codes. | The mutation table below; `make ci`, `make verify`, the facade and parity suites, and the live tier under `REPARK_PARITY_LIVE=1`. | **OPEN** |

## Red-first

(TBD: the new pin files are red on the base — the SQL-door cells refuse per the roster table
above — measured before the mechanism lands, not assumed.)

## Oracle (live PySpark 4.1.2, 2026-09-06, zulu-17, `TZ=UTC`, `local[2]`, ANSI on and off)

(TBD: recorded verbatim from `spark.sql(...).toArrow()` — value AND Arrow type AND
nullability. ANSI off quoted only where it differs.)

## Mechanism

The Spark door already parses user SQL with `DatabricksDialect` at the routing pre-parse
(`parse_single_normalized`) but re-parses with the session Generic dialect at the EXECUTING
parse (`execute_passthrough` → `state.sql_to_statement(sql, &session_dialect)`), where `->`
is the JSON arrow instead of a lambda. The session-wide flip is deferred FNP-4b's write-path
change; this unit does not do it. Instead the executing parse takes `Dialect::Databricks`
when — and only when — the SQL carries a `Token::Arrow` outside strings and comments, and
the session dialect otherwise. No working query in the tree uses `->` (measured by search:
only `kernel_eval.rs`, which is already Databricks), so the gate newly-enables only SQL that
refuses today. The same gate arms the `PyColumn.sql` throwaway context, which runs no
internal SQL. CTAS routes its SELECT through `execute_passthrough`, so it is covered by the
same change.

Two analyzer findings fell out of the first door tests, both fixed in the mechanism. First,
SQL planning binds lambda parameters before analysis, so `SparkIntegerLiteral`'s narrowing
of `make_array(1, 2, 3)` to `List<Int32>` left the bound `x` at `Int64` and the physical
planner refused (`LambdaVariable field and schema field mismatch`). `LambdaRebind`, last in
`analyzer_rules()`, re-resolves every binding from the current value types. Second, the
physical planner remaps lambda bodies by referenced position, so an `(x, i)` body mentioning
only `i` reads the element slot; the same rule packs such a body into the facade's own
`named_struct` + `get_field("__hof_body")` shape (the exact expression
`functions_lambda._keep_lambda_params` builds), which is why the rule skips unary and
fully-referenced bodies — the Column door's plans stay byte-identical.

## Design decisions taken inside the unit

| Decision | Reason |
|---|---|
| The executing parse is arrow-gated, not session-flipped | The FNP-4b flip moves double-quoted identifiers and struct literals for every query and every internal statement; the gate moves only queries carrying `->`, all of which refuse today. |
| `exists` needs no keyword rewrite | sqlparser 0.62 parses `exists(` as a function under `DatabricksDialect` unless `(SELECT` / `(WITH` follows (verified in the vendored source). |
| `F.expr` with a column reference stays `EX-FN-4` | Column binding needs the DataFrame-bound expr path, a declared-BACKLOG seam this unit does not own; the lambda parsing lands and column-free spellings answer. |

## Mutation

(TBD: one knob per name, each red on the new pin files; plus the gate knob that forces the
session dialect. Knobs applied to the shipped source, measured, reverted.)

## Gates (real exit codes, 2026-09-06)

(TBD.)

## Disk (AGENTS.md "Resource discipline")

Checked before the first build: **875 GB free of 1.8 TB** (50% used). The lane reuses the
shared `target/` and the shared cargo registry; `.ivy2` (182 MB) is a copy of `~/.ivy2.5.2`
kept for the live legs and is git-excluded, as are `scratch/` and `handback.json`.

## Delivery

(TBD.)

## Out of scope, observed

(TBD.)

The `COVERAGE_ATTESTATION` block is filed here when no clause stays `OPEN`.
