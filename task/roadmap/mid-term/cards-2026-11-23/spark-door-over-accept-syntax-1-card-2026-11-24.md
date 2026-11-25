# Card SPARK-DOOR-OVER-ACCEPT-SYNTAX-1: the Spark door answers some statements Spark rejects

**Date:** 2026-11-24. **Filed by:** the orchestrator (`claude-opus-5-5`), from an outside review of the SQL text
rewrites, checked here against Spark. **Severity:** S3 (owner, 2026-11-24). **Status:** fixed on branch `fix/spark-door-over-accept-syntax-1` (2026-11-24); five moved cells wait for a ruling (below).

## Measured

Measured on a build of main at `fa7d75e4` and on Spark 4.1.2, in both ANSI modes, unless a row says otherwise.

| Statement | Spark 4.1.2 | RePark |
|---|---|---|
| `SELECT 1e2foo AS v` | `[UNRESOLVED_COLUMN]` | 100.0 |
| `SELECT 1e2.foo AS v` | `[INVALID_EXTRACT_BASE_FIELD_TYPE]` | 100.0 |
| `SELECT named_struct('a', 1, 'b', no_such_col_xyz).a` | `[UNRESOLVED_COLUMN]` | 1 |
| `SELECT named_struct('a', 1, 'b', CAST('notanint' AS INT)).a` (ANSI on) | `[CAST_INVALID_INPUT]` | 1 |
| `SELECT * EXCLUDE (b) FROM (SELECT 1 AS a, 2 AS b) t` | `ParseException` | 1 |
| `DELETE ice.ns.t WHERE a = 1L` (no `FROM`; measured by the review on `e80e6bc8`) | `ParseException` | deletes |

**Also measured: one silent naming difference.** `SELECT drop temporary FROM (SELECT 1 AS drop) t` names its column
`temporary` in Spark and `drop` in RePark, because a text rewrite drops the alias. It is S3 with the rest (owner,
2026-11-24).

## Cause (named by the outside review)

These are text rewrites that run before parsing:
- the exponent-suffix fallback;
- the struct-field splice, which drops the sibling fields before they are resolved;
- the wildcard-except splice;
- the `DROP TEMPORARY` region planner.

## Update 2026-11-24: the fix

Measured on 122 cells (750 door cells) across `spark.sql`, temp-view bodies, `F.expr`, `selectExpr`,
`filter(str)` and Iceberg `DELETE` / `UPDATE` / `MERGE`, both ANSI modes
([evidence](../../../wo/spark-door-over-accept-syntax-1/evidence/summary.md)). Also measured: the
`DELETE` without `FROM` and without `WHERE` emptied the table, and `DROP TEMPORARY VIEW` / `TABLE`,
which Spark 4.1.2 refuses at `'VIEW'` / `'TABLE'`, answered.

- The exponent fallback reads `1e2foo` as one identifier and keeps `1e2.foo` a field access.
- The struct splice passes every sibling; constant siblings evaluate, column siblings are dropped
  after analysis, as Spark prunes them.
- `* EXCLUDE (…)`, `DELETE <table>` and `DROP TEMPORARY VIEW` / `TABLE` refuse with Spark's
  `PARSE_SYNTAX_ERROR`; `DROP TEMPORARY FUNCTION` still drops nothing; `drop temporary` keeps its alias.

**Ruling needed:** five cells that equal Spark on main refuse at head, because the siblings now plan and
evaluate. Each sibling also refuses on its own on main: `named_struct('a', 1, 'b', 'x' + 1).a` with
ANSI off (four doors; RePark cannot plan `'x' + 1` with ANSI off), and
`F.expr("named_struct('a', 1, 'b', 1 / 0).a")` with ANSI off (`F.expr("1 / 0")` raises
`DIVIDE_BY_ZERO` with ANSI off).

### Still unlike Spark (measured at head)

- **Refusal class:** unresolved identifiers (`1e2foo`, `1e2Dx`, `-1e2foo`, a struct sibling) raise the
  door's `AnalysisException: Schema error: No field named …` on `spark.sql`, temp views and `F.expr`
  (Spark `UNRESOLVED_COLUMN`); field access on a scalar (`1e2.foo`, `1e2.D`, `1e2.BD`) raises the door's
  `Cannot access field` / `Dot access not supported` (Spark `INVALID_EXTRACT_BASE_FIELD_TYPE`);
  `1.5e10foo` raises `No field named` (Spark `INVALID_EXTRACT_BASE_FIELD_TYPE`); `abs(1, 2)` as a
  sibling raises `AnalysisException` (Spark `WRONG_NUM_ARGS`); `'x' + 1` as a sibling with ANSI on
  raises `Cannot coerce` (Spark `CAST_INVALID_INPUT`).
- **`F.expr` takes no alias:** `F.expr("drop temporary")`, ``F.expr("1e2`foo`")``, `F.expr("1e2 AS
  foo")`, `F.expr("*")`, `F.expr("* EXCEPT (b)")` refuse; Spark answers.
- **Display:** an unaliased `named_struct('a', 1, 'b', 'x').a` is named `named_struct('a', 1, 'b', 'x').a`;
  Spark names it `named_struct(a, 1, b, x).a`.
- **Unchanged neighbours:** `struct(1 AS a, 2 AS b).a` is nullable (Spark non-null); `struct(1, 2).col1`
  and `SELECT s.* FROM (SELECT named_struct(…) AS s) t` refuse; `SELECT a exclude FROM …` refuses;
  `DROP TEMPORARY VARIABLE IF EXISTS v` refuses; `SELECT *, a + 1` and `2 * exclude` type `bigint`
  (Spark `int`).

Up: [map.md](map.md).
