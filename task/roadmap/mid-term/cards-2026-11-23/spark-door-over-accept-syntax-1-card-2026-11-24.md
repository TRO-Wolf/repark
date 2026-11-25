# Card SPARK-DOOR-OVER-ACCEPT-SYNTAX-1: the Spark door answers some statements Spark rejects

**Date:** 2026-11-24. **Filed by:** the orchestrator (`claude-opus-5-5`), from an outside review of the SQL text
rewrites, checked here against Spark. **Severity:** S3 (owner, 2026-11-24). **Status:** fixed on branch `fix/spark-door-over-accept-syntax-1` (2026-11-24, PR #1434) for four seats; the struct seat stays as on main
(delta 1, option B).

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

## Update 2026-11-24: the fix (after delta 1)

Measured on 141 cells (876 door cells) across `spark.sql`, temp-view bodies, `F.expr`, `selectExpr`,
`filter(str)` and Iceberg `DELETE` / `UPDATE` / `MERGE`, both ANSI modes
([evidence](../../../wo/spark-door-over-accept-syntax-1/evidence/summary.md)), and on the verifier's
405-cell attack. Also measured: the `DELETE` without `FROM` and without `WHERE` emptied the table, and
`DROP TEMPORARY VIEW` / `TABLE`, which Spark 4.1.2 refuses with `[PARSE_SYNTAX_ERROR] Syntax error at or
near 'VIEW'` (`'TABLE'`), answered.

- The exponent fallback reads `1e2foo` as one identifier and keeps `1e2.foo` a field access.
- `* EXCLUDE (…)`, `DELETE <table>` and `DROP TEMPORARY VIEW` / `TABLE` (also after `EXPLAIN`) refuse
  with Spark's `PARSE_SYNTAX_ERROR`; `DROP TEMPORARY FUNCTION`, with or without a leading `EXPLAIN`,
  plans as before; `drop temporary` keeps its alias.
- **The struct splice is unchanged from main** (delta 1, option B). Two attempts to model Spark's sibling
  analysis (every sibling planned; then ruling 1's option C) each moved cells that main answers as Spark
  does. Option C is superseded.

On the probe, 144 door cells move to Spark's answer or class and none that equal Spark on main moves
off it. Four `F.expr` cells move from a wrong column name to a refusal (below). On the attack, the only
cells whose value equals Spark on main and not on head are two of those `F.expr` cells.

### Still unlike Spark (measured at head)

- **Struct sibling over-accepts (present on main):** `named_struct('a', 1, 'b', no_such_col_xyz).a`
  answers `1` (Spark `UNRESOLVED_COLUMN`, both ANSI modes), and
  `named_struct('a', 1, 'b', CAST('notanint' AS INT)).a` answers `1` with ANSI on (Spark
  `CAST_INVALID_INPUT`); so do the other sibling shapes the splice drops (`1 / 0`, `abs(1, 2)`, nested
  access, the `UPDATE` / `DELETE` forms). A sibling-analysis fix must keep the verifier's 240 cells that
  answer `1` on main and in Spark: siblings Spark coerces and RePark cannot type (`upper(5)`, `sqrt('4')`,
  `date_add('2024-01-01', 1)`, `reverse(123)`, `upper(a)`), lateral column aliases, Spark built-ins RePark
  lacks (`raise_error`, `monotonically_increasing_id`, `current_user`), constants Spark never evaluates
  (`assert_true(false)`, `typeof(1 / 0)`, `coalesce(1, 'x')`, `if(true, 1, 'x')`), a scalar-subquery
  sibling on the DataFrame doors, and `UPDATE … SET b = named_struct('p', 5.0D, 'q', 1 / 0).p` with ANSI
  on, which Spark performs.
- **String arithmetic (present on main):** RePark cannot plan `'x' + 1`. `SELECT 'x' + 1` refuses with
  `Cannot coerce arithmetic expression Utf8 + Int64` in both ANSI modes (Spark: `NULL` with ANSI off,
  `CAST_INVALID_INPUT` with ANSI on).
- **`F.expr` division with ANSI off (present on main):** `F.expr("1 / 0")` raises `DIVIDE_BY_ZERO`;
  Spark, `spark.sql` and `selectExpr` answer `NULL`.
- **Refusal class:** unresolved identifiers (`1e2foo`, `1e2Dx`, `-1e2foo`) raise the door's
  `AnalysisException: Schema error: No field named …` on `spark.sql`, temp views and `F.expr` (Spark
  `UNRESOLVED_COLUMN`); field access on a scalar (`1e2.foo`, `1e2.D`, `1e2.BD`) raises the door's
  `Cannot access field` / `Dot access not supported` (Spark `INVALID_EXTRACT_BASE_FIELD_TYPE`);
  `1.5e10foo` raises `No field named` (Spark `INVALID_EXTRACT_BASE_FIELD_TYPE`).
- **`F.expr` takes no alias:** `F.expr("1e2 AS foo")`, `F.expr("*")`, `F.expr("* EXCEPT (b)")` refuse on
  main and head; Spark answers. **New at head:** `F.expr("drop temporary")` and ``F.expr("1e2`foo`")``
  refuse in both ANSI modes, where main answered with a wrong column name (`drop temporary` and
  ``1e2`foo` ``) and Spark names them `temporary` and `foo`.
- **`EXPLAIN` shape:** `EXPLAIN DROP TEMPORARY FUNCTION IF EXISTS f` answers RePark's two-column
  `plan_type` / `plan` rows, as on main; Spark answers one `plan` row.
- **Display:** an unaliased `named_struct('a', 1, 'b', 'x').a` is named `named_struct('a', 1, 'b', 'x').a`;
  Spark names it `named_struct(a, 1, b, x).a`.
- **Unchanged neighbours:** `struct(1 AS a, 2 AS b).a` is nullable (Spark non-null); `struct(1, 2).col1`
  and `SELECT s.* FROM (SELECT named_struct(…) AS s) t` refuse; `SELECT a exclude FROM …` refuses;
  `DROP TEMPORARY VARIABLE IF EXISTS v` refuses; `SELECT *, a + 1` and `2 * exclude` type `bigint`
  (Spark `int`).

Up: [map.md](map.md).
