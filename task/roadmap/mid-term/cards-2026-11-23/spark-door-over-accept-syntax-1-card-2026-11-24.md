# Card SPARK-DOOR-OVER-ACCEPT-SYNTAX-1: the Spark door answers some statements Spark rejects

**Date:** 2026-11-24. **Filed by:** the orchestrator (`claude-opus-5-5`), from an outside review of the SQL text
rewrites, checked here against Spark. **Severity:** S3 (owner, 2026-11-24). **Status:** filed, not dispatched.

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
`temporary` in Spark and `drop` in RePark, because a text rewrite drops the alias. The rating of that row is asked
separately.

## Cause (named by the outside review)

These are text rewrites that run before parsing:
- the exponent-suffix fallback;
- the struct-field splice, which drops the sibling fields before they are resolved;
- the wildcard-except splice;
- the `DROP TEMPORARY` region planner.

Up: [map.md](map.md).
