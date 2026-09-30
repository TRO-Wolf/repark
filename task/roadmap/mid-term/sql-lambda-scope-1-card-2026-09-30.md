# Card SQL-LAMBDA-SCOPE-1 — lambda parameters on the SQL door bind to same-named columns

**Date:** 2026-09-30 · **Filed by:** a Claude session (claude-fable-5-1) on the owner's ruling of the same day (ATTR-ID-1 OD-3) · **Source:** the fourth re-verify of PR #881, finding RC5-6 (S3, `new_since_base: false`), cells `p4 {F,T}_lam_sql_16`, `F_lam_sql_17`, `{F,T}_lam_sql_52`, `T_lam_sql_33`, `F_lam_sql_46`.

## What was measured

Silent wrong rows on `spark.sql(...)`, identical on main and on the #881 head; Spark 4.1.2 answers differently in every cell:

| Statement (over a table with `id`, `arr ARRAY<INT>`, `sa ARRAY<STRUCT<a:INT>>`, `T INT`, `v INT`, `x INT`) | Spark | RePark |
|---|---|---|
| `SELECT id FROM lt WHERE exists(sa, s -> s.a > 2)` (both settings) | `[1, 2]` | `[2]` |
| `… WHERE exists(arr, T -> T > 4) OR T > 5` (both settings) | `[1, 5]` | `[1]` |
| `… WHERE exists(arr, V -> v > 4)` (`caseSensitive=true`) | `[1, 2, 5]` | `[1, 5]` |
| ``… WHERE exists(arr, `X` -> X > 4)`` (`caseSensitive=false`) | `[1, 5]` | `[5]` |

The verifier's cause: the SQL router's lambda scoping. The same shapes through `DataFrame.filter(str)` were fixed by #881's third fold (`predicate_names.rs` scopes lambda parameters on the parsed tree); the SQL door has no equivalent pass.

## Why it is a card and not a fold

Out of #881's charter (the DataFrame door) and pre-existing on main. It is a silent wrong answer, so it is a v1.5.x candidate on its own.

## Decisions (proposed)

| id | decision |
|---|---|
| D-1 | Measure first: a lambda probe over `exists`, `filter`, `transform`, `aggregate`, `reduce`, `zip_with`, `map_filter`, with parameters spelled like a column (same case, other case, backticked), nested lambdas, and a body that references both the parameter and an outer column, under both settings, recorded on Spark once. |
| D-2 | One scoping pass on the SQL door, in Rust, shared with the DataFrame door's tree binder from #881 (or the successor of it under ATTR-ID-1) so the two doors cannot drift: on entering a lambda body push its parameters, on leaving pop them, and compare by the session rule. |
| D-3 | Every D-1 cell pinned through the SQL door; the existing DataFrame-door lambda pins stay green. |

## Done condition

Every D-1 cell EQUAL on both doors; `docs/spark-sql-iceberg-parity.md` names the lambda scoping rule once, for both doors.
