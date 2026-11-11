# Card SPARK-DOOR-PARAMETERIZED-SQL-1: parameterized SQL fails on the Spark door where Spark answers

**Date:** 2026-11-10. **Filed by:** the records clerk (`claude-haiku-5-5`), from the PREPARE evaluation of 2026-11-10 (card SPARK-DOOR-PREPARE-1).

**Severity:** S3. The door fails where Spark answers, with no wrong answer.

**Clause:** `EXECUTE IMMEDIATE`, with and without `USING`, and `ReparkSession.sql(query, args=...)` with named (`:v`, dict) and positional (`?`, list) parameters. Both work on Spark 4.1.2 and fail on the Spark door.

**Measured (2026-11-10 by the frontier planning agent; Spark 4.1.2 against RePark head, and 1.5.6 equal on all nine cells):**

| cell | Spark 4.1.2 | RePark head = 1.5.6 |
|---|---|---|
| `EXECUTE IMMEDIATE 'SELECT 1 AS x'` | `[[1]]` | error: "Prepared statement 'immediate' does not exist" |
| `EXECUTE IMMEDIATE 'SELECT ? + 1 AS x' USING 5` | `[[6]]` | error: "Execute statement with USING is not supported" |
| `sql("SELECT :v + 1 AS x", args={"v": 5})` | `[[6]]` | `TypeError: sql() got an unexpected keyword argument 'args'` |
| `sql("SELECT ? + 1 AS x", args=[5])` | `[[6]]` | `TypeError: sql() got an unexpected keyword argument 'args'` |

**Why:** `EXECUTE IMMEDIATE` is misread as a prepared statement named "immediate", so the door looks up a statement that was never prepared. The `sql()` keyword is not in the Spark-door signature at all.

**Status:** filed 2026-11-10, S3, not dispatched.

**Close:** each clause answers as Spark does, or an owner ruling declines it.
