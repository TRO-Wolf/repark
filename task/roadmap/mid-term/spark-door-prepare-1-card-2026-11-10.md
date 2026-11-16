# Card SPARK-DOOR-PREPARE-1: the Spark door accepts PREPARE and EXECUTE, and cell 04376 names the wrong refusal class

**Date:** 2026-11-10. **Source:** the frontier review memo of 2026-11-10, and the Opus 5.5 and Muse verifier verdicts on PR #1292. **Filed by:** the records clerk (`claude-haiku-5-5`).

**Owner ruling (2026-11-10, verbatim):** "merge #1293. PREPARE: A."

**Severity:** S3. **Pre-existing:** pre-existing (base and head agree).

**Clause (widened under ruling A):** the Spark door refuses `PREPARE`, `EXECUTE <name>` and `DEALLOCATE` with Spark's `PARSE_SYNTAX_ERROR`. Spark 4.1.2 refuses all three; RePark's head and 1.5.6 accept them.

**Measured:**

- Part 1: cell 04376's refusal class differs from Spark's (class only).
- Part 2: `PREPARE p1 AS SELECT 1` answers on the Spark door; Spark gives `PARSE_SYNTAX_ERROR` (SQLSTATE 42601). EXECUTE pairs struct fields by position on the head build, so no wrong answer comes from it.
- Evidence (measured 2026-11-10 by the frontier planning agent; Spark 4.1.2 against RePark head, and 1.5.6 equal on all nine cells):

| cell | Spark 4.1.2 | RePark head = 1.5.6 |
|---|---|---|
| `PREPARE p1 AS SELECT 1 AS x` | `PARSE_SYNTAX_ERROR` near 'PREPARE' | accepted |
| `EXECUTE p1` | `PARSE_SYNTAX_ERROR`, missing IMMEDIATE | `[[1]]` |
| `PREPARE p2 AS SELECT $1 + 1 AS x` | `PARSE_SYNTAX_ERROR` near 'PREPARE' | accepted |
| `EXECUTE p2(5)` | `PARSE_SYNTAX_ERROR`, missing IMMEDIATE | error: "spark integer UDF expected Int64Array, got Int32" |
| `DEALLOCATE p1` | `PARSE_SYNTAX_ERROR` near 'DEALLOCATE' | accepted |

- `EXECUTE p2(5)` with a `$1` parameter already fails on head and 1.5.6 with internal text: "spark integer UDF expected Int64Array, got Int32". The ruling does not change that cell's text; under ruling A the statement is refused before it runs.

**Change needs:** a release note for the refusal, and Spark-door tests that go through `PREPARE`, `EXECUTE <name>` or `DEALLOCATE` become refusal tests.

**Status:** ruled A on 2026-11-10 (refuse, per the clause above). Not dispatched.

**Close:** the refusal lands with its release note and refusal tests, or a later owner ruling declines it.

## Append (2026-11-16): re-measured on main `a06c1088`, both doors

Measured by the records clerk on main's post-#1339 package against Spark 4.1.2, both ANSI modes, the Spark door
(`ReparkSession.sql`) and the native door (`repark.sql`). The shape is unchanged: everything below is identical with
ANSI on and off.

- **Spark 4.1.2:** `PREPARE p1 AS SELECT 1 AS x`, `EXECUTE p1` (after the prepare) and `DEALLOCATE p1` each refuse
  with `[PARSE_SYNTAX_ERROR] Syntax error at or near 'PREPARE'. SQLSTATE: 42601 (line 1, pos 0)` (`ParseException`,
  error class `PARSE_SYNTAX_ERROR`). The `EXECUTE` refusal names `PREPARE`: the statement never parses, so the
  setup prepare fails first.
- **RePark Spark door:** `PREPARE p1 AS SELECT 1 AS x` is accepted and returns an empty schema with no rows, and it
  registers: `EXECUTE p1` then answers `[[1]]` (`int`); `DEALLOCATE p1` is accepted (empty schema, no rows) and it
  unregisters: `EXECUTE p1` afterwards refuses with `Prepared statement 'p1' does not exist`, as does
  `EXECUTE q9` with no prepare at all. `PREPARE p2 AS SELECT $1 + 1 AS x` is accepted too. A second `PREPARE` of a
  live name refuses with `Prepared statement 'p1' already exists`.
- **RePark native door:** the same three statements are accepted with the same shapes, except `EXECUTE p1` after
  the prepare answers `[[1]]` as `bigint` where the Spark door answers `int`.

So the card covers this finding in full: the refusal clause already names all three statements, and the 2026-11-10
`EXECUTE p1` → `[[1]]` row is confirmed on current main. New detail for the fix: `DEALLOCATE` genuinely unregisters,
and the native door shares the acceptance (with its own `bigint` answer type on `EXECUTE`). Severity stays S3.
