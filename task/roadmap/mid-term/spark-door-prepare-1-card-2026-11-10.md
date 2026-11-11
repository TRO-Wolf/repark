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
