# Card UPDATE-REQUIRED-NULL-TEXT-1: an `UPDATE` that puts NULL in a required column refuses with two RePark texts, neither Spark's

**Date:** 2026-11-13. **Filed by:** the records clerk (`claude-opus-5-5`). **Source:** the UPDATE-SCHEMA-BATCH-MISMATCH-1
inventory critic, 2026-11-13 (step 0 at `868f7d6f`, main `dd77603a`; points 3 and 4, and its live-oracle table), the
unit that is #1327.

**Status:** filed 2026-11-13, not dispatched. Severity **S4**: RePark refuses exactly where Spark refuses and answers
where Spark answers; only the text differs.

## Measured (main, Spark door, v2 copy-on-write, v2 merge-on-read and v3; Spark 4.1.2 with Iceberg 1.11.0)

Table `id BIGINT, i INT, r INT NOT NULL, flag STRING`, rows (4, NULL, 1) and (6, -1, 2).

| Statement | Spark | RePark |
|---|---|---|
| `SET r = NULL WHERE i IS NULL OR i <= 7` (identity route) | `[NOT_NULL_ASSERT_VIOLATION] NULL value appeared in non-nullable field` | the writer's text, `… Column 'r' is declared as non-nullable but contains null values` (or, before #1327, the batch-mismatch text) |
| `SET r = i WHERE id = 4` (a NULL arrives) | `NOT_NULL_ASSERT_VIOLATION` | the writer's text, 9 of 9 |
| `SET r = NULL WHERE id IN (4, 6)` (fork route) | `NOT_NULL_ASSERT_VIOLATION` | `Error during planning: UPDATE cannot assign NULL to required column 'r'`, 9 of 9 |

The native door gives the fork text on every form. After #1327 the identity route has one text, so the door as a
whole still has two, and the "one stable text" claim holds only for identity-route predicates (a binary operator or
an `IN` subquery).

## Close

Both routes refuse with Spark's `NOT_NULL_ASSERT_VIOLATION` text (or one RePark text the owner rules), pinned on the
identity and fork routes of the Spark door.
