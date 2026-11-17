# Card NOT-DISTINCT-FROM-NULL-1: on the Spark door, `CAST(c AS INT) IS NOT DISTINCT FROM NULL` refuses with UNRESOLVED_COLUMN, where Spark answers the NULL row

**Date:** 2026-11-16. **Filed by:** the records clerk (`claude-haiku-5-5`), from the CAST-LITERAL-UNWRAP-1 verifier's verdict of 2026-11-16 (Findings), an orchestrator item. Measured on main and on the CAST-LITERAL-UNWRAP-1 head, four times in both ANSI modes; the verdict does not record which commit each ran on, so this card makes no claim about #1341 or #1345.

**Status:** ruled 2026-11-16, not dispatched; not label-blocking.

**Severity:** S2, ruled 2026-11-16, not label-blocking. A refusal where Spark answers, on a plain null-safe comparison.

Ruled 2026-11-16 ("Merge those and defaults."): S2, not label-blocking; the label text lists this loud card as an exclusion.

## Finding

On the Spark door, `CAST(c AS INT) IS NOT DISTINCT FROM NULL` raises `UNRESOLVED_COLUMN` on main and on the head. The literal `NULL` on the right is read as a column name. Spark answers, including the NULL row. The verifier calls it a pre-existing parse defect outside the shielded shapes.

## Measured

| cell | ANSI | Spark 4.1.2 | main and head |
|---|---|---|---|
| `CAST(c AS INT) IS NOT DISTINCT FROM NULL` | both | answers, with the NULL row | raises `UNRESOLVED_COLUMN` (4 reproductions) |

Reproduction: a temp view `t(c)` over a numeric column, then `SELECT CAST(c AS INT) IS NOT DISTINCT FROM NULL FROM t` on the Spark door, in both ANSI modes. The verifier's own probe is in its scratch folder, not committed.

## Related, and not the same shape

DISTINCT-FROM-LITERAL-LEFT-1 covers a literal on the left of `IS DISTINCT FROM`. Its card records that the `<column> IS DISTINCT FROM <literal>` forms answer correctly, and this card's shape has the literal on the right. The two may share a seat in the resolver, which step 0 checks.

## Not yet known

The seat. Step 0 finds which resolver path reads the `NULL` literal as a column name.

Up: [map.md](map.md).
