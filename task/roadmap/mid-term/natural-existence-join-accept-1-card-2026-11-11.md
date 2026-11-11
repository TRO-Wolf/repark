# Card NATURAL-EXISTENCE-JOIN-ACCEPT-1: NATURAL semi and anti joins are accepted where Spark refuses

**Date:** 2026-11-11. **Filed by:** the records clerk (`claude-haiku-5-5`), from the Opus 5.5 round 1 and re-check, and the Muse MAX round 1, on PR #1295.

**Status:** filed 2026-11-11, not dispatched. Severity **S3**. Spark rejects the join type itself; the answer is accepted.

**Measured:**

- The `NATURAL` semi and anti joins are accepted for any key type: X0001 to X0060.
- Moved into acceptance by STRUCT-COMPARE-SEAT-FAMILY-1 (introduced): X0025 to X0028 and X0049 to X0052, which were refused before.
- Still accepted before and after: X0001, X0009, X0033, X0041, X0140 and M06.
- The pin labels X0030 and X0032 as FACADE-DUP-FIELD-ROW-1. Their true card is this one: on the Rust door `NATURAL LEFT SEMI` answers where Spark refuses.

**Close:** the `NATURAL` semi and anti joins refuse with Spark's class, on every key type, with the pins updated.
