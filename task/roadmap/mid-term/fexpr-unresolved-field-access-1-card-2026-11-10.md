# Card FEXPR-UNRESOLVED-FIELD-ACCESS-1: an F.expr struct comparison that names a field of the same struct fails to resolve

**Date:** 2026-11-10. **Source:** the frontier review memo of 2026-11-10, and the Opus 5.5 and Muse verifier verdicts on PR #1292. **Filed by:** the records clerk (`claude-haiku-5-5`).

**Severity:** S3. **Pre-existing:** pre-existing.

**Measured:**

- `F.expr("named_struct('a', p.a, 'a', p.b) = p")` fails with `No field named p.a`. Spark's answer is not measured; a Spark cell is needed first.

**Status:** filed 2026-11-10, not dispatched.

**Close:** fixed, or declined by a recorded ruling.
