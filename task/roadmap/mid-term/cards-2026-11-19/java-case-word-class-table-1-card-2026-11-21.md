# Card JAVA-CASE-WORD-CLASS-TABLE-1: the word-class table behind the final sigma is generated from Character.getType, where Zulu 17's word BreakIterator disagrees with it

**Date:** 2026-11-21. **Filed by:** the records clerk (`claude-haiku-5-5`). **Source:** item 5 of the wiring unit's card lines, in [card-lines.md](../../../wo/csv-string-field-text-1/built/card-lines.md), and the #1388 exception group named in the owner's ruling of 2026-11-21 (decision 4, F7's 4 cells), recorded in [rulings-2026-11-21.md](../../../wo/ice-v2-flat-aws-label-1/design/rulings-2026-11-21.md).

**Rating: S4 (the owner, 2026-11-21).**

**Status:** filed 2026-11-21; not dispatched.

- `java_case.rs`'s `WORD_CLASS_RUNS` is generated from `Character.getType`, where Zulu 17's word `BreakIterator` uses its own compiled break data. They disagree on U+E0001, U+E007F, U+110BD, U+110CD, U+13438, U+1BCA3, U+1D17A and the unassigned ranges U+18CD6–18CFF, U+2A6DE–2A6FF, U+2B735–2B73F, U+2B81E–2B81F, U+2CEA2–2CEAF and U+2FA1E–2FFFF.
- With a cased mark (U+0345) between such a code point and Σ, the final sigma differs from Java's, and CSV header dedup under `caseSensitive=false` differs from Spark's (4 cells lost against main, such as `ΑE0001ͅΣ,αE0001ͅς,k`).
- The fix: derive the table from `BreakIterator`'s behaviour.

Up: [map.md](map.md).
