# Card STRUCT-TYPED-FORM-SCHEMA-1: typed-door struct forms refuse with a schema error that carries no class

**Date:** 2026-11-11. **Filed by:** the records clerk (`claude-haiku-5-5`), from the builder's notes of STRUCT-COMPARE-SEAT-FAMILY-1 (`handback-notes.md`, the "Review delta" section, which names this card as carrying class gaps in the pins) and the pin file's `CLASS_CARDS` table.

**Status:** filed 2026-11-11, not dispatched. Severity **S3**, class only. Pre-existing per the builder's notes; the clerk did not re-measure the base leg.

**Measured:**

- The head leg refuses with "No field named", classless, where Spark refuses with its class.
- The builder's notes count 15 pinned cells with this gap. The cell file names this card on 70 lines of `python/repark/tests/sql/data/struct_compare_seat_family_cells.jsonl`; the two counts are not reconciled here.

**Close:** each cell refuses with Spark's class, with the pins updated.
