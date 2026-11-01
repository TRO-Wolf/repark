# Card ADD-FILES-IDLESS-PROMOTION-1: `add_files` refuses an id-less file whose type is promotable, nested or top-level, with an internal statistics message (RePark card)

**Date:** 2026-11-01. **Filed by:** `claude-haiku-5-5` for the orchestrator, from the RP-64 fix round 1 records round, from the RP-64 Opus 5.5 verifier's finding F4. **Widened 2026-11-01 (RP-64 fix round 2, verifier F9):** the top-level promotable file joins the nested shape.

**Status:** filed; grade C, P3; not dispatched.

**Why:** An id-less Parquet file whose leaf has a promotable type is refused by `add_files` with `PySparkException: Unexpected => Statistics {min: 1, max: 2, ...} is not match with field type long.` Two shapes. Nested: an `int` file admitted into a table column `s<d: BIGINT>`; Spark 4.1.2 admits the same file and reads `{d: 1}` and `{d: 2}`. Top-level: an `id INT, v INT` file admitted into `(id INT, v BIGINT)`; Spark admits it and reads `[(1, 1), (2, 2)]`. The message is internal and names no column. The shape is pre-existing on all three pins (`14a93602`, `ee592dcf` and `56ff5545`).

The incompatible case (a `string` file into an `int` column, nested and top-level) also refuses at `add_files` with the same message, so no silent value is returned; Spark fails at read with a `ClassCastException`. That refusal is loud and is not this card's defect, but its message is internal too, and the card's fix covers it.

**Reachability:** RePark's own frame writer (`write.parquet`, no field ids) followed by `CALL <catalog>.system.add_files(...)`.

**Site:** the `add_files` admission of an id-less file and its statistics check against the table's field type. The exact site is not located yet in RePark or in the fork; the first unit locates it.

**Fix (proposed):** `add_files` admits a promotable id-less file, and the reads return the promoted values; an incompatible file refuses with a message that names the column and both types.

**Measured:** the RP-64 Opus 5.5 verifier, 2026-11-01: round 1 on `14a93602` and `ee592dcf`, round 2 on `56ff5545`. Shape x5 (the promotable nested file; re-measured on `56ff5545` in round 2 with the same text): RePark refuses at `add_files` with the text above, and Spark reads `{d: 1}` and `{d: 2}`. Shape x5b (the promotable top-level file, round 2): RePark refuses with the same text, and Spark reads `[(1, 1), (2, 2)]`. Shapes x6 and x6b (the incompatible nested and top-level files; re-measured on `56ff5545` in round 2 with the same text): RePark refuses at `add_files` with the same text, and Spark fails at read.

**Retires:** the card when `add_files` admits a promotable id-less file, nested or top-level, and reads the promoted values, and an incompatible one, nested or top-level, refuses naming the column and both types.

**Relation:** [F-IDLESS-STRUCT-CHILD-REMAP-1](f-idless-struct-child-remap-1-card-2026-10-31.md) (closed 2026-11-01; the admission route this shape meets); RP-64 (the records are in its unit ledger, clause C-026).

Up: [map.md](map.md).
