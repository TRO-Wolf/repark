# task/wo/ice-held-frame-snapshot-1/ — ICE-HELD-FRAME-SNAPSHOT-1 step 0 (measured sketch, no product code)

A held Iceberg frame, or a DataFrame view over one, read after a write to its own table in the same session answers
the snapshot it was resolved at; Spark 4.1.2 answers the table as of the action. Rated S1, label-blocking for
ICE-V2-FLAT-AWS-LABEL-1 (Fable's guidance under the owner's delegation, 2026-11-20); no card yet, this step 0 is its
first record. Found by the step 0 of TEMP-VIEW-CACHE-LAZY-PARSE-1
([../temp-view-cache-lazy-parse-1/](../temp-view-cache-lazy-parse-1/map.md), its Q11 and domain table §8). Revised
the same day after the critic's round (sketch preamble). Measured on main `0436fdf9` (product tree `52f468ce`)
and on main before unit 1 of TEMP-VIEW-FRAME-READER-PARSE-1 (`8bc26cba`) against Spark 4.1.2; documents and probes
only.

- [sketch.md](sketch.md) — Spark's rule (from the jar and live), RePark's mechanism, the domain in short, two seats
  with their cells, files, caps, `EXPLAIN` and cost, the unit split, the questions and what earlier records got
  wrong.
- [domain-table.md](domain-table.md) — the cells on Spark, main and base in both ANSI modes: holds × writes, writes ×
  actions, holds × actions, the pinned frames, the action sequences, the cached cells, the Parquet inverse and the
  external writer; §8 the revision's writes of a held frame on every hold and write path, and the missed shapes; the
  silent and stored-wrong marks.
- [inventory.md](inventory.md) — the insertion inventory: every site where a RePark Iceberg frame binds its table,
  snapshot or provider, with file:line, and what each seat must touch.
- [evidence/](evidence/map.md) — probes, raw cells (gzipped where large), verdicts, cost samples, `javap` excerpts,
  provenance.
- [built/](built/map.md) — **1a, built (2026-11-20):** the re-bind and the command scope; its bar, cost (pending)
  and evidence. **Unit 2, built (2026-11-21):** the cached cells, a lazy cache that refills after a same-session
  commit.
- [built/](built/map.md) — **1a, built (2026-11-20):** the re-bind and the command scope; its bar, cost
  and evidence.

Up: [../map.md](../map.md).
