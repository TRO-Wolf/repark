# task/wo/struct-compare-by-position-1/evidence/after/

The three measured tables re-run on the release build of the item-6 head (r6; r5 on `adaa27494` gave the same labels), beside the before-tables in `../`.

- [moves.md](moves.md) — the moves by label per table (before = 1.5.6, after = head), the residual cells with their cards, the halt checks, and the re-run of unit C's and #1290's pins and probes.
- [provenance.md](provenance.md) — the build hash, the commands, the r4/r5 reproducibility check, the render steps and the scrub.
- `step0/` — `table.md` (the step-0 after-table, `../step0/render2.py` over the head leg), `groups.txt` and `raw/head.jsonl` (4393 records).
- `variant-seat-c/` — `table.md` (the seat C after-table, P10 and P10dml) and `raw/head.jsonl` (218 records, written in the probe's `base` slot).
- `variant-ext/` — `table.md` (the extension after-table) and `raw/head.jsonl` (246 records, written in the probe's `base` slot).
- `unit-c/` — unit C's domain and control probes re-run on this head (`*-head.jsonl`) and on main `45018a2c` (`*-main.jsonl`), the probes' own path scrub applied.
- `map-seats/` — the out-of-clause map-only comparison probe (`probe_map_seats.py`, 18 seats), its Spark 4.1.2, main and head legs in `raw/`, and `table.md`: head moves five seats to Spark's refusal and changes no other.

Up: [../map.md](../map.md).
