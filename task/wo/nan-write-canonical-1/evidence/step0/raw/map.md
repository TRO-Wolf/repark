# task/wo/nan-write-canonical-1/evidence/step0/raw/ — step 0's raw legs

One JSONL file per leg, written by `write_doors_probe.py`; the first line is the header. `repark-1d9d2808*` is main
`1d9d2808`'s release build, `spark-4.1.2*` Spark 4.1.2 with Iceberg 1.11.0 at its default ANSI mode. Local paths
are replaced by `<scratch>`, `<tmp>` and `<home>`. Error texts are cut at 400 characters; one cut that ended inside a word ends at `…` instead.

- `repark-1d9d2808.jsonl`, `spark-4.1.2.jsonl` — legs `doors`, `nested`, `plain` (and `native` on RePark): the source bits, each door's outcome, the engine's read-back checks, and each live data file's manifest entry with the bits of its rows.
- `repark-1d9d2808-rewrite.jsonl`, `spark-4.1.2-rewrite.jsonl` — leg `rewrite`: the files of two main-written tables before and after one engine's copy-on-write `UPDATE`.
- `formats-repark-1d9d2808.jsonl`, `formats-spark-4.1.2.jsonl` — `format_cells_probe.py`, legs `formats` then `rewrite`: each table's live files with their manifest entries and decoded data bits.
- `keys-repark-1d9d2808.jsonl`, `keys-spark-4.1.2.jsonl` — `format_cells_probe.py --tables k1,k2`: the float map-key cells.

Up: [../map.md](../map.md).
