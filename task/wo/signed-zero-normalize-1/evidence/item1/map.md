# task/wo/signed-zero-normalize-1/evidence/item1/ — build item 1: the re-measured legs and the additions

Measurement only, on main `68b09d18` (the clone's release build of #1295) and the Spark 4.1.2 oracle. The findings
are in [../inventory.md](../inventory.md). Run a leg with the target tree's interpreter from a scratch working
directory; each runner takes `--warehouse` and `--out` and writes nothing else.

- [add_rows.py](add_rows.py) — the order's additions on the domain probe's seeds and layouts: `A1` (the integer-literal spelling), `A2` (`PREPARE` / `EXECUTE`; Spark gets the literal twin) and `A3` (simple `CASE`). `--engine spark|repark`.
- [ext_replay.py](ext_replay.py) — replays every committed RePark cell of extension rows E1, E3, E6, E7 and R1–R3 on the same seeds (E3's seed reconstructed) and records the old answer beside the new one.
- [labels.py](labels.py) — labels two RePark legs with the probes' own label functions and lists every cell whose label, rows, outcome or columns moved. `--kind domain|tableb`, `--spark` overrides the oracle legs.
- [type_variants.py](type_variants.py) — the type-variant inventory: pandas, polars, `float16`, dictionary and run-end inputs on three routes.
- [div_probe.py](div_probe.py) — the legacy division guard cells `X-div-*`, ANSI off and on, on either engine.
- [raw/](raw/) — `repark-68b09d18-{reads,dml,tableb,ext,p15,add,type-variants}.jsonl` (main), `spark-add.jsonl` (the oracle for `add_rows.py`), `div-spark.jsonl` and `div-repark-68b09d18.jsonl`.

Up: [../map.md](../map.md).
