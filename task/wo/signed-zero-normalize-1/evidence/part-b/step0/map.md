# task/wo/signed-zero-normalize-1/evidence/part-b/step0/ — part B step 0: the measured clause table (flat keys)

Measurement only, on main `ae247767`'s release build and the Spark 4.1.2 oracle. Run a leg with `run_leg.sh` inside
the build wrapper; how is in [provenance.md](provenance.md).

- [szb_cells.py](szb_cells.py) — the cell catalogue: read, NaN-payload, write and partition sites, and the flat matrix's floatkeys shapes in three seed variants (`matrix`, `bare`, `signed`).
- [szb_probe.py](szb_probe.py) — runs one leg: `--engine spark|repark --part reads|dml|floatkeys`; imports part A's `evidence/probe_signed_zero.py` for the classifier.
- [run_leg.sh](run_leg.sh) — the leg launcher; every path comes from the environment (`SZB_CLONE`, `SZB_WORK`, `SZB_RAW`, and for Spark `SZB_SPARK_PYTHON`, `JAVA_HOME`, `SZB_ICEBERG_JAR`).
- [label_render.py](label_render.py) — labels RePark cells with part A's `label_of` and writes `labels.jsonl` and `table.md`; `--raw-file`, `--out` and `--base` label other legs.
- [idf_check.py](idf_check.py) — side check: two-column `IS DISTINCT FROM` on the Spark door.
- [partition_check.py](partition_check.py) — side check: an identity partition on a float column and its metadata tables.
- [naaj_check.py](naaj_check.py) — side check: Spark's `NOT IN` subquery under each anti-join plan.
- [raw/](raw/map.md) — the raw legs.
- [labels.jsonl](labels.jsonl) — one labelled RePark cell per line, with its truth and decimal twin; generated.
- [table.md](table.md) — the clause table: counts, per-site labels, every non-EQUAL cell, the oracle self-check, stored seeds, and the completeness check; generated.
- [named-seats.md](named-seats.md) — the hand-written §7–§8 that `label_render.py` appends to `table.md`.
- [provenance.md](provenance.md) — versions, hashes, the legs, truth rules, side checks, deviations, and the filing changes.

Up: [../map.md](../map.md).
