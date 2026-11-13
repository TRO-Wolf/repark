# task/wo/signed-zero-normalize-1/evidence/part-b/step0/raw/ — step 0's raw legs

One JSONL file per leg; the first line is the leg header. `spark-*` is the Spark 4.1.2 oracle, `repark-ae247767-*`
is main `ae247767`'s release build. Hashes and the leg list are in [../provenance.md](../provenance.md).

- `spark-reads.jsonl`, `repark-ae247767-reads.jsonl` — the read sites on every route.
- `spark-dml.jsonl`, `repark-ae247767-dml.jsonl` — the write and partition sites.
- `spark-floatkeys.jsonl`, `repark-ae247767-floatkeys.jsonl` — the flat matrix's floatkeys cells; variant `matrix` here is the pre-#1307 bare seed.
- `spark-naaj-check.jsonl` — `naaj_check.py`'s output.

Up: [../map.md](../map.md).
