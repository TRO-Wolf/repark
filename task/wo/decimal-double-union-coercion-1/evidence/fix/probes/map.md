# task/wo/decimal-double-union-coercion-1/evidence/fix/probes/ — the build's probes

Comment-free Python. The commands are in [../map.md](../map.md).

- [named.py](named.py) runs the critic's item-6 cells by name on Spark or RePark:
  - `saveAsTable`, `writeTo.append`, `createOrReplace` and `replaceWhere` of a DECIMAL and DOUBLE union;
  - a v3 DELETE and a non-ASCII union;
  - the four literal shapes `widen_decimal_against_float` covers;
  - the struct-comparison refusal;
  - the written K1 and K2 casts.
- [cards.py](cards.py) measures the new cards:
  - SIGNED-ZERO-UPDATE-IN-LIST-1: UPDATE and SELECT over DOUBLE columns with `-0.0`, copy-on-write and
    merge-on-read;
  - STRING-DECIMAL-EXPONENT-1: E-notation STRING to DECIMAL by `CAST`, `TRY_CAST` and store.
- [cost_data.py](cost_data.py) writes the cost tables as Parquet with main's package: 100M rows and 20M rows of
  `id`, `d10 DECIMAL(10,2)`, `d38 DECIMAL(38,18)` and `v DOUBLE`.
- [cost_probe.py](cost_probe.py) runs one timed cost case for one build (the rows in [../cost.md](../cost.md)).

Up: [../map.md](../map.md).
