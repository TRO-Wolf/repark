# task/wo/csv-string-field-text-1/evidence/probes/ — comment-free Python

- `common.py` — the two-engine session harness (`session`, `Rec`), unit 0's copy unchanged.
- `domain.py` — the domain table: 19 text shapes and N1's rows, written once (`gen`); the parts S (user schema, 9
  types, three doors), N (no options, four doors), F and T (`inferSchema`), M (the two modes), L (the label path,
  two Iceberg v2 writes and a read back) and E (seven `EXPLAIN`s).
- `classify.py` — joins each build's cells with Spark's: EQUAL, WRONG, RAISE, BOTHERR, by any list of builds.
- `tables.py` — the domain table's breakdowns from `classify.py`'s verdicts.
- `seat_emu.py` — the seat's scan on an existing package (the native read with `utf8_columns`, the local all-text
  pre-scan), then the facade's casts (`text_cast`) or `TRY_CAST` (`text_try`).
- `kernel.py` — 44 wider texts over 8 types: Spark against unit 1 and against the `TRY_CAST` emulation.
- `castrule.py` — `CAST` and `TRY_CAST` over 11 texts and 5 types in SQL, both engines and modes.
- `cost.py` — the cost cases (`k0_count`, `k0_hour`, `is_count`, `is_sum`, `is_none`, `is_infer`) for the base
  reader and the seat's emulation, a warm-up and five timed repetitions in one process.

Up: [../map.md](../map.md).
