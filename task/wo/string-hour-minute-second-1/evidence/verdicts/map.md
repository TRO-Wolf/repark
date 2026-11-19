# task/wo/string-hour-minute-second-1/evidence/verdicts/ — the classifier's output

- [summary.md](summary.md) — `probes/cmp.py`'s counts: all cells, by configuration, family, door, input and
  function; the not-EQUAL grid by function and text; Spark's implicit-against-explicit check; seat A's forecast and
  its residue; the NTZ counts; the card's functions by text and configuration; the literal answers of both engines
  in every configuration; the multi-row doors; the doors whose answers differ for one cell.
- `hms-cells.tsv` — every cell of `hour`, `minute`, `second`, their `CAST` controls and the `HOUR` / `MINUTE` /
  `SECOND` fields of `extract` and `date_part` (15,792 rows): configuration, input, text, door, function, Spark's
  answer, main's answer (`!` before an error condition or its first 24 characters), verdict.

Up: [../map.md](../map.md).
