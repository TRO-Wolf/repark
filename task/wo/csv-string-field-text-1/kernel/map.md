# task/wo/csv-string-field-text-1/kernel/ — CSV-FIELD-KERNEL-1, the kernel unit

**The unit (2026-11-20, Claude Opus 5.5 xhigh).** The first of the card's two units: Spark 4.1.2's CSV field
converters as one Rust kernel, its registration and a Python builder. Nothing reads through it yet, so no answer
moves; the second unit wires it into the reader.

- **The kernel** is `crates/repark-functions/src/csv/field.rs`, a module inside `csv/` beside `from_csv.rs`, so
  `from_csv`'s `append_token` can call its per-token converters later (one converter home, Fable §10 item 1;
  re-seating `from_csv` is an S2 card) and it registers through `csv::functions()` with no `lib.rs` edit. Its tests
  are `csv/field/tests.rs`.
- **The builder** is `PyColumn.csv_field(type_spec)`: the wrapper in `column/mod.rs` and the body in
  `column/expr_build.rs` beside `parse_data_type`. A new sibling file would need a row in `column/map.md`, whose
  size baseline (372 lines) the map-size gate holds, so the body joined an existing file instead.
- **Product files: 4** (`csv/field.rs` new, `csv.rs`, `column/expr_build.rs`, `column/mod.rs` at 990 of 1,000).

Records:
- [bar.md](bar.md) — the converter table: each type's cells passed, the declared differences, the refusals, the
  facade pin and the no-move proof.
- [cost.md](cost.md) — the kernel against arrow's `CAST` (instruction counts and wall time) and C-012.
- [probes/](probes/map.md) — the added-text probe, the table generator and the cost probe.
- [raw/](raw/map.md) — Spark's added cells, the cost samples and the no-move summary.

Up: [../map.md](../map.md).
