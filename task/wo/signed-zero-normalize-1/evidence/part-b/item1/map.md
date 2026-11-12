# task/wo/signed-zero-normalize-1/evidence/part-b/item1/ — part B part 1, build item 1

Measurement and records only, on main `276dc503`'s release build and the Spark 4.1.2 oracle. Run a leg with
`run_item1.sh` inside the build wrapper (the same environment as [../step0/run_leg.sh](../step0/run_leg.sh)); the record
is [item1.md](item1.md).

- [item1.md](item1.md) — the item 1 record: every cite re-verified (three moved), the re-measure against step 0, the added cells and three findings outside part 1.
- [item1_probe.py](item1_probe.py) — the added cells: `--part spill` (D16, `--memory`, `--partitions`, `--rows`) and `--part explain` (D14 plan shapes, `Distinct::On`, the FLOAT-to-DOUBLE join); reuses `../step0/szb_probe.py`.
- [run_item1.sh](run_item1.sh) — the leg launcher; paths from the environment, extra arguments passed through.
- [compare_legs.py](compare_legs.py) — compares two sets of RePark legs cell by cell (outcome, error, columns, rows, label) and writes the differences.
- [render_item1.py](render_item1.py) — writes `plans.md` and `spill.md` from the raw legs.
- [plans.md](plans.md) — every plan-shape `EXPLAIN`, verbatim, RePark and Spark, with Spark's `normalizenanandzero` counts; generated.
- [spill.md](spill.md) — the D16 spill cells with their labels, answers and `spill_count`; generated.
- [raw/](raw/map.md) — the item 1 legs.
- [remeasure/](remeasure/map.md) — the step-0 probes re-labelled on `276dc503` and their differences from step 0.
- [new-cells/](new-cells/map.md) — the added answer cells, labelled.

Up: [../map.md](../map.md).
