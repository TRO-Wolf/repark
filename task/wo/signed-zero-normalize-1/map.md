# task/wo/signed-zero-normalize-1/ — SIGNED-ZERO-NORMALIZE-1: the design sketch and the domain table

SIGNED-ZERO-NORMALIZE-1 (owner ruling G, 2026-11-08: "G yes and defaults"). A v2 label blocker: `-0.0` and `0.0`
compare unequal in Iceberg pruning and in RePark's literal paths. The directory holds the design sketch (section 12
carries the defaults the owner accepted) and the domain table the build measures against. Records and measurement
only; no product code.

- [order.md](order.md) — the build order for part A (SIGNED-ZERO-LITERALS): the rulings, the rebase onto today's main, the clause and its completeness check, the design bindings, the inventories, the pins and the halts.
- [order-part-b.md](order-part-b.md) — build order for part B part 1 (SIGNED-ZERO-KEYS, flat float keys): the rulings, item 1's premises, the clause from step 0, the design bindings, the pins, the halts and the cost gates; every question RULED 2026-11-12, not built.
- [sketch.md](sketch.md) — the design sketch of 2026-11-08: the seats, the UDF, the doors, the domain table in section 9, the pins, the split and the owner questions with defaults.
- [evidence/](evidence/map.md) — the domain table measurement (D1 to D17 as written in sketch section 9) on the Spark 4.1.2 oracle and the release wheel built from `fdc61f26`: the probe, the raw legs, the generated table and summary, and the provenance; build item 1's inventories and re-measured legs on main `68b09d18`; part B's step 0 and order evidence in `evidence/part-b/`.

Up: [../map.md](../map.md).
