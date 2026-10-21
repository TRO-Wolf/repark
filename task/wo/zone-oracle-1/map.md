# task/wo/zone-oracle-1/ — the work order for ZONE-ORACLE-1

ZONE-ORACLE-1 (adopted 2026-10-20): an expression-level Spark 4.1.2 oracle over value sources ×
leaf conversions × containers × target types × zones, recorded once and replayed on RePark as a
plain `SELECT`; the pin of ZONE-SEAM-1 and the parity-live guard against drift. No product code.

- [order.md](order.md) — the grade C order (2026-10-20): the generator, the recorder in its own
  `local[1]` interpreter, the replay test that asserts the exact set of cells DIFFERENT on main
  (a fixed cell or a regressed cell both fail), the corpus report; rules, gates, hand-back.
- [evidence/](evidence/map.md) — the corpus report on main `754fb6e5` (PR #1128): 1,640 cells, 1,150 EQUAL / 400 DIFFERENT / 20 REFUSED / 70 ERROR, the counts per axis, the ten difference shapes, the CONTAINER-CAST-ZONE-1 cells by id.

Up: [../map.md](../map.md).
