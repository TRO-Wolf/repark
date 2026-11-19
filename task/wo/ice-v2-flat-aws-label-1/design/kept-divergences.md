# ICE-V2-FLAT-AWS-LABEL-1 — kept divergences the label text names

The owner's ruling of 2026-11-18 (about 20:05 UTC, decision I) says "The label text names `sum`'s kept divergence" ([rulings](rulings-2026-11-18.md), section 8). The label text is not written yet; this list is kept here until it is.

## `sum`: the exact total past 38 digits (SUM-EXACT-TOTAL-DIVERGENCE-1)

- **Divergence:** where Spark's running total passes 38 digits and comes back, Spark raises or answers NULL and RePark answers the exact total, ANSI-on Iceberg writes Spark refuses among them.
- **Condition class:** a non-window total past 38 digits raises `ARITHMETIC_OVERFLOW` where Spark raises `NUMERIC_VALUE_OUT_OF_RANGE` after a merge (23 cells, ANSI on).
- **Card:** [SUM-EXACT-TOTAL-DIVERGENCE-1](../../../roadmap/mid-term/cards-2026-11-19/sum-exact-total-divergence-1-card-2026-11-19.md), a kept divergence, owner-accepted by decision I.
- **Registry:** the SUM-DEC-I128WRAP-1 entry of the [parity registry](../../../../docs/spark-sql-iceberg-parity.md), which carries the #1360 fix (merged as `77faf69e`).
- **Ruling:** [rulings-2026-11-18.md](rulings-2026-11-18.md), section 8 (the owner's words "Go with Fable's recommendations on all seven, re-run 1362").

## Outside the label: `PREPARE` on the Spark door (SPARK-DOOR-PREPARE-1)

The label text names `PREPARE` as outside the label until the card refuses it. Unit 1's measurement and Fable's reply of 2026-11-19 (§6.3) show that RePark accepts `PREPARE` where Spark refuses it outright (`PARSE_SYNTAX_ERROR`), and ruling 2, item 8 refuses it on the Spark door as Spark does. The card is [SPARK-DOOR-PREPARE-1](../../../roadmap/mid-term/cards-2026-11-19/spark-door-prepare-1-card-2026-11-19.md), S3, not label-blocking.

Up: [map.md](map.md).
