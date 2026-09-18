# V3-MULTIARG-1 — read, then write, multi-argument partition transforms

**Filed:** 2026-09-18 by round 1 under owner ruling **2026-09-18** ("multi-argument
transforms are NOT built in 1.x; declare them, with a card for later").
**Source:** rating row V3-05 (the v3 `source-ids` partition field, measured MISSING loud).
**Target:** post-1.x. **No code in 1.x.**

## The gap it closes

Registry row **V3-MULTIARG-1** (`docs/spark-sql-iceberg-parity.md`): the Iceberg v3
spec allows a partition/sort field with several source columns (`source-ids`, e.g. a
multi-argument bucket). RePark's SQL door refuses `bucket(4, id, name)` at analysis,
the DataFrame door has no multi-argument spelling, a foreign table carrying
`"source-ids": [1, 2]` refuses loud at `register_table`, and Spark 4.1.2 DDL cannot
create one either. All four answers are measured and pinned in
`python/repark/tests/test_v3_multiarg_1.py`; the declaration keeps the gate honest.

## Why it is not a 1.x fix

The owned fork models only the singular `source-id`: a document carrying
`source-ids` fails the whole metadata parse (`TableMetadataEnum`), so there is no
read surface to build on. Past the parse, the engine would still need the transform
evaluation over several source columns and the partition pruning that goes with it.
Spark's own DDL refuses the shape, so no live engine can produce the fixture RePark's
read needs — the first step must come from the Java API, not from Spark SQL.

## FIRST STEP — a Java-API fixture

A small Java/`spark-shell`-free program or a PySpark `_jvm` call using Iceberg
1.11.0's Java API (`UpdatePartitionSpec` / `PartitionSpec.builderFor` with a
multi-argument transform, **if the Java API supports it — measure first**) that writes
a real v3 table with `source-ids`, so RePark's read can be measured against Java's.
If the Java API also refuses, record the refusal verbatim and the card waits on the
Iceberg-Java surface instead of the fork.

## Then the fork asks it would need

1. Spec parsing in `iceberg-rust`: model `source-ids` (plural) on the partition field
   beside the singular `source-id`, keeping today's documents parsing exactly as now.
2. The transform evaluation: bucket (and later truncate) over several source columns
   per the v3 spec, on the read path first.
3. Pruning: partition-value predicates against multi-source fields, measured against
   the Java-API fixture from the first step.

## Clauses (draft)

- **C-001** The Java-API fixture exists and RePark's read of it is measured (values or
  the exact loud refusal).
- **C-002** The fork parses `source-ids` documents; the declaration pin flips to a
  read pin.
- **C-003** Multi-argument bucket evaluates and prunes per the spec; V3-MULTIARG-1 →
  FIXED.
- **C-004** The write path (`PARTITIONED BY (bucket(4, id, name))` on both doors)
  answers the measured Java behaviour.

## Pointers

- Up: [map.md](map.md) · Registry: [../../../docs/spark-sql-iceberg-parity.md](../../../docs/spark-sql-iceberg-parity.md)
- Pins: [../../../python/repark/tests/test_v3_multiarg_1.py](../../../python/repark/tests/test_v3_multiarg_1.py) ·
  Ledger: [../../ledgers/staging/v3-multiarg-1-ledger.md](../../ledgers/staging/v3-multiarg-1-ledger.md)
