# map — task/wo/ice-spark-cross-read-1/

ICE-SPARK-CROSS-READ-1 (U-10, 2026-10-14): Spark reads what RePark writes.
The unit is built: the shared cross-read helper, the local leg (green
against live Spark 4.1.2 with Iceberg 1.11), the AWS leg (written, not run),
and this order with the workflow step the owner wires. This directory closes
when the AWS leg runs green on the nightly.

## Contents

- [order.md](order.md) — what was built, the local run evidence, and — as a
  fenced diff in markdown only — the workflow step the owner adds to
  `aws-acceptance.yml` (Temurin 17, the `record` extra for PySpark 4.1.2, the
  cross-read module step). The workflow itself is untouched.

Up: [../map.md](../map.md).
