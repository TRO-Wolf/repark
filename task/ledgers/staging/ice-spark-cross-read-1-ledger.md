# Unit ledger — ICE-SPARK-CROSS-READ-1 (U-10) · Spark reads what RePark writes

**Date:** 2026-10-14 · **Branch:** `test/ice-spark-cross-read-1` · **Base:** `018d0efd`
(`origin/main`) · **Model:** Muse Spark (`muse-spark-1.3-contributor`) · **Policy:** [../../../AGENTS.md](../../../AGENTS.md).
**Path:** STANDARD. **risk_tier: standard.**

**Retires:** this ledger moves to `../completed/` when the unit's last commit lands.

**Why now.** No acceptance leg has a second engine read the tables RePark
writes. This unit builds that leg: Spark 4.1.2 with Iceberg 1.11 reads every
table the acceptance legs write and answers row-equal — locally tonight
against a local warehouse, and on AWS when the owner wires the workflow step.

## Clauses

| Clause | Statement | Proof obligation | Verdict | Evidence |
|---|---|---|---|---|
| C-001 | The shared helper adopts each RePark table into Spark and compares schema and rows, returning one EQUAL, DIFFERENT or SPARK-CANNOT-READ outcome per table, with nanosecond-column tables reported and never skipped. | The helper module plus its map row; pure-function smoke green. | PROVEN | `python/repark/tests/parity/_acceptance_cross_read.py` (ruff check and format clean, comment gate 0); smoke of columns, rows, schema and row compare plus the outcome table render, all as designed. |
| C-002 | The local leg runs the MOR, create-or-replace-twice and v3 offline scenarios unchanged and cross-reads every table each wrote. | Three live cells, each reusing its scenario runner by import and asserting its own outcome first, plus the nanosecond R-002 cell. | PROVEN | `python/repark/tests/parity/test_acceptance_cross_read_local.py`: 4 passed in 9.18s on live Spark (see the outcome table below); 4 skipped without the live flag and 4 skipped with the flag but without PySpark. |
| C-003 | The local per-table outcome table is recorded. | The table below, pasted from the run. | PROVEN | Five tables: four EQUAL, zero DIFFERENT, one expected SPARK-CANNOT-READ on nanosecond columns (Spark `UnsupportedOperationException`: `Cannot convert unsupported type to Spark: timestamp_ns`); the ns row below is the R-002 evidence. |
| C-004 | The AWS leg discovers the scratch tables on Glue and on S3 Tables, gates like the AWS legs plus a PySpark skip, collects cleanly unset, and is not run. | Two discovery cells with the module-level skip; collect-only proof. | PROVEN | `python/repark/tests/parity/test_aws_acceptance_cross_read.py`: 2 tests collected and 2 skipped with the variable unset, in an interpreter without PySpark. No AWS cell was attempted. |
| C-005 | The AWS leg runs green on wired AWS. | The owner wires the workflow step from the unit order; the nightly then runs it. | OPEN | Pending owner wire-up; the step is proposed as a fenced diff in `task/wo/ice-spark-cross-read-1/order.md`. |

| C-006 | Dated 2026-10-29: the gate now accepts `SPARK-CANNOT-READ` only for the recorded unsupported-type condition (first line the recorded prefix plus `timestamp_ns` or `timestamptz_ns`) on a nanosecond table; every other Spark failure is rejected at any precision (CROSS-READ-GATE-CLASS-1, AUD-005). C-003 keeps its text. | The cross-read-gate-class-1 ledger, clauses C-001 to C-003, and their pins. | OPEN | Built on branch `fix/cross-read-gate-class-1`; closes on its merge. |

## Local outcome table (2026-10-14)

```
table | status | repark_rows | spark_rows | detail
mem.ns.mw4mor | EQUAL | 20 | 20 | row-equal
mem.ns.replace2mem | EQUAL | 5 | 5 | row-equal
ice.sales.v3acc | EQUAL | 10 | 10 | row-equal
ice.sales.v3acc_adopted | EQUAL | 10 | 10 | row-equal
ice.ns.nsts | SPARK-CANNOT-READ | 2 | - | ns-columns; UnsupportedOperationException: Cannot convert unsupported type to Spark: timestamp_ns
```

RePark's two ns rows, for the record (both read back with all nine digits):
`[{'id': 1, 'ts': '2024-01-15 10:20:30.123456789', 'tz': '2024-01-15 10:20:30.123456789'}, {'id': 2, 'ts': '2023-06-30 12:34:56.987654321', 'tz': '2023-06-30 12:34:56.987654321'}]`.

## Measurement provenance

Spark banner for the run: version 4.1.2 (`f0bb2e6a47d0ebda424ffd633fcea8644a597954`),
session zone UTC, ANSI on, `spark.jars.packages`
`org.apache.iceberg:iceberg-spark-runtime-4.1_2.13:1.11.0`. RePark came from
the release module built off `293896ce` (main has moved by docs only since).
The leg needs one interpreter with both engines: the sparkenv interpreter
with the release module on `PYTHONPATH`, from the repo root (fill the
three bracketed paths for this box):

```bash
JAVA_HOME=/usr/lib/jvm/zulu-17-amd64 SPARK_LOCAL_IP=127.0.0.1 REPARK_PARITY_LIVE=1 PYTHONPATH=<repark release module's src>:<its site-packages> <the PySpark interpreter> -m pytest python/repark/tests/parity/test_acceptance_cross_read_local.py -q -rA -s
```

The `-s` only shows the printed outcome tables; without it the run passes
identically.

## Notes

- A Spark Hadoop catalog cannot see RePark memory-catalog tables directly
  (no `version-hint.text` is written, so a direct select misses the table):
  the helper adopts each table's newest metadata file via `register_table`,
  the same mechanism the existing cross-read cells use.
- Memory-catalog state is per session: the v3 scenario registers its
  adopted twin on a second session, so the leg re-registers the same
  metadata file on the first session before cross-reading it. Both
  registrations point at one immutable file, so the read is faithful.
- The SQL-harden S1–S9 shapes are not repeated in the local leg: their own
  suite already runs every program on RePark and on live Spark and compares
  verdicts. The AWS leg still discovers their tables.
- The bronze publish path has no offline runner (it reads a real `s3a`
  bronze entity) and the U12 loose-object writes are not Iceberg tables, so
  neither is in the local leg. The AWS leg discovers the publish tables.
- The workflow proposal was verified by exact-position application of the
  fenced diff: every context line byte-matches the workflow file and the
  result carries all three additions once. (`git apply` is unreliable on
  this box — it rejects its own generated output the same way — so byte
  comparison is the verification, not the tool.)
