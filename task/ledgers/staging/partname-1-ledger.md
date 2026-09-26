# Unit ledger — WO PARTNAME-1 · partition-field names per door pinned against Spark

**Date:** 2026-09-26 · **Branch:** `feat/partname-1` · **Base:** `5a1c8ebd`
(`origin/main`) **Model:** Muse Spark (`muse-spark-1.3-contributor`) · **Policy:** [../../../AGENTS.md](../../../AGENTS.md).
**Path:** STANDARD. **risk_tier: standard.**

**Retires:** this ledger moves to `../completed/` when the unit's last commit lands.

**Why now.** U9-TYPES-1 residue R-26 says the `bucket(4, u)` field is `u_bucket_4`
on Spark and `u_bucket` on RePark — but the two engines took different doors in that
step: Spark the UPDATE door (the Java `updateSpec().addField(bucket)` API), RePark
the CREATE door (`PARTITIONED BY (bucket(4, u))`). Measured 2026-09-26, each door
names the same way on both engines, so the residue compared doors, not engines.
This unit pins the per-door names, moves the U9 step onto the same door, and records
the one real divergence (redundant-partition rules) for the fork. No name changes
in code.

**What Spark does (measured 2026-09-26; Spark probes
`partname-spark-2026-09-26.json` and `partred-spark-2026-09-26.json`, RePark main
`partname-repark-main-2026-09-26.json`).** The CREATE door omits the width:
`bucket(16, id)` → `id_bucket`, `truncate(10, s)` → `s_trunc`, `hours(ts)` →
`ts_hour`, `(bucket(4, id), id)` → `id_bucket`, `id`. The ALTER/UPDATE door keeps
it: `ADD PARTITION FIELD bucket(8, id)` → `id_bucket_8`, `truncate(2, s)` →
`s_trunc_2`, `days(ts)` → `ts_day`, `years(ts)` → `ts_year`, `months(ts)` →
`ts_month`, `hours(ts)` → `ts_hour`, `bucket(16, id)` beside `bucket(8, id)` →
`id_bucket_16`, `truncate(4, s)` beside `truncate(2, s)` → `s_trunc_4`,
`months(ts) AS my_month` → `my_month`; `DROP PARTITION FIELD bucket(8, id)`
removes `id_bucket_8`; `REPLACE PARTITION FIELD truncate(2, s) WITH truncate(5,
s)` lands `s_trunc_5` on a fresh field id. RePark main answers every one of these
names identically on the same door.

## Clauses

| Clause | Statement | Proof obligation | Verdict | Evidence |
|---|---|---|---|---|
| C-001 | The CREATE door generates field names without the width — `bucket(16, id)` → `id_bucket`, `truncate(10, s)` → `s_trunc`, `hours(ts)` → `ts_hour`, `(bucket(4, id), id)` → `id_bucket`, `id` — each equal to Spark's measured answer on the same door. | Facade pins read the default spec from the table's latest metadata JSON. | PROVEN | `test_partname_1.py` create-door tests, 3 passed. |
| C-002 | The UPDATE door generates field names with the width — `bucket(8, id)` → `id_bucket_8`, `truncate(2, s)` → `s_trunc_2`, `days(ts)` → `ts_day`, `years(ts)` → `ts_year`, `months(ts)` → `ts_month`, `hours(ts)` → `ts_hour`, `bucket(16, id)` beside `bucket(8, id)` → `id_bucket_16`, `truncate(4, s)` beside `truncate(2, s)` → `s_trunc_4`, `months(ts) AS my_month` → `my_month`; DROP removes `id_bucket_8`; REPLACE lands `s_trunc_5` on a fresh field id — each equal to Spark's measured answer on the same door (the three shapes the pins isolate from the probe sequences — `hours(ts)` alone at field id 1000, `months(ts) AS my_month` alone at 1000, and the drop/replace sequence without `my_month` landing `s_trunc_5` at 1003 — were re-measured on Spark 4.1.2 on 2026-09-26, `partname2-spark-2026-09-26.json`, and answer exactly those ids). | Facade pins read the default spec from the table's latest metadata JSON. | PROVEN | `test_partname_1.py` update-door tests, 6 passed. |
| C-003 | The U9 `bucket_uuid` step runs both engines through the UPDATE door, so `uuid/part/md` replays EQUAL at `u_bucket_4` with no residue marker, and U9 R-26 retires. | The RePark branch takes CREATE then ADD COLUMN then ADD PARTITION FIELD; the oracle keeps Spark's `u_bucket_4`; the U9 suite is green. | PROVEN | `test_u9_types_1.py`, 13 passed; the uuid UPDATE-door pin in `test_partname_1.py`. |

## Mutation record (2026-09-26)

Each line was broken, the named tests ran, and the file was restored.

| # | Mutation | Red |
|---|---|---|
| M1 | expect `u_bucket` where the uuid pin says `u_bucket_4` | `test_update_door_uuid_bucket_carries_the_width` reds, restored green |
| M2 | restore the CREATE-door RePark branch behind the de-residued oracle | U9 `[uuid]` reds on `uuid/part/md` (`u_bucket`, EQUAL step diverges), restored green |

## Coverage

```yaml
COVERAGE_ATTESTATION:
  pr_unit: partname-1
  categories:
    - id: AT-1
      status: ATTACKED
      evidence: Each clause is walked against Spark's measured answer on the same door; the new suite pins every named field and the U9 replay holds the uuid step EQUAL.
      artifacts: [python/repark/tests/test_partname_1.py, python/repark/tests/test_u9_types_1.py]
    - id: AT-2
      status: ATTACKED
      evidence: Every named transform is pinned on its door (bucket, truncate, years, months, days, hours, identity, a named field), with the DROP and REPLACE edges; the redundancy boundary is measured and recorded, not pinned.
      artifacts: [python/repark/tests/test_partname_1.py]
    - id: AT-3
      status: N/A
      justification: No failure path changes; the redundancy refusals in scope are a recorded residue with both engines' texts, not a contract this unit pins.
    - id: AT-4
      status: N/A
      justification: No commit, isolation or concurrency surface; each pin runs on a fresh warehouse in one session.
    - id: AT-5
      status: N/A
      justification: No privileged action, credential, deserialization or path handling; SQL text only.
    - id: AT-6
      status: ATTACKED
      evidence: No stored format changes; the U9 replay (397 steps, metadata observations included) guards the metadata shape the new pins read.
      artifacts: [python/repark/tests/test_u9_types_1.py, python/repark/tests/u9_types_1_spark_oracle.json]
    - id: AT-7
      status: N/A
      justification: No performance claim; pins only, no product code.
    - id: AT-8
      status: ATTACKED
      evidence: No Rust change, no dependency, no crate edge; Spark's per-door naming contract is pinned rather than presumed, and the fork divergence is filed, not worked around.
      artifacts: [python/repark/tests/test_partname_1.py, task/roadmap/mid-term/f-partspec-redundant-1-2026-09-26.md]
    - id: AT-9
      status: N/A
      justification: No failure path in scope; a renamed field fails its pin with the expected and observed names.
    - id: AT-10
      status: ATTACKED
      evidence: Each pin asserts exact text; M1 and M2 break one expectation and one door each and the named tests red before restore.
      artifacts: [python/repark/tests/test_partname_1.py, python/repark/tests/test_u9_types_1.py]
  complete: true
```

## Residues

| # | Residue |
|---|---|
| R-1 | Dated 2026-09-26 (fork defect, not fixed here; card `task/roadmap/mid-term/f-partspec-redundant-1-2026-09-26.md`): the redundancy rules differ per door. On the UPDATE door Spark accepts every distinct pair of transforms on one source (`years`+`hours`, `years`+`months`, `days`+`hours`, `days`+`months`, `months`+`days`, `bucket(8, id)`+`id`, `id`+`bucket(8, id)`, `bucket(8, id)`+`bucket(16, id)`, `truncate(2, s)`+`truncate(4, s)`) and refuses only the exact duplicate with `IllegalArgumentException`: `Cannot add duplicate partition field null=day(ref(name="ts")), conflicts with 1000: ts_day: day(3)`; RePark refuses `months(ts)` after `days(ts)` and `hours(ts)` after `years(ts)` with `DataInvalid => Cannot add redundant partition with source id `3` and transform `time`. A partition with the same source id and transform already exists with name `ts_day`` (respectively `ts_year`). Measured on RePark main 2026-09-26, every other distinct time-transform pair refuses with the same template naming the existing field: `years`+`months` → `… already exists with name `ts_year``, `days`+`hours` → `… already exists with name `ts_day``, `months`+`days` → `… already exists with name `ts_month`` (each `DataInvalid => Cannot add redundant partition with source id `3` and transform `time`. A partition with the same source id and transform already exists with name `<field>``), while the non-time pairs commit like Spark; RePark's exact-duplicate text is `DataInvalid => Cannot add duplicate partition field, conflicts with existing field: ts_day`. On the CREATE door both engines refuse redundant time transforms, with different texts: Spark `IllegalArgumentException`: `Cannot add redundant partition: 1000: ts_year: year(0) conflicts with 1001: ts_month: month(0)` (and `… ts_day: day(0) conflicts with 1001: ts_month: month(0)`), RePark `DataInvalid => Cannot add redundant partition with source id `1` and transform `time`. A partition with the same source id and transform already exists with name `ts_year`` for `(years(ts), months(ts))` and the same template ending `… already exists with name `ts_day`` for `(days(ts), months(ts))`. Spark refuses `(bucket(4, id), bucket(8, id))` on CREATE with `AnalysisException`: `Found duplicate column(s) in the partitioning: id`; RePark's measured answer for that shape (2026-09-26) is `PySparkException`: `DataInvalid => Cannot use partition name more than once: id_bucket`. |
