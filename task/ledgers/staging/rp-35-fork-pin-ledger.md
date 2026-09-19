# Unit ledger — RP-35-FORK-PIN · the fork pin moves to `7bd2fea3` and IPI-52 is pinned against Spark

## Round 1 (2026-09-19)

**Date:** 2026-09-19 · **Branch:** `chore/rp-35-fork-pin` · **Base:** `5ceeb2cc`
**Model:** claude-opus-5 (orchestrator, run 24c) · **Policy:** [../../../AGENTS.md](../../../AGENTS.md).
**Path:** STANDARD. **risk_tier: standard** — a pin bump plus recorded-oracle pins. No RePark
product code.
**Fork:** `TRO-Wolf/iceberg-rust` #308 (F-AVRO-NAME-1): Avro field names in manifests are
sanitised as Java's `AvroSchemaUtil` does.
**Registry:** `ICE-AVRO-NAME-1` (new, FIXED).

**Measured.** Run 23c's inventory cell `E-QUOTED-SPACE-COL`: before the fork fix a RePark table
partitioned by `my col` wrote a manifest whose partition record kept the raw name, and every
later read failed. Run 24d recorded 20 cells (IPI-52 oracle); this round re-records them with
the committed recorder, and the replay equals run 24d's truth on every cell.

## PROPOSITION LEDGER — RP-35-FORK-PIN — 2026-09-19

| ID | Claim | Evidence | Status | Notes |
|----|-------|----------|--------|-------|
| C-001 | At fork `7bd2fea3` every IPI-52 cell answers Spark: rows, the `partitions` metadata table and the manifest's Avro partition record. | `test_ice_avro_name_1.py` (60 offline pins over 20 cells) against `ice_avro_name_1_spark_oracle.json`, re-derived by `_record_ice_avro_name_1_oracle.py` in the live tier. | PROVEN | Registry row `ICE-AVRO-NAME-1`. |

## Coverage attestation

```yaml
COVERAGE_ATTESTATION:
  pr_unit: rp-35-fork-pin
  categories:
    - id: AT-1
      status: ATTACKED
      evidence: Every expectation is read from the committed Spark 4.1.2
        recording; the recorder is committed and replayable.
      artifacts: [python/repark/tests/ice_avro_name_1_spark_oracle.json, python/repark/tests/_record_ice_avro_name_1_oracle.py]
    - id: AT-2
      status: ATTACKED
      evidence: Red before the bump is the inventory cell's measured read
        failure; the manifest-record pins fail if the raw name returns.
      artifacts: [python/repark/tests/test_ice_avro_name_1.py]
    - id: AT-3
      status: ATTACKED
      evidence: Ten names cover the sanitiser's branches (space, leading
        digit, punctuation, non-ASCII letters kept, surrogate pairs) plus
        two transforms and a valid control, on v2 and v3.
      artifacts: [python/repark/tests/test_ice_avro_name_1.py]
    - id: AT-4
      status: N/A
      justification: No shared mutable state is added.
    - id: AT-5
      status: N/A
      justification: The pins run on tmp_path catalogs; no network or credentials.
    - id: AT-6
      status: ATTACKED
      evidence: The registry row quotes the measured manifest names.
      artifacts: [docs/spark-sql-iceberg-parity.md]
    - id: AT-7
      status: N/A
      justification: No wall-clock claim.
    - id: AT-8
      status: ATTACKED
      evidence: The tree carries the pin, the lockfile, the pins, the
        fixture, the recorder, one registry row, map rows and this ledger.
      artifacts: [Cargo.toml, Cargo.lock]
    - id: AT-9
      status: ATTACKED
      evidence: The row names the fork PR that fixes it and the pins.
      artifacts: [docs/spark-sql-iceberg-parity.md]
    - id: AT-10
      status: ATTACKED
      evidence: CI runs the workspace Rust tests and the whole facade suite
        at the new pin.
      artifacts: [task/ledgers/staging/rp-35-fork-pin-ledger.md]
  complete: true
```

## Hand-back

`Model: claude-opus-5`. `risk_tier: standard`. Round 1 CONCLUDED with one clause PROVEN.
