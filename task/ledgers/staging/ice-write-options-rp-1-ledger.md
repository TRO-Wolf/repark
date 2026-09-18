# Unit ledger — ICE-WRITE-OPTIONS-RP-1 · a caller's `replace-partitions` summary value wins

## Round 1 (2026-09-18)

**Date:** 2026-09-18 · **Branch:** `chore/rp-30-fork-pin` · **Base:** `5dc98c20` (RP-30 pin bump)
**Model:** muse-spark-1.3-contributor · **Policy:** [../../../AGENTS.md](../../../AGENTS.md).
**Path:** STANDARD. **Rubric:** STANDARD. `risk_tier: standard`.
**Registry:** `ICE-WRITE-OPTIONS-1-R-RP` (run 22b residue).
**Fork:** `TRO-Wolf/iceberg-rust` #298 (F-RP-SUMMARY-USER-1), in the workspace pin since RP-30
`5dc98c20`; #297 (F-SHED-295) sheds relocated comment lines, no behaviour change.

**Retires:** this ledger moves to `../completed/` when the unit's last commit lands.

**Why now.** At fork `18ab9761` a caller-supplied `replace-partitions` snapshot-summary
value wins over the replace-partitions action's own marker, as Java's `SnapshotProducer`
applies user `set(...)` properties after the operation's own; the cherry-pick path reads
the key case-insensitively. The run-22b residue that said RePark wrote `true` either way
is fixed by the pin; this unit records Spark's answer and pins it.

**Not in this unit:** `STATUS.md`, `Cargo.toml` / `Cargo.lock` (the bump is done), any
product-code change, the sibling write-options suites (read, not changed).

## The measured oracle

Spark 4.1.2 + `iceberg-spark-runtime-4.1_2.13:1.11.0`, recorded 2026-09-18, checked in as
`python/repark-parity/fixtures/torture/data/ice_write_options_rp_1/spark_rp_oracle.json`
(see its `map.md`): 5 cells, each a fresh table (`id INT, p STRING`,
`PARTITIONED BY (p)`, seed `(1, 'a'), (2, 'b')`, source `(3, 'a')`) — dynamic
`insertInto` overwrite with the option `false` / `true`, `writeTo(t).overwritePartitions()`
with the option `true` / `false`, and the `overwritePartitions()` control with
`snapshot-property.k=v`. Every cell commits; the `false` cells stamp
`replace-partitions=false`, the `true` cells `true`, the control `true` with `k=v`.
The recording agrees cell for cell with the orchestrator's `probe_rp.py` `RESULT` line of
2026-09-18 (run 22b).

## Fix

Test-only. `test_ice_write_options_rp_1.py` replays each cell on a fresh RePark memory
catalog with the cell's table, seed, source and door — `insertInto` overwrite under
`partitionOverwriteMode=dynamic`, or `writeTo(t).overwritePartitions()` — then asserts
the write committed (snapshot count before + 1) and the newest snapshot's summary
`replace-partitions` (and `k` for the control) equals the cell's. The live tier replays
the `overwritePartitions_false` cell on Spark and answers the recorded summary.

## Rulings

- **Q-1 — red at the old pin by reasoning, not by re-measurement.** The orchestrator
  measured nothing new at the old pin in this round. The registry row's sentence at the
  old pin (quoted verbatim): "**repark** commits with `replace-partitions=true` either
  way: the fork's `ReplacePartitionsAction` inserts its marker after the caller's summary
  properties." At that pin the two `false` cells (`insertInto_dynamic_false`,
  `overwritePartitions_false`) would read `true` against the recorded `false` and fail,
  while the two `true` cells and the control would pass. The pins are red-first exactly
  where the fork change bites.
- **Q-2 — one fresh table per cell, not the probe's sequential table.** The recorder
  seeds a new table per cell while `probe_rp.py` ran the five writes sequentially on one
  table. The pinned fields (commit flag plus the two summary values) are per-write, not
  cumulative, and agree cell for cell with the probe's `RESULT` line; rows are not pinned.

## PROPOSITION LEDGER — ICE-WRITE-OPTIONS-RP-1 — 2026-09-18

| Clause | Proposition (checkable) | Proof obligation | Verdict | Evidence |
|---|---|---|---|---|
| C-001 | Spark's answer is recorded: five cells on Spark 4.1.2 + Iceberg 1.11.0, every cell commits, the caller value lands, the control keeps the marker with `k=v`. | `spark_rp_oracle.json` plus its `map.md` plus the parent `data/map.md` row. | **PROVEN** | Recorded 2026-09-18 (Hadoop catalog, `local[2]`; fixture SHA-256 `2d67f856b20d38876ae24bf57379fede1b25efe8f8ae563845fc9616f4e3f4a6`): `insertInto_dynamic_false` → `false`, `insertInto_dynamic_true` → `true`, `overwritePartitions_true` → `true`, `overwritePartitions_false` → `false`, `overwritePartitions_control_other` → `true` with `k=v`; every cell `committed` true. Agrees cell for cell with the orchestrator's `probe_rp.py` `RESULT` line (run 22b). |
| C-002 | The record driver reproduces the probe's five writes repo-idiomatically. | `_record_ice_write_options_rp_1.py` plus the tests `map.md` entry. | **PROVEN** | Runtime GAV from `_oracle_pins` through `spark.jars.packages`, Ivy cache from `REPARK_ORACLE_IVY`, warehouse from `tempfile`, one fresh table per cell; ruff-clean, `grep #` empty. |
| C-003 | One pin per recorded cell asserts the commit and Spark's summary values; green at this pin. | `test_ice_write_options_rp_1.py` (five cell tests). | **PROVEN** | Offline `pytest test_ice_write_options_rp_1.py -q -p no:cacheprovider -n 4` → `5 passed, 1 skipped` (the live cell skips JVM-free) on the release native at fork `18ab9761`. |
| C-004 | The pins are red at the old pin where the fork change bites, by the registry row's sentence. | Registry row `ICE-WRITE-OPTIONS-1-R-RP` old sentence, quoted in Rulings Q-1. | **PROVEN** | Old sentence: "**repark** commits with `replace-partitions=true` either way: the fork's `ReplacePartitionsAction` inserts its marker after the caller's summary properties." The two `false` cells fail against it; nothing was re-measured at the old pin. |
| C-005 | The live tier re-derives one cell on Spark. | `test_live_spark_rederives_false_cell`. | **PROVEN** | Live (`REPARK_PARITY_LIVE=1`, zulu-17, jvm-lock) `pytest test_ice_write_options_rp_1.py -q -p no:cacheprovider` → `6 passed`: the five pins plus Spark replaying `overwritePartitions_false` and answering the recorded summary. |
| C-006 | The sibling write-options suites stay green; no pin there changes meaning at this pin. | `test_ice_write_options_1.py`, `test_ice_write_options_1_rebase.py`. | **PROVEN** | `pytest test_ice_write_options_1.py test_ice_write_options_1_rebase.py -q -p no:cacheprovider -n 4` → `82 passed, 1 skipped` at this pin. Neither suite sets `snapshot-property.replace-partitions`: the rebase pins assert the engine's own `true` marker (WO-DYN-01) and its absence on the static path (WO-DYN-03), both unchanged by a caller-value-wins rule; nothing was brought true. |
| C-007 | The registry residue is FIXED with the pins. | Registry row `ICE-WRITE-OPTIONS-1-R-RP`. | **PROVEN** | Row reads **FIXED 2026-09-18 (RP-30, fork #298 F-RP-SUMMARY-USER-1)** with the pin pointer; the old `true`-either-way sentence is replaced by the caller-wins sentence. |
| C-008 | Maps and ledger are lockstep; the mechanical gates are clean. | Staging `map.md` row, tests `map.md` rows, fixture `map.md`; `check_ledger_grammar.py`, `check_docs_links.py`, `sync_map_md.py --check`. | **PROVEN** | `sync_map_md.py --check` → 298 maps clean; `ruff check` + `ruff format --check` clean on both new Python files; `check_ledger_grammar.py` and `check_docs_links.py` clean once this ledger is tracked (both flagged only the untracked ledger before the commit). |

VERDICT: 8 clauses, 8 PROVEN, 0 OPEN, 0 REJECTED.

## Red evidence

- Old-pin reasoning (no re-measurement): the registry row's quoted sentence in Rulings Q-1
  makes the two `false` cells fail at the old pin and leaves the other three green.
- The sibling suites set no caller `replace-partitions` value, so fork #298 cannot move
  them; their runs confirm it (§Facade evidence).

## Facade evidence (release native, `CARGO_PROFILE_RELEASE_CODEGEN_UNITS=16 maturin develop --release`)

- Offline: `pytest python/repark/tests/test_ice_write_options_rp_1.py -q -p no:cacheprovider -n 4` →
  5 passed, 1 skipped.
- Live: `REPARK_PARITY_LIVE=1 JAVA_HOME=/usr/lib/jvm/zulu-17-amd64
  SPARK_LOCAL_IP=127.0.0.1 ... jvm-lock.sh .venv/bin/python -m pytest
  python/repark/tests/test_ice_write_options_rp_1.py -q -p no:cacheprovider` →
  6 passed.
- Siblings: `test_ice_write_options_1.py` + `test_ice_write_options_1_rebase.py` →
  82 passed, 1 skipped.
- Lint: `ruff check` + `ruff format --check` clean on both new Python files.

## Gates

| Command | Result |
|---|---|
| `ruff check` plus `ruff format --check` (pinned 0.15.2) on the recorder and the pin file | 0 — all checks passed, both files formatted |
| Comment-ban driver over the branch against `origin/main` | 0 hits (§5) |
| `check_ledger_grammar.py` | clean once this ledger is tracked (only the untracked ledger flagged before) |
| `check_docs_links.py` | clean once this ledger is tracked (only the untracked ledger flagged before) |
| `sync_map_md.py --check` | 0 — 298 maps clean |

## 5. Comment-ban gate (verbatim)

```text
comment-ban hits=0
```

## Coverage attestation

```yaml
COVERAGE_ATTESTATION:
  pr_unit: ice-write-options-rp-1
  categories:
    - id: AT-1
      status: ATTACKED
      evidence: Every expectation is read from the committed Spark 4.1.2 oracle
        (the 2026-09-18 recording, SHA-256 in the fixture map.md, agreeing cell
        for cell with the probe RESULT line); no hand-computed summary value
        anywhere.
      artifacts: [python/repark-parity/fixtures/torture/data/ice_write_options_rp_1/spark_rp_oracle.json, python/repark/tests/_record_ice_write_options_rp_1.py]
    - id: AT-2
      status: ATTACKED
      evidence: Red at the old pin by the registry row's quoted sentence (the two
        false cells read true at the old pin); the recorder replays the probe's
        five writes with GAV from _oracle_pins and Ivy from REPARK_ORACLE_IVY.
      artifacts: [python/repark/tests/test_ice_write_options_rp_1.py, docs/spark-sql-iceberg-parity.md]
    - id: AT-3
      status: ATTACKED
      evidence: All five recorded cells are one test each (commit plus summary
        replace-partitions, k on the control); the live tier re-derives the
        false cell on Spark.
      artifacts: [python/repark/tests/test_ice_write_options_rp_1.py]
    - id: AT-4
      status: N/A
      justification: No product code and no shared mutable state change on the RePark side; the behaviour change is the fork pin.
    - id: AT-5
      status: ATTACKED
      evidence: Each pin uses a fresh memory catalog under tmp_path; the recorder
        and the live tier run under the JVM lock; no network, no credentials.
      artifacts: [python/repark/tests/test_ice_write_options_rp_1.py]
    - id: AT-6
      status: ATTACKED
      evidence: Commit and summary values are asserted as values against the
        fixture cells; rows are deliberately not pinned.
      artifacts: [python/repark-parity/fixtures/torture/data/ice_write_options_rp_1/map.md]
    - id: AT-7
      status: N/A
      justification: No wall-clock claim anywhere in the change.
    - id: AT-8
      status: ATTACKED
      evidence: The bump touches only the five rev lines and Cargo.lock (plus the
        fork-sync and root map rows); the rest is tests, fixture, recorder,
        registry, maps and this ledger. No STATUS.md edit.
      artifacts: [Cargo.toml, Cargo.lock, docs/fork-sync.md]
    - id: AT-9
      status: ATTACKED
      evidence: The behaviour is registered where it lives — row
        ICE-WRITE-OPTIONS-1-R-RP FIXED with the pin pointer, the fixture map,
        the tests map and the staging ledger map; check_docs_links is clean.
      artifacts: [docs/spark-sql-iceberg-parity.md, python/repark/tests/map.md]
    - id: AT-10
      status: ATTACKED
      evidence: The sibling write-options suites set no caller replace-partitions
        value, so fork #298 cannot move them; both stay green at this pin.
      artifacts: [python/repark/tests/test_ice_write_options_1.py, python/repark/tests/test_ice_write_options_1_rebase.py]
  complete: true
```
