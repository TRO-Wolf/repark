# Charter ledger — RP-9 · fork repin c1d6c9de → 594bdbe5 (consume F-23; close PERF-DVCLOSE-WALK-1)

**Date:** 2026-09-03 · **Branch:** `feat/rp-9-repin-f23` · **Base:** `origin/main`
`a955d61` · **Model:** grok-4.6 · **Policy:**
[../../../AGENTS.md](../../../AGENTS.md) "Version-pin contract".
**Path:** STANDARD. **Proven pattern:**
[../completed/rp-8-repin-f21-f22-ledger.md](../completed/rp-8-repin-f21-f22-ledger.md).

**Retires:** this ledger moves to `../completed/` in this unit's last commit.

**Why now.** RP-8 errata E-4 filed `PERF-DVCLOSE-WALK-1`: F-22 walked every data manifest
on the pure-DV path. Fork F-23 (`594bdbe5f257455d77ac49f1a2d50794a1aea6fd`) skips that
walk when there are no legacy deletes and `known_partitions` covers every touched path.

**Not in this unit:** any dependency change beyond the one `[patch.crates-io]` rev; a
Spark-visible design choice not measured (HALT).

## PROPOSITION LEDGER — RP-9 — 2026-09-03

| Clause | Proposition (checkable) | Proof obligation | Verdict | Evidence / open question |
|---|---|---|---|---|
| C-001 | Every `iceberg*` `[patch.crates-io]` rev is `594bdbe5f257455d77ac49f1a2d50794a1aea6fd` and `Cargo.lock` resolves to it; `datafusion`, `datafusion-spark`, `arrow*`, `parquet` and `rust-toolchain.toml` are byte-identical to `main`; the pin-history row names F-23; the bare repin blast radius is tabled. | `make bump-fork-pin`; `grep` the five revs; the two close pins at the untouched source. | **PROVEN** | Five revs and six lock sources are `594bdbe5`. Family freeze holds (`datafusion` 54.1.0, `arrow`/`parquet` 58.4.0, toolchain 1.96.0). Bare-repin: compiles; `closing_a_covered_v3_delete_reads_the_data_manifest_for_sequence_numbers` green (empty map still walks); `a_supplied_partition_map_still_walks_the_data_manifests_for_sequence_numbers` **1 red of 1** — hiding the data manifests now succeeds. Citation: `docs/fork-sync.md`. |
| C-002 | RePark's DV close passes a complete `known_partitions` map on the pure-DV path. The fork reads ZERO data manifests there (`data_sequence_numbers.is_empty()`, hide-and-succeed). A MoR statement with a live legacy delete still fills the sequence map (total). No RePark consumer assumes `data_sequence_numbers` is total on the pure-DV path. | The flipped pin; the legacy pin; the grep; mutations. | **PROVEN** | `a_supplied_partition_map_closes_a_fresh_partitioned_delete_with_no_data_manifest` green with manifests hidden and `data_sequence_numbers` empty. `a_legacy_delete_fills_data_sequence_numbers_even_with_a_complete_partition_map` green: hide refuses, map contains the touched path, `legacy_deletes` non-empty. Grep of `crates/`: `data_sequence_numbers` is only the three test assertions; `apply_close` reads `added` / `removed` only. M1 empty the map **1 red of 1**; M2 assume totality (`contains_key` on the pure-DV pin) **1 red of 1**. Citation: `crates/repark-iceberg/src/write/merge/map.md`. |
| C-003 | The pure-DV fixture (N data manifests, 0 legacy deletes, complete map) at 8 / 48 / 192 manifests, before (RP-8 pin) and after, statement wall, three runs each, medians in this ledger. No wall-clock CI pin. | The `#[ignore]`d cell; the before/after table. | **PROVEN** | `measure_pure_dv_close_cost`, this clone, debug. §8 table. Citation: `crates/repark-spark/src/tests/map.md`. |
| C-004 | The record says what landed: registry `PERF-DVCLOSE-WALK-1` FIXED 2026-09-03 (RP-9) with the table; RP-8 ledger E-4 gets a one-line closure pointer; every touched `map.md` in lockstep. | The registry row; E-4; the maps; the gates. | **PROVEN** | Registry bullet FIXED with the table. E-4 ends "Closed by RP-9 2026-09-03 at pin `594bdbe5`". V1-GATE meta-pin reads `594bdbe5`. Citation: `python/repark-parity/tests/map.md`. |

VERDICT: 4 clauses, 4 PROVEN, 0 OPEN, 0 REJECTED.

```yaml
COVERAGE_ATTESTATION:
  pr_unit: rp-9-repin-f23
  categories:
    - id: AT-1
      status: ATTACKED
      evidence: The pure-DV complete-map pin hides every data manifest and requires the close to succeed with an empty sequence map; the legacy-delete pin hides them and requires a refusal plus a total map.
      artifacts: [crates/repark-iceberg/src/write/merge/dv_close.rs]
    - id: AT-2
      status: ATTACKED
      evidence: Empty known_partitions still walks (covered-v3 pin). Complete map with zero legacy skips. Complete map with a live parquet delete still walks.
      artifacts: [crates/repark-iceberg/src/write/merge/dv_close.rs]
    - id: AT-3
      status: ATTACKED
      evidence: Hiding data manifests on the skip path succeeds; on the legacy path it refuses. No silent fallback.
      artifacts: [crates/repark-iceberg/src/write/merge/dv_close.rs]
    - id: AT-4
      status: N/A
      justification: No new shared state or concurrency; the close still runs inside the commit future.
    - id: AT-5
      status: ATTACKED
      evidence: No AWS, IAM or secret handling. No .github change. The one dependency change is the single [patch.crates-io] rev and its lock rows.
      artifacts: [Cargo.toml, Cargo.lock]
    - id: AT-6
      status: ATTACKED
      evidence: No public API break. data_sequence_numbers is not total on the pure-DV path; apply_close does not read it. Grep of crates/ shows only the three test assertions.
      artifacts: [crates/repark-iceberg/src/write/merge/dv_close.rs]
    - id: AT-7
      status: ATTACKED
      evidence: Pure-DV statement wall measured at 8/48/192 before and after, three runs, medians in §8. The load-bearing close is the zero-manifest pin, not a wall-clock CI pin.
      artifacts: [crates/repark-spark/src/tests/v3_legacy_delete.rs]
    - id: AT-8
      status: ATTACKED
      evidence: Five iceberg* revs and six lock sources are 594bdbe5. Family freeze holds. Bare-repin compiled; one pin red 1 of 1.
      artifacts: [Cargo.toml, Cargo.lock, docs/fork-sync.md]
    - id: AT-9
      status: ATTACKED
      evidence: Registry PERF-DVCLOSE-WALK-1 FIXED with the table; RP-8 E-4 closed; V1-GATE meta-pin moved with the pin.
      artifacts: [docs/spark-sql-iceberg-parity.md, python/repark-parity/tests/test_v1_gate_docs.py]
    - id: AT-10
      status: ATTACKED
      evidence: Four clauses pinned; M1 1 red of 1; M2 1 red of 1; grep proves no production totality assumption.
      artifacts: [crates/repark-iceberg/src/write/merge/dv_close.rs, crates/repark-iceberg/src/write/merge/map.md]
  complete: true
```

## 6. What changed under us (C-001)

Range `c1d6c9de1498cf04765893ef3f698d915766a6a7..594bdbe5f257455d77ac49f1a2d50794a1aea6fd`.

| Fork change | Change | Engine site that absorbs it |
|---|---|---|
| `close_touched_dv_containers_with_partitions` data-manifest walk | SKIPPED when no legacy deletes and `known_partitions` covers every touched path; otherwise walks until every wanted path is found (first manifest loaded alone) | `plan_deletion_vectors` already passes the scan's complete map on the production path; no call-site change |
| `DvContainerClose::data_sequence_numbers` | total with legacy deletes; otherwise only the paths the map missed (may be empty) | tests flipped; `apply_close` never read it |

Public API breaks absorbed: **zero**. Bare-repin compiles.

## 7. Pins (C-002)

| Pin | Observable |
|---|---|
| `a_supplied_partition_map_closes_a_fresh_partitioned_delete_with_no_data_manifest` | hide data manifests + complete map → close succeeds, `data_sequence_numbers.is_empty()` |
| `closing_a_covered_v3_delete_reads_the_data_manifest_for_sequence_numbers` | empty map + hide → refuse; restore → map contains the touched path |
| `a_legacy_delete_fills_data_sequence_numbers_even_with_a_complete_partition_map` | v2 parquet delete, upgrade, complete map + hide → refuse; restore → map total, `legacy_deletes` non-empty |

Grep `data_sequence_numbers` under `crates/` (not `map.md`): three test assertions in `dv_close.rs` only.

## 8. The walk, before and after (C-003)

`measure_pure_dv_close_cost`, this clone, debug, three runs a side. Fixture: N append commits at `commit.manifest-merge.enabled = false`, 0 legacy deletes, one `DELETE WHERE id = 0`. "Before" is pin `c1d6c9de`; "after" is this tree at `594bdbe5`.

| Data manifests | Before (RP-8 pin) | After (F-23) | Median before | Median after |
|---|---|---|---|---|
| 8 | 491.1 / 141.9 / 1586.5 ms | 144.5 / 143.7 / 143.3 ms | 491 ms | 144 ms |
| 48 | 709.0 / 703.4 / 924.4 ms | 750.7 / 697.1 / 710.9 ms | 709 ms | 711 ms |
| 192 | 2.729 / 4.609 / 2.750 s | 2.710 / 2.666 / 2.712 s | 2.750 s | 2.710 s |

48 and 192 statement walls stay inside run-to-run noise. The skip is the zero-manifest pin, not a wall-clock. Fork close-only (debug, launch line): 29.7 / 185 / 713 ms → 1.2 / 1.7 / 3.9 ms.

## 9. Delivery template

```yaml
SHIPPED_FLAG_REGISTER:
  pr_unit: rp-9-repin-f23
  flags: []
  count: 0

DELIVERY_SIGNOFF:
  pr_unit: rp-9-repin-f23
  artifacts_verified:
    ledger: PASS (C-001..C-004 PROVEN)
    coverage_attestation: PASS (AT-1..AT-10, complete true)
    findings_ledger: PASS (none filed)
    shipped_flag_register: PASS (count 0)
  done_gate: PASS (see §10)
  status_update: N/A (STATUS does not name PERF-DVCLOSE-WALK-1)
  verdict: PENDING
  rejection_route: N/A
```

## 10. Gates

| Gate | Exit |
|---|---|
| bare-repin `closing_a_covered…` | 0 |
| bare-repin `a_supplied_partition_map_still_walks…` | 101 (1 red of 1) |
| `write::merge::dv_close::tests` after C-002 | 0 (4 passed) |
| M1 empty the map | 1 red of 1 |
| M2 assume totality | 1 red of 1 |
| `make verify` | 0 |
| `make develop` | 0 |
| `.venv/bin/python -m pytest python/repark/tests -q -x --deselect python/repark/tests/test_pyspark_compat_smoke.py -k "not test_cross_validator_live_pyspark_shape"` | 0 (4428 passed; `test_cross_validator_live_pyspark_shape` is a SemLock PermissionError, not this pin) |
| `.venv/bin/python -m pytest python/repark-parity/tests -q` | 0 (555 passed) |
| `make check-map-sync` | 0 |
| `make check-ledger-grammar` | 0 |
| `make check-ledgers` | 0 |
| `make check-docs-compaction` | 0 |
| `python3 scripts/ledger_lifecycle.py check --base origin/main` | 0 |
| `typos .` | 0 |
| `cargo deny check` | 0 |
