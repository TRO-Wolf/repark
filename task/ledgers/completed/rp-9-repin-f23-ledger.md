# Charter ledger — RP-9 · fork repin c1d6c9de → 594bdbe5 (consume F-23; close PERF-DVCLOSE-WALK-1)

**Date:** 2026-09-03 · **Branch:** `feat/rp-9-repin-f23` · **Base:** `origin/main`
`a955d61` · **Model:** grok-4.6 · **Policy:**
[../../../AGENTS.md](../../../AGENTS.md) "Version-pin contract".
**Path:** STANDARD. **Proven pattern:**
[../completed/rp-8-repin-f21-f22-ledger.md](../archive/2026-09/2026-09-03-rp-8-repin-f21-f22-ledger.md).

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
| C-004 | The record says what landed: registry `PERF-DVCLOSE-WALK-1` FIXED 2026-09-03 (RP-9) with the table; RP-8 ledger E-4 gets a one-line closure pointer; every touched `map.md` in lockstep. | The registry row; E-4; the maps; the gates. | **PROVEN** | Registry bullet FIXED with the table. E-4 prepended closed. V1-GATE meta-pin reads `594bdbe5`. Citation: `python/repark-parity/tests/map.md`. |
| C-005 | A plain Spark/ANSI `DELETE … WHERE` (no subquery) on a three-part Iceberg target runs through `execute_predicate_dml`, so the production identity scan's `known_partitions` covers the touched path and the F-23 skip engages. Hide-and-succeed on that path. | `try_allowed_plain_identity`; the production-sink pin; mutation. | **PROVEN** | Probe: `record_scanned_partitions` and `retain` keep the touched `_file` when the identity SQL path runs. Owning miss: `iceberg-datafusion` `write_deletion_vectors` passed `HashMap::new()`. Fix: `plain::try_allowed_plain_identity` on both SQL doors. Pin `a_plain_identity_delete_closes_with_no_data_manifest`. M3 clear the drained map **1 red of 1**. Citation: `crates/repark-iceberg/src/write/predicate_dml/map.md`. |

VERDICT: 5 clauses, 5 PROVEN, 0 OPEN, 0 REJECTED.

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
    ledger: PASS (C-001..C-005 PROVEN)
    coverage_attestation: PASS (AT-1..AT-10, complete true)
    findings_ledger: PASS (none filed)
    shipped_flag_register: PASS (count 0)
  done_gate: PASS (see §10)
  status_update: N/A (STATUS does not name PERF-DVCLOSE-WALK-1)
  verdict: PENDING
  rejection_route: N/A
```

Duplicate-row guard (R5): `grep -oE '^- \[[^]]+\]' task/ledgers/completed/map.md | sort | uniq -d` must be empty.

## 10. Gates

| Gate | Exit |
|---|---|
| bare-repin `closing_a_covered…` | 0 |
| bare-repin `a_supplied_partition_map_still_walks…` | 101 (1 red of 1) |
| `write::merge::dv_close::tests` after C-002 | 0 (4 passed) |
| M1 empty the map | 1 red of 1 |
| M2 assume totality | 1 red of 1 |
| duplicate-row guard (`grep -oE '^- \[[^]]+\]' task/ledgers/completed/map.md \| sort \| uniq -d`) | 0 (empty) |
| `make verify` | 0 |
| M3 `known.clear()` before hide | 101 (1 red of 1) |
| `make develop` | 0 |
| `.venv/bin/python -m pytest python/repark/tests -q --deselect python/repark/tests/test_pyspark_compat_smoke.py` | 0 at `6121be6` re-measured by the round-2 critic (4395 passed, 194 skipped; the only failure is `test_compat_smoke_suite_in_subprocess` on a snapshot without the native module) — the round-1 note "5 failed + 3 errors from the B-MOR-3 refuse" was a stale lane build, not the tip |
| `.venv/bin/python -m pytest python/repark-parity/tests -q` | 0 (555 passed) |
| `make check-map-sync` | 0 |
| `make check-ledger-grammar` | 0 |
| `make check-ledgers` | 0 |
| `make check-docs-compaction` | 0 |
| `python3 scripts/ledger_lifecycle.py check --base origin/main` | 0 |
| `typos .` | 0 |
| `cargo deny check` | 0 |

## 11. Round 2 — critic FAIL, map miss, production skip (2026-09-03)

**Model:** grok-4.6

### Critic strace (before C-005; IcebergDeleteExec empty map)

| cell | data-manifest opens per statement |
|---|---|
| plain scan (COUNT) | 1× per manifest |
| INSERT commit | 1× |
| pure-DV DELETE | **4×** |
| DELETE id=191 vs id=0 | 608 vs 799 avro opens |

The 191-open delta is the close's newest-first until-found walk on an empty `known_partitions`:
id=191 is the last commit (first in the list → ~0 extra), id=0 is the first commit (last in
the list → ~192 extra). `unresolved` was not empty.

### Perf-review strace (after C-005; identity path, same 192-fixture)

| phase | data-manifest opens | wall |
|---|---|---|
| plan/scan to the puffin write | 3 × 192 (`TargetScanStream::execute` `plan_files` three times when a partition sink is set) | ~2.5 s |
| DV close | **0 extra** (F-23 skip engages) | — |
| commit | 1 × 192 (fork `validate_fresh_dvs_only`, no early exit) | ~0.8 s |

Both readings are right for their trees. The critic measured the fork delete exec (empty map).
The perf review measured the routed identity path (complete map). Total opens stay ~4× after
the skip because scan 3× + commit 1× replace close-walk 1×.

### Probe (R2 / R7)

`a_multi_manifest_identity_scan_records_the_touched_path` — `record_scanned_partitions`
(`target_scan.rs`) does not drop the task; `retain` in `plan_deletion_vectors`
(`dv_close.rs`) keeps the touched `_file`. Owning miss is **not** the sink or the retain.

Owning site: Spark/ANSI delegated `DELETE WHERE id = 0` to the fork's `IcebergDeleteExec` →
`write_deletion_vectors(..., &HashMap::new(), None)` (`delete_legacy_merge.rs`). The
production identity scan never ran, so the map the fork received was empty.

R7: once that path is the identity scan, close-phase data-manifest opens are **zero** (hide
pin). The map-miss fix is still required; it is the routing change, not a sink/retain patch.

### Fix (C-005)

`try_allowed_plain_identity` (`predicate_dml/plain.rs`) on both SQL doors
(`spark_ast.rs`, `repark-sql/src/router.rs`) routes a three-part Iceberg `DELETE` (only; `UPDATE`, literal `IN` lists, subquery `WHERE` and four-part branch targets stay on the fork `TableProvider`, pinned in `predicate_dml/tests/plain.rs`)
with a subquery-free `WHERE` through `execute_predicate_dml`.

R8: that routing does **not** collapse the 3× `plan_files`. Not contained; filed
`PERF-SCAN-3PASS-1` BACKLOG, queued **PERF-SCAN-1**. Routing is kept (it is the R2 map-miss
fix).

R9: `PERF-DVCLOSE-STMT-1` is the fork `validate_fresh_dvs_only` walk only; F-25.

### Pins and mutation

| Pin | Observable |
|---|---|
| `a_plain_identity_delete_closes_with_no_data_manifest` | identity-SQL sink + hide data manifests → close succeeds, `data_sequence_numbers` empty (close opens **zero**) |
| `execute_predicate_dml_deletes_id_zero_on_an_eight_manifest_table` | production `execute_predicate_dml` on the 8-manifest fixture deletes id 0 |
| `created_v3_mor_plain_where_dml_matches_the_subquery_cell` | Spark door `DELETE`/`UPDATE … WHERE id = 2` takes the identity path |
| `plain_where_delete_is_identity_dml` | `try_allowed_plain_identity` accepts `DELETE … WHERE id = 0`, refuses a subquery `WHERE` |

M3: `known.clear()` after the sink assert, before hide → hide pin **1 red of 1**
(`Failed to read file …-m0.avro: No such file or directory`). Restored.

### Statement wall after C-005

`measure_pure_dv_close_cost`, this clone, debug, three runs. R6: the 8-manifest "before" set
is noisy (min 141.9 ms equals the old after-median); do not claim that 8-manifest improvement.

| Data manifests | Before (RP-8 pin, empty map) | After C-005 (identity path) | Median before | Median after |
|---|---|---|---|---|
| 8 | 491.1 / 141.9 / 1586.5 ms (noisy; min equals the old after-median) | 104.4 / 97.0 / 95.2 ms | 491 ms (not a claimed win) | 97 ms |
| 48 | 709.0 / 703.4 / 924.4 ms | 411.2 / 412.8 / 417.5 ms | 709 ms | 413 ms |
| 192 | 2.729 / 4.609 / 2.750 s | 1.538 / 1.545 / 1.536 s | 2.750 s | 1.538 s |

Opens per data manifest per phase, 192-fixture `DELETE WHERE id = 0`:

| phase | before C-005 (empty map) | after C-005 (identity path) |
|---|---|---|
| scan | ~1–2× (fork delete exec) | **3×** (`PERF-SCAN-3PASS-1`) |
| close | until-found (~0× id=191, ~1× id=0) | **0×** |
| commit | 1× (`validate_fresh_dvs_only`) | **1×** (`PERF-DVCLOSE-STMT-1` / F-25) |

Duplicate-row guard (§9): `grep -oE '^- \[[^]]+\]' task/ledgers/completed/map.md | sort | uniq -d` empty.
