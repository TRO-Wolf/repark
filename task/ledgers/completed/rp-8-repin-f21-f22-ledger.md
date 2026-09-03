# Charter ledger — RP-8 · fork repin ff4764d3 → c1d6c9de (consume F-19/F-20/F-21/F-22; delete RePark's legacy-delete walk)

**Date:** 2026-09-03 · **Branch:** `feat/rp-8-repin-f21-f22` · **Base:** `origin/main`
`5285a32` · **Model:** claude-opus-5 (medium) · **Policy:**
[../../../AGENTS.md](../../../AGENTS.md) "Version-pin contract" · **Handoff:**
[../../roadmap/mid-term/iceberg-rust-handoff-2026-08-23.md](../../roadmap/mid-term/iceberg-rust-handoff-2026-08-23.md)
asks F-20, F-21, F-22 · **Path:** STANDARD (`risk_tier: standard`; one Actor cycle).
**Proven pattern:**
[../archive/2026-09/2026-09-02-rp-7-f18-repin-ledger.md](../archive/2026-09/2026-09-02-rp-7-f18-repin-ledger.md).

**Retires:** this ledger moves to `../completed/` in this unit's last commit.

**Why now.** V3-12 built the legacy-delete merge engine-side because the pinned fork could not
express it, and filed two dated DECLARED refusals (`V3-UPGRADE-DV-PLAIN-1`,
`V3-UPGRADE-DV-PART-1`) plus a duplicate manifest pass as fork asks F-21 and F-22. V3-11 filed
`F-v3-10-partition-file-order` as fork ask F-20. All four landed on fork `main` at
`c1d6c9de1498cf04765893ef3f698d915766a6a7` — `#261` (F-19/F-20), `#262` (F-21), `#263` (F-22).
Family stays frozen: datafusion 54.1.0, datafusion-spark 54.1.0, arrow*/parquet 58.4.0,
rust-toolchain 1.96.0.

**Not in this unit:** any dependency change beyond the one `[patch.crates-io]` rev; a
Spark-visible design choice not measured (HALT).

**Base moved mid-unit.** This unit started at `5285a32`, where `V3-COV-3` and
`docs/design/v3-statement-coverage.md` did not exist — V3-COV was unmerged. It merged as `#321`
(`9cef991`) while the unit was running, and the branch was rebased onto it, so C-007 is measured
rather than deferred. §11 records the reading taken while it was still blocked.

## ERRATA (2026-09-03, orchestrator, after the critic and the Rust perf review) — wins over §1–§11

| # | Correction |
|---|---|
| E-1 | Base: the branch's actual base is `9cef991` (V3-COV #321), not `5285a32`; the bare-repin evidence (C-001) was re-taken by the critic on the `9cef991` tree with the new `Cargo.toml`/`Cargo.lock`: `cargo check -p repark-iceberg` exit 101, five errors all in `dv_close.rs` (E0609 `retained_references`, E0061 five arguments, 3× E0308 `Vec<DataFile>`) — three public breaks, nothing else |
| E-2 | C-002's evidence cell says `repark-iceberg --lib` 376 passed (RP-7's number); this tree measures 381 passed, 0 failed |
| E-3 | Mutation evidence (critic, on a snapshot of the tip): M1 re-add an engine-side removal (`close.removed.extend(close.removed.clone())`) → 10 red (2 `repark-iceberg --lib` close pins, 8 of 12 `v3_legacy_delete` cells); M2 duplicate every statement position → 0 red of 1503 because a DV's `record_count` is the distinct-position cardinality (the fork's own writer pin). The §6 consumption note therefore over-claims: only the removal half is observable; the position half of a double-merge is a no-op |
| E-4 | §8 "no measurable change" does not refute a regression: `measure_legacy_walk_cost` seeds one data manifest per delete manifest, so the deleted delete-manifest walk and F-22's new unconditional data-manifest walk cancel to the byte (`strace`: 66 non-snapshot avro opens on both trees at 8 + 8). On a pure-DV fixture (N data manifests, 0 legacy deletes) the statement wall at 192 data manifests is 1.646 s → 2.286 s (+39 %, ~3.3 ms per data manifest), 48: +9.9 %, 8: noise; each data manifest is opened three times per statement where two sufficed. Filed as registry queue entry PERF-DVCLOSE-WALK-1 with fork TRIGGER F-23 and an RP-9 repin. The cell itself is unrepeatable as a wall-clock (run-to-run spread ~1.7 s) and should assert a manifest-open count |
| E-5 | The partitioned `INSERT` paths are unchanged by F-20's ascending drain: 1e6 rows / 8 partitions 53.550 s vs 53.567 s; 1e5 rows / 5k partitions 1519.056 s vs 1519.096 s (both trees, concurrent, debug) |
| E-6 | `V3-FILEORDER-1`'s widened text narrowed: no second data-file ordering rule; the fork's `write_dv_blobs` still drains `HashMap` keys for blob order inside one Puffin |

## PROPOSITION LEDGER — RP-8 — 2026-09-03

| Clause | Proposition (checkable) | Proof obligation | Verdict | Evidence / open question |
|---|---|---|---|---|
| C-001 | Every `iceberg*` `[patch.crates-io]` rev is `c1d6c9de1498cf04765893ef3f698d915766a6a7` and `Cargo.lock` resolves to it; `datafusion`, `datafusion-spark`, `arrow*`, `parquet` and `rust-toolchain.toml` are byte-identical to `main`; the pin-history row names the consumed fork PRs; the handoff's F-21/F-22 rows read LANDED with dates; and what the BARE repin breaks is tabled, not discovered later. | `make bump-fork-pin`; `grep` the five revs; `make verify` on the untouched source; §6 table. | **PROVEN** | Five revs and six lock sources are `c1d6c9de`. `make verify` at the new pin BEFORE any RePark change: **exit 2** — F-19 deleted `DvContainerClose::retained_references` and collapsed `StampedDeleteFile` to `DataFile`, five compile errors in `dv_close.rs` and nowhere else in the tree. Family freeze holds; `rust-toolchain.toml` untouched. Citation: `docs/fork-sync.md`. |
| C-002 | RePark's own legacy-delete walk is DELETED — both manifest walks and its parquet decode — and `plan_deletion_vectors` passes STATEMENT-ONLY positions plus the already-loaded `ManifestList` into `close_touched_dv_containers_with_partitions`, consuming the merge and the file-scoped removal the close now performs. The merge semantics stay the measured ones. F-18's zero-data-manifest claim is reversed and its two pins say so. | The deleted file; the new call shape; the two flipped pins; `repark-iceberg --lib`; the before/after walk table. | **PROVEN** | `crates/repark-iceberg/src/write/merge/dv_close/legacy_deletes.rs` (**493 lines**) and its `map.md` are gone; `dv_close.rs` is 34 lines shorter and calls the close once with `Some(&manifest_list)`. `closing_a_covered_v3_delete_reads_the_data_manifest_for_sequence_numbers` and `a_supplied_partition_map_still_walks_the_data_manifests_for_sequence_numbers` now require the hidden-manifest arm to REFUSE and assert `data_sequence_numbers` carries the touched path. `repark-iceberg --lib` 376 passed. Walk cost §8. Citation: `crates/repark-iceberg/src/write/merge/map.md`. |
| C-003 | The two refusals lift at Spark's measured values on all three doors: the plain-`WHERE` DELETE and UPDATE arms merge (§6 cell A2), and a delete covering two data files merges and stays LIVE (§12 P2/P4). Registry rows → FIXED with the date; the north star and STATUS say so. | Nine door pins (Spark SQL, ANSI, facade) plus two live Spark comparisons; the four documents. | **PROVEN** | `v3_legacy_delete.rs` 12 passed (the two refusal cells became three merge cells); `v3/create.rs` ANSI twins 57 passed; facade `test_v3_legacy_delete_merge.py` 6 passed with the live twins co-collected. Values in §7. Registry: `V3-UPGRADE-DV-PLAIN-1` and `V3-UPGRADE-DV-PART-1` FIXED (RP-8, 2026-09-03). Citation: `docs/spark-sql-iceberg-parity.md`. |
| C-004 | `F-v3-10-partition-file-order` closes: the delegated partitioned plain `INSERT INTO` takes Spark's exact `1→2 2→3 3→4 4→0 5→1` map, deterministically, and the V3-11 pin asserts the map instead of the sets. `V3-FILEORDER-1` stays DECLARED and widens to cover the fork's `INSERT INTO` path. | The flipped pin run twelve times; the registry and north-star text; the meta-pin. | **PROVEN** | `partitioned_table_upgrade_and_append_match_spark` green **12 of 12** consecutive runs (§9) where V3-11 measured the two halves flapping 5/10 and 4/10 independently. `F-v3-10-partition-file-order` FIXED; `V3-FILEORDER-1` DECLARED, unchanged in kind. `test_live_v3_docs.py::test_the_partition_file_order_residual_is_closed_by_the_fork_drain` re-aimed. Citation: `crates/repark-iceberg/src/write/map.md`. |
| C-005 | `V3-DV-BRANCH-1`'s two branch cells stay green at the new pin — the close still receives the snapshot the statement SCANNED, not the current one. | The two cells, unchanged. | **PROVEN** | `a_second_merge_on_read_delete_on_a_diverged_branch_merges_the_branch_only_dv` and `a_legacy_parquet_delete_that_exists_only_on_a_branch_merges_on_that_branch` both green with no source change: `snapshot_id` still reaches the close, and now also reaches the legacy collect inside it. Citation: `crates/repark-spark/src/tests/map.md`. |
| C-007 | `V3-COV-3` closes: the delegated partitioned plain `INSERT INTO` `_row_id` mapping is Spark's in 12 of 12 runs at the new pin, the coverage matrix's nine partitioned rows pin `_row_id` again on BOTH engine goldens, and the verdict totals do not move. | Twelve runs of the V3-COV cell; the re-measured goldens; the live matrix; the docs meta-pin. | **PROVEN** | 12 of 12 gave `{1:0, 2:1, 3:2, 4:3}` (§13), where V3-COV measured 7 of 12 that way and 5 of 12 `{1:2, 2:3, 3:0, 4:1}`. `_P_SEQ` is deleted and every partitioned program's lineage probe is `_P_LINEAGE` again; nine rows re-measured on repark and live on Spark, and `_verdict` over the new halves is unchanged — **71 EQUAL / 1 REFUSED / 9 DIVERGES**, so §1's totals and the 267 cells stand. `V3-COV-3` FIXED (RP-8, 2026-09-03); `test_v3_partitioned_insert_row_id_mapping_is_one_of_two_measured_orders` became `…_is_stable_and_spark_ordered`. Citation: `python/repark/tests/map.md`. |
| C-006 | The record says what landed: registry, north star §3 and §3.1, STATUS, the v3 track, the fork-sync pin row, the handoff F-21/F-22 rows, and every touched `map.md` in lockstep — with the `c1d6c9de` pin in the one place the V1-GATE meta-pin reads it. | `make check-map-sync check-ledger-grammar check-ledgers check-docs-compaction check-manifest`; `ledger_lifecycle.py check`; `make py-test`. | **PROVEN** | §10 gate table. `test_v1_gate_docs.py` re-aimed at `c1d6c9de` (the north star's fork-side heading and `Cargo.toml`), 14 passed. STATUS compacted back under its 25,000 B ceiling in the same edit. Citation: `docs/design/format-v3-track.md`. |

VERDICT: 7 clauses, 7 PROVEN, 0 OPEN, 0 REJECTED.

```yaml
COVERAGE_ATTESTATION:
  pr_unit: rp-8-repin-f21-f22
  categories:
    - id: AT-1
      status: ATTACKED
      evidence: Every lifted refusal is pinned at Spark's measured value on all three doors, and the two facade cells gained live Spark comparisons that run the same statements on both engines and compare one shape dict.
      artifacts: [crates/repark-spark/src/tests/v3_legacy_delete.rs, crates/repark-sql/src/v3/create.rs, python/repark/tests/test_v3_legacy_delete_merge.py]
    - id: AT-2
      status: ATTACKED
      evidence: Plain-WHERE DELETE and plain-WHERE UPDATE; a partition-scoped delete over the touched file and then over the other one; the data manifests hidden and required to refuse; twelve consecutive runs of each file-order cell, the V3-COV one on a fresh warehouse each time.
      artifacts: [crates/repark-spark/src/tests/v3_legacy_delete.rs, crates/repark-iceberg/src/write/merge/dv_close.rs, crates/repark-spark/src/tests/v3_upgrade.rs]
    - id: AT-3
      status: ATTACKED
      evidence: The hidden-data-manifest arm of both flipped pins asserts a hard refusal rather than a silent fallback, which is the direction F-22's always-on walk made reachable.
      artifacts: [crates/repark-iceberg/src/write/merge/dv_close.rs]
    - id: AT-4
      status: N/A
      justification: This unit deletes concurrency rather than adding it; the remaining buffer_unordered lives fork-side inside the close, in the commit's own future, holding no lock.
    - id: AT-5
      status: ATTACKED
      evidence: No AWS, IAM or secret handling. No .github change. The one dependency change is the single [patch.crates-io] rev and its lock rows.
      artifacts: [Cargo.toml, Cargo.lock]
    - id: AT-6
      status: ATTACKED
      evidence: Three public API breaks absorbed - retained_references deleted, StampedDeleteFile collapsed to DataFile, and close_touched_dv_containers_with_partitions gaining Option<&ManifestList>. referenced_data_files() is the replacement blobs only and feeds the same validate_data_files_exist set, pinned at 1 reference where F-18 pinned 2.
      artifacts: [crates/repark-iceberg/src/write/merge/dv_close.rs]
    - id: AT-7
      status: ATTACKED
      evidence: 493 lines of engine-side manifest walking and parquet decoding deleted. The walk cost is measured before and after on this clone, three runs a side at 8 and 48 delete manifests, and reported as NO measurable change with the reason - the delete-manifest walk RePark stopped making is paid back by F-22's always-on data-manifest walk.
      artifacts: [crates/repark-spark/src/tests/v3_legacy_delete.rs]
    - id: AT-8
      status: ATTACKED
      evidence: Five iceberg* revs and six lock sources are c1d6c9de. Family freeze holds; rust-toolchain.toml untouched. make verify at the bare repin recorded at exit 2 with its five errors named.
      artifacts: [Cargo.toml, Cargo.lock, docs/fork-sync.md]
    - id: AT-9
      status: ATTACKED
      evidence: Four registry rows flip to FIXED with the date, V3-FILEORDER-1 stays DECLARED and widens, and the three meta-pins that read those rows are re-aimed so a silent regrowth of the old claim reds.
      artifacts: [docs/spark-sql-iceberg-parity.md, python/repark-parity/tests/test_live_v3_docs.py, python/repark-parity/tests/test_v1_gate_docs.py, python/repark-parity/tests/test_v3_cov_docs.py]
    - id: AT-10
      status: ATTACKED
      evidence: Seven clauses pinned; maps in lockstep; the walk table and both twelve-run determinism tables are measured on this clone, not predicted; the V3-COV goldens are re-measured on both engines rather than hand-edited, and the verdict totals are recomputed rather than restated.
      artifacts: [crates/repark-iceberg/src/write/merge/map.md, crates/repark-spark/src/tests/map.md, python/repark/tests/map.md, python/repark/tests/_v3_statement_coverage_spark.py]
  complete: true
```

## 6. What changed under us (C-001)

Range `ff4764d3eba037ecfa185be5de5f639cbffef80b..c1d6c9de1498cf04765893ef3f698d915766a6a7`.

| Fork change | PR | Change | Engine site that absorbs it |
|---|---|---|---|
| `delete_vector_container.rs` | `#261` F-19 | `retained_references`, `DvDropPlan`, `rewrite_siblings_for_dropped_references`, `collect_live_dvs` deleted; `StampedDeleteFile` collapsed to `DataFile` | `dv_close.rs::referenced_data_files` deleted; `apply_close` is one `add_deletes`; the covered-close pin drops from 2 references to 1 |
| `writer/partitioning/fanout_writer.rs` | `#261` F-20 | `close` sorts partition keys with `ascending_partition_order` | C-004 — the delegated `INSERT INTO` pin flips from sets to Spark's map |
| `transaction/row_delta_fresh_dv.rs` | `#262` F-21 | the commit door blocks only file-scoped position deletes | C-003 — `V3-UPGRADE-DV-PART-1` commits |
| `integrations/datafusion/physical_plan/delete_legacy_merge.rs` (new) | `#262` F-21 | the delete exec merges legacy positions instead of refusing pre-IO | C-003 — `V3-UPGRADE-DV-PLAIN-1` merges |
| `delete_vector_container.rs` + `delete_vector_container/legacy.rs` | `#263` F-22 | one delete-manifest pass returns `legacy_deletes` + `data_sequence_numbers`; `load_legacy_positions_by_path` is one projected read per delete file; the close takes `Option<&ManifestList>`; the close merges and removes file-scoped sources itself | C-002 — RePark's `legacy_deletes.rs` is deleted and the caller passes statement-only positions |
| `delete_vector_container.rs` (`collect_live_data_files`) | `#263` F-22 | ALWAYS walks every data manifest so `data_sequence_numbers` fills, with no opt-out | C-002 — F-18's two zero-data-manifest pins flip to sequence-number pins |
| `arrow/delete_file_loader.rs` | `#263` F-22 | F-21's `load_position_deletes_by_path` removed | not on RePark's path — the engine no longer reads delete files at all |

Public API breaks absorbed: **three** — `DvContainerClose::retained_references` deleted,
`DvContainerClose::added` is `Vec<DataFile>` not `Vec<StampedDeleteFile>`, and
`close_touched_dv_containers_with_partitions` takes a fifth `Option<&ManifestList>` parameter.

**Consumption note (C-002).** The F-22 handoff §9 carries two rows that read as alternatives:
"consume `DvContainerClose::legacy_deletes` + `load_legacy_positions_by_path`" and "pass
statement-only positions into close (close merges)". The fork's code settles it — the close builds
the position overlay and pushes the file-scoped sources onto `close.removed` itself, so a caller
that ALSO merged would double-extend the position vector and offer the same `DataFile` twice to
`remove_deletes_many`. RePark therefore consumes the close's result and does not call
`load_legacy_positions_by_path` at all; `legacy_deletes` and `data_sequence_numbers` are read as
reporting surface, pinned through `data_sequence_numbers` in the two flipped close pins.

## 7. The lifted refusals, at Spark's measured values (C-003)

Spark's readings are the V3-12 live transcript (PySpark 4.1.2 + Iceberg 1.11.0,
`JAVA_HOME=/usr/lib/jvm/zulu-17-amd64`, Hadoop catalog, ANSI on, UTC): §6 cell A2 and §12 P1–P4.

| Registry row | Cell | Spark's measured value | repark at `c1d6c9de` |
|---|---|---|---|
| `V3-UPGRADE-DV-PLAIN-1` | `DELETE … WHERE id = 3` over the A2 layout | ONE Puffin `record_count` 2 referencing the data file, parquet delete GONE, `next-row-id` 4, rows `(1,'a'),(4,'d')`, lineage `(1,0,1),(4,3,1)` | identical, all three doors |
| `V3-UPGRADE-DV-PLAIN-1` | `UPDATE … SET name = 'Z' WHERE id = 3` over the same layout | merges the legacy positions too (§6 cell E, same rule) | ONE Puffin `record_count` 2, parquet GONE, rows `(1,'a'),(3,'Z'),(4,'d')` |
| `V3-UPGRADE-DV-PART-1` | P2 — `MERGE … DELETE` id 2 over a partition-granularity delete covering two data files | COMMITS: `[PARQUET rc 2 still live, PUFFIN rc 2 referencing the touched file]`, `next-row-id` 4, rows `[(4,'d',7)]` | identical |
| `V3-UPGRADE-DV-PART-1` | P3/P4 — append `(9,'z')`, then `MERGE … DELETE` id 4 on the OTHER data file | a second PUFFIN `rc` 2; the PARQUET delete is STILL live; rows `[(9,'z',7)]` | identical |

The removal rule is unchanged and still the load-bearing half: only a delete covering exactly one
data file is dropped, because removing one that covers more resurrects the rows it deletes in the
files this commit did not touch.

## 8. The walk, before and after (C-002)

`measure_legacy_walk_cost`, this clone, debug, three runs a side, same fixture: one delete
manifest per commit at `commit.manifest-merge.enabled = false`, then one more MoR DELETE that
finds ZERO legacy candidates, so the read is isolated from the walk. "Before" is the base tree at
`ff4764d3` (RePark's own walk); "after" is this tree at `c1d6c9de`.

| Delete manifests | Before (RePark's walk + the fork's) | After (the fork's close alone) |
|---|---|---|
| 8 | 336.9 / 345.6 / 351.4 ms | 328.7 / 331.8 / 315.2 ms |
| 48 | 1.522 / 1.489 / 1.459 s | 1.459 / 1.479 / 1.451 s |

**No measurable change**, and the reason is measured rather than excused: RePark stopped making a
delete-manifest walk, and F-22 started making a data-manifest walk unconditionally (its own F-18
reversal). On this fixture the two cancel. The win F-22 claims — 1,417 ms → 723 ms at 192
manifests — is fork-side and against the fork's own duplicate pass, not this one. What RePark
gains here is 493 lines of ported scoping and decoding logic it no longer owns.

`measure_projected_decode` left the tree with `legacy_deletes.rs`: the fork owns that read now
and pins it (`load_legacy_positions_projects_past_the_row_column`).

## 9. The file-order cell, twelve runs (C-004)

`cargo test -p repark-spark --locked --lib partitioned_table_upgrade_and_append_match_spark`,
run twelve times consecutively at `c1d6c9de`.

| Runs | Result |
|---|---|
| 12 | `1 passed` — Spark's exact map `1→2, 2→3, 3→4, 4→0, 5→1` every time |

V3-11 measured the same cell before F-20: the seed manifest read Spark's map 5 times of 10 and the
append manifest 4 of 10, moving independently, both Spark's on 3 of 10. The pin now asserts the
map; it asserted only the id sets while the residual was open.

## 10. Gates

| Gate | Exit |
|---|---|
| `make verify` at the bare repin, before any RePark change (C-001 evidence) | 2 |
| `make verify` | 0 |
| `make preflight` | 0 |
| `make py-test` | 0 |
| `make check-map-sync check-ledger-grammar check-ledgers check-docs-compaction check-manifest` | 0 |
| `python3 scripts/ledger_lifecycle.py check --base origin/main` | 0 |
| `cargo test -p repark-iceberg -p repark-spark -p repark-sql --locked` | 0 |
| live: `test_v3_legacy_delete_merge.py` co-collected with `test_parity_live.py::test_live_disclosure_still_diverges` (20 passed) | 0 |
| live: `test_v3_statement_coverage.py` co-collected the same way (178 passed, 1 min 44 s) | 0 |
| live: `test_v3_dv_container_close.py`, `test_v3_live_oracle.py`, `test_v3_upgrade.py` co-collected the same way (29 passed) | 0 |

The venv must be re-synced (`uv sync --locked --extra record …`) immediately before a live run:
`make py-test` / `make preflight` re-sync without the `record` extra and uninstall `pyspark`, so a
live collection straight after them fails on `ModuleNotFoundError: No module named 'pyspark'`
rather than on anything the diff did.

## 11. C-005a while it was blocked (recorded, not claimed)

At the unit's original base `5285a32` the brief's C-005a was not actionable: `V3-COV-3` and
`docs/design/v3-statement-coverage.md` did not exist, and V3-COV lived unmerged on
`origin/feat/v3-cov-statement-coverage`. The reading taken then was BLOCKED with the measurement
banked — §9's twelve-run table on the equivalent delegated cell — rather than a flip invented
against a row that was not in the tree. V3-COV merged as `#321` shortly after; the branch was
rebased onto `9cef991` and C-007 measures the real cell.

## 12. Residuals after this unit

| Registry row | State |
|---|---|
| `V3-FILEORDER-1` | DECLARED, widened — the fork's `INSERT INTO` path now follows the same ascending rule, so the divergence from Spark's `HashMap` bucket order is one rule on every writer instead of two |
| `V3-UPGRADE-V4-1` | DECLARED, untouched |
| `V3-COV-4` / `V3-COV-5` / `V3-COV-7` / `V3-COV-8` | BACKLOG, untouched — none is fork-routed |
| `V3-COV-6` | DECLARED, untouched — its TRIGGER is a fork metadata-table scan, not this repin |

## 13. The V3-COV-3 cell, twelve runs (C-007)

`_partitioned_row_id_mapping` from `python/repark/tests/test_v3_statement_coverage.py`, run twelve
times consecutively at `c1d6c9de`, each on its own fresh warehouse.

| Runs | Mapping |
|---|---|
| 12 | `{1:0, 2:1, 3:2, 4:3}` — Spark's |
| 0 | `{1:2, 2:3, 3:0, 4:1}` — the permutation V3-COV measured 5 times in 12 |

The nine partitioned matrix rows were then re-measured on both engines with `_P_LINEAGE` restored
(`insert-overwrite-partition-static-values`, `-static-select`, `-dynamic`,
`delete-where-partitioned-mor`, `delete-where-partitioned-cow`, `update-where-partitioned-mor`,
`update-partition-key-cow`, `merge-partitioned-mor`, `alter-add-column-partitioned`). Every one
agrees with live Spark on the `_row_id` column, so no verdict moves: 71 EQUAL, 1 REFUSED,
9 DIVERGES, 267 cells — §1's totals are unchanged and were not edited.
