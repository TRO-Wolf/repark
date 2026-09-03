# Charter ledger — RP-7 · fork repin fb0cacfa → ff4764d3 (consume F-18; close `V3-DV-1`)

**Date:** 2026-09-02 · **Branch:** `feat/rp-7-f18-repin` · **Base:** `origin/main`
`6e89fecd` · **Model:** claude-opus-5 (medium) · **Policy:**
[../../../AGENTS.md](../../../../AGENTS.md) "Version-pin contract" · **Handoff:**
[../../roadmap/mid-term/iceberg-rust-handoff-2026-08-23.md](../../../roadmap/mid-term/iceberg-rust-handoff-2026-08-23.md)
ask F-18 · **Path:** STANDARD (`risk_tier: standard`; one Actor cycle).
**Proven pattern:**
[../archive/2026-09/2026-09-02-rp-6-fork-repin-ledger.md](2026-09-02-rp-6-fork-repin-ledger.md).

**Retires:** this ledger moves to `../completed/` in this unit's last commit.

**Why now.** V3-9 measured the one v3 merge-on-read divergence left — closing a shared Puffin
rewrote every sibling blob — and filed it as registry `V3-DV-1` BACKLOG owned by fork ask F-18.
Fork PR `#260` landed F-18 at `ff4764d3eba037ecfa185be5de5f639cbffef80b`: removal keyed by Java's
`DeleteFileSet` triple, only the touched blob rewritten, a lazy data-file walk behind
`close_touched_dv_containers_with_partitions`. Family stays frozen: datafusion 54.1.0,
datafusion-spark 54.1.0, arrow*/parquet 58.4.0, rust-toolchain 1.96.0.

**Not in this unit:** F-19 (retire the F-17 DELETE-side skip-delete broadening; drop the
maintenance sibling copy) — the fork ruled both into F-19; any dependency change beyond the one
`[patch.crates-io]` rev; WAP; a Spark-visible design choice not measured (HALT).

## PROPOSITION LEDGER — RP-7 — 2026-09-02

| Clause | Proposition (checkable) | Proof obligation | Verdict | Evidence / open question |
|---|---|---|---|---|
| C-001 | Every `iceberg*` `[patch.crates-io]` rev is `ff4764d3eba037ecfa185be5de5f639cbffef80b` and `Cargo.lock` resolves to it; `datafusion`, `datafusion-spark`, `arrow*`, `parquet` and `rust-toolchain.toml` are byte-identical to `main`; the pin-history row names the consumed fork PR; every pin that reds on the BARE repin is tabled and explained by F-18. | `make bump-fork-pin`; `grep` the five revs; `cargo test --locked --workspace --no-fail-fast` with the source untouched; §6 table. | **PROVEN** | Five revs and six lock sources are `ff4764d3`. Bare repin: **1 red of 46 suites** — `dv_close::tests::shared_puffin_row_delta_keeps_the_untouched_sibling` on "old container must not stay live", exactly the F-18 sibling-retention row; `repark-spark --lib` 761 passed with the v3e4 cell GREEN because V3-9 narrowed it to the shared invariant. Citation: `docs/fork-sync.md`. |
| C-002 | RePark consumes the new API: the v3 close routes through `close_touched_dv_containers_with_partitions` with the `(spec_id, partition)` the statement's OWN target scan already planned — partitioned and unpartitioned alike, and only for paths that scan produced — `retained_references` reach the same `validate_data_files_exist` set, and the manifest-read budget is pinned for a FRESH PARTITIONED delete. | A pin that the scan records every planned file's partition; a pin that HIDES the live data manifests and requires a fresh partitioned close to succeed; mutations. | **PROVEN** | `partition_sink.rs::the_target_scan_records_every_planned_file_partition` equals the manifest truth on a 3-partition table (mutation M4, no-op the recorder: **1 red of 1**). `a_supplied_partition_map_closes_a_fresh_partitioned_delete_with_no_data_manifest` green with every data manifest renamed away (mutation M3, clear the map: **1 red of 3**). `closing_a_covered_v3_delete_reads_no_data_manifest` is kept but is FORK behaviour — a covered path resolves from the delete manifests with no map — and is NOT load-bearing for this clause. Measurement §10. Citation: `crates/repark-iceberg/src/write/merge/map.md`. |
| C-003 | The shared-Puffin cells assert Spark's layout, re-measured live at the matched layout: two containers after the second `DELETE`, the sibling `DeleteFile` entry unchanged in container and `content_offset`, the touched blob at offset 4 with 2 records, `removed-dvs` / `removed-delete-files` 1, rows identical. Registry `V3-DV-1` → FIXED; `V3-MOR-1` loses the residual; the north-star row → ✅; the STATUS Known-issues line goes; the handoff F-18 bullet → consumed; meta-pins re-aimed. Bytes per later single-row `DELETE` at 16 and 64 blobs measured before and after the repin. | Live oracle transcript (§7); the re-aimed Rust cells; a live cell; the byte table; the four documents; `make py-test`. | **PROVEN** | Live oracle re-measured 2026-09-02 (§7) — repark and Spark agree on the whole shape. `v3e4.rs` cell asserts two containers, the sibling tuple unchanged, offset 4 and the six summary counts; `dv_close.rs` keeps the semantic assertion and gains the layout one; `a_later_single_row_delete_writes_one_blob_not_the_whole_container` holds < 1 KiB at 16 blobs; live cell `test_v3_shared_puffin_container_close_live`. Bytes: 4,830 → 377 B at 16 blobs, 19,126 → 377 B at 64 (§8). Citation: `crates/repark-spark/src/tests/map.md`. |
| C-004 | Everything else stays green at the new pin: every V3 pin, the RDF-1 pins, the MW-7 / MW-8 runbooks, the `v3_lineage.rs` byte tripwire (re-record only if a tripwired file changed), and the live cells. | `make verify`, `make preflight`, `make py-test`, the touched cargo suites, the live cells. | **PROVEN** | §9 gate table. The tripwire files were not touched by this unit, so no hash is re-recorded. Citation: `crates/repark-spark/src/tests/v3_lineage.rs`. |

| C-005 | The identity DML scratch scan carries the subquery's key bounds instead of reading every column of every data file, gated to positive uncorrelated `IN` / positive `EXISTS`; `NOT IN` / `NOT EXISTS` keep the unfiltered scan; and **no residual is derived when ownership is ambiguous** — each side of the correlation is classified by ONE resolution, and a qualifier naming neither owner, or BOTH (a target alias shadowing the subquery relation's alias or bare table name), leaves the scan unfiltered. Every V3-8 / V3-9 pin stays green; the pair collectors stream instead of collecting. | A twelve-cell matrix against the live oracle on the Spark door and the facade; a file-open pin that fails closed when an unadmitted file is opened; two mutations; the V3 suites; measurement. | **PROVEN** | §11 matrix: ten executable cells and two refusals, repark == Spark on every one, live. `v3_dml_scan.rs::subquery_delete_opens_only_the_files_the_key_bounds_admit` hides the seven data files whose lower bound cannot hold the key and the DELETE still succeeds. Mutations M5 (`identity_scan_residual` returns `None`) **1 red of 1**; M6 (independent per-side classification restored) **1 red of 1**, `shadow_exists_alias` left `[1, 6]` where Spark leaves `[]`. `repark-spark --lib` 765 passed, `repark-sql --lib` 336 passed, `repark-iceberg --lib` 376 passed. Measurement §10. Citation: `crates/repark-iceberg/src/write/map.md`. |
| C-006 | The perf review's Python items land: neither live helper creates a per-call Ivy cache, and the DV reads project only the columns they use. | `grep` the helpers; the live cells green. | **PROVEN** | Both `spark.jars.ivy` `mkdtemp` + `rmtree` pairs are gone from `test_v3_live_oracle.py`, so a jar-less nightly runner resolves the Iceberg runtime through the default Ivy cache the other helpers already share instead of re-resolving twice. The four `SELECT *` reads over `.delete_files` are one `_DV_COLUMNS` template projecting `referenced_data_file, file_path, content_offset, record_count`. Live cells 37 passed. Citation: `python/repark/tests/map.md`. |

VERDICT: 6 clauses, 6 PROVEN, 0 OPEN, 0 REJECTED.

```yaml
COVERAGE_ATTESTATION:
  pr_unit: rp-7-f18-repin
  categories:
    - id: AT-1
      status: ATTACKED
      evidence: The shared-Puffin close is pinned at Spark's exact layout on the Spark door and in the writer, plus a live cell that compares the whole shape against a running Spark, plus the twelve-cell subquery-DML matrix live on both engines.
      artifacts: [crates/repark-spark/src/tests/v3e4.rs, crates/repark-iceberg/src/write/merge/dv_close.rs, python/repark/tests/test_v3_dv_container_close.py, crates/repark-spark/src/tests/v3_dml_scan.rs]
    - id: AT-2
      status: ATTACKED
      evidence: Covered path and fresh path, partitioned and unpartitioned, 16 and 64 blobs, positive IN and positive EXISTS against NOT IN / NOT EXISTS, and two hidden-file edges (data manifests, data files) where a stray read fails closed.
      artifacts: [crates/repark-iceberg/src/write/merge/dv_close.rs, crates/repark-spark/src/tests/v3e4.rs, crates/repark-spark/src/tests/v3_dml_scan.rs]
    - id: AT-3
      status: ATTACKED
      evidence: The close error path is unchanged; hiding the data manifests turns a stray walk into a hard failure rather than a silent fallback.
      artifacts: [crates/repark-iceberg/src/write/merge/dv_close.rs]
    - id: AT-4
      status: N/A
      justification: The partition sink is an Arc<Mutex<HashMap>> written only from the scan that owns it and drained once at commit; PartitionStream::execute may run more than once and the insert is idempotent on the path key.
    - id: AT-5
      status: ATTACKED
      evidence: No AWS, IAM or secret handling. No .github change. The one dependency change is the single [patch.crates-io] rev.
      artifacts: [Cargo.toml, Cargo.lock]
    - id: AT-6
      status: ATTACKED
      evidence: DvContainerClose gained retained_references and removed now carries only touched blobs; referenced_data_files() unions both and feeds the same validate_data_files_exist set, pinned at 2 references on the two-file fixture.
      artifacts: [crates/repark-iceberg/src/write/merge/dv_close.rs, crates/repark-iceberg/src/write/merge/snapshot_commit.rs]
    - id: AT-7
      status: ATTACKED
      evidence: the partition map is harvested from a scan RePark already runs and retained down to the touched paths; retained_references is moved, not cloned; the pair collectors stream per batch with a reserve; write amplification 19,126 B to 377 B at 64 blobs; partitioned fresh-path DELETE 2,176 ms to 761 ms at 192 partitions.
      artifacts: [crates/repark-spark/src/tests/v3e4.rs, crates/repark-spark/src/tests/v3_dml_scan.rs, crates/repark-iceberg/src/write/merge/dv_close.rs]
    - id: AT-8
      status: ATTACKED
      evidence: Five iceberg* revs and six lock sources are ff4764d3. Family freeze holds; rust-toolchain.toml untouched.
      artifacts: [Cargo.toml, Cargo.lock, docs/fork-sync.md]
    - id: AT-9
      status: ATTACKED
      evidence: V3-DV-1 FIXED with both readings equal, V3-MOR-1 loses its residual, the north-star MOR row is ✅, the STATUS Known-issues entry is deleted rather than moved, and the meta-pin now fails if that link comes back.
      artifacts: [docs/spark-sql-iceberg-parity.md, STATUS.md, task/roadmap/epic-term/v1-0-iceberg-v3-northstar.md, python/repark-parity/tests/test_v3r_1_rulings.py]
    - id: AT-10
      status: ATTACKED
      evidence: Five clauses pinned; maps in lockstep; mutations M3/M4/M5 each 1 red then restored; the bare-repin red table and both measurement tables are measured, not predicted.
      artifacts: [crates/repark-iceberg/src/write/merge/dv_close.rs, crates/repark-iceberg/src/write/merge/tests/partition_sink.rs, crates/repark-spark/src/tests/v3_dml_scan.rs]
  complete: true
```

## 6. What changed under us (C-001)

Range `fb0cacfa8ceda87f865fb0ae53be4b46e0ef8b7a..ff4764d3eba037ecfa185be5de5f639cbffef80b`.
Compare:
`https://github.com/TRO-Wolf/iceberg-rust/compare/fb0cacfa8ceda87f865fb0ae53be4b46e0ef8b7a...ff4764d3eba037ecfa185be5de5f639cbffef80b`

| Fork change | PR | Change | Engine site that absorbs it |
|---|---|---|---|
| `transaction/snapshot/removal_targets.rs` (new) | `#260` F-18 | DATA entries match by path, DELETE entries by the `DeleteFileSet` triple | compile; `remove_deletes_many` now removes one blob, not a container |
| `transaction/snapshot.rs` | `#260` F-18 | `resolve_removed_delete_files` triple-keyed; `process_deletes` matches each manifest kind on its own key | C-003 layout pins |
| `delete_vector_container.rs` | `#260` F-18 | only touched blobs rewritten; `retained_references`; one manifest-list load; lazy `collect_live_data_files`; `close_touched_dv_containers_with_partitions` | C-002 / C-003 |
| `writer/base_writer/deletion_vector_writer.rs` | `#260` F-18 | previous positions taken as a loaded `DeleteVector`; `file_scope.rs` extraction | compile |
| `integrations/datafusion/physical_plan/delete.rs` | `#260` F-18 | position-delete arm uses `..DvContainerClose::default()` | not on RePark's path (RePark owns its DML) |

Public API break absorbed: `DvContainerClose.removed` now carries only the touched blobs and the
struct gained `retained_references`; `referenced_data_files()` unions both, which is the only
place RePark reads it.

## 7. Oracle transcript (C-002 / C-003)

Live oracle: PySpark 4.1.2 + Iceberg 1.11.0, `JAVA_HOME=/usr/lib/jvm/zulu-17-amd64`, Hadoop
catalog, `local[2]`, ANSI on, UTC. Seed matched on both engines: v3 merge-on-read table
partitioned by `part`, ONE `INSERT` of `(1,a,0),(2,b,0),(3,c,0),(4,d,1),(5,e,1),(6,f,1)` → two
data files; `DELETE … WHERE id IN (2, 5)` → ONE Puffin holding two blobs; then
`DELETE … WHERE id = 1`, which touches the `part = 0` file only.

| Reading | Containers after | Touched file's DV | Untouched sibling's DV | Rows |
|---|---|---|---|---|
| Apache Spark | **2** | new container, offset 4, 2 records | **old** container, **old** offset 46, 1 record, entry untouched | `(3,c,0),(4,d,1),(6,f,1)` |
| repark at `ff4764d3` | **2** | new container, offset 4, 2 records | **old** container, **old** offset, 1 record, entry untouched | `(3,c,0),(4,d,1),(6,f,1)` |
| repark at `fb0cacfa` (V3-9) | 1 | new container, offset 4 | **same new** container, offset 48 | same rows |

Spark's summary for that statement: `removed-delete-files 1`, `removed-dvs 1`,
`removed-position-deletes 1`, `added-delete-files 1`, `added-dvs 1`, `added-position-deletes 2` —
now asserted verbatim by the `v3e4.rs` cell.

Ruling closed (orchestrator, 2026-09-02): the first draft supplied the map only when EVERY spec in
the metadata was unpartitioned, which the perf reviewer measured to be useless — one partitioned
spec anywhere in the history emptied the map and the statement paid the full lazy walk. The
partition was already free: the `FileScanTask`s the target scan plans carry `partition_spec` and
`partition`. `TargetScanStream::with_partition_sink` records them, and the DML and MERGE MoR
commits hand the drained map to the close. Entries are supplied ONLY for paths that scan produced,
so the fork's "data file is not a live file of the scanned snapshot" guard still bites.

## 8. Write amplification (C-003)

`crates/repark-spark/src/tests/v3e4.rs::measure_later_single_row_delete_bytes` (`#[ignore]`d), one
blob per data file, debug profile, same clone, same fixture; only the pin (and `dv_close.rs`)
differ between the columns.

| blobs in the container | pin `fb0cacfa` | pin `ff4764d3` |
|---|---|---|
| 16 | 1 container / 4,830 B rewritten | 2 containers / 377 B |
| 64 | 1 container / 19,126 B rewritten | 2 containers / 377 B |

`a_later_single_row_delete_writes_one_blob_not_the_whole_container` is the non-ignored budget pin
at 16 blobs (two containers, under 1 KiB written).

## 10. Remediation round (2026-09-02) — measurements

Statement wall, `v3_dml_scan.rs::measure_v3_mor_subquery_delete_statement_wall`, debug profile,
same clone, one `DELETE … WHERE id IN (SELECT id FROM keys)` after the seed. `flat` is an
unpartitioned table of N data files each holding 200 rows the `WHERE` discards (C-005's shape);
`partitioned fresh` is an identity-partitioned table of N partitions with no DV yet (C-002's
shape). Before = this unit's own first tree (`34d6bff`, pin already `ff4764d3`), so only the
remediation is in the delta.

| cell | before `34d6bff` | after |
|---|---|---|
| flat, 64 files | 562 ms | 511 ms |
| flat, 192 files | 1,597 ms | 1,381 ms |
| partitioned fresh, 64 partitions | 752 ms | 302 ms |
| partitioned fresh, 192 partitions | 2,176 ms | 761 ms |

The partitioned fresh-path column is C-002: the supplied map removes the fork's data-manifest walk
(−60 % at 64, −65 % at 192). The flat column is C-005: the residual push (−9 % / −14 %). Recorded
honestly — the residual's payoff is bounded in this fixture because each file is one small row
group, so the pre-`WHERE` column read it skips is cheap here; the pin, not the clock, is what holds
it.

Mutations, one knob at a time, restored after each:

| id | knob | result |
|---|---|---|
| M3 | `plan_deletion_vectors` clears the supplied map | 1 red of 3 (`a_supplied_partition_map_closes_a_fresh_partitioned_delete_with_no_data_manifest`) |
| M4 | `record_scanned_partitions` returns immediately | 1 red of 1 (`the_target_scan_records_every_planned_file_partition`) |
| M5 | `identity_scan_residual` returns `None` | 1 red of 1 (`subquery_delete_opens_only_the_files_the_key_bounds_admit`) |

## 11. Round 3 — subquery-DML owner resolution (C-005)

The round-2 critic measured a WRONG ANSWER, not a slow one: a target alias that shadows the
subquery relation. Spark binds the inner name to the subquery's own relation, so
`DELETE FROM t s WHERE EXISTS (SELECT 1 FROM src s WHERE s.id = s.id)` is uncorrelated and true
for every row. The first draft classified the two sides with independent predicates, read it as a
correlation, and pruned. Fix: one resolution per side (`owner_side`), `None` when a qualifier
names neither owner or BOTH, and the whole hint dropped when the target alias also names the
subquery relation's alias or bare table name.

Matrix, live PySpark 4.1.2 + Iceberg 1.11.0 against repark, same seed `(1..6)`, `src = {2, 5}`,
`src2 = {6}`, `srcnull = {2, NULL}`, `srcbig = {2, 5}` BIGINT, `srcempty = {}`, `k = {2, 5}`.
Survivors after the DELETE:

| cell | Spark | repark |
|---|---|---|
| `shadow_exists_alias` (`t s` / `src s`, `s.id = s.id`) | `[]` | `[]` |
| `shadow_exists_bare` (`t k` / table `k`, `k.id = k.id`) | `[]` | `[]` |
| `filtered_in` (`IN (SELECT id FROM src WHERE id > 4)`) | `1,2,3,4,6` | same |
| `empty_source` | `1,2,3,4,5,6` | same |
| `null_source_keys` | `1,3,4,5,6` | same |
| `projection_alias` (`SELECT id AS key`) | `1,3,4,6` | same |
| `int_vs_bigint` (source BIGINT, target INT) | `1,3,4,6` | same |
| `correlated_exists` (`s.id = t.id`) | `1,3,4,6` | same |
| `plus_one` (`t.id = s.id + 1`) | `1,2,4,5` | same |
| `and_filter` (`s.id = t.id AND s.id > 1`) | `1,3,4,6` | same |
| `distinct` (`SELECT DISTINCT id`) | `1,3,4,6` | REFUSED (G3-E8 allow-list), table untouched |
| `union` (`SELECT … UNION SELECT …`) | `1,3,4` | REFUSED (G3-E8 allow-list), table untouched |

The two refusals pre-date this unit and are the subquery allow-list, not the residual: a refusal
is not a wrong answer, and the pin asserts the table is left at the seed. Pins:
`crates/repark-spark/src/tests/v3_dml_scan.rs::subquery_dml_matrix_matches_spark_with_the_residual_pushed`
(Spark door, the ten executable cells plus the two refusals) and
`python/repark/tests/test_v3_dv_container_close.py::test_v3_subquery_dml_matrix_matches_spark`
(facade JVM-free, then repark == live Spark under `REPARK_PARITY_LIVE=1`).

Mutation M6: restore the independent per-side classification. `shadow_exists_alias` leaves
`[1, 6]` against Spark's `[]` — **1 red of 1**. Restored.

`crates/repark-iceberg/src/write/merge/tests/occ_partitions.rs::the_production_partition_carrying_commit_honors_the_snapshot_pin`
runs the production `commit_row_delta_kind_with_partitions` variant with a real partition map on a
partitioned v3 table: the first commit lands, and a stale pin is still rejected with the table
unmoved. The OCC batteries in `occ.rs` / `occ_conflict.rs` keep their existing spellings and
exercise the empty-map wrappers, which are now `#[cfg(test)]`.

## 9. Gate exits

| gate | exit |
|---|---|
| `make verify` | 0 |
| `scripts/check_rust_file_size.py` (baselines ratcheted DOWN: `write/merge/mod.rs` 1889 → 1795 behind `merge/target_scan.rs`; `write/predicate_dml.rs` 1164 → 1142 behind `predicate_dml/residual.rs`, mirrored in `test_cap_1_source_file_line_cap.py`) | 0 |
| `make preflight` | 0 |
| `make py-test` | 0 |
| `make check-map-sync check-ledger-grammar check-ledgers check-docs-compaction` | 0 |
| `python3 scripts/ledger_lifecycle.py check --base origin/main` | 0 |
| `cargo test --locked -p repark-iceberg --lib` | 0 |
| `cargo test --locked -p repark-spark --lib` | 0 |
| `.venv/bin/python -m pytest python/repark/tests/test_v3_live_oracle.py` (`REPARK_PARITY_LIVE=1`) | 0 |
