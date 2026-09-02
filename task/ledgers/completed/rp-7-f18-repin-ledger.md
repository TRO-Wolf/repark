# Charter ledger — RP-7 · fork repin fb0cacfa → ff4764d3 (consume F-18; close `V3-DV-1`)

**Date:** 2026-09-02 · **Branch:** `feat/rp-7-f18-repin` · **Base:** `origin/main`
`6e89fecd` · **Model:** claude-opus-5 (medium) · **Policy:**
[../../../AGENTS.md](../../../../AGENTS.md) "Version-pin contract" · **Handoff:**
[../../roadmap/mid-term/iceberg-rust-handoff-2026-08-23.md](../../../roadmap/mid-term/iceberg-rust-handoff-2026-08-23.md)
ask F-18 · **Path:** STANDARD (`risk_tier: standard`; one Actor cycle).
**Proven pattern:**
[../archive/2026-09/2026-09-02-rp-6-fork-repin-ledger.md](../archive/2026-09/2026-09-02-rp-6-fork-repin-ledger.md).

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
| C-002 | RePark consumes the new API: the v3 close routes through `close_touched_dv_containers_with_partitions`, `retained_references` reach the same `validate_data_files_exist` set through `referenced_data_files()`, and the manifest-read budget is pinned — a v3 MoR `DELETE` closes with ZERO data-manifest reads both when the touched files already carry DVs and when RePark supplies the partition map. | Two pins that HIDE the live data manifests and require the close to succeed; one mutation. | **PROVEN** | `closing_a_covered_v3_delete_reads_no_data_manifest` and `a_first_v3_delete_on_an_unpartitioned_table_reads_no_data_manifest` green with every data manifest renamed away. Mutation M1 (supply an empty map): **1 red of 3**, `No such file or directory … -m0.avro`. `free_partitions` supplies `(default_spec_id, Struct::empty())` only when EVERY spec in the metadata is unpartitioned. Open: a fresh path in a PARTITIONED table still uses the fork's lazy walk — RePark has no cheaper source (§7). Citation: `crates/repark-iceberg/src/write/merge/map.md`. |
| C-003 | The shared-Puffin cells assert Spark's layout, re-measured live at the matched layout: two containers after the second `DELETE`, the sibling `DeleteFile` entry unchanged in container and `content_offset`, the touched blob at offset 4 with 2 records, `removed-dvs` / `removed-delete-files` 1, rows identical. Registry `V3-DV-1` → FIXED; `V3-MOR-1` loses the residual; the north-star row → ✅; the STATUS Known-issues line goes; the handoff F-18 bullet → consumed; meta-pins re-aimed. Bytes per later single-row `DELETE` at 16 and 64 blobs measured before and after the repin. | Live oracle transcript (§7); the re-aimed Rust cells; a live cell; the byte table; the four documents; `make py-test`. | **PROVEN** | Live oracle re-measured 2026-09-02 (§7) — repark and Spark agree on the whole shape. `v3e4.rs` cell asserts two containers, the sibling tuple unchanged, offset 4 and the six summary counts; `dv_close.rs` keeps the semantic assertion and gains the layout one; `a_later_single_row_delete_writes_one_blob_not_the_whole_container` holds < 1 KiB at 16 blobs; live cell `test_v3_shared_puffin_container_close_live`. Bytes: 4,830 → 377 B at 16 blobs, 19,126 → 377 B at 64 (§8). Citation: `crates/repark-spark/src/tests/map.md`. |
| C-004 | Everything else stays green at the new pin: every V3 pin, the RDF-1 pins, the MW-7 / MW-8 runbooks, the `v3_lineage.rs` byte tripwire (re-record only if a tripwired file changed), and the live cells. | `make verify`, `make preflight`, `make py-test`, the touched cargo suites, the live cells. | **PROVEN** | §9 gate table. The tripwire files were not touched by this unit, so no hash is re-recorded. Citation: `crates/repark-spark/src/tests/v3_lineage.rs`. |

VERDICT: 4 clauses, 4 PROVEN, 0 OPEN, 0 REJECTED.

```yaml
COVERAGE_ATTESTATION:
  pr_unit: rp-7-f18-repin
  categories:
    - id: AT-1
      status: ATTACKED
      evidence: The shared-Puffin close is pinned at Spark's exact layout on the Spark door and in the writer, plus a live cell that compares the whole shape against a running Spark.
      artifacts: [crates/repark-spark/src/tests/v3e4.rs, crates/repark-iceberg/src/write/merge/dv_close.rs, python/repark/tests/test_v3_live_oracle.py]
    - id: AT-2
      status: ATTACKED
      evidence: Covered path and fresh path, partitioned and unpartitioned, 16 and 64 blobs, and the manifests-hidden edge where a data-manifest read would fail closed.
      artifacts: [crates/repark-iceberg/src/write/merge/dv_close.rs, crates/repark-spark/src/tests/v3e4.rs]
    - id: AT-3
      status: ATTACKED
      evidence: The close error path is unchanged; hiding the data manifests turns a stray walk into a hard failure rather than a silent fallback.
      artifacts: [crates/repark-iceberg/src/write/merge/dv_close.rs]
    - id: AT-4
      status: N/A
      justification: No new shared mutable engine state. free_partitions is a pure read of table metadata.
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
      evidence: free_partitions allocates one map entry per touched path and no manifest is read; the write amplification it rides on fell from 19,126 B to 377 B per later single-row DELETE at 64 blobs.
      artifacts: [crates/repark-spark/src/tests/v3e4.rs]
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
      evidence: Four clauses pinned; maps in lockstep; mutation M1 1 red of 3 then restored; the bare-repin red table is measured, not predicted.
      artifacts: [crates/repark-iceberg/src/write/merge/dv_close.rs, crates/repark-spark/src/tests/v3e4.rs]
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

Open finding kept honest: RePark supplies `(spec_id, partition)` only when every partition spec in
the metadata is unpartitioned. For a fresh path in a partitioned table the fork's lazy walk still
runs. RePark's merge/predicate plan does not carry the partition today — the `(_file, _pos)` pairs
come from a scratch scan projecting only those two reserved columns, `_spec_id` / `_partition` are
declared in the fork but served nowhere, and resolving them RePark-side would mean a data-manifest
walk RePark does not otherwise do, i.e. strictly worse than the fork's. Raised as a RULING rather
than built.

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

## 9. Gate exits

| gate | exit |
|---|---|
| `make verify` | 0 |
| `make preflight` | 0 |
| `make py-test` | 0 |
| `make check-map-sync check-ledger-grammar check-ledgers check-docs-compaction` | 0 |
| `python3 scripts/ledger_lifecycle.py check --base origin/main` | 0 |
| `cargo test --locked -p repark-iceberg --lib` | 0 |
| `cargo test --locked -p repark-spark --lib` | 0 |
| `.venv/bin/python -m pytest python/repark/tests/test_v3_live_oracle.py` (`REPARK_PARITY_LIVE=1`) | 0 |
