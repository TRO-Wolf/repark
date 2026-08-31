# Charter ledger — RP-4 · fork repin d408da42 → 33be9a0 (consume F-7 slice 1, carry F-6)

**Date:** 2026-08-31 · **Branch:** `feat/rp-4-fork-repin` · **Base:** `main`
`bb7fa54af48632c52d28aa8f7f446fac1dbf3742` · **Policy:**
[../../../AGENTS.md](../../../AGENTS.md) "Version-pin contract" · **Handoff:**
[../../roadmap/mid-term/iceberg-rust-handoff-2026-08-23.md](../../roadmap/mid-term/iceberg-rust-handoff-2026-08-23.md)
§5 (the repin protocol) · **Path:** STANDARD (`risk_tier: standard`; one Actor cycle).
**Proven pattern:**
[2026-08-30-rp-3-fork-repin-ledger.md](../archive/2026-08/2026-08-30-rp-3-fork-repin-ledger.md).

**Retires:** moved to `completed/` in this unit's departure commit.

**Why now.** RP-3 froze the pin at `d408da42fb91db2010662fe1da3783b82fa6e1ed`. Fork
`main` has three later commits: `#241` (`d4f55e1`, test pin only), `#243` (`4f6fa4e`,
F-7 slice 1, BEHAVIOR — v3 row-lineage carry through `rewrite_data_files`), `#244`
(`33be9a0`, F-6, potentially BREAKING — `SnapshotUpdate.to_branch`). The family stays
frozen: datafusion 54.1.0, datafusion-spark 54.1.0, arrow*/parquet 58.4.0,
rust-toolchain 1.96.0 byte-identical to `main`. F-7 slice 1 is consumed by
re-measure; F-6 is carried, not consumed (REF later).

**Not in this unit:** FNP registry/kernels/`python/repark/functions`; engine
`to_branch` routing; V3-5 charter; archive/completed ledger moves;
`briefs/next-sequence.md`.

## PROPOSITION LEDGER — RP-4 — 2026-08-31

| Clause | Proposition (checkable) | Proof obligation | Verdict | Evidence / open question |
|---|---|---|---|---|
| C-001 | Every `iceberg*` `[patch.crates-io]` rev is `33be9a0f411c37cd8d7b38c4db81eec30c1344cc` and `Cargo.lock` resolves to it; `datafusion`, `datafusion-spark`, `arrow*`, `parquet` and `rust-toolchain.toml` are byte-identical to `main`. | `rg` on the workspace `Cargo.toml` + lock source entries; `git diff origin/main -- rust-toolchain.toml` empty; Cargo.toml vs origin/main is only the five revs plus citation lockstep. | **PROVEN** | Five `[patch.crates-io]` revs and six lock sources (`iceberg`, `iceberg-catalog-glue`, `iceberg-catalog-s3tables`, `iceberg-datafusion`, `iceberg-sketches`, `iceberg-storage-opendal`) are `33be9a0f411c37cd8d7b38c4db81eec30c1344cc`; zero `d408da42` remain. `cargo check --locked --workspace` exit 0 (1m 42s, 2026-08-31). `git diff origin/main -- rust-toolchain.toml` empty. Cargo.toml vs origin/main is the five revs. Lock vs origin/main is 12 lines (the six iceberg* sources). Family freeze: datafusion 54.1.0, datafusion-spark 54.1.0, arrow*/parquet 58.4.0, rust-toolchain 1.96.0. Citation: `crates/repark-iceberg/map.md`. |
| C-002 | The two standing repin duties hold at the new rev (`NamespaceScopedCatalog` forwards every required `Catalog` method; the metadata-projection shim is kept iff the fork's metadata-table `scan` still ignores `projection`), and the what-changed note lists every commit in `d408da42..33be9a0` (`d4f55e1` #241, `4f6fa4e` #243 F-7 slice 1, `33be9a0` #244 F-6) with the engine site that absorbs each. | Trait diff; fork `metadata_table.rs`; the two metadata-table pins; the note in §6. | **PROVEN** | Range is 3 commits (listed in §6). `Catalog` trait still 14 required + 16 defaulted; no method added or removed. Three `NamespaceScopedCatalog` omissions still compose. `cargo test -p repark-iceberg --lib catalog::tests::namespace_scoped` → 4 passed. Shim stays: `iceberg-datafusion` `table/metadata_table.rs` `scan` still takes `_projection` and ignores it. `IcebergSchemaProvider::try_new` is still lazy. F-6 `to_branch` exists on snapshot producers; engine does not call it (C-004). Compile green is the F-6 absorption. Citation: `crates/repark-iceberg/map.md`. |
| C-003 | **F-7 slice 1 re-measured at the frozen SHA.** Engine `CALL system.rewrite_data_files` (direct fork action below the public guard, as RP-3 C-005) on the v3 fixture, then PySpark 4.1.2 + Iceberg 1.11.0 read-back of `_row_id` / `_last_updated_sequence_number`. If lineage now carries Spark-equal, registry `V3-LINEAGE-1` moves to FIXED (dated 2026-08-31, fork #243) and the public guard lifts; STATUS / north-star / the handoff truth up; V3-5 becomes charterable. If it still reassigns, the guard stays and the measured divergence is filed against the fork row. A green fork row is not evidence. | RP-3 §11 driver re-run; Spark read-back of both state copies; red-first on any pin that flips. | **PROVEN** | Native v3 12-file fixture: rewritten=12 added=1. Engine scan before=after `(1,0,1)…(12,11,12)`. PySpark 4.1.2 + Iceberg 1.11.0 `toArrow` schemas `id:int64, _row_id:int64, _last_updated_sequence_number:int64`; `LINEAGE_EQUAL=True`. Guard lifted. Pin `call_rewrite_data_files_on_v3_preserves_row_lineage` (5/5 `tests::call_v3` 2026-08-31). Registry `V3-LINEAGE-1` FIXED. Citation: `crates/repark-spark/src/tests/call_v3.rs`. |
| C-004 | **F-6 carried, not consumed.** No engine surface calls `to_branch` in this unit. Compile is green at the new transaction shape; checked-in Spark fixtures are byte-flat vs `origin/main`; the REF / F-6 handoff row notes that the fork surface now exists. | Grep engine sources for `to_branch`; fixture byte comparison; the REF row note. | **PROVEN** | `cargo check --locked` green at `33be9a0` (F-6 compile). `test_engine_sources_do_not_call_to_branch` + fixture byte-flat. Handoff F-6 row and registry REF-1 note the fork surface. Citation: `python/repark/tests/test_rp4_c004_to_branch.py`. |
| C-005 | The documents say what the pins prove: north star `rewrite_data_files` row, STATUS, the handoff F-7 / F-6 take-or-carry, `docs/fork-sync.md` pin history, crate maps and the divergence registry in lockstep. | `make check-map-sync`, `check-docs-compaction`, `check-ledger-grammar`. | **PROVEN** | North star rewrite row ✅; STATUS RP-4; handoff F-7/F-6; `docs/fork-sync.md` pin history; registry FIXED + REF-1 carry note. STATUS 24841 B / 25000. Citation: `crates/repark-spark/src/map.md`. |
| C-006 | Green on the bound gates: `make verify`, `make check-map-sync check-ledger-grammar`, `python3 scripts/ledger_lifecycle.py check --base bb7fa54af48632c52d28aa8f7f446fac1dbf3742`, `make py-test`. | Gate output attached. Real exit codes. | **PROVEN** | 2026-08-31: `make verify` exit 0; `make check-map-sync check-ledger-grammar` exit 0 (162 maps; 15 live ledgers); `python3 scripts/ledger_lifecycle.py check --base bb7fa54af48632c52d28aa8f7f446fac1dbf3742` exit 0; `make py-test` exit 0 (472 passed). Citation: `crates/repark-spark/src/map.md`. |

VERDICT: 6 clauses, 6 PROVEN, 0 OPEN, 0 REJECTED.

## 2. Sequence

1. This ledger (grammar-gate clean, verdicts OPEN) — this commit.
2. Repin commit (C-001). Compile is the measurement. What-changed note (C-002).
3. F-7 slice 1 re-measure (C-003) and any guard / registry move.
4. F-6 carry proof (C-004).
5. Truth-up (C-005) and gates (C-006).

## 3. Pickup — what the next agent needs to know

- RP-3 C-005 driver (direct `RewriteDataFiles` on twelve single-row v3 files, Spark
  Hadoop-catalog read-back of both metadata pointers) is the re-measure, not the
  public `CALL` path (that path is still guarded by `V3-LINEAGE-1`).
- Oracle: PySpark 4.1.2 + Iceberg 1.11.0; `JAVA_HOME=/usr/lib/jvm/zulu-17-amd64`.
- Standing duties live in [crates/repark-iceberg/map.md](../../../crates/repark-iceberg/map.md)
  "Known limitations".
- Engine `RowDelta` / transaction talk is the F-6 compile measurement; `ref_ddl.rs`
  `write_to_branch_refuses_loud_naming_fork_gap` stays refused this unit.

```yaml
PROPORTIONALITY_RUBRIC:
  id: RUBRIC-rp-4-fork-repin
  pr_unit: rp-4-fork-repin
  criteria:
    blast_radius: FAIL (workspace pin + lock; possible V3-LINEAGE-1 lift)
    reversibility: PASS (one revert commit; no migration)
    size: FAIL (pin, lock, maps, registry, possible guard)
    novelty: PASS (no new dependency; consume a frozen fork SHA)
    sensitivity: FAIL (row-lineage compaction path if C-003 is green)
    clarity: PASS (charter frozen 2026-08-31; six clauses; RP-3 pattern)
  path: STANDARD
  recorded_by: Actor
```

```yaml
SELF_LOGIC_REVIEW:
  id: SLR-RP4-CHARTER
  agent: Actor
  action: File the RP-4 staging ledger and lockstep staging map, no pin yet
  charter_trace: C-001..C-006
  preconditions:
    - AGENTS.md version-pin contract and RP-3 ledger read: SATISFIED
    - Branch is feat/rp-4-fork-repin at bb7fa54: SATISFIED (git)
    - Disk headroom: SATISFIED (/ has 571 G free of 1.8 T, 2026-08-31)
    - Pickup archive: SATISFIED as SKIP (unit fence: do not archive ledgers)
  success_condition: staging ledger exists, staging/map.md links it, check-ledger-grammar accepts OPEN clauses
  step_risks:
    - Widening the range past 33be9a0: HANDLED(charter names the three commits)
    - Consuming F-6 in this unit: HANDLED(C-004 carry-only; HALT if engine rework)
    - Treating a green fork row as evidence: HANDLED(C-003 requires Spark read-back)
  contingencies:
    - Grammar red: EXECUTABLE(fix citation / clause table and recommit)
  tripwire_scan: CLEAN
  uncertainty: NONE
  verdict: PROCEED
  escalation: —
```

```yaml
PRE_EXECUTION_REVIEW:
  id: PER-rp-4-fork-repin
  slr: SLR-RP4-CHARTER
  plan_checklist:
    charter_frozen: SATISFIED (this file, dated 2026-08-31)
    carving_clause_complete:
      forward:  SATISFIED (C-001..C-006 → one PR unit)
      backward: SATISFIED (the unit traces to all six)
    rubric_recorded: SATISFIED (1/1 STANDARD)
    bindings_resolved: SATISFIED (green = make verify + make py-test + ledger checks)
    contingencies_executable: SATISFIED (F-6 HALT; C-003 red keeps the guard)
  verdict: PROCEED
  gap_route: "—"
  gap_detail: "—"
```

## 6. What changed under us (C-002)

Range `d408da42fb91db2010662fe1da3783b82fa6e1ed..33be9a0f411c37cd8d7b38c4db81eec30c1344cc`
is **3 commits**. Compare:
`https://github.com/TRO-Wolf/iceberg-rust/compare/d408da42fb91db2010662fe1da3783b82fa6e1ed...33be9a0f411c37cd8d7b38c4db81eec30c1344cc`

| Fork commit | Change | Engine site that absorbs it |
|---|---|---|
| `d4f55e1` (#241) | fork-side parity test pin only (`CurrentFileStatus` never-started) | none; `cargo check --locked --workspace` exit 0 |
| `4f6fa4e` (#243, F-7 slice 1, BEHAVIOR) | v3 row-lineage carry through `rewrite_data_files` (`maintenance/rewrite_data_files_write.rs`, `metadata_columns.rs`, datafusion `physical_plan/row_lineage.rs`) | C-003 re-measure; public `V3-LINEAGE-1` guard lift iff Spark-equal |
| `33be9a0` (#244, F-6, potentially BREAKING) | `SnapshotUpdate.to_branch` commit target (`crates/iceberg/src/transaction/to_branch.rs`); `transaction/snapshot.rs` restructured | compile against `RowDelta` / transaction actions (green, 2026-08-31); REF row note (C-004); no engine caller this unit |

Listed from the cargo git checkout at `33be9a0f411c37cd8d7b38c4db81eec30c1344cc`:
`d4f55e1d`, `4f6fa4e4`, `33be9a0f`.

```yaml
SELF_LOGIC_REVIEW:
  id: SLR-C-001
  agent: Actor
  action: commit the repin (C-001) and the what-changed note (C-002)
  charter_trace: C-001, C-002
  preconditions:
    - charter committed: SATISFIED (ae29e04)
    - target SHA named: SATISFIED (33be9a0f411c37cd8d7b38c4db81eec30c1344cc)
    - family freeze target: SATISFIED (datafusion 54.1.0, arrow/parquet 58.4.0, rust-toolchain 1.96.0 on origin/main)
    - disk: SATISFIED (571 G free)
  success_condition: cargo check --locked --workspace exit 0; five iceberg* revs and lock sources are 33be9a0; rust-toolchain.toml identical to origin/main
  step_risks:
    - family pin moves: HANDLED(diff origin/main; halt if datafusion/arrow/toolchain change)
    - F-6 compile break beyond mechanical absorption: HANDLED(HALT with questions)
    - grammar does not count root Cargo.toml: HANDLED(citation lives in crates/repark-iceberg/map.md)
  contingencies:
    - cargo update fails: EXECUTABLE(record the error; do not hand-edit the lock)
  tripwire_scan: CLEAN
  uncertainty: NONE
  verdict: PROCEED
  escalation: —
```

## 7. Gates (C-006)

| Command | Exit |
|---|---|
| `make verify` | 0 |
| `make check-map-sync check-ledger-grammar` | 0 (162 maps; 15 live ledgers) |
| `python3 scripts/ledger_lifecycle.py check --base bb7fa54af48632c52d28aa8f7f446fac1dbf3742` | 0 |
| `make py-test` | 0 (472 passed) |

```yaml
COVERAGE_ATTESTATION:
  pr_unit: rp-4-fork-repin
  categories:
    - id: AT-1
      status: ATTACKED
      evidence: CALL rewrite_data_files on a 12-file v3 table keeps _row_id/seq Spark-equal; PySpark 4.1.2 + Iceberg 1.11.0 Arrow int64.
      artifacts: [crates/repark-spark/src/tests/call_v3.rs, python/repark/tests/test_v3_create_opt_in.py]
    - id: AT-2
      status: ATTACKED
      evidence: Twelve-file compaction; opt-in single-file CALL no longer refuses; v2 rewrite control still compacts.
      artifacts: [crates/repark-spark/src/tests/call_v3.rs]
    - id: AT-3
      status: ATTACKED
      evidence: V3-COW-1 MERGE/UPDATE keep-refusal still fires on opt-in CREATE; write-to-branch still refuses.
      artifacts: [crates/repark-spark/src/tests/call_v3.rs, crates/repark-spark/src/tests/ref_ddl.rs]
    - id: AT-4
      status: N/A
      justification: Repin and CALL rewrite are sequential table commits; no new shared mutable engine state.
    - id: AT-5
      status: N/A
      justification: No AWS, IAM, or secret handling. Fixture bytes stay flat vs origin/main.
    - id: AT-6
      status: ATTACKED
      evidence: NamespaceScopedCatalog still forwards 14 required Catalog methods; metadata-projection shim stays; IcebergSchemaProvider try_new stays lazy.
      artifacts: [crates/repark-iceberg/map.md, crates/repark-iceberg/src/catalog/tests/namespace_scoped.rs]
    - id: AT-7
      status: N/A
      justification: No new recursion or unbounded allocation; fork pin only.
    - id: AT-8
      status: ATTACKED
      evidence: Five iceberg* revs and six lock sources are 33be9a0; family freeze holds; no engine to_branch caller.
      artifacts: [Cargo.toml, python/repark/tests/test_rp4_c004_to_branch.py]
    - id: AT-9
      status: ATTACKED
      evidence: V3-LINEAGE-1 FIXED names fork #243 and the Spark-equal transcript; REF-1 notes to_branch exists unconsumed.
      artifacts: [docs/spark-sql-iceberg-parity.md]
    - id: AT-10
      status: ATTACKED
      evidence: Six clauses pinned; maps lockstep; STATUS under 25000 B; gates recorded.
      artifacts: [task/ledgers/staging/rp-4-fork-repin-ledger.md, crates/repark-spark/src/map.md]
  reattested: []
  complete: true
```


