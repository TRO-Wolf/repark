# Charter ledger — RP-6 · fork repin 00cdde0 → fb0cacfa (consume PR-1..PR-7; lift V3-COW-1 where Spark-equal)

**Date:** 2026-09-01 · **Branch:** `feat/rp-6-fork-repin` · **Base:** `origin/main`
`a431190ac523fd111fe96404eec308ce0d18ab6f` · **Policy:**
[../../../AGENTS.md](../../../../AGENTS.md) "Version-pin contract" · **Handoff:**
[../../roadmap/mid-term/iceberg-rust-handoff-2026-08-23.md](../../../roadmap/mid-term/iceberg-rust-handoff-2026-08-23.md)
asks F-7, F-rp3-c7, F-16 residue 2 · **Path:** STANDARD (`risk_tier: standard`; one Actor cycle).
**Proven pattern:**
[2026-09-01-rp-5-fork-repin-ledger.md](../archive/2026-09/2026-09-01-rp-5-fork-repin-ledger.md).

**Retires:** this ledger moves to `../completed/` in this unit's last commit.

**Why now.** RP-5 froze the pin at `00cdde00685bbc94552b29fcf8ed6767fe051ce6`. Fork
`main` is seven merged PRs past that pin: `#253` PR-1 (REPLACE added>deleted refused),
`#254` PR-2 (`RewriteDataFiles` partition-safe after spec evolution; bounded
`max_open_partition_writers`), `#252` PR-5A (Glue / S3 Tables commit-transport seams),
`#255` PR-3 (V3 MoR UPDATE keeps `_row_id` and advances
`_last_updated_sequence_number`; F-rp3-c7 reclassified as a two-file-seed layout
artefact), `#256` PR-6B (MoR UPDATE lineage on a branch), `#257` PR-4 (V2→V3 upgrade
and five maintenance actions), PR-7 (closeout evidence). Family stays frozen:
datafusion 54.1.0, datafusion-spark 54.1.0, arrow*/parquet 58.4.0, rust-toolchain
1.96.0 byte-identical to `main`. New pin:
`fb0cacfa8ceda87f865fb0ae53be4b46e0ef8b7a`.

**Not in this unit:** F-16 residue 2 unless the fork landed it (check the fork ledger;
C-004 re-measures the MW-7 pin and states the result); DataFusion/arrow/toolchain
family bumps (HALT); WAP; a Spark-visible design choice not measured (HALT).

## PROPOSITION LEDGER — RP-6 — 2026-09-01

| Clause | Proposition (checkable) | Proof obligation | Verdict | Evidence / open question |
|---|---|---|---|---|
| C-001 | Every `iceberg*` `[patch.crates-io]` rev is `fb0cacfa8ceda87f865fb0ae53be4b46e0ef8b7a` and `Cargo.lock` resolves to it; `datafusion`, `datafusion-spark`, `arrow*`, `parquet` and `rust-toolchain.toml` are byte-identical to `main`. | `rg` on the workspace `Cargo.toml` + lock source entries; `git diff origin/main -- rust-toolchain.toml` empty; family freeze vs `origin/main`. | **PROVEN** | Five revs and six lock sources are `fb0cacfa`. `cargo check --locked --workspace` exit 0 (7m 10s). Family freeze: datafusion 54.1.0, datafusion-spark 54.1.0, arrow*/parquet 58.4.0, rust-toolchain 1.96.0. Citation: `crates/repark-iceberg/map.md`. |
| C-002 | The standing `NamespaceScopedCatalog` duty holds at the new rev (forwards every required `Catalog` method; trait diff old vs new). The what-changed note lists every fork PR `#253` `#254` `#252` `#255` `#256` `#257` PR-7 with the engine site that absorbs each. V3-COW-1 is re-measured at the new pin on adopted v3 and created v3 (opt-in), COW and MOR, single-table: row multiset, `_row_id`, `_last_updated_sequence_number`, `next-row-id` after DELETE (each position); DELETE then DELETE; UPDATE then DELETE (both orders); INSERT OVERWRITE then DELETE; MERGE matched-update. Seed with a single data file (`coalesce(1)` / `local[1]`); record file and manifest counts beside every number; never compare counters across layouts. Where the engine equals Spark, lift the guard on Spark, ANSI, and facade doors and turn the refusal pins into Spark-equal pins with the absolute values. Where it does not, keep the refusal and record the measured cell as a fork finding. Re-record the V3-COW-1 tripwire hashes honestly (cite this unit). | Trait diff; oracle transcript; three-door pins; mutation-proof (`N` red of `M`). | **PROVEN** | Catalog trait still 14 required + 16 defaulted. Range is 8 commits (§6). Plain-`WHERE` UPDATE/DELETE Spark-equal; sequential COW DELETE `next-row-id` 6 (mutation 1/1 red). MERGE still reassigns via the RePark-owned writer (engine finding, keep-refusal). F-rp3-c7 consumed as layout artefact. Tripwire re-recorded. Citation: `crates/repark-spark/src/tests/v3_cow.rs`. |
| C-003 | F-7 preserve-half: MoR UPDATE through the engine's doors against the live oracle. If Spark-equal (`_row_id` kept, `_last_updated_sequence_number` advanced on the updated row), retire "Preserve-half stays F-7" in the north star and the registry. | Three-door MoR UPDATE pins; north-star §3 Read lineage row; registry. | **PROVEN** | MoR UPDATE `(1,0,1),(2,1,2),(3,2,1)` on Spark, ANSI, facade, created and adopted. North-star Read row retires preserve-half. Citation: `crates/repark-spark/src/tests/v3_cow.rs`. |
| C-004 | RDF-1 stays honest: re-measure `test_delete_laden_in_band_file_survives_the_runbook` at the new pin. F-16 residue 2 is not in this repin unless the fork landed it. State the result. | Pin red/green at the new rev; registry row; fork ledger check. | **PROVEN** | Fork F-16r ledger still names partition-scoped survival; residue 2 is not in `00cdde0..fb0cacfa`. RDF-1 stays BACKLOG. Citation: `python/repark/tests/test_mw7_scale_smoke.py`. |
| C-005 | A Spark-door pin that compaction after a same-arity spec evolution stamps the current spec (fork `#254` PR-2), measured against the oracle's `CALL system.rewrite_data_files` on an evolved-spec table. | Spark-door pin; oracle cell. | **PROVEN** | `rewrite_after_same_arity_spec_evolution_stamps_current_spec` green: `REPLACE PARTITION FIELD x WITH y` then CALL stamps every live data file with the current spec. Citation: `crates/repark-spark/src/tests/call_v3.rs`. |
| C-006 | Documents say what the pins prove: `docs/fork-sync.md` pin-history row; registry rows touched; north star §3 rows touched (exactly what is proven); handoff F-7 / F-rp3-c7 / F-16 marked consumed; STATUS v3 workstream truth-up and nothing else; every touched directory's `map.md`; this ledger `move`d to `completed/` last. | `make check-map-sync`, `check-ledger-grammar`, `check-ledgers`. | **PROVEN** | Pin-history row; registry V3-COW-1 / RDF-1; north-star Read/MOR/COW rows; handoff F-7 / F-rp3-c7 / F-16 residue 2; STATUS Next is MERGE. Citation: `crates/repark-iceberg/map.md`. |

VERDICT: 6 clauses, 6 PROVEN, 0 OPEN, 0 REJECTED.

```yaml
COVERAGE_ATTESTATION:
  pr_unit: rp-6-fork-repin
  categories:
    - id: AT-1
      status: ATTACKED
      evidence: UPDATE/DELETE Spark-equal pins; MERGE keep-refusal with measured reassignment; sequential COW mutation 1/1 red.
      artifacts: [crates/repark-spark/src/tests/v3_cow.rs, crates/repark-spark/src/tests/v3_cow_lift.rs, crates/repark-sql/src/v3/cow.rs, python/repark/tests/test_v3_cow_dml.py]
    - id: AT-2
      status: ATTACKED
      evidence: DELETE each position, DELETE then DELETE, UPDATE then DELETE both orders, INSERT OVERWRITE then DELETE, MERGE matched-update; created and adopted v3; COW and MOR.
      artifacts: [crates/repark-spark/src/tests/v3_cow_lift.rs]
    - id: AT-3
      status: ATTACKED
      evidence: MERGE keep-refusal leaves rows and snapshot unmoved. Subquery DML keep-refusal.
      artifacts: [crates/repark-spark/src/tests/v3_cow.rs, crates/repark-sql/src/v3/cow.rs]
    - id: AT-4
      status: N/A
      justification: No new shared mutable engine state. The valve is a pass-through or the existing MERGE refuse.
    - id: AT-5
      status: ATTACKED
      evidence: No AWS, IAM, or secret handling. Tag/branch routing unchanged except the v3 UPDATE lift.
      artifacts: [crates/repark-iceberg/src/write/row_lineage_guard.rs]
    - id: AT-6
      status: ATTACKED
      evidence: NamespaceScopedCatalog still forwards 14 required Catalog methods at fb0cacfa. Trait unchanged vs 00cdde0.
      artifacts: [crates/repark-iceberg/map.md]
    - id: AT-7
      status: N/A
      justification: No new recursion or unbounded allocation.
    - id: AT-8
      status: ATTACKED
      evidence: Five iceberg* revs and six lock sources are fb0cacfa. Family freeze holds.
      artifacts: [crates/repark-iceberg/map.md, Cargo.toml]
    - id: AT-9
      status: ATTACKED
      evidence: V3-COW-1 remaining refusal is MERGE; F-7 preserve-half retired; RDF-1 stays BACKLOG.
      artifacts: [docs/spark-sql-iceberg-parity.md]
    - id: AT-10
      status: ATTACKED
      evidence: Six clauses pinned; maps lockstep; second-delete mutation 1/1 red then restored; evolved-spec rewrite stamps current spec.
      artifacts: [crates/repark-spark/src/tests/v3_cow.rs, crates/repark-spark/src/tests/call_v3.rs]
  complete: true
```

## 6. What changed under us (C-002)

Range `00cdde00685bbc94552b29fcf8ed6767fe051ce6..fb0cacfa8ceda87f865fb0ae53be4b46e0ef8b7a`
is **8 commits**. Compare:
`https://github.com/TRO-Wolf/iceberg-rust/compare/00cdde00685bbc94552b29fcf8ed6767fe051ce6...fb0cacfa8ceda87f865fb0ae53be4b46e0ef8b7a`

No separate PR-7 commit is in this range; the pin tip is PR-6B `#256`.

| Fork commit | PR | Change | Engine site that absorbs it |
|---|---|---|---|
| `4cb56b9f` | `#250` | V3 production work plan (docs) | none (docs) |
| `50f10cee` | `#251` PR-6A | Java/Rust branch read and commit interop | already consumed as F-6c (RP-5); compile |
| `ebc00a62` | `#253` PR-1 | REPLACE snapshots with added-records > deleted-records refused (`DataInvalid`) | compile; shared producer |
| `89e8701e` | `#254` PR-2 | `RewriteDataFiles` partition-safe after spec evolution; bounded `max_open_partition_writers` | C-005 Spark-door pin |
| `02077f9b` | `#252` PR-5A | Glue / S3 Tables commit-transport seams | compile; no engine API change |
| `1fc6747a` | `#255` PR-3 | V3 MoR UPDATE keeps `_row_id` and advances seq; F-rp3-c7 layout artefact | C-002 / C-003 valve lift |
| `8efa8cd7` | `#257` PR-4 | V2→V3 upgrade + five maintenance actions interop | compile; upgrade still opt-in |
| `fb0cacfa` | `#256` PR-6B | MoR UPDATE lineage on a branch | C-002 branch UPDATE pin |

`Catalog` trait: 14 required + 16 defaulted; no method added or removed.

## 2. Sequence

1. This ledger (grammar-gate clean, verdicts OPEN) — this commit.
2. Repin commit (C-001). Compile is the measurement. What-changed note and trait diff (C-002 duty).
3. Live-oracle transcript for V3-COW-1 sequences and MoR UPDATE, `local[1]` / `coalesce(1)`.
4. Lift where Spark-equal (C-002, C-003); keep refusal + fork finding where not. Re-record tripwire.
5. RDF-1 re-measure (C-004). Evolved-spec rewrite pin (C-005).
6. Truth-up (C-006) and gates. Departure `move` last.

## 3. Pickup — what the next agent needs to know

- Pickup archive already clean: `make ledger-archive` filed nothing (2026-09-01).
- Oracle: PySpark 4.1.2 + Iceberg 1.11.0; `JAVA_HOME=/usr/lib/jvm/zulu-17-amd64`;
  interpreter `<pyspark-4.1.2-oracle>`. Measure **before** lifting any guard.
- Standing duties live in [crates/repark-iceberg/map.md](../../../../crates/repark-iceberg/map.md)
  "Known limitations". `NamespaceScopedCatalog` re-enumerate at the new rev.
- Tripwire: `crates/repark-spark/src/tests/v3_lineage.rs::cow_keep_refusal_files_are_byte_untouched`.
  This unit (RP-6) may re-record those hashes when a keep-refusal file changes for a measured lift.

```yaml
PROPORTIONALITY_RUBRIC:
  id: RUBRIC-rp-6-fork-repin
  pr_unit: rp-6-fork-repin
  criteria:
    blast_radius: FAIL (workspace pin + lock + v3 row-DML guard lift)
    reversibility: PASS (one revert commit; no migration)
    size: FAIL (pin, lock, guard lift, maps, registry)
    novelty: PASS (consume frozen fork SHAs; no new dependency)
    sensitivity: FAIL (write/commit path)
    clarity: PASS (charter frozen 2026-09-01; six clauses; RP-5 pattern)
  path: STANDARD
  recorded_by: Actor
```

```yaml
SELF_LOGIC_REVIEW:
  id: SLR-RP6-CHARTER
  agent: Actor
  action: File the RP-6 staging ledger and lockstep staging map, no pin yet
  charter_trace: C-001..C-006
  preconditions:
    - AGENTS.md version-pin contract and RP-5 ledger read: SATISFIED
    - Branch is feat/rp-6-fork-repin at a431190: SATISFIED (git)
    - Disk headroom: SATISFIED (/ has 513 G free of 1.8 T, 2026-09-01)
    - Pickup archive: SATISFIED (nothing to archive)
  success_condition: staging ledger exists, staging/map.md links it, check-ledger-grammar accepts OPEN clauses
  step_risks:
    - Widening the range past fb0cacfa: HANDLED(charter names the seven PRs and the SHA)
    - Comparing next-row-id across layouts: HANDLED(C-002 layout rule; coalesce(1)/local[1])
    - Lifting a guard without an oracle cell: HANDLED(measure first; HALT on unmeasured Spark-visible choice)
    - Family pin moves: HANDLED(HALT)
  contingencies:
    - Grammar red: EXECUTABLE(fix citation / clause table and recommit)
    - Oracle cannot start: EXECUTABLE(HALT with the SQL scripts)
    - Fork API is not as described: EXECUTABLE(HALT)
  tripwire_scan: CLEAN
  uncertainty: NONE
  verdict: PROCEED
  escalation: —
```

```yaml
PRE_EXECUTION_REVIEW:
  id: PER-rp-6-fork-repin
  slr: SLR-RP6-CHARTER
  plan_checklist:
    charter_frozen: SATISFIED (this file, dated 2026-09-01)
    carving_clause_complete:
      forward:  SATISFIED (C-001..C-006 → one PR unit)
      backward: SATISFIED (the unit traces to all six)
    rubric_recorded: SATISFIED (1/1 STANDARD)
    bindings_resolved: SATISFIED (green = make verify + make py-test + preflight + ledger checks)
    contingencies_executable: SATISFIED (family HALT; oracle HALT; fork-API HALT)
  verdict: PROCEED
  gap_route: "—"
  gap_detail: "—"
```
