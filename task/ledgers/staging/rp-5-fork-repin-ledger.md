# Charter ledger — RP-5 · fork repin 33be9a0 → 00cdde0 (consume F-6b/F-6c, F-8, F-16r; close REF-1 and RDF-1)

**Date:** 2026-09-01 · **Branch:** `feat/rp-5-fork-repin` · **Base:** `main`
`75f5ee35f4355f8a9a3d03ccc77cc751a9610f7a` · **Policy:**
[../../../AGENTS.md](../../../AGENTS.md) "Version-pin contract" · **Handoff:**
[../../roadmap/mid-term/iceberg-rust-handoff-2026-08-23.md](../../roadmap/mid-term/iceberg-rust-handoff-2026-08-23.md)
asks F-0, F-6, F-8, F-16 · **Path:** STANDARD (`risk_tier: standard`; one Actor cycle).
**Proven pattern:**
[2026-08-31-rp-4-fork-repin-ledger.md](../archive/2026-08/2026-08-31-rp-4-fork-repin-ledger.md).
**REF oracle transcript (write leg):**
[ref-branch-tag-wap-ledger.md](ref-branch-tag-wap-ledger.md).

**Retires:** this ledger moves to `../completed/` in this unit's last commit.

**Why now.** RP-4 froze the pin at `33be9a0f411c37cd8d7b38c4db81eec30c1344cc`. Fork
`main` is five merged PRs past that pin: `#245` F-6b (`IcebergTableProvider::with_commit_branch`),
`#246` R91 (`unknown` refused on parquet write), `#247` F-8 (metadata-table `scan` honors
`projection`; `table_names` lists catalog entries only), `#248` F-16r (`rewrite_data_files`
delete-ratio counts bounds-only parquet position deletes), `#249` F-6c (every DataFusion
scan site resolves the branch head). The family stays frozen: datafusion 54.1.0,
datafusion-spark 54.1.0, arrow*/parquet 58.4.0, rust-toolchain 1.96.0 byte-identical to
`main`. New pin: `00cdde00685bbc94552b29fcf8ed6767fe051ce6`.

**Not in this unit:** WAP (`spark.wap.*`, publish procedures — REF-3 BACKLOG);
`df.writeTo("cat.ns.t.branch_b").append()` unless `writeTo` already funnels into the
same SQL/commit path with trivial plumbing; ANSI-door dotted selector spelling;
the 1e7 × 50 MW-7 driver (optional; record if skipped); DataFusion/arrow/toolchain
family bumps (HALT).

## PROPOSITION LEDGER — RP-5 — 2026-09-01

| Clause | Proposition (checkable) | Proof obligation | Verdict | Evidence / open question |
|---|---|---|---|---|
| C-001 | Every `iceberg*` `[patch.crates-io]` rev is `00cdde00685bbc94552b29fcf8ed6767fe051ce6` and `Cargo.lock` resolves to it; `datafusion`, `datafusion-spark`, `arrow*`, `parquet` and `rust-toolchain.toml` are byte-identical to `main`. | `rg` on the workspace `Cargo.toml` + lock source entries; `git diff origin/main -- rust-toolchain.toml` empty; family freeze vs `origin/main`. | **OPEN** | Target SHA named. Bump not yet run. |
| C-002 | The standing `NamespaceScopedCatalog` duty holds at the new rev (forwards every required `Catalog` method; trait diff old vs new), and the what-changed note lists every commit in `33be9a0..00cdde0` with the engine site that absorbs each. The metadata-projection duty retires under C-003. | Trait diff; namespace-scoped pins; the note in §6. | **OPEN** | Range is the five fork PRs in the brief table. |
| C-003 | F-8 is consumed: `catalog/metadata_projection.rs` (`ProjectingMetadataTableProvider` and `MetadataProjectionSchemaProvider`) and the wrap at `catalog/provider.rs` plus the `pub use` are deleted. The two existing metadata-table pins stay green without the shim, and each reds if the property it guards is broken. | Green before delete; green after; one cheap mutation per pin then restore. | **OPEN** | Pins: `crates/repark-spark/src/tests/metadata_tables.rs`. |
| C-004 | The write leg of REF lands. Spark-door `INSERT`/`UPDATE`/`DELETE`/`MERGE`/`INSERT OVERWRITE` onto a diverged branch commit onto that branch, parented off the branch head, and leave `main` unmoved. A write naming a tag refuses with a Spark-shaped message. Missing-branch outcome matches the live oracle, not the fork's INSERT-VALUES create. Both execution halves land: fork-executed families through `IcebergTableProvider::with_commit_branch`; RePark-owned families through `.to_branch(b)` plus a branch-head scan. | Per-family pins on Spark door and facade; mutation-proof by forcing the branch to `main`; oracle transcript. | **OPEN** | Oracle not yet run this unit. REF ledger's measured Spark tables are the write-leg starting transcript. |
| C-005 | F-16r is consumed: `test_delete_laden_in_band_file_survives_the_runbook` is re-measured at the new pin. If red, rewrite as the Spark-equal assertion (zero delete files and zero delete records after the runbook on the 2,500-row shape) and mark registry `RDF-1` FIXED with the measured counts. If green, keep the pin and report the measured counts as a finding. | Pin red/green at the new rev; registry row. | **OPEN** | 1e7 × 50 MW-7 driver is optional. |
| C-006 | R91 is carried: any engine pin or registry sentence about `unknown` that the new rev falsifies is corrected; no new surface. | Grep engine pins / registry `unknown` / R91; compile and the existing V3-6 refuse pins. | **OPEN** | V3-6 pinned CREATE refusal and filed a scan gap. |
| C-007 | F-0 engine follow-up: measure whether `write.merge.isolation-level = snapshot` is exposed through the MERGE path the way `write.overwrite.isolation-level` is. Honored → a pin that the snapshot level skips data-conflict validation on MERGE. Not exposed → a pin that the property is ignored or refused, plus a dated registry row. One pin, no new feature work. | Existing MERGE isolation tests plus one load-bearing pin; Spark oracle cell. | **OPEN** | OCC-1 already reads the property; this unit re-measures at the new pin. |
| C-008 | Documents say what the pins prove: `docs/fork-sync.md` pin-history row; registry REF-1 FIXED / REF-3 BACKLOG / RDF-1; handoff F-6 / F-6b / F-6c / F-8 / F-16 / F-0 marked consumed; STATUS REF workstream truth-up and nothing else; every touched directory's `map.md`; iceberg-guide only if it states the write-to-branch refusal. | `make check-map-sync`, `check-ledger-grammar`, `check-ledgers`. | **OPEN** | Departure `move` is the last commit. |

VERDICT: 8 clauses, 0 PROVEN, 8 OPEN, 0 REJECTED.

## 2. Sequence

1. This ledger (grammar-gate clean, verdicts OPEN) — this commit.
2. Repin commit (C-001). Compile is the measurement. What-changed note (C-002).
3. F-8 shim delete (C-003) with mutation-proof of the two existing pins.
4. Write-to-branch (C-004) after the live oracle transcript; both execution halves.
5. RDF-1 re-measure (C-005), R91 carry (C-006), F-0 isolation pin (C-007).
6. Truth-up (C-008) and gates. Departure `move` last.

## 3. Pickup — what the next agent needs to know

- Pickup archive already committed: `d6b6408` filed dfp-1, fnp-7, rp-4 into
  `archive/2026-08/`.
- Fork source checkout (read-only): `/tmp/fork-src` is shallow. Use the PR table plus
  the cargo git checkout under `~/.cargo/git/checkouts/` for the what-changed note.
- Oracle: PySpark 4.1.2 + Iceberg 1.11.0; `JAVA_HOME=/usr/lib/jvm/zulu-17-amd64`;
  interpreter `<pyspark-4.1.2-oracle>`. Measure missing-branch, tag, diverged-branch
  writes, and MERGE isolation **before** pinning C-004 / C-007.
- Standing duties live in [crates/repark-iceberg/map.md](../../../crates/repark-iceberg/map.md)
  "Known limitations". The metadata-projection duty retires in this unit.

```yaml
PROPORTIONALITY_RUBRIC:
  id: RUBRIC-rp-5-fork-repin
  pr_unit: rp-5-fork-repin
  criteria:
    blast_radius: FAIL (workspace pin + lock + write-to-branch routing)
    reversibility: PASS (one revert commit; no migration)
    size: FAIL (pin, lock, shim delete, branch routing, maps, registry)
    novelty: PASS (consume frozen fork SHAs; no new dependency)
    sensitivity: FAIL (write/commit path)
    clarity: PASS (charter frozen 2026-09-01; eight clauses; RP-4 pattern)
  path: STANDARD
  recorded_by: Actor
```

```yaml
SELF_LOGIC_REVIEW:
  id: SLR-RP5-CHARTER
  agent: Actor
  action: File the RP-5 staging ledger and lockstep staging map, no pin yet
  charter_trace: C-001..C-008
  preconditions:
    - AGENTS.md version-pin contract and RP-4 ledger read: SATISFIED
    - Branch is feat/rp-5-fork-repin at 75f5ee3 plus pickup archive: SATISFIED (git)
    - Disk headroom: SATISFIED (/ has 498 G free of 1.8 T, 2026-09-01)
    - Pickup archive: SATISFIED (d6b6408)
  success_condition: staging ledger exists, staging/map.md links it, check-ledger-grammar accepts OPEN clauses
  step_risks:
    - Widening the range past 00cdde0: HANDLED(charter names the five PRs and the SHA)
    - Partial write-to-branch (MERGE yes, INSERT no): HANDLED(C-004 requires both halves)
    - Pinning fork INSERT-VALUES create if Spark refuses missing branches: HANDLED(oracle first)
    - Family pin moves: HANDLED(HALT)
  contingencies:
    - Grammar red: EXECUTABLE(fix citation / clause table and recommit)
    - Oracle cannot start: EXECUTABLE(HALT with the SQL scripts)
  tripwire_scan: CLEAN
  uncertainty: NONE
  verdict: PROCEED
  escalation: —
```

```yaml
PRE_EXECUTION_REVIEW:
  id: PER-rp-5-fork-repin
  slr: SLR-RP5-CHARTER
  plan_checklist:
    charter_frozen: SATISFIED (this file, dated 2026-09-01)
    carving_clause_complete:
      forward:  SATISFIED (C-001..C-008 → one PR unit)
      backward: SATISFIED (the unit traces to all eight)
    rubric_recorded: SATISFIED (1/1 STANDARD)
    bindings_resolved: SATISFIED (green = make verify + make py-test + preflight + ledger checks)
    contingencies_executable: SATISFIED (family HALT; oracle HALT; fork-API HALT)
  verdict: PROCEED
  gap_route: "—"
  gap_detail: "—"
```

## 6. What changed under us (C-002)

Range `33be9a0f411c37cd8d7b38c4db81eec30c1344cc..00cdde00685bbc94552b29fcf8ed6767fe051ce6`
is **5 commits** (to be listed from the cargo git checkout after the bump). Compare:
`https://github.com/TRO-Wolf/iceberg-rust/compare/33be9a0f411c37cd8d7b38c4db81eec30c1344cc...00cdde00685bbc94552b29fcf8ed6767fe051ce6`

| Fork PR | Change | Engine site that absorbs it |
|---|---|---|
| `#245` F-6b | `IcebergTableProvider::with_commit_branch` | C-004 fork-executed families |
| `#246` R91 | `unknown`-typed column refused on parquet write | C-006 carry; correct only what the rev falsifies |
| `#247` F-8 (`f80372db5`) | metadata-table `scan` honors `projection`; `table_names` lists catalog entries only | C-003 shim delete |
| `#248` F-16r (`6801659bf`) | delete-ratio counts bounds-only parquet position deletes | C-005 RDF-1 re-measure |
| `#249` F-6c (`00cdde00685bbc94552b29fcf8ed6767fe051ce6`) | every DataFusion scan site resolves the branch head | C-004 read-of-branch for INSERT/UPDATE/DELETE |

## 7. Gates (C-008)

Recorded at departure.

## 8. Oracle transcript (C-004 / C-007)

Path scrubbed to `<pyspark-4.1.2-oracle>`. Filled after the live run.
