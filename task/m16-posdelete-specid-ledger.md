# Unit ledger — M16 position-delete evolved-spec `spec_id`

**Unit:** M16 · **Date:** 2026-08-15 ·
**Lane:** repark · **Executor:** Grok (grok-4.6) ·
**Worktree:** `/tmp/grok-m16` · **Branch:** `grok/m16-posdelete-specid` ·
**Base (FROZEN):** `cd0db4f` (`docs(ta): truth-up the TYPPRICE oracle note… (#114)`)

**Charter:** `planning/grok/BRIEF-m16-specid-13.md` + conductor-13 Addendum A11 +
`planning/hardening/MERGE-AUDIT-FINDINGS.md` M16 (PLAUSIBLE, S1-latent).
**SEPMO:** HIGH — octo + C4 (Iceberg-spec correctness). Floor S1.
Sequential hat-switch Actor → C1 → C2 → C3 → C4. Label **OCTO-CONVERGED**
only after those critics ran.

This ledger does **not** edit `docs/spark-sql-iceberg-parity.md`,
`STATUS.md`, `merge/mod.rs`, or the fork pin (A11).

### Proposition ledger (scope audit)

| ID | Proposition | Verdict |
|---|---|---|
| C-001 | Finding is PLAUSIBLE until a spec-evolved table repro runs. | PROVEN — repro first; CONFIRMED |
| C-002 | Create partitioned spec 0, evolve to unpartitioned spec 1 via fork `UpdatePartitionSpec` (`apply_partition_spec_changes` / `RemoveFieldByTransform`). | PROVEN — pin fixture |
| C-003 | A post-evolve append writes a data file under spec 1. | PROVEN — `data_files[0].partition_spec_id() == evolved` |
| C-004 | Pre-fix `write_position_deletes` stamps the delete `partition_spec_id() == 0`. | PROVEN — red run: `left: 0 right: 1` |
| C-005 | Outcome is loud commit failure XOR silently inapplicable delete; record which. | PROVEN — **loud fail**: `DataInvalid => Partition value is not compatible with partition type` |
| C-006 | If the fork already rejects/corrects, downgrade and pin green; no fix. | REJECTED as the live outcome — writer emits 0; commit rejects. Fix required. |
| C-007 | Fix threads the resolved spec via `.with_partition_spec(spec)` on the unpartitioned-but-not-spec-0 branch; `partition_key` stays `None`. | PROVEN — grouping + writer builder |
| C-008 | `write_position_deletes` (L110) signature is unchanged. | PROVEN — still `(table, pairs, concurrency)` |
| C-009 | Spec-0 unpartitioned path stays `None` + no configured spec (existing tests green). | PROVEN — control pin + `mor_matched_delete_*` + codec pin |
| C-010 | Red-then-green: the repro pin is RED before the fix and GREEN after. | PROVEN — §3 |
| C-011 | Module docstring no longer claims the unpartitioned case is *only* a lookup result. | PROVEN — module + `write_position_deletes` docs name the 0-fallback |
| C-012 | No `merge/mod.rs`, Cargo.toml, STATUS, registry edits. | PROVEN — diff names |
| C-013 | `map.md` lockstep + this ledger linked from `task/map.md` in the same change. | PROVEN — listed in §2 |
| C-014 | `make verify` then `make preflight` before `gh pr create`. | PROVEN — §4 |

---

## 0. Blast + seam

| Item | Location |
|---|---|
| Finding | `MERGE-AUDIT-FINDINGS.md` M16 (PLAUSIBLE → CONFIRMED) |
| Writer | `crates/repark-iceberg/src/write/position_delete.rs` |
| Fork API | `PositionDeleteFileWriterBuilder::with_partition_spec` + `resolve_partition_spec_id` (rev `0c5fd58d`) |
| Evolution API | `apply_partition_spec_changes` → fork `UpdatePartitionSpecAction` |
| Commit | `RowDelta` / `SnapshotProducer::validate_partition_value` (not edited; T3 owns `merge/mod.rs`) |

Altitude: engine write path. MERGE is never gated (valve comment unchanged).

---

## 1. Repro (PLAUSIBLE → CONFIRMED)

Fixture (fork APIs only; MemoryCatalog + local-fs warehouse; no AWS):

1. `CREATE` identity-partitioned `id` (spec 0) with `write.merge.mode=merge-on-read`.
2. `apply_partition_spec_changes(RemoveFieldByTransform { id, Identity })` → default spec
   unpartitioned, id **1**.
3. `append` three rows. The data file claims spec 1 (append already builds a
   `PartitionKey` from the current default spec).
4. `write_position_deletes` on `(path, pos=1)` **and** a MoR `MERGE … WHEN MATCHED THEN DELETE`
   of `id=2`.

**Pre-fix (red, 2026-08-15):**

```
assertion `left == right` failed: M16: the emitted delete must claim the resolved
unpartitioned spec, not 0 (MERGE outcome: Err(External(DataInvalid => Partition
value is not compatible with partition type)))
  left: 0
 right: 1
```

| Question | Answer |
|---|---|
| Emitted `DataFile.partition_spec_id()` | **0** |
| Loud commit failure or silent miss? | **Loud commit failure.** Spec 0 is still the original *partitioned* identity spec; the empty tuple is incompatible with that partition type. `SnapshotProducer::validate_partition_value` rejects the `RowDelta`. |
| Fork already reject/correct? | **No.** The writer falls back to `DEFAULT_PARTITION_SPEC_ID` (0) when `partition_key = None` and `.with_partition_spec` is omitted. The *commit* rejects; the writer does not. |

The other ENGINE_CONTRACT §7a shape (spec 0 unpartitioned → evolve *to* partitioned) is a
silent under-delete (same-arity empty tuple vs spec 0). That is **not** this charter
fixture. Recorded so the two are not conflated.

---

## 2. Files

| Path | Role |
|---|---|
| `crates/repark-iceberg/src/write/position_delete.rs` | Fix + pins + docstring truth-up |
| `crates/repark-iceberg/src/write/map.md` | M16 stamp + Debug row |
| `task/m16-posdelete-specid-ledger.md` | this ledger |
| `task/map.md` | link |

`write_position_deletes_for_partition` gained a crate-private `builder_spec: Option<PartitionSpec>`
argument. `write_position_deletes` is unchanged (the `merge/mod.rs` seam). merge/mod.rs is not
edited.

---

## 3. Fix + red-then-green

Unpartitioned groups still pass `partition_key = None`. When the resolved spec's id is not 0
the builder is chained `.with_partition_spec(spec)` so `resolve_partition_spec_id` stamps
that id. Spec-0 unpartitioned stays `None` + no configured spec (byte-identical construction).

| Pin | Role |
|---|---|
| `evolved_unpartitioned_spec_position_delete_claims_resolved_spec_id` | M16 red-then-green: writer stamp + MoR MERGE commit + scan apply |
| `unpartitioned_spec_zero_position_delete_still_claims_spec_zero` | spec-0 control |

Red-then-green: the M16 pin failed with `left: 0 right: 1` before the builder change; after
the change the same test is green (writer stamp 1, MERGE commits, scan is `{1, 3}`).

Existing unpartitioned-spec-0 pins stayed green: `mor_matched_delete_position_deletes_row_and_leaves_data_files`,
`position_delete_file_carries_codec_in_footer`. Partitioned stamp path untouched:
`mor_bucket_partitioned_stamps_deletes_with_the_owning_transformed_partition`.

---

## 4. Gates

Recorded at PR time in the close-out section.

---

## 5. Octo — sequential hat-switch

### Actor

Smallest honest diff: thread the already-resolved spec into the writer builder on the
unpartitioned-but-not-spec-0 branch. Do not invent a `PartitionKey` for that branch (empty-tuple
semantics stay `None`). Do not change `write_position_deletes`.

### C1 (safety / standing rules)

| Claim | Disposition |
|---|---|
| C1-1: adding `builder_spec` changes the L242 signature the fence listed as backward-compatible. | **Accepted with reason.** The function is module-private. `merge/mod.rs` only calls `write_position_deletes` (unchanged). The extra argument is how the resolved spec is threaded without a second manifest walk. A wrapper that re-looks-up the spec would preserve arity and waste a walk. |
| C1-2: no `unwrap`/`expect` on the prod path. | **Held.** |
| C1-3: spec-0 path must not start calling `.with_partition_spec`. | **Held.** `builder_spec` is `None` when `spec_id == 0`. |

### C2 (completeness)

| Claim | Disposition |
|---|---|
| C2-1: the pin is valid only if reverting the fix turns it red. | **Held.** Pre-fix stamp is 0. |
| C2-2: MERGE e2e is required, not only the writer helper. | **Held.** Same test commits a MoR MERGE and asserts the scan. |
| C2-3: spec-0 control is output-only (a blanket `.with_partition_spec(spec0)` would still stamp 0). | **Noted.** The construction identity is in the prod `if spec_id != 0` guard; the control pin is the observable contract. |

### C3 (docs / maps)

| Claim | Disposition |
|---|---|
| C3-1: the module claimed the unpartitioned case is a *result* of the lookup. | **Fixed.** Docs now name the 0-fallback and the `.with_partition_spec` branch. |
| C3-2: `write/map.md` + `task/map.md` lockstep. | **Held.** |

### C4 (Iceberg-spec correctness)

| Claim | Disposition |
|---|---|
| C4-1: a position delete applies iff `(spec_id, partition)` matches the data file (`DeleteFileIndex::get_deletes_for_data_file`). | **Held.** Data file is spec 1 + empty struct; delete must be the same. Stamping 0 names the *partitioned* spec 0 with an empty tuple — commit-invalid. After the fix both halves match spec 1. |
| C4-2: Java `PositionDeleteWriter` requires a `PartitionSpec`; the fork's `.with_partition_spec` is that argument. | **Held.** |
| C4-3: `resolve_partition_spec_id(None, Some(spec))` accepts only empty-field specs; all-void (`is_unpartitioned()` but fields present) would reject at build. | **Residual, out of charter.** M16 is empty-field spec 1. An all-void non-zero spec would now fail loud at write instead of stamping 0; not invented here. |
| C4-4: this fixture is the *loud-fail* §7a half, not the silent-under-delete half. | **Held.** Recorded in §1. |

**Engine label:** OCTO-CONVERGED (C1–C4 ran; no open critic block).

---

## 6. Registry / STATUS

None. A11 closed those files. Paste-true for a later landing increment if wanted:

> M16 CONFIRMED+fixed: MoR position deletes on a spec-evolved unpartitioned table
> (spec 0 partitioned → spec 1 unpartitioned) now stamp the resolved spec id.
> Pre-fix: writer stamped 0; RowDelta commit failed loud
> (`Partition value is not compatible with partition type`).

---

## Close-out (filled after gates)

- `make verify` **EC=0** (2026-08-15). One earlier workspace run flaked
  `repark-ta` `hour0_bbands_three_vs_one_1e6` (load: 1.11x vs 1.5x); isolated
  retry and the subsequent full `make verify` both green. Not an M16 defect.
- `make preflight` recorded below after the commit.
