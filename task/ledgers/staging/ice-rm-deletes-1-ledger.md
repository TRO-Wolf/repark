# Charter ledger — ICE-RM-DELETES-1 · `CALL rewrite_manifests` rewrites delete manifests, honours `spec_id`

**Date:** 2026-09-20 (round 1, RePark half) · **Branch:** `fix/ice-rm-deletes-1` · **Base:** `ff5a13c3` · **Model:** muse-spark-1.3-contributor · **Policy:** [../../../AGENTS.md](../../../AGENTS.md).
**Path:** STANDARD. **risk_tier: standard.**
**Registry:** `docs/spark-sql-iceberg-parity.md` §7 (row only if a declared divergence survives; TBD).
**Fork half:** none — the opt-in (`RewriteManifestsAction::rewrite_delete_manifests`, fork #318) rides the pinned fork `44834673`; this round touches only the RePark side.

**Why now.** `crates/repark-spark/src/call/rewrite_manifests.rs` never calls the fork's
delete-manifest opt-in, so a MoR table's delete manifests stay put and the result counts
differ from Spark's; an explicit `spec_id` raises `NotImplemented`. The oracle is two
recorded Spark 4.1.2 cells files: `/tmp/oc-worker/pd-oracle/rewrite_manifests_truth.json`
(12 cells) and `/tmp/oc-worker/pd-oracle/rewrite_manifests_truth2.json` (2 cells), from
`record_rewrite_manifests.py` / `record_rewrite_manifests2.py`.

**Not in this unit:** RePark's partitioned-DELETE shape (it writes position deletes where
Spark's cells show copy-on-write consolidation — pre-existing, out of scope per the
ICE-RDF-OPTIONS-1 round-2 ledger); the in-flight fork `replace`-summary added-file-count
fix (different keys); `STATUS.md` (never edited by a unit).

## PROPOSITION LEDGER — ICE-RM-DELETES-1 — 2026-09-20

| Clause | Proposition (checkable) | Proof obligation | Verdict | Evidence / open question |
|---|---|---|---|---|
| C-001 | The CALL rewrites delete manifests; counts and layout equal the Spark MoR cells (v2+v3, partitioned+unpartitioned). | `call_rm_deletes.rs` + `test_ice_rm_deletes_1.py` replay pins vs the copied oracle JSONs. | **OPEN** | Skeleton. |
| C-002 | A table with no delete files behaves exactly as today (`no_deletes_*` cells). | Same pins, literal replay. | **OPEN** | Skeleton. |
| C-003 | `spec_id` is honoured (`part_mor_spec_*`, `evolved_spec_*` hold); unknown ids refuse. | Same pins + refusal pin. | **OPEN** | No cell covers unknown/non-current ids; refusal text follows the sibling `output-spec-id` shape, recorded in Rulings. |
| C-004 | `use_caching` keeps its current meaning (`part_mor_nocache_*`). | Same pins. | **OPEN** | Skeleton. |
| C-005 | Delete-only work no longer answers zeros. | Reworked delete-only pin (was the zeros-refusal test). | **OPEN** | Skeleton. |
| C-006 | Mutations bite: opt-in off reds C-001/C-005; `spec_id` refusal restored reds C-003. | Mutation runs recorded below. | **OPEN** | Skeleton. |

## Oracle analysis (measured 2026-09-20, from the JSONs + warehouse metadata)

- Every v2/v3 pair is byte-identical (`unpart_mor`, `part_mor`, `part_mor_spec`,
  `part_mor_nocache`, `no_deletes`, `evolved_spec`, `part_mor_real`).
- Manifest tuple columns (from the recorders): `content, partition_spec_id`,
  total data files, total delete files, added/existing data, added/existing delete.
- Result row is always `[manifests-replaced, manifests-created]`, matching the commit
  summary's `manifests-replaced` / `manifests-created` (`kept` is asserted too).
- `unpart_mor`: 3 data + 2 delete manifests rewrite to 1 + 1, result `[5, 2]`.
- `part_mor_real`: 4 data + 3 delete manifests (one empty) rewrite to 1 + 1, result
  `[7, 2]` — the empty delete manifest counts as replaced and leaves nothing behind.
- `part_mor` / `part_mor_spec` / `part_mor_nocache`: Spark ran the DELETE statements as
  copy-on-write (`deleted-data-files: 1`, zero delete files per delete snapshot), so the
  before-state is data-only and the CALL is data-only (`[3, 1]`). RePark's DELETE writes
  position deletes on the same DML, so replay pins assert Spark's two-leg semantics on
  RePark's before-state; rows agree on every cell.
- `evolved_spec`: default-spec CALL matches one empty manifest → `[0, 0]`, no new snapshot
  (`op` stays `delete`). This is the measured keep-a-lone-manifest rule the per-leg
  no-op mirrors.
- No cell passes a non-current or unknown `spec_id`; `part_mor_spec` passes the current id.

## Design (step 3)

- Turn on `rewrite_delete_manifests(true)`; gate `rewrite_if` per leg: a data manifest
  matches only when the data leg has work, a delete manifest only when the delete leg has
  work (same `count > 1 or bytes > target` rule per leg). Global no-op when neither leg
  has work. Delete `refuse_uncompactable_delete_manifests` (zeros are then honest).
- Explicit `spec_id` replaces the default-spec value in the predicate; unknown ids raise
  the sibling `output-spec-id` shaped refusal. `use_caching` stays parse-and-drop.

## Rulings

- R-SPEC-TEXT (self, 2026-09-20): no oracle cell passes an unknown `spec_id`, so Spark's
  exact wording for this procedure is unmeasured. The refusal mirrors the measured sibling
  `rewrite_data_files` text (`Cannot use output spec id 99 because the table does not
  contain a reference to this spec-id.`), which is Spark's own validation shape.

## Red-first

(TBD step 2.)

## Gates

(TBD step 4/6.)

## Mutations (C-006)

(TBD step 5.)

## Hand-back

(TBD step 6.)
