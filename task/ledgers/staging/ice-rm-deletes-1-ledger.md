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

> **Round-2 correction (ICE-META-DELETE-1, 2026-09-19).** The shape named on that first line
> is no longer RePark's: a DELETE covering whole data files is now answered from metadata, as
> Spark answers it, and **six of the nine cells this unit could not replay literally now equal
> the recorded Spark cell on every field.** The declared list below is corrected in the PR
> that closes them (#739). See [Corrected declared list](#corrected-declared-list--ice-meta-delete-1-2026-09-19).

## PROPOSITION LEDGER — ICE-RM-DELETES-1 — 2026-09-20

| Clause | Proposition (checkable) | Proof obligation | Verdict | Evidence / open question |
|---|---|---|---|---|
| C-001 | The CALL rewrites delete manifests; counts and layout equal the Spark MoR cells (v2+v3, partitioned+unpartitioned). | `call_rm_deletes.rs` + `test_ice_rm_deletes_1.py` replay pins vs the copied oracle JSONs. | **PROVEN** | 16/16 Rust + 16/16 Python green: unpartitioned cells literal (`(5, 2)`, 1+1 layout, rows, op, summary); partitioned cells on Spark's two-leg rule over RePark's before-state; `part_mor_real` `(7, 2)` both versions. **Corrected 2026-09-19 (ICE-META-DELETE-1 round 2):** the partitioned whole-file cells no longer need the rule — `part_mor`, `part_mor_spec` and `part_mor_nocache` (v2 and v3) are literal, and the pins assert the cell. The clause's own claim is unchanged and still PROVEN; only the evidence is. |
| C-002 | A table with no delete files behaves exactly as today (`no_deletes_*` cells). | Same pins, literal replay. | **PROVEN** | `no_deletes_v2/v3` literal on both doors (`(3, 1)`, 6-file layout, rows, op, summary); pre-existing data-only tests untouched and green. |
| C-003 | `spec_id` is honoured (`part_mor_spec_*`, `evolved_spec_*` hold); unknown ids refuse. | Same pins + refusal pin. | **PROVEN** | Current `spec_id => 0` runs like the default (named + positional, Rust + Python); evolved default stays `(0, 0)` with no new snapshot; non-current `spec_id => 0` rewrites spec 0 alone (`(3, 1)`, kept 3); unknown ids raise `IllegalArgumentException: Invalid spec id 99` (Spark's recorded text, registry MANIFEST-2). **Corrected 2026-09-19 (ICE-META-DELETE-1 round 2):** `part_mor_spec_*` is now literal against the recorded cell, and the non-current pin keeps 1, not 3 — the two delete manifests it used to keep are no longer written. |
| C-004 | `use_caching` keeps its current meaning (`part_mor_nocache_*`). | Same pins. | **PROVEN** | `part_mor_nocache_v2/v3` answer like the cached shape on both doors; quoted `use_caching` still refuses (unchanged, registry-declared). **Corrected 2026-09-19 (ICE-META-DELETE-1 round 2):** that shared answer is now `(3, 1)`, the recorded Spark cell, where it was `(5, 2)` over RePark's own before-state. The clause — `use_caching` changes nothing — is what it always was, and both cells still answer identically. |
| C-005 | Delete-only work no longer answers zeros. | Reworked delete-only pin (was the zeros-refusal test). | **PROVEN** | 1 data + 3 delete manifests answer `(3, 1)` with the data manifest kept and one delete manifest after, rows kept (Rust `merges_the_delete_leg_alone`). |
| C-006 | Mutations bite: opt-in off reds C-001/C-005; `spec_id` refusal restored reds C-003. | Mutation runs recorded below. | **PROVEN** | M-A: 10 rm_deletes + 2 call_manifests pins red, rest green. M-B: 4 rm_deletes + 1 call_manifests pin red, rest green. Both reverted green. |

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
- `part_mor` / `part_mor_spec` / `part_mor_nocache`: Spark's delete snapshots carry
  `deleted-data-files: 1` and zero delete files, so the before-state is data-only and the
  CALL is data-only (`[3, 1]`). RePark's DELETE wrote position deletes on the same DML, so
  replay pins asserted Spark's two-leg semantics on RePark's before-state; rows agreed on
  every cell. **Corrected 2026-09-19 (ICE-META-DELETE-1 round 2):** the reading of Spark's
  side was one step off — those snapshots are not copy-on-write rewrites but METADATA
  deletes, which is why they carry no added file at all. RePark now takes the same route and
  all six cells are literal.
- `evolved_spec`: default-spec CALL matches one empty manifest → `[0, 0]`, no new snapshot
  (`op` stays `delete`). This is the measured keep-a-lone-manifest rule the per-leg
  no-op mirrors. **Corrected 2026-09-19:** this cell's before-state differs from RePark's for
  a SECOND reason, unstated here at the time and still live — Spark holds its five live
  spec-0 data files in ONE manifest, RePark in three. The merge happens on Spark's APPEND
  after the partition evolution (`manifests-created: 2, manifests-kept: 0,
  manifests-replaced: 3`), not on either DELETE. Registry row **MANIFEST-4**, the
  `TP-MANIFEST-MIN-MERGE` half of IPI-11.
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
- R-SPEC-TEXT-2 (self, 2026-09-20, supersedes R-SPEC-TEXT): the registry's MANIFEST-2 Spark
  column records this procedure's own measured wording — `Invalid spec id 7` — so the
  refusal is `Invalid spec id {id}` via the existing `IllegalArgumentException` path, not
  the sibling text. The registry also confirms the non-current semantic the implementation
  chose (`spec_id` selects the rewritten spec). Registry MANIFEST-1 → FIXED, MANIFEST-2
  spec half FIXED in the same change.

## Red-first (unfixed tree `f928a0ce` + pins only, 2026-09-20)

Rust (`cargo test -p repark-spark rm_deletes`): `12 failed, 4 passed`. The 4 passes are
`rm_deletes_no_deletes_v2/v3` and `rm_deletes_evolved_spec_v2/v3` — already-correct
behavior kept as regression guards. Every MoR result pin fails data-only vs both-legs
(`(3, 1)` vs `(5, 2)`, `(3, 1)` vs `(6, 2)`); both spec pins fail on the `NotImplemented`
refusal. The before-layout assertions in the failing tests all pass, so RePark's replay
before-states are as the pins assume.
Rust (`cargo test -p repark-spark rewrite_manifests`): 3 reworked pins fail for the named
reasons — the delete-only shape hits the zeros-refusal, `spec_id => 0` hits the
`NotImplemented` refusal, the MERGE shape answers `(4, 1)` instead of `(7, 2)`.
Python (`test_ice_rm_deletes_1.py`): `12 failed, 4 passed`, mirroring Rust exactly
(same 4 passes: `no_deletes`, `evolved_spec` per version).

## Green (fixed tree `06b95b15`, 2026-09-20)

- Rust `cargo test -p repark-spark rm_deletes`: `16 passed, 0 failed` (the v3
  `part_mor_real` replay carries one empty delete manifest, as Spark's recorded before
  does, and answers `(7, 2)` with a two-file delete manifest after — pinned literally).
- Rust `cargo test -p repark-spark rewrite_manifests` (the brief's gate command):
  `11 passed, 0 failed` (8 in `call_manifests`, 2 new inline leg-rule pins, 1
  maintenance planner pin).
- Release native rebuilt (`maturin develop --release`, 7m34s) +
  `pytest python/repark/tests/test_ice_rm_deletes_1.py`: `16 passed`.
- `pytest python/repark/tests/test_maintenance_call.py -k rewrite_manifests`: `3 passed`.

## Gates (final tree, 2026-09-20)

- `cargo test -p repark-spark rm_deletes`: `16 passed, 0 failed`.
- `cargo test -p repark-spark rewrite_manifests` (the brief's gate command):
  `11 passed, 0 failed`.
- `pytest python/repark/tests/test_ice_rm_deletes_1.py
  python/repark/tests/test_maintenance_call.py`: `31 passed` (release native rebuilt
  after the refusal-text switch).
- `cargo fmt --all`: clean (applied twice: the step-2 pin formats, one step-3 hunk).
- `cargo clippy --locked -p repark-spark --all-targets -- -D warnings
  -A clippy::disallowed_methods` (the Makefile `rust-clippy` form): clean. The brief's
  literal form without `-A` reds only on pre-existing test-target `expect` hits
  (documented since ICE-RDF-OPTIONS-1 round 2); the one new finding it surfaced in my
  files (`sort_unstable` on primitive tuples) is fixed. No new findings.
- `python3 /tmp/oc-worker/_lib/comment_ban.py /tmp/qa-bs origin/main`:
  `comment-ban hits=0`.
- `python3 scripts/check_ledger_grammar.py`: 224 live ledgers clean.
- `python3 scripts/sync_map_md.py --check`: 309 maps clean (new registry anchors resolve).
- Ruff `check` + `format --check` (pinned `ruff@0.15.22`): clean on both touched Python files.

## Mutations (C-006, 2026-09-20, both reverted green)

- M-A (`rewrite_delete_manifests(true)` → `false`): `cargo test -p repark-spark rm_deletes`
  goes `10 failed, 6 green` — every MoR result pin reds (unpartitioned, partitioned,
  spec-selected, nocache, real shapes), while `no_deletes`, `evolved_spec`, non-current
  spec and unknown spec stay green. `call_manifests` goes `2 failed, 6 green`: exactly
  `merges_the_delete_leg_alone` and `merges_both_legs` red. C-001 and C-005 bite.
- M-B (early `NotImplemented` refusal restored for any present `spec_id`):
  `rm_deletes` goes `4 failed, 12 green` — exactly `part_mor_spec_v2/v3`,
  `non_current_spec_rewrites_that_spec` and `unknown_spec_id_refuses` red;
  `call_manifests` goes `1 failed, 7 green` — exactly `argument_surface_is_sparks` red.
  C-003 bites.
- Revert check: each mutation was reverted with the inverse edit; after the M-B revert
  the working tree `git diff` is empty and `rm_deletes` is `16 passed` again.

## Corrected declared list — ICE-META-DELETE-1, 2026-09-19

This unit stated that its partitioned replay pins assert Spark's rule over RePark's own
before-state, and named the cause. It did not write the list of affected cells down. It is
written here, corrected to what #739 measured, because #739 is the PR that closes most of it.

**Nine of the fourteen cells were not literal when this unit landed** (not ten: the
`part_mor_real_v3` replay was literal from the start, as the Green section below records).
Their state after the metadata-delete routing:

| Cell | Then | Now | Grounds |
|---|---|---|---|
| `part_mor_v2`, `part_mor_v3` | rule over RePark's before-state (`(6, 2)`, six manifests) | **CLOSED** — literal `(3, 1)`, `[(0,0,1,0)×3]` → `[(0,0,3,0)]`, rows `[2,3,6]`, `replace`, `1/0/3` | the three DELETE statements each cover a whole data file |
| `part_mor_spec_v2`, `_v3` | rule (`(5, 2)`, five manifests) | **CLOSED** — literal `(3, 1)`, `[(0,0,1,0),(0,0,1,0),(0,0,2,0)]` → `[(0,0,4,0)]`, rows `[2,3,5,6]`, `replace`, `1/0/3` | same |
| `part_mor_nocache_v2`, `_v3` | rule (`(5, 2)`) | **CLOSED** — same cell as `part_mor_spec`, literal | same |
| `evolved_spec_v2`, `_v3` | rule: six manifests including two delete manifests | still DECLARED, narrower — four manifests, data-only, `(0, 0)` and the rows equal Spark; Spark holds the five spec-0 files in one manifest and RePark in three | registry **MANIFEST-4** (new), the merge-on-commit half of IPI-11 |
| `part_mor_real_v2` | rule: three one-entry delete manifests, after `(1,0,0,3)` | unchanged — those DELETE statements are partial matches and keep the row-level route | registry **ICE-META-DELETE-1-D1** — Spark rewrites the superseded position-delete file, RePark adds a second one. Never named in this ledger before; named now |

`unpart_mor_v2/v3`, `no_deletes_v2/v3` and `part_mor_real_v3` were literal then and are
literal now. The full round-2 measurement, including the rows read before and after every
CALL, is in [ice-meta-delete-1-ledger.md](ice-meta-delete-1-ledger.md) clause C-009.

```yaml
COVERAGE_ATTESTATION:
  pr_unit: ice-rm-deletes-1
  categories:
    - id: AT-1
      status: ATTACKED
      evidence: Walked C-001..C-006 against the diff and the runs. Every oracle success cell asserts result, manifest layout, live row ids, and op; the summary manifest keys are asserted in Rust; error cells assert class plus text end to end on both doors.
      artifacts: [crates/repark-spark/src/tests/call_rm_deletes.rs, python/repark/tests/test_ice_rm_deletes_1.py, python/repark/tests/ice_rm_deletes_1_spark_oracle.json, python/repark/tests/ice_rm_deletes_1_spark_oracle2.json]
    - id: AT-2
      status: ATTACKED
      evidence: Empty table (no snapshot), evolved specs (default and non-current ids), unknown spec 99, positional spec, quoted use_caching, single-manifest legs, the v3 empty delete manifest, and the above-target MANIFEST-3 shape (pre-existing, untouched and still green) all pinned.
      artifacts: [crates/repark-spark/src/tests/call_rm_deletes.rs, crates/repark-spark/src/tests/call_manifests.rs]
    - id: AT-3
      status: ATTACKED
      evidence: The unknown-spec refusal fires after table load but before any commit; the evolved zeros pin asserts the snapshot count does not move; every refusal path commits nothing by construction (validation precedes the transaction).
      artifacts: [crates/repark-spark/src/call/rewrite_manifests.rs, crates/repark-spark/src/tests/call_rm_deletes.rs::rm_deletes_evolved_spec_v2]
    - id: AT-4
      status: ATTACKED
      evidence: Argument-shape errors surface before table load (unknown names, excess positionals, mistyped values); spec validation runs after load but before snapshot work; a missing table still reports before any spec check.
      artifacts: [crates/repark-spark/src/call/rewrite_manifests.rs]
    - id: AT-5
      status: N/A
      justification: No privilege, secret, or trust boundary is touched; the new argument is an integer id that flows only into the existing spec lookup and refusal text, the same sinks every CALL argument already uses.
    - id: AT-6
      status: ATTACKED
      evidence: Every success pin asserts live row id lists (not just counts) plus per-manifest file counts, so a dropped, duplicated, or unmasked row reds the suite; the rewrite itself is unchanged fork code.
      artifacts: [crates/repark-spark/src/tests/call_rm_deletes.rs, python/repark/tests/test_ice_rm_deletes_1.py]
    - id: AT-7
      status: ATTACKED
      evidence: One extra byte counter per manifest in the existing manifest-list scan; no new loops, scans, or retained state; the commit path is the unchanged fork action in a single transaction.
      artifacts: [crates/repark-spark/src/call/rewrite_manifests.rs]
    - id: AT-8
      status: ATTACKED
      evidence: Every expectation was measured from the recorded Spark cells or the registry's recorded Spark text (`Invalid spec id 7`); the two spots no cell covers (unknown-spec wording, non-current-spec counts) are disclosed in R-SPEC-TEXT-2 and the non-current pin asserts the RePark-observed answer, never a presumed Spark one.
      artifacts: [task/ledgers/staging/ice-rm-deletes-1-ledger.md, docs/spark-sql-iceberg-parity.md]
    - id: AT-9
      status: ATTACKED
      evidence: The unknown-spec path carries Spark's verbatim diagnostic (`Invalid spec id 99`) with `IllegalArgumentException` class on both doors; result columns and summary sourcing are unchanged.
      artifacts: [crates/repark-spark/src/tests/call_rm_deletes.rs::rm_deletes_unknown_spec_id_refuses, python/repark/tests/test_ice_rm_deletes_1.py::test_unknown_spec_id_refuses]
    - id: AT-10
      status: ATTACKED
      evidence: Red-first held both ways: 12 Rust + 12 Python pins failed pre-fix for the named gaps while the already-correct pins stayed green; M-A reddens exactly the C-001/C-005 pins and M-B exactly the C-003 pins, both reverting green.
      artifacts: [crates/repark-spark/src/tests/call_rm_deletes.rs, python/repark/tests/test_ice_rm_deletes_1.py]
  complete: true
```

## Hand-back

(TBD step 6.)
