# Charter ledger — MW-0 · merge-on-read is written, not operated

**Date:** 2026-08-21 · **Branch:** `feat/mw-maintenance-wave` · **Base:** `1a36b72` (`main`,
post-#194) · **Design:** [../docs/design/iceberg-maintenance-wave.md](../docs/design/iceberg-maintenance-wave.md) ·
**Slate:** [../briefs/iceberg-maintenance-wave.md](../briefs/iceberg-maintenance-wave.md)

Three independent evaluations of whether merge-on-read is production grade converged on one
verdict from three different tree snapshots: **the write path is production-grade in correctness
terms, and MOR as a capability is not** — because the operational half is fenced off on exactly
the catalogs that hold production data. This ledger is the scope audit for closing that gap. It
is MW-0's entire product: the measurements below, and no product change.

Everything here was measured on `1a36b72` — against the built wheel for engine behavior, against
a live PySpark 4.1.2 oracle plus the shipping Iceberg 1.10.0 jar for Spark behavior. Nothing is
transcribed from the source evaluations without re-verification, and §5 records the two places
where re-verification changed the answer.

## 1. The gap, measured

Ten sequential MERGEs into a format-v2 table at `write.merge.mode = 'merge-on-read'`, 1,000 rows,
each merge touching the same 200 ids:

| MERGE | rows | delete files | `COUNT(*)` scan |
|---:|---:|---:|---:|
| 1 | 1000 | 1 | 153.6 ms *(cold)* |
| 2 | 1000 | 2 | 60.1 ms |
| 5 | 1000 | 5 | 94.6 ms |
| 10 | 1000 | 10 | 127.9 ms |

Delete files grow one per merge, strictly, and are never reclaimed. Scan cost tracks that growth
**2.1× from merge 2 to merge 10 on a table whose contents never change** (merge 1 is a cold-start
outlier — first scan of the session). Every scan returns exactly 1,000 rows, which is the
correctness half, measured rather than assumed.

Full ten-row table in design §3. MW-5 re-runs this identical demo and records the delta.

## 2. Procedure result schemas, measured before any pin is written

The oracle **runs**: an Iceberg Spark-4.0/Scala-2.13 1.10.0 runtime loads into the pinned PySpark
4.1.2 oracle and executes procedures. Two of four execute cleanly. `expire_snapshots` and
`remove_orphan_files` die on a Spark 4.0→4.1 binary break (`DataSourceV2Relation.create`'s
signature moved), so their schemas were read out of the shipping jar's own `OUTPUT_TYPE`
constant — the artifact, not the documentation.

| Procedure | Columns | Nullable | Source |
|---|---|---|---|
| `rewrite_data_files` | 5 | false | Executed |
| `rewrite_position_delete_files` | 4 | false | Executed |
| `expire_snapshots` | 6 (`bigint`) | **true** | `OUTPUT_TYPE` |
| `remove_orphan_files` | 1 (`orphan_file_location:string`), **one row per orphan** | — | `OUTPUT_TYPE` |

Full schemas in design §5.

## 3. Fork surface, confirmed

The four actions MW-1..MW-3 wire, read at the pin rather than assumed from the evaluations:

| Action | Surface | Consequence |
|---|---|---|
| `RewritePositionDeleteFiles` | `new(table)` → `filter(Predicate)` → `execute(&dyn Catalog)`; result exposes `rewritten_delete_files_count`, `added_delete_files_count`, `rewritten_bytes_count`, `added_bytes_count` | Matches Spark's measured schema **exactly** — MW-2 chooses nothing |
| `DeleteOrphanFiles` | `new(table)` → `location` / `older_than` / `prefix_mismatch_mode` / `equal_schemes` / `equal_authorities` / `delete_with` → `execute()`. Takes **no catalog**. Defaults: `older_than = now − 3 days`, `PrefixMismatchMode::Error`. Refuses up front when `gc.enabled` is false. Returns the full orphan list regardless of the deleter | No `dry_run` knob, but `delete_with` makes one — MW-3 is engine-side plumbing |
| `RewriteDataFiles` | already wired | Result omits `removed_delete_files_count` (see §4) |
| `ExpireSnapshots` + `ExpireSnapshotsCleanup` | already wired; `CleanupReport` carries `deleted_content_files`, `deleted_manifests`, `deleted_manifest_lists`, `deleted_statistics_files`, `failures` | `deleted_content_files` is a single funnel (see §4) |

## 4. Two disclosed divergences, one undisclosed

**Disclosed in code, absent from the registry.** `call.rs` documents both in its own doc tables,
with sound reasoning — the fork's `CleanupReport.deleted_content_files` funnels data,
position-delete, equality-delete and DV puffin files into one number, so `expire_snapshots`
reports four columns under Spark's names and omits the two it cannot honestly split; and
`rewrite_data_files` omits `removed_delete_files_count` because the fork does not expose
dangling-delete removal there. The module's stated rule is that counts are never fabricated, and
both decisions follow it.

They are **not new defects**. They are correctly-decided divergences filed in the wrong place:
the divergence registry is where parity divergences live *with pins*, and neither has a row or a
pin. MW-5 lands them.

**Undisclosed, found while measuring §2:** Spark declares `expire_snapshots`'s result columns
**nullable**; the engine pins them non-nullable. For the other two procedures Spark pins
non-nullable and the engine agrees, so this is not a blanket policy — it is one procedure out of
step. MW-1 fixes or registers it.

## 5. Where re-verification changed the answer

Recorded because the campaign's own discipline is that measured beats inherited:

1. **The intake called the `rewrite_data_files` column gap "an undeclared divergence."** It is
   declared — in `call.rs`'s doc table, with the reason. The finding survives only in the weaker
   and more accurate form in §4: declared in code, absent from the registry. This ledger corrects
   the intake, and the correction is why the claim was re-read rather than carried forward.
2. **The evaluations disagreed about the fence's nature.** Reading the execute paths settled it:
   nothing downstream of `refuse_non_local_catalog` assumes a local filesystem. The fence is pure
   policy, which is what makes MW-1 small.
3. **The service-side-maintenance race was overstated, by this orchestrator, from a secondhand
   citation.** The recommendation to keep the service-managed catalog fenced rested on a hazard
   whose primary source had not been read. The fork's engine contract §8 describes a **commit
   conflict**, not corruption: `validate_data_files_exist` trips and the commit fails. The
   validation is implemented fork-side. The owner's ruling to lift for both was therefore better
   supported than the recommendation against it, and MW-1 shrinks from "build a mitigation" to
   "document a failure mode".

## Propositions

| # | Clause | State | Evidence |
|---|---|---|---|
| C-201 | **The gap reproduces on this tree, and is quantified.** | PROVEN | §1 — ten merges, delete files 1→10 monotonic, scan 2.1× on constant contents, all on `1a36b72` against the built wheel. |
| C-202 | **Every expected Spark value comes from an independent source, never read back out of the engine.** | PROVEN | §2 — two procedures executed on a live oracle; two read from the shipping jar's `OUTPUT_TYPE`. The ledger records which is which per procedure. |
| C-203 | **The fence is policy, not capability.** | PROVEN | §5.2 — `refuse_non_local_catalog` inspects a `LocationPolicy` and returns; the execute paths take `&dyn Catalog` and reach storage through FileIO. No local-path assumption downstream. |
| C-204 | **No fork work is on the critical path.** | PROVEN | §3 — all four actions exist at the pin with the surfaces named, none local-only. `DeleteOrphanFiles` lacks a `dry_run` knob but `delete_with` supplies one, so even MW-3's stricter posture is engine-side. |
| C-205 | **MW-2's result schema is determined, not chosen.** | PROVEN | §2 + §3 — the measured Spark schema and the fork result type agree on names, order, and the int/bigint split. |
| C-206 | **MOR MERGE is not gated by the BUG-001 valve.** | PROVEN | `position_delete.rs` gates SQL `DELETE`/`UPDATE` under merge-on-read with an unpartitioned current spec and multiple specs in history, and states MERGE is never gated because the engine-owned MOR writer stamps per-file partitions correctly. MW-4's leg uses MERGE. |
| C-207 | **The existing result-schema divergences are decided, not defective.** | PROVEN | §4 — both disclosed in `call.rs` doc tables with reasoning consistent with the module's never-fabricate rule. The deliverable is registry rows, not fixes. |
| C-208 | **The campaign changes no query's answer.** | PROVEN | Every unit adds or unfences a maintenance procedure. No kernel, no planner, no type. The one behavioral change to an existing surface is `expire_snapshots`'s result nullability (§4), which is metadata, not a value. |
| C-209 | **No forbidden surface.** | PROVEN | MW-0 touches three new documents and two maps. No AWS credential or environment, no `Cargo.toml [patch]`, no `.github/` change, no lockfile edit. MW-4 needs an IAM change that **the owner executes** (OD-3) — the campaign never touches IAM itself. |
| C-210 | **The lifted fence exposes a conflict, not a corruption.** | PROVEN | Fork engine contract §8, read directly: service-side compaction on S3 Tables commits concurrently, so `CommitFailed` requirement mismatches are routine and `validate_data_files_exist` trips when the service rewrites a file an in-flight position delete references. That validation is implemented fork-side (`transaction/row_delta.rs`). The commit fails loudly; the table is not damaged. MW-1's obligation is to document the failure mode, not to build a mitigation. |

**OPEN clauses: none.** C-210 was drafted OPEN, on the assumption that lifting the fence left a
hazard needing a replacement mitigation. Reading the fork's engine contract closed it: the
hazard is a commit conflict that Iceberg's own validation already catches. §5.3 records the
correction.

## APPROVAL_GATE

**RULED 2026-08-21.** The owner ruled all four decisions and green-lit the campaign:

> "OD-1, lift for both. S3 Tables is arguably more important. OD-2 Yes, OD-3, yes. OD-4,
> greenlight. We need this right away."

| ID | Decision | Ruling |
|---|---|---|
| OD-1 | Lift the maintenance fence for the service-managed catalog too, or the explicit-location catalog only? | **Lift for BOTH.** This overrides the recommendation to keep the service-managed catalog fenced. On re-verification the recommendation was the weaker position: what the fence guarded against is a commit conflict the fork already catches, not corruption (§5.3, design §6). MW-1 documents the failure mode. |
| OD-2 | Orphan-files safety posture: dry-run default plus a minimum `older_than` floor. | **Adopt both**, and declare the divergence from Spark's more dangerous default as a registry row. |
| OD-3 | Extend the tier-2 acceptance role with scoped delete on the scratch prefix. | **Yes**, owner executes. Gates MW-4 and no other unit. |
| OD-4 | Campaign green-light and priority against the standing queue. | **Green-lit, start immediately.** |
| Q5 | The two procedures the 4.1.2 oracle cannot execute. | **Read the schema from the shipping jar's own constant** rather than installing a second oracle or asserting from documentation. Done — §2. |

The three questions this gate is read against:

1. **Is the change authorized?** Yes, and broader than recommended: both catalog policies, not
   one. Recorded as an override rather than a concurrence, so the reasoning behind the narrower
   recommendation stays legible if it is ever revisited.
2. **Does anything become silently riskier?** No. The concern that produced the narrower
   recommendation was a service-side-maintenance race, and the fork's validation already fails
   that commit loudly rather than corrupting the table (C-210). What remains is a failure mode an
   operator could find confusing, which the slate requires MW-1 to document.
3. **Is the destructive unit adequately fenced?** MW-3 inverts the usual defaults — required
   `older_than`, enforced floor, dry-run default, and a fixture that proves the armed run deletes
   the orphans *and provably not one live file*. The stricter-than-Spark posture is declared, not
   silent.

## Unit roster, as ruled

| Unit | Scope | State |
|---|---|---|
| **MW-0** | Charter, design, slate, measured floor, procedure schemas | **This commit** |
| **MW-1** | Lift the fence on the three existing procedures, both catalog policies; the nullability item; document the service-side conflict failure mode | Queued |
| **MW-2** | Wire `rewrite_position_delete_files` | Queued |
| **MW-3** | Wire `remove_orphan_files`, dry-run default, floor, registry row | Queued |
| **MW-4** | MOR leg in the tier-2 AWS acceptance | Queued behind OD-3 |
| **MW-5** | Registry rows, STATUS scorecard, guide + map lockstep, re-measured delta, close | Queued |

## Sizing

Six units, each at or under a typical parity-wave unit. MW-1 is the highest leverage and among
the smallest — it is a policy change plus tests. MW-3 is the highest danger and gets the
strictest contract. MW-4 is the only one that cannot be completed by the orchestrator alone.

**Without MW-4 the campaign is not done**: MOR would ship on unit-test evidence alone, and the
whole point of the wave is that the existing live evidence covers copy-on-write only.
