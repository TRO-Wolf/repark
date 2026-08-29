# Charter ledger — RP-2 · fork repin (F-13, F-7 U1+U2, F-3)

**Date:** 2026-08-27 · **Branch:** `feat/rp-2-fork-repin` (built by the OpenCode lane; salvaged
as `feat/rp-2-salvage` on 2026-08-28) · **Base:** `06a3e42` (`main`, post-#250) · **Policy:**
[../../../AGENTS.md](../../../AGENTS.md) "Version-pin contract" · **Handoff:**
[../../roadmap/mid-term/iceberg-rust-handoff-2026-08-23.md](../../roadmap/mid-term/iceberg-rust-handoff-2026-08-23.md)
§5 (the repin protocol) · **Path:** STANDARD (code changes; one Actor cycle, one Critic pass).

**Retires:** moved to `completed/` in this unit's departure commit.

**Why now.** The engine pin `5e7b2e4` is 15 fork commits behind fork `main`
`ce92a7bfe2c1be569ed0de1178ed410e8ec3a117`, and that range closes work every engine document
still lists as blocking: **F-13** (Puffin deletion-vector write path — fork #219, #221, #222;
row R114 ✅ 2026-08-24), **F-7 U1+U2** (row lineage through `RewriteFiles`, Java
`first_row_id` suppression, manifest-list ordering — fork #225, #226; row R166 ✅ 2026-08-25),
and **F-3** (`remove-dangling-deletes` composed into `RewriteDataFiles` — row R135,
2026-08-23). At the original 2026-08-27 draft, the north star, STATUS, and slate all treated
F-13 as V3-3's only fork gate. The 2026-08-28 ruling below corrects that premise. This unit takes
the pin, measures what the new rev makes true on the engine's own
surfaces, and flips exactly the pins the evidence supports. Not in this unit: DV writes
behind a new engine surface (that is V3-3, chartered from this unit's §2 measurement), F-14,
F-15, F-16, and any DataFusion family move.

**Owner ruling, 2026-08-28 — salvage the guarded increment.** RP-2 keeps the `ce92a7bf` repin
and only the capabilities its pins prove. A first MOR DELETE on a DV-free v3 table may commit a
Puffin DV. Any table carrying a live DV refuses DELETE before a write, including a second engine
DELETE and the Spark shared-Puffin fixture. COW DELETE may lift if its lineage pin is Spark-equal;
COW UPDATE and MERGE stay guarded. `rewrite_data_files` stays guarded after its measured lineage
reassignment. F-3 may land independently. Fork F-17 and a later RP-3 own shared-Puffin closure,
DV merge and supersession, and the complete DV input-state matrix. The same-day full-batch
amendment (#254, merged 2026-08-28 as `6d75b78`; target `26088bb`, twelve clauses) is superseded
by this ruling: its four added clauses — C-009 F-16 measured, C-010 F-9 taken, C-011 F-7 U3
measured, C-012 F-15 carried — leave this ledger and transfer unchanged to RP-3's charter, which
takes the whole post-`ce92a7bf` batch (F-14 and F-17 included) at one frozen fork SHA. Their
text stays readable at `6d75b78`.

## PROPOSITION LEDGER — RP-2 — 2026-08-27

| Clause | Proposition (checkable) | Proof obligation | Verdict | Evidence / open question |
|---|---|---|---|---|
| C-001 | Every `iceberg*` `[patch.crates-io]` rev is `ce92a7bfe2c1be569ed0de1178ed410e8ec3a117` and `Cargo.lock` resolves to it; `datafusion`, `datafusion-spark`, `arrow*`, `parquet` and `rust-toolchain.toml` are byte-identical to `main` at `06a3e42` (the fork's family is still arrow/parquet 58.4). | `rg` on the workspace `Cargo.toml` + lock source entries; `git diff main -- Cargo.toml rust-toolchain.toml` empty outside the five revs. | **PROVEN** | commit `664701e`: five revs `ce92a7bfe…`; lock has 6 fork sources (incl. `iceberg-sketches`) at `ce92a7bf`, zero `5e7b2e4`; `cargo check --locked --workspace` exit 0 (1m29s). |
| C-002 | The two standing repin duties hold on the new rev: `NamespaceScopedCatalog` forwards every required `Catalog` method (defaulted ones forwarded or an omission stated), and the metadata-projection shim is kept iff the fork's metadata-table `scan` still ignores `projection`; the two metadata-table emptiness pins pass. | Trait diff at the new rev; read fork `metadata_table.rs`; `cargo test` the two named pins (`repark-sql/tests/introspection.rs`, `repark-spark/src/tests/metadata_tables.rs`). | **PROVEN** | The 97-file range touches NO `Catalog` trait file and no `metadata_table.rs` (the only catalog change is a comment line in `memory/catalog.rs`). 14 required + 16 defaulted stand; shim stays. `introspection` 5 passed; `metadata_tables_are_hidden…spark_door` passed. |
| C-003 | **F-13 measured under the narrowed guard.** A first MOR `DELETE` on a DV-free v3 table commits a Puffin deletion vector and reads back Spark-equal on both SQL doors and the facade. A second engine DELETE and the Spark-written shared-Puffin fixture refuse before a write. No document or pin claims DV merge or supersession. | First-delete `.delete_files` content and Spark read-back; one success pin per door; committed second-delete and shared-Puffin refusal pins with unchanged metadata and object sets. | **PROVEN** | First delete pinned per door — Spark `adopted_v3_mor_delete_commits_a_puffin_deletion_vector`, ANSI `adopted_v3_mor_first_delete_commits_a_deletion_vector_and_a_second_refuses`, facade `test_facade_v3_mor_first_delete_commits_a_deletion_vector_and_a_second_refuses` — with the Spark read-back in §3. The second DELETE refuses on all three doors (`adopted_v3_mor_second_delete_refuses_while_a_deletion_vector_is_live` on the Spark door, the ANSI and facade pins above) naming `1 live deletion vector`, with snapshot, pointer, lineage counters, rows, live-DV set and object count unchanged; the Spark shared-Puffin fixture refuses (`ansi_cow_delete_on_a_dv_carrying_v3_table_refuses`) with snapshot, object set and Spark's live set unchanged. `rg 'merge \+ supersede|supersede on re-delete'` finds nothing live. Fresh execution §7. |
| C-004 | **F-7 U1 measured and guarded.** `CALL system.rewrite_data_files` on the v3 fixture is measured at the new rev. The measured lineage reassignment is recorded and `V3-LINEAGE-1` stays armed. | Before/after lineage projection through the fork's R166 read path; Spark read-back; the existing refusal pin remains green. | **PROVEN** | Measured RED: 12 single-row INSERT statements → `rewrite_data_files` committed (rewritten 12, added 1) and Spark read `_row_id` 0..11 → 12..23 with `_last_updated_sequence_number` → 13. The guard stays; transcript in §3. Pin unchanged: `call_rewrite_data_files_refuses_a_v3_table_rather_than_reassigning_row_lineage`. RP-3 re-measures at its frozen SHA. |
| C-005 | **F-7 U2 measured on the COW path.** COW DELETE may lift only if the adopted-v3 lineage projection is Spark-equal. COW UPDATE and MERGE remain guarded in this unit. | V3E-1 driver re-run at the new rev; Spark read-back; one COW DELETE disposition and committed pre-write refusals for UPDATE and MERGE. | **PROVEN** | Scratch: COW `DELETE id=2` on a DV-free v3 table; Spark reads survivors' `_row_id`/seq byte-identical to pre-delete; `next_row_id` 5 = Spark's own allocate-then-suppress counter (live oracle 2026-08-27). COW DELETE lifted per door; UPDATE and MERGE refusal pins stay on all three doors. |
| C-006 | **F-3 taken.** `CALL system.rewrite_data_files(..., 'remove-dangling-deletes' => true)` on both doors passes the option to the fork and reports a true `removed_delete_files_count` instead of the hard-coded `0`; default stays `false` (Java's); the `V3-DANGLE-1` queue row is measured on the v3 fixture and dispositioned. | The option through `call.rs`; a 2-file position-delete fixture where the count is non-zero; the pin that asserted `0` retargeted. | **PROVEN** | `'remove-dangling-deletes' => true` accepted (quoted-name CALL grammar, `CallArgs`); fork GC composed; partitioned v2 fixture reports a true count with live rows intact. The unpartitioned-single-spec early return measured (Java-faithful). Pin: `call_rewrite_data_files_remove_dangling_deletes_reports_a_true_count`. The v3 half stays unreachable (C-004 guard). |
| C-007 | The documents say only what the narrowed pins prove: first-delete support, every live-DV state guarded, `rewrite_data_files` guarded, and COW UPDATE/MERGE guarded. The north-star plan points shared-Puffin closure to F-17 and full DV support to RP-3. | `rg 'merge.*superseded|gated on fork F-13'` reviewed; `make check-map-sync`, `check-docs-compaction`, and `check-ledger-grammar` green. | **PROVEN** | Registry `V3-COW-1` rewritten (one measured DELETE lifts; the 2026-08-25 ruling's refusals kept; F-17 / RP-3 named), `V3-LINEAGE-1` re-measured note, `V3-DANGLE-1` queue note; north star §3 MOR / COW / rewrite rows dated, §4 lanes current; STATUS v3 workstream; the slate (RP-2 leaves, RP-3 chartered); the handoff marks F-3 / F-7 U1+U2 / F-13 taken and F-14 / F-16 / F-9 / F-15 / F-17 landed with fork PR and date; crate maps, test maps and `docs/fork-sync.md` in lockstep. `make check-map-sync`, `check-docs-compaction`, `check-ledger-grammar` green. |
| C-008 | Green on the whole surface: branch placeholders and duplicate headings are removed, provenance is accurate, `make preflight`, the parity suite, and the V3E-3/V3E-4 fixture pins pass at the new rev; the one-page note lists every fork BEHAVIOR/BREAKING change in the range. | Branch diff review, gate output, and the note in this ledger's §4. | **PROVEN** | Duplicate `## 2` / `## 3` headings and the "filled at readiness" stub removed; provenance stated (OpenCode-lane build, Fable salvage on 2026-08-28); `make ci`, `make verify` (workspace `cargo test`), `make preflight` (facade suite) and the parity suite exit 0 at departure — §8; §4 lists #216 / #217 / #221 / #222 / #226 with the absorbing engine site and routes #227 / #232 / #233 to RP-3. |

VERDICT: PASS (OPEN=0, REJECTED=0). LOGIC_SCORE = 8/8.

## 2. Sequence

1. Pickup ritual (`make ledger-archive`, drift checks), then the repin commit (C-001) alone —
   the compile is the first measurement.
2. Standing duties (C-002), then the narrowed measurements (C-003, C-004, C-005) before any
   guard moves; each writes its Spark read-back into this ledger.
3. Add the second-DELETE and shared-Puffin refusal pins; flip only the COW DELETE seat if its
   measurement is Spark-equal; take F-3 (C-006).
4. Truth-up (C-007), gates (C-008), Critic pass with a novel input through each door whose
   guard lifted, departure commit.

## 3. Measurements (verbatim oracle evidence, 2026-08-27, zulu-17, PySpark 4.1.2 + Iceberg 1.11.0)

**C-003 (MOR DELETE → Puffin DV).** Engine: `CREATE … TBLPROPERTIES('format-version'='3',
'write.delete.mode'='merge-on-read')`, 6 rows over 2 partitions, `DELETE id = 1`, `DELETE id = 5`
→ live rows `[(2,"b",0),(3,"c",0),(4,"d",1),(6,"f",1)]`; live delete files `[("Puffin", 2)]`.
Spark: `register_table` of the committed pointer →
`{"rows": [{"id": 2…}, {"id": 3…}, {"id": 4…}, {"id": 6…}]}`. This scratch run predates the
guard: the second DELETE succeeded only because every engine-written Puffin holds one blob, so
there was no sibling to lose. It is **not** a claim of DV merge or supersession — the shipped
guard refuses the second DELETE, and RP-3 owns that cell.

**C-004 (rewrite lineage, RED).** rw_t: 12 single-row INSERT statements (snapshots 1..12) → before-pointer
00012; `CALL rewrite_data_files` → commit 00013. Spark direct Hadoop load of both state copies:
before lineage `{1:[0,1], 2:[1,2], …, 12:[11,12]}`; after `{1:[23,13], 2:[22,13], …, 12:[12,13]}`;
`lineage_equal: false`, `rows_equal: true`. Guard stays.

**C-005 (COW lineage, GREEN).** cow_t: 6 single-row INSERT statements, `DELETE id = 2`: Spark before
`{1:[0,1], 2:[1,2], 3:[2,3], 4:[3,4], 5:[4,5], 6:[5,6]}` → after `{1:[0,1], 3:[2,3], 4:[3,4],
5:[4,5], 6:[5,6]}` — survivors byte-identical. Spark's OWN COW delete on the same recipe leaves
`next-row-id = 5`; the engine's counter reads exactly 5 (allocate-then-suppress, #226).

**C-006 (F-3).** Partitioned v2 MOR fixture: 6 INSERT statements (one partition), `DELETE id = 2`,
`CALL rewrite_data_files('remove-dangling-deletes' => true)` → `removed_delete_files_count ≥ 1`,
live rows 5. Unpartitioned single-spec variant measures the Java-faithful early return (count 0).

## 4. What changed under us

| Fork PR | Change | Engine site that absorbs it |
|---|---|---|
| #216 (F-3) | `RewriteDataFiles` composes `remove-dangling-deletes`; `RewriteDataFilesResult` gains `#[non_exhaustive]` | `call.rs::execute_rewrite_data_files` — option accepted (quoted-name CALL grammar), `.remove_dangling_deletes(…)`, true count via `count_as_i32` |
| #217 (F-5) | `ReplacePartitions` `dataSpec()` scope branches (BEHAVIOR) | No engine call site constructs `ReplacePartitions` — `overwrite.rs` stage-then-swap uses `OverwriteFiles`; absorbed by the green suite (overwrite + writer pins) |
| #221 (F-13 U2+U3a+U4) | V3 MOR writes deletion vectors (BREAKING API + BEHAVIOR) | Passthrough DELETE seat: DV-free v3 tables commit DVs; guard narrowed to refuse DV-carrying tables (F-rp2-1) |
| #222 (F-13 U3b) | Row-lineage read path + variant Arrow type (3 BREAKING) | `FileScanTask` gained `first_row_id` / `file_sequence_number` — `file_scoped_rewrite.rs` test constructor updated (`None`); variant type unused engine-side |
| #226 (F-7 U1+U2) | Java `first_row_id` suppression + manifest-list ordering (BEHAVIOR) | COW DELETE survivor lineage now Spark-clean (measured); `RewriteDataFiles` still reassigns (measured RED — guard stays) |
| #227, #232, #233 | F-7 U3, F-16 + DV removal accounting, F-9 + F-15 | Beyond `ce92a7bf` — RP-3's range (the #254 clauses) |

## 5. Gates

- `cargo test --locked --workspace` — 46/46 test targets ok (final run after F-rp2-1 remediation).
- `make ci` / `make preflight` — exit 0 at departure (readiness).

## 6. Findings

FINDING: F-rp2-1
  severity: S0 (silently-wrong-results class, caught pre-merge)
  category: AT-1 / AT-2
  clauses: [C-003]
  description: With the MOR lift unscoped, `DELETE` on the DV-carrying partitioned-DV fixture
    (ANSI door, full-suite run) resurrected DV-deleted row id 5 — live rows became
    {3,4,5,6} where Spark's live set is {3,4,6}. Mechanism (diagnosed 2026-08-28, fixed in the
    fork as F-17, #237): the Spark Puffin holds two DV blobs; the engine's DELETE touched the
    `part=0` data file, superseded the container by path and dropped the untouched `part=1`
    sibling blob.
  disposition: REMEDIATED — the passthrough seat now counts live deletion vectors and refuses
    the DELETE when any exist (message names the measurement); regression pin
    `ansi_cow_delete_on_a_dv_carrying_v3_table_refuses` (suite red before, green after).

FINDING: F-rp2-2
  severity: S3
  category: AT-6
  clauses: [C-003]
  description: The charter's C-003 wording said "content 2, `referenced_data_file` set"; the
    committed DV is manifest content `PositionDeletes` in Puffin format per the fork's model.
  disposition: WITHDRAWN as a defect — the charter paraphrased the `.delete_files` content
    column; the measurement records the observed shape. No code change.

```yaml
CONTEXT_BREAK:
  id: CB-rp-2-fork-repin-1
  mechanism: PROCEDURAL_IN_SESSION
  manifest_binding: context_break_mechanics procedural default; CCC review-only
  handed_to_critic: [unit_charter_clauses, diff_and_artifacts, test_results, "attack_taxonomy (ref 05 + CCC)"]
  withheld_until_initial_findings_filed: [actor_build_summary, actor_self_logic_review]
  declaration_logged: "Context break executed; attacking artifacts, not memory."
  honesty_note: procedural, not amnesia; compensated per s0_fresh_execution below
```

```yaml
COVERAGE_ATTESTATION_CYCLE_1:
  pr_unit: rp-2-fork-repin
  cycle: 1
  risk_tier: high
  critic_engine: ccc
  complete: true
  note: >
    Procedural in-session CCC quad over the worktree diff. Full suite
    46/46 Rust targets + facade suite green after remediation. Fresh
    executions below. No OPEN finding >= S1.
  fresh_executions:
    - input: partitioned DV-free v3 table, rows (10..14), facade .sql() "DELETE FROM ice.sales.crit WHERE id >= 12"
      entry_point: facade .sql() on the built module
      observed: live rows [10, 11] — the multi-row, two-partition delete committed correctly
      note: the probe's printed "expected" label was wrong (listed 13, 14 as survivors);
        recomputed expectation [10, 11] matches the observed output
    - input: facade .sql() "UPDATE ice.sales.crit SET part = 9 WHERE id = 10" on the same table
      entry_point: facade .sql() on the built module
      observed: UnsupportedOperationException naming V3-COW-1 (refusal, no commit)
  categories:
    - id: AT-1
      status: ATTACKED
      artifacts: [Cargo.toml, Cargo.lock, docs/fork-sync.md, task/ledgers/staging/rp-2-fork-repin-ledger.md]
    - id: AT-2
      status: ATTACKED
      artifacts: [crates/repark-spark/src/tests/v3_cow.rs, crates/repark-sql/src/v3_cow.rs, python/repark/tests/test_v3_cow_dml.py, crates/repark-spark/src/tests/call_rewrite_dangling.rs]
    - id: AT-3
      status: ATTACKED
      artifacts: [crates/repark-iceberg/src/write/row_lineage_guard.rs, crates/repark-spark/src/call.rs]
    - id: AT-4
      status: ATTACKED
      artifacts: [crates/repark-iceberg/src/write/row_lineage_guard.rs]
    - id: AT-5
      status: ATTACKED
      artifacts: [git diff origin/main...HEAD — no .github/, no AWS/IAM, no secrets; [patch] revs only]
    - id: AT-6
      status: ATTACKED
      artifacts: [docs/spark-sql-iceberg-parity.md, task/roadmap/epic-term/v1-0-iceberg-v3-northstar.md, task/roadmap/mid-term/iceberg-rust-handoff-2026-08-23.md, STATUS.md]
    - id: AT-7
      status: N/A
      justification: no performance claim; the guard's manifest walk is the same cost class as the existing BUG-001 valve loads
    - id: AT-8
      status: ATTACKED
      artifacts: [crates/repark-spark/src/call_args.rs, crates/repark-spark/src/call.rs, scripts/check_rust_file_size.py]
    - id: AT-9
      status: ATTACKED
      artifacts: [crates/repark-iceberg/src/write/row_lineage_guard.rs, crates/repark-spark/src/tests/v3_cow.rs]
    - id: AT-10
      status: ATTACKED
      artifacts: [crates/repark-spark/src/tests/call.rs, crates/repark-spark/src/tests/call_rewrite_dangling.rs]
```

## 7. Cycle 2 — the salvage (2026-08-28, Fable lane)

The 2026-08-28 ruling narrowed the unit after cycle 1 converged. The salvage rebased the branch
onto #255's head, adopted the narrowed clauses, and attacked the branch as artifacts.

FINDING: F-rp2-3
  severity: S1
  category: AT-10
  clauses: [C-007, C-008]
  description: The parity meta-pin
    `python/repark-parity/tests/test_v3r_1_rulings.py::test_v3_cow_1_is_a_refusal_row_dated_by_the_ruling`
    was RED on the branch — it pinned the old `V3-COW-1` heading, the renamed Spark pin and the
    phrase "append-only", all of which the branch's registry rewrite removed; cycle 1's C-008
    named the parity suite without running it (it is not in `make preflight`).
  disposition: REMEDIATED — the registry row keeps the 2026-08-25 ruling citation and the pin
    is retargeted to the narrowed truth (new heading, both new pin names, "live deletion
    vector"); red before, 5/5 green after.

FINDING: F-rp2-4
  severity: S1
  category: AT-2
  clauses: [C-003]
  description: Under the narrowed C-003 the ANSI door and the facade had no first-delete
    success pin, no door had a second-delete refusal pin, and the registry claimed
    "merge + supersede on re-delete" — unpinned, and false on the shipped guard, which refuses
    the second DELETE.
  disposition: REMEDIATED — one first-delete + second-refusal pin per door (Spark
    `adopted_v3_mor_second_delete_refuses_while_a_deletion_vector_is_live`, ANSI
    `adopted_v3_mor_first_delete_commits_a_deletion_vector_and_a_second_refuses`, facade
    `test_facade_v3_mor_first_delete_commits_a_deletion_vector_and_a_second_refuses`), each
    asserting snapshot, pointer, lineage, rows, live-DV set and object count unchanged; the
    claim removed from the registry and the handoff.

FINDING: F-rp2-5
  severity: S2
  category: AT-6
  clauses: [C-007]
  description: Two guard messages were stale after the lift — the resolver seat still said
    "a v3 table is append-only in this engine", and the live-DV refusal said "wait for V3-3"
    after fork F-17 landed the closure.
  disposition: REMEDIATED — strings updated (not test-expressible: the refusal pins assert the
    row id and the vector count, not the advisory tail).

FINDING: F-rp2-6
  severity: S3
  category: AT-6
  clauses: [C-008]
  description: The ledger carried duplicate `## 2` / `## 3` headings and a "filled at
    readiness" stub table beside the filled one.
  disposition: REMEDIATED — single numbered section sequence.

```yaml
COVERAGE_ATTESTATION:
  pr_unit: rp-2-fork-repin
  cycle: 2
  risk_tier: high
  critic_engine: ccc
  complete: true
  note: >
    Procedural in-session pass over the rebased branch (feat/rp-2-salvage) as
    artifacts. Rust v3 pins 11/11 (Spark door) and 14/14 (ANSI door), facade
    pin 2/2, parity rulings pin 5/5 before the gates; fresh execution below.
  fresh_executions:
    - input: partitioned MOR v3 table (ids 1..6 over part 0/1, two data files), facade .sql() "DELETE FROM ice.sales.probe WHERE id IN (3, 4)" then "DELETE FROM ice.sales.probe WHERE id = 1"
      entry_point: facade .sql() + .to_arrow() on the built module
      observed: first DELETE committed two Puffin DVs (record_count 1 each), live rows {1,2,5,6}; the second DELETE refused naming "2 live deletion vector(s)"; the object set under the table location and the live rows were unchanged
      note: a two-file first delete and a count of 2 appear in no committed pin
  categories:
    - id: AT-1
      status: ATTACKED
      artifacts: [Cargo.toml, Cargo.lock, docs/fork-sync.md]
    - id: AT-2
      status: ATTACKED
      artifacts: [crates/repark-sql/src/v3_cow.rs, crates/repark-sql/src/v3_branch_tag_time_travel.rs, crates/repark-spark/src/tests/v3_cow.rs, python/repark/tests/test_v3_cow_dml.py]
    - id: AT-3
      status: ATTACKED
      artifacts: [crates/repark-iceberg/src/write/row_lineage_guard.rs]
    - id: AT-4
      status: N/A
      justification: the salvage changes no concurrency, lock or retry path; the guard's manifest walk is unchanged
      artifacts: [crates/repark-iceberg/src/write/row_lineage_guard.rs]
    - id: AT-5
      status: ATTACKED
      artifacts: [git diff 97eb178...HEAD — no .github/, no AWS/IAM, no secrets; [patch] revs unchanged from cycle 1]
    - id: AT-6
      status: ATTACKED
      artifacts: [docs/spark-sql-iceberg-parity.md, task/roadmap/epic-term/v1-0-iceberg-v3-northstar.md, task/roadmap/mid-term/iceberg-rust-handoff-2026-08-23.md, docs/design/format-v3-track.md, STATUS.md, briefs/next-sequence.md]
    - id: AT-7
      status: N/A
      justification: no performance claim in the salvage
      artifacts: [git diff 97eb178...HEAD]
    - id: AT-8
      status: ATTACKED
      artifacts: [crates/repark-sql/src/v3_cow.rs, crates/repark-spark/src/tests/common.rs, scripts/check_rust_file_size.py]
    - id: AT-9
      status: ATTACKED
      artifacts: [crates/repark-sql/src/v3_branch_tag_time_travel.rs, python/repark/tests/test_v3_cow_dml.py]
    - id: AT-10
      status: ATTACKED
      artifacts: [python/repark-parity/tests/test_v3r_1_rulings.py, python/repark-parity/tests/test_plan_1_northstar_fnp_sequence.py, scripts/check_ledger_grammar.py]
```

```yaml
PR_READINESS_CHECKLIST:
  id: RA-rp-2-fork-repin-2
  self_run_by_orchestrator: true
  checks:
    ci_green: PASS (make ci, make verify, make preflight, parity suite — §8)
    unit_clauses_proven: PASS (C-001..C-008, narrowed wording)
    coverage_attestation_attached: PASS (cycles 1 and 2 complete: true)
    findings_ledger_closed: PASS (F-rp2-1, F-rp2-3..6 REMEDIATED; F-rp2-2 WITHDRAWN)
    clause_trace_complete: PASS (pins cited from the crate, test and facade maps)
  verdict: READY
  send_back_target: "N/A"
```

## 8. Gates at departure (2026-08-28)

Filled on the departure commit: `make ci`, `make verify`, `make preflight` and
`uv run --package repark-parity pytest python/repark-parity/tests -q` exit codes and counts.

Disposition: CONVERGED (Critic, cycle 2). CCC-CONVERGED is not Delivery.
