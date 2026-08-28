# Charter ledger — RP-2 · fork repin (F-13, F-7, F-3, F-16, F-9, F-15)

**Date:** 2026-08-27 · **Branch:** `feat/rp-2-fork-repin` (opens when the owner charters) ·
**Base:** `06a3e42` (`main`, post-#250) · **Policy:** [../../../AGENTS.md](../../../AGENTS.md)
"Version-pin contract" · **Handoff:**
[../../roadmap/mid-term/iceberg-rust-handoff-2026-08-23.md](../../roadmap/mid-term/iceberg-rust-handoff-2026-08-23.md)
§5 (the repin protocol) · **Path:** STANDARD (code changes; one Actor cycle, one Critic pass).

**Retires:** moved to `completed/` in this unit's departure commit.

**Why now.** The engine pin `5e7b2e4` is 20 fork commits behind fork `main`
`26088bb46e655c0825c408dd305cab2228a033fd`, and that range closes work every engine document
still lists as blocking: **F-13** (Puffin deletion-vector write path — fork #219, #221, #222;
row R114 ✅ 2026-08-24), **F-7 in full** (lineage through rewrites and Java `first_row_id`
suppression, #225/#226; `RewritePositionDeleteFiles` on v3, #227; DV removal accounting on
compaction, #232), **F-3** (`remove-dangling-deletes`, row R135), **F-16** (the delete-ratio
candidate clause, #232), **F-9** (dated S3 Tables `register_table` service-gap ruling on row
R126, #233) and **F-15** (`write_default` filled at `DataFileWriter::write`, #233). *Amended
2026-08-27, same day: the first draft targeted `ce92a7b` and three items; the fork landed
#227/#232/#233 before the unit opened, and one repin takes the whole landed batch.* The north
star §3, STATUS and the slate all say "V3-3 ← fork F-13"; that gate is open. This unit takes
the pin, measures what the new rev makes true on the engine's own surfaces, and flips exactly
the pins the evidence supports. Not in this unit: DV writes behind a new engine surface (that
is V3-3, chartered from this unit's C-003 measurement), V3-6 (the engine's consumption of the
v3 types), F-14 (the fork's next unit), and any DataFusion family move.

## PROPOSITION LEDGER — RP-2 — 2026-08-27

| Clause | Proposition (checkable) | Proof obligation | Verdict | Evidence / open question |
|---|---|---|---|---|
| C-001 | Every `iceberg*` `[patch.crates-io]` rev is `26088bb46e655c0825c408dd305cab2228a033fd` and `Cargo.lock` resolves to it; `datafusion`, `datafusion-spark`, `arrow*`, `parquet` and `rust-toolchain.toml` are byte-identical to `main` at `06a3e42` (the fork's family is still arrow/parquet 58.4). | `rg` on the workspace `Cargo.toml` + lock source entries; `git diff main -- Cargo.toml rust-toolchain.toml` empty outside the five revs. | OPEN | Closes on the repin commit. |
| C-002 | The two standing repin duties hold on the new rev: `NamespaceScopedCatalog` forwards every required `Catalog` method (defaulted ones forwarded or an omission stated), and the metadata-projection shim is kept iff the fork's metadata-table `scan` still ignores `projection`; the two metadata-table emptiness pins pass. | Trait diff at the new rev; read fork `metadata_table.rs`; `cargo test` the two named pins (`repark-sql/tests/introspection.rs`, `repark-spark/src/tests/metadata_tables.rs`). | OPEN | Which defaulted methods did the 15-commit range add? |
| C-003 | **F-13 measured, engine side.** With the v3 arm of the R113 guard lifted in a scratch build, a merge-on-read `DELETE` on the adopted partitioned-DV v3 fixture commits a Puffin deletion vector (content 2, `referenced_data_file` set, no new position-delete file), and the PySpark 4.1.2 + Iceberg 1.11.0 oracle reads the engine's commit back to the same live set — on both SQL doors and the facade. | The fixture from V3E-3; `.delete_files` content after the commit; Spark read-back rows + `sum(id)` versus the engine's; one pin per door. | OPEN | If green on all three doors the guard lifts here and `V3-3` shrinks to UPDATE/MERGE + partitioned/spec-evolved coverage; if red on any door the guard stays and the red case is V3-3's first clause. The measurement decides which — recorded either way. |
| C-004 | **F-7 U1 measured.** `CALL system.rewrite_data_files` on the v3 fixture carries `_row_id` and `_last_updated_sequence_number` through compaction unchanged — Spark-equal on the read-back — so the `V3-LINEAGE-1` guard lifts and the registry row moves to FIXED with the date; or it does not, and the row's evidence gains the measured divergence with the fork row it waits on. | Before/after lineage projection through the fork's R166 read path; Spark read-back of the compacted table; the guard pin retargeted from "refuses" to "carries". | OPEN | Does U1's `RewriteFiles` carry reach `RewriteDataFiles` (row R135) or only the transaction (row R107)? Measured, not read. |
| C-005 | **F-7 U2 measured on the COW path.** After the repin a COW `DELETE` on an adopted v3 table assigns lineage as Spark does (a deleted row's survivors keep their `_row_id`; added rows take `first_row_id` from the manifest-list order Java uses) — or the `V3-COW-1` guard stays with the measured `next_row_id` delta recorded against fork row R166's residue. | V3E-1's driver re-run at the new rev; Spark read-back; the registry row updated with the date either way. | OPEN | The fork's `FirstRowIdPolicy` is `pub(crate)` — no engine call site changes; the question is only what the engine's `OverwriteFiles` path now writes. |
| C-006 | **F-3 + #232 taken.** `CALL system.rewrite_data_files(..., 'remove-dangling-deletes' => true)` on both doors passes the option to the fork and reports a true `removed_delete_files_count` instead of the hard-coded `0`; default stays `false` (Java's); on the v3 fixture the count includes the DVs the rewrite stranded, so `V3-DANGLE-1` is dispositioned by measurement. | The option through `call.rs`; a 2-file position-delete fixture where the count is non-zero; the v3 fixture's count against Spark's `removed_delete_files_count = 6`; the pin that asserted `0` retargeted. | OPEN | Closes on the v3 run. |
| C-007 | The documents say what the pins prove: north star §3 rows for MOR DML, `rewrite_data_files`, COW DML and DV maintenance carry the measured state and date; STATUS's v3 workstream and the slate stop saying "gated on fork F-13"; the handoff marks F-13 / F-7 U1+U2 / F-3 with the fork PR and landing date and records the take/skip decision per AGENTS.md "Version-pin contract"; crate maps and the divergence registry in lockstep. | `rg 'gated on fork F-13|← fork F-13'` returns nothing live; `make check-map-sync`, `check-docs-compaction`, `check-ledger-grammar` green. | OPEN | Closes on the departure commit. |
| C-008 | Green on the whole surface: `make preflight`, the parity suite (`python/repark-parity/tests`), and the v3 fixture legs (V3E-3/V3E-4 pins) pass at the new rev; the one-page "what changed under us" note lists every fork BEHAVIOR/BREAKING change in the range (#221, #222, #226) with the engine site that absorbs it. | Gate output attached; the note in this ledger's §3. | OPEN | Closes at readiness. |

| C-009 | **F-16 measured.** MW-7's 1e7×50 MERGE-then-maintain sequence on a merge-on-read table ends at zero delete files and zero delete records, as Spark's does, with the default `delete-ratio-threshold` (0.3); the MW-7 pin that recorded 8 surviving delete files flips from "documents the gap" to "asserts zero", and the maintenance runbook drops its residual-delete caveat. | Re-run the MW-7 driver at the new rev (its 2,500-row reproduction first, the 1e7 run once); the pin; `docs/` runbook diff. | OPEN | Closes on the re-measurement. |
| C-010 | **F-9 taken.** `CALL system.register_table` against S3 Tables refuses with a message that names the dated service gap, and the guide / divergence registry cite fork row R126's ruling (#233) instead of "refuses in the fork". | Grep the guide and registry for the citation; the existing refusal pin retargeted to the message. | OPEN | Closes on the truth-up commit. |
| C-011 | **F-7 U3 measured.** `CALL system.rewrite_position_delete_files` on the adopted v3 fixture no longer refuses (`B-MOR-3`): it runs the fork's v3 DV arm and the Spark read-back is unchanged before and after — or it stays refused with the measured reason recorded against fork row R136's ENGINE-FIRST note. | Both doors + facade on the V3E-3 fixture; read-back rows + `sum(id)`; `.delete_files` before/after. | OPEN | R136's v3 arm has no Java oracle (ENGINE-FIRST); the engine's evidence is Spark read identity, which is the measurement. |
| C-012 | **F-15 carried, not consumed.** The repin compiles and every gate passes with the fork's `write_default` fill in `DataFileWriter::write`; no engine surface sets a `write_default` yet, so the append fixtures are byte-flat before/after, and V3-6's charter gains the note that the fork surface exists. | Fixture byte comparison; the V3-6 note. | OPEN | Closes at readiness. |

VERDICT: OPEN — 12 clauses, 0 PROVEN, 0 REJECTED. The gate passes when every row is PROVEN
with its pin (`pins: rp-2-fork-repin/C-NNN`) and the owner confirms.

## 2. Sequence

1. Pickup ritual (`make ledger-archive`, drift checks), then the repin commit (C-001) alone —
   the compile is the first measurement.
2. Standing duties (C-002), then the measurements (C-003, C-004, C-005, C-009, C-011) in a
   scratch build before any guard moves; each writes its Spark read-back into this ledger.
3. Flip only the pins the measurements support; take F-3 / #232 (C-006), F-9 (C-010), F-15 (C-012).
4. Truth-up (C-007), gates (C-008), Critic pass with a novel input through each door whose
   guard lifted, departure commit.

## 3. What changed under us (filled at C-008)

| Fork PR | Change | Engine site that absorbs it |
|---|---|---|
| #221 | V3 MOR writes deletion vectors — BREAKING API + BEHAVIOR | filled at readiness |
| #222 | F-13 U3b + row-lineage read path + variant Arrow type — 3 BREAKING | filled at readiness |
| #226 | `first_row_id` suppression + manifest-list ordering — 2 BEHAVIOR | filled at readiness |
| #227 | `RewritePositionDeleteFiles` extends to v3 (ENGINE-FIRST) | filled at readiness |
| #232 | delete-ratio clause + v3 DV removal accounting | filled at readiness |
| #233 | S3 Tables `register_table` service gap; `write_default` fill | filled at readiness |
