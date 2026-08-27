# Charter ledger — RP-2 · fork repin (F-13, F-7 U1+U2, F-3)

**Date:** 2026-08-27 · **Branch:** `feat/rp-2-fork-repin` (opens when the owner charters) ·
**Base:** `06a3e42` (`main`, post-#250) · **Policy:** [../../../AGENTS.md](../../../AGENTS.md)
"Version-pin contract" · **Handoff:**
[../../roadmap/mid-term/iceberg-rust-handoff-2026-08-23.md](../../roadmap/mid-term/iceberg-rust-handoff-2026-08-23.md)
§5 (the repin protocol) · **Path:** STANDARD (code changes; one Actor cycle, one Critic pass).

**Retires:** moved to `completed/` in this unit's departure commit.

**Why now.** The engine pin `5e7b2e4` is 15 fork commits behind fork `main`
`ce92a7bfe2c1be569ed0de1178ed410e8ec3a117`, and that range closes work every engine document
still lists as blocking: **F-13** (Puffin deletion-vector write path — fork #219, #221, #222;
row R114 ✅ 2026-08-24), **F-7 U1+U2** (row lineage through `RewriteFiles`, Java
`first_row_id` suppression, manifest-list ordering — fork #225, #226; row R166 ✅ 2026-08-25),
and **F-3** (`remove-dangling-deletes` composed into `RewriteDataFiles` — row R135,
2026-08-23). The north star §3, STATUS and the slate all say "V3-3 ← fork F-13"; that gate
is open. This unit takes the pin, measures what the new rev makes true on the engine's own
surfaces, and flips exactly the pins the evidence supports. Not in this unit: DV writes
behind a new engine surface (that is V3-3, chartered from this unit's §2 measurement), F-14,
F-15, F-16, and any DataFusion family move.

## PROPOSITION LEDGER — RP-2 — 2026-08-27

| Clause | Proposition (checkable) | Proof obligation | Verdict | Evidence / open question |
|---|---|---|---|---|
| C-001 | Every `iceberg*` `[patch.crates-io]` rev is `ce92a7bfe2c1be569ed0de1178ed410e8ec3a117` and `Cargo.lock` resolves to it; `datafusion`, `datafusion-spark`, `arrow*`, `parquet` and `rust-toolchain.toml` are byte-identical to `main` at `06a3e42` (the fork's family is still arrow/parquet 58.4). | `rg` on the workspace `Cargo.toml` + lock source entries; `git diff main -- Cargo.toml rust-toolchain.toml` empty outside the five revs. | OPEN | Closes on the repin commit. |
| C-002 | The two standing repin duties hold on the new rev: `NamespaceScopedCatalog` forwards every required `Catalog` method (defaulted ones forwarded or an omission stated), and the metadata-projection shim is kept iff the fork's metadata-table `scan` still ignores `projection`; the two metadata-table emptiness pins pass. | Trait diff at the new rev; read fork `metadata_table.rs`; `cargo test` the two named pins (`repark-sql/tests/introspection.rs`, `repark-spark/src/tests/metadata_tables.rs`). | OPEN | Which defaulted methods did the 15-commit range add? |
| C-003 | **F-13 measured, engine side.** With the v3 arm of the R113 guard lifted in a scratch build, a merge-on-read `DELETE` on the adopted partitioned-DV v3 fixture commits a Puffin deletion vector (content 2, `referenced_data_file` set, no new position-delete file), and the PySpark 4.1.2 + Iceberg 1.11.0 oracle reads the engine's commit back to the same live set — on both SQL doors and the facade. | The fixture from V3E-3; `.delete_files` content after the commit; Spark read-back rows + `sum(id)` versus the engine's; one pin per door. | OPEN | If green on all three doors the guard lifts here and `V3-3` shrinks to UPDATE/MERGE + partitioned/spec-evolved coverage; if red on any door the guard stays and the red case is V3-3's first clause. The measurement decides which — recorded either way. |
| C-004 | **F-7 U1 measured.** `CALL system.rewrite_data_files` on the v3 fixture carries `_row_id` and `_last_updated_sequence_number` through compaction unchanged — Spark-equal on the read-back — so the `V3-LINEAGE-1` guard lifts and the registry row moves to FIXED with the date; or it does not, and the row's evidence gains the measured divergence with the fork row it waits on. | Before/after lineage projection through the fork's R166 read path; Spark read-back of the compacted table; the guard pin retargeted from "refuses" to "carries". | OPEN | Does U1's `RewriteFiles` carry reach `RewriteDataFiles` (row R135) or only the transaction (row R107)? Measured, not read. |
| C-005 | **F-7 U2 measured on the COW path.** After the repin a COW `DELETE` on an adopted v3 table assigns lineage as Spark does (a deleted row's survivors keep their `_row_id`; added rows take `first_row_id` from the manifest-list order Java uses) — or the `V3-COW-1` guard stays with the measured `next_row_id` delta recorded against fork row R166's residue. | V3E-1's driver re-run at the new rev; Spark read-back; the registry row updated with the date either way. | OPEN | The fork's `FirstRowIdPolicy` is `pub(crate)` — no engine call site changes; the question is only what the engine's `OverwriteFiles` path now writes. |
| C-006 | **F-3 taken.** `CALL system.rewrite_data_files(..., 'remove-dangling-deletes' => true)` on both doors passes the option to the fork and reports a true `removed_delete_files_count` instead of the hard-coded `0`; default stays `false` (Java's); the `V3-DANGLE-1` queue row is measured on the v3 fixture and dispositioned. | The option through `call.rs`; a 2-file position-delete fixture where the count is non-zero; the pin that asserted `0` retargeted. | OPEN | Does R137's removal also drop DVs (the handoff's open question to the fork)? The v3 run answers it. |
| C-007 | The documents say what the pins prove: north star §3 rows for MOR DML, `rewrite_data_files`, COW DML and DV maintenance carry the measured state and date; STATUS's v3 workstream and the slate stop saying "gated on fork F-13"; the handoff marks F-13 / F-7 U1+U2 / F-3 with the fork PR and landing date and records the take/skip decision per AGENTS.md "Version-pin contract"; crate maps and the divergence registry in lockstep. | `rg 'gated on fork F-13|← fork F-13'` returns nothing live; `make check-map-sync`, `check-docs-compaction`, `check-ledger-grammar` green. | OPEN | Closes on the departure commit. |
| C-008 | Green on the whole surface: `make preflight`, the parity suite (`python/repark-parity/tests`), and the v3 fixture legs (V3E-3/V3E-4 pins) pass at the new rev; the one-page "what changed under us" note lists every fork BEHAVIOR/BREAKING change in the range (#221, #222, #226) with the engine site that absorbs it. | Gate output attached; the note in this ledger's §3. | OPEN | Closes at readiness. |

VERDICT: OPEN — 8 clauses, 0 PROVEN, 0 REJECTED. The gate passes when every row is PROVEN
with its pin (`pins: rp-2-fork-repin/C-NNN`) and the owner confirms.

## 2. Sequence

1. Pickup ritual (`make ledger-archive`, drift checks), then the repin commit (C-001) alone —
   the compile is the first measurement.
2. Standing duties (C-002), then the three measurements (C-003, C-004, C-005) in a scratch
   build before any guard moves; each writes its Spark read-back into this ledger.
3. Flip only the pins the measurements support; take F-3 (C-006).
4. Truth-up (C-007), gates (C-008), Critic pass with a novel input through each door whose
   guard lifted, departure commit.

## 3. What changed under us (filled at C-008)

| Fork PR | Change | Engine site that absorbs it |
|---|---|---|
| #221 | V3 MOR writes deletion vectors — BREAKING API + BEHAVIOR | filled at readiness |
| #222 | F-13 U3b + row-lineage read path + variant Arrow type — 3 BREAKING | filled at readiness |
| #226 | `first_row_id` suppression + manifest-list ordering — 2 BEHAVIOR | filled at readiness |
