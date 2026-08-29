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
| C-001 | Every `iceberg*` `[patch.crates-io]` rev is `ce92a7bfe2c1be569ed0de1178ed410e8ec3a117` and `Cargo.lock` resolves to it; `datafusion`, `datafusion-spark`, `arrow*`, `parquet` and `rust-toolchain.toml` are byte-identical to `main` at `06a3e42` (the fork's family is still arrow/parquet 58.4). | `rg` on the workspace `Cargo.toml` + lock source entries; `git diff main -- Cargo.toml rust-toolchain.toml` empty outside the five revs. | OPEN | Closes on the repin commit. |
| C-002 | The two standing repin duties hold on the new rev: `NamespaceScopedCatalog` forwards every required `Catalog` method (defaulted ones forwarded or an omission stated), and the metadata-projection shim is kept iff the fork's metadata-table `scan` still ignores `projection`; the two metadata-table emptiness pins pass. | Trait diff at the new rev; read fork `metadata_table.rs`; `cargo test` the two named pins (`repark-sql/tests/introspection.rs`, `repark-spark/src/tests/metadata_tables.rs`). | OPEN | Which defaulted methods did the 15-commit range add? |
| C-003 | **F-13 measured under the narrowed guard.** A first MOR `DELETE` on a DV-free v3 table commits a Puffin deletion vector and reads back Spark-equal on both SQL doors and the facade. A second engine DELETE and the Spark-written shared-Puffin fixture refuse before a write. No document or pin claims DV merge or supersession. | First-delete `.delete_files` content and Spark read-back; one success pin per door; committed second-delete and shared-Puffin refusal pins with unchanged metadata and object sets. | OPEN | F-17 and RP-3 own the refused input states. |
| C-004 | **F-7 U1 measured and guarded.** `CALL system.rewrite_data_files` on the v3 fixture is measured at the new rev. The measured lineage reassignment is recorded and `V3-LINEAGE-1` stays armed. | Before/after lineage projection through the fork's R166 read path; Spark read-back; the existing refusal pin remains green. | OPEN | RP-3 re-measures at its selected post-F-17 SHA before V3-5 charters. |
| C-005 | **F-7 U2 measured on the COW path.** COW DELETE may lift only if the adopted-v3 lineage projection is Spark-equal. COW UPDATE and MERGE remain guarded in this unit. | V3E-1 driver re-run at the new rev; Spark read-back; one COW DELETE disposition and committed pre-write refusals for UPDATE and MERGE. | OPEN | The result decides only the COW DELETE seat. |
| C-006 | **F-3 taken.** `CALL system.rewrite_data_files(..., 'remove-dangling-deletes' => true)` on both doors passes the option to the fork and reports a true `removed_delete_files_count` instead of the hard-coded `0`; default stays `false` (Java's); the `V3-DANGLE-1` queue row is measured on the v3 fixture and dispositioned. | The option through `call.rs`; a 2-file position-delete fixture where the count is non-zero; the pin that asserted `0` retargeted. | OPEN | Does R137's removal also drop DVs (the handoff's open question to the fork)? The v3 run answers it. |
| C-007 | The documents say only what the narrowed pins prove: first-delete support, every live-DV state guarded, `rewrite_data_files` guarded, and COW UPDATE/MERGE guarded. The north-star plan points shared-Puffin closure to F-17 and full DV support to RP-3. | `rg 'merge.*superseded|gated on fork F-13'` reviewed; `make check-map-sync`, `check-docs-compaction`, and `check-ledger-grammar` green. | OPEN | Closes on the departure commit. |
| C-008 | Green on the whole surface: branch placeholders and duplicate headings are removed, provenance is accurate, `make preflight`, the parity suite, and the V3E-3/V3E-4 fixture pins pass at the new rev; the one-page note lists every fork BEHAVIOR/BREAKING change in the range. | Branch diff review, gate output, and the note in this ledger's §3. | OPEN | Closes at readiness. |

VERDICT: OPEN — 8 clauses, 0 PROVEN, 0 REJECTED. The gate passes when every row is PROVEN
with its pin (`pins: rp-2-fork-repin/C-NNN`) and the owner confirms.

## 2. Sequence

1. Pickup ritual (`make ledger-archive`, drift checks), then the repin commit (C-001) alone —
   the compile is the first measurement.
2. Standing duties (C-002), then the narrowed measurements (C-003, C-004, C-005) before any
   guard moves; each writes its Spark read-back into this ledger.
3. Add the second-DELETE and shared-Puffin refusal pins; flip only the COW DELETE seat if its
   measurement is Spark-equal; take F-3 (C-006).
4. Truth-up (C-007), gates (C-008), Critic pass with a novel input through each door whose
   guard lifted, departure commit.

## 3. What changed under us (filled at C-008)

| Fork PR | Change | Engine site that absorbs it |
|---|---|---|
| #221 | V3 MOR writes deletion vectors — BREAKING API + BEHAVIOR | filled at readiness |
| #222 | F-13 U3b + row-lineage read path + variant Arrow type — 3 BREAKING | filled at readiness |
| #226 | `first_row_id` suppression + manifest-list ordering — 2 BEHAVIOR | filled at readiness |
| #227 | `RewritePositionDeleteFiles` extends to v3 (ENGINE-FIRST) | beyond `ce92a7bf` — RP-3's range |
| #232 | delete-ratio clause + v3 DV removal accounting | beyond `ce92a7bf` — RP-3's range |
| #233 | S3 Tables `register_table` service gap; `write_default` fill | beyond `ce92a7bf` — RP-3's range |
