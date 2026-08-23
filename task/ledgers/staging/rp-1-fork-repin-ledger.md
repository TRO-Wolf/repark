# Charter ledger — RP-1 · fork repin (F-0, F-1, F-2, F-8a)

**Date:** 2026-08-23 · **Branch:** `feat/rp-1-fork-repin` · **Base:** `2b319de` (`main`,
post-#225) · **Policy:** [../../../AGENTS.md](../../../AGENTS.md) "Version-pin contract" ·
**Handoff:** [../../roadmap/mid-term/iceberg-rust-handoff-2026-08-23.md](../../roadmap/mid-term/iceberg-rust-handoff-2026-08-23.md)

**Retires:** this ledger moves to `../completed/` in the unit's last commit.

Owner-chartered 2026-08-23 as the first row of the post-MW sequence. The engine pin
`0c5fd58d4ab73a0113a8b28b717cf5d002b0f8f2` is a genuine ancestor of fork `main`
`5e7b2e4f8fcb0ff65943cdbc10cdd8f4132fe0b6` (20 commits ahead). Landed on that range:
F-0 `#214`, F-1 through `#213`, F-2 `#215`. Open fork F-3 `#216` is **not** in this
unit. DataFusion family does not move. MW-6 is a later unit, never a passenger.

## PROPOSITION LEDGER — RP-1 — 2026-08-23

| Clause | Proposition (checkable) | Proof obligation | Verdict | Evidence / open question |
|---|---|---|---|---|
| C-001 | Every `iceberg*` `[patch.crates-io]` rev is `5e7b2e4f8fcb0ff65943cdbc10cdd8f4132fe0b6`, and `Cargo.lock` resolves to that commit. | `rg` on workspace `Cargo.toml` + lock source entries. | OPEN | Actor writes the pin and `cargo update`. |
| C-002 | `datafusion`, `datafusion-spark`, `arrow*`, `parquet`, and `rust-toolchain.toml` are byte-identical to `origin/main` at `2b319de`. | `git diff origin/main -- Cargo.toml rust-toolchain.toml` shows only the iceberg patch rev (and lock iceberg sources). | OPEN | Family must not ride this unit. |
| C-003 | `NamespaceScopedCatalog` is re-enumerated against the new `Catalog` trait: every required method is forwarded; every defaulted method is either an explicit forward or a stated omission. | Diff the trait surface at the new rev against `crates/repark-iceberg/src/catalog/provider.rs` and the crate-root map's last-audit counts. | OPEN | Standing repin duty. |
| C-004 | The metadata-projection shim is kept if and only if the new rev's metadata-table `scan` still ignores `projection`, including the empty-projection case. | Read fork `metadata_table.rs` at the new rev; keep or delete `metadata_projection.rs` per its stated removal criterion. | OPEN | Standing repin duty. |
| C-005 | The two metadata-table emptiness pins still pass: `crates/repark-sql/tests/introspection.rs` and `crates/repark-spark/src/tests.rs`. | `cargo test` those two (or the named tests inside them). | OPEN | Handoff F-8 breakage criterion. |
| C-006 | The `a$b` "unresolvable through the fork" residue note is gone from `crates/repark-iceberg/map.md` and `src/catalog/map.md`; the ADR-0006 enumeration filter remains. | Grep those maps; filter code still drops synthesized `$` names. | OPEN | F-8a. Resolution may now work; enumeration parity does not. |
| C-007 | `CALL rewrite_position_delete_files` on a 4-file position-delete group returns `rewritten_delete_files_count = 0` and `added_delete_files_count = 0`; registry row `MOR-1` is retired. | Flip `call_mor1_compacts_below_sparks_min_input_files_floor` to equality; edit `docs/spark-sql-iceberg-parity.md`. | OPEN | F-1 breaking default (floor 2 → 5). |
| C-008 | No remaining engine test requires compaction of a 2-file or 4-file position-delete group. | `rg` for rewritten_delete_files_count assertions at 2 or 4, and for `min-input-files` / `entries.len() < 2`. | OPEN | F-1 follow-up. |
| C-009 | `execute_expire_snapshots` fills Spark's three content-file columns from `CleanupReport`'s typed views; `ExpireCounts::tally` and `classify_content_files` are deleted; `call_expire_splits_content_files_like_spark` still holds. | Diff `crates/repark-spark/src/call.rs`; re-run that test. | OPEN | F-2 additive split; `#[non_exhaustive]` is construction-site only. |
| C-010 | `write.merge.isolation-level = snapshot` remains a supported opt-down (drops `validate_no_conflicting_data`); a pin records that a successful MOR delete is not undone by a concurrent `Replace` on that arm after F-0. | Existing snapshot OCC tests stay green; add or retarget a files-exist/`Replace` pin on the snapshot arm. | OPEN | F-0 engine follow-up. The serializable arm is not this clause. |

VERDICT: FAIL (OPEN=10, REJECTED=0). LOGIC_SCORE = 0/10.

Flips to PASS when the Actor's tests cite every `PROVEN` row (`pins: rp-1-fork-repin/C-NNN`)
and the Critic files `COVERAGE_ATTESTATION`.

```yaml
KILLED_ASSUMPTIONS:
  - "Wait for fork F-3 before repinning": REMOVED (owner sequenced RP-1 now; F-3 is opt-in dangling-delete compose, later R135)
  - "MW-6 can share this PR": REMOVED (handoff §5: one repin per landed batch, never a passenger)
  - "Family bump rides any iceberg* rev change": REMOVED (fork did not move its DataFusion 54.1 base)
RISK_HEATMAP:
  - risk: F-0 changes commit conflict behaviour; a test that expected a silent commit of Replace-vs-delete may go red
    severity_if_realized: S1
    mitigation: C-010
  - risk: F-1 floor 5 makes MW-5's 10-delete compact still work (10 >= 5) but any 2-file helper in MW-2 tests breaks
    severity_if_realized: S1
    mitigation: C-007, C-008
CLARIFYING_QUESTIONS: []
```
