# Unit ledger — MB-1 · the batch source over the incremental append scan

**Date:** 2026-10-07 · **Branch:** `feat/mb-1-source` · **Base:** `origin/main`
`575f57ca` · **Model:** muse-spark-1.3-contributor · **Policy:** [../../../AGENTS.md](../../../AGENTS.md).
**Path:** STANDARD. **risk_tier: standard.**

**Retires:** this ledger moves to `../completed/` in this unit's last commit (round 3).

**Scope.** Three rounds per the guided brief. Round 1 (this commit): the
sketch's §3.1 offsets and §3.2 errors —
`crates/repark-iceberg/src/microbatch/{mod.rs, offset.rs, error.rs, map.md}`,
the `pub mod microbatch;` line, the `Cargo.toml` lines, and the lockfile edges
they entail. Round 2: the window and provider (§3.3). Round 3: the core
source wrapper (§3.5). No other file is touched; `session.rs` is not touched.

## PROPOSITION LEDGER — MB-1 round 1 — 2026-10-07

| Clause | Proposition (checkable) | Proof obligation | Verdict | Evidence |
|---|---|---|---|---|
| C-001 | Every §3.1 newtype constructs through `new` and reads through `get`; `TableUuid::of` reads table metadata, `RunId::fresh` yields v4, `Epoch::{FIRST, next, get}` and `OffsetFormatVersion::CURRENT` hold. | The newtype pins in `offset.rs`'s test module, including the memory-catalog `of` pin. | **PROVEN** | 4 pins green (`newtypes_new_get_round_trip`, `epoch_first_next_get`, `run_id_fresh_yields_unique_v4`, `table_uuid_of_matches_table_metadata`). pins: mb-1/C-001 |
| C-002 | `QueryId::derive` is deterministic over (sink uuid, name), sensitive to sink and name, distinguishes `None` from `Some("")`, and matches an independent RFC-4122 computation on two fixed vectors. | The derive pins in `offset.rs`'s test module. | **PROVEN** | 2 pins green (`query_id_derive_matches_independent_vector` against Python-`uuid`-computed values, `query_id_derive_is_deterministic_and_sensitive`). pins: mb-1/C-002 |
| C-003 | `OffsetVector::single`/`get`/`inputs` behave; `try_from_inputs` sorts by table uuid and refuses an empty vector or a repeated table, loud (FL-1). | The vector pins in `offset.rs`'s test module. | **PROVEN** | 3 pins green (`offset_vector_single_get_inputs`, `offset_vector_try_from_inputs_sorts_by_table_uuid`, `offset_vector_try_from_inputs_refuses_empty_and_duplicates`). pins: mb-1/C-003 |
| C-004 | `summary_entries` on both doors and `property` round-trip through `from_summary`/`from_property`; the spark keys ride the `Table` door only; an absent stamp reads `None`; a bad version or partial stamp refuses (FL-2). | The stamp pins in `offset.rs`'s test module. | **PROVEN** | 5 pins green (`summary_entries_round_trip_on_both_doors`, `property_round_trip_through_from_property`, `from_summary_returns_none_without_stamp`, `from_summary_refuses_bad_version_and_partial_stamp`, `from_property_refuses_corrupt_value`). pins: mb-1/C-004 |
| C-005 | `spark_source_offset_json` renders Spark's exact Iceberg JSON shape byte for byte. | The rendering pin in `offset.rs`'s test module. | **PROVEN** | 1 pin green (`spark_source_offset_json_matches_spark_shape`). pins: mb-1/C-005 |
| C-006 | `MicroBatchError` carries every §3.2 variant and `RecoveryReason` carries every reason; every display text is non-empty, and the §4-quoted rows render verbatim (MBE-1, MBE-2, MBE-3, MBE-4, MBE-8, MBE-10, MBE-12, MBE-15). | The display pins in `error.rs`'s test module. | **PROVEN** | 10 pins green (one per quoted row plus `every_variant_renders_a_message` and `recovery_reasons_render`). pins: mb-1/C-006 |
| C-007 | The module is wired (`mod.rs` headed by `#![forbid(unsafe_code)]`, the `lib.rs` line, the `Cargo.toml` lines per D-1), every touched map is current, and the round gate list is green. | The 11 round gates. | **OPEN** (gates run after this commit; evidence lands in the round's closing commit) | Wiring complete; gate evidence pending. |

VERDICT: 7 clauses, 6 PROVEN, 1 OPEN, 0 REJECTED.

## Dated decision rows

- **D-1 (2026-10-07).** `crates/repark-iceberg/Cargo.toml` carries two lines
  beyond the granted `thiserror.workspace = true`, both forced by the
  sketch's fixed content: the `v5` feature on `uuid` (§3.1 `QueryId::derive`
  calls `Uuid::new_v5`, and the unified features in the `repark-iceberg`
  closure are `v4`/`v7`/`serde`/`std`, so the call cannot compile without it),
  and `serde_json.workspace = true` in `[dependencies]` (the Q6 offset and
  property JSON carries arbitrary table-name strings, which need a real JSON
  library's escaping; a hand-rolled parser is the larger attack surface). No
  version moved; the `Cargo.lock` change is edges only (`thiserror` on
  `repark-iceberg`, `sha1_smol` on `uuid`).
- **FL-1 (2026-10-07).** Question: what does `OffsetVector::try_from_inputs`
  refuse — the sketch fixes the `Result` but no failure mode. Flink: corrupt
  checkpoint state fails the restore, never guesses. Spark: a corrupt offsets
  file fails the query. North Star default: refuse loud (NS-6) through the
  enum's generic `Catalog` carrier — an empty vector and a repeated table uuid
  both refuse with the cause named. Acted on; pins in C-003.
- **FL-2 (2026-10-07).** Question: what does `from_summary` do with a partial
  or corrupt stamp. Flink: corrupt state fails the restore. Spark: corrupt
  offsets fail the query. North Star default: the absent discriminator reads
  `None` (an unstamped snapshot is ordinary); a parsed-but-wrong version
  refuses `UnsupportedOffsetFormat`; a version that does not parse and any missing or
  unreadable companion key refuse `Catalog` naming the key. `from_property`
  refuses the same way with no absent case. Acted on; pins in C-004.
- **D-2 (2026-10-07).** Two saturations keep infallible signatures total.
  `QueryId::derive` saturates the `u32` length prefix at `u32::MAX` past 4
  GiB of name; the full name still feeds the v5 digest, so distinct names keep
  distinct ids. `Epoch::next` saturates at `u64::MAX` rather than wrapping to
  `FIRST`. Both reasons live in `microbatch/map.md`.

## Gates

The round gate list, in brief order: `cargo test -p repark-iceberg --lib`,
`make rust-clippy`, `cargo fmt --check`, `make rust-panic-ban`,
`python3 scripts/check_rust_file_size.py`, `./scripts/check_lib_rs.sh`,
`./scripts/check_crate_dag.sh`, `python3 scripts/sync_map_md.py --check`,
`bash scripts/check_map_md.sh --base origin/main`,
`python3 scripts/check_ledger_grammar.py`, and the comment-ban probe with
`hits=0`. Results land here in the round's closing commit, when C-007 flips
to PROVEN and the attestation is filed.
