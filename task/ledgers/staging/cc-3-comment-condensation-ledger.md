# CC-3 — comment condensation round 3 + test/module layout

**Date:** 2026-08-30 · **Branch:** `grok/cc3-comment-condensation` · **Base:** `6774ebd`
(`#261`) · **Path:** STANDARD (named Rust roster; no intended runtime change) · **Prior sweep:**
[CC-2](../archive/2026-08/2026-08-29-comment-condensation-2-ledger.md).

**Retires:** the orchestrator moves this ledger to `../completed/` after the closing Critic
converges. The Actor does not move it.

CC-3 is the same unit shape as CC-2, one level deeper, plus one layout commit. Banners go.
Every remaining `//` / `///` / `//!` block becomes one line. Protected `Model:`, `pins:`, and
`MUTATION:` bytes stay. Executable tokens stay.

## Measured population

Census at base `6774ebd` on 2026-08-30. Comment lines are `grep -rhE '^\s*(//|///|//!)'` over
`*.rs`. Scope B rosters only these trees; layout (Scope A) also moves test modules in
`repark-sql` / `repark-core` / `repark-iceberg`.

| Sequential slice | Comment/doc lines |
|---|---:|
| 1. `crates/repark-iceberg/src/**/*.rs` | 3767 |
| 2. `crates/repark-core/src/` named roster (19 files) | 1308 |
| 3. `crates/repark-python/src/dataframe.rs`, `session.rs` | 572 |
| 4. `crates/repark-ta/src/**/*.rs` | 944 |
| 5. `crates/repark-spark/src/**/*.rs` minus the two byte-frozen files | 4689 |
| **Roster total** | **11280** |

Frozen files (do not edit): `crates/repark-spark/src/tests/spark_string_literals.rs`,
`crates/repark-spark/src/tests/cast_binary.rs`. Repo-wide protected inventory at pickup:
`Model:` 28, `MUTATION:` 31. Per-slice `grep -c` of `Model:`, `pins:`, `MUTATION:` is the
preservation proof.

Core roster (slice 2): `catalog_config.rs`, `catalog_state.rs`, `dialect.rs`, `error_map.rs`,
`extension.rs`, `idents.rs`, `lib.rs`, `namespace_create.rs`, `object_store_s3.rs`,
`pre_execute.rs`, `runtime.rs`, `session.rs`, `session_time_zone.rs`, `sorted_view.rs`,
`temp_view.rs`, `time_travel.rs`, `session/df_guards.rs`, `session/spill.rs`,
`session/temp_views.rs`.

## Disposition rubric

Each comment block in a rostered file receives one disposition.

**Keep byte-exact**

- every `Model:` header line;
- every `pins:` line;
- every `MUTATION:` sentence (join a two-line payload into one line; drop no word);
- license headers;
- `#[allow]` / `#[cfg]` attributes and their exact placement.

**Condense to one line** (rustfmt `max_width = 100`)

- every other `//`, `///`, `//!` block, stating the one fact a maintainer needs;
- a `=====` banner block becomes its one-line doc summary (the `=====` lines are deleted).

**Delete**

- inline `//` that explains rationale or history (the `error_map.rs`
  `#[allow(clippy::match_same_arms)]` narration is the named example);
- second paragraphs, `## Example`, blank `///` spacers, banner lines.

**The only permitted multi-line doc shape** is what clippy pedantic forces on public items:
`/// <one summary line>` then `/// # Errors` (or `# Panics` / `# Safety`) then one line.

Do not repeat CC-2 defects: truncated `MUTATION:` payloads, truncated `pins:` docstrings,
or un-restored byte pins.

## PROPOSITION LEDGER — CC-3 — 2026-08-30

| Clause | Proposition (checkable) | Proof obligation | Verdict | Evidence / open question |
|---|---|---|---|---|
| C-001 | Scope A is one commit of `git mv` plus the minimal `mod` path edits that keep the tree compiling. No comment edits and no other content edits in that commit. | Rename detection; diff of non-path tokens empty except `mod` / `use` path lines. | **OPEN** | Q-001: destination `tests/mod.rs` files grow if they gain sibling `mod` lines. |
| C-002 | Every comment block in the five-slice roster receives the one-line rule. | Per-slice file coverage; before/after comment census. | **OPEN** | Slices run after Scope A. |
| C-003 | Protected inventory is preserved: `grep -c` of `Model:`, `pins:`, `MUTATION:` per rostered file is identical before and after each slice. | Per-slice inventory tables. | **OPEN** | Pickup repo-wide counts: Model 28, MUTATION 31. |
| C-004 | No executable change: identifiers, signatures, control flow, literals, test inputs, assertions, attributes, and dependencies stay. | Equivalence harness at `/tmp/grok-worker/cc3/equiv.py` plus crate tests. | **OPEN** | Harness not yet run. |
| C-005 | Size gates ratchet in both homes (`scripts/check_rust_file_size.py` and `test_cap_1_source_file_line_cap.py`) to the exact new line counts. A file that drops to ≤ 1000 lines loses its row. | Both tables equal the measured lengths; `make check-rust-file-size` exit 0. | **OPEN** | Scope A also renames moved EXCEPTIONS keys. |
| C-006 | Maps, ledger links, and doc links stay in lockstep with moves and condensation. | `make check-map-sync`, `make check-ledgers`; path-only repairs on archived-ledger links and `docs/spark-sql-iceberg-parity.md`. | **OPEN** | Citation home: `crates/map.md`. |
| C-007 | One PR. `make verify` and `make preflight` exit 0. | Recorded commands and exit codes. | **OPEN** | After the last slice. |
| C-008 | Closing Critic attestation. | Orchestrator Critic. | **OPEN** | Actor does not close this clause. |

VERDICT: OPEN (PROVEN=0, OPEN=8, REJECTED=0).

```yaml
PROPORTIONALITY_RUBRIC:
  id: RUBRIC-CC3
  pr_unit: cc-3-comment-condensation
  criteria:
    blast_radius: FAIL (named roster across five crates plus layout moves)
    reversibility: PASS (layout + comment-only; one normal revert restores it)
    size: FAIL (well above 150 changed lines and five files)
    novelty: PASS (no dependency, external call, interface, or architecture change)
    sensitivity: FAIL (comments on write, FFI, concurrency, and compatibility paths)
    clarity: PASS (8/8 clauses written; C-001 has Q-001)
  path: STANDARD
  recorded_by: Actor
```
