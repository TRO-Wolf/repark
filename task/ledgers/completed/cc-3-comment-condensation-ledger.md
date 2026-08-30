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
- test-pinned comment bytes: a comment whose exact text a test asserts (named pin:
  `crates/repark-spark/src/router.rs` canonicalize reasons, asserted by
  `test_pr245_router_comment_states_only_the_local_front_door_invariant`; 102-column line kept);
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
| C-001 | Scope A is one commit of `git mv` plus the minimal `mod` path edits that keep the tree compiling. No comment edits and no other content edits in that commit. | Rename detection; diff of non-path tokens empty except `mod` / `use` path lines. | **PROVEN** | Q-001 alternative: thin `tests/mod.rs` indexes; batteries `git mv` with unchanged line counts. rustfmt alphabetizes `mod` lines. |
| C-002 | Every comment block in the five-slice roster receives the one-line rule. | Per-slice file coverage; before/after comment census. | **PROVEN** | Iceberg 3762→1378 then D-001 rewrite (332 fragments→0). Core roster 1308→510 then D-001 (112→0); `error_map.rs` identical-body `//` deleted. Python dataframe+session 572→269 then D-001 (32→0). TA 944→385, fragments 0. Spark minus frozen 4689→2015, fragments 0. D-002: wrapped-line fragments rewritten as complete sentences; leftover dash banners became `// === name ===`. `fragments.py` (criterion a: body equals an original incomplete comment LINE) reports 0 on iceberg, core roster, python dataframe+session, TA, and spark roster. |
| C-003 | Protected inventory is preserved: `grep -c` of `Model:`, `pins:`, `MUTATION:` per rostered file is identical before and after each slice. Test-pinned comment bytes are the same class. | Per-slice inventory tables. | **PROVEN** | Equiv harness protected counts identical per slice (iceberg Model 2 / pins 22). Spark `pins:` bytes kept; MUTATION lines kept even when over width. Router canonicalize comments restored byte-exact to `6774ebd` (102-col line kept). Grep `'"//'` in parity and facade suites: only this pin targets a rostered file. |
| C-004 | No executable change: identifiers, signatures, control flow, literals, test inputs, assertions, attributes, and dependencies stay. | Equivalence harness at `/tmp/grok-worker/cc3/equiv.py` plus crate tests. | **PROVEN** | `equiv.py` mismatches=0 on iceberg, core roster, python dataframe/session, TA, and spark roster after D-002. Crate tests green. Frozen spark_string_literals.rs and cast_binary.rs untouched. D-004 restored the router pin bytes and reverted the pin-test expected string; `len(rust_approved) == 38` stays. |
| C-005 | Size gates ratchet in both homes (`scripts/check_rust_file_size.py` and `test_cap_1_source_file_line_cap.py`) to the exact new line counts. A file that drops to ≤ 1000 lines loses its row. | Both tables equal the measured lengths; `make check-rust-file-size` exit 0. | **PROVEN** | Dual-home ratchets with each slice. Retired iceberg occ.rs/position_delete.rs and spark session_timezone.rs (891) at ≤1000. EXCEPTIONS 41→38. Pre-commit rust-file-size clean. |
| C-006 | Maps, ledger links, and doc links stay in lockstep with moves and condensation. | `make check-map-sync`, `make check-ledgers`; path-only repairs on archived-ledger links and `docs/spark-sql-iceberg-parity.md`. | **PROVEN** | Per-directory `map.md` CC-3 notes. AGENTS.md house-style clause is one-line comments; `// === name ===` stay. rustfmt.toml leading comment no longer claims hand-authored banners. D-003 restored the AGENTS.md compaction ceiling to 32000 (file is 32000 B); removed the 32000→32100 sentence from scripts/map.md. `make check-docs-compaction` exit 0. |
| C-007 | One PR. `make verify` and `make preflight` exit 0. | Recorded commands and exit codes. | **OPEN** | Round-3 HEAD `8a1a94a` (2026-08-30): `make ci` exit 0; `make verify` exit 0; `make preflight` exit 0 (facade 3764 passed, 74 skipped); `make py-test` exit 0 (459 passed). PR stays with the orchestrator. |
| C-008 | Closing Critic attestation. | Orchestrator Critic. | **PROVEN** | Orchestrator Critic 2026-08-30 over a fresh clone at `42d1bba` (context break: artifacts, not the Actor's summary): comment-stripped token equivalence base→HEAD for all 161 changed Rust files — every difference is a `mod` declaration or module path; every moved test index declared under `#[cfg(test)]`; size-gate table: 41→38 rows, no key ratcheted upward, retired rows at 908/919/891 lines; whole-tree inventory `Model:` 28/28, `pins:` 152/152, `MUTATION:` 31/31; byte-frozen family hashes unchanged; forbidden-literal scan 0 hits over added lines and messages; orchestrator-run `make ci` and `make verify` exit 0; 88 remaining banner lines are all in non-rostered files. Four findings filed and remediated during the run (D-001 width-cut fragments S1, D-002 wrapped-line residue S2, D-003 upward compaction ceiling S2, D-004 test-pinned comment reworded S1); no open S0/S1. |

VERDICT: OPEN (PROVEN=6, OPEN=2, REJECTED=0).

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

## Closing Critic (orchestrator, 2026-08-30)

```yaml
COVERAGE_ATTESTATION:
  pr_unit: cc-3-comment-condensation
  categories:
    - id: AT-1
      status: ATTACKED
      evidence: >
        Walked C-001..C-008 against the brief and the two rulings. C-004 was attacked with an
        independent comment-stripped token comparison of every changed Rust file, not the
        Actor's equiv.py. C-003 was widened to test-pinned comment bytes after D-004.
      artifacts: [task/ledgers/completed/cc-3-comment-condensation-ledger.md:C-001..C-008]
    - id: AT-2
      status: ATTACKED
      evidence: >
        Edge inputs for the condensation: protected lines, clippy-forced # Errors shape,
        wrapped original lines, byte-pinned comments, files crossing the 1000-line ceiling.
        D-001 and D-002 were found by exactly these probes.
      artifacts: [scripts/check_rust_file_size.py, python/repark-parity/tests/test_pr_245_revalidation_record.py]
    - id: AT-3
      status: N/A
      justification: comment-only and rename-only change; no state is written by the diff.
    - id: AT-4
      status: N/A
      justification: no concurrency surface changes; token streams are identical.
    - id: AT-5
      status: ATTACKED
      evidence: >
        Rename purity checked with git -M similarity and cfg(test) placement of every new
        index; pub(crate) visibility on tests::tracing is the minimum that keeps the callers
        compiling and is test-only.
      artifacts: [crates/repark-iceberg/src/tests/mod.rs, crates/repark-core/src/session/tests/mod.rs]
    - id: AT-6
      status: ATTACKED
      evidence: >
        Full gate roster re-run by the orchestrator (make ci, make verify) on the delivered
        HEAD in addition to the Actor's make preflight and make py-test; fast gates re-run on
        the fresh Critic clone.
      artifacts: [scripts/check_ledger_grammar.py, scripts/check_docs_compaction.py]
    - id: AT-7
      status: N/A
      justification: no dependency, lockfile, or workflow change.
    - id: AT-8
      status: ATTACKED
      evidence: >
        Forbidden-literal scan over every added line and commit message on the branch: 0
        hits. Identity byte-exact and the Grok trailer present on all 17 Actor commits.
      artifacts: [task/ledgers/completed/cc-3-comment-condensation-ledger.md]
    - id: AT-9
      status: N/A
      justification: no Spark-visible behavior, error text, or default changes.
    - id: AT-10
      status: ATTACKED
      evidence: >
        Quantified claims remeasured: comment lines per crate, banner census, inventory
        counts, size-gate rows, frozen hashes. Residual banners (88) are outside the roster
        and are recorded as CC-4 candidates, not claimed done.
      artifacts: [crates/map.md]
  complete: true
```
