# Unit ledger — ICE-LIST-NULL-2 · copy-on-write DELETE with a compound predicate over a nested column answers Spark

## Round 1 (2026-09-19)

**Date:** 2026-09-19 · **Branch:** `fix/ice-list-null-2` · **Base:** `6a6f190c` (origin/main)
**Model:** muse-spark-1.3-contributor · **Policy:** [../../../AGENTS.md](../../../AGENTS.md).
**Path:** STANDARD. **risk_tier: standard** — product-path valve plus pins: one gate helper,
two call-site valves, one Rust door battery, one gate unit battery, the 128-cell Python
pins flipped from strict-xfail to plain, one registry correction, maps. No fork change,
no `STATUS.md`, no `Cargo.toml` / `Cargo.lock`.
**Fork:** unchanged (owns the sound DataFusion DELETE path since #299, F-LIST-NULL-ACCESSOR-1).

**Why now.** RP-31 (#710) pinned run 23a's 128-cell Spark oracle for `IS [NOT] NULL` on
list/map/struct columns. 112 cells answer Spark; the 16 copy-on-write DELETE cells with a
compound predicate (`id > 1 AND xs IS NULL`, `xs IS NULL OR id = 1`, 4 shapes x v2/v3) fail
loud with `DataInvalid => Accessor for Field xs not found`. The refusal is RePark-side, not
fork-side: a compound predicate is an `Expr::BinaryOp`, which the plain-identity claim
accepts, so RePark's own identity DELETE runs it and builds a commit-scope Iceberg predicate
(`for_identity_dml`) the fork cannot bind on a non-primitive column. A bare `xs IS NULL` is
`Expr::IsNull`, which the claim refuses, so it delegates to the fork's sound path — those
cells pass, as do the merge-on-read twins.

**Not in this unit:** `STATUS.md`, `Cargo.toml` / `Cargo.lock`, any fork work, the
CAST-MAP-SPELL-1 parser gap (BACKLOG, substitution stays), the merge-on-read `IS NOT NULL`
delete-file packing divergence (still pins RePark's measured values), EXPLAIN output (planned
by DataFusion, not evidence of the executed path).

## Fix

The identity path no longer claims a selection that references a non-primitive (list, map,
struct) column: `selection_refs_non_primitive` in
`crates/repark-iceberg/src/write/predicate_dml/plain.rs` parses the claimed selection SQL and
resolves every bare or target-qualified column against the table's Iceberg schema through the
shared `conflict_filter::top_level_field` rule — never by name-guessing in the SQL text.
`plain_identity_needs_fork` is the shared async seat both doors call after loading the target
(the Spark door in `crates/repark-spark/src/spark_ast.rs`, the native door in
`crates/repark-sql/src/router.rs`); a hit returns "not mine" so the statement falls through
to the fork's DataFusion DELETE path, exactly as a bare `IS NULL` already does. Every
primitive-column identity DELETE stays on the existing path with no behaviour or performance
change. The sixteen strict xfails flip to plain pins against the recorded Spark cells.

## Rulings

- **Q-23b-LN-N — the preferred design is adopted: decline, do not widen.** The identity path
  returns "not mine" for any selection over a non-primitive column instead of widening a row
  filter or teaching the commit scope nested binding (the table-format engine lives in the
  fork; duplicating its binding in RePark would split the contract). The check runs where the
  table is loaded — one shared helper both doors use — and resolves through the Iceberg
  schema, so a renamed or qualified spelling cannot slip past. Delegation was possible for
  every failing shape (the fork binds nested terms in `delete_from` since #299), so no shape
  keeps the old path.
- **Q-1 — the Rust RED pins live at the Spark door, the gate pins beside the helper.** Six
  door tests (`list_null_compound.rs`) run the full claim path on copy-on-write tables and
  expect the fixture's surviving ids (`[1, 3, 4]` / `[3, 4]`); seven gate unit tests
  (`predicate_dml/tests/plain.rs`) pin the helper on list, map and struct schemas, the
  bare-`IS NULL` non-claim, the primitive-only and unknown-column negatives, and the
  alias-qualified spelling. No Python compute, no oracle re-record: one representative seed
  per kind, Spark's ids as expectations.
- **Q-2 — the eight `xs IS NULL OR id = 1` cells pin RePark's measured `overwrite`
  operation.** Delegation answers Spark's rows on all sixteen cells, but the fork's
  file-rewriting CoW DELETE commits `overwrite` where Spark records `delete` (with zero
  delete files). No RePark path commits `delete` for a file-rewriting CoW DELETE — the old
  identity path commits `overwrite` too — so matching Spark's operation would mean changing
  commit semantics on the sensitive write path, out of scope for this unit. The pins follow
  the file's own `FILE_COUNT_DIVERGENCES` discipline: the recorded statement runs verbatim,
  Spark's ids are asserted, and only the operation asserts RePark's measured value, so a
  future convergence reds the pin. Flagged in the hand-back for orchestrator confirm.

## PROPOSITION LEDGER — ICE-LIST-NULL-2 — 2026-09-19

| ID | Claim | Evidence | Status | Notes |
|----|-------|----------|--------|-------|
| C-001 | The gate declines exactly the non-primitive selections: compound predicates over list, map and struct columns need the fork; a primitive-only compound and an unknown column stay on identity; a bare nested `IS NULL` is not a plain-identity claim. | `predicate_dml/tests/plain.rs` (7 tests green) + `predicate_dml/tests/map.md`. | PROVEN | See §Red evidence. |
| C-002 | Six Spark-door pins run copy-on-write DELETE with both compound shapes over list, map and struct tables and keep the fixture's surviving ids. | `crates/repark-spark/src/tests/list_null_compound.rs` (6 tests green) + `src/tests/map.md`. | PROVEN | Red pre-fix on the loud accessor error; see §Red evidence. |
| C-003 | The identity path declines non-primitive selections on both doors and keeps every primitive-column identity DELETE on the existing path. | `predicate_dml/plain.rs` (`selection_refs_non_primitive`, `plain_identity_needs_fork`), the two call-site valves, `predicate_dml/map.md`. | PROVEN | Q-23b-LN-N. |
| C-004 | All 128 recorded cells answer Spark's ok and ids as plain pins, offline and live; operation equals Spark's except the eight OR cells, which pin RePark's measured `overwrite`. | `test_ice_list_null_1.py` (offline 257 passed, 4 skipped; live green) + `python/repark/tests/map.md`. | PROVEN | The 16 former strict xfails are plain pins; Q-2. |
| C-005 | Registry row ICE-LIST-NULL-1 and the fixture map state the copy-on-write compound residue FIXED 2026-09-19 (ICE-LIST-NULL-2), RePark-side, with pins. | `docs/spark-sql-iceberg-parity.md` ICE-LIST-NULL-1 row + `ice_list_null_1/map.md`. | PROVEN | Corrects the fork #299 residue sentence. |
| C-006 | Every touched directory map is current in the same commits, and the ledger-grammar, docs-links and map-sync gates plus the targeted Rust, lint, format and facade checks are green. | Touched `map.md` rows with `pins:` citations; §Facade evidence. | PROVEN | `pins: ice-list-null-2/C-006` rides the Python tests map row. |

## Red evidence

- Pre-fix `cargo test -p repark-spark --lib tests::list_null_compound`: 6 failed, each loud
  with `External(DataInvalid => Accessor for Field xs not found)` — the exact run-23a refusal.
- Pre-fix `test_ice_list_null_1.py` on the release native: 225 passed, 4 skipped (live tier),
  32 xfailed (16 cells x both parametrizations) on `_COW_COMPOUND_REASON`.
- Post-fix gate negatives stay put: `primitive_only_compound_stays_on_identity` and
  `unknown_column_never_needs_fork` answer false, so primitive identity DELETE statements keep the
  fast path (the existing `dml.rs` / `v3_mor_dml.rs` identity pins stay green unmodified).

## Facade evidence

- `test_ice_list_null_1.py` offline (`-n 4`): 257 passed, 4 skipped (live tier), 0 xfailed.
- `test_ice_list_null_1.py` live (`REPARK_PARITY_LIVE=1`, Spark 4.1.2): 261 passed.
- Identity-DELETE regression files (`test_dml_subquery_parity`, `test_sql_dml_eager`,
  `test_v3_cow_dml`, `test_v3_legacy_delete_merge`, `-n 4`): 52 passed, 3 skipped.
- `cargo test -p repark-iceberg predicate_dml`: 55 passed, 0 failed (48 pre-existing + 7 new).
- `cargo test -p repark-spark --lib list_null_compound`: 6 passed; `tests::dml`: 30 passed.
- `cargo fmt --all --check`: clean. `cargo clippy -p repark-iceberg -p repark-spark
  -p repark-sql --all-targets -- -D warnings -A clippy::disallowed_methods`: clean.
  `ruff check .`: clean. `ruff format --check` on the changed Python file: clean.
- `check_ledger_grammar.py`: 209 live ledgers clean. `check_docs_links.py`: 6005 links
  clean. `sync_map_md.py --check`: 302 maps clean.
- `grep -rn "LIST-NULL" python/repark-parity/tests`: no hits — no parity-docs pin to bring true.

```yaml
COVERAGE_ATTESTATION:
  pr_unit: ice-list-null-2
  categories:
    - id: AT-1
      status: ATTACKED
      evidence: Every failing shape (list, map, struct x AND, OR x v2, v3 x CoW) is pinned against the recorded Spark oracle ids offline and live; red pre-fix, green post-fix.
      artifacts: [python/repark/tests/test_ice_list_null_1.py, crates/repark-spark/src/tests/list_null_compound.rs]
    - id: AT-2
      status: N/A
      justification: No numeric result; ids compared as exact integers, no float or decimal path touched.
    - id: AT-3
      status: ATTACKED
      evidence: Case variants (alias-qualified, unknown column, bare IS NULL non-claim) pinned at the gate; quoted and dotted spellings resolve through the shared top-level-field rule.
      artifacts: [crates/repark-iceberg/src/write/predicate_dml/tests/plain.rs]
    - id: AT-4
      status: N/A
      justification: No shared state or ordering; the gate is a pure function of selection SQL and schema, the valve reads the target once per statement.
    - id: AT-5
      status: N/A
      justification: No privileged action, environment read or secret; table loads use the established catalog handles.
    - id: AT-6
      status: ATTACKED
      evidence: Primitive-only selections keep the identity path (negative pins), so no previously fast DELETE silently changes engine; parse and load failures keep the old loud path.
      artifacts: [crates/repark-iceberg/src/write/predicate_dml/tests/plain.rs]
    - id: AT-7
      status: ATTACKED
      evidence: One extra catalog load per plain-identity DELETE on the claiming path only; the hot primitive path is one load it already paid in the Spark door canonicalization.
      artifacts: [crates/repark-iceberg/src/write/predicate_dml/plain.rs]
    - id: AT-8
      status: ATTACKED
      evidence: No ceiling raised; exact-baseline files untouched in line count except by ratchet-down rule — predicate_dml.rs and the 1440-line battery unchanged.
      artifacts: [scripts/check_rust_file_size.py]
    - id: AT-9
      status: ATTACKED
      evidence: Both SQL doors valve through the one shared helper; the Python pins cover the native door end to end, the Rust door battery the Spark door.
      artifacts: [crates/repark-spark/src/spark_ast.rs, crates/repark-sql/src/router.rs]
    - id: AT-10
      status: ATTACKED
      evidence: Ledger carries the proposition table, red evidence, facade evidence and this attestation; registry and fixture residue corrected with pins.
      artifacts: [task/ledgers/staging/ice-list-null-2-ledger.md]
  complete: true
```

## Outcome

Done: three commits on `fix/ice-list-null-2` — red pins (`2c498d5a`), the fix with the
flipped Python pins (`2fcd08a1`), registry plus ledger (`a1cd884f`) — and this close-out.
All sixteen cells answer Spark's rows; the eight OR cells pin the measured `overwrite`
operation (Q-2, flagged for orchestrator confirm). No ceiling raised, no fork change.
