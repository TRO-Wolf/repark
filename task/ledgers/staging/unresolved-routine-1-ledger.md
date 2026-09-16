# Charter ledger — UNRESOLVED-ROUTINE-1 · every unknown function refuses with Spark's `UNRESOLVED_ROUTINE` on both doors

**Date:** 2026-09-16 · **Branch:** `feat/unresolved-routine-1` · **Base:** `origin/main`
`33c87cbf` · **Model:** muse-spark-1.3-contributor · **Policy:** [../../../AGENTS.md](../../../AGENTS.md).
**Path:** STANDARD. **risk_tier: standard.**

**Retires:** this ledger moves to `../completed/` in this unit's last commit.

**Why now.** Q-17c-3 (owner, 2026-09-16): one fix corrects every missing name at
once, ahead of adding more names. The SQL door refuses every unknown routine as
DataFusion's `Invalid function 'x'.`; only `F.call_function` answers Spark's
`UNRESOLVED_ROUTINE` today, and it does so in Python. The blanket path is one
contract, not one row per name.

**Not in this unit:** `functions*.py`, `dataframe/**`, `column.py`, `catalog.py`,
`types.py` (runs 18a/18b); `spark_literals.rs`, `spark_typed.rs`, `type_table.rs`
(run 18c SQL-LITERAL-TYPING-1 lane); any new kernel; LATERAL VIEW parsing
(SPARK-SQL-GRAMMAR-1 C-009, not on main).

## Rulings recorded at open

- Q-17c-3 (owner, 2026-09-16): a 1.5 card, ahead of adding more names — one fix
  corrects every missing name at once.
- Q-17a-2 (owner, 2026-09-16): a decision that raises, casts, coerces or branches
  on a value is a kernel too — if the SQL door cannot reach it, it is in the
  wrong place; Python holds names, argument shapes and API plumbing only. This
  unit's mapping therefore lives in `crates/repark-spark` (reachable from the SQL
  door, the `F.expr` door, and the string-`filter` door through the native
  boundary), not in Python.
- Q-15c-6 (owner): narrow per-function fix, no registry-wide wrapper that
  re-binds every UDF's return field. The mapping matches only the unknown-routine
  error texts and passes everything else through byte-identical.
- R-18c-1 (this lane, 2026-09-16): engine `AnalysisException.getCondition()` stays
  `None` by pinned contract (`test_e1_errorclass.py` pins `None` for constructed
  natives and for engine analysis errors); the class and SQLSTATE travel in the
  message. Making the accessor parse the message is a cross-cutting contract
  change for the error-surface owner — recorded as P2 hand-off H-002, not done
  here. Pins assert the full message text.
- R-18c-2 (this lane, 2026-09-16): `selectExpr` wraps the fragment as
  `SELECT <fragment> FROM <view>` before the native boundary, so the Rust mapping
  sees position 7 where Spark records position 0 relative to the fragment. The
  class is pinned from Rust; the fragment-relative rewrite is a P2 hand-off to
  run 18b (owns `dataframe/core.py`), H-001. Not guessed from Rust: matching on
  the scratch view name would couple the engine to facade internals.
- R-18c-3 (this lane, 2026-09-16): the `system.builtin` qualifier rule is the one
  measured shape (`UR-SQL-05`); wider namespace generalization is unmeasured and
  stays a blanket `UNRESOLVED_ROUTINE` outside that exact qualifier.
- Skill `audit-repark-parity` was not re-run: no JVM may start on this lane, so
  the live oracle is not re-measured. The recorded fixtures named in the brief
  are the spec; every RePark-side value below was observed live on the release
  native built from this branch's base.

## PROPOSITION LEDGER — UNRESOLVED-ROUTINE-1 — 2026-09-16

| Clause | Proposition (checkable) | Proof obligation | Verdict | Evidence / open question |
|---|---|---|---|---|
| C-001 | The SQL door refuses every unknown scalar/aggregate/window routine as `AnalysisException` carrying `[UNRESOLVED_ROUTINE]`, the name rendered per door rules, the search path, `SQLSTATE: 42883`. Cells UR-SQL-00…04, 06…13, 15. | `test_unresolved_routine_1.py` full-message pins per cell. | PROVEN | `unknown_routine.rs` + `engine_err_for_sql` at the core `sql_with` choke point; 14 SQL pins green on the rebuilt release native (45 passed total). pins: unresolved-routine-1/C-001. §2. |
| C-002 | The query context `; line L pos P` answers per cell. | Same pins assert the position suffix. | PROVEN | All recorded positions answer, incl. nested (pos 11), whitespace (pos 9), multi-line (line 2 pos 1), backticked (pos 7 at the quote) and both fragments (pos 0). Residue: `selectExpr` answers pos 7 of its wrapped `SELECT` — H-001 to run 18b, registry row BL-19-POS-SELX in step 7. pins: unresolved-routine-1/C-002. §2. |
| C-003 | The Python door: `F.expr`, `selectExpr`, string `filter` answer the same class; `F.call_function` stays green. Cells UR-PY-00…03. | Same pin file, py-door section. | PROVEN | `F.expr` maps with the fragment, `filter` with the predicate, `selectExpr` rides the core seam (class green); `call_function` untouched and green. H-003 stands. pins: unresolved-routine-1/C-003. §2. |
| C-004 | The three out-of-shape classes: TVF implemented at the same seam; LATERAL VIEW and `system.builtin` recorded. | TVF pin UR-SQL-17; BACKLOG rows for the rest as decided. | PROVEN | TVF four-sentence pin green; `system.builtin` → `REQUIRES_SINGLE_PART_NAMESPACE` pin green. LATERAL VIEW stays a disclosure pin; its BACKLOG row lands in step 7. pins: unresolved-routine-1/C-004. §2. |
| C-005 | Blanket, not per name: a Rust unit test maps an arbitrary unknown name; one pin iterates ≥20 Spark-unknown names. | In-module Rust test + parametrized facade pin. | PROVEN | 14 in-module tests (`arbitrary_unknown_name_maps_blanket` uses a name from no list); 22-name facade blanket pin green. No name list in the implementation. pins: unresolved-routine-1/C-005. §2. |
| C-006 | Every existing `Invalid function` assertion for a Spark-unknown name now asserts the Spark class; names Spark has are listed. | Retired pins + the two ledger lists. | PROVEN | Files: `test_fnp11b_typeof.py`, `test_fnp7_try_inversions.py`, `test_functions_gt2.py`, `test_filter_predicate_rewrite.py` (docstring only, no assertion), `test_w0_window_bench_smoke.py`, `test_fnp_gen_1.py` (negative assertion, untouched), `test_column_parity_1.py`, Rust `keyword_lower.rs` / `bare_nullary.rs`. Verification critic (Grok 4.6, head `05c94c2f`) L-004 CLOSED: no retired assertion claims Spark's class for a name Spark has; `schema_of_csv` is an explicit divergence tripwire (UR3-SQL-13). |
| C-007 | Registry: BL-19 → FIXED with pins; residue rows dated. | `docs/spark-sql-iceberg-parity.md` rows. | PROVEN | BL-19 FIXED with pins; BL-19-POS-SELX (H-001) and BL-19-LATERAL-1 BACKLOG rows dated 2026-09-16; FNP-11B-TYPEOF-BINARY-1 reworded to the new shape. pins: unresolved-routine-1/C-007. |

## 1. Red-first record (base `33c87cbf`, release native from base, 2026-09-16)

Probe: the brief's `msg.py` over `b2-oracle.json` with the clone `.venv`
(`UR` prefix). Result: every in-scope cell `NE` except `UR-PY-00` (`EQ`).

| Cell | Oracle (Spark 4.1.2) | RePark on base | State |
|---|---|---|---|
| UR-SQL-00 `SELECT nosuchfn(1)` | `[UNRESOLVED_ROUTINE] … \`nosuchfn\` … SQLSTATE: 42883; line 1 pos 7` | `AnalysisException: Error during planning: Invalid function 'nosuchfn'.` | RED (C-001, C-002) |
| UR-SQL-01 ``SELECT `nosuchfn`(1)`` | same, pos 7 | same `Invalid function` | RED |
| UR-SQL-02 `SELECT NoSuchFn(1)` | name keeps case `` `NoSuchFn` ``, pos 7 | `Invalid function 'nosuchfn'.` (folded, case lost) | RED (case recovery needed) |
| UR-SQL-03 `SELECT nosuchfn()` | same, pos 7 | `Invalid function 'nosuchfn'.` | RED |
| UR-SQL-04 `SELECT spark_catalog.default.nosuchfn(1)` | `` `spark_catalog`.`default`.`nosuchfn` ``, pos 7 | `Invalid function 'spark_catalog.default.nosuchfn'.` | RED |
| UR-SQL-05 `SELECT system.builtin.nosuchfn(1)` | `[REQUIRES_SINGLE_PART_NAMESPACE] … got \`system\`.\`builtin\`. SQLSTATE: 42K05` (no position) | `Invalid function 'system.builtin.nosuchfn'.` | RED (C-004) |
| UR-SQL-06 `SELECT abs(nosuchfn(1))` | pos 11 | `Invalid function 'nosuchfn'.` | RED (nested position) |
| UR-SQL-07 `SELECT 1 WHERE nosuchfn(1) = 1` | pos 15 | `Invalid function 'nosuchfn'.` | RED |
| UR-SQL-08 `… GROUP BY id` | pos 7 | `Invalid function 'nosuchfn'.` | RED |
| UR-SQL-09 `… OVER ()` | pos 7 | `Invalid function 'nosuchfn'.` | RED |
| UR-SQL-10 `SELECT fortnight(1)` | `` `fortnight` `` pos 7 | `Invalid function 'fortnight'.` | RED |
| UR-SQL-11 `SELECT   nosuchfn(1)` | pos 9 | `Invalid function 'nosuchfn'.` | RED (whitespace) |
| UR-SQL-12 `SELECT 1,\n nosuchfn(2)` | line 2 pos 1 | `Invalid function 'nosuchfn'.` | RED (multi-line) |
| UR-SQL-13 `SELECT nosuch.fn(1)` | `` `nosuch`.`fn` `` pos 7 | `Invalid function 'nosuch.fn'.` | RED |
| UR-SQL-14 `SELECT typeof(1)` | ok | ok | CONTROL (Spark has the name; must keep resolving) |
| UR-SQL-15 `SELECT count(nosuchfn(1))` | pos 13 | `Invalid function 'nosuchfn'.` | RED |
| UR-SQL-16 LATERAL VIEW generator | `[ROUTINE_NOT_FOUND] … SQLSTATE: 42883; line 1 pos 23` | `UnsupportedOperationException: … LATERAL VIEWS` | RED → BACKLOG (grammar unit owns the parse) |
| UR-SQL-17 `SELECT * FROM nosuchtvf(1)` | `[UNRESOLVABLE_TABLE_VALUED_FUNCTION] … SQLSTATE: 42883; line 1 pos 14` | `AnalysisException: Error during planning: table function 'nosuchtvf' not found` | RED (C-004, same seam) |
| UR-PY-00 `call_function` | no position | EQ | GREEN, keep |
| UR-PY-01 `F.expr('nosuchfn(1)')` | pos 0 | `Invalid function 'nosuchfn'.` | RED (C-003) |
| UR-PY-02 `selectExpr('nosuchfn(id)')` | pos 0 | `Invalid function 'nosuchfn'.` | RED class (C-003); position → H-001 |
| UR-PY-03 `filter('nosuchfn(id) = 1')` | pos 0 | `Invalid function 'nosuchfn'.` | RED (C-003) |

## 1b. Red-run evidence (step 3, base native)

`.venv/bin/python -m pytest python/repark/tests/test_unresolved_routine_1.py -q -p no:cacheprovider`
→ `41 failed, 4 passed`. The 4 passes are the already-green `UR-PY-00`
(`call_function`), the LATERAL VIEW backlog disclosure, the `typeof` control,
and the fixture-presence check. Sample failure (`UR-SQL-00`):

`assert "Error during planning: Invalid function 'nosuchfn'.\nDid you mean 'cosh'?" == '[UNRESOLVED_ROUTINE] …'`

Finding F-001: DataFusion appends a `\nDid you mean 'x'?` suggestion whose
target varies per name (`cosh`, `count`, `try_validate_utf8` in the probe), so
the name parser reads up to the closing quote and ignores the tail. The blanket
pin asserts only the message prefix plus the position suffix for this reason.

## 2. Implementation record (steps 4–5, 2026-09-16)

New module `crates/repark-core/src/unknown_routine.rs` (string-level, no SQL
parse): `map_unknown_routine_message(sql, message)` matches DataFusion's
`Invalid function 'dotted.name'` (any tail, including the `\nDid you mean`
suggestion) and `table function 'name' not found`, and renders Spark's shape.
Name case and the `line L pos P` span recover from the caller SQL text with a
case-insensitive call-site search (name followed by `(`; quoted names position
at the opening backtick), because DataFusion folds names to lowercase. The
`system.builtin` qualifier alone renders `REQUIRES_SINGLE_PART_NAMESPACE`
without a position (the one measured shape, R-18c-3); every other dotted shape
quotes each part under `UNRESOLVED_ROUTINE`. TVF renders Spark's four-sentence
`UNRESOLVED_TABLE_VALUED_FUNCTION` with the same position rule.

Seams (one choke point per door, all with the caller's text): core `sql_with`
maps through `engine_err_for_sql` (covers `spark.sql`, the ANSI door, and
`selectExpr`'s wrapped `SELECT` — class green, fragment position stays H-001);
`F.expr` maps in `plan_expr_column` with the original fragment (the `SELECT`
wrapper is now built inside, so `column/mod.rs` shrinks one line and its size
row ratchets 1014 → 1013); string `filter` maps in `filter_sql` with the
predicate (`dataframe.rs` line count unchanged). `session.rs` (PyO3) is
untouched — the core seam covers it.

Finding F-002: `DataFusionError::Plan` displays with an `Error during planning: `
prefix (datafusion-common 54.1 `error.rs`), so the fragment doors must map
string → `Error::Analysis` → `PyErr` directly (`unknown_routine_to_py_err` in
`repark-python/lib.rs`); round-tripping through a fresh `Plan` re-adds the
prefix. The first build reded `UR-PY-01/03` and `UR-SQL-01` on exactly these two
points (prefix; backtick pos 8 vs 7); both fixed in the second build; the pin file runs 45 passed on the rebuilt
release native (2026-09-16).

`bare_unit.rs` keeps its position-less refusal (grammar unit's match-pin stays
green; out of this card's recorded cells).

## 3. C-006 retirement record (step 6, 2026-09-16)

Assertion-only edits (`Invalid function` → `UNRESOLVED_ROUTINE`); docstrings and
test intent unchanged. `test_fnp_gen_1.py:384` is a negative assertion
(`Invalid function` must NOT appear) and is untouched. `test_filter_predicate_rewrite.py:218`
is docstring prose about DataFusion case-sensitivity with no assertion — untouched
per the card's edit-only-the-assertion rule.

### pins retired — announce to run 18a

- `test_fnp11b_typeof.py::test_typeof_binary_function_owner_is_recorded`
  (TYPEOF-SQL-24, `binary` — Spark has the name; still blocked, new shape).
- `test_fnp7_try_inversions.py::test_try_names_unresolved_on_ansi_sql_door`
  (all twelve `TRY_NAMES`, ANSI door — Spark has every name; still unresolved).
- `test_functions_gt2.py` shuffle-NULL planning pin (line 318 — the empty-array
  and `F.shuffle` answer pins beside it are untouched).
- `test_column_parity_1.py::test_no_sql_spelling_for_struct_edit_names`
  (`withField`/`dropFields`/`astype`/`name`/`outer` — Spark SQL has none of these
  names, so the new class is the true Spark shape, not just a reword).
- `test_w0_window_bench_smoke.py::test_remaining_absents_fail_at_planning`
  (planning-miss disjunct is now `UNRESOLVED_ROUTINE`).
- Rust `keyword_lower.rs::unrelated_errors_pass_through` and
  `bare_nullary.rs::non_schema_error_passes_through`: fixtures swapped to a
  genuinely unrelated `Plan` error — the old-shape blessing moves to
  `unknown_routine.rs` tests, which pin the new shape.
- W-0 bench classifier (round-1 F-004, still load-bearing): the full-suite run
  had shown `live_absent == ()` because the absent needles only knew DataFusion's
  `invalid function`; round 1 added the `unresolved_routine` needle (the old
  needle stays, still pinned) plus one classifier pin in `test_w0_window_bench.py`
  (`python/repark-parity/bench/windows/classify.py` is outside this lane's fence —
  announced to the bench owner in round 1). Round 2 re-verified them against the
  token rewrite (`test_remaining_absents_fail_at_planning` green standalone and in
  both full runs) and changed nothing there; a redundant re-application in this
  round was a no-op (blame: `63f40f67a`).

### names Spark has that the SQL door lacks

- `binary` (TYPEOF-SQL-24 answers `binary` on Spark).
- `try_add`, `try_avg`, `try_divide`, `try_element_at`, `try_mod`,
  `try_multiply`, `try_subtract`, `try_sum`, `try_to_binary`, `try_to_date`,
  `try_to_number`, `try_to_time` (all twelve `TRY_NAMES`).
- `shuffle` over a NULL-typed argument (`shuffle(CAST(NULL AS ARRAY<INT>))`;
  populated and empty arrays answer on both doors already).
- `schema_of_csv` on the SQL door (FNP-GEN-1 steps 3–4 pending): its strict-xfail
  tripwire (`test_sql_door_schema_of_csv_uninferable_literal_raises`) XPASSed on
  the shape change alone, so the marker is retired and the refusal now pins
  `UNRESOLVED_ROUTINE`. The kernel-landing signal survives: answering flips the
  `pytest.raises` block red. Announced to the FNP-GEN-1 owners alongside the 18a
  list.

## 4. Hand-offs

- H-001 (P2 → run 18b): `selectExpr` fragment-relative `line 1 pos 0` needs a
  facade edit in `dataframe/core.py` (fenced here). Rust pins the class.
  Round-2 evidence it cannot come from Rust: `SELECT nosuchfn(id) FROM range(3)`
  answers absolute pos 7 (UR-SQL-08) while the wrapped selectExpr twin answers
  fragment-relative pos 0 (UR-PY-02 round 1) — same Rust-visible shape, so the
  relativization needs the fragment boundary only the facade has. UR3-PY-01
  (`SELECT id, id + nosuchfn(1) FROM <view>`, Spark pos 5 in the second
  fragment) pins the class from Rust.
- H-004 (P2 → run 18b): string `filter`/`where` fragment positions need the
  original predicate in `dataframe/core.py` (`filter`, line ~1254, fenced here):
  pass it to the native `filter_sql` alongside the quoted text (e.g. an extra
  parameter) and map the error against it; keep planning the quoted text.
  Rust-only recovery is impossible: quoting wraps bare schema idents in `"…"` but
  preserves pre-quoted spans, so original `id = nosuchfn(1)` (Spark pos 5,
  UR3-PY-00) and a hypothetical pre-quoted original `"id" = nosuchfn(1)` (name
  at fragment offset 7 by the same fragment rule) reach Rust as the
  byte-identical `"id" = nosuchfn(1)`. Rust pins
  the class (UR3-PY-00/03); registry row BL-19-POS-FILTER filed.
- H-005 (note → runs 18a/18b): `functions_byname.py` (H-003) and
  `catalog_surface.py` keep Python-side message builders (PYPERF-001/002);
  delegating them to the Rust path is their owners' call, not this unit's.
- H-002 (P2 → error-surface owner): engine `AnalysisException.getCondition()` /
  `getSqlState()` parsing the `[CLASS]` / `SQLSTATE:` out of the message.
- H-003 (note → run 18a): `functions_byname._resolve_routine` can delegate to
  the Rust path; also its dotted-name arm raises `REQUIRES_SINGLE_PART_NAMESPACE`
  for every dotted name while the SQL door answers `UNRESOLVED_ROUTINE` for
  `spark_catalog.default.x` and `nosuch.fn` — doors disagree there, no oracle
  cell pins it. Measured 2026-09-16: `F.call_function('spark_catalog.default.nosuchfn', …)`
  raises `REQUIRES_SINGLE_PART_NAMESPACE` naming `` `spark_catalog`.`default` ``
  while `spark.sql("SELECT spark_catalog.default.nosuchfn(1)")` raises
  `UNRESOLVED_ROUTINE`. Also measured: an all-caps `SYSTEM.BUILTIN` qualifier
  renders recovered-case `` `SYSTEM`.`BUILTIN` `` (unmeasured by the oracle).

## Remediation round 1 (2026-09-16, critic-logic + perf reads on `63f40f67`)

| Id | Disposition | Evidence |
|---|---|---|
| L-001 | FIX in this round | Spelling now comes from the matched call-site ident values only; string-literal case no longer leaks (UR3-SQL-00 pin). |
| L-002 | FIX in this round | Call sites match on sqlparser word tokens (strings/comments are never words); nested unknowns resolve to the outer call DataFusion names (UR3-SQL-01/02/03/11 pins). |
| L-003 | FIX in this round | Name structure comes from the SQL ident values (single dotted quoted ident stays one part); the error text is only a flattened lookup key; backticked `system.builtin` reaches REQUIRES (UR3-SQL-04/05/06/07 pins). |
| L-004 | FIX in this round | `schema_of_csv` pin reframed as an explicit divergence tripwire (Spark HAS the name, UR3-SQL-13); every other retired assertion re-checked name by name, table below. |
| L-005 | PARTIAL: F.expr fragment positions proven (UR3-PY-02); filter/where and selectExpr fragment positions HANDED OFF (H-004/H-001 — the original text is behind fenced call sites, see H-004) | UR3-PY-00/01/03 class pins green; BL-19-POS-FILTER row filed; BL-19-POS-SELX stays open. |
| L-006 | PINNED (was already correct) | `chars().count()` kept; UR3-SQL-08/09 pins (é/😀 both pos 12). |
| PERF-001/002 | FOLLOWS from the rewrite | Single lazy token pass, first match wins, no hit Vec; boundaries structural. |
| PERF-003/PYPERF-003 | NOTED | Each seam formats the DataFusion error exactly once already. |
| PYPERF-001/002 | HAND-OFF rows (H-003/H-005) | `functions_byname.py` / `catalog_surface.py` are 18a/18b files; no edit. |
| ORCH-001 | FIX in this round | `engine_err_for_sql` moved above the `engine_err` doc comment; no new doc. The pre-commit comment grep shows exactly one `+///` line, which is that pre-existing doc comment relocated by the move — no new comment in code. |
| UR3-SQL-14/15 | GUARD pins | Arity errors and parse errors bypass the mapper (critic null report); pins hold that. |
| RED (round 2) | `.venv/bin/python -m pytest python/repark/tests/test_unresolved_routine_1.py -q -p no:cacheprovider` → 8 failed, 58 passed on `63f40f67`. The 8 are UR3-SQL-00…07 (literal-case leak, three decoy positions, backticked REQUIRES class, two backticked renders, dotted-quotient structure); UR3-SQL-08…15 and all UR3-PY pins already hold (guards for the rewrite). |

Retired-assertion re-check (L-004 table): `binary` — Spark HAS (TYPEOF-SQL-24 ok), pin records repark's refusal, docstring claims blockage only; `try_*` ×12 — Spark HAS, ANSI-door unresolved pins, no Spark-shape claim; `shuffle`-over-NULL — Spark HAS shuffle, ANSI unreachability pin, Spark-door answers beside it; `withField`/`dropFields`/`astype`/`name`/`outer` — Spark SQL lacks the names, true-shape pins; `schema_of_csv` — Spark HAS (UR3-SQL-13 execution error), reframed tripwire above; w0 absents — planning-miss disjunct, classifier needle added (F-004).

## Remediation round 1 — gates (2026-09-16)

- `cargo test -p repark-core --lib` → exit 0: 552 passed, 0 failed, 1 ignored
  (26 `unknown_routine` tests, one per decoy shape).
- `make rust-clippy` → exit 0, Finished, no errors.
- `make verify` → exit 0, zero failure markers (dag, sizes, lib-py, conventions,
  docstrings, ledgers, grammar, compaction, links all clean).
- Release native: rebuilt after the final Rust edit; mtime-proofed current
  afterwards (no tracked `.rs`/`.toml` newer than the `.so` in the worktree),
  so no second rebuild.
- Pin file: 66 passed on the rebuilt native (45 round-1 + 21 round-2).
- `PYTHONPATH=python/repark-parity/src .venv/bin/python -m pytest
  python/repark-parity/tests -q` → exit 0: 757 passed, 2 skipped, 12 xfailed.
- `.venv/bin/python -m pytest python/repark/tests -q -p no:cacheprovider` →
  see F-005.

Finding F-005 (facade full-suite stability under box load, not this unit): the
first attempt stalled 24+ min at 85% with the worker idle on a futex (zero
failures to that point; owned run, killed). Head slice (303 files) then ran
7952 passed, tail slice (45 files) 1152 passed, both exit 0 — the stall does
not reproduce and no slice implicates this unit's paths. Second full attempt:
9103 passed with one failure in
`test_h3_spill_matrix.py::test_a_pool_refusal_is_the_documented_spark_shaped_exception[window_unbounded-64M]`
(`Failed: DID NOT RAISE`), a 64M-pool threshold test: it passed round-1's full
run and passes standalone 7/7, and this unit cannot suppress a refusal (the
mapper only reshapes raised errors carrying the routine markers). Third full
attempt → exit 0: 9104 passed, 367 skipped, 33 xfailed — the clean single line;
the flake never reproduced outside load.

## 5. Gates (step 8, 2026-09-16, release native rebuilt after the final Rust edit)

- `cargo test -p repark-spark --lib` → exit 0: 1023 passed, 0 failed, 4 ignored.
- `cargo test -p repark-core --lib` → exit 0: 540 passed, 0 failed, 1 ignored
  (14 new `unknown_routine` tests green).
- `make rust-clippy` → exit 0, Finished, no errors.
- `make verify` → green end to end (crate-dag, lib-rs, rust-file-size, lib-py,
  python-conventions, docstring-presence, ledger-check, ledger-grammar,
  docs-compaction, docs-links all clean). Two reds on the way, both fixed in
  this unit: ledger-grammar wanted a `pins:` citation for PROVEN C-007 (added to
  the `crates/repark-core` and tests maps); py-lint E501 on three long
  docstrings in the new pin file (wrapped).
- Release native: rebuilt with `maturin develop --release` after the final Rust
  edit; later commits touch only `#[cfg(test)]` fixtures, `.py` pins, `.md` and
  the registry, so the binary is current (no rebuild needed).
- `.venv/bin/python -m pytest python/repark/tests -q -p no:cacheprovider`
  (WHOLE facade suite) → exit 0: 9081 passed, 367 skipped, 33 xfailed. Two reds
  on the way, both fixed here: the FNP-GEN-1 strict-XPASS tripwire (F-003) and
  the W-0 absent-classifier (F-004).
- `PYTHONPATH=python/repark-parity/src .venv/bin/python -m pytest
  python/repark-parity/tests -q` (WHOLE parity suite) → exit 0: 757 passed,
  2 skipped, 12 xfailed. One red on the way, fixed here: CAP-1 mirror row
  `column/mod.rs` 1014 → 1013 moved with the script baseline.

In-flight finding F-003: the full facade run surfaced one strict-XPASS in
another unit's file (`test_fnp_gen_1.py::test_sql_door_schema_of_csv_uninferable_literal_raises`):
its guarded refusal changed shape under this unit (`Invalid function` →
`UNRESOLVED_ROUTINE`) without the kernel landing. Repaired narrowly in step 6
(marker retired, refusal pins the new class, kernel-landing signal preserved);
see §3.

## Orchestrator close-out — run 18c, 2026-09-16

**Rulings applied:** Q-17c-3 (owner: the blanket refusal is a 1.5 card ahead of more names), Q-17a-2 (the refusal is decided in
Rust), Q-17a-4 (whole facade + whole parity suites at the gate), Q-17b-1 (commit as its own step). R-18c-O1 (orchestrator, G-2):
L-005's string `filter` / `where` and `selectExpr` fragment positions ship as the dated residue rows BL-19-POS-FILTER and
BL-19-POS-SELX — the class and message are right on those doors, and the original fragment text only exists in
`dataframe/core.py`, run 18b's file today; the verification critic judged the hand-off honest.

**Verification critic** (Grok 4.6, read-only, head `05c94c2f`): L-001, L-002, L-003, L-004, L-006, ORCH-001, PERF-001 CLOSED;
PERF-002 NOT CLOSED as V-001 (P3): a non-parse miss still formats the DataFusion error twice — ledger note, no product impact.
Nested block comments are the one unmeasured decoy shape.

**Orchestrator gate re-run** (rebased head `05c94c2f`, release native): `make verify` rc 0; `make rust-clippy` rc 0; release native
rc 0; the whole parity suite rc 0 (757 passed, 2 skipped, 12 xfailed); example coverage rc 0; the whole facade suite 9105 passed,
1 failed — `test_h3_spill_matrix.py::test_a_pool_refusal_is_the_documented_spark_shaped_exception[window_unbounded-64M]`, a
memory-pool refusal case under box load, unrelated to this unit; the file re-run alone: 24 passed. Comment-ban grep 0 hits;
forbidden-trailer grep 0 hits. Main moved during the unit (#653 NEVER-OOM-PANIC-1): one conflict in
`crates/repark-core/src/lib.rs` re-exports, resolved by keeping both sides.

**Names Spark has that the SQL door lacks, handed to run 18a:** `schema_of_csv` (see the tripwire); the retired-assertion table
above lists the rest.

```yaml
COVERAGE_ATTESTATION:
  pr_unit: unresolved-routine-1
  categories:
    - id: AT-1
      status: ATTACKED
      evidence: Every in-scope oracle cell (UR-SQL, UR-PY, UR3-SQL, UR3-PY) is pinned with the full Spark message; red on base, green on the head.
      artifacts: [python/repark/tests/test_unresolved_routine_1.py, python/repark/tests/unresolved_routine_1_spark_oracle.json]
    - id: AT-2
      status: N/A
      justification: No numeric result; the error path is formatted only when a query already fails.
    - id: AT-3
      status: ATTACKED
      evidence: Decoys in strings and comments, quoted and dotted names, multi-byte text, nested unknown calls and wrong-arity calls were attacked by two critic rounds and measured on Spark.
      artifacts: [crates/repark-core/src/unknown_routine.rs]
    - id: AT-4
      status: N/A
      justification: No shared state or ordering; the mapper is a pure function of the SQL text and the error.
    - id: AT-5
      status: N/A
      justification: No privileged action, environment read or secret.
    - id: AT-6
      status: ATTACKED
      evidence: Arity and parse errors bypass the mapper (UR3-SQL-14/15 guard pins), so a known function's error is never rewritten.
      artifacts: [python/repark/tests/test_unresolved_routine_1.py]
    - id: AT-7
      status: ATTACKED
      evidence: One tokenizer pass only on the failure path; the success path is unchanged (both S2-21 reads).
      artifacts: [crates/repark-core/src/error_map.rs]
    - id: AT-8
      status: ATTACKED
      evidence: No dependency or workflow edit; size ceilings moved down only.
      artifacts: [scripts/check_rust_file_size.py]
    - id: AT-9
      status: ATTACKED
      evidence: The refusal names the routine, the search path, SQLSTATE and the position, as Spark prints them.
      artifacts: [crates/repark-core/src/unknown_routine.rs]
    - id: AT-10
      status: ATTACKED
      evidence: Red-first runs are pasted for both rounds (round 2 — 8 failed, 58 passed on 63f40f67).
      artifacts: [task/ledgers/staging/unresolved-routine-1-ledger.md]
  reattested: []
  complete: true
```
