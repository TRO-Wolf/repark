# Charter ledger — SUBQ-CELLS-1 · the six measured subquery cells replace the shape-only pins

**Date:** 2026-09-16 · **Branch:** `test/subq-cells-1` · **Base:** `33c87cbf` · **Model:**
zai/glm-5.3-flash · **Policy:** [../../../AGENTS.md](../../../AGENTS.md).
**Path:** STANDARD. **risk_tier: standard.**
**Registry:** `DF-SUBQUERY-1`
([../../../docs/spark-sql-iceberg-parity.md](../../../docs/spark-sql-iceberg-parity.md)) —
one dated sentence appended to the existing row.

**Originating ruling Q-17b-3 (owner, 2026-09-16, binding):** "SUBQ-CELLS-1 is a 20-minute
follow-up."

**Ruling S-1 (orchestrator, 2026-09-16, binding):** `LIMIT 1` without `ORDER BY` is
nondeterministic in Spark, so the correlated-`LIMIT 1` SQL-door pin keeps its match-set
assertion; the pin adds the measured cell's columns / schema simpleString / nullable as
exact shape assertions, and the `scalar_limit1_correlated_sql` cell's recorded Spark rows
are one legal answer of the nondeterministic query, not a byte-contract on the values.

**Why now.** The run-18b orchestrator ran `oracle-input/probe_subq_wanted.py` on live
PySpark 4.1.2 (2026-09-16) and recorded the six cells DF-SUBQUERY-1's round-3 pins could
only derive shape-only: the three aliased-LATERAL spellings, the two qualified-lateral
SQL-door spellings, and the correlated `LIMIT 1` scalar. This unit folds the cells into
`facade_df_subquery_oracle.json` verbatim and repoints the pins. No product code, no Rust.

**Not in this unit:** the still-unmeasured wanted cell `lateral_on_outer_expr`
(`test_lateral_on_hoisted_column` stays shape-only); any product code; any cell the probe
did not record.

## PROPOSITION LEDGER — SUBQ-CELLS-1 — 2026-09-16

| Clause | Proposition (checkable) | Proof obligation | Verdict | Evidence / open question |
|---|---|---|---|---|
| C-001 | The six probe cells — `lateral_inner_qualified_sql`, `lateral_left_qualified_sql`, `lateral_sql_outer_expr_aliased_filter`, `lateral_sql_outer_expr_aliased_on`, `lateral_sql_outer_expr_aliased_plain`, `scalar_limit1_correlated_sql` — sit in `python/repark/tests/facade_df_subquery_oracle.json` under their probe names, `simpleString` converted to the fixture's `schema` key, `columns` / `nullable` / `rows` verbatim; the file keeps its serialization (sorted keys, indent 1, no trailing newline); no existing cell changes. | `git diff` on the fixture shows additions only; the six names resolve through `_cell()`. | **PROVEN** | The fold script asserted every probe cell carries exactly `result{columns, simpleString, nullable, rows}` and none of the six names pre-existed, then re-dumped the whole fixture with `json.dumps(indent=1, sort_keys=True)` and no trailing newline. `git diff --numstat` on the fixture: **129 insertions, 0 deletions** — a changed existing line would show as a deletion pair, so no existing cell (and not `meta`) moved. 42 → 48 keys. All six cells resolved in the green run. pins: subq-cells-1/C-001 |
| C-002 | Three pins repoint to the measured cells through `_assert_frame` exactly as their neighbours do — `test_lateral_sql_door_outer_ref_qualified_filter` → `lateral_sql_outer_expr_aliased_filter`, `test_lateral_sql_door_outer_ref_aliased_on` → `lateral_sql_outer_expr_aliased_on`, and `test_lateral_sql_door_outer_ref_aliased` keeps its `lateral_outer_expr` assertion and gains a second of the same query against `lateral_sql_outer_expr_aliased_plain` — and `test_lateral_left_qualified_keeps_unmatched_rows`'s docstring points at the new SQL-door pin and cell `lateral_left_qualified_sql` instead of the pending-wanted wording. | The test-file diff; the pins green on the folded tree. | **PROVEN** | The three repoints call `_assert_frame(frame, cell)` — the same helper the neighbouring pins use (read first; it sorts expected rows and compares `_res(frame)` whole, so columns/schema/nullable/rows all bind). Red first: all three KeyError'd on the base fixture (§Red first), green after the fold. The docstring-only pin stayed green through both runs, as a docstring edit must. pins: subq-cells-1/C-002 |
| C-003 | New pins `test_lateral_sql_door_left_qualified` and `test_lateral_sql_door_inner_qualified` run the probe cells' exact SQL — `SELECT * FROM emp e LEFT JOIN LATERAL (SELECT budget FROM dept d WHERE d.dept = e.dept) t ON true` and the `JOIN` spelling — over `_emp` / `_dept` temp views and `_assert_frame` against `lateral_left_qualified_sql` / `lateral_inner_qualified_sql`, with `pins: df-subquery-1/C-006` docstrings like the neighbours. | The two new tests green on the folded tree. | **PROVEN** | The SQL strings are the probe's queries byte-for-byte (wrapped only across lines); both tests register `_emp`/`_dept` temp views exactly as the probe does and `_assert_frame` against their cells. Red first: both KeyError'd (`lateral_left_qualified_sql`, `lateral_inner_qualified_sql`) on the base fixture; both green after the fold — so the engine's LEFT-JOIN-LATERAL `ON true` shape answers the measured Spark rows including the NULL-dept outer row. pins: subq-cells-1/C-003 |
| C-004 | Ruling S-1 on `test_scalar_correlated_limit_sql_door`: the match-set assertion stays; the pin adds assertions that the frame's columns, schema simpleString and nullable equal the `scalar_limit1_correlated_sql` cell; the docstring names that cell and says the recorded Spark rows are one legal answer; the test's `dup` frame equals the probe's `dup` frame. | The pin green on the folded tree; the dup frame compared to probe line 9. | **PROVEN** | The match-set asserts (`by_id[1] in (100, 200) and by_id[3] in (100, 200)`, NULL for ids 2 and 4) are unchanged; the three shape asserts bind `columns` / `schema.simpleString()` / field nullability to the cell (`["id", "b"]`, `struct<id:int,b:int>`, `[True, True]`). The test's `dup` frame `[("a", 100), ("a", 200), ("c", 300)]`, `"dept string, budget int"` equals the probe's line-9 `dup` frame exactly (verified by comparison). Red first: the pin KeyError'd on `scalar_limit1_correlated_sql`; green after the fold — the engine's columns/schema/nullable answer the measured cell on the first try. pins: subq-cells-1/C-004 |
| C-005 | Gates: `pytest python/repark/tests/test_df_subquery_1.py -q -p no:cacheprovider` green; full facade suite and parity suite run once each into logs with real exit codes; `make verify` rc 0. | The Gates table below. | **PROVEN** | Target file exit 0, **51 passed**. Facade suite exit 0, **9037 passed, 367 skipped, 34 xfailed**. Parity suite exit 1 — **756 passed, 1 failed** (`test_dl_6_docs_links.py::test_real_tree_is_green_under_the_seeded_allowlist`: `subq-cells-1-ledger.md -> exists but is not tracked`, the run-order artifact of the once-only run finishing before the round's single commit) **, 2 skipped, 12 xfailed**; after `git add` the direct gate `python scripts/check_docs_links.py` answers **920 files, 5718 links checked — clean, exit 0** and `pytest python/repark-parity/tests/test_dl_6_docs_links.py -q` answers **19 passed, exit 0** on the staged tree. `make verify` **rc 0** (cargo test 3605 passed / 0 failed; all static gates clean). pins: subq-cells-1/C-005 |

## Rust-first roll-call

No kernel, conversion, planner rule or type rule is touched: the diff is a fixture copy,
oracle assertions and docstrings. Nothing that raises, casts, coerces or branches on a
value moved or stayed anywhere new — Python holds names, argument shapes and API plumbing
only, and the seven touched or new pin docstrings plus the six fixture cells are the
complete Python surface of this unit. No further ledger line is owed.

## Red first

The six test edits (repoints, the second aliased assertion, the two new pins, the S-1
shape assertions) ran against the base fixture **before** the cells landed —
`.venv/bin/python -m pytest python/repark/tests/test_df_subquery_1.py -q
-p no:cacheprovider`, exit code **1**: **6 failed, 45 passed** —
`test_lateral_sql_door_outer_ref_aliased`,
`test_lateral_sql_door_outer_ref_qualified_filter`,
`test_lateral_sql_door_outer_ref_aliased_on`, `test_lateral_sql_door_left_qualified`,
`test_lateral_sql_door_inner_qualified`, `test_scalar_correlated_limit_sql_door`, every
failure `python/repark/tests/test_df_subquery_1.py:38: KeyError` on its own cell name
(`lateral_sql_outer_expr_aliased_filter`, `lateral_sql_outer_expr_aliased_on`,
`lateral_sql_outer_expr_aliased_plain`, `lateral_left_qualified_sql`,
`lateral_inner_qualified_sql`, `scalar_limit1_correlated_sql` — one each). The
docstring-only pin `test_lateral_left_qualified_keeps_unmatched_rows` stayed green, as
expected. This proves every new or changed pin reads its recorded cell rather than
passing vacuously.

## Gates

| gate | result |
|---|---|
| Red first — `.venv/bin/python -m pytest python/repark/tests/test_df_subquery_1.py -q -p no:cacheprovider` on the test edits, before the fixture fold | **exit 1 — 6 failed, 45 passed**; every failure `test_df_subquery_1.py:38: KeyError` on its own new cell name (one each: `lateral_sql_outer_expr_aliased_filter`, `lateral_sql_outer_expr_aliased_on`, `lateral_sql_outer_expr_aliased_plain`, `lateral_left_qualified_sql`, `lateral_inner_qualified_sql`, `scalar_limit1_correlated_sql`). |
| Green — the same command after the fixture fold | **exit 0 — 51 passed** in 0.45 s. |
| Full facade suite — `.venv/bin/python -m pytest python/repark/tests -q -p no:cacheprovider` (once, into a log) | **exit 0 — 9037 passed, 367 skipped, 34 xfailed** in 298.13 s. |
| Parity suite — `PYTHONPATH=python/repark-parity/src VIRTUAL_ENV=$PWD/.venv uv run --no-project python -m pytest python/repark-parity/tests -q` (once, into a log) | **exit 1 — 756 passed, 1 failed, 2 skipped, 12 xfailed**; the one failure is `test_dl_6_docs_links.py::test_real_tree_is_green_under_the_seeded_allowlist` — `subq-cells-1-ledger.md -> exists but is not tracked`, the artifact of the once-only run finishing before the round's single commit, not a docs defect. After `git add`: `python scripts/check_docs_links.py` **exit 0** (920 files, 5718 links — clean) and `pytest python/repark-parity/tests/test_dl_6_docs_links.py -q` **exit 0, 19 passed** on the staged tree. |
| `make verify` (CARGO_BUILD_JOBS=10 RUST_TEST_THREADS=8) | **rc 0**; cargo test --workspace **3605 passed / 0 failed**; fmt, clippy, panic ban, crate DAG, size ratchets, conventions, docstrings, example coverage, manifest, ledgers, ledger grammar, docs compaction, docs links, owner ruling, parity-live dual wire, spell check all clean. |

## Questions

None — every gate is green on the staged tree and no pin needed an expected value changed.

VERDICT: 5 clauses, 5 PROVEN, 0 OPEN, 0 REJECTED.

## Rulings

- Q-17b-3 (owner, 2026-09-16, binding): SUBQ-CELLS-1 is a 20-minute follow-up.
- S-1 (orchestrator, 2026-09-16, binding): the correlated-`LIMIT 1` SQL-door pin keeps the
  match-set assertion and adds shape assertions; the recorded Spark rows are one legal
  answer.

```yaml
COVERAGE_ATTESTATION:
  pr_unit: subq-cells-1
  categories:
    - id: AT-1
      status: ATTACKED
      evidence: Each of the card's six items walked one by one — the fold key-by-key against
        the probe JSON (columns / nullable / rows verbatim, simpleString renamed to schema),
        each repoint through the neighbours' `_assert_frame` helper read before use, the
        second aliased assertion of the same query, the two new pins' SQL compared byte-for-byte
        to the probe's queries, and ruling S-1's keep-match-set-add-shape split on the LIMIT pin.
      artifacts: [oracle-input/subq_wanted_probe_2026-09-16.json, python/repark/tests/facade_df_subquery_oracle.json, python/repark/tests/test_df_subquery_1.py]
    - id: AT-2
      status: ATTACKED
      evidence: The pinned shapes exercise the boundary rows — the NULL-dept outer (4, None, None, None)
        kept by LEFT JOIN LATERAL with NULL right columns, the non-matching outer's NULL budget,
        the filtered and ON-restricted row sets, and the nondeterministic LIMIT 1 match set
        (`by_id[1] in (100, 200)`, NULL-dept rows NULL) beside the cell's exact columns, schema
        and nullability.
      artifacts: [python/repark/tests/test_df_subquery_1.py::test_lateral_sql_door_left_qualified, python/repark/tests/test_df_subquery_1.py::test_scalar_correlated_limit_sql_door]
    - id: AT-3
      status: ATTACKED
      evidence: No product code ships, so the unit's only failure surface is a pin diverging from
        the recorded cell — that path was exercised for real: the red-first run failed all six
        touched pins with KeyError on the base fixture, and the card's rule (never change the
        expected value; stop and record) is the round's recorded escalation contract.
      artifacts: [task/ledgers/staging/subq-cells-1-ledger.md]
    - id: AT-4
      status: ATTACKED
      evidence: Every pin builds its own frames and temp views through the per-test `spark` fixture;
        no shared or mutable state was added; row-order dependence stays on the suite's
        sorted-repr contract (`_res`) and the LIMIT pin's match set is order-free by construction.
      artifacts: [python/repark/tests/test_df_subquery_1.py]
    - id: AT-5
      status: ATTACKED
      evidence: The diff is a JSON fixture, test assertions and markdown — no IO, no network, no
        secrets, no deserialization of untrusted input; the probe input file stays untracked by
        the card's order (verified in the final `git status`), so no oracle payload is committed
        from outside the fixture.
      artifacts: [python/repark/tests/facade_df_subquery_oracle.json]
    - id: AT-6
      status: ATTACKED
      evidence: The fold re-serialized the fixture with its own settings (indent 1, sorted keys, no
        trailing newline) and `git diff --numstat` proves 129 insertions / 0 deletions — no
        existing cell's bytes moved, so every other consumer of the fixture sees it unchanged;
        the six new names collide with no existing key (asserted before the write).
      artifacts: [python/repark/tests/facade_df_subquery_oracle.json]
    - id: AT-7
      status: ATTACKED
      evidence: System-breaking risk has no surface — no product code, no engine path; the changed
        tests add constant-shape assertions over 4-row frames, and the full facade suite's wall
        (298 s, 9037 passed) matches the suite's ordinary run profile.
      artifacts: [task/ledgers/staging/subq-cells-1-ledger.md]
    - id: AT-8
      status: ATTACKED
      evidence: The probe→fixture contract was read from the consumer, not assumed — `_res()` names
        the fixture's `schema` key, so the fold renames `simpleString` to it; the probe's cell
        field set was asserted exactly (`columns, simpleString, nullable, rows`) before
        conversion; the SQL strings are the probe's own; nullability comparisons go through the
        frame's schema field API like every neighbouring pin.
      artifacts: [python/repark/tests/test_df_subquery_1.py, oracle-input/probe_subq_wanted.py]
    - id: AT-9
      status: ATTACKED
      evidence: A diverging pin fails loudly naming its cell — `_assert_frame` prints the full
        `_res` diff and a missing cell is a KeyError naming the cell at the shared `_cell`
        helper; the ledger's Questions section plus the card's no-expected-value-change rule
        are the escalation path; nothing silent was added.
      artifacts: [python/repark/tests/test_df_subquery_1.py]
    - id: AT-10
      status: ATTACKED
      evidence: Red-first is the mutation proof — all six touched/new pins failed on the pre-fold
        tree and passed only once their cells landed, so each provably reads its recorded cell;
        every added assert has a nameable input that flips it (a wrong value, name, schema string
        or nullability in the cell, or the wrong row set from the engine); the docstring-only pin
        stayed green across both runs, bounding the change to prose there.
      artifacts: [task/ledgers/staging/subq-cells-1-ledger.md, python/repark/tests/test_df_subquery_1.py]
  complete: true
```
