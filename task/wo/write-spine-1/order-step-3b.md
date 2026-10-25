# WRITE-SPINE-1 step (3b) — the door matrix against the step-0 fixtures     grade: B     engine band: Claude Opus 5.5 build (one lane), Opus 5.5 + Muse MAX verify     budget: 10 h

Parent order: `task/wo/write-spine-1/order-steps-1-3.md` (its §0 scope, §1 rulings, §6 halts bind this step unchanged).
Merged inputs on main: #1181 (step 0: `_write_spine_1_spellings.py`, `_write_spine_1_shapes.py`, `_record_write_spine_1.py`, the two fixtures, `census-2026-10-24.md`) and #1182 (steps 1 and 3a: `WriteDoor`, `door_of`, `write_door_of(sql, options)`, `last_write_doors(session)`, `_write_spine_1_doors.py`, `test_write_spine_1_enumeration.py`). Ledger: `task/ledgers/staging/write-spine-1-ledger.md` (C-005 and C-006 are this step's rows).

## 0. What this step proves and does not touch
Steps 1 and 3a changed no value. Step 3b shows it on the census: every one of the 12,192 cells answers on this head exactly as `write_spine_1_main_7a789fc5.json` recorded, and the door each spelling enters is the door the code takes, statically and at run time. No product code changes in this step. No fixture is re-recorded unless a cell differs, and a differing cell is a halt, not a re-record.

## 1. Rulings already made (nothing is decided inside the round)
- One source of spelling truth: `_write_spine_1_spellings.SPELLINGS` (41 entries). The frozen `DOOR_SPELLINGS` copy in `_write_spine_1_doors.py` is replaced by a derivation from it; the module's public functions keep their names and answers.
- The static classifier is `repark._native.write_door_of(sql, options)`; the runtime record is `repark._native.last_write_doors(session)`. Both exist on main (`crates/repark-python/src/session_write_options.rs:142-161`).
- The documented divergence class of C-010 stands: on a MERGE whole-column update of a leaf `TIMESTAMP`/`TIMESTAMP_NTZ` column the static reading answers D8 and the runtime record D9. The matrix exempts exactly that class by name, nothing wider.
- Reach rules from the step-0 corpus: `spark_sql` and `dataframe` spellings run through the facade session; `native_sql` through `repark.sql()`, which records no door (C-011) and is classified statically only; `rust_api` (D15 to D20) and `none` (D21) are excluded from the live matrix and the exclusion set is asserted equal to what the corpus says.
- A runtime record that disagrees with the corpus's door, outside the C-010 class, is RECORDED, not fixed: a dated row in a `DIVERGENCES` table inside the test module, pinned by one parametrized case each, and listed in the hand-back. The enum, `door_of` and the corpus do not change in this round.
- Maps, ledger rows, the card line and `task/wo/write-spine-1/map.md` are written by a clerk round after the builder hands back (owner ruling 2026-10-24). The builder writes `handback-notes.md`, one line per touched file: purpose, the claim it proves, the test name a `pins:` citation cites.

## 2. Files (exact; the builder corrects a line number, never a file)
| file | change |
|---|---|
| `task/wo/write-spine-1/order-step-3b.md` | this text, committed first, unchanged |
| `python/repark/tests/timestamps/_write_spine_1_doors.py` | `DOOR_SPELLINGS` becomes a derivation from `SPELLINGS` grouped by `door`; `RUST_API_SPELLINGS` from entries whose `reach == RUST_API`; the functions `spelling_reach`, `rust_api_only_doors`, `spelling_doors` keep their signatures and answers |
| `python/repark/tests/timestamps/test_write_spine_1_enumeration.py` | the corpus test drops `importorskip` and imports the corpus directly; its assertions stay |
| `python/repark/tests/timestamps/_write_spine_1_matrix.py` (new, not a `test_` module) | rendering of one cell to its executable form reusing the recorder's functions (`_record_write_spine_1.create_sql`, the template fill of `_write_spine_1_shapes`), the facade-session builder of the recorder, `static_door(cell) -> str | None`, `runtime_doors(session, cell) -> list[str]`, `sample(cells, per_class) -> list[Cell]` (one cell per spelling × target-shape class × one zone, deterministic order) |
| `python/repark/tests/timestamps/test_write_spine_1_doors.py` (new) | the matrix, §3 |
| `handback-notes.md` (clone root, git-excluded) | one line per touched file |

## 3. Tests (names fixed; bodies the builder's)
1. `test_fixture_cells_are_the_census` — recount the two fixtures: 12,192 ids, identical id sets, the main fixture's status counts, the Spark-door red set (2,829 cells) and its eight family counts all equal the numbers in `task/wo/write-spine-1/census-2026-10-24.md` (read the numbers from that file with a regex, not retyped).
2. `test_every_cell_spelling_names_its_corpus_door` — each cell id's spelling name resolves in the corpus, and the door is one of the eighteen.
3. `test_static_door_agrees_with_the_corpus_on_every_spark_sql_cell` — for every `spark_sql` cell in the main fixture, render the SQL and call `write_door_of` with the cell's options; the answer equals the corpus door, except the C-010 class (asserted to answer D8) and the rows of `DIVERGENCES`. `None` for any spark_sql cell is a failure (this is order mutant 5(a)'s red test).
4. `test_static_door_classifies_native_sql_cells` — for every `native_sql` cell, `write_door_of` on the native text answers D14's dispatch shape or the module records why not (a `DIVERGENCES` row); no crash on any cell.
5. `test_runtime_record_agrees_with_the_corpus_on_the_sample` — build the facade session once per zone as the recorder does; for each sampled `spark_sql` cell, run seed and statement, read `last_write_doors`; the set contains the corpus door (a MERGE records both arms); C-010 class exempt; the session records nothing after a SELECT. Run under the native module only; mark `@pytest.mark.timeout(900)` if the suite uses markers, else keep the sample under 300 cells.
6. `test_dataframe_writers_record_their_doors` — one execution per `dataframe` spelling per target class: `last_write_doors` after the writer call contains the corpus door; a writer that issued a router statement after its write (ruling Q10's question) shows as an empty record and becomes a `DIVERGENCES` row, not a fix.
7. `test_excluded_reaches_are_exactly_the_corpus_says` — the doors excluded from 5 and 6 equal `{D15..D20} ∪ {D21}` as the corpus's `rust_api`/`none` reaches, and D18's Rust test still exists (reuse the step-3a name check).
8. `test_head_replay_equals_main_fixture` — reads a replay document the lane produces (§4 step 3) from `write_spine_1_head_<sha>.json` when present beside the fixtures and asserts zero differing cells; when the file is absent the test is skipped with the reason "replay document not present" (CI never has it; the lane's hand-back pastes the recount). The replay document is NOT committed.

## 4. Steps (one commit each)
1. Order file committed. `uv sync --locked --extra record --no-install-package repark && (cd python/repark && uvx maturin@1.14.1 develop --profile ci)`.
2. `_write_spine_1_doors.py` derivation + the enumeration test's direct import. The enumeration module green. Commit.
3. In the background, under `systemd-run --user --scope -p MemoryMax=40G --same-dir --quiet taskset -c 0-25 .venv/bin/python python/repark/tests/timestamps/_record_write_spine_1.py --main --base <head short sha>` (the recorder's main mode; it writes `write_spine_1_main_<sha>.json`; rename it to `write_spine_1_head_<sha>.json`, never overwrite the committed fixture). Meanwhile write `_write_spine_1_matrix.py` and tests 1 to 4 and 7. Commit when green.
4. Tests 5, 6, 8. Commit when green. Diff the replay against `write_spine_1_main_7a789fc5.json` with a five-line script and paste the recount in the hand-back; any differing cell is halt 3 of the parent order.
5. Mutants (each: the change, the red test, the line; revert after): (a) `door_of` returns `None` for the `Insert` arm; (b) one corpus entry's `door` changed; (c) `last_write_doors` clear removed (the step-3a mutant, re-run against test 5); (d) the C-010 exemption removed from test 3. Paste the table in `handback-notes.md`.
6. Gates: `make py-lint py-format-check`, `uvx typos@1.47.2`, `scripts/check_docstring_presence.sh`, the comment-gate grep, `.venv/bin/python -m pytest python/repark/tests/timestamps/test_write_spine_1_enumeration.py python/repark/tests/timestamps/test_write_spine_1_doors.py python/repark/tests/timestamps/test_write_spine_1_door_carrier.py -q`. `make ci` is the orchestrator's after the clerk commit; do not run the full facade suite.

## 5. Halts (first one ends the round; the hand-back names it)
Those of the parent order §6, plus: a spelling whose rendered SQL the static classifier cannot parse on more than one cell (one is a `DIVERGENCES` row); a sample runtime that exceeds 20 minutes (narrow the sample to one zone and say so); the recorder's main mode not finishing in 4 h (hand back what is green, with the partial diff).

## 6. Hand-back
`handback.json` per COMMON.md plus: the replay recount (cells compared, differing), the sample size and wall, the `DIVERGENCES` rows verbatim, the mutant table, and the model id first.

## Ledger rows this step closes (`task/ledgers/staging/write-spine-1-ledger.md`, written by the clerk from the notes)
C-005 → PROVEN or OPEN with the differing cells; C-006 → PROVEN with mutant (a) red; new C-013: static classifier agrees with the corpus on every spark_sql cell outside the C-010 class and the recorded divergences; new C-014: the runtime record on the sample and the DataFrame writers; new C-015: one source of spelling truth (the frozen copy is gone).
