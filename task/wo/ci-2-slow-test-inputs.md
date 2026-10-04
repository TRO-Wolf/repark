# CI-2 — the ten slowest facade tests reuse their immutable inputs per worker · grade C · clerk band · after CI-1

Clerk grade, after CI-1 (merged as `ffee8692`). The shape is [TEMPLATE.md](TEMPLATE.md). The measurements are CI-1's `--durations=25` on run `37121982216`. Every path below is at that commit. A fixture line that does not match the file is a halt before any edit.

## 0. Why, and what is out

CI-1 shards the facade suite into three ([smoke.yml](../../.github/workflows/smoke.yml), `pytest-xdist -n 4`, `--durations=25`). On run `37121982216` the ten slowest tests sum to 393.69 s. The slowest shard is facade shard 0, whose job wall is 289 s (`12:28:32Z` to `12:33:21Z`). A quarter of that wall is 72.25 s. 393.69 s is more than 72.25 s. The owner's rule (2026-10-03): "If the ten slowest tests sum to more than a quarter of the slowest shard, write a CI-2 order at clerk grade for those tests only: reuse immutable inputs per worker, keep every assertion, move no test between languages."

The same run's other facade walls are shard 1 at 193 s (`12:28:32Z` to `12:31:45Z`) and shard 2 at 157 s (`12:28:32Z` to `12:31:09Z`). Most of the ten tests record their seconds as **setup**, so the fixture is the lever.

**Out.** Paid runners, a fourth shard, a change to `-n` inside `smoke.yml`, any edit of `crates/`, any edit of `python/repark/tests/conftest.py`, any edit of `uv.lock` or a `pyproject.toml`, any new mark, skip, or deletion, any move of a test between languages, any code comment, and the memory-pin file named in §1. A docs-only push does not measure durations: `smoke.yml`'s `changes` job short-circuits when every path is a doc.

## 1. Rulings already made

- Those tests only.
- Reuse immutable inputs per pytest-xdist worker. The owner's example is a `scope="session"` or module fixture that builds the input once. Each xdist worker is its own process, so either scope already runs once in that process. A fixture that already has one of those scopes is the reuse this order asks for. Leave it. Do not widen `module` to `session`: the value would then be visible to every other test that worker runs.
- Keep every assertion and every parameter.
- No test moves between languages.
- No test is deleted, skipped, or marked.
- `test_perf_ice_catalog_io_1.py` is a memory pin on the Python process: it never moves and is never consolidated (owner, 2026-10-03). It is excluded even though one of its tests is in the top ten. Do not open it.
- No edits under `crates/`.
- No code comments.
- A build with one reader stays in that test. Wrapping the only call in a new fixture edits the test body and reuses nothing.
- A value the tests mutate is not an immutable input.
- `python/repark/tests/conftest.py` autouse fixture `_isolate_active_session` calls `_reset_active_session_for_tests` in `python/repark/src/repark/spark/session/session_state.py`, which marks the process-wide active session stopped after every test. A live `ReparkSession` held in a module fixture is dead on the second test. Widening a function-scoped session fixture needs that reset changed. That is product code.

Dropping the excluded 12.11 s still leaves 381.58 s, which is still above 72.25 s. The exclusion does not retire the order.

## 2. The tests

Each line is the test id, its phase, and its seconds, from CI-1's hand-back.

1. `python/repark/tests/test_deep_subquery_expression_1.py::test_thousand_filter_chain_answers_under_subqueries` (setup) 167.94
2. `python/repark/tests/test_deep_filter_chain_crash_1.py::test_thousand_filter_chain_answers_on_dataframe_door` (setup) 96.74
3. `python/repark/tests/test_deep_reverify_1.py::test_sql_text_or_chains_answer_on_every_door` (setup) 29.75
4. `python/repark/tests/test_deep_reverify_1.py::test_gc_collects_deep_cycles_on_main_and_small_threads` (setup) 21.56
5. `python/repark/tests/test_deep_expr_build_small_stack_1.py::test_six_thousand_term_or_build_refuses_through_filter` (setup) 20.58
6. `python/repark/tests/test_h3_spill_matrix.py::test_never_oom_panic_1_tight_pool_join_values_match_or_refuse_typed` (call) 17.15
7. `python/repark/tests/test_perf_ice_catalog_io_1.py::test_peak_rss_over_five_hundred_tables_stays_within_the_default_cache_budget` (call) 12.11: **EXCLUDED** (memory pin)
8. `python/repark/tests/test_ice_views_1.py::test_nested_view_depth_guard` (call) 10.09
9. `python/repark/tests/test_ice_tt_resolve_1.py::test_reader_cells[TT-DF-VAS-INT-2-load]` (setup) 8.89
10. `python/repark/tests/test_ice_tt_resolve_1.py::test_reader_tas_date_past_2262[table-2]` (setup) 8.88

Fixture facts read at `ffee869`. Re-read the decorator and the test body. A mismatch halts the round with `halt` set to `"table-stale"` and no edit.

| # | immutable input on the tree | action |
|---|---|---|
| 1 | [test_deep_subquery_expression_1.py](../../python/repark/tests/test_deep_subquery_expression_1.py): module fixture `worker_results` runs `_WORKER` once and returns a JSON dict. The test reads `subquery_1000_scalar` and `subquery_1000_in`. | Already once per worker. Leave the file. |
| 2 | [test_deep_filter_chain_crash_1.py](../../python/repark/tests/test_deep_filter_chain_crash_1.py): module fixture `worker_results`, same shape. The test reads `filter_1000_df`. | Already once per worker. Leave the file. |
| 3 | [test_deep_reverify_1.py](../../python/repark/tests/test_deep_reverify_1.py): module fixture `main_results`. The test reads `fexpr_2000_or`, `selectexpr_2000_or`, and `stringfilter_2000_or`. | Already once per worker. Leave the file. |
| 4 | Same file: module fixture `gc_results`. The test reads `gc_main` and `gc_small`. | Already once per worker. Leave the file. |
| 5 | [test_deep_expr_build_small_stack_1.py](../../python/repark/tests/test_deep_expr_build_small_stack_1.py): module fixture `worker_results`. The test reads `or_6000_main`. | Already once per worker. Leave the file. |
| 6 | [test_h3_spill_matrix.py](../../python/repark/tests/test_h3_spill_matrix.py): the test calls `_run_join_values` three times (`"8M", 4`, `"1G", 4`, `"8M", 1`). Nothing else calls `_run_join_values`. Phase is call. | Single consumer. Leave the file. Hand the test back as `single-consumer`. |
| 7 | Memory pin. | Do not open the file. |
| 8 | [test_ice_views_1.py](../../python/repark/tests/test_ice_views_1.py): function-scoped `spark` builds catalog `sc` and table `t`. The test then `CREATE VIEW`s `w0` through `w100` and one more view. Every neighbouring test in the file writes through the same fixture. | The catalog is the assertion's mutable state. **H-3.** Leave the file. Hand the test back. |
| 9 and 10 | [test_ice_tt_resolve_1.py](../../python/repark/tests/test_ice_tt_resolve_1.py): `warehouse` and `seeded` are module-scoped. `spark` and `spark_ny` are function-scoped and call `getOrCreate`. `test_reader_cells` requests both sessions on every parameter. | Run the setup-show in step 1. `seeded` or `warehouse` means already once per worker: leave the file. `spark` or `spark_ny` means **H-4**: the autouse reset stops the session, and the fix is product code. Leave the file. Hand both tests back. |

Files a green run may edit: `python/repark/tests/test_ice_tt_resolve_1.py`, and only by adding `scope="module"` to the `spark` and `spark_ny` decorators, and only when step 1's setup-show names those fixtures **and** a re-read shows `_reset_active_session_for_tests` no longer marks the active session stopped. On `ffee869` the reset does mark it stopped, so this edit is not available. No other path is in the edit set.

## 3. Steps

1. For each test, read its fixtures and name the immutable input built in setup. The names are the table in §2. Then, for rows 9 and 10 only, from a provisioned facade venv (step 4's provision, once):

   ```bash
   PYTHONPATH=python/repark-parity/src VIRTUAL_ENV="${VIRTUAL_ENV:-$PWD/.venv}" \
     uv run --no-project python -m pytest \
     python/repark/tests/test_ice_tt_resolve_1.py::test_reader_cells \
     -k TT-DF-VAS-INT-2-load --setup-show --durations=10 -p no:xdist -q
   ```

   Record which fixture owns the seconds. Apply that row's action.

2. Move that build into a worker-scoped fixture without changing what it builds. Where the table says "Leave the file", the move is already done and the file stays byte-identical. Where the table says H-3 or H-4 or `single-consumer`, leave the file and record the hand-back. The only permitted text change is the `scope="module"` addition in §2's last paragraph, and the fixture body stays byte-identical.

3. Prove that each test's assertions and parameters are byte-identical (`git diff` shows fixture plumbing only). `git diff -- python/repark/tests` is empty on the path this tree describes. A non-empty diff may contain only the two decorator edits, and `git diff -U0` must show no `assert` line and no parameter line. Anything else is H-2.

4. Run the full facade suite locally at `-n 8`. The passed, skipped, and xfailed counts must equal the pre-change counts. Provision the way `make py-test-facade` does, then install xdist into that venv without writing the lockfile:

   ```bash
   uv sync --locked --extra numpy --extra pandas --extra polars --extra ml-ext --no-install-package repark
   ( cd python/repark && VIRTUAL_ENV="${VIRTUAL_ENV:-$PWD/../../.venv}" uvx maturin@1.14.1 develop )
   VIRTUAL_ENV="${VIRTUAL_ENV:-$PWD/.venv}" uv pip install --python .venv/bin/python pytest-xdist
   ```

   Pre-change, from the repo root, record the summary line:

   ```bash
   PYTHONPATH=python/repark-parity/src VIRTUAL_ENV="${VIRTUAL_ENV:-$PWD/.venv}" \
     uv run --no-project python -m pytest python/repark/tests -q -n 8
   ```

   When step 2 edits a file, run the same command again. `passed`, `skipped`, and `xfailed` each match, and both lines contain no `failed` and no `error`. When step 2 edits nothing, the one line is both counts. CI-1's historical line `14451 passed, 489 skipped, 147 xfailed` (2026-10-02) is not the gate: the tree has moved, and the gate is this run against itself.

5. Push, and read the new `--durations=25` from `smoke.yml`. Push only a commit that edits a test file from §2. Then:

   ```bash
   gh run list --workflow smoke.yml --branch "$(git branch --show-current)" --limit 5 \
     --json databaseId,headSha,status,conclusion,url
   gh run view <id> --json jobs --jq '.jobs[] | "\(.name)\t\(.startedAt)\t\(.completedAt)\t\(.conclusion)"'
   gh run view <id> --log | grep -E 'slowest 25 durations|s (setup|call|teardown) '
   ```

   Shard wall seconds are `completedAt` minus `startedAt` for `facade shard 0`, `1`, and `2`. When the diff is empty there is no durations push: `smoke.yml` would short-circuit a docs commit, and that short-circuit is not a measurement. `after` in the hand-back stays null.

6. Commit, only when step 2 edited a file. One commit, TRO-Wolf identity, one trailer `Authored-By:` for the engine that ran the order. Subject: `perf(ci-2): reuse immutable facade inputs once per xdist worker (owner, 2026-10-03)`. No `--no-verify`. The commit contains only the test file §2 names. When step 2 edited nothing, there is no commit and `commit` in the hand-back is null.

## 4. Gates and halt rules

| gate | green |
|---|---|
| `git diff -- python/repark/tests` | empty, or only `scope="module"` on `spark` and `spark_ny` in `test_ice_tt_resolve_1.py` |
| the `-n 8` summary | `passed`, `skipped`, and `xfailed` equal the pre-change line; no `failed`; no `error` |
| `git diff -- crates python/repark/tests/conftest.py python/repark/tests/test_perf_ice_catalog_io_1.py .github/workflows/smoke.yml uv.lock` | empty |
| a durations run, when step 5 pushed | three `facade shard` jobs `success`, and `--durations=25` present in the log |

- **H-1.** A count changes. Stop. Hand back both summary lines. Do not re-run to see if it moves, and do not edit a shard rule.
- **H-2.** An assertion or a parameter would change. Stop. Hand back the diff. Do not land it.
- **H-3.** A fixture would be shared across tests that mutate it, because the input is not immutable. Hand back that test. Leave its file unchanged. The other rows still finish.
- **H-4.** A test needs product code. Hand back that test. Leave `crates/` and `conftest.py` unchanged. The other rows still finish.

`table-stale` stops the round the same way H-1 does: no edit, the hand-back names the decorator that disagreed.

## 5. Hand-back

```json
{"unit":"CI-2","before":{"run":37121982216,"shards_s":{"0":289,"1":193,"2":157},"slowest_shard_s":289,"quarter_s":72.25,"ten_sum_s":393.69,"tests":[{"id":"python/repark/tests/test_deep_subquery_expression_1.py::test_thousand_filter_chain_answers_under_subqueries","phase":"setup","s":167.94},{"id":"python/repark/tests/test_deep_filter_chain_crash_1.py::test_thousand_filter_chain_answers_on_dataframe_door","phase":"setup","s":96.74},{"id":"python/repark/tests/test_deep_reverify_1.py::test_sql_text_or_chains_answer_on_every_door","phase":"setup","s":29.75},{"id":"python/repark/tests/test_deep_reverify_1.py::test_gc_collects_deep_cycles_on_main_and_small_threads","phase":"setup","s":21.56},{"id":"python/repark/tests/test_deep_expr_build_small_stack_1.py::test_six_thousand_term_or_build_refuses_through_filter","phase":"setup","s":20.58},{"id":"python/repark/tests/test_h3_spill_matrix.py::test_never_oom_panic_1_tight_pool_join_values_match_or_refuse_typed","phase":"call","s":17.15},{"id":"python/repark/tests/test_perf_ice_catalog_io_1.py::test_peak_rss_over_five_hundred_tables_stays_within_the_default_cache_budget","phase":"call","s":12.11,"excluded":true},{"id":"python/repark/tests/test_ice_views_1.py::test_nested_view_depth_guard","phase":"call","s":10.09},{"id":"python/repark/tests/test_ice_tt_resolve_1.py::test_reader_cells[TT-DF-VAS-INT-2-load]","phase":"setup","s":8.89},{"id":"python/repark/tests/test_ice_tt_resolve_1.py::test_reader_tas_date_past_2262[table-2]","phase":"setup","s":8.88}]},"after":null,"counts":{"before":{"passed":0,"skipped":0,"xfailed":0},"after":{"passed":0,"skipped":0,"xfailed":0},"equal":false},"handed_back":[],"commit":null,"halt":null}
```

Fill `counts` from step 4. Fill `handed_back` with each id from rows 6, 8, 9, and 10 that the table's action handed back, plus the rule id. Fill `after` only from a step 5 run: `run`, `shards_s` for the three facade jobs, and `tests` as `[id, seconds]` for the nine in-scope ids (the memory pin stays in `before` only). `equal` is true when the three counts match. `halt` is null when the round finishes.
