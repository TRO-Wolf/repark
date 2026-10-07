# Charter ledger — PARITY-LIVE-STOP-1 · no test stops the shared live-oracle context

**Date:** 2026-10-07 · **Branch:** `fix/parity-live-stop-1` · **Base:** `origin/main`
`b5214494` · **Model:** claude-opus-5-5 · **Policy:** [../../../AGENTS.md](../../../AGENTS.md).
**Path:** STANDARD. **risk_tier: standard.** **Card:**
[v1-5-3-card-2026-10-04.md](../../roadmap/mid-term/v1-5-3-card-2026-10-04.md) `## PARITY-LIVE-STOP-1`.

**Owner, 2026-10-04:** "The fix is the live-cell rule (guard the shared session, never stop it),
nothing more. Not a release item."

**What was already on main.** `29a87501` (2026-10-04, Muse lane) removed the PySpark `.stop()`
from the three in-process recorders the live tier runs on the shared context
(`_record_ice_meta_delete_1.py`, `_record_ice_procs_route_1_oracle.py`,
`_record_ice_sorted_insert_2_oracle.py`), and left the card open with no ledger. The line the
owner named, `test_ice_meta_delete_1.py:131`, is `ReparkSession.stop()` in the RePark-only
`replayed` fixture: no JVM, untouched.

**How the measurement was taken.** A scratch pytest plugin (outside the repository, never
committed) wrapped `SparkSession.stop`, `SparkContext.stop`, `SparkContext.__init__`,
`SparkSession.newSession` and `Builder.getOrCreate`, recording the running nodeid, the calling
test-file frames and every builder option that differed from the live session. It ran
observe-only over the full nightly command, and in a suppressing mode over the 126 test files
that can reach PySpark (5,292 tests, collected in a shuffled order).

**What it found.** No stop and no rebuild anywhere. One live-cell violation remained: three of
the formerly-stopping recorders bind the same generic Spark catalog name `sc` — procs-route as
an `InMemoryCatalog`, meta-delete and sorted-insert-2 as Hadoop catalogs at their own warehouses.
An Iceberg catalog binds its warehouse on first use for the shared session's life, so whichever
module runs first owns `sc` and the others write into its warehouse. Alphabetical order (the
nightly's) happens to stay green; the shuffled order failed `test_ice_sorted_insert_2.py`.

**The fix.** The meta-delete and sorted-insert-2 recorders now bind module-private catalogs
(`ice_meta_delete_1_live`, `ice_sorted_insert_2_live`), and the meta-delete replay registers its
RePark memory catalog under the recorder's `CATALOG` instead of a literal. Procs-route keeps `sc`
and `hc` — its committed fixture records `sc.ns` SQL — and is now their only user on the live
tier. Neither recorded fixture names a catalog (0 `sc.` occurrences in either JSON), so no
fixture changes. No `.github` file changes.

## PROPOSITION LEDGER — PARITY-LIVE-STOP-1 — 2026-10-07

| Clause | Proposition (checkable) | Proof obligation | Verdict | Evidence / open question |
|---|---|---|---|---|
| C-001 | No test collected by `pytest python/repark/tests` stops, replaces or rebuilds the shared PySpark context under `REPARK_PARITY_LIVE=1`, and the guard fires on no test. | The full nightly command on the base tree with the observe-only probe; the guard's failure text counted. | PROVEN | Base `b5214494`, `JAVA_HOME=/usr/lib/jvm/zulu-17-amd64 SPARK_LOCAL_IP=127.0.0.1 REPARK_PARITY_LIVE=1 uv run --locked --no-sync pytest python/repark/tests`: `4 failed, 15968 passed, 21 skipped, 153 xfailed in 7210.26s`, 0 errors, 0 `stopped the shared PySpark SparkContext` hits. Probe: 0 `SparkSession.stop`, 0 `SparkContext.stop`, 1 `SparkContext.__init__` (the first PySpark test, `test_applyinpandas_oracle.py`), 3 `newSession` (`test_ice_hadoop_vn_1.py::test_live_spark_race_scan_forwards`, a same-context side session, out of scope). The 4 failures are the card's out-of-scope set: `test_ice_hadoop_vn_1` race, `test_parity_live_fnp8` indexed transform ×2, `test_pyspark_compat_smoke`. Shuffled 126-file pass with stops suppressed: still 0 stops recorded. |
| C-002 | The formerly-stopping recorders bind no Spark catalog name another live module binds: meta-delete and sorted-insert-2 use module-private names, and `sc`/`hc` are procs-route's alone. | Red first on the base tree in the colliding order, green after; the probe's catalog map over the live tier. | PROVEN | Red, base: `pytest test_ice_procs_route_1.py test_ice_sorted_insert_2.py -k live` → `FAILED test_ice_sorted_insert_2.py::test_live_cells_match_fixture`, `FileNotFoundError: …/test_live_oracle_matches_recor0/live-wh/w/m3/data/…` (procs-route's warehouse, reached through `sc`). Green, fixed: the same order and two more passes below, every live cell `PASSED`. Probe catalog map on base: only `sc` (three modules, conflicting `catalog-impl` and `type`) and `local` (the lifecycle default, by design shared, every user green) span modules. |
| C-003 | With the formerly-stopping modules first, the live modules after them stay green and the guard stays silent, in both the colliding and the nightly order. | Three ordered runs under `REPARK_PARITY_LIVE=1`, two with the probe and one plain. | PROVEN | (1) procs-route, sorted-insert-2, meta-delete, then `test_ice_evo_dml_1`, `test_cast_map_spell_1`: `477 passed, 6 xfailed`, 0 stops. (2) meta-delete, procs-route, sorted-insert-2, then `test_ice_tt_resolve_1`, `test_ice_list_null_1`: `530 passed, 4 xfailed`, 0 stops. (3) plain nightly command, procs-route, sorted-insert-2, meta-delete, `test_ice_evo_dml_1`, `test_ice_tt_resolve_1`: `560 passed, 4 xfailed`, 0 failed, 0 guard hits. |
| C-004 | The diff is test-side and narrow: two catalog constants and the replay's catalog registration; no test deleted, skipped or weakened, no fixture or workflow file changed, no code comment added; the gates are green. | `git diff --stat origin/main`; the gate commands with exit codes. | PROVEN | Three Python files, 6 insertions, 4 deletions; zero `def test` lines touched; `ice_meta_delete_1_spark_oracle.json` and `ice_sorted_insert_2_spark_oracle.json` unchanged. `ruff check` and `ruff format --check` exit 0; `check_python_conventions.py` exit 0; `sync_map_md.py --check` exit 0; `check_ledger_grammar.py` clean; the comment-ban scan reports 0 hits. |

VERDICT: 4 clauses, 4 PROVEN, 0 REJECTED.

```yaml
COVERAGE_ATTESTATION:
  pr_unit: parity-live-stop-1
  categories:
    - id: AT-1
      status: ATTACKED
      evidence: Every scope line of the card walked. Find every stop (C-001, measured over the full nightly command, not grepped — the grep's 300 `.stop()` lines are almost all RePark sessions named spark); apply the live-cell rule to each formerly-stopping recorder (C-002); prove no cascade with the stopper first (C-003). The owner-named line 131 was read and is a RePark stop.
      artifacts: [task/ledgers/staging/parity-live-stop-1-ledger.md, python/repark/tests/_record_ice_meta_delete_1.py]
    - id: AT-2
      status: ATTACKED
      evidence: Ordering is the edge — the collision is order-dependent, so the colliding order (procs-route first), the nightly order (meta-delete first) and a shuffled 126-file order were each run. A private name binds lazily to its own warehouse, so a second module can no longer redirect the first's writes.
      artifacts: [python/repark/tests/_record_ice_sorted_insert_2_oracle.py, python/repark/tests/test_ice_meta_delete_1.py]
    - id: AT-3
      status: ATTACKED
      evidence: The guard's failure path is unchanged and still names the stopper. The recorders keep their own try/except error capture per cell; a failed live cell surfaces as drift, never as a silent pass.
      artifacts: [python/repark/tests/conftest.py]
    - id: AT-4
      status: ATTACKED
      evidence: The shared JVM context is the concurrency surface. One SparkContext was constructed per run in every measurement, and the only side session (`newSession` in the out-of-scope hadoop-vn race test) shares the context and passes the guard by design.
      artifacts: [python/repark/tests/conftest.py, python/repark/tests/test_ice_hadoop_vn_1.py]
    - id: AT-5
      status: ATTACKED
      evidence: Test-only change. No environment variable popped or set, warehouses stay under pytest tmp paths, no workflow file touched (the owner-gated `parity-live.yml` is unchanged), no dependency change, no network beyond the pinned Ivy cache.
      artifacts: [python/repark/tests/_record_ice_meta_delete_1.py]
    - id: AT-6
      status: ATTACKED
      evidence: Every expectation was re-derived on live PySpark 4.1.2 with Iceberg 1.11.0 under the private names; neither committed fixture names the catalog, so the oracle values are byte-identical and the live cells pass against them.
      artifacts: [python/repark/tests/ice_meta_delete_1_spark_oracle.json, python/repark/tests/ice_sorted_insert_2_spark_oracle.json]
    - id: AT-7
      status: N/A
      justification: No production or hot path touched; two constants and one registration in test code.
    - id: AT-8
      status: ATTACKED
      evidence: PySpark 4.1.2 getOrCreate on an existing session applies only modifiable settings through applyModifiableSettings (read from the installed package), which is why a recorder's catalog options reach the shared session and why the first binder of a name wins.
      artifacts: [python/repark/tests/_record_ice_sorted_insert_2_oracle.py]
    - id: AT-9
      status: ATTACKED
      evidence: A lost test cannot hide a pass — no def test line changed and the collected counts match the base run. The out-of-scope failures stay red and are named, not skipped.
      artifacts: [task/ledgers/staging/parity-live-stop-1-ledger.md]
    - id: AT-10
      status: ATTACKED
      evidence: Red first on the unfixed tree in the colliding order (FileNotFoundError into procs-route's warehouse), then green in three orders after the fix, one of them the plain nightly command with no probe loaded.
      artifacts: [python/repark/tests/test_ice_sorted_insert_2.py, python/repark/tests/test_ice_procs_route_1.py]
  complete: true
```
