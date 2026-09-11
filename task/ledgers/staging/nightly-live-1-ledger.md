# Charter ledger — NIGHTLY-LIVE-1 · the parity-live nightly's shared PySpark oracle dies mid-run

**Date:** 2026-09-11 · **Branch:** `fix/nightly-live-1` · **Base:** `origin/main`
`75406e4` · **Model:** swe-2-high · **Policy:** [../../../AGENTS.md](../../../AGENTS.md).
**Path:** STANDARD. **risk_tier: standard.**

**Why now.** `.github/workflows/parity-live.yml` runs
`uv run --locked --no-sync pytest python/repark/tests -q` under `REPARK_PARITY_LIVE=1`. It was
green on 2026-09-04 and red every night since (72 failures on 09-05, 111 on 09-11). Every failure
raises `SparkException: [INTERNAL_ERROR] No active or default Spark session found` or a follow-on.
PySpark runs every session on ONE JVM `SparkContext`, so a test calling `.stop()` on any PySpark
session — even a session it built itself — kills the context under the session-cached oracle
`Engine` built by `_live_parity.build_spark_engine`, and every later oracle call fails. The
regression entered in the 2026-09-04 merge window: `test_cutover_schema_1.py` became the first
alphabetical consumer of the shared `spark_engine` fixture, so the context now exists *before*
the later modules that stop their sessions run.

**The fix.** An autouse function-scoped guard in `conftest.py` turns "111 downstream failures"
into "one named culprit": under the live flag, once `pyspark` is imported, it asserts after each
test that the captured oracle `SparkContext` (`sparkContext._jsc`) is still the live one — the
test that stopped it fails in teardown, naming itself. Every genuine PySpark `.stop()` in the
suite is then removed (`if owned:` stops included — a "private" session shares the same context,
so ownership is not a defense); `ReparkSession.stop()` (no JVM) is untouched, as are the
`_record_*` scripts, which are not collected by pytest. `spark_engine` itself no longer stops
the oracle at session teardown — the JVM exits with the pytest process.

**Shared-context consequences measured and repaired.** With one context for the whole run,
two previously-masked states surfaced: (a) a hadoop Iceberg catalog binds its warehouse at first
use and never re-reads `spark.sql.catalog.<name>.warehouse`, so lifecycle sites that assumed a
private catalog now share `local` — the suite's own convention is a unique catalog name per site
(`v312legacy`, `v3cov`, `sqlh1`, `v3_11_file_order`), so the five broken sites got private names
(`v3e5part`, `v3e5del`, `v3_10_upg`, `wo_meta`, `wo_nested`) via a new `catalog` kwarg on
`build_spark_iceberg_engine`; (b) the shared context is `local[1]` (first module wins), and
`test_perf_agg_avg_1`'s overflow pin was measured on the oracle's `local[2]` — Spark answers
`NUMERIC_VALUE_OUT_OF_RANGE` at leaf parallelism 2 and `ARITHMETIC_OVERFLOW` at 1 (both measured),
so the leg pins `spark.sql.leafNodeDefaultParallelism=2` through `spark_session_conf`, which now
restores never-set keys via `conf.unset`. One stale pin surfaced too: NULLABILITY-2 (#393)
converged the Spark-door `map` flag to non-null (its commit names it "Spark-measured"), and the
live re-coercion cell still expected the retired divergence — the cell now asserts the converged
`(False, False)`, matching the file's own non-live pin at line 207.

## PROPOSITION LEDGER — NIGHTLY-LIVE-1 — 2026-09-11

| Clause | Proposition (checkable) | Proof obligation | Verdict | Evidence / open question |
|---|---|---|---|---|
| C-001 | The conftest guard turns the downstream-failure wave into a named culprit: on the base tree, the test that stops the shared context fails in teardown with its own nodeid. | `_shared_oracle_context_guard` in `conftest.py`, red on the base tree before any fix. | PROVEN | Red-first run (three files, guard active, culprits still in place): `ERROR at teardown of test_cross_engine_collect_and_multi_count_distinct_vs_pyspark` — `Failed: tests/test_group_agg.py::test_cross_engine_collect_and_multi_count_distinct_vs_pyspark stopped the shared PySpark SparkContext the live oracle runs on. Every PySpark session shares the one context, so no test in this suite may call .stop() on a PySpark session.` — and the six downstream `test_nullability_2.py` live tests red on the killed context (`No active or default Spark session found`), exactly the nightly signature. |
| C-002 | The full live suite passes on this box: `JAVA_HOME=/usr/lib/jvm/zulu-17-amd64 REPARK_PARITY_LIVE=1 uv run --locked --no-sync pytest python/repark/tests -q` reports 0 failed. | The full-suite run, count recorded. | PROVEN | `6298 passed, 25 skipped, 65 warnings in 1316.97s (0:21:56)` — all seven post-fix residuals (five shared-catalog collisions, the `leafNodeDefaultParallelism` overflow class, the stale map pin) green. |
| C-003 | No test lost: the diff removes `.stop()` calls, `try/finally` teardown wrappers and two shared-catalog names only; no test is deleted, skipped, or weakened, and `ReparkSession.stop()` calls stand. | The diff itself + the collected-test count. | PROVEN | `git diff` touches zero `def test` lines (0 added, 0 removed); the types-1 map cell re-pins measured reality post-#393 rather than weakening (the file's own non-live pin already asserts `False`); collected count 6298+25 = 6323, identical to the pre-fix run's 6291+7+25. |
| C-004 | All required gates green on the final tree: `make verify`, the whole `python/repark-parity` suite, `make preflight`. | The three commands, exit codes recorded. | PROVEN | `make verify` exit 0 (clippy, panic-ban, crate-dag, lib-rs, both file-size gates, docstring, manifest, cargo test); parity suite `738 passed, 1 skipped, 11 xfailed`; `make preflight` — verify + facade `5928 passed, 369 skipped` + parity-cap 23 + dbt `59 passed, 1 skipped` + cargo deny/audit + pip-audit + workflows-parse all green; `workflows-lint` (`zizmor .`) cannot run its online `artipacked` audit in this sandbox (401 on github.com git-upload-pack) — `zizmor --no-online-audits .` reports no findings and the diff touches no workflow file. |

VERDICT: 4 clauses, 4 PROVEN, 0 REJECTED.

```yaml
COVERAGE_ATTESTATION:
  pr_unit: nightly-live-1
  categories:
    - id: AT-1
      status: ATTACKED
      evidence: Every clause walked against the brief. The guard names the culprit (C-001 red-first), the suite is zero-failed (C-002), nothing was deleted or weakened (C-003), and the gates ran to their exit codes (C-004). The seven residual failures the first green-context run exposed were diagnosed, fixed and re-measured — reported, not folded.
      artifacts: [task/ledgers/staging/nightly-live-1-ledger.md, python/repark/tests/conftest.py]
    - id: AT-2
      status: ATTACKED
      evidence: Guard inert outside the live tier (REPARK_PARITY_LIVE unset) and cheap when pyspark was never imported (sys.modules check before any JVM call); a context swap (same-context newSession passes, a rebuilt context fails) and a stopped context are both caught; spark_session_conf restores a never-set key via conf.unset after measuring that set(None) raises IllegalArgumentException; private catalogs bind lazily to their own warehouse (probed: cat2 -> w2 while local stayed bound to w1).
      artifacts: [python/repark/tests/conftest.py, python/repark/tests/_live_parity.py]
    - id: AT-3
      status: ATTACKED
      evidence: The guard's failure names the offending test's nodeid and states the rule. The removed stops were teardown only; DROP TABLE and rmtree restoration survive everywhere a helper mutated shared state. No new production error path exists — all edits are test-side.
      artifacts: [python/repark/tests/conftest.py, python/repark/tests/test_v3_live_oracle.py]
    - id: AT-4
      status: ATTACKED
      evidence: The shared JVM context IS this unit's concurrency surface: one SparkContext serves the session-scoped oracle and every test-built session. The guard serializes detection through pytest's per-test teardown; no thread, lock or async was added anywhere.
      artifacts: [python/repark/tests/conftest.py]
    - id: AT-5
      status: ATTACKED
      evidence: Test-code-only change: no secrets logged, no path traversal added (warehouses stay under mkdtemp/tmp_path), no workflow or CI surface touched, no dependency change. zizmor offline audit clean; the online artipacked audit is unreachable from this sandbox (401 on github.com git-upload-pack) and is recorded, not bypassed.
      artifacts: [docs/testing.md, task/ledgers/staging/nightly-live-1-ledger.md]
    - id: AT-6
      status: ATTACKED
      evidence: Every changed expectation was measured on the pinned live PySpark 4.1.2: local[1] vs local[2] produced ARITHMETIC_OVERFLOW vs NUMERIC_VALUE_OUT_OF_RANGE respectively, leafNodeDefaultParallelism=2 restored the pinned class on the shared context, the spark-door map flag measured False (matching nullability-2's recorded convergence), and the hadoop catalog's init-time warehouse binding was probed live before the private-name fix.
      artifacts: [python/repark/tests/test_perf_agg_avg_1.py, python/repark/tests/test_types_1.py]
    - id: AT-7
      status: N/A
      justification: No production or hot path touched; the guard adds one getActiveSession/isStopped call per test under the live tier only, and the suite wall time stayed at ~22 min.
    - id: AT-8
      status: ATTACKED
      evidence: PySpark RuntimeConfig semantics (get returns None for unset keys, set(None) raises, unset exists in 4.1) were verified against the installed package, not assumed; SparkSession.getOrCreate's application of builder options to an existing session and the hadoop catalog's lazy first-use binding were both probed live; Iceberg register_table syntax was reused verbatim from the passing siblings.
      artifacts: [python/repark/tests/_live_parity.py]
    - id: AT-9
      status: ATTACKED
      evidence: A silently dead oracle cannot pass: any later oracle call raises No active or default Spark session and the guard additionally fails the stopper itself. A silently lost test cannot pass C-003: the diff touches zero def test lines and the collected count is identical pre- and post-fix (6323). No test was skipped to reach green — the live legs all ran.
      artifacts: [python/repark/tests/conftest.py, task/ledgers/staging/nightly-live-1-ledger.md]
    - id: AT-10
      status: ATTACKED
      evidence: Red-first held: the guard was run on the unfixed tree and failed the named culprit while six downstream tests died on the killed context. The seven residuals were each reproduced (standalone pass vs in-suite fail isolated the parallelism leak; AlreadyExists and empty-metadata-glob proved the catalog binding) before their fixes, and the polluter-first ordering was re-run to prove the private catalogs, not luck, carry the fix.
      artifacts: [python/repark/tests/test_v3_live_oracle.py, python/repark/tests/test_write_order_dist_1.py]
  complete: true
```
