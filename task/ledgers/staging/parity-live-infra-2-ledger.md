# Charter ledger — PARITY-LIVE-INFRA-2 · the nightly live run goes green on its ten reds

**Date:** 2026-10-11 · **Branch:** `test/parity-live-infra-2` · **Base:** `2c33f289`
(main at pickup) · **Model:** Muse Spark (muse-spark-1.3-contributor) · **Policy:** [../../../AGENTS.md](../../../AGENTS.md).
**Path:** STANDARD. **risk_tier: standard.**

**The ten.** The scheduled `parity-live.yml` run of 2026-10-11 on main `2c33f289`:
`10 failed, 16934 passed, 23 skipped, 153 xfailed`, red every night since at least
2026-10-07. Six `test_live_oracle_fixture_reproduces` fail with `FileNotFoundError`
on the box-lane Spark interpreter (present on the shared box, absent on the runner);
`test_live_fnp8_indexed_transform_arrow_shapes[ansi_on]` and `[ansi_off]` fail with
`assert DataType(int32) == DataType(int64)`; `test_compat_smoke_suite_in_subprocess`
fails with 3 of 221 inner tests at `FAIL-MISSING` (`test_hour`, `test_minute`,
`test_second`); `test_live_spark_race_scan_forwards` fails with a Spark-side
`AnalysisException`.

**Local provisioning (not committed).** The venv was synced with the `record` extra
added to the brief's setup line (the Makefile `parity-live` target syncs it; the box
venv otherwise has no PySpark for in-process live tests), and the native module was
built once with the brief's maturin line. No repo file records this; it mirrors the
runner.

**Item 1 (A): one interpreter resolver.** The two passing patterns read: subprocess
recorders via `sys.executable` (`test_ice_overwrite_mode_1.py`) and the in-process
shared session (`test_parity_live_fnp8.py`). Both work on the runner because its venv
carries PySpark 4.1.2 through the `record` extra. No shared helper existed, so
`_spark_python.resolve_live_spark_python` was added: `REPARK_SPARKENV_PYTHON` wins
when set, else the box lane when present, else the current interpreter. The six
reproduce tests and the one other test file carrying the literal
(`test_ice_rdf_options_1.py`, which used to skip without the box lane) call it.

**Item 2B: a converged width.** The Column door over `array(1, NULL, 3)` now answers
`list<item: int32>`; the test pinned int64. Live Spark 4.1.2 (banner `4.1.2 UTC`)
answers `list<element: int32>` with `[[1, None, 5]]` under ANSI on and off, so the
pin was stale and the one flag was corrected to int32.

**Item 2C: both engines refuse TIME.** Each of hour/minute/second classifies
FAIL-MISSING with cause `Error during planning: [UNSUPPORTED_TIME_TYPE] The data
type TIME is not supported. SQLSTATE: 0A000` (oracle frames `test_functions.py:994`,
`:1002`, `:1010`). Live Spark 4.1.2 (banner `4.1.2 UTC`) raises the byte-identical
`AnalysisException` on all three cells, so RePark equals Spark and the PASS pins
(dating to commit `64f34cff`, whose lockfile pins oracle PySpark 4.2.0) are stale
against the 4.1.2 oracle. Demoted to three known-FAIL meta pins (status plus two
cause needles each, bait-checked); the always-PASS count moves 215 to 212 and the
inner total stays 221.

**Item 2D: order-dependence, not a timing race.** Standalone the race passes 20/20
(~10 s each) and the full file passes 21/21, but with any Spark-booting prefix it
fails at the `committed` assert: after `test_ice_evo_dml_1` (1 failed, 291 passed)
and after `test_parity_live_fnp8` (1 failed, 16 passed). An instrumented probe caught
the swallowed Spark error: `[REQUIRES_SINGLE_PART_NAMESPACE] spark_catalog requires
a single-part namespace, but got vn.ns. SQLSTATE: 42K05`. Mechanism: `newSession()`
inherits only shared-context conf, while the `vn` catalog lands in shared conf only
when this module boots the context first (builder conf on an existing context applies
session-only). In the nightly the first PySpark user boots it, so the side session
resolved `vn.ns.conc` against `spark_catalog`. The fix wires the three `vn` confs
onto the side session (test-only, no pin change). Versions compared and identical:
PySpark 4.1.2, PyArrow 25.0.1, pandas 3.0.5, Python 3.12, Java 17. The failing shape
also burned the 180 s parquet-watch deadline on all three rounds, so the fix recovers
about nine minutes of nightly wall clock.

**What is left as a question.** Nothing. All four groups were test faults with
measured evidence; no product defect was found and no pin or workflow change was needed.

## PROPOSITION LEDGER — PARITY-LIVE-INFRA-2 — 2026-10-11

| Clause | Proposition (checkable) | Proof obligation | Verdict | Evidence / open question |
|---|---|---|---|---|
| C-001 | The six reproduce tests and the rdf_options rerun run the recorder under the resolved interpreter on the box and on the runner. | Both resolutions green: box lane as-is, and lane-hidden via TMPDIR with the venv fallback asserted. | PROVEN | `resolve_live_spark_python` in `_spark_python.py`, called from seven tests; `REPARK_SPARKENV_PYTHON` override kept. 7 passed in 271 s with the lane; 7 passed in 238 s with the lane hidden and the fallback asserted equal to `sys.executable`. |
| C-002 | The `array(1, NULL, 3)` indexed-transform case pins the Column door at the width live Spark answers. | Spark measurement on both ANSI settings beside the change; the full file green. | PROVEN | Live Spark 4.1.2 (banner `4.1.2 UTC`): `list<element: int32>`, `[[1, None, 5]]`, ANSI on and off. One flag flipped; `test_parity_live_fnp8.py` 16 passed. |
| C-003 | The hour/minute/second pins assert the shared refusal instead of PASS. | Spark measurement of all three cells; demotion in the meta-pin idiom with a bait check; the wrapper green. | PROVEN | Live Spark 4.1.2 (banner `4.1.2 UTC`) raises `AnalysisException [UNSUPPORTED_TIME_TYPE] The data type TIME is not supported. SQLSTATE: 0A000` on all three, byte-identical to RePark. Three meta pins (FAIL-MISSING + needles), count assert 212, one needle bait-checked red then restored, `test_pyspark_compat_smoke.py` passed. |
| C-004 | The 400k race passes regardless of which module boots the shared context. | Red first in two prefix orders with the Spark error captured; green after in four orders; the 20-run count. | PROVEN | Before: standalone 20/20 passed, but evo_dml+race 1 failed and fnp8+race 1 failed, Spark error `REQUIRES_SINGLE_PART_NAMESPACE ... got vn.ns` (probe). After wiring the side session's `vn` conf: standalone 1 passed, full file 21 passed, fnp8+race 17 passed, evo_dml+race 292 passed. Deterministic order-dependence, not a timing race. |

VERDICT: 4 clauses, 4 PROVEN, 0 REJECTED.

```yaml
COVERAGE_ATTESTATION:
  pr_unit: parity-live-infra-2
  categories:
    - id: AT-1
      status: ATTACKED
      evidence: All ten nightly reds walked, each reproduced and classified before repair. A is the resolver (C-001); B the stale width (C-002); C the shared TIME refusal (C-003); D the side-session catalog wiring (C-004).
      artifacts: [task/ledgers/staging/parity-live-infra-2-ledger.md, python/repark/tests/_spark_python.py]
    - id: AT-2
      status: ATTACKED
      evidence: Resolution edges (env override, box lane present/absent, venv fallback); both ANSI settings for B and the Spark probe; four suite orders for D (standalone, full file, two prefixes).
      artifacts: [python/repark/tests/_spark_python.py, python/repark/tests/iceberg/test_ice_hadoop_vn_1.py]
    - id: AT-3
      status: ATTACKED
      evidence: The demotions pin status plus two cause needles each, so a changed failure mode re-reds; one needle bait-checked. The race keeps its three-round retry and parquet watch untouched.
      artifacts: [python/repark-parity/compat/smoke_suite.py, python/repark/tests/iceberg/test_ice_hadoop_vn_1.py]
    - id: AT-4
      status: ATTACKED
      evidence: The 400k race mechanism measured directly (RePark lands v3 in the shared metadata dir, Spark scan-forwards to v4, count 400002). The D fix touches only side-session conf, never the race itself.
      artifacts: [python/repark/tests/iceberg/test_ice_hadoop_vn_1.py]
    - id: AT-5
      status: ATTACKED
      evidence: Test files and maps only; no workflow, lockfile, or product change. The record extra was provisioned in the local venv only. Runner-vs-box versions compared identical (PySpark, PyArrow, pandas, Python, Java).
      artifacts: [task/ledgers/staging/parity-live-infra-2-ledger.md]
    - id: AT-6
      status: ATTACKED
      evidence: Every corrected expectation carries a live Spark 4.1.2 measurement with banner 4.1.2 UTC: the int32 width (B) and the three TIME refusals (C). D's Spark error captured verbatim from an instrumented probe.
      artifacts: [task/ledgers/staging/parity-live-infra-2-ledger.md, python/repark-parity/compat/smoke_suite.py]
    - id: AT-7
      status: N/A
      justification: No production or hot path touched; tests, pins, and prose only.
    - id: AT-8
      status: ATTACKED
      evidence: newSession inherits shared-context conf only, so a session-scoped catalog binding does not reach it; proven by the failing REQUIRES_SINGLE_PART_NAMESPACE shape and the passing wired shape in the same prefix orders.
      artifacts: [python/repark/tests/iceberg/test_ice_hadoop_vn_1.py, python/repark/tests/_live_parity.py]
    - id: AT-9
      status: ATTACKED
      evidence: No test deleted, skipped, or weakened. Inner smoke total stays 221 (212 PASS + 9 metas); the rdf_options rerun now runs instead of skipping without the box lane (proven green in the lane-hidden run).
      artifacts: [python/repark-parity/compat/smoke_suite.py, python/repark/tests/iceberg/test_ice_rdf_options_1.py]
    - id: AT-10
      status: ATTACKED
      evidence: Red first everywhere: B 2 failed before the flip; C 3 failed before the demotion; D failed in both prefix orders before the wiring; A failed FileNotFound on the runner and passes in both resolutions now.
      artifacts: [python/repark/tests/parity/test_parity_live_fnp8.py, python/repark-parity/compat/smoke_suite.py]
  complete: true
```
