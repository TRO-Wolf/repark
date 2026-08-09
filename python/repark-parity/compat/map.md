# map — python/repark-parity/compat

## Purpose

The PySpark-compat measurement harness plus **the port's acceptance gate**. The harness runs
Apache's own `pyspark.sql.tests` against the repark facade via a session/bootstrap redirect;
the comparator turns two such runs into a pass/fail verdict. Measurement-only: a failing
Apache test is a FINDING, never a mid-unit product fix. Every cohort keeps its **own
denominators** — classic (/345), C3 expand, and C4 expand2 are never blended.

The recorded end-to-end procedure (environment recipe, cohort argument vectors, the mandatory
stability run and quarantine rule, the full-extras facade cohort, the attribution rule) is
[../../../docs/port/census.md](../../../docs/port/census.md).

## Contents

| Path | Role |
|---|---|
| [`__init__.py`](__init__.py) | Package marker + census class names |
| [`bootstrap.py`](bootstrap.py) | Redirect seam (`install_redirect`) + **patch map** deliverable (incl. classic.*); overlay uses source `__all__` when present (octo X2 C5 — no `re`/`json` pollution); **E2:** `ReusedSQLTestCase` seeds in-memory `spark_catalog` + `default` NS for bare-table Apache paths; **F1:** also overlays private `_merge_type` / `_make_type_verifier` from repark.types; **C4 octo C2–C3/C8:** errors overlay rebinds already-imported `pyspark.testing*` exception names; PATCH_MAP errors notes name **PySparkAssertionError** + check_error/assert* |
| [`fetch.py`](fetch.py) | Runtime sparse-clone of Spark tag → `~/.cache/repark-pyspark-tests/<tag>/` |
| [`classify.py`](classify.py) | PASS / FAIL-* / NEEDS-JVM / HARNESS / SKIP-UPSTREAM / MODULE-TIMEOUT (message-first JVM) |
| [`runner.py`](runner.py) | Subprocess-per-module census CLI + markdown/JSON report writer (`_md_cell` table escape); secret-scrubbed worker env; SIGALRM → MODULE-TIMEOUT; `STRETCH_MODULES` = classic column/readwriter + C3 expand (`test_group`/`test_session`/`test_conf`/`test_catalog`/`test_sql` + **U8 `test_udf`**); **phase-3 EC-8:** additive `CLASSIC_MODULES` + `--classic` (the isolated /345 cohort; precedence c4 > c3 > classic > `--modules`/`--stretch`) — `--stretch` is left byte-identical and is NOT the classic cohort (it appends the C3 modules; a harness test pins that blending behavior); `resolve_census_modules` + `--c3-expand` / **`--c4-expand`** own denoms (never blend classic /345; C4 never blends C3; stretch names re-validated; ruff format); series-labeled reports (`C3 / R-CENSUS-EXPAND`, **`C4 / R-CENSUS-EXPAND2`**, C2); C3 default scratch `…/c3-expand`; **C4** scratch `…/census/c4-expand2` + `C4_EXPAND_MODULES` nine-module charter order; **U8:** `test_udf` in `STRETCH_MODULES` + `C3_EXPAND_MODULES` (DF half shipped; own census module); **r20 C4** owns module lists + `_KNOWN_FATAL_TESTS` (exact method-name deselect; C4 cohort keys forbidden); **octo C4–C5:** `REPARK_COMPAT_SERIES` worker env (honored only with `--worker`) so single-module workers keep C3/C4 findings branding without rebranding classic parent runs |
| [`compare_reports.py`](compare_reports.py) | **NEW in this repository** (design §6.4): two census JSON reports (or two JUnit XMLs with `--junit`) in, a verdict out. Manifest comparison FIRST (loud exit 2 before any diff); deferred ledger subtracted from the BASELINE side only and echoed; quarantine excluded both sides and reported separately; sorted-rendering **byte** comparison; both denominators recomputed via `classify.denominators`; delta grouped pass→fail / fail→pass / class-change / appeared / vanished; exit 1 on any difference. **The checked-in ledger files are the only subtraction inputs** — the module reads no environment and its option set is frozen. Three properties keep the first gate from being nominal: gated keys include `pandas_version` / `pyarrow_version`; `python_version` + `pandas_version` (+ `pyspark_version` in census mode) must be **recorded**, since a key nobody records compares equal by absence; and an external `--manifest-*` file may only **augment** a report's own manifest — a contradiction is exit 2, so the CLI cannot fabricate agreement. Each report's **own recorded** denominator block is also validated against the rows it carries (the post-subtraction re-assert is implied by the byte comparison and cannot catch a malformed report). **PR-5 fixer:** `junit_node_id` canonicalizes ledger ids into the JUnit id space before subtraction (`tests/x.py::t` → `tests.x::t`) — without it a ledger written in collect-only form subtracts NOTHING in `--junit` mode, which is the only mode the facade cohort has; the translation runs path→dotted because that direction is total (dotted→path cannot say where the module ends and the class begins) |
| [`redact.py`](redact.py) | **NEW in this repository** (docs/port/census.md §3): the recorded path-redaction transform, applied identically on both sides before artifacts are committed or compared. Format-aware **through the parser** — JSON is loaded/rewritten/re-serialized, XML likewise, everything else is plain text — so a redacted census report is valid JSON and a redacted JUnit XML is well-formed **by construction**. Tokens `<scratch>` / `<repo>` / `<home>`, longest prefix first. A textual substitution over these bytes breaks the JSON string escaping and turns an angle-bracketed token into an XML start tag; that failure is pinned as a regression in `../tests/test_redact.py` |
| [`__main__.py`](__main__.py) | `python -m compat` entry |
| [`smoke_suite.py`](smoke_suite.py) | Always-green Apache pin list + meta-pins (X1+X2+X3+E1+E2: tip PASS pins — **96** after E2 +4 ndarray; F2: known-FAIL meta = `test_field_accessor` FAIL-VALUE nested dotted resolve; pin list / exact-count untouched) |

## I want to...

| ...do this | go to |
|---|---|
| Run the classic /345 cohort | `… -m compat.runner --classic` (never `--stretch` — it blends C3 in) |
| Run C3 expand census only (own denoms) | `… -m compat.runner --c3-expand` (not blended with classic /345) |
| Run C4 expand2 census only (own denoms) | `… -m compat.runner --c4-expand` (not blended with classic /345 or C3) |
| See what the redirect patches | `bootstrap.PATCH_MAP` / `patch_map_as_markdown()` |
| Refresh the Apache test cache | `fetch.ensure_spark_tests(force=True)` or delete `~/.cache/repark-pyspark-tests/` |
| Pin always-green Apache cases | `smoke_suite.py` + the facade's `test_pyspark_compat_smoke.py` (arrives with the facade package) |
| Compare two census runs (the gate) | `… -m compat.compare_reports --baseline <v1>.json --candidate <v2>.json --deferred … --quarantine … --manifest-baseline … --manifest-candidate …` (the manifests are required in census mode too — they carry `pandas_version`) |
| Redact scratch paths out of artifacts | `… -m compat.redact --map "$SCRATCH=<scratch>" --map "$PWD=<repo>" --map "$HOME=<home>" <artifacts>` — never `sed` |
| Read the recorded reports | `task/census/` (committed as evidence by the baseline and phase-close PRs) |

## Pointers

- Up: [../map.md](../map.md)
- Unit ledger: [p3d-parity-ledger.md](../../../docs/history/port-v2/p3d-parity-ledger.md)
- Procedure: [`../../../docs/port/census.md`](../../../docs/port/census.md);
  design: [`../../../docs/design/python-facade.md`](../../../docs/design/python-facade.md) §5 F1, §6

### Comparator duplicate/denominator semantics (PR-4 orchestrator pass)
- Duplicate `test_id`s refuse loudly with ONE escape: ids named in the quarantine ledger may
  repeat (first row wins; the v1 runner's expand `test_udf` pair is the recorded case).
- A report's recorded denominator block is validated against its rows AS CARRIED (duplicates
  included, `Side.carried_statuses`); the deduped `classes` drive the comparison.

### PR-7: --added (milestone one)
- The comparator gains `--added` — the mirror of `--deferred`: a checked-in v2-only-additions
  ledger (`task/port/added-python-tests.txt`) subtracted from the CANDIDATE side, echoed,
  junit-canonicalized, in the frozen option set. Identity `(candidate - added) U deferred =
  baseline`. First consumer: the PR-7 facade acceptance comparison.

## Debug

| Symptom | First check |
|---|---|
| `ModuleNotFoundError: pyspark.sql.tests` | Cache missing / `install_redirect` not given `spark_tests_root` |
| All tests `NEEDS-JVM` | Redirect factories not applied — import order (bootstrap before suite) |
| Worker HARNESS on import | `PYTHONPATH` must include `python/repark-parity` **and** the facade's `src` |
| JVM gateway starts | `ReusedSQLTestCase.setUpClass` not patched; check `bootstrap.redirect_log()` |
| MODULE-TIMEOUT | Default wall 20 min/module; raise `--timeout` only for debug, never to hide hangs. Worker uses SIGALRM (Unix); parent always has subprocess hard-kill +30s. |
| False NEEDS-JVM on `.json` / source `parallelize` | Classifier must be message-first — see unit pins in `../tests/test_compat_harness.py` |
| pandas ImportError on assertDataFrameEqual classed FAIL-MISSING | `_is_third_party_import` must not treat `~/.cache/repark-pyspark-tests` path as product repark (X1 pin); octo C1: also must not treat `site-packages/repark` / `python/repark/` frames as product — message-first product detection only |
| Smoke pin count drift | `smoke_suite.py` exact count assert |
| Comparator refuses to diff | Environment manifests differ — that is the gate working; re-provision to the pinned recipe, do not bypass |
| `ENVIRONMENT NOT RECORDED` | The run's freeze half never reached the comparator: pass `--manifest-baseline` / `--manifest-candidate` pointing at each run's `census-manifest.json`. Do NOT "fix" it by deleting the key from the required list |
| `EXTERNAL MANIFEST CONTRADICTS` | The external manifest disagrees with the report's own recorded value. One of the two is wrong about the environment the run actually had — find out which; the flags cannot override a report |
| `recorded denominators disagree with the rows it carries` | The report is malformed (its `denominators` block does not describe its own `modules[].rows[]`). Regenerate the run; never patch the block |
| A committed artifact will not parse | It was redacted textually instead of through `compat.redact`. Regenerate — evidence is never hand-repaired |
| A row "should not count" | Put it in a checked-in ledger file (deferred or quarantine) or it counts. There is no flag and no env var |
| Clone hang / bad tag | `fetch.CLONE_TIMEOUT_S`; tags must match `v?\d+(\.\d+)*` |
| Worker JSON path outside scratch | `validate_module_short` — no `..` / path separators in `--modules` |
| Import of `pyspark.sql.tests` fails | Classified **HARNESS** (injection seam), not FAIL-MISSING |
| `Py4JJavaError` → FAIL-VALUE | Type name is NEEDS-JVM (`py4j` embedded; not only `\bpy4j\b` messages) |
| MODULE-TIMEOUT lost mid-test | `_RecordingResult` marks TimeoutError + `stop()`; post-suite emits MODULE-TIMEOUT |
| pandas ImportError as FAIL-MISSING | Third-party import markers → HARNESS |
| `from pyspark.sql.column import Column` still JVM | Bootstrap patches submodule `column`/`dataframe` (+ classic) |

First checks: run one test with `--inprocess --modules test_dataframe --filter test_range -v`.
Escalate to: [../map.md#debug](../map.md).

- 2026-08-01 rider: `smoke_suite.py` (moved from repark/tests) — the C2 smoke MUST run in a
  pristine subprocess: the redirect patches pyspark permanently and live-pyspark oracles boot a
  JVM; in-process union runs are order-dependent both directions.

<!-- 2026-08-02: r16 combine rider — known-fail meta pin FAIL-MISSING→FAIL-VALUE (session.conf landed; wall moved to ndarray-lit cast) -->

<!-- 2026-08-06: r18 sole-owner pin bump (Q4-B) — smoke pin list regenerated from the dual census PASS union: 143 exact (classic 5 modules + C3 expanded 5) -->

<!-- 2026-08-08: r19 — _deselect_known_fatal (Apache deliberate-segfault tests → NEEDS-JVM rows; in-process UDF divergence); sole-owner pin bump 143→158 -->

<!-- 2026-08-03 (r20 combine): smoke pin list regenerated from mega-tip census PASS union — 158 → 215 (classic 126 + C3 38 + C4 expand2 51); exact-count assert bumped. -->

<!-- 2026-08-03 (r20 combine): pin list 215 -> 205 — ten pandas-3-sensitive rows dropped (Apache helpers import pandas.core.common._builtin_table, removed in pandas 3; HARNESS under uv.lock pandas 3.0.3). Pins verified green under the record-extra locked env. -->

<!-- 2026-08-03 (r21 combine): pins regenerated from r21 census union 261 -> 206 always-green (55 pandas-3 _builtin_table-class rows excluded; validated green in BOTH venvs). -->

<!-- 2026-08-03 (r22): pins 206 -> 214 (union 269 - 55 pandas-3 class). -->

<!-- 2026-08-04 (r23): pins 214 -> 218 (union 273 - 55 pandas-3 class), both-venv validated. -->

<!-- Phase-3 PR-4 (V2 port): ported verbatim except the additive `--classic` cohort
     (EC-8), the NEW `compare_reports.py`, and this map's truthful re-pointing of five v1-only
     links (facade smoke path, the v1 unit ledger, two v1 briefs, the v1 report directory). -->
