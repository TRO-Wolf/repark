# map — python/repark-parity/tests

## Purpose

Unit tests for the parity comparison core (no Spark, no JVM, no repark required). See
[../map.md](../map.md).

## Contents

- `test_compare.py` — equal/unequal frames, order-insensitivity, null handling, schema/row-count
  mismatches, and a **field-nullability difference** (part of the schema signature — a differing
  `nullable` flag with identical name/type/values is a parity failure).
- `test_compat_harness.py` — C2 / R-PYSPARK-COMPAT unit pins: message-first JVM classification,
  HARNESS narrowness, both denominators (incl. MODULE-TIMEOUT stay-in), `tag_for_pyspark_version`,
  worker env scrub, method-name filter (no prefix steal), `Py4J*` type → NEEDS-JVM,
  `validate_module_short` path-injection refuse, zero-padded tag normalize,
  TimeoutError → MODULE-TIMEOUT (RecordingResult + classify), third-party ImportError → HARNESS
  (incl. site-packages TB + **X1** cache path `repark-pyspark-tests` must not false-FAIL-MISSING;
  **octo C1** site-packages/repark frame + pandas still HARNESS; product `repark.*`
  ModuleNotFoundError + cache path stays FAIL-MISSING),
  unknown status clamp in rank+denominators, module-name dotted IDs, markdown cell escape;
  **C3** `STRETCH_MODULES`/`C3_EXPAND_MODULES` order pin (**U8:** `test_udf` IN);
  **octo C3** `resolve_census_modules` dual-denom pin (`--c3-expand` ignores night-1/`--stretch`);
  C3 series markdown never claims C2 zero-fix /345; ruff-clean import order + format;
  **r20 C4** `C4_EXPAND_MODULES` charter-order pin + `resolve_census_modules(--c4-expand)` dual-denom
  (never blend classic /345 or C3); C4 series/scratch distinct; `_KNOWN_FATAL_TESTS` disjoint
  from C4 cohort + exact-method deselect (not endswith prefix collision); dual-denom markdown %
  pin; filter×fatal dual-denom; testing.utils error rebind pin; PATCH_MAP AssertionError note;
  install_redirect errors-before-factories order; testing.utils rebind via **fake modules**
  (parity isolation — no pyspark/repark); `REPARK_COMPAT_SERIES` worker-only series branding
  pin (no C2 zero-fix on C4 workers; parent ignores leaked env — octo C5); frozen
  `_KNOWN_FATAL_TESTS` exact map pin (octo C6); worker JSON unknown-status clamp.
- **Phase-3 EC-8 additions to `test_compat_harness.py`** (6 tests, new in this repository):
  `CLASSIC_MODULES` charter order + disjointness from both expand cohorts;
  `resolve_census_modules(classic=True)` denominator isolation (hostile `--modules` ignored);
  the fixed precedence `--c4-expand` > `--c3-expand` > `--classic`; the keyword-only default
  that keeps every ported call site unchanged; the **`--stretch` blending pin** (eleven modules,
  not five — the trap `--classic` exists to avoid, documented in executable form); and the CLI
  wiring pin that `--classic` actually reaches the resolver.
- `test_compare_reports.py` — the battery for `compat/compare_reports.py`, the census report
  comparator (NEW code; design §6.4). Over synthetic reports: identical → exit 0; each of the
  five delta directions (pass→fail, fail→pass, class-change, appeared, vanished) named and
  exit 1; both denominators re-asserted; deferred subtraction from the baseline side only, with
  the echo, the not-present entry, and the "deferred id appeared in the candidate" finding;
  quarantine excluded both sides; mismatched environment manifests → loud exit 2 **with no diff
  body at all**; `generated_at` / `repark_version` deliberately not gated; sorted-rendering byte
  exactness (a case-only class difference is still a difference); duplicate ids and malformed
  JSON → loud failure; junit mode with skips first-class (skip→pass detected, xfail
  distinguished from skip, manifests required); and the **provoked undeclared subtraction** that
  proves the checked-in ledger file is the only way a row leaves the diff (five plausible
  environment variables set, an `--exclude` flag attempted, the frozen option set asserted).
  Also pins the three properties that keep the manifest gate from being nominal: an external
  `--manifest-*` file may not overwrite a key the report records (the shared-manifest attack that
  makes two different environments render identical); `pandas_version` is refused when it
  **differs** and equally when it is **unrecorded** (a key nobody records compares equal by
  absence); and each report's own recorded denominator block is validated against its own rows —
  including the case where both sides are byte-identical *and* identically wrong, which the
  post-subtraction re-assert alone cannot catch.
- `test_redact.py` — the battery for `compat/redact.py`, the recorded path-redaction transform.
  Its one hard property is that the artifact still parses afterwards, so the two regressions are
  explicit contrasts: a naive text substitution over a traceback-bearing census report emits an
  unescaped quote and stops being JSON, and one over a JUnit XML turns `<scratch>` into an element
  start tag; the parser-based transform cannot do either. Plus key redaction, non-string scalars
  untouched, XML attributes, longest-prefix-wins ordering, malformed-input loud failures, plain
  text passthrough, in-place rewrite idempotence, and the CLI exit codes.

## Pointers

- Up: [../map.md](../map.md)

## Debug

First checks: `PYTHONPATH=python/repark-parity/src pytest python/repark-parity/tests -q`.
Escalate to: [../map.md#debug](../map.md).
