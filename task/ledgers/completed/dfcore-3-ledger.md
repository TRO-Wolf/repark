# Unit ledger — DFCORE-3 · the statistics family out of `core.py`

**Date:** 2026-09-07 · **Branch:** `refactor/dfcore-3` · **Base:** `origin/main`
`f0fb6ab3` (PR #415, DFCORE-2) · **Model:** muse-spark-1.3 · **Policy:** [../../../AGENTS.md](../../../AGENTS.md).
**Path:** STANDARD. **risk_tier: standard.**
**Registry:** `DFCORE-3` **FIXED**.

**Retires:** this ledger moves to `../completed/` when the unit's last commit lands.

**Why now.** The decomposition plan
(`task/roadmap/epic-term/dataframe-core-decomposition-plan-2026-09-07.md`, §4 row DFCORE-3)
opens the third slice: the six statistics bodies (`describe`, `summary`,
`approxQuantile`, `corr`, `cov`, `crosstab`) leave `core.py` and the `freqItems`
refusal leaves `DataFrameStatFunctions`, all for one leaf module
`statistics.py`; the public methods stay as one-line wrappers so the class
surface is frozen, and an extended export pin proves the surface moved exactly
as declared.

**Not in this unit:** `STATUS.md`, `briefs/next-sequence.md`, `.github/`,
`Cargo.lock`, dependency lists, completed ledgers, any behaviour change,
`sampleBy` (DFCORE-4a owns it), the `approxQuantile` batching (DFCORE-5 owns
it; the per-probability collect loop is preserved byte-for-byte and its count
is recorded below as the DFCORE-5 baseline).

**Environment.** The lane native was stale on arrival: `test_perf_approxpct_1.py`
failed 57 tests with `PyColumn.approx_percentile_cont() takes 1 positional
argument but 2 were given`. Rebuilt via `make develop` (exit 0); the same file
then passes inside the 328-test battery. This mirrors DFCORE-1 round 2 and
DFCORE-2 (R2-S2 class).

## PROPOSITION LEDGER — DFCORE-3 — 2026-09-07

| Clause | Proposition (checkable) | Proof obligation | Verdict | Evidence / open question |
|---|---|---|---|---|
| C-001 | The package export surface equals the pre-slice surface under the DFCORE-1 filter; the unfiltered delta is exactly the gain of `statistics`; nothing lost. | `test_dfcore_1_exports.py::test_package_export_set_unchanged`. | **PROVEN** | Pin 9 passed. `dir()` 163 → 164; gained exactly the seven submodule names (DFCORE-1's four plus DFCORE-2's two plus `statistics`); lost none. Pre-move provocation reds exactly the declared delta. |
| C-002 | The core export surface equals the pre-slice surface under the same filter; the unfiltered delta is exactly the `statistics` module binding; `__annotations__` still absent. | `test_dfcore_1_exports.py::test_core_export_set_unchanged`. | **PROVEN** | Pin 9 passed. `dir()` 152 → 153; gained exactly `EXPECTED_NEW_CORE_SUBMODULES`; lost none. |
| C-003 | `DataFrame.__module__`, `__slots__`, slot-only storage, all 38 aliases, the `head` overload pair, and `sorted(dir(DataFrame))` are unchanged; the `stat` accessor still returns `DataFrameStatFunctions`. | Identity/alias/overload/dir pin tests; the T0 freeze test. | **PROVEN** | Pin 9 passed; freeze 4 passed (13 combined). Class `dir()` 251 → 251, non-dunder 221 → 221. Stat shape `approxQuantile/corr/cov/crosstab/freqItems/sampleBy` unchanged. |
| C-004 | The six core statistics bodies live in `statistics.py` as frame-first module functions; the class keeps one-line wrappers; bodies AST-identical; docstrings byte-identical; stripped-comment reasons live in `dataframe/map.md`. | `test_moved_statistics_helpers_live_in_new_home`; the AST proof; the stat suites. | **PROVEN** | 261 lines, no row. 6 receivers renamed `self` → `frame`; `frame: DataFrame` annotated (contract). `summary`'s wrapper imports its helper locally: its `*statistics` parameter shadows the module binding. `DataFrame` under `TYPE_CHECKING` only. |
| C-005 | The `freqItems` refusal lives in `statistics.py` under the same guarantees; `DataFrameStatFunctions` keeps its one-line wrapper; `writer_readwriter.py` drops 1113 → 1111. | Ownership test; the AST proof; the freq refusal pins. | **PROVEN** | The refusal text, `del` shape, and signature are verbatim; the helper's `frame` arg is unused by a body that never touched state (frame-first convention, no ARG rule selected). |
| C-006 | `core.py` 5263 → 5060 and `writer_readwriter.py` 1113 → 1111 in `scripts/check_lib_py.py` and the CAP-1 test; new file carries no row; no sibling ceiling rises; the dataframe `map.md` exact-row sentence stays true. | `make check-lib-py`; the parity harness (holds CAP-1). | **PROVEN** | `make check-lib-py` clean (578 files). `joins_columns.py` 1238 ± 0, `plan_collapse.py` 1168 ± 0, all other rows untouched. |
| C-007 | Every stat suite is green; one mutation per moved function reds an existing behavioural pin; collected IDs are preserved plus the one new pin test; `approxQuantile` still collects six times for two columns by three probabilities. | Gate-list run; M1–M7; `--collect-only` diff; the collect-count probe. | **PROVEN** | Battery 328 passed + 7 skipped; mutations red 1/2/2/1/3/3/2 existing pins; collect 6102 → 6103 (+1, all preserved); collects 6 == 6 (DFCORE-5 baseline). |
| C-008 | Leaf-before-core import order holds with no cycle; no nested defs; zero new `#` bytes; ruff lint and format clean. | `-X importtime`; `make check-python-conventions`; `make py-lint`; `make py-format-check`. | **PROVEN** | `statistics` → `udf_projection` → `core`. Conventions 267 files clean; ruff check and format clean (788 files). The one `#` in the new file is the moved `noqa: N803`. |

VERDICT: 8 clauses, 8 PROVEN, 0 OPEN, 0 REJECTED.

## Evidence

**Counts.** `core.py` 5263 → 5060 (−203 = −212/+9 by numstat: six removed bodies
plus one extended bottom import line). `writer_readwriter.py` 1113 → 1111 (−2 =
−4/+2: the `freqItems` body gives 3 lines back for 1 delegation plus 1 module
import). New: `statistics.py` 261, under the default ceiling with no exception
row. Pin file 870 → 913 (+43: declared deltas plus the ownership test).

**Snapshot deltas.** Class loses nothing and gains nothing: `dir(DataFrame)` is
251 → 251 (non-dunder 221 → 221). Package 163 → 164 and core 152 → 153, each
gaining exactly `{statistics}` and losing nothing.

**AST proof** (`/tmp/dfcore3-ast.py`, throwaway). Normalizations: (a) receiver
`self`/`frame` → `RECV` (first arg dropped plus every `Name` load); (b)
signatures compared past the first arg, annotations included; (c) docstrings
compared separately (cleaned content plus raw source modulo the uniform 4-space
dedent). Result: all seven bodies IDENTICAL, all seven signatures IDENTICAL,
all seven docstrings IDENTICAL. The ruff-format signature join on `_freq_items`
is parser-invisible, confirmed by the identical dump. The one declared
non-delegation: `_describe` calls `frame.summary`, the public wrapper —
zero rewrites, identical to the pre-move `self.summary` call.

**Mutations** (each applied, run, reverted; restore verified by `diff -q` against
pristine copies plus a remnant grep with exit 1):

| id | home | mutation | reds (existing pins) |
|---|---|---|---|
| M1 | approx | always return the nested `results` (break single-column shape) | 1 failed: `test_stat_approx_quantile_list` |
| M2 | corr | wrapper delegates to `_cov` | 2 failed: `test_stat_corr_pearson`, `test_corr_cov_null_pair_divergence` |
| M3 | cov | wrapper delegates to `_corr` | 2 failed: `test_stat_cov_sample`, `test_corr_cov_null_pair_divergence` |
| M4 | crosstab | `left_name` joins with `__` | 1 failed: `test_stat_crosstab_counts` |
| M5 | summary | reversed statistic order | none — row order is deliberately unpinned (EX-DF-4 documents unordered rows), see below |
| M5b | summary | drop the `stddev` statistic | 3 failed: `test_describe_summary_replace`, `test_describe_row_order_divergence`, `test_h1_withcolumns_describe_dropdup_multi_name` |
| M6 | describe | drop `"max"` from the summary call | 3 failed: same three describe pins |
| M7 | freq | answer `frame.limit(0)` instead of refusing | 2 failed: `test_stat_freq_items_still_loud`, `test_stat_freq_items_refuses` |

**Move-forced mechanical edits** (no behaviour change): (1) the `summary`
wrapper imports `_summary` locally (2 lines, not 1): its `*statistics`
parameter shadows the module binding, so `statistics._summary` would read the
argument tuple — the only wrapper that cannot use the module attribute; (2) the
seven `frame` params carry `DataFrame` annotations (the contract requires every
parameter typed; `self` was exempt); (3) ruff format joins the `_freq_items`
signature to one 99-char line (261 lines, count recorded post-format); (4) the
core bottom import extends in place (96 chars, count-neutral); (5) the
`relativeError: float,  # noqa: N803` line moves with its parameter — the new
file's only `#` byte.

**DFCORE-5 baseline.** `/tmp/dfcore3-collect-count.py` (throwaway) counts
`DataFrame.collect` calls for `approxQuantile(["a", "b"], [0.25, 0.5, 0.75],
0.01)`: 6 before, 6 after, same values `[[1.0, 2.0, 3.0], [10.0, 20.0, 30.0]]`.
The per-probability loop is preserved exactly; DFCORE-5 batches it to one
collect per frame.

**Gates** (counts read from pytest summary lines): stat battery 328 passed + 7
skipped; parity harness 624 passed; dbt 59 passed + 1 skipped; facade 5754
passed + 354 skipped (carries the live-mirror gate). Static/doc gates
(`check-python-conventions`, `check-docstring-presence`, `py-lint`,
`py-format-check`, `check-lib-py`, `check-map-sync`, `check-ledger-grammar`,
`check-ledgers`, `check-docs-compaction`, `ledger_lifecycle check`) all exit 0.

**Out of scope observed** (pre-existing, the move weakens nothing — no test
removed, no behaviour changed): (a) summary/describe row order is unpinned by
design — M5 stays green because EX-DF-4 documents unordered rows and every pin
compares sets or dicts; (b) the stale-native signature mismatch on arrival
(`approx_percentile_cont` arity), resolved by the rebuild, same class as the
DFCORE-1/DFCORE-2 lane stales.

```yaml
COVERAGE_ATTESTATION:
  pr_unit: dfcore-3
  categories:
    - id: AT-1
      status: ATTACKED
      evidence: Export surfaces compared as recorded pre-slice lists with exact declared deltas; the pin reds pre-move on exactly those deltas. Bodies proven identical by normalized AST; signatures and docstrings byte-identical.
      artifacts: [python/repark/tests/test_dfcore_1_exports.py, python/repark/src/repark/spark/dataframe/statistics.py]
    - id: AT-2
      status: ATTACKED
      evidence: Quantile, corr/cov, crosstab, summary/describe, and freq-refusal paths ride the moved bodies; the stat battery reports 328 passed + 7 skipped on the fresh native, and seven mutations prove the pins live (1/2/2/1/3/3/2 reds).
      artifacts: [python/repark/tests/test_g1_stat_and_expander.py, python/repark/tests/test_df_easy.py, python/repark/tests/test_perf_approxpct_1.py, python/repark/tests/test_examples_dataframe_a.py, python/repark/tests/test_examples_dataframe_c.py, python/repark/tests/test_examples_dataframe_d.py]
    - id: AT-3
      status: ATTACKED
      evidence: All refusal branches (relativeError type/value, probability domain, non-pearson method, bare summary, unsupported stats, zero-column frame, freqItems loud refusal) moved verbatim; M5b/M6/M7 prove three refusal-adjacent pins live; the suite pins the rest.
      artifacts: [python/repark/src/repark/spark/dataframe/statistics.py]
    - id: AT-4
      status: N/A
      justification: Move-only relocation of stateless statistics bodies. No shared mutable state, no ordering change, no async spawn, no lock; module imports execute once at load in leaf-before-core order.
    - id: AT-5
      status: N/A
      justification: No privileged action, no auth surface, no secret, no deserialization, no path handling in the moved code; the import graph gains one leaf module with no new dependency.
    - id: AT-6
      status: ATTACKED
      evidence: The module binding surface is the compatibility contract; the pin freezes it (class frozen, modules plus one, ownership by identity). The summary shadow is the one declared local-import deviation.
      artifacts: [python/repark/tests/test_dfcore_1_exports.py, python/repark/src/repark/spark/dataframe/core.py]
    - id: AT-7
      status: N/A
      justification: No new execution path, no new loop, no new allocation shape. Import count grows by one leaf module at package load; per-call cost adds one module-attribute load on statistics calls only. Collect count 6 == 6.
    - id: AT-8
      status: ATTACKED
      evidence: Names, logic, docstrings, and error texts preserved verbatim; wrappers delegate in one line each (summary in two for the shadowing reason); core and writer bind only the module. Ceilings ratcheted down only.
      artifacts: [python/repark/src/repark/spark/dataframe/core.py, python/repark/src/repark/spark/dataframe/writer_readwriter.py, scripts/check_lib_py.py, python/repark-parity/tests/test_cap_1_source_file_line_cap.py]
    - id: AT-9
      status: N/A
      justification: No logging, metric, or alarm surface in the moved statistics; failure messages and exception types are unchanged, so diagnosis paths are unchanged.
    - id: AT-10
      status: ATTACKED
      evidence: The pin fails pre-move on exactly the declared deltas (3 reds) and passes post-move (9 passed). Seven mutations red existing behavioural pins. Collect IDs preserved plus the one declared pin test (6102 -> 6103).
      artifacts: [python/repark/tests/test_dfcore_1_exports.py]
  complete: true
```
