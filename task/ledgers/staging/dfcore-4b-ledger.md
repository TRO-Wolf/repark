# Unit ledger — DFCORE-4b · display out of `core.py`

**Date:** 2026-09-07 · **Branch:** `refactor/dfcore-4b` · **Base:** `origin/main`
`ba77977a` (PR #418, DFCORE-4a) · **Model:** muse-spark-1.3 · **Policy:** [../../../AGENTS.md](../../../AGENTS.md).
**Path:** STANDARD. **risk_tier: standard.**
**Registry:** `DFCORE-4b` **FIXED**.

**Retires:** this ledger moves to `../completed/` when the unit's last commit lands.

**Why now.** The decomposition plan
(`task/roadmap/epic-term/dataframe-core-decomposition-plan-2026-09-07.md`, §4 row DFCORE-4b)
opens the fifth slice: the ten display bodies (`show`, `__repr__`,
`_repr_html_`, `_preview_tail_rows`, the eager-eval conf reads and limits, the
show-argument validation, the style resolution, and the styled renderer) leave
`core.py` for one leaf module `display.py`; the four instance-called names stay
as one-line wrappers so the class surface moves exactly as declared, and an
extended export pin plus byte-identical show/repr/HTML goldens prove the move.

**Not in this unit:** `STATUS.md`, `briefs/next-sequence.md`, `.github/`,
`Cargo.lock`, dependency lists, completed ledgers, any behaviour change, the
eager-preview `count()` removal (DFCORE-6 owns it), the `approxQuantile`
batching (DFCORE-5 owns it). `printSchema` and `__str__` stay in `core.py`:
they share no helper with the moved code. The table/HTML renderers
(`_format_show_table`, `_format_eager_eval_table`, `_table_to_cell_rows` and
peers) already live in `plan_collapse.py`; the move imports them, never
re-homes them.

**Environment.** The lane native was stale on arrival: `test_display_styles.py`
failed 26 tests with `RecursionError` because `_native.logical_column_names`
was absent (the `columns` property raised `AttributeError` into
`__getattr__`). Rebuilt via `make develop` (exit 0); the display battery then
passes (44 passed). This mirrors DFCORE-1 round 2, DFCORE-2, DFCORE-3, and
DFCORE-4a (R2-S2 class).

## PROPOSITION LEDGER — DFCORE-4b — 2026-09-07

| Clause | Proposition (checkable) | Proof obligation | Verdict | Evidence / open question |
|---|---|---|---|---|
| C-001 | The package export surface equals the pre-slice surface under the DFCORE-1 filter; the unfiltered delta is exactly the gain of `display`; nothing lost. | `test_dfcore_1_exports.py::test_package_export_set_unchanged`. | **PROVEN** | Pin 10 passed. `dir()` 165 → 166; gained exactly the nine submodule names (DFCORE-1's four plus DFCORE-2's two plus `statistics` plus `sampling` plus `display`); lost none. Pre-move provocation reds exactly the declared deltas. |
| C-002 | The core export surface equals the pre-slice surface under the same filter; the unfiltered delta is exactly the `display` module binding; `__annotations__` still absent. | `test_dfcore_1_exports.py::test_core_export_set_unchanged`. | **PROVEN** | Pin 10 passed. `dir()` 154 → 155; gained exactly `EXPECTED_NEW_CORE_SUBMODULES`; lost none. |
| C-003 | `DataFrame.__module__`, `__slots__`, slot-only storage, all 38 aliases, the `head` overload pair, and the `stat` accessor are unchanged; `sorted(dir(DataFrame))` loses exactly the six display leavers. | Identity/alias/overload/dir pin tests; the T0 freeze test. | **PROVEN** | Pin 10 passed; freeze 4 passed (14 combined). Class `dir()` 250 → 244, non-dunder 220 → 214, lost exactly `_conf_lookup`, `_eager_eval_enabled`, `_eager_eval_limits`, `_normalize_show_args`, `_render_styled_show`, `_resolve_display_style`, gained none. `print_schema` alias still binds `printSchema`. |
| C-004 | The ten display bodies live in `display.py` as frame-first module functions; the class keeps one-line wrappers for `show`, `__repr__`, `_repr_html_`, and `_preview_tail_rows`; bodies AST-identical; docstrings byte-identical; stripped-comment reasons live in `dataframe/map.md`. | `test_dfcore_4b_exports.py::test_moved_display_helpers_live_in_new_home`; the AST proof; the display suites. | **PROVEN** | 322 lines, no row. 10 receivers renamed `self` → `frame`; `frame: DataFrame` annotated (contract). Six declared call rewrites to module-local calls; the tail preview stays instance-routed so the class-level spy keeps firing. `DataFrame` under `TYPE_CHECKING` only. |
| C-005 | `core.py` 4819 → 4539 in `scripts/check_lib_py.py` and the CAP-1 test; the new file carries no row; no sibling ceiling rises; the dataframe `map.md` exact-row sentence stays true. | `make check-lib-py`; the parity harness (holds CAP-1). | **PROVEN** | `make check-lib-py` clean (587 files). `joins_columns.py` 1238 ± 0, `plan_collapse.py` 1168 ± 0, `writer_readwriter.py` 1111 ± 0, all other rows untouched. |
| C-006 | Every recorded display string answers byte-identically before and after the move: `show()` at each truncate/n/vertical/empty/unicode shape plus both styled formats, `repr()` and `_repr_html_()` at eager caps 1/2/20 over empty/exact/over-cap frames and a `mapInArrow`-backed frame, with the same `count()` tallies. | `test_dfcore_4b_show_goldens.py`, `test_dfcore_4b_eager_goldens.py` (13 pins). | **PROVEN** | 13 passed on the tip, recorded on the pre-slice tree: 6 show tests (19 shapes), 7 eager tests (22 shapes), tallies repr-full 1 / repr-exact 1 / html-full 1 / vertical-full 1 / plain-show 0 / mia-repr 1 — the DFCORE-6 baseline. |
| C-007 | Every display suite is green; one mutation per moved function reds an existing behavioural pin; collected IDs are preserved plus the fourteen new pin tests. | Gate-list run; M1–M10; `--collect-only` diff. | **PROVEN** | Battery green; mutations red 27/1/1/1/1/1/1/22/7/19 existing pins; collect 6108 → 6122 (+14, all preserved, delta exactly the new pin tests). |
| C-008 | Leaf-before-core import order holds with no cycle; no nested defs; zero new `#` bytes beyond the one `noqa`; ruff lint and format clean. | `-X importtime`; `make check-python-conventions`; `make py-lint`; `make py-format-check`. | **PROVEN** | `plan_collapse` → `display` → `core` with no cycle. Conventions clean; ruff check and format clean. The new file holds zero `#` bytes; the core diff adds one `noqa: E402` line. |

VERDICT: 8 clauses, 8 PROVEN, 0 OPEN, 0 REJECTED.

## Evidence

**Counts.** `core.py` 4819 → 4539 (−280 = −293/+13 by numstat: ten removed
bodies, four one-line delegations, one bottom import line, and the F402
loop-variable rename with its wrapped f-string). New: `display.py` 322, under
the default ceiling with no exception row. Pin file 958 → 975 (+21/−12:
declared deltas plus the sibling-file pointer). Ownership pin: one file, 66
lines, one test. Golden pins: two files, 339/370 lines, 13 tests.

**Snapshot deltas.** Class loses exactly the six leavers and gains nothing:
`dir(DataFrame)` is 250 → 244 (non-dunder 220 → 214). Package 165 → 166
and core 154 → 155, each gaining exactly `{display}` and losing nothing.

**AST proof** (`/tmp/dfcore4b-ast.py`, throwaway). Normalizations: (a) receiver
`self`/`frame` → `RECV` (every `Name` load); (b) the six declared call rewrites
restored (module-local leaf calls → `self.` calls with the frame argument
dropped); (c) signatures compared past the first arg, annotations and defaults
included; (d) docstrings compared separately (parsed content plus raw source
modulo the uniform 8→4 dedent). Result: all ten bodies IDENTICAL, all ten
signatures IDENTICAL, all ten module docstrings IDENTICAL (raw shifts
`{0,4}`), all four wrapper docstrings byte-identical. Zero `self` references
and zero nested defs in `display.py`. The proof caught one real transcription
slip (a dropped `\"` escape in the `_normalize_show_args` docstring copy),
fixed before the ledger commit.

**Mutations** (each applied, run, reverted; restore verified by md5 against a
pristine copy plus zero `MUT-M` markers):

| id | home | mutation | reds (existing pins) |
|---|---|---|---|
| M1 | show | drop the tail `print(rendered)` | 27 failed across `test_display_styles.py` |
| M2 | repr | always take the schema arm (`if True`) | 1 failed: `test_eager_eval_repr_and_html` |
| M3 | html | always answer `None` (`if True`) | 1 failed: `test_eager_eval_repr_and_html` |
| M4 | enabled | always answer `False` | 1 failed: `test_eager_eval_repr_and_html` |
| M5 | limits | `max_rows` off by one (`+ 1`) | 1 failed: `test_eager_eval_repr_and_html` |
| M6 | conf | always answer `None` | 1 failed: `test_eager_eval_repr_and_html` |
| M7 | normalize | `n` error class `NOT_INT` → `NOT_BOOL` | 1 failed: `test_show_rejects_bool_n` |
| M8 | style | always answer `"spark"` | 22 failed across `test_display_styles.py` |
| M9 | tail | return head rows (drop `limit_with_skip`) | 7 failed incl. `test_preview_tail_rows_returns_last_n`, `test_preview_tail_rows_uses_limit_with_skip`, `test_styled_show_does_not_full_collect` |
| M10 | styled | swap the polars/duckdb renderers | 19 failed across `test_display_styles.py` |

**Move-forced mechanical edits** (no behaviour change): (1) the ten `frame`
params carry `DataFrame` annotations (the contract requires every parameter
typed; `self` was exempt); (2) six `self._helper(…)` calls become module-local
calls with the frame passed first (the declared rewrites, same arguments);
(3) `_render_styled_show` keeps calling the tail preview through the instance
(`frame._preview_tail_rows`, not module-local): `test_styled_show_does_not_full_collect`
patches the class attribute and asserts the spy fires, so routing around the
wrapper breaks that pin unchanged — verified red-then-green during the move;
(4) the `display` module binding shadows the `_emit_join_side_columns` loop
variable, renamed `display_name` with its f-string wrapped to hold 100 columns
(ruff F402, +2 lines, comments untouched); (5) the new module carries its own
`logging.getLogger(__name__)` (a leaf cannot import the core logger without a
cycle): record names move `core` → `display` while message text and levels
stay identical, and the caplog pin filters by level only.

**Comment census** (R-1). Whole-file `#` lines: pre `core.py` 484 → tip `core.py`
454 + `display.py` 0. The core diff removes 31 `#` lines (all inside the ten
moved spans, including the audit-named marker-validation narration) and adds 1
(the E402 bottom import). Zero comments deleted from staying code; every
stripped reason is recorded under the `display.py` entry in `dataframe/map.md`.

**Gates** (counts read from pytest summary lines): gate-list battery 650 passed
+ 3 skipped; parity harness 624 passed; facade 5773 passed + 354 skipped
(carries the live-mirror gate); dbt 59 passed + 1 skipped. Static/doc gates
(`check-python-conventions` 269 files, `check-docstring-presence` 233 files,
`make check-lib-py` 587 files, `make py-lint`, `make py-format-check` 797
files, `check-map-sync` 224 maps, `check-ledger-grammar`, `check-ledgers`,
`check-docs-compaction`, `ledger_lifecycle check`, `typos`) all exit 0.

**Out of scope observed** (pre-existing, the move weakens nothing — no test
removed, no behaviour changed): (a) the stale-native signature mismatch on
arrival (`logical_column_names` absent, surfacing as `RecursionError` through
`__getattr__`), resolved by the rebuild, same class as the DFCORE-1/DFCORE-2/
DFCORE-3/DFCORE-4a lane stales; (b) the eager preview counts to decide its
footer even when the frame holds exactly `maxNumRows` rows (tally
repr-exact 1) — DFCORE-6 owns removing that count, and the tallies pin the
current shape as its baseline.

```yaml
COVERAGE_ATTESTATION:
  pr_unit: dfcore-4b
  categories:
    - id: AT-1
      status: ATTACKED
      evidence: Export surfaces compared as recorded pre-slice lists with exact declared deltas; the pin reds pre-move on exactly those deltas. Bodies proven identical by normalized AST; signatures and docstrings byte-identical.
      artifacts: [python/repark/tests/test_dfcore_1_exports.py, python/repark/tests/test_dfcore_4b_exports.py, python/repark/src/repark/spark/dataframe/display.py]
    - id: AT-2
      status: ATTACKED
      evidence: Show, repr, HTML, eager-eval, and styled paths ride the moved bodies; the display battery reports 44 passed on the fresh native plus 13 goldens byte-identical to the pre-slice recording, and ten mutations prove the pins live (27/1/1/1/1/1/1/22/7/19 reds).
      artifacts: [python/repark/tests/test_display_styles.py, python/repark/tests/test_g2_window_rand_sampleby.py, python/repark/tests/test_dfcore_4b_show_goldens.py, python/repark/tests/test_dfcore_4b_eager_goldens.py]
    - id: AT-3
      status: ATTACKED
      evidence: Every refusal branch (n/vertical/bool types, truncate shapes, NOT_INT/NOT_BOOL classes, eager conf parsing with Spark defaults, tail short-circuits) moved verbatim; M5/M7 prove two refusal-adjacent pins live; the suite pins the rest.
      artifacts: [python/repark/src/repark/spark/dataframe/display.py]
    - id: AT-4
      status: N/A
      justification: Move-only relocation of stateless display bodies. No shared mutable state, no ordering change, no async spawn, no lock; module imports execute once at load in leaf-before-core order.
    - id: AT-5
      status: N/A
      justification: No privileged action, no auth surface, no secret, no deserialization, no path handling in the moved code; the import graph gains one leaf module with no new dependency. The SEC-008 INFO/DEBUG split moves verbatim.
    - id: AT-6
      status: ATTACKED
      evidence: The module binding surface is the compatibility contract; the pin freezes it (class minus the six declared leavers, modules plus one, ownership by identity of home). The F402 loop-variable rename and the instance-routed tail call are the declared deviations.
      artifacts: [python/repark/tests/test_dfcore_1_exports.py, python/repark/tests/test_dfcore_4b_exports.py, python/repark/src/repark/spark/dataframe/core.py]
    - id: AT-7
      status: N/A
      justification: No new execution path, no new loop, no new allocation shape. Import count grows by one leaf module at package load; per-call cost adds one module-attribute load on display calls only. Golden strings and count tallies equal pre and tip.
    - id: AT-8
      status: ATTACKED
      evidence: Names, logic, docstrings, and error texts preserved verbatim; wrappers delegate in one line each; core binds the module. Ceilings ratcheted down only.
      artifacts: [python/repark/src/repark/spark/dataframe/core.py, scripts/check_lib_py.py, python/repark-parity/tests/test_cap_1_source_file_line_cap.py]
    - id: AT-9
      status: N/A
      justification: Log message text and levels are unchanged, so diagnosis paths are unchanged; only the record logger name moves from core to the new leaf module, and the caplog pin filters by level.
    - id: AT-10
      status: ATTACKED
      evidence: The pin fails pre-move on exactly the declared deltas (4 reds) and passes post-move (11 passed). Ten mutations red existing behavioural pins. Collect IDs preserved plus the fourteen declared pin tests (6108 -> 6122).
      artifacts: [python/repark/tests/test_dfcore_1_exports.py, python/repark/tests/test_dfcore_4b_exports.py, python/repark/tests/test_dfcore_4b_show_goldens.py, python/repark/tests/test_dfcore_4b_eager_goldens.py]
  complete: true
```

## Critic round 1 (2026-09-07, Muse Spark 1.3) — FAIL → served by the orchestrator

- F1 (S2, SERVED): the styled-vertical `warnings.warn(..., stacklevel=2)` moved one frame
  deeper behind the `show` wrapper, so the warning attributed to `core.py` instead of the
  caller. `display.py` now carries `stacklevel=3`;
  `test_dfcore_4b_show_goldens.py::test_show_styled_vertical_warning_attributes_to_caller`
  pins the warning's `filename` to the caller (mutation `stacklevel=9` → 1 red). The
  critic's own probe re-run on the tip records `dfcore4b-probe.py:28:True`, the pre-slice
  value.
- F2 (S4, noted): the vertical-branch `count()` is held only by the new goldens; DFCORE-6
  adds a non-golden pin when it changes that path.
