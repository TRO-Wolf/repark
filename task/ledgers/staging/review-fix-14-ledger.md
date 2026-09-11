# Unit ledger — REVIEW-FIX-14 step 1 · the display configuration keeps its promises

**Unit:** REVIEW-FIX-14 step 1 · **Date:** 2026-09-10 · **Branch:** `fix/review-fix-3-9-14` · **Base:** `origin/main`
**Model:** swe-2-high
**Policy:** [AGENTS.md](../../../AGENTS.md). **Path:** STANDARD. **risk_tier: standard.**

**Why now.** The review-1 sweep confirmed two configuration-integrity defects:
`repark.display.max_rows` has no ceiling, so the styled probe's
`limit(max_rows + 1)` is an unbounded fetch bound driven straight from configuration
(Q-51), and `conf.get("repark.display.style")` re-reads `REPARK_DISPLAY_STYLE` after
an `unset` — a query-time environment read ADR-0004 forbids, which desyncs
`conf.get` from `display_style`/`show()` on a live session (Q-52).

**Retires:** this ledger moves to `../completed/` when the unit's last commit lands.

**Not in this step:** ceilings for `max_cols`/`str_len` (no fetch bound is driven by
them; adding caps there would be a semantic-adjacent change), the env-derived reset
target `unset` resolves once at the unset call (recorded under D-2's reasoning —
`default_display_style()` is the module's documented "default" and the snapshot it
writes is what `get` now serves), any dependency file, `STATUS.md`,
`briefs/next-sequence.md`, `gh`, pushes.

## PROPOSITION LEDGER — REVIEW-FIX-14 step 1 — 2026-09-10

| Clause | Proposition (checkable) | Proof obligation | Verdict | Evidence |
|---|---|---|---|---|
| C-001 | D-1: `conf.get("repark.display.style")` reads the session's snapshot, never the process environment, after an `unset`; `conf.get`, `session.display_style` and `show()` agree through a post-unset `REPARK_DISPLAY_STYLE` mutation. | `test_display_style_unset_reads_session_snapshot_not_env` | **PROVEN** | Red 2026-09-10 (`pytest -k unset_reads_session`, base tree): with env `spark` at build, `conf.set` → `duckdb`, `conf.unset`, then `REPARK_DISPLAY_STYLE` mutated to `polars`, `conf.get` returned `'polars'` while the snapshot held `'spark'` — `assert 'polars' == 'spark'` fails at the first assertion. Green after `builder_conf.py`'s `get` serves the alive-token value (`token["display_style"]`, what `unset` snapshotted) for the tombstoned key instead of calling `default_display_style()` again: `conf.get`/`display_style`/`show()` all report spark-grid. |
| C-002 | D-2: `repark.display.max_rows` carries a ceiling, so the styled probe's `limit(max_rows + 1)` cannot be driven unbounded from configuration. **Chosen shape: refuse-loud at `_DISPLAY_MAX_ROWS_CEILING = 10_000`.** Refuse-loud rather than clamp because the module's whole int-display contract is refuse-loud (`parsed < 1` and non-digits already raise `INVALID_CONF_VALUE.REQUIREMENT` at the same funnel), and a silent clamp would make `conf.get` report a value the writer asked for differently. 10_000 bounds the probe fetch while leaving generous REPL headroom — polars' own default `tbl_rows` is 10 and any display table past thousands of rows is already unreadable. The check lands in `_normalize_display_int`, the single funnel every writer (runtime `conf.set`, builder `.config`, builder-map read, reuse fold) already passes through. | `test_display_max_rows_ceiling` | **PROVEN** | Red 2026-09-10 (`pytest -k max_rows_ceiling`, base tree): `DID NOT RAISE IllegalArgumentException` for `"10001"`. Green: `conf.set("repark.display.max_rows", "10000")` round-trips, `"10001"` and `"99999999"` raise `IllegalArgumentException` naming `repark.display`, `ReparkSession.builder.config("repark.display.max_rows", "10001")` raises at `.config()` time, and the rejected set leaves the prior value intact (`conf.get` still `"10000"`). |

VERDICT: 2 clauses, 2 PROVEN, 0 OPEN, 0 REJECTED.

## Green

`pytest python/repark/tests/test_display_polars_default.py
python/repark/tests/test_display_lazy_1.py python/repark/tests/test_display_styles.py -q`
2026-09-10: `79 passed`; the 110-test display-adjacent suite (show goldens,
t3-ux-polish, metadata tables, session, cache/persist) green in the same tree.

## Red first

## Residue

- `unset` still resolves `default_display_style()` — env `REPARK_DISPLAY_STYLE`
  first — at the unset call itself, and writes that value into the snapshot. That is
  the module's documented "reset to the default" semantic and is what the Q-52
  finding names as the correct snapshot mechanism; the defect it confirmed was the
  `get`-side re-read, which is now closed. A reset-to-build-time-default semantic
  (snapshot the env at session build instead) would need a new alive-token field
  written in `session_core.py`, outside this card's Python Home — flagged here rather
  than widened.
- `conf.get(key, default)` on a tombstoned style key still returns the explicit
  `default`, matching the general tomb contract.

## Gates

- `pytest … test_display_polars_default.py test_display_lazy_1.py test_display_styles.py -q`
  2026-09-10: `79 passed` (exit 0).
- `python scripts/check_lib_py.py` 2026-09-10: `640 files clean` (exit 0).
- Comment fence 2026-09-10: the staged-diff comment grep prints nothing.

```yaml
COVERAGE_ATTESTATION:
  pr_unit: review-fix-14
  complete: true
  categories:
    - id: AT-1
      status: ATTACKED
      evidence: Both clauses walked pin by pin. C-001 red with conf.get reporting the mutated environment ('polars' vs the snapshot 'spark'), green with all three surfaces agreeing; C-002 red with DID NOT RAISE for 10001, green with refuse-loud at and acceptance at the ceiling.
      artifacts: [python/repark/src/repark/spark/session/builder_conf.py, python/repark/src/repark/spark/session/session_configuration.py, python/repark/tests/test_display_polars_default.py]
    - id: AT-2
      status: ATTACKED
      evidence: The ceiling pin exercises both sides — 10000 accepted and round-tripped, 10001 and 99999999 refused — through both the runtime door (conf.set) and the builder door (.config), and asserts the rejected write leaves no residue.
      artifacts: [python/repark/tests/test_display_polars_default.py]
    - id: AT-3
      status: ATTACKED
      evidence: The refusal happens in _normalize_display_int before any store/token mutation, so a refused set cannot half-apply; the pin asserts conf.get still reports the last accepted value after the refused write.
      artifacts: [python/repark/src/repark/spark/session/session_configuration.py]
    - id: AT-4
      status: ATTACKED
      evidence: The C-001 pin is the state attack: session built under env=spark, style overridden, unset, then the environment mutated on the live session — conf.get, the display_style property, and show()'s actual rendered output are all asserted to agree with the snapshot, closing the query-time environment read ADR-0004 forbids.
      artifacts: [python/repark/tests/test_display_polars_default.py, python/repark/src/repark/spark/session/builder_conf.py]
    - id: AT-5
      status: ATTACKED
      evidence: The env-read removal is a confidentiality-adjacent fix by construction: process environment can no longer steer a live session's reported style after unset; the get path serves only session-held state.
      artifacts: [python/repark/src/repark/spark/session/builder_conf.py]
    - id: AT-6
      status: ATTACKED
      evidence: The full display trio (79) plus the 110-test adjacent suite green; the pre-existing test_conf_unset_display_style_resets_to_spark pin still passes because unset keeps its env-aware reset-at-unset semantic.
      artifacts: [python/repark/tests/test_display_styles.py, python/repark/tests/test_display_polars_default.py]
    - id: AT-7
      status: ATTACKED
      evidence: The ceiling directly bounds the resource the finding named — the styled probe's limit(max_rows + 1) can no longer be driven past 10001 from configuration; runtime get/set paths remain O(1) dict reads.
      artifacts: [python/repark/src/repark/spark/session/session_configuration.py]
    - id: AT-8
      status: ATTACKED
      evidence: The D-1 mechanism follows REVIEW-FIX-5's shape at the Python layer (a build/unset-time snapshot on the shared alive token, reads never re-hit the environment); no new file, edge, or session_core widening was needed inside the card's Home.
      artifacts: [python/repark/src/repark/spark/session/builder_conf.py]
    - id: AT-9
      status: N/A
      justification: No log or metric surface; refusals raise the existing INVALID_CONF_VALUE.REQUIREMENT shape naming the key and bound.
    - id: AT-10
      status: ATTACKED
      evidence: Both clauses carry red-first pins with the failing output pasted in the clause cells; the ceiling choice (refuse-loud, 10000) is recorded in the C-002 row with its reason, per the card's delegated-decision instruction.
      artifacts: [task/ledgers/staging/review-fix-14-ledger.md, python/repark/tests/test_display_polars_default.py]
```
