# Unit ledger — REVIEW-FIX-1 · the CFG-1 mirror agrees with the loader

**Unit:** REVIEW-FIX-1 · **Date:** 2026-09-10 · **Branch:** `fix/review-fix-1-2` · **Base:** `origin/main`
**Model:** Muse Spark (muse-spark-1.3-contributor)
**Policy:** [AGENTS.md](../../../AGENTS.md). **Path:** STANDARD. **risk_tier: standard.**

**Why now.** The REVIEW-1 sweep confirmed four mirror-vs-loader gaps (Q-3, Q-4, Q-5, Q-6)
from [review-1-findings-2026-09-10.md](../../roadmap/mid-term/review-1-findings-2026-09-10.md):
a float/list session knob escapes as `AttributeError` instead of `ValidationError`,
non-ASCII digit strings pass the mirror but refuse at load, a same-name pair inside the
database family passes the mirror but refuses in `sources.rs refuse_duplicate_names`, and
`to_toml()` drops an empty overlay profile. One round, tier M, Python only.

**Retires:** this ledger moves to `../completed/` in this unit's last commit.

**Not in this step:** REVIEW-FIX-2 (the CFG-1 HOME-isolation pins), anything in `crates/`,
any dependency file, `STATUS.md`, `briefs/next-sequence.md`, REVIEW-FIX-7's header-injection
pins (already green, untouched).

**Environment note.** The brief's premise (a built native module in `.venv`) did not hold in
this clone: no `_native*.so` existed anywhere under it, so the committed suite could not
import. The lane restored the believed state without building: it copied the
`_native.abi3.so` from the `repark 1.1.1` wheel in the local uv cache (whose Python sources
are this tree minus REVIEW-FIX-7, `errors.py` identical) into
`python/repark/src/repark/`. The file is git-ignored (`*.so`), uncommitted, and proven
compatible by the run below (all 17 pre-existing pins green on the base tree, including
both session-through-the-engine pins).

## PROPOSITION LEDGER — REVIEW-FIX-1 — 2026-09-10

| Clause | Proposition (checkable) | Proof obligation | Verdict | Evidence |
|---|---|---|---|---|
| C-001 | D-1: a session knob is an `int` (not `bool`) or an ASCII digit string after `strip()` (`value.isascii() and value.isdigit()`, the `_normalize_display_int` rule); anything else raises so pydantic wraps it as `ValidationError`, never `AttributeError`. | `test_non_int_knob_types_refuse`, `test_knob_digit_rule_is_ascii` | **PROVEN** | Red first on the base tree: `AttributeError: 'float' object has no attribute 'strip'` (`config.py:105`) for `1.5`, and `Failed: DID NOT RAISE ValidationError` for `"٠١٢"` and `"²"`. Green after: `"4096"` and `" 4096 "` accepted, float/list/`"٠١٢"`/`"²"` refuse, full file `21 passed` (`.venv/bin/python -m pytest python/repark/tests/test_config_mirror.py -q`). |
| C-002 | D-2: `check_names` tracks `name -> key path` over every catalog and every database source together, so `postgres.acme` beside `trino.acme` refuses naming both paths, the way `sources.rs refuse_duplicate_names` does. | `test_name_collision_inside_database_family_refuses_naming_both` | **PROVEN** | Red first on the base tree: `Failed: DID NOT RAISE ValidationError` (the old code unioned database names into a set, so an intra-family collision vanished). Green after, message names both profile-relative paths: `duplicate name 'acme': 'database.postgres.acme' and 'database.trino.acme' — names must be unique across the catalog and database families`. The catalog-vs-database refusal keeps its pin (`test_name_collision_across_families_refuses`) and now names both paths too. Paths stay profile-relative because `ProfileConfig` validates without its profile name; the `sources.rs` shape (both paths, one sentence) is otherwise matched. |
| C-003 | D-3: `to_toml()` emits `[<profile>]` for every profile in `profiles`, so an empty overlay survives the round trip. | `test_empty_overlay_profile_survives_round_trip` | **PROVEN** | Red first on the base tree: `KeyError: 'prod'` — `render_lines` emitted subsections only, so the empty profile vanished and `tomllib.loads` had no `prod` table. Green after: the file renders `["prod"]` plus the existing subsection headers, `parsed["prod"] == {}`, full file `21 passed`. The spaced-name pin (`test_spaced_profile_name_renders_one_quoted_header`) stays green: the header uses the same `_toml_text` quoting REVIEW-FIX-7 introduced. |

VERDICT: 3 clauses, 3 PROVEN, 0 OPEN, 0 REJECTED.

```yaml
COVERAGE_ATTESTATION:
  pr_unit: review-fix-1
  categories:
    - id: AT-1
      status: ATTACKED
      evidence: Each card decision maps to one clause with its named pins; the pins assert the decided behavior directly (refusal class, ASCII rule, both-path naming, round-trip table).
      artifacts: [task/ledgers/staging/review-fix-1-ledger.md, python/repark/tests/test_config_mirror.py]
    - id: AT-2
      status: ATTACKED
      evidence: Adversarial knob values exercised (float, list, bool, negative int and string, empty, decimal and hex strings, non-string type, non-ASCII digits) plus the refused/accepted boundary (bare and space-padded ASCII digits); same-name pairs in both directions (intra-family and catalog-vs-database).
      artifacts: [python/repark/tests/test_config_mirror.py]
    - id: AT-3
      status: ATTACKED
      evidence: Every invalid shape refuses as ValidationError (float and list no longer escape as AttributeError); the pre-existing refusal pins (booleans, negative knob, cross-family collision, header-break names, CFG-2 session load) stay green.
      artifacts: [python/repark/tests/test_config_mirror.py]
    - id: AT-4
      status: N/A
      justification: No concurrency, locks, or shared state; the mirror builds immutable pydantic models from caller-owned dicts.
    - id: AT-5
      status: ATTACKED
      evidence: The REVIEW-FIX-7 header-injection refusals still fire unchanged through the rewritten check_names and to_toml; no new render path takes unvalidated text.
      artifacts: [python/repark/src/repark/config.py, python/repark/tests/test_config_mirror.py]
    - id: AT-6
      status: ATTACKED
      evidence: Rendered text parses back to the same tables (existing round-trip pins green); the added profile header carries no keys, so empty and non-empty profiles read back identically.
      artifacts: [python/repark/tests/test_config_mirror.py]
    - id: AT-7
      status: N/A
      justification: No hot loop, growth, or resource touch; one bounded scan of the profile names per validation.
    - id: AT-8
      status: ATTACKED
      evidence: The pydantic contract is used as documented (before/after validators raising ValueError wrap as ValidationError); no new dependency; the duplicate scan mirrors sources.rs refuse_duplicate_names naming both paths.
      artifacts: [python/repark/src/repark/config.py]
    - id: AT-9
      status: ATTACKED
      evidence: The duplicate refusal names both key paths with the uniqueness rule; the knob refusal names the non-negative-integer rule; both diagnose without leaking values beyond the offending name.
      artifacts: [python/repark/tests/test_config_mirror.py]
    - id: AT-10
      status: ATTACKED
      evidence: Every added branch has a naming input: non-int non-string, non-ASCII digits, the accepted ASCII pair, intra-family and cross-family collisions, empty and non-empty profiles.
      artifacts: [python/repark/tests/test_config_mirror.py]
  complete: true
```
