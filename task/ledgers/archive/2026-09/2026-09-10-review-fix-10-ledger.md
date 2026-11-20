# Unit ledger — REVIEW-FIX-10 · the READING exemption is a field, not a substring

**Retires:** this ledger moves to `../completed/` in the unit's last commit.
This file closes when REVIEW-FIX-10 merges, or when the owner closes the slate row.

**Unit:** REVIEW-FIX-10 · **Date:** 2026-09-10 · **Executor:** Muse Spark (muse-spark-1.3-contributor), Actor ·
**Branch:** `fix/review-fix-10` · **Base:** `59910754`
**Model:** muse-spark-1.3-contributor
**Card:** REVIEW-FIX-10 (Q-33) · **Home:** `scripts/check_ledger_grammar.py`,
`python/repark-parity/tests/test_dl_2_ledger_grammar.py`, `scripts/map.md`
**risk_tier:** standard.

The gate decided `is_reading` by substring: the first 40 lines containing the marker
with only a word boundary after it. A `STANDARD` ledger quoting the marker in prose and a
header valued `READING-FOO` were both wrongly exempted from rule B. D-1 makes it a field
parse; R10-A/R10-B (recorded as D-1a below) settle the anchor and the token against the
two frozen reading ledgers this step may not touch.

## Proposition ledger

| ID | Clause | Proof obligation | Verdict | Evidence |
|---|---|---|---|---|
| C-001 | `is_reading` parses the `Path` header field — the marker matches anywhere in the first 40 lines except inside backtick code spans, the value is the leading identifier run compared to `READING` exactly — so the real field stays exempt while the prose quote and `READING-FOO` fire rule B, and the mid-line `READING.` field stays exempt (D-1 as amended by R10-A/R10-B). | Four scratch-ledger pins in `test_dl_2_ledger_grammar.py`, red first. | PROVEN | Red 2026-09-10 (`-k reading`): 2 failed, 4 passed, 11 deselected; FAILED test_reading_quoted_marker_in_prose_is_not_exempt and FAILED test_reading_foo_value_is_not_exempt, both `assert 0 == 1` with stdout `ledger-grammar: 4 live ledgers clean`. Green after the fix: `test_dl_2_ledger_grammar.py` 17 passed; whole-tree `ledger-grammar: 78 live ledgers clean`. Cited from `python/repark-parity/tests/map.md` and `scripts/map.md`. |

## Rulings and residue

| Item | Record |
|---|---|
| D-1a (R10-A, 2026-09-10) | The line-start anchor is dropped: the marker matches anywhere in the header window except inside a backtick code span. The anchor was a guess about where the field sits, not a decision about what it means; the literal anchor reds the whole-tree gate on the two frozen reading ledgers below. |
| D-1a (R10-B, 2026-09-10) | The value token is the leading `[A-Za-z0-9_-]+` run after the marker, compared to `READING` exactly. A trailing sentence period reads as `READING`; `READING-FOO` is one non-matching token. |
| Residue (not fixed here) | `ballista-audit-0-ledger.md` carries 12 unpinned PROVEN clauses and `facade-audit-0-ledger.md` 9, with zero `pins:` citations between them — they survive today only because of the exemption. REVIEW-FIX-13 and REVIEW-FIX-15 material; measured 2026-09-10 on this branch before the fix. |

## Red first

The two negative pins fail on the base tree (exit 0, wrongly exempt); the two positives
pass throughout. The failing lines sit in C-001's evidence cell; the fix follows in this
same round.

## Fix

| Name | Layer |
|---|---|
| `is_reading` field parse | `scripts/check_ledger_grammar.py`: `READING_FIELD` matches the `Path` marker and captures the leading identifier run, `CODE_SPAN` strips backtick spans per header line, the run is compared to `READING` exactly |
| Pin | `python/repark-parity/tests/test_dl_2_ledger_grammar.py`: the prose-quote, `READING-FOO`, and mid-line `READING.` scratch ledgers |

```yaml
COVERAGE_ATTESTATION:
  pr_unit: review-fix-10
  complete: true
  categories:
    - id: AT-1
      status: ATTACKED
      evidence: Four scratch-ledger pins over the gate on seeded scratch trees, red first for the two negatives.
      artifacts: [python/repark-parity/tests/test_dl_2_ledger_grammar.py]
    - id: AT-2
      status: ATTACKED
      evidence: Controls are the four field shapes plus the pre-fix whole-tree measure that killed the line-start anchor.
      artifacts: [python/repark-parity/tests/test_dl_2_ledger_grammar.py]
    - id: AT-3
      status: ATTACKED
      evidence: No error surface in the change; a pure predicate with no raise.
      artifacts: [scripts/check_ledger_grammar.py]
    - id: AT-4
      status: N/A
      justification: No shared mutable state, no concurrency in the gate.
    - id: AT-5
      status: ATTACKED
      evidence: No AWS, IAM, secrets, .github, or dependency-file change; Python script only.
      artifacts: [scripts/check_ledger_grammar.py]
    - id: AT-6
      status: ATTACKED
      evidence: Internal predicate only; the CLI exit contract is unchanged.
      artifacts: [scripts/check_ledger_grammar.py]
    - id: AT-7
      status: ATTACKED
      evidence: Python-script-only round; scratch-repo pins, no live engine involved.
      artifacts: [python/repark-parity/tests/test_dl_2_ledger_grammar.py]
    - id: AT-8
      status: ATTACKED
      evidence: No size-gate row covers either touched file; the comment fence over staged sources printed nothing.
      artifacts: [scripts/check_ledger_grammar.py, python/repark-parity/tests/test_dl_2_ledger_grammar.py]
    - id: AT-9
      status: N/A
      justification: No new log or metric surface.
    - id: AT-10
      status: ATTACKED
      evidence: C-001 cited from the tests map and the scripts map; this ledger plus staging map.md trued in the same commit.
      artifacts: [task/ledgers/staging/review-fix-10-ledger.md, python/repark-parity/tests/map.md, scripts/map.md]
```
