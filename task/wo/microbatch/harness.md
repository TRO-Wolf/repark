# HARNESS — the crash harness, red before MB-2c                       grade: B   engine band: Muse at max   release: 1.7

> **North Star briefing (owner, 2026-10-05).** Every executor and verifier on this order reads [the CDC and micro-batch North Star](../../roadmap/epic-term/cdc-microbatch-north-star-2026-10-05.md) first: NS-1…NS-19, the authority order in §2 (Flink governs guarantees, Spark the surface, Iceberg's own sinks the commits, else refuse with a dated row), the Rust placement in §4, and the production-grade lists in §5. The North Star sits below every ruled O/D/CC/ES/CL row and above the agent's judgment. For a question no ruled row answers, write the four lines (the question, Flink's answer, Spark's answer, the NS default), act on the default, and record the four lines as a dated ledger row. Halt only for the three §8 cases.


## 0. Why and what is out of scope

The CC-6 shape: the five crash scenarios exist as pins before the code they
test. Test-only, written after MB-1/MB-2a land and before MB-2c. Out of
scope, named: any product code, the MB-2c fix itself.

## 1. Rulings already made

- D-4, CC-9 (accepted vs durable; durable only advances), the sketch's §5
  test names with their setup, kill point and assertion each.
- MB-1 and MB-2a have merged; the harness builds on their modules.

## 2. Files

| path | kind | ceiling |
|---|---|---|
| `python/repark/tests/test_microbatch_crash_1.py` | new: the five scenario pins, strict-xfail until MB-2c | default 1000 (`scripts/check_lib_py.py`) |
| `python/repark/tests/map.md` | edited: the new test row | lockstep |

No Rust file is touched. The C-0 harness
(`python/repark-parity/tests/live_db/test_c0_cdc_scenarios.py`) is the shape
precedent, not an edited file.

## 3. Design (from the sketch)

**PENDING-SKETCH:** the five test names, the kill-point mechanics per
scenario, whether file/rate sources serve as harness helpers (Q11), and the
exact duplicate-delivery setup (Q9).

## 4. Steps

Step 1. One pin per scenario from the sketch's §5: kill between sink commit
and the next trigger; duplicate delivery; two drivers on one sink;
unknown-outcome reconcile; a Bronze overwrite inside a window. Step 2. Run:
all five fail (strict-xfail) on the MB-2a head. Step 3. Commit test-only.

## 5. Gates and their expected output

- `pytest python/repark/tests/test_microbatch_crash_1.py` — five xfailed,
  zero passed, zero failed.
- `bash scripts/check_map_md.sh --base origin/main` — exit 0.
- `uvx typos@1.47.2 python/repark/tests/test_microbatch_crash_1.py` — exit 0.

## 6. Halt rules

1. The sketch's §5 is missing a scenario's kill point or assertion.
2. A scenario passes before MB-2c (the pin does not pin): halt with the
   scenario, do not weaken it.
3. MB-1 or MB-2a has not merged.

## 7. Hand-back

`{"status":"DONE|HALT","pins":"5 xfailed","questions":[]}` plus the commit
sha. No verifier: test-only PR.
