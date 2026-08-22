# map — .agent/skills/audit-repark-parity/

## Purpose

The **parity audit procedure**: measure a repark surface against the pinned
live PySpark oracle, classify every divergence (product bug / disposed
divergence / stale claim), and land each finding as a fix with fail-before
evidence, a cause-string meta pin, or a reported registry finding — never
prose. Adapted from Apache DataFusion's expression-audit skill; the testing
contract it serves is [../../../docs/testing.md](../../../docs/testing.md),
which wins on any conflict.

## Contents

- [SKILL.md](SKILL.md) — the three trigger points (nightly-red triage first
  step; behavior-flipping PRs; pre-release sweep), the banner ritual and
  measurement mechanics, the classification buckets, and the
  findings-never-prose apply rules with their bite-proof traps.

## I want to...

| ...do this | go to |
|---|---|
| Triage a parity-live nightly red | [SKILL.md](SKILL.md) — mandatory first step, classify before repairing |
| Flip a Spark-visible default or error contract | [SKILL.md](SKILL.md) "When it runs" #2 — sweep the affected pins in the same PR |
| Re-measure the pinned set before a release | [SKILL.md](SKILL.md) "When it runs" #3 |
| Read the testing contract of record | [../../../docs/testing.md](../../../docs/testing.md) |
| See the meta-pin idiom to copy | [../../../python/repark-parity/compat/smoke_suite.py](../../../python/repark-parity/compat/smoke_suite.py) (`test_field_accessor` + the #205 demotions) |

## Pointers

- Up: [../map.md](../map.md)
- Related: [../../../python/repark-parity/compat/map.md](../../../python/repark-parity/compat/map.md)
  (the compat machinery the audit drives), [../../../DEVELOPMENT.md](../../../DEVELOPMENT.md)
  (`make parity-live`).

## Debug

| Symptom | First check |
|---|---|
| The audit "found nothing" fast | Step 0 stop conditions — did a named path fail to resolve and get skipped? |
| A pin the audit blessed goes red later | The cause-string assertions — did the failure *mode* change? Re-run the audit, do not patch the pin |
| The skill states a project rule | Bug — move it to the spine, leave a pointer (`.agent/` contract) |
