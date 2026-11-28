# map — .agents/skills/spark-door-scoreboard/

## Purpose

The **Spark-door scoreboard procedure**: replay a RePark package against the
recorded Spark 4.1.2 answers of the 2026-11-21 audit corpus, score every cell
by class, and compare two scoreboards cell by cell. One divergence's
classification into a fix, a pin or a registry entry is
[../audit-repark-parity/](../audit-repark-parity/map.md)'s.

## Contents

- [SKILL.md](SKILL.md) — scope and what the EQUAL share means; prerequisites and
  the settings; getting a package; the sequence (corpus check, controls, two
  full replays and the flaky rule, per-lane replay, scoreboard, comparison,
  branch check, filing); each class with a corpus example; regressions and the
  S1 rule; new cases, the digest rule, the path scrub, the checksum manifest
  and re-recording; a fresh whole-door
  audit; pitfalls; reporting; Iceberg.

## I want to...

| ...do this | go to |
|---|---|
| Score a package against the whole Spark door | [SKILL.md](SKILL.md) section 4 |
| Check a branch moves no cell off Spark | [SKILL.md](SKILL.md) 4.6 |
| Add cells with live Spark answers | [SKILL.md](SKILL.md) section 7 |
| Read the harness, the corpus or a committed scoreboard | [../../../python/repark-parity/src/repark_parity/scoreboard/map.md](../../../python/repark-parity/src/repark_parity/scoreboard/map.md) |

## Pointers

- Up: [../map.md](../map.md)
- Related: [../audit-repark-parity/map.md](../audit-repark-parity/map.md)
