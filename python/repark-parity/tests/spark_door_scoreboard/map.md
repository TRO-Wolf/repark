# map — python/repark-parity/tests/spark_door_scoreboard

## Purpose

Unit tests of the Spark-door scoreboard harness
([../../src/repark_parity/scoreboard/map.md](../../src/repark_parity/scoreboard/map.md)). They need
no Spark, no JVM and no RePark package, so `make py-test` runs them.

## Contents

| File | Pins |
|---|---|
| `test_scoreboard_classify.py` | one hand-made answer pair per class; class order (type, then value, then nullability); name and decimal scale as structure; nested nullability; signed zero, infinities and NaN as one class; timestamps on the nanosecond timeline; rendered text counting only when both sides render; multiset rows unless ordered; struct position and sorted map entries; the six classifier mutations |
| `test_scoreboard_digest.py` | the digest rule; refusal of a changed definition, a wrong engine tag, unequal id sets, duplicate ids; an existing replay destination; `ingest` never overwriting a lane; the stored corpus (51 lanes, 17,264 cells, 619 exclusions) and the baseline scoreboard equal to the audit's counts |
| `test_scoreboard_summary.py` | scoreboard totals, lanes, ANSI split and families on a fixture; flaky cells leaving the count; the comparison by lane and id; flaky detection across runs; settings errors and precedence; the RePark import path order |

## Pointers

- Up: [../../map.md](../../map.md) (the tests map is at its line ceiling, so this directory is
  linked from the package map)
