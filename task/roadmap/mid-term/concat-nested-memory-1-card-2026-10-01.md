# CONCAT-NESTED-MEMORY-1 — deeply nested F.concat blows memory on a tiny frame

**Filed: 2026-10-01 from the overnight owner-items backlog** (source:
`/tmp/oc-worker/direct/wo/overnight-2026-09-29-owner-items.md`, line 83;
#892 lane). **Severity: HIGH** (memory blow-up). **Status:** pre-existing on
base. **Target:** mid-term.

## The divergence it records

300 nested `F.concat(e, F.lit('x'))` over a 50-row frame, then
`select(length(e)).agg(sum)`, grew a debug process to 90+ GB over 4.5 h, and
to 119 GB in another run. It froze `repark.slice` overnight. Likely
exponential plan or expression expansion, e.g. a nested concat lowering that
duplicates its argument.

## Repro, reconstructed inline from the source description

The probe `/tmp/concat.py` (#892 lane) is gone (evidence not retained); the
repro below is rebuilt from the description and must be re-measured on the day:

```python
from repark.spark import functions as F
frame = spark.range(50).withColumn("e", F.lit("a"))
for _ in range(300):
    frame = frame.withColumn("e", F.concat(frame.e, F.lit("x")))
frame.select(F.length(frame.e)).agg(F.sum("length(e)")).collect()
```

Runs must be memory-capped (`ulimit -v 67108864`, about 240 s).

## Suspected cause and suggested fix direction (a suggestion, not a decision)

Suspected cause, verbatim: a nested concat lowering that duplicates its
argument. No fix direction decided; the card opens with a re-measure under the
cap above, then a plan-shape inspection. Any direction the investigation
proposes is a suggestion, not a decision.

## Clauses (draft)

- **C-001** The 300-deep repro completes under the memory cap with Spark's answer.
- **C-002** A plan-size pin (expression nodes linear in nesting depth) lands red-first.
- **C-003** The neighbouring `concat`/`length`/`agg` cells still answer Spark.

## Pointers

- Up: [map.md](map.md)
