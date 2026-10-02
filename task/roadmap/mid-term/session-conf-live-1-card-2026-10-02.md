# SESSION-CONF-LIVE-1 — frames keep the case rule from creation after conf.set flips it

**Filed: 2026-10-02 from ATTR-ID-1 S3c** (source:
`/tmp/oc-worker/direct/wo/attr-id-1/s3c/final-handback.json`,
`out_of_scope_observed[0]`; verified present 2026-10-02). **Severity:**
wrong answers on 121 replay cells, not yet rated (owner). **Status:**
pre-existing session-infrastructure gap, outside S3a–S3e. **Target:**
mid-term.

## The divergence it records

Frames keep the case rule from creation time in `task_ctx`; `conf.set`
flips leave facade-live and native planning disagreeing (121 gap-moved
cells; the S3c hand-back names it as needing its own session-infrastructure
card).

## Repro

Not yet measured: the source names the count but carries no Spark-versus-RePark
cell dump. Repro to run on the day, against live Spark 4.1.2:

```python
df = createDataFrame([(1,)], ["AbC"])
spark.conf.set("spark.sql.caseSensitive", "true")
df.select("abc").collect()
spark.conf.set("spark.sql.caseSensitive", "false")
df.select("abc").collect()
```

A frame built under one case rule, read after the flip, must answer the
live rule on both the facade and the native planning path.

## Scope

Live session-config propagation to already-built frames: the case rule a
frame plans under follows the session's current setting.

## Out of scope

- The S3a–S3e attribute-id binding work (shipped on
  `wip/feat/attr-id-1-2026-10-01`).
- Java case-fold convergence (the unicode-case card, RC3-5).

## Clauses (draft)

- **C-001** The repro answers the live rule after the flip, both paths.
- **C-002** The 121 gap-moved cells answer Spark.
- **C-003** Frames that never see a flip are unchanged.

## Pointers

- Up: [map.md](map.md) · Evidence (evidence location):
  `/tmp/oc-worker/direct/wo/attr-id-1/s3c/final-handback.json`
