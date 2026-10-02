# VERBATIM-CTAS-ESCAPE-1 — verbatim-mode backslash escapes fail on CTAS and INSERT OVERWRITE

**Filed: 2026-10-01 from the overnight owner-items backlog** (source:
`/tmp/oc-worker/direct/wo/overnight-2026-09-29-owner-items.md`, line 68;
#890 re-verify, finding **VE2-3**). **Severity:** not rated. **Status:**
pre-existing. **Target:** mid-term.

## The divergence it records

In verbatim mode (`spark.sql.parser.escapedStringLiterals=true`), CTAS and
INSERT OVERWRITE with a `\'` literal fail with a parse error, while Spark
accepts them.

## Repro (from the source description; re-measure on the day)

```sql
SET spark.sql.parser.escapedStringLiterals=true;
CREATE TABLE t AS SELECT 'a\'b';
INSERT OVERWRITE TABLE t SELECT 'a\'b';
-- RePark: parse error; Spark 4.1.2: stores a'b
```

## Cause and suggested fix direction (a suggestion, not a decision)

The CTAS / INSERT OVERWRITE doors do not honour the verbatim literal rule the
plain SELECT door follows. Any fix direction is suggested by the
verifier/orchestrator, not a decision.

## Clauses (draft)

- **C-001** Both doors accept the `\'` literal under verbatim mode with Spark's value.
- **C-002** VE2-3 closes; the non-verbatim door still unescapes as before.
- **C-003** Neighbouring VE-2 escape cells are re-measured on the day.

## Pointers

- Up: [map.md](map.md)
