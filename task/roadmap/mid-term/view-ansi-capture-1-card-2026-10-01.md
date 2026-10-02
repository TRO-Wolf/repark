# VIEW-ANSI-CAPTURE-1 — views lose their creation-time ANSI setting

**Filed: 2026-10-01 from the overnight owner-items backlog** (source:
`/tmp/oc-worker/direct/wo/overnight-2026-09-29-owner-items.md`, line 66;
#888 re-verify 8, finding **VN9-4**). **Severity:** wrong results, not yet
rated (owner). **Target:** mid-term.

## The divergence it records

Views are re-analyzed under the current session's ANSI setting, not the
setting they were created under, so a view created under ANSI on with
sequence(1, '3') or a CAST changes behaviour when read under ANSI off. Spark
keeps the view's creation-time setting.

## Repro, verbatim from the source line

```sql
-- under ANSI on:
CREATE VIEW v AS SELECT sequence(1, '3');
-- under ANSI off:
SELECT * FROM v;  -- RePark: changed behaviour; Spark 4.1.2: creation-time behaviour
```

The same applies to a view carrying a CAST whose ANSI-on/ANSI-off answers differ.

## Cause and suggested fix direction (a suggestion, not a decision)

View analysis reads the live session ANSI flag instead of a captured
creation-time setting. Any fix direction (capture the flag at CREATE, carry it
on the view definition) is suggested by the verifier/orchestrator, not a
decision.

## Clauses (draft)

- **C-001** A view created under ANSI on answers identically when read under ANSI off, and vice versa.
- **C-002** VN9-4 closes; both the `sequence` and the CAST shapes are pinned.
- **C-003** Runtime `SET` of the ANSI flag still governs non-view queries.

## Pointers

- Up: [map.md](map.md)
