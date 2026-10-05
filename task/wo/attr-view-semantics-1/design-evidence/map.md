# task/wo/attr-view-semantics-1/design-evidence/ — the sketch round's raw measurement

Probe outputs that [../design.md](../design.md) cites, copied out of the boot-wiped `/tmp` work directory on 2026-10-05. Spark rows come from live Spark 4.1.2 (session zone America/New_York) on that date. RePark rows come from debug builds of the ATTR-ID-1 stack at `b9db3536`:
- base: the stack;
- b1: (a) + the B1 marker;
- head: (a) + B1 + the general (c) refusal, the throwaway in `throwaway-a-b1-c.diff`;
- planlevel: the rejected plan-only alias mint.

The probe scripts stay outside the repo, because ruff would rewrite them.

| probe | Spark | base | other builds | measures |
|---|---|---|---|---|
| mint | [mint_spark.json](mint_spark.json) | [mint_stack.json](mint_stack.json) | [mint_b1.json](mint_b1.json), [mint_head.json](mint_head.json), [mint_planlevel.json](mint_planlevel.json) | the §2.1 id rows and their foreign-column outcomes |
| case | [case_spark.json](case_spark.json) | [case_stack.json](case_stack.json) | [case_b1.json](case_b1.json), [case_head.json](case_head.json), [case_planlevel.json](case_planlevel.json) | spelling and scope id rows (case-variant references, CTE, UNION, GROUP BY) |
| ops | [ops_spark.json](ops_spark.json) | [ops_stack.json](ops_stack.json) | [ops_head.json](ops_head.json) | operation × source (V3, V4, V9, V12, V15, DFX, WCR) |
| edge | [edge_spark.json](edge_spark.json) | [edge_stack.json](edge_stack.json) | [edge_head.json](edge_head.json); [edge_main.json](edge_main.json) is main `9f41347f`, measured by the orchestrator for Q4 | pull-up edges, view reads, Q02/Q03/Q05 |
| pin | [pin_spark.json](pin_spark.json) | — | [pin_head.json](pin_head.json) | the facade pins (c) rewrites |
| join | [join_spark.json](join_spark.json) | — | [join_head.json](join_head.json) | join keys, the hidden class |
| reserved | [reserved_spark.json](reserved_spark.json) | — | — | the reserved marker name |
| views | — | — | [views_head.json](views_head.json) | head on the order's `views_probe.py`; the Spark and base rows are [../spark.json](../spark.json) and [../stack.json](../stack.json) |

- [replay-moves-head-vs-abase.json](replay-moves-head-vs-abase.json): the replay cells that moved between the a-base build and head; 16 toward, 0 away.
- [throwaway-a-b1-c.diff](throwaway-a-b1-c.diff): the measured throwaway against `b9db3536`. It is never to be committed as is; the slices restate it without comments and within the ceilings.

Up: [../map.md](../map.md).
