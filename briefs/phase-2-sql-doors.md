# Phase-2 execution brief — the two SQL doors

Status: SETTLED 2026-08-07. Operator confirmed: (a) repark-ta IS phase-2 scope (PR-4);
(b) repark-postgres/repark-excel → explicit POST-MILESTONE-ONE bucket in
[../task/todo.md](../task/todo.md) (the 4 manifest rows re-point there). Design SSOT:
[../docs/design/sql-doors.md](../docs/design/sql-doors.md) (a three-design adversarial review;
lands in-repo with PR-1). Port-source pin unchanged: v1 `main` @ `fc3f48102`. Census ground
truth: 342 repark-sql + 62 repark-functions test names at the pin — the lists are regenerated
from `cargo test -- --list` at the pin per [../docs/testing.md](../docs/testing.md), never
hand-written; the repark-ta census is generated at its PR.

## 0. Deliverables

Four crates (all tier 3): `repark-functions` (verbatim port), `repark-ta` (port + thin
`TaExtension`), `repark-spark` (v1 repark-sql ported; `SparkDialect` + `SparkExtension`),
`repark-sql` (NEW ANSI door per the design). Three hoists (declared-rename): MoR valve →
repark-iceberg; DF-54.1 subquery guard → repark-core defaults; `stamp_read_only` →
repark-core. Plus: `repark-common::surfaces` ID list, per-door `matrix.rs` + audit test,
[../docs/design/session-api.md](../docs/design/session-api.md) seam-freeze edits
(UNSTABLE→frozen + extension-session-scoped line), deferred-manifest reconciliation (14/18
zero; 4 escalated to an explicit scheduling note in todo.md).

## 1. PR slate (every commit green under `make ci`; map.md lockstep; carve-outs
   (.github/, AGENTS.md, CLAUDE.md, Makefile) orchestrator-only)

- **PR-1 — repark-functions + docs.** Verbatim copy-then-re-home (crate name KEPT — no
  rename map), full 62-test battery, DAG TIERS rows pre-declared for all four new crates;
  in-repo design doc + this brief land here. Parallel-start: PR-5 may begin once PR-1 merges.
- **PR-2 — repark-spark skeleton.** Router spine: normalize, spark_ast passthrough, guards,
  describe/show, metadata tables, time-travel scanner, `SparkDialect`; `SparkExtension`
  (register_all + analyzer rules + cardinality + configure knobs); DF-54.1 guard hoist to
  repark-core rides here with its tests. Unblocks deferred #1 (functions shim).
- **PR-3a — repark-spark DDL.** ctas, create_table, namespace_ddl, catalog_ops,
  local_fs_ddl, alter (+ their test batteries). Unblocks the CTAS-blocked deferred rows
  (#2, #4–#7).
- **PR-3b — repark-spark DML + refs.** merge, insert_overwrite, ref_ddl, call + MoR-valve
  hoist to repark-iceberg (declared-rename, tests ride). Census closes: 342-name empty
  sorted-diff under prefix `repark_sql::`→`repark_spark::`. Unblocks deferred #3
  (eager-DML routing) — all 7 Spark-door rows zero by here.
- **PR-4 — repark-ta.** Kernels + goldens (148 .bin fixtures) + `TaExtension`; Spark
  extension composes it; TA census generated + empty-diff. Unblocks deferred #8–#14.
  (CONFIRMED in scope 2026-08-07.)
- **PR-5 — repark-sql M1.** Crate + `AnsiDialect` delegation core; guard set (multi-statement
  FIRST, P11 read-only, SEC-02, write-to-branch); error-path wrong-door sniff; CTAS/CREATE
  `WITH (…)` curated vocab + `extra_properties` + Q15 loud-refuse routing; CREATE/DROP SCHEMA
  `WITH (location=…)`; DROP TABLE; surfaces registry + matrix.rs + audit test seeded; R1/R2
  spikes recorded day 1.
- **PR-6 — repark-sql M2.** ALTER schema-evolution handlers; MERGE thin lowering; `FOR … AS
  OF` scanner + the double-quote pin set (spans/comments/string-refs); branch/tag ALTER DDL
  (Q6 — first deferral candidate on overrun, rationale = scope); full refuse set (INSERT
  OVERWRITE w/ dbt-trino evidence, CALL/EXECUTE, TRUNCATE, Q3 absence); cross-door
  two-session equivalence rows (CTAS content+schema, MERGE result, time-travel pin);
  matrix completion; session-api.md freeze edits; ADR-0002 design-pass obligation discharged
  (the design doc + matrix are the artifact).

Order: 1 → 2 → 3a → 3b (port spine); 4 after 2; 5 after 1 (pipeline-parallel with the port);
6 after 5 and after the hoists it consumes (2, 3b). dbt-repark may start after 6.

## 2. Execution pattern (per PR, as phase 1)

Staged delegated workstreams → assemble/integrate → orchestrator carve-outs → verification
panel (four lenses: port-fidelity/census, design-conformance, testing-discipline,
public-hygiene) → fixer → orchestrator push/PR. Isolated worktrees per workstream. All
standing rules carry ([../AGENTS.md](../AGENTS.md) "Delegated-agent standing rules"): no AWS
calls or acceptance env vars from delegated agents; the forbidden-content greps before every
push; v1 repo read-only (worktree at the pin, never push/fetch); never `--all-features`;
carve-outs orchestrator-only.

## 3. Acceptance (phase close)

(1) 404 ported names empty-diff (342 + 62, + the TA census); (2) matrix audit green in both
doors, every surface ID mapped Tested/Absent-with-ADR; (3) cross-door rows green under the
two-session protocol; (4) deferred manifest reconciled 14/18 with the 4 postgres/excel rows
re-scheduled by explicit decision; (5) seam-freeze edits landed; (6) `make preflight` green;
retrospective + lessons per SEPMO.

## 4. Decisions record (settled 2026-08-07)

1. **repark-ta**: IN phase 2 (PR-4) — operator-confirmed.
2. **repark-postgres / repark-excel**: post-milestone-one bucket, recorded in
   [../task/todo.md](../task/todo.md); the 4 manifest rows re-point there —
   operator-confirmed.
3. **PR slate**: as §1 (7 PRs, 3a/3b split mandatory).
