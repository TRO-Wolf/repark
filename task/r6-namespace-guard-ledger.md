# Unit ledger — R-6 namespace-create location guard

**Unit:** R-6 · G-6 Q1 · **Date:** 2026-08-14 ·
**Lane:** repark · **Executor:** Grok (grok-4.5) ·
**Worktree:** `/tmp/grok-r6` · **Branch:** `grok/r6-namespace-guard` ·
**Base (FROZEN):** `fddf1bc4840ade68274ca5c55993dda0fb182a61`
(`feat(ansi): Spark-door spark.sql.ansi.enabled default TRUE (U5) (#94)`)

**Charter:** `BRIEF-r6-namespace-guard.md` + conductor-9 A9 grant expansion +
R-WAVE-KICKOFF. **SEPMO:** STANDARD — acc + C4. Floor S1. max_cycles=2.

This ledger does **not** edit `docs/spark-sql-iceberg-parity.md` or
`STATUS.md` (A4 — no registry writer tonight). §6 is paste-true for the
landing increment.

### Proposition ledger (scope audit)

| ID | Proposition | Verdict |
|---|---|---|
| C-001 | Core `create_namespace` create-new with an explicit location stores it. | PROVEN — `create_namespace_create_new_stores_location` |
| C-002 | Core re-create with the same location is idempotent (no AlreadyExists). | PROVEN — `create_namespace_recreate_same_location_is_idempotent` |
| C-003 | Core re-create with a contradictory location fails loud naming both paths; stored location unchanged. | PROVEN — `create_namespace_recreate_conflicting_location_fails_loud` |
| C-004 | Core re-create without a request location is idempotent. | PROVEN — `create_namespace_recreate_without_location_is_idempotent` |
| C-005 | Spark SQL `IF NOT EXISTS` create-new creates the namespace. | PROVEN — `sql_create_namespace_if_not_exists_create_new` |
| C-006 | Spark SQL `IF NOT EXISTS` same LOCATION is idempotent. | PROVEN — `sql_create_namespace_if_not_exists_same_location_is_idempotent` + updated `sql_create_namespace_if_not_exists_is_idempotent` |
| C-007 | Spark SQL `IF NOT EXISTS` contradictory LOCATION fails loud naming both paths. | PROVEN — `sql_create_namespace_if_not_exists_conflicting_location_fails_loud` |
| C-008 | Spark SQL `IF NOT EXISTS` without LOCATION is idempotent. | PROVEN — `sql_create_namespace_if_not_exists_without_location_is_idempotent` |
| C-009 | One facade pin (memory catalog, `repark.sql`-era imports) covers the four shapes through `spark.create_namespace`. | PROVEN — `test_namespace_location_guard.py::test_create_namespace_location_guard_four_shapes` |
| C-010 | Catalog crate, iceberg fork, `session.rs` surfaces beyond `create_namespace`, and `repark-python/src/session.rs` stay CLOSED. | PROVEN — diff names (no `crates/repark-iceberg`, no `repark-python/src`, no builder/configure). |
| C-011 | SQL without `IF NOT EXISTS` still AlreadyExists (no adopt). | PROVEN — `test_errors.py::test_create_namespace_duplicate_raises_analysis_exception` unchanged; handler only guards the `if_not_exists` arm. |
| C-012 | Shared predicate (one comparison) is the hook both doors call. | PROVEN — `repark_core::refuse_contradictory_namespace_location`; Spark `namespace_ddl.rs` + ANSI `schema_ddl.rs`. |
| C-013 | ANSI `CREATE SCHEMA IF NOT EXISTS` has the same four shapes (two-doors / silent-wrong residual closed). | PROVEN — `schema_ddl::location_guard_tests::ansi_create_schema_if_not_exists_*` |
| C-014 | Registry / STATUS / lockfiles / `.github` untouched. | PROVEN — diff names. |
| C-015 | `make verify` + `make preflight` exit 0. | PROVEN — §4. |
| C-016 | `map.md` / `task/map.md` both-adds in the same change. | PROVEN — listed in §2. |

**Enumeration (C-001–C-009, C-013):** four shapes × {core Session, Spark SQL IF NOT EXISTS, ANSI IF NOT EXISTS, facade programmatic} = 16 cells. Facade is one test function covering all four shapes (charter: one facade pin).

---

## 0. Blast + seams

### 0.1 Where the silent adopt actually lived

Verified on freeze SHA:

| Path | Today (pre-R-6) | After |
|---|---|---|
| `Session::create_namespace` | catalog `create_namespace` → `NamespaceAlreadyExists` / `Error::Analysis` on any re-create | exists → load props → shared refuse → adopt or fail-loud |
| Spark `execute_create_namespace` | `IF NOT EXISTS` + `namespace_exists` → `read_empty()`; **LOCATION ignored** (`namespace_ddl.rs` :98–99) | same exists-check, then shared refuse |
| ANSI `execute_create_schema` | same silent adopt on `IF NOT EXISTS` | same hook (two-doors; silent-wrong class) |
| SQL **without** `IF NOT EXISTS` | catalog AlreadyExists | **unchanged** |
| Catalog crate / fork | n/a | **CLOSED** |

Altitude: engine, not harness. G-6 Q1.

### 0.2 Shared predicate (ADR-0005 decision 4)

New standalone module `crates/repark-core/src/namespace_create.rs` — not a Session
split. `resolve_namespace_location` (location, else location_uri) on both maps.
Trailing slashes stripped for compare (S3 case-sensitive). Existing-without-location
or request-without-location → adopt. Conflict message names namespace + both paths.

### 0.3 Existing pin that encoded the defect

`sql_create_namespace_if_not_exists_is_idempotent` used to `CREATE … IF NOT EXISTS
LOCATION '<other>'` and assert success. That *was* the silent adopt. Fixture now
uses the **same** location (name stays honest). Conflict twin is
`sql_create_namespace_if_not_exists_conflicting_location_fails_loud`.

### 0.4 JVM probe

Not taken. Lock FIFO is R-1 → R-2 → R-3 → R-4; R-6 is last and optional. Default:
never take it. Fail-loud is engine policy (data-loss guard, G11-adjacent), not a
parity surface. §6 notes this.

---

## 1. Implementation

- `crates/repark-core/src/namespace_create.rs` — predicate + unit comparison tests.
- `crates/repark-core/src/session.rs` — `create_namespace` exists-check + refuse.
- `crates/repark-spark/src/namespace_ddl.rs` — `IF NOT EXISTS` hook.
- `crates/repark-sql/src/schema_ddl.rs` — ANSI `IF NOT EXISTS` hook.

No `Cargo.lock` / `uv.lock` / `STATUS.md` / registry / catalog / fork / binding edits.

---

## 2. Tests

| Home | Tests |
|---|---|
| `session/namespace_create_tests.rs` | four core shapes |
| `repark-spark/src/tests/namespace_ddl.rs` | four SQL twins + updated idempotent |
| `repark-sql/src/schema_ddl/location_guard_tests.rs` | four ANSI twins |
| `python/repark/tests/test_namespace_location_guard.py` | one facade pin, four shapes |

Maps updated: `crates/repark-core/{map.md,src/map.md,src/session/map.md}`,
`crates/repark-spark/src/{map.md,tests/map.md}`,
`crates/repark-sql/src/{map.md,schema_ddl/map.md}`,
`python/repark/tests/map.md`, `task/map.md`.

---

## 3. ACC + C4

### 3.1 Actor Self Logic Review

```yaml
SELF_LOGIC_REVIEW:
  id: SLR-R6-ACTOR-1
  agent: Actor
  action: implement G-6 Q1 location guard on core create_namespace and both SQL IF NOT EXISTS paths
  charter_trace: C-001..C-016
  preconditions:
    - freeze SHA fddf1bc4840ade68274ca5c55993dda0fb182a61: SATISFIED (git rev-parse)
    - silent adopt at namespace_ddl.rs IF NOT EXISTS: SATISFIED (read :98–99)
    - catalog crate / fork CLOSED: SATISFIED (not in diff)
  success_condition: four shapes pin on core + Spark SQL; conflict names both paths; matching/no-location idempotent; facade one file repark.sql-era; maps+ledger; verify+preflight 0
  step_risks:
    - existing SQL idempotent test still pins silent adopt: HANDLED(flipped to same-location)
    - ANSI door left silent: HANDLED(same hook + four twins)
    - session.rs file-size ceiling: HANDLED(1599 < 1650; predicate extracted)
  tripwire_scan: CLEAN
  uncertainty: NONE
  verdict: PROCEED
  escalation: —
```

### 3.2 Critic-1

Context break executed; attacking artifacts, not memory. Sequential single-session
mode (procedural, not amnesia).

```yaml
COVERAGE_ATTESTATION:
  pr_unit: R-6
  critic: Critic-1
  categories:
    - id: AT-1
      status: ATTACKED
      evidence: >
        Walked C-001..C-016 against the diff. Core four shapes live in
        session/namespace_create_tests.rs. Spark IF NOT EXISTS twins live in
        namespace_ddl.rs (plus the flipped silent-adopt fixture). ANSI twins
        in schema_ddl/location_guard_tests.rs. Facade one file, repark.sql-era
        imports. SQL without IF NOT EXISTS still falls through to catalog
        AlreadyExists. CLOSED surfaces absent from the stat.
      artifacts:
        - crates/repark-core/src/session.rs:851-878
        - crates/repark-spark/src/namespace_ddl.rs:102-113
        - crates/repark-sql/src/schema_ddl.rs:47-59
        - python/repark/tests/test_namespace_location_guard.py
    - id: AT-2
      status: ATTACKED
      evidence: >
        Empty request map, location-less existing, trailing-slash-only,
        location_uri-only existing vs location request, conflict. Trailing
        slash is unit-only (not Session/SQL integration).
      artifacts: [crates/repark-core/src/namespace_create.rs:78-131]
    - id: AT-3
      status: ATTACKED
      evidence: >
        Conflict returns Analysis/Plan and does not rewrite stored location
        (session + Spark + ANSI pins). Create-new still re-registers. Adopt
        skips catalog create.
      artifacts:
        - create_namespace_recreate_conflicting_location_fails_loud
        - sql_create_namespace_if_not_exists_conflicting_location_fails_loud
    - id: AT-4
      status: ATTACKED
      evidence: >
        exists-then-get_namespace is TOCTOU; drop-between fails loud via
        get_namespace. Same class as pre-existing create. Not a new silent path.
    - id: AT-5
      status: N/A
      justification: no auth, no AWS, no path-compose of user input beyond stored URI compare
    - id: AT-6
      status: ATTACKED
      evidence: >
        Stored location unchanged after refuse. Matching adopt does not
        overwrite. resolve_namespace_location precedence (location then
        location_uri) reused, not a new key pick.
    - id: AT-7
      status: N/A
      justification: no unbounded work; one exists + one get per adopt
    - id: AT-8
      status: ATTACKED
      evidence: >
        Error::Analysis / DataFusionError::Plan (engine_err → Analysis).
        Facade expects AnalysisException. No binding edit.
    - id: AT-9
      status: ATTACKED
      evidence: message names namespace + both paths (predicate unit + all three integration conflict pins)
    - id: AT-10
      status: ATTACKED
      evidence: >
        Mutation: drop exists-check → same/no-location become AlreadyExists
        (core tests red). Drop compare → conflict silently adopts (conflict
        tests red). Drop SQL hook → Spark/ANSI conflict tests red, core still
        green. Facade is one function covering four shapes (charter: one pin).
        Synonym SCHEMA/DATABASE share the handler; conflict not re-pinned per
        synonym.
  reattested: []
  complete: true
  s0_fresh_execution:
    input: >
      predicate location_uri-only existing `/warehouse/glue` vs request
      location `/warehouse/glue` (not a Session integration case; the
      committed unit test is the closest public-function run). Additional
      novel: `/warehouse/a` vs `/warehouse/a/extra` after slash-strip still
      differs — executed via the same refuse function in
      namespace_create::tests::conflicting_locations_name_both_paths shape
      (paths `/warehouse/a` vs `/warehouse/b`).
    entry_point: repark_core::refuse_contradictory_namespace_location
    observed: conflict names both paths; matching keys adopt
    expected: same
    note: sequential-mode compensation; re-ran committed unit tests, not a new binary
```

```yaml
FINDING:
  id: F-R6-1
  severity: S2
  category: AT-2
  clause: [C-003, C-007]
  claim: Trailing-slash equivalence is pinned only on the pure predicate, not through Session or SQL IF NOT EXISTS.
  evidence: namespace_create.rs::trailing_slash_is_not_a_conflict; no sibling in session/namespace_create_tests.rs or namespace_ddl.rs
  disposition: REMEDIATED (create_namespace_recreate_trailing_slash_is_idempotent)
```

```yaml
FINDING:
  id: F-R6-2
  severity: S2
  category: AT-10
  clause: [C-009]
  claim: Facade pin asserts exception text only; it does not re-read stored location after the conflict.
  evidence: test_namespace_location_guard.py (no catalog property read-back)
  disposition: ACCEPTED_FLAGGED
```

No finding at/above S1. Critic-1 converges.

### 3.3 Critic-2

Context break executed; attacking artifacts, not memory.

Re-walked the same taxonomy. Additional attacks:

- Spark `CREATE SCHEMA` / `CREATE DATABASE` IF NOT EXISTS LOCATION share
  `execute_create_namespace` (parser synonyms). Conflict branch is one. Not a
  second silent path.
- ANSI extra vs fence: two-doors + silent-wrong class; C-013. Not a defect.
- `file://` vs bare path still fail-closed (documented residual). Not S1.
- `test_create_namespace_duplicate_raises_analysis_exception` (no IF NOT EXISTS)
  still AlreadyExists — C-011 holds.

No new findings at/above S1. No S2 beyond F-R6-1/F-R6-2. Converged.

```yaml
COVERAGE_ATTESTATION:
  pr_unit: R-6
  critic: Critic-2
  categories:
    - id: AT-1
      status: ATTACKED
      evidence: re-read charter vs diff; C-010 CLOSED surfaces absent; C-011 SQL-without-IF-NOT-EXISTS unchanged
    - id: AT-2
      status: ATTACKED
      evidence: re-checked slash-strip / location_uri / empty-existing; F-R6-1 stands
    - id: AT-3
      status: ATTACKED
      evidence: refuse does not call create_namespace or update_namespace
    - id: AT-4
      status: ATTACKED
      evidence: TOCTOU same as pre-existing catalog create
    - id: AT-5
      status: N/A
      justification: no security surface
    - id: AT-6
      status: ATTACKED
      evidence: stored location asserted unchanged on all three conflict pins
    - id: AT-7
      status: N/A
      justification: not system-breaking
    - id: AT-8
      status: ATTACKED
      evidence: Plan→Analysis; facade AnalysisException
    - id: AT-9
      status: ATTACKED
      evidence: both paths in message
    - id: AT-10
      status: ATTACKED
      evidence: F-R6-2 stands; Rust pins would red if location were rewritten
  reattested: [AT-1, AT-2, AT-10]
  complete: true
```

**Convergence:** attestation complete, no open finding ≥ S1. Cycle 1 of 2.

---

## 4. Gates

| Gate | EC | Where |
|---|---|---|
| `make verify` | 0 | `/tmp/grok-r6`, cd-fused, 2026-08-14 |
| `make preflight` | 0 | `/tmp/grok-r6`, cd-fused, 2026-08-14 (facade 3054 passed / 71 skipped; `test_namespace_location_guard.py` 1 passed) |

---

## 5. Closed / residual

**Closed:** silent contradicted adopt on core `create_namespace` and Spark
`CREATE … IF NOT EXISTS` (and ANSI `CREATE SCHEMA IF NOT EXISTS`).

**Residual / §6:**

1. Spark live `CREATE SCHEMA … LOCATION` contradicted-adopt behavior was **not**
   probed (JVM lock not taken). Fail-loud is engine policy, not a parity row.
2. SQL **without** `IF NOT EXISTS` remains `NamespaceAlreadyExists` even when
   locations match — Spark-shaped, unchanged.
3. Binding `repark-python/src/session.rs` untouched (S-3 neighbor). Facade pin
   rides the existing `create_namespace(location=)` forwarding.
4. Trailing-slash equivalence only; `file://` vs bare path still fail-closed.
5. No `getDatabase` facade activation (G-6 follow-on 1) — out of fence.

---

## 6. Handoff (paste-true for the landing increment)

**R-6 / G-6 Q1 — namespace-create location guard.** An idempotent create that
would adopt an existing namespace whose resolved `location`/`location_uri`
contradicts the request now **fails loud**, naming both paths. Matching location
and no-request-location stay idempotent. Altitude: engine
(`Session::create_namespace` + SQL `IF NOT EXISTS` on both doors via
`repark_core::refuse_contradictory_namespace_location`). Not a Spark-parity
surface (G11-adjacent data-loss guard). No JVM probe tonight. Catalog crate and
fork unchanged. Registry: no row required (this is not a dialect divergence);
optional STATUS note that G-6 Q1 engine follow-on is closed.
