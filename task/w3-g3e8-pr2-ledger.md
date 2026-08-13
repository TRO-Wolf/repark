# Unit ledger — W-3 / G3-E8 PR-2: NOT IN + the NULL trap

**Unit:** G3-E8 phase 1, PR-2 · **Date:** 2026-08-13 · **Lane:** overnight W-3 ·
**Worktree:** `/tmp/grok-w3` · **Branch:** `grok/w3-g3e8-pr2` ·
**Base (frozen):** `c7e6589088111ded62848751a30a45adfea0973a` (`#79`)

**Charter:** `planning/grok/BRIEF-w3-g3e8-pr2.md` + `G3E8-FIX-DESIGN.md` PR-2 +
BRIEF-y2 addendum + conductor-6 A4/A5. Design authority is the memo; addenda win.

**SEPMO:** HIGH · octo + C4 · `cycles=4` · `early_stop=true` · `claims_critic=true`.
Spawn unavailable — sequential hat-switch, no Grok isolation worktree.

**A5 fence:** `spark_ast.rs` stays **byte-identical** (comments included). The L69–91
path already calls `try_allowed_delete_in` generically. The stale "IN only" comment
is W-4's ride-along.

---

## 0. Proposition ledger (scope audit)

| ID | Proposition | Verdict | Evidence |
|---|---|---|---|
| C-001 | Enable uncorrelated `DELETE … WHERE col NOT IN (SELECT …)` **and** its NULL trap as one unit, only through full-statement `try_allowed_delete_in` | PROVEN | `predicate_dml.rs` `is_allowed_uncorrelated_in_selection` accepts `InSubquery { negated: _ }`; USING/RETURNING stay `None` |
| C-002 | Spark 3VL: ANY NULL in the subquery ⇒ match **zero** rows; empty subquery ⇒ match all; no "match none" shortcut bypassing the executed SELECT | PROVEN | `identity_select_not_in_matches_spark_three_valued_logic` is the SELECT the identity path runs; `execute_predicate_dml` still formats `WHERE {selection}` verbatim |
| C-003 | A4: ship only if that SELECT row-set matches live Spark 4.1.2 3VL on every fixture. Mismatch ⇒ `negated: false` unchanged + named deferral | PROVEN | DF SELECT + identity DELETE + facade content rows + live record 2026-08-13T13:26 `10 rows, 0 mismatch` (`delete_not_in_subquery` + `*_with_null_key` PASS) |
| C-004 | Fixtures: empty subquery, NULL in subquery, NULL in target column, duplicates both sides, MoR vs COW | PROVEN | Rust identity + Spark/ANSI door pins for empty; Python flips the two recorded NOT IN goldens |
| C-005 | USING/RETURNING and every compound spelling stay intercepted | PROVEN | allow-list refuse: `NOT (id IN)`, OR/AND, EXISTS, UPDATE, USING+NOT IN, RETURNING |
| C-006 | `cross_door.rs` ROW 9 restated: NOT IN moves to executed column; EXISTS ± correlation, nested, CTE, scalar, UPDATE stay refused with unchanged needle | PROVEN | `cross_door_g3e8_not_in_delete_executes_identically` + restated refuse list |
| C-007 | `spark_ast.rs` is byte-identical | PROVEN | `git diff -- crates/repark-spark/src/spark_ast.rs` empty |
| C-008 | No registry edit (§6 handoff text only); W-5 owns the file | PROVEN | this ledger §6; no `docs/spark-sql-iceberg-parity.md` in the diff |
| C-009 | Tests land with code; both doors; Arrow path value AND type AND row-set | PROVEN | Rust identity + both-door execute + Python content rows |
| C-010 | Out of scope stays out: EXISTS, UPDATE IN, QueryPlanner, detector hoist, rest of `write/**` | PROVEN | allow-list refuse + no merge/mod.rs edit this unit |

---

## 1. What landed

1. **Allow-list:** `try_allowed_delete_in` accepts uncorrelated
   `DELETE … WHERE col NOT IN (SELECT col FROM <table> …)` by treating `InSubquery.negated`
   as either polarity. Same fail-closed shape as PR-1 (no USING / RETURNING / OUTPUT /
   LIMIT / ORDER BY / multi-table; 3-part Iceberg target; simple uncorrelated subquery).
2. **No new write path.** `execute_predicate_dml` is unchanged: it still runs
   `SELECT _file, _pos FROM scratch AS alias WHERE <original predicate>`. NOT IN 3VL is
   whatever DataFusion's SELECT plans. There is no hand-rolled "match none if NULL".
3. **Door wiring:** already generic (Spark `execute_passthrough` + ANSI Delete arm +
   both valve skip sites). Extending the iceberg allow-list is the only product hole.
4. **Valve restated** over EXISTS / nested / scalar / UPDATE / `NOT (id IN …)`.

---

## 2. Per-spelling proof table (memo §5)

| # | Spelling | This PR | Proof |
|---|---|---|---|
| 1 | uncorrelated `IN` | already FIXED (PR-1) | unchanged |
| 3 | `NOT IN` (no NULL) | **FIXED** | identity + Spark `g3e8_delete_not_in_subquery_*` + ANSI `dml_subquery_not_in_delete_*` + Python `delete_not_in_subquery` **content** vs recorded `{2}` |
| 4 | `NOT IN` + NULL key | **FIXED** | identity SELECT + identity DELETE + both-door pins + Python `delete_not_in_subquery_with_null_key` **content** vs recorded `{1,2,3}` |
| — | `NOT IN` empty subquery | **FIXED** | identity + Spark `g3e8_delete_not_in_empty_subquery_*` + ANSI empty CTAS pin (not a new Python corpus row — empty INSERT is outside the 10-row budget) |
| — | `NOT IN` NULL target column | **FIXED** (Rust identity) | `identity_delete_not_in_null_target_column_survives` |
| — | `NOT IN` duplicates both sides | **FIXED** (Rust identity) | `identity_delete_not_in_duplicates_both_sides` |
| — | `NOT IN` MoR vs COW | **FIXED** (Rust identity) | `identity_delete_not_in_honors_write_delete_mode_not_merge_mode` |
| 5–16 | EXISTS / NOT EXISTS / correlated IN / ANY/ALL / nested / mixed AND/OR / scalars | still refused | family + ROW 9 |
| 17 | UPDATE twins | still refused | `g3e8_update_subquery_family_all_refuse`; corpus `update_*` splits |
| 19 | SET subquery, no WHERE subquery | untouched (D-4) | existing pin |

---

## 3. A4 3VL proof (engine)

`identity_select_not_in_matches_spark_three_valued_logic` runs the **same** SELECT the
identity path executes (`SELECT id FROM tgt WHERE id NOT IN (SELECT id FROM keys)`):

| keys | SELECT ids | Spark 3VL |
|---|---|---|
| `{2}` | `{1, 3}` (NULL LHS unknown) | delete non-keys |
| `{2, NULL}` | `{}` | ANY NULL ⇒ match none |
| `{}` | `{NULL, 1, 2, 3}` | empty ⇒ vacuously TRUE |

Identity DELETE pins then commit that row-set through COW/MoR. Live Spark
re-record (lock) confirms the two 2026-08-11 goldens plus the empty-subquery row.

---

## 4. Allow-list (fail-closed)

Accepted only when **all** of:

- `Statement::Delete` (not Update)
- no USING / RETURNING / OUTPUT / LIMIT / ORDER BY / multi-table
- `WHERE` is exactly `InSubquery { negated: true \| false }`
- LHS is a column; subquery is simple uncorrelated (same as PR-1)
- three-part Iceberg target

Unhandled ⇒ valve, never DataFusion DML.

---

## 5. Gates

- `make verify` — exit 0
- `make preflight` — exit 0 (`2922 passed, 71 skipped` facade)
- live record — exit 0 (`10 rows, 0 mismatch`)
- `spark_ast.rs` diff — 0 bytes

---

## 5b. Registry handoff (W-5 owns the registry file — do **not** edit it here)

**Do not delete G3-E8.** Surface is still mostly refused.

### G3-E8 (BACKLOG — update, do not delete)

- **repark** — `DELETE`/`UPDATE` with a subquery `WHERE` are still **refused** (needle
  `subquery predicates are silently mis-executed`) **except** uncorrelated
  `DELETE … WHERE col IN (SELECT col FROM …)` **and**
  `DELETE … WHERE col NOT IN (SELECT col FROM …)` (including ANY-NULL-in-subquery
  matches nothing, empty subquery matches all), which now execute on both doors via
  the A1-identity path. UPDATE IN/NOT IN, EXISTS, scalars, nested, mixed AND/OR
  remain refused. SET-assignment / INSERT / MERGE source still unaffected.
- **Apache Spark** — unchanged (runs all of them).
- **Pin** — `test_dml_subquery_parity.py::test_dml_subquery_row[delete_not_in_subquery]`
  and `…[delete_not_in_subquery_with_null_key]` (now **content**); residual splits
  unchanged; ROW 9 restated over EXISTS / UPDATE IN / nested / scalar.
- **Rationale** — DEFECT, partial fix. Delete the row only when the claimed surface is
  actually re-enabled (memo §6).

### G3-E8-NULL

- **Keep the row.** Flip repark to "matches Spark" for DELETE NOT IN + NULL (the
  identity SELECT reproduces 3VL). UPDATE NOT IN + NULL stays refused.

### Classification

| Item | Status |
|---|---|
| IN-DELETE (uncorrelated) | FIXED (PR-1) |
| NOT IN-DELETE + NULL trap | **FIXED** (this PR) |
| UPDATE IN / UPDATE NOT IN | still refused |
| EXISTS / scalars / rest of §5 | still refused |
| Registry G3-E8 | **BACKLOG** (footnote IN+NOT IN); W-5 pastes |
| Registry G3-E8-NULL | **keep**; repark DELETE half now matches |
| dbt delete+insert → honest DELETE | **not yet** (Q12=A: wait for IN + NOT IN + EXISTS) |

---

## 6. Octo (C4, cycles=4, early_stop)

Spawn unavailable. Sequential hat-switch.

| Cycle | Half A OPEN ≥floor | Half B | Verify |
|---|---|---|---|
| 1 | (Actor) allow-list + 3VL fixtures + door restatement | (Critic) after green | targeted tests |
| 2+ | recorded in this ledger after Critic pass | | |

---

## 7. JVM lock

| Event | Time | Detail |
|---|---|---|
| observed W-1 lock | 12:54–13:25 | `MARKER=w1-blast` pid=2051068; did **not** stale-rm |
| W-1 sentinel | 13:25 | `/tmp/grok-w1-first-released` appeared; lock gone |
| ACQUIRE | 13:26:26 | `/tmp/grok-jvm-record.lock` `MARKER=w3-g3e8` pid=2450558 attempt=1 |
| record | 13:26:40–13:26:59 | `_record_dml_subquery_goldens.py` exit 0; 10 rows, 0 mismatch |
| RELEASE-ON-EXIT | 13:26:59 | trap `rm` of own lock |

Live transcript (verbatim): both flipped NOT IN content rows PASS against Spark 4.1.2 +
`iceberg-spark-runtime-4.1_2.13:1.11.0`. Residual splits still reproduce their goldens.

## 9. Octo cycle 1 Critic (procedural break; sequential, not amnesia)

Context break executed; attacking artifacts, not memory.

COVERAGE_ATTESTATION:
  pr_unit: w3-g3e8-pr2
  categories:
    - id: AT-1
      status: ATTACKED
      evidence: >
        Walked C-001–C-010 against the diff. Allow-list is full-statement
        `try_allowed_delete_in` (`negated: _`); execute_predicate_dml still
        renders WHERE verbatim. spark_ast.rs diff size 0.
    - id: AT-2
      status: ATTACKED
      evidence: >
        Empty subquery, NULL-in-subquery, NULL target column, duplicate keys
        and target rows, MoR vs COW, quoted, FROM-less, 1-part name, USING,
        RETURNING all pinned.
    - id: AT-3
      status: ATTACKED
      evidence: >
        USING/RETURNING stay None → valve. Empty-match commits nothing
        (existing IN pin + NULL trap no-op).
    - id: AT-4
      status: N/A
      justification: no new concurrency surface; reuses MERGE commit arms
    - id: AT-5
      status: N/A
      justification: no auth/secrets/new parser
    - id: AT-6
      status: ATTACKED
      evidence: >
        3VL trap is the integrity claim. Identity SELECT + live Spark
        record + facade content rows all keep `{1,2,3}` when keys contain NULL.
    - id: AT-7
      status: N/A
      justification: no new unbounded path
    - id: AT-8
      status: ATTACKED
      evidence: >
        A5: spark_ast.rs byte-identical. Doors already generic. Needle
        unchanged. ROW 9 restated.
    - id: AT-9
      status: N/A
      justification: no new operator surface
    - id: AT-10
      status: ATTACKED
      evidence: >
        Reverting `negated: _` to `negated: false` reds allow-list + every
        NOT IN execute pin. Facade 2922 passed including flipped content rows.
  reattested: []
  complete: true

FINDINGS: none at/above S1.

S0 fresh execution (novel): Spark-door `g3e8_delete_not_in_empty_subquery_deletes_every_row`
was not in the 2026-08-11 Python corpus; executed through `execute()` (public Spark SQL
door) — observed empty table, expected empty (vacuous NOT IN). Distinct from committed
Python rows.

**OCTO-CONVERGED** cycle 1 (early_stop; no OPEN ≥S1).

---

## 8. Residuals / next PRs

- EXISTS ± correlation (PR-3 — dbt gate)
- UPDATE IN after DELETE of the same predicate
- QueryPlanner (PR-N)
- Detector hoist (Q14=A)
- W-4 may refresh the stale `spark_ast.rs` "IN only" comment
