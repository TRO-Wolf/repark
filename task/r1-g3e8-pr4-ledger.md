# Unit ledger — R-1 / G3-E8 PR-4: identity UPDATE IN + correlated IN + ANY/ALL

**Unit:** G3-E8 phase 1, PR-4 (family close) · **Date:** 2026-08-14 · **Lane:** overnight R-1 ·
**Worktree:** `/tmp/grok-r1` · **Branch:** `grok/r1-g3e8-pr4` ·
**Base (frozen):** `fddf1bc4840ade68274ca5c55993dda0fb182a61` (`#94`)

**Charter:** `BRIEF-r1-g3e8-pr4.md` + `BRIEF-overnight-conductor-9.md` Amendment +
Addendum A1–A11 + `G3E8-FIX-DESIGN.md` + y2 addendum (Q1=A1-identity, A11/D-4).
Repo contracts win.

**SEPMO:** HIGH · octo + C4 · `cycles=4` · `early_stop=true` · `claims_critic=true`.
Spawn unavailable — sequential hat-switch, no Grok isolation worktree.

---

## 0. Design (§0)

| Decision | Ruling | Why |
|---|---|---|
| UPDATE home | Same module `predicate_dml.rs`; tests in sibling `predicate_dml_update_tests.rs` | DELETE test file is 1382/1500. UPDATE write path (SET projection, COW UNION ALL, MoR delete+append, `write.update.mode`) lives next to DELETE execute. No new crate. |
| `merge/mod.rs` / `overwrite.rs` | **No factor.** `overwrite.rs` untouched. `merge/mod.rs` identity-diff | Adding `RowDeltaKind::Update` blew the 2700-line ceiling by 1. Java buckets UPDATE with MERGE (L251-254); identity UPDATE reuses `RowDeltaKind::Merge`. Existing MERGE tests stay identity-diff. |
| Correlated IN | Extend IN allow-list (`subquery_has_disallowed_ref`); pass through to identity SELECT | Recorded row-sets equal correlated EXISTS on every fixture. Equivalence recorded, not assumed. No rewrite to EXISTS. |
| ANY / ALL | **DEFERRED entire family** | Live Spark 4.1.2 `ParseException` on every `= ANY` / `<> ALL` / `> ANY` / `= SOME` / … DML **and** SELECT spelling. `ANY(array)` is the boolean aggregate. A4 cannot ship an equivalence Spark cannot parse. Family pins stay. |
| D-4 | SET-subquery stays ungated / unimplemented | `try_allowed_update_in` returns `None` if any SET value contains a `Query`. Non-subquery-WHERE SET-subquery still executes on the DF path. |
| Valve remainder | Permanent v1 set | mixed AND/OR, nested, CTE (loud today), scalar `WHERE`, SET-subquery, UPDATE NOT IN / EXISTS, every ANY/ALL, USING/RETURNING. ROW 9 restated as such. |

---

## 0. Proposition ledger (scope audit)

| ID | Proposition | Verdict | Evidence |
|---|---|---|---|
| C-001 | Identity `UPDATE … SET <scalar> WHERE col IN (SELECT …)` executes both doors | PROVEN | `try_allowed_update_in` + `execute_identity_update`; Spark `g3e8_update_in_subquery_*`; ANSI `dml_subquery_correlated_in_and_update_in_execute`; `cross_door_g3e8_update_in_executes_identically` |
| C-002 | Fixtures: multi-column SET, SET expression, NULL keys, dups, empty, MoR vs COW | PROVEN | Rust `identity_update_*` + Python `update_in_subquery_{multi_set,expr,empty}` + live probe 2026-08-13 |
| C-003 | D-4: `SET col = (SELECT …)` stays ungated / unimplemented | PROVEN | allow-list refuse; `g3e8_update_set_subquery_without_where_subquery_still_executes` green |
| C-004 | Correlated IN ships; identity SELECT ≡ EXISTS ≡ Spark on every fixture | PROVEN | probe + `identity_select_correlated_in_matches_exists_and_spark_412` + content row |
| C-005 | A4: ship only if identity SELECT / recorded result matches live Spark 4.1.2 | PROVEN | official record 27/0; probe UPDATE + corr IN transcripts |
| C-006 | ANY/ALL stay refused with family pins | PROVEN | Spark ParseException on every operator; ROW 9 + `g3e8_delete_subquery_family_all_refuse` |
| C-007 | ROW 9 restated as permanent v1 valve | PROVEN | `cross_door_g3e8_refusals_render_identically` + two new executed columns |
| C-008 | `merge/mod.rs` / `overwrite.rs` / existing MERGE tests identity-diff | PROVEN | `git diff` empty on `merge/mod.rs` and `overwrite.rs` |
| C-009 | Out of scope stays out: mixed, nested, CTE, scalar, SET-subquery, UPDATE NOT IN, QueryPlanner, detector hoist, registry file | PROVEN | splits / family pins remain; no `docs/spark-sql-iceberg-parity.md` / `STATUS.md` |
| C-010 | Python stays `repark.sql`-era imports | PROVEN | no `import repark.spark` in edited files |

---

## 0b. Fixtures + live Spark 4.1.2 transcripts

Recorded 2026-08-13 under `/tmp/grok-jvm-record.lock` (`MARKER=r1-record`).
`JAVA_HOME=/usr/lib/jvm/zulu-17-amd64 SPARK_LOCAL_IP=127.0.0.1`, Iceberg GAV
`org.apache.iceberg:iceberg-spark-runtime-4.1_2.13:1.11.0`, `local[2]`, ANSI on.

Standalone probe `/tmp/grok-r1-probe-g3e8-pr4.py` (not committed) then official
`_record_dml_subquery_goldens.py` — **27 rows, 0 mismatch**.

### UPDATE IN (write path)

| Fixture | Spark remaining `(id, name)` |
|---|---|
| basic `SET name='z'` keys={2} | `(1,a), (2,z), (3,c)` |
| multi `SET name='z', id=id+10` | `(1,a), (3,c), (12,z)` |
| expr `SET name=concat(name,'_x')` | `(1,a), (2,b_x), (3,c)` |
| empty keys | `(1,a), (2,b), (3,c)` (no commit) |
| NULL target id | `(NULL,n), (1,a), (2,z)` |
| NULL in keys | `(1,a), (2,z), (3,c)` — `2 IN (2,NULL)` is TRUE |
| duplicate target+keys | `(1,z), (1,z), (2,b)` |
| NULL both sides | `(NULL,n), (1,a), (2,z)` |

MoR vs COW is write-mode honesty (same remaining row-set); pinned in Rust
`identity_update_honors_write_update_mode_not_merge_or_delete_mode`
(`write.update.mode` vs contradictory `write.delete.mode` + `write.merge.mode`).

### Correlated IN ≡ EXISTS (recorded, not assumed)

| Fixture | Spark remaining after IN | after EXISTS |
|---|---|---|
| some keys={2} | `(1,a), (3,c)` | `(1,a), (3,c)` |
| empty keys | all three | all three |
| NULL both sides | `(NULL,n), (1,a)` | `(NULL,n), (1,a)` |
| duplicates | `(2,b)` | `(2,b)` |
| name IN … WHERE k.id = t.id | `(1,a), (3,c)` | `(1,a), (3,c)` |

### ANY / ALL — Spark 4.1.2 cannot parse (mismatch evidence)

Every operator (`= ANY`, `<> ALL`, `> ANY`, `> ALL`, `< ANY`, `< ALL`, `= ALL`,
`<> ANY`, `>= ANY`, `<= ALL`, `= SOME`) on DELETE **and** on SELECT:
`ParseException: Syntax error at or near '('`. `id = ANY(ARRAY(2))` analyzes as
the boolean aggregate `any(array(2))` → `DATATYPE_MISMATCH`. **No operator ships.**

---

## 1. What landed

1. **`try_allowed_update_in`:** uncorrelated positive `UPDATE … SET <scalar columns> WHERE col IN (SELECT …)`. Tuple SET, SET-subquery, NOT IN, EXISTS, ANY/ALL, FROM/RETURNING stay `None`.
2. **`execute_identity_update`:** identity SELECT projects `_file, _pos` + SET-applied data columns. Empty match commits nothing. COW: survivors UNION ALL new values → `commit_overwrite`. MoR: position-delete + append via `commit_row_delta_kind(RowDeltaKind::Merge)` honoring `write.update.mode` / `write.update.isolation-level`.
3. **Correlated IN on DELETE:** `is_allowed_in_selection` (same correlation rules as EXISTS). FQN rewrite already existed.
4. **Doors:** `spark_ast::execute_passthrough` + ANSI `Delete\|Update` arm + Spark `execute_update` early skip + both valve skip lists.
5. **ROW 9** restated as the permanent v1 valve; executed columns for correlated IN and UPDATE IN.
6. **Corpus:** `delete_correlated_in_subquery` + `update_in_subquery` flip `split` → `content`; three new UPDATE fixture content rows. `update_not_in_subquery_with_null_key` stays split.

---

## 2. Per-spelling completeness table

| # | Spelling | Disposition | Evidence |
|---|---|---|---|
| 1 | uncorrelated `DELETE … IN` | already LANDED (PR-1) | unchanged |
| 3–4 | `NOT IN` + NULL trap | already LANDED (PR-2) | unchanged |
| 5–8 | `[NOT] EXISTS` ± correlation | already LANDED (PR-3) | unchanged |
| 9 | correlated `DELETE … IN` | **LANDED** | probe ≡ EXISTS; identity SELECT pin; both doors; Python content `{1,3}` |
| 10 | ANY / ALL (all operators) | **DEFERRED** | Spark ParseException; family pins; ROW 9 |
| 11–16 | nested / mixed AND/OR / scalars / CTE | **DEFERRED** (permanent v1 valve) | ROW 9 + family pins |
| 17 | `UPDATE … IN` (scalar SET) | **LANDED** | probe + Rust fixture matrix + both doors + Python content |
| 17b | UPDATE multi-SET / expr / empty / NULL / dups / MoR vs COW | **LANDED** | Rust + Python extras + probe |
| 18 | `UPDATE SET col = (SELECT …) WHERE <subquery>` | **DEFERRED** (D-4) | allow-list `None` |
| 19 | `UPDATE SET col = (SELECT …)` no WHERE subquery | **untouched** (D-4) | existing pin green |
| 20 | UPDATE NOT IN / EXISTS | **DEFERRED** | family + `update_not_in_subquery_with_null_key` split |
| 21 | CTE-prefixed DML | **DEFERRED** (loud today) | existing pin |

---

## 3. A4 identity-SELECT / record evidence

- Official driver: **27 rows, 0 mismatch** (2026-08-13T21:48, `MARKER=r1-record`).
- Correlated IN identity SELECT remaining equals EXISTS remaining equals Spark on 4 fixtures (some / empty / NULL / dups).
- UPDATE write-path remaining matches the probe table on every charged fixture.
- `execute_predicate_dml` does not special-case empty/all; the SELECT vehicle is the oracle.

---

## 4. Allow-list (fail-closed)

**DELETE accepted when all of:** `Statement::Delete`; no USING/RETURNING/OUTPUT/LIMIT/ORDER BY/multi-table; three-part Iceberg target; `WHERE` is `InSubquery` (uncorrelated or target-correlated) or `[NOT] EXISTS`; simple SELECT, one plain FROM, no WITH/ORDER/LIMIT/DISTINCT/GROUP/HAVING/joins/nested Query.

**UPDATE accepted when all of:** `Statement::Update`; no FROM/RETURNING/OUTPUT/LIMIT/ORDER BY/joins; nonempty scalar SET (no `Query` in values, no tuple targets, no duplicate columns); `WHERE` is uncorrelated **positive** `col IN (SELECT …)`; three-part Iceberg target.

Unhandled ⇒ valve, never DataFusion DML. Needle unchanged:
`subquery predicates are silently mis-executed`.

---

## 5. Lock events

| Event | Time (local -04:00) | Detail |
|---|---|---|
| inspect | 21:21:50 | lock absent; no local `pyspark`/`SparkSubmit` (HiveThrift ignored) |
| ACQUIRE | 21:21:58 | `/tmp/grok-jvm-record.lock` `MARKER=r1-record pid=2727182` (`set -o noclobber`) |
| refresh | 21:27:26 | `step=refresh-pre-sync` pid=2771224 |
| refresh | 21:28:22 | `step=probe` pid=2778314 |
| probe | 21:28:22–21:29:10 | UPDATE IN + corr IN + ANY/ALL vs Spark 4.1.2 |
| refresh | 21:29:23 | `step=probe-anyall-select` |
| probe | 21:29:23–21:29:40 | SELECT-side ANY/ALL also ParseException |
| refresh | 21:48:03 | `step=official-record` pid=3007306 |
| record | 21:48:03–21:48:48 | official driver **27 rows, 0 mismatch** |
| RELEASE | 21:48:49 | marker-verify `lane=r1-g3e8-pr4`; `rm` of **own** lock only |
| sentinel | 21:48:49 | `/tmp/grok-r1-first-released` |

No stale-rm. No foreign marker overwritten.

---

## 6. Registry handoff (R-7 deferred — do **not** edit the registry)

**Do not delete G3-E8.** Surface is still partially refused (permanent v1 valve below).

### G3-E8 (BACKLOG — update, do not delete)

- **repark** — `DELETE`/`UPDATE` with a subquery `WHERE` are still **refused** (needle
  `subquery predicates are silently mis-executed`) **except** uncorrelated
  `DELETE … WHERE col IN (SELECT col FROM …)`, uncorrelated
  `DELETE … WHERE col NOT IN (SELECT …)` including the NULL 3VL trap,
  `DELETE … WHERE [NOT] EXISTS (SELECT …)` uncorrelated and correlated,
  correlated `DELETE … WHERE col IN (SELECT s.col FROM s WHERE s.k = t.k)`,
  and identity `UPDATE … SET <scalar assignments> WHERE col IN (SELECT …)`.
- **Permanent v1 valve (ROW 9 final form):** mixed AND/OR, nested subquery FROM,
  CTE-prefixed DML (loud `NotImplemented` today), uncorrelated scalar `WHERE`,
  `SET col = (SELECT …)` (D-4, ungated when WHERE is clean), UPDATE NOT IN / EXISTS,
  and every ANY/ALL quantified comparison (Spark 4.1.2 parse-fails the family).
- **G3-E8-NULL** stays (NOT IN + NULL is already content on DELETE; UPDATE NOT IN remains split).

---

## 7. Octo + C4 (sequential hats)

`cycles=4`, `early_stop=true`, `claims_critic=true`. Spawn unavailable.

### Cycle 1 — Critic-1 quality

Findings:

- **C1-Q-001 (S1, remediating):** `RowDeltaKind::Update` would have raised
  `merge/mod.rs` over its 2700-line ceiling. Reverted to reuse `RowDeltaKind::Merge`
  (Java UPDATE/MERGE bucket). `merge/mod.rs` identity-diff.
- **C1-Q-002 (S1, remediating):** clippy `too_many_arguments` on
  `commit_identity_update_cow` — bundled `(pairs, batches)`.
- **C1-Q-003 (S1, remediating):** `doc_markdown` on `MoR` — backticked.

After remediations: CLEAN ≥ S1.

### Cycle 1 — Critic-2 security

- Fail-closed allow-list: unhandled ⇒ valve, never DF DML.
- SET-subquery cannot enter the identity path.
- Non-three-part targets stay valved.
- No AWS / secrets / lockfile / `.github` edits.
- CLEAN.

### Cycle 1 — Critic-3 logic

- UPDATE empty match: no commit (same as DELETE).
- IN + NULL in keys: `2 IN (2,NULL)` TRUE — matches Spark, not the NOT IN trap.
- Correlated IN NULL: `NULL IN (SELECT … WHERE k.id = NULL)` is empty ⇒ FALSE; matches EXISTS.
- MoR UPDATE honors `write.update.mode` even when delete/merge modes contradict.
- CLEAN.

### Cycle 1 — Critic-4 claims

- Every charged spelling is LANDED or DEFERRED with evidence (table §2).
- ANY/ALL not silently omitted — DEFERRED with ParseException evidence.
- D-4 not implemented.
- ROW 9 stated as permanent v1 valve.
- CLEAN.

**Cycle 1 Half A CLEAN ≥ floor.** Remaining cycles skipped (`early_stop`).

**OCTO-CONVERGED**. **SEPMO-UNIT-READY** after gates + `%ae`.

---

## 8. Gates

| Gate | Exit |
|---|---|
| `make verify` | **0** |
| `make preflight` | **0** (facade `3056 passed, 71 skipped`) |
| official record | **0** (27/0) |
