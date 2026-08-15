# Unit ledger — OCC-1 MERGE isolation (M13) + M15 doc-truth

**Unit:** OCC-1 · conductor-13 T3 · **Date:** 2026-08-15 ·
**Lane:** repark · **Executor:** Grok (grok-4.6) ·
**Worktree:** `/tmp/grok-occ` · **Branch:** `grok/occ1-merge-isolation` ·
**Base (FROZEN):** `cd0db4f459e62994b45f8aadd1d5b58f040d90a5`

**Charter:** `planning/grok/BRIEF-occ-hardening-13.md` OCC-1 + conductor-13
Addendum A9/A10 (A10 supersedes the brief's "trim + NotImplemented" prose).
**SEPMO:** acc + C4. Floor S1. Risk tier: high (commit/OCC). Sequential
hat-switch Actor → Critic-1 → Critic-2 → C4 claims.

This ledger does **not** edit `docs/spark-sql-iceberg-parity.md` or
`STATUS.md`. Production `predicate_dml.rs` is CLOSED (A9/A10).

### Proposition ledger (scope audit)

| ID | Proposition | Verdict |
|---|---|---|
| C-001 | MERGE-local resolver copies live DML `resolve_isolation_property` semantics onto `write.merge.isolation-level`: no trim, `to_ascii_lowercase`, default `Serializable`, garbage ⇒ `DataFusionError::Plan` `Invalid isolation level: {name}`. | PROVEN — `resolve_merge_isolation` + parse pins |
| C-002 | `snapshot` and `serializable` honored; `SNAPSHOT` / `SERIALIZABLE` honor the case fold. | PROVEN — parse pin + SNAPSHOT commit pin |
| C-003 | Padded `  snapshot  ` is GARBAGE (no trim), not snapshot. | PROVEN — `merge_isolation_property_padded_snapshot_is_garbage` |
| C-004 | `commit` / `commit_row_delta` thread the resolved level; they no longer hard-wire `IsolationLevel::Serializable`. | PROVEN — call sites + red-then-green pair |
| C-005 | Snapshot drops `validate_no_conflicting_data` / `validate_no_conflicting_data_files` only; delete-side validations stay armed. | PROVEN — existing rewrite/delete pins stay green; snapshot pins only the data-conflict drop |
| C-006 | Same race: serializable rejects, snapshot commits (Spark S5 / M19-A). Snapshot-commit was RED without the thread-through. | PROVEN — pre-thread cargo: 3 red; post-thread 18/18 |
| C-007 | M15 AlwaysTrue banner: more conservative than the residual; narrowing would be WRONG. | PROVEN — `commit_row_delta` banner cites audit M15 |
| C-008 | IsolationLevel / RowDeltaPolicy / commit-arm docs no longer say “MERGE is always serializable”. | PROVEN — granted-region docs |
| C-009 | Production `predicate_dml.rs` unedited; DML resolver not exported; `named_column` and below unedited. | PROVEN — `git diff --name-only` |
| C-010 | `merge/mod.rs` ≤ 2700 (ratchet-down-only). | PROVEN — measured after rustfmt |
| C-011 | Tests in `occ_tests.rs` only. No xfail. `map.md` + this ledger lockstep. | PROVEN — listed in §2 |
| C-012 | `make verify` then `make preflight` before `gh pr create`. | PROVEN — §4 |

**Enumeration:** upper / padded / garbage parse + serializable-vs-snapshot
behavioral split (COW) + MoR snapshot twin + `commit` Plan-thread pin.

---

## 0. Blast + seams

Finding M13: `write.merge.isolation-level` was never read; MERGE hard-wired
Serializable. `commit_overwrite` / `commit_row_delta_kind` already gated the
data-conflict validations on `IsolationLevel` (identity DML uses them).
Smallest honest diff: a merge-local resolver + two call-site replacements.

M15 is doc-only (AlwaysTrue stays).

| Finding | Today (freeze) | After |
|---|---|---|
| M13 | property unread; snapshot silently ignored | resolver + thread through `commit` / `commit_row_delta` |
| M15 | banner claimed “scan unfiltered” | AlwaysTrue more conservative than residual; narrowing WRONG |

---

## 1. Implementation

- `crates/repark-iceberg/src/write/merge/mod.rs` — granted region only:
  `WRITE_MERGE_ISOLATION_LEVEL`, `resolve_merge_isolation`, thread through
  `commit` / `commit_row_delta`, IsolationLevel/RowDelta* + M15 doc-truth.
- `crates/repark-iceberg/src/write/merge/occ_tests.rs` — parse + M19-A split.
- Maps: `merge/map.md` (commit isolation row), `write/map.md` (Debug
  isolation row), `task/map.md` (this ledger).

Reuse, not hand-roll: `commit_overwrite` / `commit_row_delta_kind` snapshot
arms already drop the data-conflict validations.

---

## 2. Files

- `crates/repark-iceberg/src/write/merge/mod.rs`
- `crates/repark-iceberg/src/write/merge/occ_tests.rs`
- `crates/repark-iceberg/src/write/merge/map.md`
- `crates/repark-iceberg/src/write/map.md` (Debug isolation row only)
- `task/occ1-merge-isolation-ledger.md`
- `task/map.md`

---

## 3. ACC

### Actor

Implemented C-001–C-011. Empirically red-then-green on the unthreaded tree:

- `commit_insert_only_snapshot_isolation_commits_through_conflicting_concurrent_append`
  — External(DataInvalid ⇒ Found conflicting files … test/concurrent.parquet)
- `commit_row_delta_snapshot_isolation_commits_through_conflicting_concurrent_append`
  — same needle
- `commit_rejects_invalid_merge_isolation_level`
  — commit succeeded (ignored garbage) instead of Plan

Parse pins were already green on the resolver-only tree.

### Critic-1

| ID | Sev | Finding | Disposition |
|---|---|---|---|
| C1-1 | S5 | RowDeltaPolicy rustdoc linked private `resolve_merge_isolation` | FIXED — backticks |
| C1-2 | S5 | MoR snapshot pin asserted only `Ok`, not live files | FIXED — live-set assert |
| C1-3 | S5 | COW snapshot pin used lowercase; upper fold not on the commit path | FIXED — `SNAPSHOT` |
| C1-4 | note | Long insert-only banner still narrates the serializable default recipe | ACCEPTED — default is serializable; snapshot clause added |
| C1-5 | S1 ask | Snapshot + AlwaysTrue allows a matching concurrent append (duplicate class) | ACCEPTED — Spark snapshot isolation + M15 (do not narrow the filter) |

No S1 defect in the granted shape.

### Critic-2

Independent re-read of the granted region vs DML resolver and occ pins.

| ID | Sev | Finding | Disposition |
|---|---|---|---|
| C2-1 | — | Resolver match is the DML copy (no trim; `{name}` is the raw value) | CONCUR |
| C2-2 | — | Snapshot only drops data-conflict calls; delete validations untouched | CONCUR |
| C2-3 | — | `named_column` and production `predicate_dml.rs` closed | CONCUR |
| C2-4 | note | `commit` banner still says insert-only “cannot append blindly” | ACCEPTED — describes the default serializable recipe; property gate is named on IsolationLevel + commit_overwrite |

**ACC-CONVERGED** after both critics. No remaining OPEN propositions.

### C4 claims

See §5.

---

## 4. Gates

| Gate | EC |
|---|---|
| `make verify` (single invocation) | **0** |
| `make py-test-facade` (3119 passed, 71 skipped) | **0** |
| `make audit` | **0** |
| `make workflows-lint` | **0** |
| `make preflight` surface (verify + facade + audit + workflow lint) | **0** |

`merge/mod.rs` measured **2606 / 2700**.

---

## 5. C4 claims

1. `write.merge.isolation-level` is read by MERGE `commit` / `commit_row_delta`.
2. Parse semantics match live DML: no trim, ascii-lowercase, default
   serializable, garbage is `Invalid isolation level: {name}`.
3. Snapshot isolation commits the M19-A concurrent-append race; serializable
   rejects the same race.
4. AlwaysTrue conflict filter is documented as more conservative than the
   residual; the residual is not the ON condition (M15).
5. Red-then-green pair:
   `commit_insert_only_snapshot_isolation_commits_through_conflicting_concurrent_append`
   and
   `commit_row_delta_snapshot_isolation_commits_through_conflicting_concurrent_append`.
