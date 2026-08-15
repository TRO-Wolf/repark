# Unit ledger — MG-1 scan-prune / residual-probe hardening

**Unit:** MG-1 · **Date:** 2026-08-15 ·
**Lane:** repark · **Executor:** Grok (grok-4.6) ·
**Worktree:** `/tmp/grok-mg1` · **Branch:** `grok/mg1-scanprune-hardening` ·
**Base (FROZEN):** `a2b385f4113a725a3b013553d2ee99fcf8278cfb`
(`refactor(python): split functions.py (FN-SPLIT, move-only) (#105)`)

**Charter:** `planning/grok/BRIEF-mg1-scanprune-hardening.md` +
conductor-12 Addendum A1–A9 (A3 fence, A4 skip-conjunct, A5 test file).
**SEPMO:** HIGH — octo + C4. Floor S1. `octo.cycles=4`, `early_stop=true`,
`claims_critic=true`, `octo.after=actor_build_green`.

This ledger does **not** edit `docs/spark-sql-iceberg-parity.md` or
`STATUS.md` (A9 — registry/STATUS closed). §6 is paste-true for a later
landing increment.

### Proposition ledger (scope audit)

| ID | Proposition | Verdict |
|---|---|---|
| C-001 | M5 scanners walk `char_indices()` (or char-boundary slices only). | PROVEN — `split_and_conjuncts` / `contains_or_outside_parens` / `matches_keyword_at`; rust `utf8_*` + python `test_merge_on_utf8_literal_does_not_panic` + `test_merge_on_utf8_column_name_does_not_panic` |
| C-002 | ASCII ON behavior is unchanged (existing scan_prune battery stays green). | PROVEN — existing `extracts_*` / `or_skips_all` / `null_safe_*` kept; `make verify` |
| C-003 | M1: push a bounds conjunct only when source Arrow type == target Int32/Int64 type. | PROVEN — `identical_int_key_width`; python r1 2-row pin; rust skip + `utf8_source_int32_target_does_not_push_residual` |
| C-004 | A4: no Utf8=Utf8 prune, no order-preserving-cast whitelist. | PROVEN — `identical_int_key_width` returns None for Utf8=Utf8; no new `range_predicate` types |
| C-005 | M6: any remaining probe failure continues that conjunct, never `?`-aborts MERGE. | PROVEN — `try_int_key_range_predicate` / `residual_bounds_predicate` return `Option`; python r2 |
| C-006 | M7: resolve source column case-insensitively first, then quote the resolved name; ambiguous → skip. | PROVEN — `unique_schema_field` via `CaseInsensitiveColumnIndex`; rust mixed-case push + collision skip; python r11 on≡off |
| C-007 | `residual_join_key_filter` stays a thin caller; new logic lives in `scan_prune.rs`. | PROVEN — function body + `residual_bounds_predicate` |
| C-008 | `merge/mod.rs` stays ≤2700 (net-zero or net-negative). | PROVEN — measured 2631 / 2700 after the thin |
| C-009 | `resolve_schema_field_name` / `source_column_names` are not edited. | PROVEN — diff names |
| C-010 | Orchestrator-reserved symbols (`insert_sql`, `cast_one_batch_to_write_schema`, `insert_projection`, `resolve_merge_mode`, `conflict_detection_filter`, commit arms) are not edited. | PROVEN — diff hunks only in `residual_join_key_filter` + import |
| C-011 | `repark.merge.scan-pruning` default unchanged. | PROVEN — `ReparkMergeConfig.scan_pruning` still `default = true` |
| C-012 | Existing PERF-04 residual-push pins stay green (identical Int32/Int64 still prune). | PROVEN — `cow_equi_key_residual_keeps_colocated_survivors`, `mor_equi_key_residual_upsert_correct`, `residual_pushes_identical_int32_keys` |
| C-013 | Python pins land in `test_merge_scan_prune_semantics.py` only; `test_merge_semantics_audit.py` / `test_merge_insert_scope*` / `test_merge_store_assign*` untouched. | PROVEN — diff names |
| C-014 | map.md lockstep + this ledger linked from `task/map.md` in the same change. | PROVEN — listed in §2 |
| C-015 | `make verify` exit 0. `make preflight` before `gh pr create`. | PROVEN verify — §4. preflight recorded at Delivery. |

**Enumeration:** four repro cases × {rust unit / rust scan-level / python Arrow} as chartered.
R1 also has the streaming-scan push-counter pin (must stay 0).

---

## 0. Blast + seams

Findings live in `planning/hardening/MERGE-AUDIT-FINDINGS.md` (M1/M5/M6/M7).
The residual bounds probe sat in `residual_join_key_filter` (`merge/mod.rs`)
and walked ON text + source min/max in `scan_prune.rs`.

| Finding | Today (freeze) | After |
|---|---|---|
| M5 | `as_bytes()` + `&sql[index..]` mid-UTF-8 → panic on `'Zürich'` | `char_indices()` + `is_char_boundary` guard |
| M1 | min/max in source type, strict-cast to target → inverted Utf8 range | skip unless source type == target type and that type is Int32/Int64 |
| M6 | `cast_with_options(...)?` aborts MERGE on BIGINT 3e9 → INT | `continue` / `Option` — skip conjunct |
| M7 | `quote_ident(original_case)` vs case-insensitive join | `CaseInsensitiveColumnIndex` resolve, then `quote_ident_spark(resolved)` |

Altitude: engine (`repark-iceberg` write path). No config-knob change.

---

## 1. Implementation

- `crates/repark-iceberg/src/write/scan_prune.rs` — scanners + helpers
  `identical_int_key_width`, `unique_schema_field`, `residual_bounds_predicate`.
- `crates/repark-iceberg/src/write/merge/mod.rs` — `residual_join_key_filter`
  only (banner + thin call). 2700 → 2631.
- Tests: scan_prune unit battery; `utf8_source_int32_target_does_not_push_residual`
  in `streaming_scan_tests.rs`; new
  `python/repark/tests/test_merge_scan_prune_semantics.py`.

Reuse, not hand-roll: `CaseInsensitiveColumnIndex` / `SourceMatch` (name_resolution)
and `quote_ident_spark` (idents). `resolve_schema_field_name` /
`source_column_names` were **not** edited.

---

## 2. Files

- `crates/repark-iceberg/src/write/scan_prune.rs`
- `crates/repark-iceberg/src/write/merge/mod.rs` (`residual_join_key_filter` only)
- `crates/repark-iceberg/src/write/merge/streaming_scan_tests.rs`
- `crates/repark-iceberg/src/write/map.md`
- `crates/repark-iceberg/src/write/merge/map.md`
- `python/repark/tests/test_merge_scan_prune_semantics.py` (new)
- `python/repark/tests/map.md` (additive row)
- `task/mg1-scanprune-hardening-ledger.md` (this file)
- `task/map.md` (lockstep row)

---

## 3. Pins — repro → test → red-without-fix

| Finding | Repro | Pin | Red without the fix |
|---|---|---|---|
| M1 | battery r1 | `test_merge_string_source_int_target_updates_both_keys` + `utf8_source_int32_target_does_not_push_residual` + `residual_skips_utf8_source_int32_target` | 4 rows / lost updates; rust push-counter == 1 |
| M5 | battery r3 | `test_merge_on_utf8_literal_does_not_panic` + `test_merge_on_utf8_column_name_does_not_panic` + rust `utf8_*` | panic / `repark internal error` |
| M6 | battery r2 | `test_merge_bigint_source_int_target_does_not_abort` + `residual_skips_int64_source_int32_target_without_abort` | Arrow cast abort |
| M7 | battery r11 | `test_merge_mixed_case_on_matches_pruning_off` + `residual_resolves_source_column_case_insensitively` | `No field named "CustomerId"` with pruning on |

**Existing tests that prove identical-type pruning still fires:**

- `cow_equi_key_residual_keeps_colocated_survivors` (push == 1, survivors kept)
- `mor_equi_key_residual_upsert_correct` (push == 1)
- `cow_file_scoped_off_does_not_push_residual` (mode gate)
- `scan_pruning_false_does_not_push_residual` (conf gate)
- `residual_pushes_identical_int32_keys` (new unit; Some residual)

---

## 4. Gates

Recorded at Actor-build / Delivery time:

| Gate | Exit |
|---|---|
| `make verify` | 0 (Actor, 2026-08-15) |
| `make preflight` | pending — before `gh pr create` |
| `merge/mod.rs` lines | 2631 / 2700 |
| `streaming_scan_tests.rs` lines | 3345 / 3400 |
| `scan_prune.rs` lines | 839 / 1500 |

---

## 5. Octo

Scratch: `/tmp/critic-octo-repark-2026-08-15/`.
**Label:** `OCTO-CONVERGED` (early stop after cycle-2 Half A CLEAN).
Cycle 1: C1-Q-001 S1 REMEDIATED (M5 python split) + CL-001/002 cheap.
Cycle 2: no OPEN ≥ S1. Cycles 3–4 skipped (`early_stop`).
`OCTO-CONVERGED` is not ship; readiness is `SEPMO-UNIT-READY` after
`make preflight` + identity check.

---

## 6. Registry paste-true (not landed — A9)

M1/M5/M6/M7 rows for `docs/spark-sql-iceberg-parity.md` belong to the
orchestrator landing increment when this PR merges. This unit does not
touch the registry.

**Residual (named, not in this PR):** order-preserving casts (Int32→Int64
bounds) and Utf8=Utf8 prune types stay OUT (A4). M15
`conflict_detection_filter` is orchestrator-reserved (do not narrow to the
residual).
