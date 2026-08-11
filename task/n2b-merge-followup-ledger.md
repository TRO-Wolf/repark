# Unit ledger — N-2b / W-2: MERGE follow-up (PARTIAL)

**Unit:** N-2b (H-2 gap G3 follow-up) · **Date:** 2026-08-11 · **Lane:** W-2 ·
**Branch:** `grok/w2-n2b-merge-followup` · **Executor:** Grok (grok-4.5)

**This ledger does NOT claim N-2b closed.** Items 1 + 4 ship in this PR; items 2 + 3 are
**explicitly deferred** to a second PR after morning design approval. Design note (item 2
shape only, no code): `planning/grok/W2-LIFECYCLE-DESIGN.md` (orchestrator planning tree,
outside this repo).

Charter: `planning/grok/BRIEF-w2-n2b-merge-followup.md` + overnight addendum A1.

---

## 1. What landed (this PR)

| Item | Artifact | Role |
|---|---|---|
| **1 — 4 Rust MERGE pins** | [`crates/repark-spark/src/tests/merge.rs`](../crates/repark-spark/src/tests/merge.rs) | G3 pins deferred by G-4's file ban |
| map lockstep | [`crates/repark-spark/src/tests/map.md`](../crates/repark-spark/src/tests/map.md) | documents the four new pin names |
| **4 — NIT: GAV pin CP-8** | [`python/repark/tests/test_merge_differential_parity.py`](../python/repark/tests/test_merge_differential_parity.py) | Spark-minor derived from pinned pyspark |
| **4 — NIT: dead knob** | same + `_record_merge_differential_goldens.py` | `spark_needs_cow_props` removed |
| **4 — NIT: re-derive recipe** | module docstring + record driver + this ledger | full parity-live sync line quoted |
| map lockstep | [`python/repark/tests/map.md`](../python/repark/tests/map.md) | N-2b status + re-derive wording |
| this ledger | `task/n2b-merge-followup-ledger.md` | linked from [`task/map.md`](map.md) |

### 1.1 The 4 Rust pins (mirror Python differential shapes)

| # | Test name | Mirrors | Assertion |
|---|---|---|---|
| 1 | `merge_duplicate_source_keys_with_matched_raises` | `duplicate_source_keys_with_matched_raises` | `MERGE_CARDINALITY_VIOLATION`; target untouched |
| 2 | `merge_duplicate_source_keys_insert_only_commits_both` | `duplicate_source_keys_insert_only_commits_both` | both unmatched dup-key source rows insert |
| 3 | `merge_matched_and_arm_order_update_then_delete` | `matched_and_arm_order_update_then_delete` | first-match-wins UPDATE-then-DELETE |
| 4 | `merge_matched_and_threshold_update_or_delete` | `matched_and_threshold_update_or_delete` | threshold multi-arm sibling |

Leaf-private helper: `score_table_rows` (the two score-arm pins). Pre-existing
`merge_cardinality_violation_errors` / `merge_clause_order_first_match_wins` remain as the
simpler shapes; the four new pins are the G3 differential mirrors.

### 1.2 Item 4 NIT dispositions

| NIT | Action |
|---|---|
| Tautological GAV pin | `test_iceberg_gav_pin_is_exact_spark_minor` derives expected `{major}.{minor}_2.13` from `python/repark-parity/pyproject.toml`'s `pyspark==X.Y.Z` record-extra pin via `_pinned_pyspark_version` + `_spark_major_minor` (CP-8). Restated `"4.1_2.13"` assertion removed. |
| Dead `spark_needs_cow_props` | Field removed from `MergeDiffRow`. Lifecycle helpers still take `with_cow_props` (repark callers pass `True`; Spark record path hard-codes `False`). |
| Re-derive recipe wording | Module docstring + record driver + §1.3 below quote the full parity-live sync line. |

### 1.3 Re-derive block (full parity-live sync + record driver)

```bash
# Full parity-live sync line (load-bearing flags; dual-wired Makefile ↔ parity-live.yml)
uv sync --locked --extra record \
  --extra numpy --extra pandas --extra polars --extra ml-ext \
  --no-install-package repark

JAVA_HOME=/usr/lib/jvm/zulu-17-amd64 SPARK_LOCAL_IP=127.0.0.1 \
  PYTHONPATH=python/repark-parity/src \
  .venv/bin/python python/repark/tests/_record_merge_differential_goldens.py
```

---

## 2. DEFERRED (items 2 + 3) — second PR after morning approval

| Item | What | Why deferred | Gate for second PR |
|---|---|---|---|
| **2 — 2 live-tier MERGE scenarios + `_live_parity` lifecycle abstraction** | create→seed→MERGE→read path on the live oracle tier; abstraction shape for multi-statement table lifecycle | Design surface — note written (`W2-LIFECYCLE-DESIGN.md`); **HALT for orchestrator /grok-qa** before any code. Never invent lifecycle overnight. | Owner approves design; then implement in `_live_parity.py` (+ tests) only after approval |
| **3 — Live-scenario conversion (G1 class)** | 13 converged timezone rows → live-tier drift scenarios; registry size pin update is code-side (A5) | Depends on item 2's lifecycle / scenario surface | Same second PR (or a follow-on if design splits them) |

**Hard ban remains for this PR:** no `.github/` edits (parity-live.yml only after morning
approval); no `docs/spark-sql-iceberg-parity.md`; no engine MERGE production changes; no
unit-queue / STATUS edits.

The design note names the abstraction shape, `_live_parity.py` changes, `parity-live.yml`
changes, and dual-wire impact — **design only, zero implementation**.

---

## 3. Decisions

**D-N2b-1 — Four pins, not rewrites of pre-existing ones.** Pre-G3 pins
(`merge_cardinality_violation_errors`, `merge_clause_order_first_match_wins`) stay; the new
four are named after the Python differential rows and pin the G3-budget shapes (insert-only
dups, UPDATE-then-DELETE order, threshold multi-arm) that those earlier pins do not cover.

**D-N2b-2 — Score-arm pins use a leaf-private `score_table_rows` helper.** Do not grow
`common.rs` for a two-call-site helper. Int32 throughout to match the leaf's existing
`register_source` / `table_rows` int surface.

**D-N2b-3 — GAV Spark-minor is derived, Iceberg runtime version is still a pin.** CP-8
attacks the tautology of restating `4.1` next to a constant that already contains `4.1`. The
Iceberg artifact version (`1.11.0`) and Scala binary (`2.13`) remain explicit pins — they are
not encoded in the pyspark version string.

**D-N2b-4 — Partial ship is the charter, not a shortcut.** A1: items 1+4 now; 2+3 second PR
post-approval. This ledger never claims N-2b / G3 full budget closed.

---

## 4. Gate evidence

### 4.1 Rust MERGE pins

```
cargo test -p repark-spark --lib 'tests::merge::'
running 23 tests
… merge_duplicate_source_keys_with_matched_raises ... ok
… merge_duplicate_source_keys_insert_only_commits_both ... ok
… merge_matched_and_arm_order_update_then_delete ... ok
… merge_matched_and_threshold_update_or_delete ... ok
… (19 pre-existing merge pins)
test result: ok. 23 passed; 0 failed; 0 ignored; 0 measured; 336 filtered out
EXIT 0
```

### 4.2 Facade differential (JVM-free)

```
pytest python/repark/tests/test_merge_differential_parity.py -q
.............                                                            [100%]
13 passed in 3.48s
EXIT 0
```

### 4.3 `make ci`

```
make ci → EXIT 0
  rust-fmt-check / rust-clippy / rust-panic-ban clean
  crate-dag / lib-rs / rust-file-size / lib-py / manifest clean
  parity-live dual-wire: OK
  cargo check --locked --workspace clean
  ruff check + format --check clean
  uv lock --locked / taplo / typos clean
```

---

## 5. Provocations (item 1 / item 4)

### P1 — GAV pin tracks the pyspark pin (CP-8 tooth)

`_pinned_pyspark_version()` reads `python/repark-parity/pyproject.toml`. A hand-edited GAV
that still says `4.1_2.13` while the pyproject pin moved to e.g. `4.2.x` would fail
`test_iceberg_gav_pin_is_exact_spark_minor` because the expected token is derived, not
restated. (No overnight pin-bump; the tooth is structural.)

### P2 — remove `spark_needs_cow_props` residual

```
rg spark_needs_cow_props python/repark/tests/
# expected: no matches after the NIT
```

---

## 6. Ready-to-paste registry rows

None from this partial unit. The NMBS refuse disclosure remains the only G3 registry candidate
(already noted in the archived N-2 ledger §6 as REG-G3-1). Items 2+3 may produce live-tier
registry text when they land; orchestrator owns `docs/spark-sql-iceberg-parity.md`.

---

## 7. Octo / critic

| Stage | Label | Detail |
|---|---|---|
| procedural ACC-style | **ACC-CONVERGED** | C1 quality/bugs + C2 security/safety + C4 claims — CLEAN |
| sepmo-octo cycle 1 | **CLEAN** ≥ S1 | Half-A C1+C2+C3+C4 quad; claims_critic=true; early_stop eligible |
| sepmo-octo | **OCTO-CONVERGED** | cycles=2 requested, early_stop after CLEAN cycle 1 |
| overload | **not run** | A2: no wave-global overload overnight |

### Critic-4 (claims) null-report

| Class | Inventory | Verdict |
|---|---|---|
| CL-MANDATE | items 1+4 claimed done; 2+3 claimed deferred | tree has 4 pin fns + NIT diffs; no item-2/3 code; design note outside repo only |
| CL-QUANT | "4 Rust pins", "23 passed", "13 passed" | re-ran: 4 new pins green; full `tests::merge::` was 23; differential 13 |
| CL-STALE | ledger "does NOT claim N-2b closed" | holds; §2 DEFERRED present; W2-COMPLETE PARTIAL |
| CL-RATIONALE | G-4 file ban as prior deferral reason | historical (archived n2 ledger); G-4 merged; pins now land |
| CL-TRANSCRIPT | make ci EXIT 0; dual-wire OK | re-ran dual-wire OK; ci log EXIT 0 |
| CL-COUNT | 4 pins named in map + ledger + code | three homes agree on the four names |
| CL-DUALHOME | archived n2 ledger still says "0 Rust pins deferred" | **history** under docs/history — not a live claim |
| CL-VACUOUS | GAV pin derives from pyproject | re-ran `test_iceberg_gav_pin_is_exact_spark_minor` PASS; field `spark_needs_cow_props` absent |
| CL-GHOST | design note path `planning/grok/W2-LIFECYCLE-DESIGN.md` | exists on planning tree; not a repo path (A11) |

OPEN ≥ S1: **none**. Residual notes (below floor): score-arm pins use Int32 (leaf surface) vs Python BIGINT — value semantics match; type pin is the downcast.

---

## 8. Explicit non-claims

- N-2b is **not closed**.
- G3 full budget (4 Rust + 2 live + record-side) is **not fully delivered** — live half open.
- No lifecycle abstraction code lands in this PR.
- No `.github/workflows/parity-live.yml` edit in this PR.
- No STATUS / unit-queue / registry file edits.
