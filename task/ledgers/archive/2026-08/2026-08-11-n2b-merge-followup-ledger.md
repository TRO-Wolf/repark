# Unit ledger — N-2b / W-2: MERGE follow-up

**Unit:** N-2b (H-2 gap G3 follow-up) · **Date:** 2026-08-11 · **Lane:** W-2 ·
**Branches:** `grok/w2-n2b-merge-followup` (items 1+4, PR #50) ·
`grok/w2-n2b-lifecycle-live` (items 2+3, this PR) · **Executor:** Grok (grok-4.5)

**Full N-2b is closed only when both PRs land.** Items 1+4 = PR #50; items 2+3 = this PR
(stacked on #50). Design note: `planning/grok/W2-LIFECYCLE-DESIGN.md` (orchestrator planning
tree, outside this repo).

Charter: `planning/grok/BRIEF-w2-n2b-merge-followup.md` + overnight addendum A1 + approved
lifecycle design (option A).

---

## 1. What landed (this PR)

| Item | Artifact | Role |
|---|---|---|
| **1 — 4 Rust MERGE pins** | [`crates/repark-spark/src/tests/merge.rs`](../../../../crates/repark-spark/src/tests/merge.rs) | G3 pins deferred by G-4's file ban |
| map lockstep | [`crates/repark-spark/src/tests/map.md`](../../../../crates/repark-spark/src/tests/map.md) | documents the four new pin names |
| **4 — NIT: GAV pin CP-8** | [`python/repark/tests/test_merge_differential_parity.py`](../../../../python/repark/tests/test_merge_differential_parity.py) | Spark-minor derived from pinned pyspark |
| **4 — NIT: dead knob** | same + `_record_merge_differential_goldens.py` | `spark_needs_cow_props` removed |
| **4 — NIT: re-derive recipe** | module docstring + record driver + this ledger | full parity-live sync line quoted |
| map lockstep | [`python/repark/tests/map.md`](../../../../python/repark/tests/map.md) | N-2b status + re-derive wording |
| this ledger | `task/n2b-merge-followup-ledger.md` | linked from [`task/map.md`](../../../map.md) |

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

## 2. SHIPPED — items 2 + 3 (second PR: `grok/w2-n2b-lifecycle-live`)

| Item | Artifact | Role |
|---|---|---|
| **2 — `_oracle_pins.py`** | [`python/repark/tests/_oracle_pins.py`](../../../../python/repark/tests/_oracle_pins.py) | ONE importable home for GAV + pyspark-version helpers |
| **2 — LifecycleScenario** | [`python/repark/tests/_live_parity.py`](../../../../python/repark/tests/_live_parity.py) | multi-statement create→seed→act→read; no `error_needle` |
| **2 — build_spark_iceberg_engine** | same | option A: sibling of `build_spark_engine`; GAV from `_oracle_pins` |
| **2 — 2 MERGE live rows** | `LIFECYCLE_SCENARIOS` (+ spark twin list) | budget pin == 2 |
| **2 — lifecycle tests** | [`python/repark/tests/test_parity_live.py`](../../../../python/repark/tests/test_parity_live.py) | repark routine + live triple via `spark_iceberg_engine` fixture |
| **2 — record driver GAV import** | [`_record_merge_differential_goldens.py`](../../../../python/repark/tests/_record_merge_differential_goldens.py) | GAV from `_oracle_pins` only (never from test module) |
| **3 — 13 tz live Scenarios** | `_live_parity.SCENARIOS` | extraction-class equality rows with `session_conf` |
| **3 — size pin** | `test_registry_covers_the_mandated_golden_family` | **29 → 42** (deliberate, same diff) |
| map lockstep | [`python/repark/tests/map.md`](../../../../python/repark/tests/map.md) + [`task/map.md`](../../../map.md) | new files + status |
| this ledger | §2 | items 2+3 SHIPPED |

### 2.1 Chosen MERGE live rows + reasons

| Live name | Differential source | Why chosen | Why not alternatives |
|---|---|---|---|
| `live_merge_basic_upsert` | `basic_upsert_update_and_insert` | **Control equality** — publish-job upsert shape (UPDATE * + INSERT *); anchors the live tier against the well-known happy path | — |
| `live_merge_matched_arm_order` | `matched_and_arm_order_update_then_delete` | **Arm-order first-match-wins** — SQL `WHEN MATCHED AND … UPDATE` then unconditional `DELETE`; detects arm-order drift the builder path does not cover | Not insert-only (builder already has insert shapes; less drift signal); not null-keys (three-valued logic, lower live-drift priority vs arm order); not threshold multi-arm (sibling of arm-order; one arm-order row is enough for the 2-row budget); not cardinality error (no `error_needle` / `run_lifecycle_expect_error` this PR) |

Neither row duplicates builder-path live coverage (`test_merge_into.py` DataFrame API). Both are
SQL lifecycle shapes that add live Spark+Iceberg drift detection on the facade `sql()` door.

### 2.2 Item 3 — the 13 extraction-class timezone live rows

From `test_session_timezone_parity.test_the_extraction_class_converged` (17 equality = 13
extraction + 2 composition + 2 zone-independent controls). Converted the **13 pure extraction
equalities** only — NOT composition (type disclosure residue TZ-4), NOT controls (already
represented by the H-1a DATE scenarios), NOT disclosures (TZ-4/5/6/7).

| # | Live scenario name | Zone |
|---|---|---|
| 1 | `tz_live_year_of_instant_under_new_york_session` | America/New_York |
| 2 | `tz_live_month_of_instant_under_new_york_session` | America/New_York |
| 3 | `tz_live_day_of_instant_under_new_york_session` | America/New_York |
| 4 | `tz_live_hour_of_instant_under_new_york_session` | America/New_York |
| 5 | `tz_live_hour_of_instant_under_tokyo_session` | Asia/Tokyo |
| 6 | `tz_live_year_month_day_of_instant_under_tokyo_session` | Asia/Tokyo |
| 7 | `tz_live_dst_spring_forward_instant_hour` | America/New_York |
| 8 | `tz_live_dst_fall_back_repeated_local_hour` | America/New_York |
| 9 | `tz_live_column_extract_under_new_york_session` | America/New_York |
| 10 | `tz_live_column_extract_under_tokyo_session` | Asia/Tokyo |
| 11 | `tz_live_pre_1970_extract_under_new_york_session` | America/New_York |
| 12 | `tz_live_year_boundary_extract_and_format_under_new_york_session` | America/New_York |
| 13 | `tz_live_leap_day_extract_under_new_york_session` | America/New_York |

Column-path rows use `register_tz_column_view` (same two instants as the differential corpus).

### 2.3 Dual-wire / workflow

**Zero** Makefile / `parity-live.yml` flag changes. Iceberg jar by Maven coordinates only
(`spark.jars.packages`); no new Python extra. `make check-parity-live-dual-wire` green is the
required gate.

### 2.4 Hard bans honored

- no `docs/spark-sql-iceberg-parity.md` edit (registry FILE orchestrator-owned; §6 paste below)
- no `Cargo.lock` / `uv.lock`
- no unit-queue / STATUS
- no engine MERGE production code
- no `error_needle` / `run_lifecycle_expect_error` this PR

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

**D-N2b-4 — Partial ship is the charter, not a shortcut.** A1: items 1+4 first PR; 2+3 second
PR post-approval. Full N-2b closed only when both land.

**D-N2b-5 — LifecycleScenario + separate build_spark_iceberg_engine (option A).** Keeps the
default live Spark session Iceberg-free; only lifecycle tests request the provisioned engine.
Approved design.

**D-N2b-6 — GAV one home in `_oracle_pins`.** Record driver and live tier never import GAV from
a `test_` module. Differential re-exports for its GAV pin test.

**D-N2b-7 — Same PR for items 2+3.** Combined diff reviewable; no split.

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

### 4.3 `make ci` (items 1+4 PR)

```
make ci → EXIT 0
  rust-fmt-check / rust-clippy / rust-panic-ban clean
  crate-dag / lib-rs / rust-file-size / lib-py / manifest clean
  parity-live dual-wire: OK
  cargo check --locked --workspace clean
  ruff check + format --check clean
  uv lock --locked / taplo / typos clean
```

### 4.4 Items 2+3 (lifecycle + tz live) — JVM-free + dual-wire

```
make check-parity-live-dual-wire → EXIT 0
  parity-live dual-wire: OK (maturin@1.14.1, extras=[…], uv-run=[--locked, --no-sync])

pytest test_parity_live.py test_merge_differential_parity.py -q
  63 passed, 48 skipped  (live legs skip without REPARK_PARITY_LIVE=1)
  EXIT 0
  # includes: 42 SCENARIOS repark + 2 LIFECYCLE repark + budget pins + 13 merge differential
```

### 4.5 `make ci` (items 2+3 PR)

```
make ci → EXIT 0
  rust-fmt-check / rust-clippy / rust-panic-ban clean
  crate-dag / lib-rs / rust-file-size / lib-py / manifest clean
  parity-live dual-wire: OK
  cargo check --locked --workspace clean
  ruff check + format --check clean
  uv lock --locked / taplo / typos clean
```

### 4.6 Critic (items 2+3)

| Stage | Label | Detail |
|---|---|---|
| ACC-style C1+C2+C4 | **ACC-CONVERGED** | quality/bugs + safety + claims CLEAN |
| claims inventory | **CLEAN** | see §7.2 |
| octo cycles=2 early_stop | **OCTO-CONVERGED** | single-session Half-A; no OPEN ≥ S1 |
| overload | **not run** | A2 |

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

## 6. Ready-to-paste registry rows (orchestrator-owned file)

Lane does **not** edit `docs/spark-sql-iceberg-parity.md`. Paste candidates for the
orchestrator when both N-2b PRs are merged:

### REG-N2b-LIVE-1 — live-tier MERGE lifecycle surface

```
### Live-tier MERGE lifecycle (N-2b item 2 / G3 live half)

- Surface: `_live_parity.LifecycleScenario` + `LIFECYCLE_SCENARIOS` (budget 2)
- Rows: `live_merge_basic_upsert` (control upsert), `live_merge_matched_arm_order`
  (first-match-wins UPDATE-then-DELETE)
- Engine path: repark memory catalog + COW; Spark via `build_spark_iceberg_engine`
  (GAV `ICEBERG_SPARK_RUNTIME_GAV` from `_oracle_pins`)
- Tests: `test_lifecycle_scenario_matches_golden_on_repark` (JVM-free) +
  `test_live_lifecycle_scenario_matches_repark_golden_and_spark` (REPARK_PARITY_LIVE=1)
- Does not replace the 10-row record-side differential (`test_merge_differential_parity.py`)
```

### REG-N2b-LIVE-2 — G1 extraction-class timezone live conversion

```
### G1 / G16 extraction-class live scenarios (N-2b item 3)

- 13 equality rows converted into `_live_parity.SCENARIOS` with `session_conf` zone override
- Size pin: 29 → 42 (code-side only; this registry file is orchestrator-owned)
- Prefix: `tz_live_*`; column-path rows register `tz_aware_instants` via `register_tz_column_view`
- NOT converted: composition date_trunc type disclosures (TZ-4), zone-independent DATE controls
  (already live as H-1a rows), TZ-5/6/7 disclosures
```

The NMBS refuse disclosure remains the only G3 *divergence* registry candidate (archived N-2
ledger §6 as REG-G3-1).

---

## 7. Octo / critic

### 7.1 Items 1+4 (PR #50)

| Stage | Label | Detail |
|---|---|---|
| procedural ACC-style | **ACC-CONVERGED** | C1 quality/bugs + C2 security/safety + C4 claims — CLEAN |
| sepmo-octo cycle 1 | **CLEAN** ≥ S1 | Half-A C1+C2+C3+C4 quad; claims_critic=true; early_stop eligible |
| sepmo-octo | **OCTO-CONVERGED** | cycles=2 requested, early_stop after CLEAN cycle 1 |
| overload | **not run** | A2: no wave-global overload overnight |

### 7.2 Items 2+3 (this PR)

| Stage | Label | Detail |
|---|---|---|
| procedural ACC-style | **ACC-CONVERGED** | C1 quality/bugs + C2 security/safety + C4 claims — CLEAN |
| claims inventory | **CLEAN** | see null-report below |
| sepmo-octo | **OCTO-CONVERGED** | cycles=2 early_stop after CLEAN cycle 1; claims_critic |
| overload | **not run** | A2 |

#### Critic-4 (claims) null-report — items 2+3

| Class | Inventory | Verdict |
|---|---|---|
| CL-MANDATE | items 2+3 claimed SHIPPED | `_oracle_pins`, `LifecycleScenario`, 2 MERGE live rows, 13 `tz_live_*`, size pin 42, tests present |
| CL-QUANT | "2 lifecycle", "13 tz", "42 scenarios", "63 passed" | LIFECYCLE==2; tz_live count 13; SCENARIOS==42; pytest 63p/48s |
| CL-STALE | "full N-2b only when both PRs land" | holds; §8 explicit; W2B-COMPLETE does not claim solo close |
| CL-TRANSCRIPT | dual-wire EXIT 0; make ci EXIT 0 | re-ran both green |
| CL-COUNT | 2 MERGE names + 13 tz names in ledger/map/code | three homes agree |
| CL-VACUOUS | no error_needle; dual-wire-neutral | zero Makefile/yml flag deltas; no error_needle field |
| CL-GHOST | GAV only from `_oracle_pins` in record driver | grepped: record driver imports `_oracle_pins`; test re-exports only |
| CL-DUALHOME | size pin 29→42 documented deliberate | docstring + ledger + map agree |

OPEN ≥ S1: **none**.

---

## 8. Explicit non-claims / status

- **Full N-2b closed only when both PRs land** (#50 items 1+4 + this PR items 2+3).
- No `.github/workflows/parity-live.yml` edit (dual-wire-neutral).
- No STATUS / unit-queue / registry FILE edits.
- No engine MERGE production changes.
- No `error_needle` / `run_lifecycle_expect_error` (deferred to a consumer).

## Landing note (L-1, 2026-08-12)

REG-N2b-LIVE-1 / REG-N2b-LIVE-2 classified **ALREADY-LANDED**: `LIFECYCLE_SCENARIOS` is 2 and
`SCENARIOS` is 42 on merged `main`. No new registry divergence rows (these were coverage notes,
not disposed differences).
