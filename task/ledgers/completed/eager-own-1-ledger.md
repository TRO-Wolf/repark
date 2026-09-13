# Unit ledger — EAGER-OWN-1 · eager results own their materialization

**Date:** 2026-09-13 · **Branch:** `fix/eager-own-1` · **Base:** `8936346a`
**Model:** swe-2-high · **Policy:** [../../../AGENTS.md](../../../AGENTS.md).
**Path:** STANDARD. **risk_tier: standard.**

**Retires:** this ledger moves to `../completed/` when the unit's last commit lands.

**Why now.** The owner's
[eager materialization retention review](../../../docs/history/eager-own-1/eager-materialization-retention-review-2026-09-13.md)
confirms in source that a bare `temp_df.eager()` (result not assigned) registers a
`__repark_cache_*` MemTable that nothing owns: the frame registry is a `WeakSet`,
`unpersist()` on the source does not know the dropped child, and only
`catalog.clearCache()` sweeps orphans by prefix. Each repeat adds a full result set
to resident memory. The review measured nothing, so step 0 measures first (D-7):
a deterministic 1,000,000-row TA `withColumns` fixture, ten bare `eager()` calls,
per-iteration wall / VmRSS / VmHWM / registration count, in a subprocess on the
release native. No product code changes in step 0.

**Not in this step:** the session cache budget (D-6 — separate card
EAGER-BUDGET-1), the docs/guide paragraph and the review's move to
`docs/history/` (step 2). `STATUS.md`, `briefs/next-sequence.md`, `.github/`,
`Cargo.toml`, `Cargo.lock`, `pyproject.toml`, `uv.lock` are untouched.

**Step 1 (this step).** The D-2/D-3 handle landed as
`repark/spark/dataframe/cache_handle.py` — `CacheViewHandle` owns one
`__repark_cache_*` registration; frames carry an immutable `_handles` tuple;
`_materialize_cache_if_needed` (cache/persist and the checkpoint path) makes
the registering frame the owner; `unpersist()`/`clearCache()` release
explicitly; the module-level `weakref.finalize` callback drops the view when
the last holder dies and returns immediately on a stopped session. `eager()`
on a live cache-backed frame returns a sharing wrapper (D-4) — no collection,
no new registration. `core.py` shrank 4117 → 4094 (the cosmetic-warning helper
moved with the handle module); both CAP-1 baselines ratcheted down to the exact
count.

## PROPOSITION LEDGER — EAGER-OWN-1 — 2026-09-13

| Clause | Proposition (checkable) | Proof obligation | Verdict | Evidence / open question |
|---|---|---|---|---|
| C-001 | The base-tree defect is measured, not asserted: ten bare `temp_df.eager()` calls (results not assigned) on the deterministic 1e6-row, 25-column TA `withColumns` fixture leave ten `__repark_cache_*` registrations after the loop and after `gc.collect()`, with per-iteration wall / VmRSS / VmHWM recorded on the release native. | `python/repark-parity/tests/eager_own/` bench pin under `REPARK_EAGER_OWN_BENCH=1`; committed `docs/perf/eager-own-1-2026-09-13/base.json`. | **PROVEN** | Base `8936346a`, release native (`__debug_assertions__` False), Python 3.12.3, `nproc` 64, `free -g` total 125 GiB, under `systemd-run --user --scope -p MemoryMax=24G -p MemorySwapMax=0`. Per-iteration (s / VmRSS / VmRSS-gc / VmHWM / regs): 0: 0.827 / 546 MB / 546 MB / 1,012 MB / 1 · 1: 0.814 / 1,253 / 1,253 / 1,253 / 2 · 2: 0.804 / 1,196 / 1,196 / 1,492 / 3 · 3: 0.844 / 1,630 / 1,630 / 1,630 / 4 · 4: 0.805 / 1,803 / 1,803 / 1,858 / 5 · 5: 0.809 / 1,877 / 1,877 / 2,262 / 6 · 6: 0.815 / 2,052 / 2,052 / 2,354 / 7 · 7: 0.856 / 2,667 / 2,667 / 2,667 / 8 · 8: 0.810 / 2,457 / 2,457 / 2,945 / 9 · 9: 0.824 / 2,967 / 2,967 / 3,041 / 10. Post-loop regs 10, post-gc regs 10, post-clearCache regs 0; RSS stays 2,967 MB after clearCache (allocator retention, as the review predicted). Wall is FLAT (0.80–0.86 s) at this size on this box — the retention and RSS growth are measured; the reported per-iteration slowdown is not reproduced at 10 iterations under no paging pressure and is honestly recorded as such. JSON: [docs/perf/eager-own-1-2026-09-13/base.json](../../../docs/perf/eager-own-1-2026-09-13/base.json). pins: eager-own-1/C-001 |
| C-002 | Repeated eager calls on the original lazy plan, discarding results: abandoned eager registrations do not accumulate after their owners are released (review validation row 1). | `test_repeated_bare_eager_releases_every_registration` — ten bare `eager()` calls, `gc.collect()`, no `clearCache()`. | **PROVEN** | RED on base copy (`PYTHONPATH=/tmp/eager-own-base`, product files from `8936346a`): `assert ['__repark_ca…48ac582', …] == []` — `Left contains 10 more items`. GREEN on the fix: each unassigned child is collected inside the loop, its handle's finalizer drops the view, post-loop + post-gc registrations = `[]`. |
| C-003 | Reuse one eager frame across actions: no new materialization; values and Arrow types remain unchanged (review validation row 2). | `test_one_eager_frame_across_actions_registers_once` — `collect`, `count`, `filter().count`, `select().count`, `to_arrow` on one eager frame. | **PROVEN** | Guard pin — green on base AND on the fix (one registration, stable values and Arrow schema `id: int64 / label: string`); it pins the invariant that must not change, so no red exists for this row. Mutation-checked in the review round: `mut-reuse-guard` (skipping the `_cache_view` early-return) reds it (`assert 2 == 1`). |
| C-004 | Eager called on an eager frame: backing reuse follows the D-4 identity contract — a new wrapper sharing the handle and shape, no collection, no new registration; a frame whose view was explicitly dropped materializes afresh. | `test_eager_on_eager_reuses_the_backing` — `c = b.eager()` shares view, handle object and `_eager_shape`; del+gc of either wrapper leaves the other answering; `b.unpersist(); b.eager()` makes a fresh view. | **PROVEN** | RED on base: `assert '__repark_cac…ef745aa059f09' == '__repark_cac…92d03d2593520'` — base materialized a SECOND view for eager-on-eager (the orchestrator's measured fact, now pinned). GREEN on the fix: `_eager_materialize` fast-path returns a `_spawn_preserving_identity` wrapper carrying the same `_handles` and shape; the count action is skipped because `_eager_shape` is already known. |
| C-005 | Parent released while a `.lazy()` copy, a derived frame, a join or a union survives: remaining users still produce correct values and Arrow types (review validation row 4; D-2 handle union across join/union parents). | `test_survivors_keep_the_registration_until_the_last_dies` — four survivor shapes built, the source eager frame deleted + gc, each survivor's values/`to_arrow` schema compared, the registration asserted alive until the last survivor dies then `[]`. Review round adds `test_every_derived_holder_kind_keeps_the_registration` — parametrized over the remaining D-2 kinds (withColumn, withColumns, limit, offset, orderBy, sort, groupBy.agg, distinct, drop, sample, intersect, subtract, crossJoin, mapInArrow), each asserting the owner's handle in `child._handles`, the name still listed after `del owner + gc`, unchanged values + Arrow schema, then `[]` after `del child + gc`. | **PROVEN** | RED on base: `assert '__repark_cache_20d3…' not in ['__repark_cache_20d3…']` — after the last survivor died the orphaned registration still listed (nothing owned it). GREEN on the fix: each survivor's `_handles` unions the parents' (`_spawn(inner, *others)`), the registration dies only when the last survivor is collected. Review round red (L-001): under a scratch package copy with `_spawn`'s handle copy stripped (`child._handles = ()`), all 14 arms red — one line per arm: `test_every_derived_holder_kind_keeps_the_registration[withColumn|withColumns|limit|offset|orderBy|sort|groupBy.agg|distinct|drop|sample|intersect|subtract|crossJoin|mapInArrow]` each failing `assert <CacheViewHandle …> in ()` at the `_handles` membership check. |
| C-006 | Explicit `unpersist()`, `clearCache()`, and session stop each stay documented-correct and idempotent under the handle; the finalizer never touches a stopped session (review validation row 5; D-2). | `test_unpersist_and_clear_cache_are_idempotent` (guard), `test_wrapper_unpersist_releases_only_its_own_hold` (R11-D-1 wrapper arm), `test_finalizer_never_calls_a_stopped_session` (the `_DropViewSpy` on `session.drop_temp_view`). | **PROVEN** | RED on base: `test_finalizer_never_calls_a_stopped_session` — `assert [] == ['"datafusion"."public"."__repark_cache_…']` — on base the view outlived its frame so the expected explicit-drop record never appeared. GREEN on the fix: after `spark.stop()` + del + `gc.collect()` the spy records zero calls (the finalizer returns on `alive_token["alive"] is False`); owner `unpersist()` twice and `clearCache()` twice are no-ops on a released handle; wrapper `unpersist()` clears only its own view ref/shape and restores its lineage — the registration stays live for the other holder. Review round (L-002): the same test now asserts `handle._finalizer.atexit is False` on a live eager handle — RED under the `atexit = True` mutation (`assert True is False`). Review round (L-005): `test_release_twice_drops_once` — a `_DropViewSpy` on the released handle's session records zero `drop_temp_view` calls from a second `release()`; RED under the `if self._released: return` removal (`assert ['"datafusion"…cache_…'] == []`). |
| C-007 | A collection or post-registration failure leaves no unintended cache registration (review validation row 6). | `test_post_registration_failure_leaves_no_registration` — `repark.cache.max_bytes` refusal, plus a post-registration `.sql` override that raises. | **PROVEN** | RED on base: `assert ['__repark_ca…e31060f8a21c'] == []` — the refused/abandoned frame's registration leaked. GREEN on the fix: both arms leave `_cache_view_names == []`; the failed frame's handle is the only holder, so dropping it runs the finalizer; `_DropViewSpy` also confirms no call into the session for a `max_bytes` refusal (the guard runs before registration). |
| C-008 | Several simultaneous eager results remain independently valid; cleanup matches the ownership contract (review validation row 7). | `test_simultaneous_eager_results_are_independent` — two `eager()` frames from one lazy source hold distinct views; releasing one leaves the other answering. | **PROVEN** | RED on base: `assert ['__repark_ca…c86a7fe7e95e'] == ['__repark_ca…cd523b940df9']` — releasing the first left its registration behind. GREEN on the fix: distinct views and distinct handles; del+gc of one drops only its registration. |
| C-009 | Original source changes between eager calls are observed: fresh lazy-plan evaluation still sees the new source (D-5 — no plan-equivalence caching) and existing snapshots keep their semantics (review validation row 8). | `test_source_change_between_eager_calls_is_observed` — a CSV source rewritten between two `eager()` calls; first snapshot keeps old rows, second sees new. | **PROVEN** | RED on base: `assert ['__repark_ca…ae0533cc309b'] == []` — both abandoned registrations leaked after the frames died. GREEN on the fix: first snapshot `id=1,2`, second `id=10,20`, and both registrations are gone after the frames are collected — D-5 holds (each `eager()` evaluates the lazy plan afresh). |
| C-010 | Exported Arrow objects (`toArrow`, `toPandas`, `to_polars`, `collect`) stay valid after the registration is gone — exports share the MemTable buffers by refcount and need no handle (D-2). | `test_exports_outlive_the_registration` — `to_arrow`, `to_pandas`, `to_polars`, `collect` taken, eager frame deleted + gc, every export re-read unchanged; `test_cached_frame_registration_dies_with_last_holder` — D-3 same mechanism for `cache()`. | **PROVEN** | RED on base: `assert '__repark_cache_4008…' not in ['__repark_cache_4008…']` (registration survived the frame) and the D-3 arm red the same way. GREEN on the fix: registration list is `[]` after the frame dies while all four exports still return `id=[1,2]`; a `cache()`d frame's view dies with its last holder (D-3: ownership moved, explicit-drop policy unchanged). |
| C-011 | The `test_cache_persist.py` orphan assertion is flipped: after the handle, a GC'd cached frame's `__repark_cache_*` view no longer survives as an orphan — the pin asserts the inverse of today's "table SURVIVES the frame" row, and `clearCache()` remains the explicit sweep. | `test_clear_cache_drops_orphan_cache_views` inverted + a hand-registered `__repark_cache_*` view keeps the prefix-sweep pin; step-0 small pin flipped to `post_gc == 0`. | **PROVEN** | RED on base: `test_clear_cache_drops_orphan_cache_views` fails its new assertion (the GC'd frame's view is still listed on base — the behaviour being fixed). GREEN on the fix: `del f; gc.collect()` removes the view; a `session.register_temp_view("__repark_cache_hand_made", …)` orphan still survives GC and is swept only by `clearCache()` — the safety net is pinned independently. Step-0 small pin (`test_bare_eager_registrations_do_not_accumulate_small`) now asserts `registrations_after_call == 0` and post-loop/post-gc/post-clearCache all zero — red on base, green here. |
| C-012 | The step-0 harness re-run after the fix: registration count 0 after the loop + `gc.collect()`, RSS plateaus, per-iteration time flat — the before/after tables pasted beside C-001's. | `REPARK_EAGER_OWN_BENCH=1` re-run on the step-1 tree; `after.json` beside `base.json`; spawn-cost comparison on the `chain` facade cells vs a `git show 8936346a:` base copy. | **PROVEN** | Same scope (`MemoryMax=24G`, box idle first, release native). Per-iteration (s / VmRSS / regs): 0: 0.802 / 540 MB / 0 · 1: 0.818 / 1,041 / 0 · 2: 0.916 / 1,096 / 0 · 3: 0.833 / 1,109 / 0 · 4: 0.869 / 917 / 0 · 5: 0.820 / 858 / 0 · 6: 0.797 / 959 / 0 · 7: 0.864 / 1,210 / 0 · 8: 0.838 / 1,059 / 0 · 9: 0.829 / 1,199 / 0. Post-loop 0, post-gc 0, post-clearCache 0 (the pin asserts the zero shape). RSS plateaus ~0.85–1.2 GB vs climbing to 2,967 MB; VmHWM 1,257 MB vs 3,041 MB; wall flat 0.80–0.92 s. JSON: [after.json](../../../docs/perf/eager-own-1-2026-09-13/after.json). Spawn cost (`chain` cells, warmup + 5 interleaved reps, medians): `chain/10/build_only` 1.862 → 1.823 ms (−2.1 %), `chain/50` 40.36 → 40.21 ms (−0.4 %), `chain/100` 343.5 → 340.3 ms (−0.9 %), `chain_collapsed/100` 66.86 → 66.83 ms (−0.05 %), `chain_old/100` 3,708.9 → 3,703.4 ms (−0.2 %) — every delta inside run-to-run noise; the handle adds no measurable per-spawn cost. Full table in Evidence. |

## Decisions

Orchestrator rulings R11-D-1…R11-D-5 from the step-1 and step-2 briefs, and how the tree
honours them:

| ID | Ruling | Honoured by |
|---|---|---|
| R11-D-1 | The frame whose `_materialize_cache_if_needed` registers the view is the handle's owner; `unpersist()` on the owner and `clearCache()` drop the registration and mark the handle released; `unpersist()` on a D-4 wrapper releases only that wrapper's hold (clears its view ref + shape, restores its lineage); plans still holding a resolved provider keep answering after the name is dropped. | `bind_registered_view` sets `_cache_view_owned_handle` only on the registering frame; `cache_handle.release_view_hold` drops via `CacheViewHandle.release()` only when that owner link matches, and otherwise just removes the hold from the frame's `_handles`; `core.py::unpersist` then clears `_cache_view`/`_eager_shape` and restores `_lineage_inner` the same for both; pins `test_wrapper_unpersist_releases_only_its_own_hold`, `test_unpersist_and_clear_cache_are_idempotent`, and the survivor answers in C-005. |
| R11-D-2 | Frames carry an immutable tuple of handles, shared empty tuple when none; `_spawn(inner, *others)` unions each other frame's handles only when it carries some; no per-spawn allocation on frames without handles; audit every `_spawn` and every direct `DataFrame(...)` that embeds another frame's plan. | `DataFrame.__slots__` grows `_handles` (+ `_cache_view_owned_handle` for the owner link); `_spawn` assigns `child._handles = self._handles` (shared tuple, no allocation) and calls `union_handles` only for an `other` whose `_handles` is non-empty; the audit table below lists every site and its verdict. |
| R11-D-3 | `weakref.finalize(handle, <module-level fn>, session, alive_token, view_name)` with `atexit=False`; callback references no handle/frame, returns when `alive_token["alive"]` is false, never raises; drop failures debug-logged and swallowed only inside the GC callback; explicit `unpersist`/`clearCache` errors still propagate; no nested defs, no lambdas, no dataclasses. | `cache_handle.py::_drop_cache_view_registration` is the module-level callback — signature `(session, alive_token, view_name)`, early return on `not alive_token.get("alive")`, `except Exception` around `session.drop_temp_view` logged through `_LOGGER.debug`; `CacheViewHandle.release()` calls `drop_temp_view` directly so explicit-path errors propagate; no nested def/lambda/dataclass anywhere in the module; the spy pin (C-006) proves zero calls on a stopped session. |
| R11-D-4 | `clearCache()` releases every live handle of the session (drop + mark released) before today's registry loop and prefix sweep, both of which stay. | `CacheViewHandle.__init__` self-registers into a per-session `WeakSet` under `alive_token["cache_view_handles"]`; `catalog.py::clear_cache` iterates that set calling `handle.release()` first, then runs the unchanged registry `unpersist` loop and the `_CACHE_VIEW_PREFIX` sweep; idempotent (`release()` no-ops once released); checkpoint views untouched (C-006, C-011 prefix-sweep pin). |
| R11-D-5 | `_warn_storage_level_cosmetic_once` moves verbatim from `core.py` to `cache_handle.py`, re-imported into `core.py` by identity, to keep `core.py` under its 4117-line ceiling without condensing unrelated code. | `cache_handle.py` carries the helper byte-identical; `core.py` does `from repark.spark.dataframe.cache_handle import _warn_storage_level_cosmetic_once` so both module surfaces keep the name (the frozen `EXPECTED_*` tables in `_dfcore_1_expected.py` pass unchanged); `core.py` ends at 4094 lines and both CAP-1 baselines were ratcheted down to the exact count; orchestrator-accepted in the step-2 brief. |

R11-D-2 audit — every `_spawn` call site and every `DataFrame(...)`
construction that embeds another frame's plan, with verdict:

| Site | Verdict |
|---|---|
| `core.py` join paths (`join`, `crossJoin`, `joinWith` family — the `session.sql(join_sql)`/`_spawn(joined, other)` sites ~2833/2847/2933) | already pass `other` to `_spawn`; no change needed — handles union through `*others`. |
| `core.py` union / unionByName / set-ops (`exceptAll`, `intersect`, `subtract`, `union` family ~3176–3277) | already pass `other`; union of both parents' handles flows. |
| `core.py::_identity_child` / `_spawn_preserving_identity` | carry `self`'s handles by construction (spawn from `self`); the D-4 eager-on-eager wrapper lands here and inherits the backing frame's handle. |
| `core.py` checkpoint path (`localCheckpoint`, eager checkpoint) | the checkpoint view is a `__repark_ckpt_*` `materialize_as_temp_view`, deliberately outside the handle model; the path calls `release_view_hold(self, old_cache_view)` which drops the registration only when the checkpointing frame owned it (a sharing holder just sheds its hold) — matching R11-D-1 owner-vs-holder. |
| `core.py::unpersist` | owner vs wrapper split per R11-D-1 (see row above). |
| `core.py::parent_for_stream` | constructs a parent frame over the same inner — audited: it passes `self`'s handles so the streaming parent keeps the view alive. |
| `joins_columns.py` `_agg_via_pandas_udfs` (~388–471) | the second frames are `udf_frame`/`builtin_frame`, both spawned from the same source `frame`, so their handles ⊆ `frame._handles`; every result (`frame._spawn(session.sql(...))` over the `__repark_mix_*` intermediates) carries the source's handles, which cover every cache view the embedded plans can scan. No change needed — verified by reading, and C-005's join survivor exercises the shape. |
| `udf_window_projection.py` (~178–219) | windowed UDF join embeds the input frame via temp views through `session.sql`; the result `_spawn`s from `self` and unions the UDF-side frame where applicable — audited, propagation correct. |
| `polars.py` joins (~211–242) | `_spawn(…, other)` — passes the second frame; handles union. |
| `session/session_core.py` `createDataFrame` / `sql` / `read` constructions | build plans from data/SQL, not from another frame's `_inner`; no embedded frame to propagate from. |
| `writer_readwriter.py`, `mapInArrow` nested-stream path | construct frames from IO/Arrow plans; no second frame's plan embedded without the frame being passed. |
| `ml/` estimators | consume frames via actions/fits; no `DataFrame(...)` wrapping a foreign `_inner`. |

## Review findings

Critic-logic round (Grok, read-only, on `e1587cbb`; report
`/tmp/grok-worker/e-critic/report-1.md`): 0 P1, 2 P2, 3 P3.

| id | Severity | Disposition |
|---|---|---|
| L-001 | P2 | FIXED — `test_every_derived_holder_kind_keeps_the_registration` pins every remaining D-2 holder kind (withColumn, withColumns, limit, offset, orderBy, sort, groupBy.agg, distinct, drop, sample, intersect, subtract, crossJoin, mapInArrow): owner's handle in `child._handles`, name listed after `del owner + gc`, unchanged values + Arrow schema, `[]` after the child dies. All 14 arms red under the `_spawn` strip mutation (see C-005 evidence). |
| L-002 | P2 | FIXED — `test_finalizer_never_calls_a_stopped_session` now asserts `handle._finalizer.atexit is False`; red under the `atexit = True` mutation. |
| L-003 | P3 | CLOSED-BY-L-001 — `_collapse_base` can mask handle stripping on `withColumn`; the parametrized pin asserts `_handles` membership directly, which reds regardless of the parent strong-ref. |
| L-004 | P3 | ACCEPTED-DOCUMENTED — `derived.eager()` keeps the parent's handle alongside its own new one; bounded, conservative retention until the rematerialized child dies. One sentence added to `dataframe/map.md` "cache-view ownership". |
| L-005 | P3 | FIXED — `test_release_twice_drops_once` (three lines): a `_DropViewSpy` on the released handle's session records zero `drop_temp_view` calls from a second `release()`; red under the `if self._released: return` removal. |
| P-001 | P3 | ACCEPTED-DOCUMENTED — Python perf reviewer (S2-21, Grok, read-only, on `e1587cbb`; report `/tmp/oc-worker/e-rev/report.md`): `cache_handle.py::union_handles` is O(k²) and always copies (list membership); 100 k `_spawn(inner, other)` with two one-handle frames +1.85 µs (+7.1 %). Typical k is 1–2 and no product path shows it. Reviewer verdict **S2-21 PASS**: dependent `withColumn` chain inside noise at depth 50 / 200 / 500 (build −0.94 % / +0.70 % / −1.21 %; count −1.02 % / +0.04 % / −0.37 %), no-handle `_spawn` +650 ns with no allocation, eager-on-eager 66 µs vs 4.66 ms on base, C-012 re-measured (0 registrations, RSS plateau). No P1, no P2. |
| drift | — | FIXED — C-003's pin is `test_one_eager_frame_across_actions_registers_once` (not `test_cache_view_reuse_across_actions`); C-006's idempotence pin is `test_unpersist_and_clear_cache_are_idempotent`. Ledger names corrected. |

```yaml
COVERAGE_ATTESTATION:
  pr_unit: eager-own-1
  categories:
    - id: AT-1
      status: ATTACKED
      evidence: Step 0 measured the defect end to end; step 1 closes the review's full validation table — every clause C-002..C-012 is PROVEN, each red-first against the 8936346a base copy (10 failed on base) then green on the fix, except the two guard pins that pin invariants rather than the defect and are labelled as such in their evidence cells.
      artifacts: [python/repark/tests/test_eager_own_1.py, python/repark-parity/tests/eager_own/test_eager_own.py]
    - id: AT-2
      status: ATTACKED
      evidence: The pins drive the real defect paths — bare eager() unassigned, del + gc.collect(), a live drop_temp_view spy on the session, a repark.cache.max_bytes refusal, a rewritten CSV source, four survivor shapes (lazy copy, derived, join, union), and all four export surfaces — never proxies like inspecting the WeakSet.
      artifacts: [python/repark/tests/test_eager_own_1.py]
    - id: AT-3
      status: ATTACKED
      evidence: Step 1 adds one piece of shared state — the per-session WeakSet under alive_token["cache_view_handles"] that lets clearCache enumerate live handles. It is session-scoped (inside the existing alive token, same home as the frame registry), weak (a dead handle removes itself), and exercised by the idempotence pins; no cross-session or process-global mutable state is introduced.
      artifacts: [python/repark/src/repark/spark/dataframe/cache_handle.py, python/repark/src/repark/spark/catalog.py]
    - id: AT-4
      status: N/A
      justification: No concurrency: the finalizer is a refcount-driven callback on the owning thread; the pins are single-threaded; the bench worker is one subprocess.
    - id: AT-5
      status: N/A
      justification: No authn/authz, deserialization of hostile input, path, credential, or network surface; the new module touches only the session's own drop_temp_view.
    - id: AT-6
      status: ATTACKED
      evidence: Ownership is asserted on the observable surface — catalog.listTables()/list_temp_view_names() registration counts — not on private flags; the spy pin proves the finalizer's no-call contract on a stopped session from the call log, not from internals.
      artifacts: [python/repark/tests/test_eager_own_1.py, python/repark-parity/tests/eager_own/eager_own_worker.py]
    - id: AT-7
      status: ATTACKED
      evidence: C-012 re-ran the identical fixture/scope on the step-1 tree (0 registrations at every reading, RSS plateau ~0.85–1.2 GB, VmHWM 1,257 vs 3,041 MB, wall flat) and measured the handle's spawn cost on the chain cells against a worktree-free base copy — all 12 cells within ±2.3 % of base, inside noise.
      artifacts: [docs/perf/eager-own-1-2026-09-13/after.json, docs/perf/eager-own-1-2026-09-13/base.json]
    - id: AT-8
      status: ATTACKED
      evidence: R11-D-2's audit obligation is discharged as a written table — every _spawn call site and every DataFrame(...) construction embedding another frame's plan is listed with its verdict in the Decisions section; the two construction paths that needed propagation (parent_for_stream, the D-4 identity child) are named there.
      artifacts: [task/ledgers/staging/eager-own-1-ledger.md]
    - id: AT-9
      status: ATTACKED
      evidence: The one user-visible change is orphan lifetime: a GC'd frame's __repark_cache_* view now dies with it (C-011's flipped assertion and the hand-registered prefix-sweep pin keep clearCache()'s sweep contract pinned independently); eager-on-eager reuse is observable as one registration instead of two (C-004).
      artifacts: [python/repark/tests/test_cache_persist.py, python/repark/tests/test_eager_own_1.py]
    - id: AT-10
      status: ATTACKED
      evidence: Mutation coverage: a handle that leaks (never releases) reds C-002/C-008/C-011 and the bench pin; a handle that double-drops reds the idempotence and survivor pins; a finalizer that touches a stopped session reds the spy pin; an eager-on-eager that rematerializes reds C-004's same-view assertion.
      artifacts: [python/repark/tests/test_eager_own_1.py, python/repark-parity/tests/eager_own/test_eager_own.py]
  complete: true
```

## Evidence

**C-001 measure (base `8936346a`, release native).**
`.venv/bin/python -c 'import repark._native as n; print(n.__debug_assertions__)'`
printed `False`. Box idle first (`pgrep -x cargo|rustc|maturin` all empty), then:

```
systemd-run --user --scope -p MemoryMax=24G -p MemorySwapMax=0 \
  env REPARK_EAGER_OWN_BENCH=1 \
  REPARK_EAGER_OWN_BENCH_JSON=/tmp/e-own/docs/perf/eager-own-1-2026-09-13/base.json \
  .venv/bin/python -m pytest python/repark-parity/tests/eager_own -q
→ 2 passed in 9.93 s
```

Machine: `nproc` = 64, `free -g` total = 125 GiB, Python 3.12.3. Per-iteration
table (iteration · wall s · VmRSS after call · VmRSS after gc · VmHWM ·
`__repark_cache_*` in `catalog.listTables()`):

| iter | s | VmRSS (MB) | VmRSS-gc (MB) | VmHWM (MB) | regs |
|---|---|---|---|---|---|
| 0 | 0.827 | 546 | 546 | 1,012 | 1 |
| 1 | 0.814 | 1,253 | 1,253 | 1,253 | 2 |
| 2 | 0.804 | 1,196 | 1,196 | 1,492 | 3 |
| 3 | 0.844 | 1,630 | 1,630 | 1,630 | 4 |
| 4 | 0.805 | 1,803 | 1,803 | 1,858 | 5 |
| 5 | 0.809 | 1,877 | 1,877 | 2,262 | 6 |
| 6 | 0.815 | 2,052 | 2,052 | 2,354 | 7 |
| 7 | 0.856 | 2,667 | 2,667 | 2,667 | 8 |
| 8 | 0.810 | 2,457 | 2,457 | 2,945 | 9 |
| 9 | 0.824 | 2,967 | 2,967 | 3,041 | 10 |

Post-loop: 10 registrations, VmRSS 2,967 MB. Post-`gc.collect()`: 10
registrations, VmRSS 2,967 MB — the orphans survive collection exactly as the
review's confirmed path says. Post-`clearCache()`: 0 registrations; VmRSS still
2,967 MB — the allocator retains the freed arenas, matching the review's "a
high resident reading alone does not prove live-object retention" caveat; the
registration count is the concrete ownership signal. Per-iteration wall is
flat (0.80–0.86 s) at this size on this box — the retained-result accumulation
and RSS growth are measured; the reported per-iteration slowdown is NOT
reproduced at ten iterations on a 125 GiB host and the claim is recorded
honestly rather than inflated. Raw run:
[docs/perf/eager-own-1-2026-09-13/base.json](../../../docs/perf/eager-own-1-2026-09-13/base.json).

The small pin (`test_bare_eager_registrations_accumulate_small`, 2,000 rows ×
3 iterations) is green on the base tree in the default suite and is the pin
step 1 flips to a post-gc count of 0.

**Step 1 red run (against the `git show 8936346a` base copy, product files only).**
`PYTHONPATH=/tmp/eager-own-base .venv/bin/python -m pytest python/repark/tests/test_eager_own_1.py
python/repark/tests/test_cache_persist.py -q` → `10 failed, 28 passed`: the nine
ownership pins plus the flipped orphan pin red exactly where the defect lives;
the five guard pins (`test_one_eager_frame_across_actions_registers_once`,
`test_unpersist_and_clear_cache_are_idempotent`, the wrapper-hold arm of C-006 on
the live path, and the `test_cache_persist.py` remainder) stay green on base —
they pin invariants the change must not break. The step-0 small pin and bench
pin red on their flipped zero-registration assertions the same way.

**Step 1 green run.** `.venv/bin/python -m pytest python/repark/tests/test_eager_own_1.py
test_cache_persist.py test_df_eager_1.py test_dfcore_4b_eager_goldens.py
test_dfcore_6_eager_preview.py test_sql_dml_eager.py -q` → `77 passed`; the
eager_own subprocess pins → `1 passed, 1 skipped` (bench skipped without the
env flag); the whole facade suite → `6004 passed, 364 skipped`.

**C-012 after-run (same scope, box idle first, release native).**
`systemd-run --user --scope -p MemoryMax=24G -p MemorySwapMax=0 env
REPARK_EAGER_OWN_BENCH=1 REPARK_EAGER_OWN_BENCH_JSON=…/after.json .venv/bin/python
-m pytest python/repark-parity/tests/eager_own -q` → `2 passed in 9.85 s`.

| iter | before s | after s | before VmRSS MB | after VmRSS MB | before regs | after regs |
|---|---|---|---|---|---|---|
| 0 | 0.827 | 0.802 | 546 | 540 | 1 | 0 |
| 1 | 0.814 | 0.818 | 1,253 | 1,041 | 2 | 0 |
| 2 | 0.804 | 0.916 | 1,196 | 1,096 | 3 | 0 |
| 3 | 0.844 | 0.833 | 1,630 | 1,109 | 4 | 0 |
| 4 | 0.805 | 0.869 | 1,803 | 917 | 5 | 0 |
| 5 | 0.809 | 0.820 | 1,877 | 858 | 6 | 0 |
| 6 | 0.815 | 0.797 | 2,052 | 959 | 7 | 0 |
| 7 | 0.856 | 0.864 | 2,667 | 1,210 | 8 | 0 |
| 8 | 0.810 | 0.838 | 2,457 | 1,059 | 9 | 0 |
| 9 | 0.824 | 0.829 | 2,967 | 1,199 | 10 | 0 |
| post-loop | — | — | 2,967 | 1,199 | 10 | 0 |
| post-gc | — | — | 2,967 | 1,199 | 10 | 0 |
| post-clearCache | — | — | 2,967 | 1,199 | 0 | 0 |
| VmHWM | — | — | 3,041 | 1,257 | — | — |

Each bare `eager()`'s registration now dies with its dropped child inside the
iteration, so the count is 0 at every reading; RSS plateaus ~0.85–1.2 GB
instead of climbing to ~3.0 GB; VmHWM falls 3,041 → 1,257 MB; wall stays flat
0.80–0.92 s. An earlier contaminated run (facade suite still running in
background) showed iterations 6–9 at 1.4–2.2 s; the idle-box re-run above is
the recorded number. Raw run:
[docs/perf/eager-own-1-2026-09-13/after.json](../../../docs/perf/eager-own-1-2026-09-13/after.json).

**C-012 spawn cost (S2-21 surface: the `withColumn` chain cells).** Base copy
built worktree-free — `git show 8936346a:<path>` of the three changed product
files plus `catalog.py` into `/tmp/eager-own-base/repark` over a full source
copy (same `.venv`, same `repark._native` release `.so`), first on `PYTHONPATH`.
`run_facade.py --cells chain` interleaved base/fixed, 1 warmup + 5 measured
reps each side; medians of medians:

| cell | base median ms | fixed median ms | Δ |
|---|---|---|---|
| `chain/10/build_only` | 1.862 | 1.823 | −2.1 % |
| `chain/50/build_only` | 40.359 | 40.210 | −0.4 % |
| `chain/100/build_only` | 343.464 | 340.342 | −0.9 % |
| `chain_collapsed/10/build_only` | 1.863 | 1.864 | +0.1 % |
| `chain_collapsed/50/build_only` | 19.884 | 19.945 | +0.3 % |
| `chain_collapsed/100/build_only` | 66.861 | 66.826 | −0.05 % |
| `chain_old/10/build_only` | 12.278 | 12.275 | −0.03 % |
| `chain_old/50/build_only` | 514.277 | 514.619 | +0.07 % |
| `chain_old/100/build_only` | 3,708.862 | 3,703.397 | −0.15 % |
| `chain/10/count` | 4.048 | 4.141 | +2.3 % |
| `chain/50/count` | 34.707 | 34.621 | −0.3 % |
| `chain/100/count` | 133.669 | 133.408 | −0.2 % |

Every `build_only` cell — the direct per-`_spawn` measure — lands within ±2.1 %
of base, inside run-to-run noise (e.g. `chain/10` spread 1.82–2.37 ms across
base reps alone); the handle adds no measurable per-spawn cost.

**Step 2 (docs close).** Every multi-line docstring the unit added or grew
trimmed to one line — `cache_handle.py` (module, `CacheViewHandle`,
`bind_registered_view`), `eager.py::_eager_materialize` (restored),
`test_eager_own_1.py`, `python/repark-parity/tests/eager_own/`,
`_dfcore_1_expected.py`, `test_cache_persist.py`; the removed prose lives in the
new `## cache-view ownership (EAGER-OWN-1)` section of
[python/repark/src/repark/spark/dataframe/map.md](../../../python/repark/src/repark/spark/dataframe/map.md).
One lifetime paragraph added to
[docs/guide/dataframe-guide.md](../../../docs/guide/dataframe-guide.md). The
owner's review `git mv`'d to
[docs/history/eager-own-1/](../../../docs/history/eager-own-1/map.md) with a
dated closure note; inbound links repaired in the card,
[task/roadmap/mid-term/map.md](../../roadmap/mid-term/map.md), and this ledger.
Gates: focused pins `54 passed, 1 skipped`; `check-docs-links`,
`check-map-sync`, `check-ledger-grammar`, `check-lib-py`, `make verify` — see
the step-2 commit.

**Review round (post-step-2).** New pins:
`test_every_derived_holder_kind_keeps_the_registration` (14 parametrized D-2
holder kinds, L-001), the `atexit is False` assert in
`test_finalizer_never_calls_a_stopped_session` (L-002), and
`test_release_twice_drops_once` (L-005). Red proofs on scratch package copies
(`/tmp/eager-own-mut*`, `PYTHONPATH` override, package including
`_native.abi3.so`): `_spawn` strip reds all 14 arms (`assert
<CacheViewHandle …> in ()`); `atexit = True` reds the finalizer test
(`assert True is False`); removing `if self._released: return` reds the
release-twice spy (`assert ['"datafusion"…'] == []`). Green on product: 29
passed in `test_eager_own_1.py`.

