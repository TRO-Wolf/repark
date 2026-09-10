# Unit ledger — DF-EAGER-1 · `.eager()`, `.compute()`, `.lazy()`

**Retires:** this ledger moves to `../completed/` in the unit's last commit.
This file closes when DF-EAGER-1 merges, or when the owner closes the slate row.

**Unit:** DF-EAGER-1 · **Date:** 2026-09-09 · **Executor:** GLM 5.3 Flash (zai/glm-5.3-flash), Actor ·
**Branch:** `feat/df-eager-1` · **Base:** `f00ed9ea` (card facts); branch at `2215315a` =
`origin/main` at dispatch
**Model:** GLM 5.3 Flash (zai/glm-5.3-flash)
**risk_tier:** standard.
**Step 2 actor:** Muse Spark (muse-spark-1.3-contributor) · 2026-09-09 · branch `feat/df-eager-1-step2`.

Step 1 of the card's Steps table (red-first pins) is done; step 2 (implement D-1..D-6 in
`core.py` + `polars.py`, the guide section, the maps) is open. Every red below was measured on
this clone 2026-09-09 through the built facade; nothing is guessed.

## Proposition ledger

| ID | Clause (card decision) | Proof obligation | Verdict |
|---|---|---|---|
| C-001 | D-1: `eager()` materialises through the cache-view path and answers a **new** `repark.DataFrame` (same class, full Spark surface, never a polars object) whose `_eager_shape` is `(rows, cols)` read from the materialised table, leaves the source frame unchanged, and over `repark.cache.max_bytes` refuses naming `.eager()` and the key. | Pins `test_eager_returns_new_frame_with_eager_shape_rows_and_columns` (new frame + shape), `test_eager_leaves_source_frame_unchanged` (source plan and cache mark untouched), `test_eager_frame_survives_csv_source_deletion` (materialised: answers after the file is gone), `test_eager_over_max_bytes_refuses_naming_eager` (guard names `.eager()` and the key); guard `test_guard_spark_collect_still_returns_rows` holds the full-Spark-surface half. Recorded red: `PySparkAttributeError` on `eager` (below). | **PROVEN** |
| C-002 | D-2: `compute()` is an alias — the same function object as `eager()` (`DataFrame.compute is DataFrame.eager`). | Pin `test_compute_is_eager`. Recorded red: class-level `AttributeError` (below). | **PROVEN** |
| C-003 | D-3: `lazy()` on a non-eager frame returns `self`; on an eager frame returns a copy without `_eager_shape` over the same view, no re-execution, no drop. | Pin `test_lazy_identities_per_d3` (identity `is self` when lazy; `is not` + no `_eager_shape` + same answers with the eager frame intact when eager). Recorded red: `PySparkAttributeError` on `lazy` (below). The no-drop / view-answer halves are pinned observably; "over the same view (no re-execution)" stays the card's binding on step 2 — see Notes. | **PROVEN** |
| C-004 | D-4: `repr` of an eager frame uses `_eager_shape`, skips `count()` (head/tail fetches still run against the view) and prints the same table as the lazy frame. | Pin `test_repr_of_eager_frame_skips_count_and_matches_lazy_table` — count spy per the D-11 ruling (`_install_count_spy`, the `test_display_polars_default.py` idiom), on a 12-row polars-style frame so the lazy door measurably pays exactly one `count()` and the eager door must pay zero with an identical table. Recorded red: `PySparkAttributeError` on `eager` (below). D-4's other half — `count()` on an eager frame returns `_eager_shape[0]` without a query — has no pin in this round's list; see Notes. | **PROVEN** |
| C-005 | D-5: an eager frame owns its view; `unpersist()` drops it (existing behaviour); no `__del__`; the session's `stop()` drops every view already. | No new pin this round — the card's step-1 list names none for D-5. Held at step 2 by `test_cache_persist.py`'s unpersist rows staying green plus the `test_lazy_identities_per_d3` no-drop assertions. See Notes. | **PROVEN** |
| C-006 | D-6: `PolarsFrame.eager()` wraps `self._frame.eager()`; `PolarsFrame.collect()` is not touched. | Guard `test_guard_pl_collect_still_returns_polars_dataframe` pins the untouched-`collect()` half. The `eager()` mirror half has no pin in this round's list; see Notes. | **PROVEN** |

## Gate 1 — the committed shape (markers on)

Command: `VIRTUAL_ENV=$PWD/.venv uv run --no-project python -m pytest python/repark/tests/test_df_eager_1.py -q -rxX`

```text
xxxxxxx..                                                                [100%]
2 passed, 7 xfailed in 0.80s
```

Seven pins carry `pytest.mark.xfail(strict=True, reason="DF-EAGER-1 step 2 implements
.eager()/.compute()/.lazy()")` through the module's single `_XFAIL_STEP_2` decorator; step 2
turns each green by deleting its marker, and `strict=True` reds the suite if a pin passes early.
The two `test_guard_*` rows carry no marker and pass on the base tree today. This is why the red
is recorded from a direct run rather than from the suite (D-9): in the suite the seven reds
report as `xfailed` by design, so the true failure output is visible only with the markers
removed.

## Red-first run (markers removed in the working copy only, never committed)

Same command without `-rxX`, the seven `@_XFAIL_STEP_2` lines stripped from the working copy,
markers restored before staging (`git diff --cached` shows them present).

```text
FFFFFFF..                                                                [100%]
FAILED python/repark/tests/test_df_eager_1.py::test_eager_returns_new_frame_with_eager_shape_rows_and_columns
FAILED python/repark/tests/test_df_eager_1.py::test_eager_leaves_source_frame_unchanged
FAILED python/repark/tests/test_df_eager_1.py::test_compute_is_eager - Attrib...
FAILED python/repark/tests/test_df_eager_1.py::test_lazy_identities_per_d3 - ...
FAILED python/repark/tests/test_df_eager_1.py::test_repr_of_eager_frame_skips_count_and_matches_lazy_table
FAILED python/repark/tests/test_df_eager_1.py::test_eager_frame_survives_csv_source_deletion
FAILED python/repark/tests/test_df_eager_1.py::test_eager_over_max_bytes_refuses_naming_eager
7 failed, 2 passed in 0.88s
```

Exit 1; both guards green, every pin red. Two distinct failure shapes, one trace each:

| Shape | Pins | Trace (verbatim core) |
|---|---|---|
| Instance attribute door — `DataFrame.__getattr__` raises the facade's classified error | 6: eager-shape, source-unchanged, lazy identities, repr, CSV, over-limit | `E repark.errors.PySparkAttributeError: [ATTRIBUTE_NOT_SUPPORTED] Attribute `eager` is not supported.` raised at `python/repark/src/repark/spark/dataframe/core.py:2006` (`test_lazy_identities_per_d3` shows the same shape with ``Attribute `lazy` ``) |
| Class attribute access — no metaclass door | 1: `compute is eager` | `E AttributeError: type object 'DataFrame' has no attribute 'compute'` at the pin's `assert DataFrame.compute is DataFrame.eager` |

A step-2 actor seeing `ATTRIBUTE_NOT_SUPPORTED` is looking at the missing method — the same door
the DF-EXPLAIN-1 red used — and the class-level red is the missing alias pair.

## Guards (keep-green, no marker)

| Pin | Holds | Base runs |
|---|---|---|
| `test_guard_spark_collect_still_returns_rows` | Spark `collect()` answers `Row` objects, untouched (card fact) | passed in both runs |
| `test_guard_pl_collect_still_returns_polars_dataframe` | `df.pl.collect()` answers a real `polars.DataFrame`, untouched (card fact) | passed in both runs |

Both compare values order-insensitively (`sorted(...)`) because the first marker-less run
measured the unordered `SELECT 1 AS id UNION ALL SELECT 2` answering `[2, 1]` through
`pl.collect()` where an earlier run answered `[1, 2]` — row order is not defined on an unordered
plan and a guard must not flake on it. Reported to the orchestrator as observed.

## Notes for step 2 — what the card's contract leaves open

| Item | Note |
|---|---|
| `_eager_shape` access | Pins read it as an instance attribute on the eager frame (`eager._eager_shape == (rows, len(columns))`) and require the lazy copy to carry no shape (`getattr(lazy_back, "_eager_shape", None) is None`). The card's "an `_eager_shape` slot" reading — an instance attribute present only on eager frames — is the binding one; a property answering `None` on lazy frames would also satisfy the pins. |
| Over-limit message and class | The pin expects the existing guard's `IllegalArgumentException` (measured on the `cache()` path in `test_cache_persist.py::test_cache_max_bytes_refuses_oversized_materialize`) with a message containing `.eager()` and `repark.cache.max_bytes`. Measured today (`crates/repark-core/src/session/temp_views.rs` `register_collected_memtable`): the guard's fixed text says "avoid cache()/persist() on this plan", so step 2 must make the eager path's refusal name `.eager()` — the card says reuse the guard, not duplicate it. |
| D-3 mechanism | "Over the same view (no re-execution, no drop)" is pinned observably: the copy answers the same rows and the eager frame stays intact afterwards. Whether the copy carries `_cache_view` is step 2's choice; the pin does not reach into it. |
| D-4's count half | "count() on an eager frame returns `_eager_shape[0]` without a query" has no pin in this round's step-1 list. Step 2 should extend the `_install_count_spy` idiom to `eager.count()` when it deletes the markers (one assertion, no new file). |
| D-5 | No new pin this round — the card names none and binds to existing behaviour. `test_cache_persist.py`'s unpersist rows plus a green facade suite carry it at step 2. |
| D-6's mirror half | `PolarsFrame.eager()` wrapping `self._frame.eager()` has no pin in this round's step-1 list; step 2 owes one beside the mirror in `polars.py`. |
| Unordered-plan row order | The first marker-less run measured `[2, 1]` from `pl.collect()` on the unordered two-row UNION ALL after an earlier run answered `[1, 2]`. All value comparisons in this file are order-insensitive; nothing here pins row order on an unordered plan. |

## Provisioning

The clone carried a warm `.venv` and a same-day native module (`_native.abi3.so`, built
2026-09-09 11:29 against this tree), so gate 1 and the red run needed no rebuild. Gate 3
(`make py-test-facade`) re-provisions exactly per the target (`uv sync --locked` + the four
facade extras + `maturin develop`) and is recorded at the hand-back. Disk before the run:
473G free.

## Step 2 implementation (2026-09-09, Muse Spark)

Step-1 red confirmed at pickup: `2 passed, 7 xfailed` on the markers-on suite, and a live
probe showed `hasattr` false for `eager` / `compute` / `lazy` with no `_eager_shape` slot.

Shape. New `python/repark/src/repark/spark/dataframe/eager.py` (92 lines) holds the three
frame-first bodies (`_eager_materialize`, `_to_lazy`, `_count_rows`) plus the moved
cache-guard trio, following the DFCORE-4a split pattern: public methods stay as one-line
wrappers on the class, the leaf imports stay function-local, and `core.py` re-imports the
moved names by identity. `core.py` 4525 → 4487 (baseline ratcheted down in the same commit);
`display.py` +6 (`_styled_total_rows`); `polars.py` +4 (`PolarsFrame.eager`).

HALT-tripwire measurement (card: HALT if the row count needs a separate query, i.e. a Rust
return-value change). Measured: `materialize_as_cache_view` returns unit (`PyResult<()>` in
`crates/repark-python/src/session.rs`); `PyDataFrame` exposes `count` (executes) and
`column_names` (logical only) — no zero-execution row count exists on the Python side. The
card's D-1 mechanism ("read from the materialised table (no separate count)") was first
implemented as one MemTable scan via `to_arrow()` on the fresh view — then the audit
finding (D-7 ruling 2026-09-09) measured that copy as O(n) extra memory for two integers
and ordered the cheaper middle way. `_eager_shape` is now filled once, at `.eager()` time,
as `(sibling._action_inner().count(), len(sibling.columns))`: the count runs over the
already-built MemTable (O(1) memory, no Arrow copy crosses to Python) and the column count
comes from the schema with no execution. D-1's "no separate count" bars an extra pass over
the source plan; a count over the resident view is not that. No Rust change was needed, so
no HALT. No pin needed a spy adjustment: the fill calls `_action_inner` directly (never the
patched `DataFrame.count`), and every spy window in the pins opens after `.eager()` returns
— all 11 pins stayed green untouched. `count()` and the styled `repr`/`show` totals on an
eager frame reuse the shape with zero engine actions (pinned by the `_action_inner` spy and
the D-11 count spy).

Over-limit refusal reuses the guard call and wraps only its `IllegalArgumentException` to
name `.eager()` beside the key (other engine failures pass through unchanged). `unpersist()`
clears `_eager_shape` with the view so a shape never outlives its materialization; the lazy
copy from D-3 carries no shape and no cache ownership, so dropping either side cannot strand
the other beyond ordinary temp-view lifetime.

Step-2 pins added beside the mirror (no new file): `test_eager_count_returns_shape_without_action`
(D-4 count half, with a lazy-frame control proving the spy observes real actions) and
`test_polars_frame_eager_wraps_spark_eager` (D-6 mirror half). No step-1 pin assertion was
changed — only markers deleted.

Gates (this round): `test_df_eager_1.py` 11 passed, zero markers; `make py-test-facade`
exit 0, then the identical pytest rerun on the final tree: 5851 passed, 369 skipped;
`make py-lint` green; `python3 scripts/check_lib_py.py` green (599 files, 32 exceptions,
core row at 4487); `ruff format --check` clean on every touched file;
`check_python_conventions`, `check_docstring_presence.sh`, `sync_map_md.py --check`, and
`check_map_md.sh` all clean.

Follow-up round (D-7 shape-read fix, same actor): `test_df_eager_1.py` 11 passed with no
spy change; `make py-test-facade` exit 0 with the identical totals (5851 passed,
369 skipped); `make py-lint`, `check_lib_py.py` (core row untouched at 4487), and
`ruff format --check` all green.

Step-3 gates (same actor): `test_df_eager_1.py` 11 passed; `make check-example-coverage`
green (924 names, 794 covered, 210 examples); `make check-docs-links` clean (701 files,
4532 links); `python3 scripts/check_ledger_grammar.py` clean; `make py-test-facade` exit 0
(5851 passed, 369 skipped); `make py-lint` green; `check_lib_py.py` green (599 files).
No size baseline raised or ratcheted this round. `test_dfcore_1_exports.py` sits at exactly the 1000-line default ceiling
after its declared delta — the next unit that touches it must split it (the file's own
sanctioned out).

Step-3 close (this round) files the attestation below. The `STATUS.md` truth-up stays
with the departure edit, and the ledger stays in `staging/`.

## Step 3 (2026-09-09, Muse Spark)

Guide: `docs/guide/dataframe-guide.md` gained the "Lazy and eager" section after "The lazy
model" — the `df.lazy()` / `lf.eager()` pair table plus one subsection per card bullet, every
code block a transcript run on this clone (ordered three-row frame; the over-limit message
pasted verbatim including its measured 312-byte size).

Example: new `docs/examples/dataframe/lazy_and_eager.py` (corpus form, executed clean with
`.venv/bin/python`) covers `DataFrame.eager`, `DataFrame.compute`, `DataFrame.lazy`; the
three rows joined `docs/examples/inventory.txt` in sorted position and the file joined
`docs/examples/dataframe/map.md`. `make check-example-coverage` is green again
(dataframe=153, 210 examples).

Mutation probe (AT-10 evidence, throwaway edit, reverted): deleting the `_count_rows`
shape shortcut fails exactly `test_eager_count_returns_shape_without_action` (1 failed,
10 passed); the revert returns 11 passed. The working tree after the revert is byte-identical.

```yaml
COVERAGE_ATTESTATION:
  pr_unit: df-eager-1
  categories:
    - id: AT-1
      status: ATTACKED
      evidence: Every card decision walked against behavior, not paraphrase — D-1 (new frame plus shape, source untouched, CSV deletion proves materialization, over-limit names .eager()), D-2 (class-level identity, not a wrapper), D-3 (is-self on lazy, shape-less answering copy on eager), D-4 (identical repr table with zero count calls, plus the count shortcut), D-5 (existing unpersist rows green), D-6 (polars collect guard plus the eager mirror) — eleven pins in python/repark/tests/test_df_eager_1.py, seven of them red-first.
      artifacts: [python/repark/tests/test_df_eager_1.py, python/repark/src/repark/spark/dataframe/eager.py]
    - id: AT-2
      status: ATTACKED
      evidence: Boundaries exercised — two-row and three-row frames, the 12-row probe-overflow frame (lazy pays exactly one count, eager zero), the CSV-backed frame answering after the file is gone, max_bytes=1 refusing a 312-byte materialize, unordered plans compared order-insensitively after the suite measured both orders. No pin covers an empty-frame eager() or a bridge-frame eager(); neither shape is reachable through a distinct code path (both flow the same materialize call), so both stay unpinned by decision, not by miss.
      artifacts: [python/repark/tests/test_df_eager_1.py]
    - id: AT-3
      status: ATTACKED
      evidence: The over-limit refusal raises IllegalArgumentException naming .eager() and the key with the measured sizes, chained to the original error; only that class is wrapped, engine failures pass through unchanged. A refused materialize commits no view and no lineage (the cache path's existing guarantee, unchanged). No retry, timeout, or crash-mid-operation surface exists on this single-node synchronous path.
      artifacts: [python/repark/tests/test_df_eager_1.py, python/repark/src/repark/spark/dataframe/eager.py]
    - id: AT-4
      status: ATTACKED
      evidence: The source frame is provably untouched (is_cached False, same answers after eager()), each eager() mints a fresh view so two eager frames never share ownership, the D-3 copy carries no shape and no cache ownership, and unpersist() clears shape with view so a shape never outlives its materialization. No threads, no shared mutable frame state; the alive token is shared read-mostly session state, unchanged by this unit.
      artifacts: [python/repark/tests/test_df_eager_1.py, python/repark/src/repark/spark/dataframe/eager.py]
    - id: AT-5
      status: N/A
      justification: Materialization touches no privilege boundary — no auth, no credential, no secret, no network, no path traversal (view names are session-generated scratch names), no deserialization of untrusted input.
    - id: AT-6
      status: ATTACKED
      evidence: The eager frame answers byte-identical rows to the lazy plan on every pin (collect/compare, CSV deletion, lazy-copy agreement), keeps display and engine identity through _identity_child, and leaves both surfaces' meanings unchanged (Spark collect still rows, pl.collect still a real polars frame). The known forward-compat item is disclosed residue R-001, not an absorbed divergence.
      artifacts: [python/repark/tests/test_df_eager_1.py, docs/examples/dataframe/lazy_and_eager.py]
    - id: AT-7
      status: ATTACKED
      evidence: The system-breaking shape on this path was a full Arrow copy to learn two integers, and it is gone: the D-7 audit finding replaced to_arrow() with one MemTable count plus schema columns, so .eager() holds exactly one resident copy. No other unbounded growth on the path — the view itself is the requested materialization, guarded by repark.cache.max_bytes.
      artifacts: [python/repark/src/repark/spark/dataframe/eager.py, docs/guide/dataframe-guide.md]
    - id: AT-8
      status: ATTACKED
      evidence: compute-is-eager is a same-object identity pin, not a behavioral alias; the over-limit error keeps its IllegalArgumentException class through the wrap; the dfcore export snapshots absorbed the surface change with declared deltas (class dir plus three, slots plus one, package plus eager); the three new public names carry covering-example rows and check-example-coverage is green.
      artifacts: [python/repark/tests/test_dfcore_1_exports.py, python/repark/tests/test_df_eager_1.py, docs/examples/dataframe/lazy_and_eager.py]
    - id: AT-9
      status: ATTACKED
      evidence: The refusal message carries the entry point, the key, and both sizes verbatim (pasted into the ledger and the guide); every red-first run pasted its failing atom into this ledger; the stored shape is introspectable as _eager_shape. No new log lines were added — failures arrive as raised errors with the measured values inline.
      artifacts: [task/ledgers/staging/df-eager-1-ledger.md, docs/guide/dataframe-guide.md]
    - id: AT-10
      status: ATTACKED
      evidence: Red-first every round that added behavior — step 1 (7 failed, recorded), step 2 (7 unmarked plus 2 new pins, all green), follow-up (mechanism swap, all green untouched). Branch liveness by live mutation: deleting the _count_rows shortcut fails exactly the shortcut pin and nothing else; the revert returns 11 green. The over-limit wrap branch flips on max_bytes set versus unset; the lazy self-versus-copy branch flips on eager versus lazy input.
      artifacts: [python/repark/tests/test_df_eager_1.py]
  complete: true
```

## Residue

| ID | Residue | Disposition |
|---|---|---|
| R-001 | `materialize_as_cache_view` returns unit today, so `.eager()` pays one MemTable count to learn the row count. If a later step teaches the call to return the collected row count, the fill becomes free — D-1's shape read with no query at all. | Open for a later step; the Rust change the card defers. |
