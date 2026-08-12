# Unit ledger — Y-5 / G4b-R2: semi/anti origin-map join-type awareness

**Unit:** unit-queue **G4b-R2** (Y-5) of the V2 Engine Hardening campaign · **Date:** 2026-08-12 ·
**Lane:** grok Y-5 · **Branch:** `grok/y5-g4br2-origin-map` · **Worktree:** `/tmp/grok-y5` ·
**Frozen base:** `a985edf7e22b68ea720cb2a8e08fca6cdd1a33b7` (#65 / L-1)

**This unit is a FIX**, not a record-side sweep. G4b D6 recorded that after a semi/anti join,
`result.select(right["k"])` resolved to the LEFT column: the H1 origin map had no entry for a
side that was never emitted, so lookup fell back to name resolution. A semi/anti output has no
right-side columns — a resolved answer is silently wrong attribution. Live Spark raises.

---

## 0. §0 live-oracle probe (mandatory first)

**JVM lock.** `/tmp/grok-jvm-record.lock` acquired atomically (`set -o noclobber`) at
`2026-08-12T23:28:29Z` with `MARKER=y5-origin-map`. Marker-verified before use. Waited FIFO
behind Y-6 (`MARKER=y6-g10-boundary`, holder pid dead, then a live y6 Spark driver, then
release) and Y-3 (`MARKER=y3-getdatabase`, ~2 min). **No stale lock was removed.** Probe used
`/tmp/grok-y1/.venv` (PySpark **4.1.2** already installed) so this worktree's `uv.lock` was
not touched. Basis: `local[2]`, ANSI on, `spark.sql.shuffle.partitions=2`, UI off,
`JAVA_HOME=/usr/lib/jvm/zulu-17-amd64`, `SPARK_LOCAL_IP=127.0.0.1`. Driver script
`/tmp/y5-spark-probe.py` (not in-repo). Lock released immediately after the probe (marker
re-verified). The only other JVM driver on the box was the standing containerized
thrift-server cluster (ignored per conductor).

**Class is not `UNRESOLVED_COLUMN`.** Every raising recipe is
`pyspark.errors.exceptions.captured.AnalysisException` with `getCondition()` /
`getErrorClass()` one of two `MISSING_ATTRIBUTES` subclasses:

| Right-ref name vs output | Spark 4.1.2 condition |
|---|---|
| same spelling as a surviving left column (`k`) | `MISSING_ATTRIBUTES.RESOLVED_ATTRIBUTE_APPEAR_IN_OPERATION` |
| spelling not in the output (`rk`, `v`, `z`) | `MISSING_ATTRIBUTES.RESOLVED_ATTRIBUTE_MISSING_FROM_INPUT` |

### Verbatim — same-key name-join leftsemi `select(right["k"])`

```
type: pyspark.errors.exceptions.captured.AnalysisException
getCondition: MISSING_ATTRIBUTES.RESOLVED_ATTRIBUTE_APPEAR_IN_OPERATION
getErrorClass: MISSING_ATTRIBUTES.RESOLVED_ATTRIBUTE_APPEAR_IN_OPERATION
str: [MISSING_ATTRIBUTES.RESOLVED_ATTRIBUTE_APPEAR_IN_OPERATION] Resolved attribute(s) "k" missing from "k", "a" in operator !Project [k#2L]. Attribute(s) with the same name appear in the operation: "k".
Please check if the right attribute(s) are used. SQLSTATE: XX000;
!Project [k#2L]
+- Project [k#0L, a#1]
   +- Join LeftSemi, (k#0L = k#2L)
      :- LogicalRDD [k#0L, a#1], false
      +- LogicalRDD [k#2L], false
```

`filter(right["k"] == 1)` and `withColumn("x", right["k"])` raise the **same class**, operator
`!Filter` / `!Project` respectively. `leftanti` is the same class on the same recipes.
Condition-join (`left["k"] == right["k"]`) is the same class.

### Verbatim — distinct-name condition leftsemi `select(right["rk"])`

```
getCondition: MISSING_ATTRIBUTES.RESOLVED_ATTRIBUTE_MISSING_FROM_INPUT
str: [MISSING_ATTRIBUTES.RESOLVED_ATTRIBUTE_MISSING_FROM_INPUT] Resolved attribute(s) "rk" missing from "k", "a" in operator !Project [rk#5L].  SQLSTATE: XX000;
```

### Drop — brief guess FALSIFIED

`joined.drop(right["k"])` after leftsemi/leftanti is a **no-op** on live Spark 4.1.2: columns
stay `["k", "a"]`, left `k` is kept, **no raise**. Same for `drop(right["rk"])` /
`drop(right["z"])`. `drop(left["k"])` still drops. `drop("k")` (string) still drops.

The brief asked to pin drop as a raise with Spark's class. The live oracle says drop does not
raise. Matching Spark (the charter rule) means no-op, not an invented raise. Pinned that way.

### Controls (must stay working)

- `select(left["k"])` / `filter(left["k"] == 1)` / `select("k")` / `F.col("k")` after semi/anti → OK
- `inner` name-join `select(right["k"])` / `filter(right["k"] == 1)` → OK (merged key)
- `inner` condition-join `select(right["k"])` → OK (both `k`s present)

---

## 1. What landed

| Artifact | Path | Role |
|---|---|---|
| Origin-map join-type awareness | `python/repark/src/repark/dataframe/core.py` | `_origin_not_emitted` frozenset of right-side plan ids; set on both name-key and H1 condition semi/anti paths; copied by `_spawn` so filter/select/withColumn descendants see it; `_rebind_origin_column` + QCOL rewrite raise Spark's class; `drop` no-ops |
| Pins | `python/repark/tests/test_g4b_semi_join.py` | right-ref select/filter/withColumn raise; drop no-op; left refs; inner regression; distinct-name subclass |
| Maps + this ledger | `dataframe/map.md`, `tests/map.md`, `task/map.md`, `task/y5-origin-map-ledger.md` | lockstep |

`test_join_parity.py` (Y-4) and `column.py` (Y-7) were **not** touched.

### The altitude

The defect is not "select is wrong". `_rebind_origin_column` is the shared origin lookup for
`select` / `filter` / `withColumn` / `_column_of`. Drop has its own lookup and must consult the
**same** not-emitted set (otherwise it name-drops the left column). QCOL rewrite (filter
compounds that clear origin bits but keep `__REPARK_QCOL_*` tokens) consults it too.

A sentinel-in-`_origin_map` was rejected: name-key semi currently attaches no origin map
(inner-join name fallback must stay), and `_spawn_preserving_identity` only copies the map
when `_display_names` is set. A dedicated `_origin_not_emitted` copied by `_spawn` is the
join-type bit that every descendant sees without changing inner-join identity.

Self-semi (`df.join(df, …)`) is exclusive-set: right plan ids minus left plan ids is empty, so
`df["k"]` still resolves (the output *is* that origin).

---

## 2. Decisions, with rationale

**D1 — Match Spark's `MISSING_ATTRIBUTES` class, not the brief's `UNRESOLVED_COLUMN` guess.**
§0 is the oracle. The facade raises `AnalysisException` whose message carries the Spark
condition tag (`MISSING_ATTRIBUTES.RESOLVED_ATTRIBUTE_APPEAR_IN_OPERATION` or
`…MISSING_FROM_INPUT`). SQLSTATE / plan-dump / expr-ids (`k#2L`) are omitted — the same
deliberate surface as `[AMBIGUOUS_REFERENCE]` (no repark error carries SQLSTATE). Tests pin
class + condition needle + `Resolved attribute(s) "…"`.

**D2 — `drop(right[…])` is a no-op, not a raise.** Live Spark 4.1.2. Pre-fix repark dropped
the left column by name. The origin-map fix is: recognize the origin as unemitted and
`continue`, never name-fallback. The brief's "drop raises" clause is recorded here as
falsified, not implemented.

**D3 — Two subclasses, not one family tag.** Same-name vs distinct-name is a real Spark split
(the same-name class exists specifically to say "an attribute of this spelling is present —
you bound the wrong one"). A single `MISSING_ATTRIBUTES` needle would green a bug that always
emitted the same-name subclass. Both are pinned.

**D4 — Inner join is a regression guard, not a rewrite.** Name-key inner still has
`_origin_map is None` and name-fallback; condition inner still has a two-sided origin map.
The not-emitted set is empty. `test_inner_join_right_ref_still_resolves` covers both `on`
shapes.

**D5 — No registry row.** This is a silent-wrong-attribution bug closed to Spark. G4b D6 is
FIXED. No remaining divergence to disclose. Conditionless semi/anti refusal is unchanged
(already disclosed by G4b / L-1).

---

## 3. Gate evidence

Real exit codes, never a pipe's. Logs under `/tmp/y5-<gate>.log`.

| Gate | Command | Exit |
|---|---|---|
| targeted facade | `pytest python/repark/tests/test_g4b_semi_join.py` | **0** (43 passed; was 17) |
| H1/H2 origin regression | `pytest test_g1_stat_and_expander.py test_h2_group_h2.py -k 'h1_ or H1 or origin or join'` | **0** (26 passed) |
| `make verify` | `make verify > /tmp/y5-verify.log 2>&1; echo $?` | **0** |
| `make preflight` | `make preflight > /tmp/y5-preflight.log 2>&1; echo $?` | **0** (facade **2848 passed, 71 skipped**) |

`map.md` lockstep, same commit: `python/repark/src/repark/dataframe/map.md`,
`python/repark/tests/map.md`, `task/map.md`, this ledger.

---

## 4. Provocations (the pins are not vacuous)

| Pin | Evidence it reds without the fix |
|---|---|
| `test_right_ref_select_raises_missing_attributes_same_key` | **Observed at frozen base:** `joined.select(right["k"]).to_arrow()` returned `{'k': [1]}` — the left column — on both the name-key path (`_origin_map is None`) and the condition path (left-only map). Revert `_remember_unemitted_right_origins` / the `_rebind` check and the pin reds by succeeding. |
| `test_right_ref_filter_raises_missing_attributes_same_key` | Pre-fix `filter(right["k"] == 1)` counted 1 (name-bound left `k`). Binary ops clear origin bits but keep QCOL tokens; the raise is the QCOL scan in `_rebind_origin_column`, not a select special-case. |
| `test_right_ref_with_column_raises_missing_attributes_same_key` | Same shared `_rebind` path. |
| `test_right_ref_drop_is_spark_noop` | Pre-fix `drop(right["k"])` returned columns `['a']` — dropped the LEFT `k`. The pin asserts `["k", "a"]`. A raise here would also red (and would be an invented Spark). |
| `test_left_refs_still_resolve_after_semi_family` | Exclusive-set: left plan ids are not recorded as unemitted. A sloppy "all non-self plan ids" implementation would refuse `left["k"]`. |
| `test_inner_join_right_ref_still_resolves` | `_SEMI_JOIN_HOWS` only. If the remember-call leaked into `inner`, this reds. |
| `test_distinct_name_right_ref_raises_missing_from_input` | Wrong subclass (always `APPEAR_IN_OPERATION`) reds the `MISSING_FROM_INPUT` needle. |

---

## 5. Deviations from brief

1. **Drop does not raise.** Brief: "right-ref select/drop/filter raise with Spark class+needle".
   Live Spark drop is a no-op. Implemented and pinned as no-op (D2).
2. **Class is `MISSING_ATTRIBUTES.*`, not `UNRESOLVED_COLUMN` / `AMBIGUOUS_REFERENCE`.**
   Brief listed those as guesses. §0 wins.
3. **`withColumn` is pinned too.** Brief named it as a map consumer; one parametrized test
   keeps the "not bolted onto select" claim revert-red.

No `test_join_parity.py` edit. No `column.py` edit. No new join types. Conditionless refusal
untouched.

---

## 6. Handoff — registry + queue (orchestrator owns the file)

> Do **not** paste into `docs/spark-sql-iceberg-parity.md` from this unit. There is **no new
> live divergence**. G4b D6 (H1 origin-map gap on semi results) is **FIXED**.

### G4b D6 is FIXED

- **repark** — after `left.join(right, on, "leftsemi"|"leftanti")`, `select` / `filter` /
  `withColumn` of a right-parent Column raise `AnalysisException` carrying Spark 4.1.2's
  `MISSING_ATTRIBUTES.RESOLVED_ATTRIBUTE_APPEAR_IN_OPERATION` (same-name) or
  `MISSING_ATTRIBUTES.RESOLVED_ATTRIBUTE_MISSING_FROM_INPUT` (distinct-name).
  `drop(right[…])` is a no-op (left schema unchanged). Left-parent refs and inner-join
  origin resolution are unchanged.
- **Apache Spark** — the same classes and the same drop no-op (oracle: live PySpark 4.1.2,
  2026-08-12, transcript in §0).
- **Pin** —
  `python/repark/tests/test_g4b_semi_join.py::test_right_ref_select_raises_missing_attributes_same_key`
  (and the filter / withColumn / drop / left-ref / inner / distinct-name siblings).

No `Disclosure(...)` block. No live-mirror token. Conditionless semi/anti refusal remains
the G4b disclosure already landed by L-1 (`conditionless_semi_anti_refuses`).

### For the morning union

`python/repark/tests/map.md` and `task/map.md` are both-add files this wave; this lane adds
the G4b-R2 bullets / Debug rows / ledger link. `test_join_parity.py` is Y-4's.

---

## 7. Authorship

Commits authored **TRO-Wolf** (`64240326+TRO-Wolf@users.noreply.github.com`) with the
`Authored-By: Grok (grok-4.5) <noreply@x.ai>` trailer, per-command `-c` identity only. No
co-author trailers, no session ids or URLs. `%ae` checked after every commit.
