# Unit ledger — G4b: DataFrame-API `leftsemi` / `leftanti` join widening

**Unit:** unit-queue **G4b** of the V2 Engine Hardening campaign · **Date:** 2026-08-11 ·
**Lane:** opus O-1 · **Branch:** `hardening/g4b-df-join-widening` · **Worktree:** `/tmp/opus-o1`

**This unit is a FIX, not a record-side sweep.** W-3's G4 corpus documented the DataFrame semi /
anti surface as two refuse **splits** (registry-queued REG-G4-1 / REG-G4-2, "BACKLOG, intent to
FIX"). G4b lands the fix and, in the same PR (docs/testing.md hard block), flips those two rows
to content equalities. The registry file `docs/spark-sql-iceberg-parity.md` is **not edited from
this unit** — §6 is the paste-true handoff, and the two queued rows become FIXED entries there,
never live divergences.

---

## 1. What landed

| Artifact | Path | Role |
|---|---|---|
| Engine `how`-token map | `crates/repark-python/src/dataframe.rs` | `join_type_from_str` widened with the semi family; new `join_keeps_only_left_columns` gates the Spark key-merge projection off |
| Engine pins (3) | `crates/repark-python/tests/bindings.rs` | semi rows/schema, anti rows/schema, no-key-merge invariant over every spelling |
| Facade alias map + routing | `python/repark/src/repark/dataframe/core.py` | `how_aliases` semi family, `_SEMI_JOIN_HOWS`, conditionless refusal, `_join_on_condition_h1` left-only projection |
| Differential rows | `python/repark/tests/test_join_parity.py` | 2 splits → content equalities; +4 DF semi-family rows; meta-assertions inverted; classifier probe row |
| Non-differential surface | `python/repark/tests/test_g4b_semi_join.py` | spellings, usable-frame, conditionless refusal, refusal-message contents |
| `map.md` lockstep | 5 maps (see §3) | same commit as the code |

### The engine change

`join_type_from_str` gained `semi`/`left_semi`/`leftsemi` → `JoinType::LeftSemi` and
`anti`/`left_anti`/`leftanti` → `JoinType::LeftAnti`. Every PySpark spelling is accepted at the
binding even though the facade normalizes first: `join_on_names` is a public engine surface the
Rust tests drive directly, so it must not depend on a caller-side normalization it cannot
enforce.

`join_on_names` previously ran `spark_join_projection` unconditionally to merge Spark's single
key column out of DataFusion's two-copy join output. For `LeftSemi`/`LeftAnti` the join output is
already the LEFT input's schema — there is no duplicate right key to merge — so the projection is
skipped via `join_keeps_only_left_columns`. (It would have been a no-op today, but a no-op that
would silently start mangling the schema the moment DataFusion's semi-join output shape changed.)

### The facade change

Three sites, because the `how` token reaches the engine by three different routes:

1. **Name / list keys** → `PyDataFrame::join_on_names`, which now accepts the tokens directly.
2. **Column condition** → `_join_on_condition_h1`, which builds SQL text rather than calling the
   binding. It gained `LEFT SEMI` / `LEFT ANTI` spellings *and* a `left_only` flag: a semi join
   contributes no right-hand columns, so emitting the right side in the projection would be an
   unresolvable reference, not a wider result. `display_counts` is computed from the left side
   alone for the same reason — otherwise the shared key name `k` would count as a duplicate and
   the left engine field would be mangled for nothing.
3. **Conditionless** (`on=None`, `on=[]`) → refused loud. See §2.

---

## 2. Decisions, with rationale

**D1 — Conditionless semi/anti refuses instead of cross-joining. (Declared divergence.)**
Both conditionless shapes fall through the facade's `join` to the Cartesian path. Silently
answering an m×n cross join for `how="leftsemi"` would be a wrong result set, not a missing
feature. Live PySpark 4.1.2 was probed for the real behaviour on the same basis as the corpus
(`local[2]`, ANSI on, shuffle=2), 2026-08-11:

| Recipe | Live Spark 4.1.2 | repark (this unit) |
|---|---|---|
| `left.join(right, None, "leftsemi")`, right non-empty | every left row (`k=1,2`) | `AnalysisException` |
| `left.join(right, None, "leftsemi")`, right EMPTY | zero rows | `AnalysisException` |
| `left.join(right, None, "leftanti")`, right non-empty | zero rows | `AnalysisException` |
| `left.join(right, None, "leftanti")`, right EMPTY | every left row | `AnalysisException` |
| `left.join(right, [], "leftsemi"/"leftanti")` | `IndexError: list index out of range` (PySpark internal) | `AnalysisException` |

So the conditionless semi join is "keep every left row iff the right side is non-empty", with the
anti side its complement — a real behaviour, cheap to describe, and NOT a cross join. Refusing is
the conservative half of the trade: a loud refusal is discoverable and fixable; a silent m×n
answer is neither. Pinned in `test_g4b_semi_join.py::test_conditionless_semi_family_refuses_loud`
(4 cases) plus a guard that the refusal did not widen into `how='inner'`.

**Not folded into the corpus as split rows,** even though live halves were recorded above and the
budget could have carried them: (a) the brief scopes this unit to the name/list-key and
Column-condition shapes; (b) the split runner asserts `spark.num_rows >= 1`, which the anti case
(zero rows) fails, so carrying the pair would have meant editing W-3's harness guard in a fix
unit. Queued for the orchestrator in §6 instead — declared, with the evidence attached, not
absorbed.

**D2 — The two flipped rows keep their names byte-identical.**
`df_left_semi_unsupported` / `df_left_anti_unsupported` now pin *support*, so the names read
false. Renaming them changes two pytest node ids, and docs/testing.md "Relocation discipline" §2
makes a test rename a **declared-rename unit that ships alone** with an old→new map — "a pin's
name is part of the pin". A behaviour fix is not that unit. The names are kept, each row's `note`
says why in-band, and the rename is queued in §6.

**D3 — The corpus now holds zero splits; the classifier keeps a probe row.**
Flipping the last two splits leaves the `kind == "split"` arm of `test_join_parity_row` with no
live row, and W-3's two classifier tests (CP-1, both arms) referenced the flipped rows by name.
Deleting that coverage because today's corpus happens to be split-free would leave the machinery
unguarded for the next lane that records a disclosure. `_CLASSIFIER_PROBE_SPLIT` — a `JoinRow`
deliberately **not** in `ROWS` — keeps both arms provable. It is excluded from `ROWS` on purpose:
in `ROWS` it would be an unrecorded golden and would consume budget.

**D4 — Budget ceiling 28 → 30, floor `MIN_DF_API_ROWS` 2 → 6.**
The budget is a sprawl guard, so growing it is a reviewed act with a named driver: +4 DF
semi-family rows. The DF-door floor moves up to match the surface that now exists (9 content
rows) so the door cannot quietly shrink back. Family coverage is additionally pinned
**name-gated and shape-gated** (`(how, on_mode)` pairs), per W-3's CP-2 lesson — a count alone
could be satisfied by any four rows.

**D5 — `on_mode` gained `condition` and `name_list`.**
The four `on` shapes are genuinely different engine paths (`join_on_names` vs the H1 SQL
rewrite). A claim proven on `on="k"` says nothing about `left.k == right.k`. Both new modes go
through `run_join_content`, the single recipe SSOT the record driver imports, so the recorded
golden and the asserted recipe still cannot drift apart.

**D6 — Known limitation, not fixed here.** On a semi result, `joined.select(right["k"])` resolves
to the left `k` instead of raising: the H1 origin map has no entry for the right plan id (the
side was never emitted), so lookup falls back to name resolution. Spark raises. Out of scope for
this unit (it is an H1 origin-map behaviour, not the semi binding), recorded here and in §6 so it
is not discovered as a surprise.

---

## 3. Gate evidence

Real exit codes, never a pipe's. Logs under `/tmp/opus-o1-*.log`.

| Gate | Command | Exit |
|---|---|---|
| `make verify` | `make verify > /tmp/opus-o1-verify.log 2>&1; echo $?` | **0** |
| `make py-test-facade` | `make py-test-facade > /tmp/opus-o1-facade.log 2>&1; echo $?` | **0** |
| `make preflight` | `make preflight > /tmp/opus-o1-preflight.log 2>&1; echo $?` | **0** |

Test counts: facade suite **2728 passed, 61 skipped** (base was 2707 collected: 2705 passed + the
2 split rows, which is what this unit fixes). **+21** = 4 new corpus rows + 17
`test_g4b_semi_join.py` cases (5 semi spellings + 5 anti spellings + 4 conditionless refusals +
usable-frame + inner-join guard + refusal message). Rust workspace suite green with the 3 new
`bindings.rs` pins; `cargo test -p repark-python --test bindings -- join_on_names` = 4 passed.

**Record mode (live PySpark 4.1.2, JVM):** all **30** corpus rows re-derived,
**0 mismatches**, exit 0 —

```
JAVA_HOME=/usr/lib/jvm/zulu-17-amd64 SPARK_LOCAL_IP=127.0.0.1 \
    PYTHONPATH=python/repark-parity/src \
    .venv/bin/python python/repark/tests/_record_join_goldens.py
```

Every `spark` half in this unit's four new rows is therefore **recorded, not hand-computed** —
including the two flipped rows, whose Spark halves are unchanged from W-3's recording. (The first
record pass reported 2 UNEXPECTED RAISEs, both `CANNOT_DETERMINE_TYPE`: the NULL-key rows seeded
an all-NULL right column, which Spark's `createDataFrame` cannot infer. Fixed in the fixture by
giving the right side a non-matching REAL key beside the NULL — which also strengthens the pin,
since the empty semi result is now provably the NULL logic and not an empty right side.)

The JVM lock `/tmp/grok-jvm-record.lock` was acquired atomically (`set -o noclobber`) at
`2026-08-12T01:02:00Z` and released immediately after the recording and the live-Spark probe in
§2 D1 (held ≈13 minutes). The only other JVM driver on the box was the standing containerized
thrift-server cluster, which the conductor's rule says to ignore. **No stale lock was removed** —
the lock was free when taken.

**Boundary rule (docs/testing.md "Boundary changes need a real-artifact test").** This unit
changes PyO3 seam code (`crates/repark-python/src/dataframe.rs`) behaviourally, so `maturin
develop` facade tests alone do not discharge it. They do not have to: `.github/workflows/
wheels.yml`'s `smoke` job runs the **full facade pytest suite against the PACKAGED wheel** on
every PR, so the four new corpus rows and the 17 `test_g4b_semi_join.py` cases cross the real
artifact in CI. No new wheel-path harness was needed.

`map.md` lockstep, same commit: `crates/repark-python/src/map.md`,
`crates/repark-python/tests/map.md`, `python/repark/src/repark/dataframe/map.md`,
`python/repark/tests/map.md`, `task/map.md`.

---

## 4. Provocations (the pins are not vacuous)

No new mechanical gate landed in this unit, so the "gate provocation proof" clause does not
apply. The behaviour pins carry their revert-red evidence instead:

| Pin | Evidence it reds without the fix |
|---|---|
| `df_left_semi_unsupported` / `df_left_anti_unsupported` as content | **Observed, not argued.** At the frozen base these two rows were splits and passed. After the engine+facade widening and BEFORE the corpus edit, the facade suite failed exactly these two ids with the harness's own classifier verdict: *"repark and Spark have CONVERGED — repark now succeeds with the RECORDED SPARK output, so this split disclosure is stale."* That is the corpus's designed evidence that the fix is real and the flip is warranted. |
| 3 `bindings.rs` semi-family pins | Without the `join_type_from_str` arms, `join_on_names(..., "leftsemi")` returns `Err`, so `.expect("semi-family join plans")` panics — every one of the three reds. |
| `join_keeps_only_left_columns` branch | `join_on_names_semi_family_never_merges_a_key_column` asserts the semi output is exactly 2 columns against an inner-join baseline of 3 on the same fixture; the baseline is what makes the assertion non-vacuous. |
| `_join_on_condition_h1` `left_only` branch | `df_left_semi_on_condition` / `df_left_anti_on_condition`. Without the branch the SELECT projects right-side columns from a `LEFT SEMI` join — an unresolvable reference, so the rows red rather than merely widening. |
| Conditionless refusal branch | 4 parametrized cases in `test_g4b_semi_join.py`; the companion guard (`how='inner'` still cross-joins, 3×2 = 6 rows) proves the branch did not swallow the Cartesian path — the failure mode a bare refusal test would miss. |
| semi / anti as complements | Every semi pin has an anti twin on identical inputs. An all-empty bug reds the anti side; an all-pass bug reds the semi side. Neither can be green at once. |

---

## 5. Deviations from brief

1. **Two extra deliverables beyond the brief's five.** (a) The conditionless-semi refusal (D1) —
   the widening created a path the brief did not name, and leaving it to fall through to
   `crossJoin` would have shipped a silent wrong answer. (b) `test_g4b_semi_join.py` — a new
   module for the non-differential surface (spellings, refusals), which has no Spark golden and
   so cannot be a corpus row.
2. **Budget ceiling raised** 28 → 30 (D4). The brief's deliverable 4 requires 4 new rows and the
   corpus held 26 against a ceiling of 28.
3. **The two flipped rows were NOT renamed** (D2) — the brief did not ask for a rename, and
   docs/testing.md forbids smuggling one into a behaviour change. Queued in §6.
4. **The conditionless divergence was not added to the corpus** (D1, second paragraph) — declared
   and evidenced here, queued in §6, rather than editing W-3's harness guard from a fix unit.

Never touched, per the conductor: `crates/repark-sql/tests/cross_door.rs`,
`python/repark/tests/_live_parity.py`, the live size pins in `test_parity_live.py`,
`crates/repark-iceberg/src/catalog/provider.rs`,
`python/repark-parity/src/repark_parity/compare.py`, `docs/spark-sql-iceberg-parity.md`,
`CLAUDE.md` / `AGENTS.md` / `PROJECT.md` / `STATUS.md`, `.github/`, `briefs/`, `Cargo.lock`,
`uv.lock`. No AWS access; memory catalog / local only.

---

## 6. Handoff — registry + queue (orchestrator owns the file)

> Do **not** paste into `docs/spark-sql-iceberg-parity.md` from this unit. The rows below are
> paste-true for the registry's bullet template with resolvable `path::test[case]` node ids.

### REG-G4-1 / REG-G4-2 are FIXED — land them as fixed entries, never as live divergences

W-3 queued both as "BACKLOG, intent to FIX". G4b landed the fix in the SAME campaign, so if they
are still unpasted they should enter the registry already closed (or be skipped entirely if the
registry does not carry fixed entries — the divergence no longer exists to disclose).

- **REG-G4-1 — DataFrame `leftsemi` surface gap — FIXED (G4b, 2026-08-11).**
  repark's `df.join(other, on="k", how="leftsemi")` now matches Apache Spark on the Arrow path
  (value, Arrow type, nullability): left rows with a match, left schema only.
  **Pin:** `python/repark/tests/test_join_parity.py::test_join_parity_row[df_left_semi_unsupported]`
  (now a content equality; the node id is unchanged on purpose — see the rename item below).
- **REG-G4-2 — DataFrame `leftanti` surface gap — FIXED (G4b, 2026-08-11).**
  **Pin:** `python/repark/tests/test_join_parity.py::test_join_parity_row[df_left_anti_unsupported]`

### New disclosure to queue — conditionless semi/anti join

- **repark** — `df.join(other, how="leftsemi")` with no `on` (and `on=[]`) raises
  `AnalysisException`: *"join type 'leftsemi' requires an `on` condition. A conditionless leftsemi
  join is not a Cartesian product…"*.
- **Apache Spark** — runs it: `on=None` keeps every left row when the right side is non-empty and
  none when it is empty; the anti side is the complement. `on=[]` raises a PySpark internal
  `IndexError`. *(oracle: recorded live, PySpark 4.1.2, 2026-08-11 — table in §2 D1.)*
- **Pin** — `python/repark/tests/test_g4b_semi_join.py::test_conditionless_semi_family_refuses_loud`
- **Rationale** — DELIBERATE refusal, low priority to fix. The facade's only fallback is the
  Cartesian path, which returns an m×n result set — a wrong answer, not a narrower one. A loud
  refusal is the honest state until a conditionless semi join is planned properly.

### Queued follow-up units

1. **Declared-rename unit (small, ships alone).** `df_left_semi_unsupported` →
   `df_left_semi_on_name`, `df_left_anti_unsupported` → `df_left_anti_on_name`. Old→new map above
   is the whole map; the identity gate reads empty after applying it. Held out of G4b on purpose
   (D2).
2. **H1 origin-map gap on semi results** (D6): `joined.select(right["k"])` after a semi join
   resolves to the left `k` instead of raising as Spark does. Not the semi binding — the H1
   origin map has no entry for a side that was never emitted.

### For the morning union

`python/repark/tests/map.md` and `task/map.md` are both-add files across the O- and X-lanes;
this unit adds one `test_g4b_semi_join.py` bullet + one "I want to…" row + two Debug rows to the
first, and one ledger row to the second. `python/repark/tests/test_join_parity.py` is edited by
this lane only (W-3 merged at the frozen base).

---

## 7. Authorship

Commits authored **TRO-Wolf** with the `Authored-By: Claude (claude-opus-5)` trailer, per-command
`-c` identity only. No co-author trailers, no session ids or URLs.
