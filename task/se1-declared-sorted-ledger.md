# SE-1 — declared-sorted temp views (sort elimination, PR-A: engine seam)

Unit: SE-1 (evening wave E, 2026-08-16; owner P1 "TA lib optimizations first"). Funded by
the release-build engine-time split: `SortExec` is ~77% of engine time on the windowed
serving shape; elision is worth up to 4.3× single-series. Orchestrator-executed.

## Evidence (probe, 2026-08-16, DF 54.1)

Scratch probe (not committed): identical 1.2M-row (symbol, ts)-sorted data registered as a
plain `MemTable` vs `with_sort_order`:

| cell | plain | declared |
|---|---|---|
| tp=1 | SortExec ×1 | **SortExec ×0** |
| tp=default (RepartitionExec ×1 present at this scale) | SortExec ×1 | **SortExec ×0** |

Results byte-identical in every cell. The declared ordering survives DataFusion's hash
repartition — no `prefer_existing_sort` needed. At small row counts (≲6k) DF plans no
repartition at all; the committed pins therefore assert only the `SortExec` contract.

## Decisions

- **Trust model: declare + ALWAYS verify, refuse loud.** O(n) adjacent-pair lexicographic
  pass (ASC NULLS LAST, cross-batch) at declaration time; no skip switch. A wrong claim
  raises `Analysis` naming the offending row pair and the original registration stays.
- **Scope: in-memory (`MemTable`) views only** — createDataFrame/cache frames, the bench
  serving path. Parquet `file_sort_order` and Iceberg sort-order metadata are later
  increments. Non-`MemTable` providers refuse loudly.
- Sort keys are ENGINE field names via `Column::from_name` (no ident parsing — the
  U-DF-1 lowercase-fold class cannot recur here).

## Delivered (this PR)

- `crates/repark-core/src/sorted_view.rs` — verification + declared order construction.
- `crates/repark-core/src/session.rs::declare_temp_view_sorted` — the public door.
- `crates/repark-python/src/session.rs::declare_temp_view_sorted` — PyO3 binder
  (GIL released for the scan). Facade `df.declareSorted(...)` is PR-B (after the
  conductor-17 explode fix merges — it owns the facade bind seam this wave).
- `crates/repark-core/tests/declared_sorted.rs` — plan pins + refusal battery.

## Residue / next (after PR-A)

- PR-B: facade `declareSorted` + display→engine key resolution + facade pins + a
  serving-shape EXPLAIN pin; timing numbers on release builds in a quiet-machine window
  (shared handshake with the allocator lane).
- Parquet + Iceberg ordering declarations: separate increments, design in the SE-1 section
  of the wave plan.

# PR-B — the facade door (2026-08-17)

## Headroom extract first (T0b, move-only)

`core.py` sat at 8199 of an 8200-line ceiling, so PR-B had to make its own room. The
module-level helper block that trailed the `DataFrame` class — the r23b N2 plan-collapse
helpers, the G2 range-order gate, the `show` / eager-eval / polars / duckdb formatters, the
Arrow→display and Arrow→SQL type mappers, the r24 DF1 `dynamicFlatten` struct expander and
the r20 H1 join-qcol rewriters — moved **verbatim** to
`python/repark/src/repark/spark/dataframe/plan_collapse.py`.

| | before | after |
|---|---|---|
| `core.py` lines | 8199 | 7224 |
| `core.py` ceiling | 8200 | **7350** |
| `plan_collapse.py` | — | 1096 (default 2500 ceiling; no EXCEPTIONS row) |

32 defs + 3 module constants moved; not one line of their bodies changed. The new module
imports nothing from `core` at module scope (the two `core`-side names it mentions are
annotations only, under `TYPE_CHECKING`), which is why it can be imported *before* the
other region modules. `core.py` re-exports **all 35** moved names from the existing tail bind
block (the r27 T0 precedent: every moved private helper stays reachable, not just the ones
still called) — that block is now hand-ordered (`# noqa: E402, I001`) because
`joins_columns` and `writer_readwriter` import two of the moved helpers *from core*, so
`plan_collapse` must bind first. Every
`repark.spark.dataframe[.core]` import path is unchanged (Q7 freeze).

## Door shape

`DataFrame.declare_sorted(*cols)` with the disclosed camelCase alias `declareSorted`
(one function object, not two implementations). It:

1. refuses `PySparkValueError` with no keys;
2. refuses `PySparkValueError` naming "declareSorted applies to source frames" unless the
   frame carries `_source_view_name` — a new `__slots__` entry stamped **only** by the two
   `__repark_cdf_*` materializers in `session/_funcs.py`, and copied by no `_spawn` path, so
   every transformed frame refuses without needing to inspect its plan;
3. resolves each display name through `_resolve_getitem_column_name` (case-insensitive,
   raises listing the available columns) then `_engine_field_for_display` (the H1
   display→engine overlay) — the same pair `select` uses, so mixed-case createDataFrame
   fields declare under any spelling and the engine always receives ENGINE field names;
4. calls the native `declare_temp_view_sorted`, which ALWAYS verifies and refuses loud;
5. **re-plans its own `_inner`** — see below;
6. returns `self`, so the call chains and declaring twice is idempotent.

### The re-plan (the non-obvious half)

A frame's logical plan captures the table source at planning time. `declare_temp_view_sorted`
re-registers the view's `MemTable`, so a frame built before the declaration keeps scanning
the *old* provider: measured, the declaring frame planned `SortExec ×1` while a frame created
afterwards planned `SortExec ×0`. The door therefore re-issues `SELECT * FROM <view>` into
`_inner` after a successful declaration. That is exact here and only here: the door only ever
runs on a source frame, whose plan *is* that scan. Mutation-tested by the plan pin.

## Measured, on the facade (tp=1, createDataFrame source)

| shape | undeclared | declared |
|---|---|---|
| SQL window, `ORDER BY ts ASC NULLS LAST` | SortExec ×1 | **×0** |
| SQL window, `ORDER BY ts` (Spark default) | ×1 | ×1 |
| `Window.partitionBy(sym).orderBy(ts)` facade spec | ×1 | ×1 |

Results identical in every cell.

**Re-measured 2026-08-17 (F-4 docs lane, writing `docs/guide/ta-guide.md`).** Row 2 does not
reproduce. On a `repark.target.partitions=1` session over 3 symbols × 300 bars, counting `SortExec`
in the physical plan, with both `row_number()` and `ta_sma(close, 3)` as the window function:

| window ordering | undeclared | declared |
|---|---|---|
| SQL `ORDER BY ts ASC NULLS LAST` | ×1 | **×0** |
| SQL `ORDER BY ts` (bare) | ×1 | **×0** |
| SQL `ORDER BY ts ASC NULLS FIRST` | ×1 | ×1 |
| `Window.partitionBy(sym).orderBy(ts)` facade spec | ×1 | ×1 |

So the discriminator is the **null placement**, and a *bare* SQL `ORDER BY ts` in a window takes
DataFusion's NULLS LAST rather than Spark's NULLS FIRST — which is why it elides. Rows 1 and 3 of
the original table stand; row 2's "Spark default" label is true of the **facade `WindowSpec`**
(row 4, which does plan `nulls_first=true` and does not elide), not of the bare SQL spelling. The
PR-B conclusion is unchanged: the headline `Window.partitionBy(sym).orderBy(ts)` serving shape
still does not elide, and PR-C's analysis below is unaffected. Worth a look from the SE-1 owner:
the bare SQL-door window ordering may itself be a Spark-parity question separate from this door.

## Residue — the NULLS-placement gap (honest, and it bounds the win)

The engine declares **ASC NULLS LAST** (DataFusion's `ORDER BY` default). Spark's
`ORDER BY x ASC` is **NULLS FIRST**, and repark's `WindowSpec` follows Spark: ascending
window keys always plan `nulls_first=true`. For a *nullable* key those two orderings are
genuinely different, so DataFusion correctly declines the elision — which is why the table
above elides only when the query spells `NULLS LAST`. Measured corollary: registering the
same data with a **non-nullable** Arrow field DOES elide under the Spark-default window
(nulls cannot occur, so the placement is moot), but `createDataFrame` builds all-nullable
Arrow schemas, so that route is not reachable from the facade today.

So the door works and is pinned, but the headline serving shape
(`Window.partitionBy(sym).orderBy(ts)` over a nullable `ts`) does **not** yet elide. Closing
it is an engine decision, not a facade one, and there are two candidate shapes — declare both
null placements when the column is non-nullable-in-fact, or declare the ordering the way the
data actually is (a null-count check during the verification scan already touches every key
value). Neither is in this PR: PR-B's fence is facade-only.

## Non-goals carried forward

- **The cache door.** `cache()` / `persist()` register through `materialize_as_cache_view`
  and redirect the SAME handle in place, while `_source_view_name` keeps naming the original
  cdf view — so the source-frame gate alone does NOT catch a cached frame (SQM review
  finding on the first cut of this PR: declaring after cache silently un-pinned the cache
  while `is_cached` kept reporting true). The door now refuses explicitly when
  `_cache_view` / `_persist_requested` / `_checkpoint_lazy` is set: declare first, cache
  afterwards (both-orders pinned). Declaring a cache view itself: recorded, not attempted —
  the design scoped PR-B to createDataFrame.
- No session-wide "assume sorted" conf, no unverified fast path, no parquet/Iceberg ordering
  (all still PR-A's non-goals).

## Pins (`python/repark/tests/test_declare_sorted.py`, ~~11 nodes~~ **13 nodes** — Z-4, round 5)

> **Z-4 truth-up (round 5, counted firsthand):**
> `pytest python/repark/tests/test_declare_sorted.py --collect-only -q` → **13 tests collected**
> on this head. Round 4's critic line "'13 nodes' remediating to 'hint-mode' / 11" claimed a
> remediation that never landed in the count: the file has 13 collected nodes, and 11 was never
> re-measured. The heading above is corrected; the prose below describes the hint-mode family
> and is unchanged.

Results bit-identical declared vs undeclared (`to_arrow().equals`); the plan pin both ways
plus the `output_ordering=… ASC NULLS LAST` the scan now advertises; unsorted data refusing
loud with the row indices and the view still answering afterwards; transformed frames
(`filter` / `select` / `withColumn`) refusing with the source-frames message; no keys;
unknown name listing the available ones; case-insensitive resolution of capitalized fields;
snake and camel being one function; declaring twice being idempotent.

# PR-C — the null-placement gap: measured dead end, and the one lever that works (2026-08-17)

The PR-B residue chartered "declare both null placements when the verification scan proves
the keys null-free." Implemented and measured: **DataFusion 54.1 cannot consume the second
ordering.** `OrderingEquivalenceClass::get_options` returns the FIRST declared ordering whose
leading expression matches, and `options_compatible` is plain equality for a nullable column —
so a dual `[ASC NULLS LAST, ASC NULLS FIRST]` declaration leaves the second ordering
unreachable (SortExec stays ×1 under NULLS FIRST). Not a bug in our seam; an upstream
representation limit.

What DOES work, measured: tightening the re-registered MemTable's verified null-free key
fields to `nullable: false` — `options_compatible` then ignores placement, and ALL cells
elide including the facade `Window.partitionBy(sym).orderBy(ts)` serving shape (SortExec
1 → 0 in every cell, results identical). But that narrows the frame's REPORTED schema
(`df.schema`, `to_arrow()` — and an Iceberg write of a declared frame would emit `required`
fields), which falsifies the door's "planner hint, nothing else" contract on a
PySpark-visible surface. That is an OWNER decision, not an increment:

- (a) allow `declareSorted` to narrow verified-null-free key nullability, disclosed in the
  door docstring + guide + this ledger (schema-truth argument: the scan PROVED null-free);
  two PR-B pins re-anchor to value + non-key-type identity; Iceberg `required` consequence
  documented; or
- (b) keep the door a pure hint — the Spark-default-window gap stays open, marked
  closed-upstream-only (DataFusion would need multi-placement ordering equivalence).

**Default in force until the owner rules: (b)** — no schema change; the door remains exactly
as PR-B shipped it. The null-free detection built for the probe (`Array::null_count()` per
key per batch during verification) is the right input for (a) and drops in unchanged.

## Addendum 2026-08-17 (late) — EXPLAIN is not the executed plan; elision re-proven at execution

Orchestrator probes with a live PySpark 4.1.2 oracle (nullable-key window, values compared):

- **Parity: CORRECT.** Both repark doors execute windows and top-level sorts with Spark's
  NULLS FIRST semantics — byte-identical row numbering to PySpark. No divergence row needed.
- **Facade `EXPLAIN` renders the UNREWRITTEN plan.** A top-level `ORDER BY ts` that
  provably executes NULLS FIRST prints `SortExec: expr=[ts ASC NULLS LAST]` through
  `spark.sql("EXPLAIN …")`. The Spark-parity null-placement rewrite is applied at execution
  but not reflected in EXPLAIN output. Consequences: (a) the displayed plan is not the
  executed plan — a diagnostic-honesty bug, chartered for the next wave; (b) facade
  EXPLAIN-based sort-spelling pins (including this campaign's PR-B plan pins and the
  2026-08-17 addendum's four-cell table) are evidence about the EXPLAIN path, not
  necessarily the executed path. PR-A's Rust-level plan pins are unaffected.
- **The elision is real where it counts.** Execution-layer A/B, 1M rows × 4 symbols,
  tp=1, `SUM OVER (PARTITION BY sym ORDER BY ts ASC NULLS LAST)`, declared vs undeclared
  medians over 3 runs each: **1.20× faster declared**. The declaration is consumed by
  execution regardless of what EXPLAIN prints.
- Next-wave charter: locate the null-placement rewrite seam, make EXPLAIN show the
  executed plan (or document the gap loudly), and re-anchor the facade elision pins to
  execution-layer evidence (timing or engine-level plans).

# PR-D1 — tightenNulls (c+) (2026-08-17)

Owner locked (c+): default stays a pure hint; `tightenNulls=True` unlocks full elision;
Iceberg writes stay optional (CREATE refused this PR; exact relax is PR-D2).

- Engine: `declare_temp_view_sorted(..., tighten_nulls: bool)`. New logic in
  `sorted_view.rs` (`apply_declare_nullability`): every call restores prior tighten
  metadata first, then optionally flips verified-null-free **nullable** keys to
  non-nullable and tags them `repark.tighten_nulls=1`. Already-non-nullable keys are
  not tagged. A NULL in a declared key refuses naming the key and `tightenNulls`.
- Both SQL doors refuse Iceberg CREATE at CTAS derivation. **SQM F1 (2026-08-17):**
  tag-only detection on the *output* schema is not enough — DataFusion 54.1
  propagates non-nullability through computed expressions (`ts + 1 AS ts2`) while
  dropping field metadata, so a derived CTAS would persist a required Iceberg
  column. Detection is now **source-based**:
  - Engine: `refuse_iceberg_create_of_tightened_plan` walks every `TableScan` and
    refuses if the registered provider schema carries `repark.tighten_nulls`.
    Output-schema tag check remains as a belt.
  - Facade: `_tighten_derived` is set on a successful `tightenNulls=True` and
    copied by `_spawn`; `saveAsTable` create / `writeTo().create()` /
    `createOrReplace` / `replace` refuse on that marker **before** the temp-view
    re-registration hop.
  INSERT into an existing table stays allowed.

  **Correction (round 3, measured):** the temp-view re-registration hop does
  **not** drop field tags on DataFusion 54.1 `SELECT *` / Column refs. The
  earlier claim that the hop drops tags is struck. Tags *do* drop on computed
  expressions (the F1 class) and on cache/persist remint of those derived
  schemas (R-A). The facade marker remains defense-in-depth for writer paths
  that never hit a tagged scan (delete-the-layer pin).
- Facade: keyword-only `tightenNulls: bool = False` on both spellings. Docstring +
  `docs/guide/ta-guide.md` disclose the in-engine schema change. No parity-corpus row
  (no Spark twin). Orchestrator owns `docs/spark-sql-iceberg-parity.md` from the
  payload below.
- Rebuilds use `Schema::new_with_metadata` so top-level schema metadata survives
  tighten and hint-restore (SQM F2).
- Pins: existing hint-mode `test_declare_sorted.py` nodes untouched. Facade file
  `test_declare_sorted_tighten.py` (value+type on `to_arrow()` **and** `df.schema`
  — SQM F4; writer CREATE refuse on source and derived — SQM F3). Rust
  execution-layer serving-shape pin plus derived-expression CTAS refuse in
  `crates/repark-spark/tests/declared_sorted_tighten.rs`. ANSI twins in
  `crates/repark-sql/tests/declared_sorted_tighten.rs`. Schema-metadata pin in
  `crates/repark-core/tests/declared_sorted.rs`.

## Extension-registry payload (orchestrator writes)

- Surface: `DataFrame.declareSorted` / `declare_sorted` keyword `tightenNulls`
  (repark extension, not PySpark).
- Default `False`: planner hint, schema unchanged.
- `True`: after verify, verified-null-free keys report non-nullable in-engine;
  Iceberg CREATE refused until PR-D2; INSERT into existing tables allowed.
- Dual-door CREATE refuse: Spark CTAS + ANSI CTAS.

## Residue

- PR-D2: **do not** relax "exactly the tagged fields" — tags do not survive
  derivation (this PR's F1 evidence). Relax via the **same source walk** used
  by the CREATE refuse (orchestrator re-rules D2's charter at its Q&A). Until
  then the CREATE refuse stays.
- PR-D3: attach Spark ORDER BY rewrite to `DfStatement::Explain`; re-anchor the
  PR-B EXPLAIN pin.
- Accepted residual: parquet path-write round-trip laundering — the parquet
  writer enforces non-null physically, so provenance ends there. Documented in
  `docs/guide/ta-guide.md`; no code.

# PR-D1 round 3 — close the seams (2026-08-17)

SQM round-2 (33 agents) validated the source-based design and confirmed F1–F4
fixed. Round 3 is incremental: same architecture, three incomplete seams.

- **R-A:** `register_collected_memtable` (shared by cache / persist / checkpoint
  / createDataFrame remint) runs the source walk and stamps schema-level
  `repark.tighten_nulls=1` onto the new MemTable when any source is
  tighten-derived. Re-minted handles inherit detection.
- **R-B:** `refuse_iceberg_create_of_tightened_plan` uses
  `LogicalPlan::apply_with_subqueries` so a one-statement CTAS with the
  tightened view in a scalar/IN/EXISTS subquery refuses.
- **R-C:** `_spawn(*others)` ORs `_tighten_derived` across every parent;
  `union` / `unionByName` / `intersect` / `subtract` / `crossJoin` / `join`
  pass the right operand; `mapInArrow` (and `mapInPandas` through it) routes
  through `_spawn`.
- **R-D:** refuse iff (a source is tightened) AND (≥1 output field is
  non-nullable). Conservative class (literals / aggregates over a tightened
  source) stays refused and is documented. All-nullable projection CREATE and
  INSERT/append stay allowed (allowed-side pin).
- Export: `repark.tighten_nulls` is stripped at the Arrow export boundary
  (`_strip_internal_tighten_metadata` on `to_arrow` / `to_arrow_batches`).
- Pins added to discriminate: delete-the-facade-layer mutant (saveAsTable /
  writeTo create + createOrReplace); right-side combinators; cache remint;
  R-D allowed + conservative; `df.schema` type-exactness; executed docstring
  examples.
- Critic remediations (round-3 ACC): schema-level stamp even when no field
  flipped (L-004); remint also field-tags untagged non-null outputs so hint
  restore cannot leave required untagged columns (L-001); R-D walks nested
  Arrow types (L-002). Walk follows `TableSource::get_logical_plan` so a
  lazy `into_view` / `createOrReplaceTempView` hop cannot hide the tightened
  `MemTable` (Q-001 / L-1); `PolarsFrame.join` ORs the marker (L-2).
  **Correction (Q-002):** an earlier draft kept the remint schema-stamp across
  hint restore and over-refused an all-nullable restored remint. Restore now
  removes field tags **and** the schema stamp; remint field-tags computed
  non-nulls (including nested struct children / list items / map values) so
  those fields can unflip (L-001 / C1-L-001). Skip-by-name remint tagging
  was rejected (C2-Q-001): `ts + 1 AS symbol` would stay required. Remint
  tags every reminted required field; remint+hint may widen originally-
  required columns (conservative for Iceberg). Pins:
  `remint_hint_restore_does_not_leave_required_untagged_fields`,
  `remint_hint_unflips_name_colliding_computed_column`,
  `remint_hint_restore_unflips_nested_required_child`.
- Walk visit budget is 4096 inner-plan visits with a **generic** overflow
  error (never a `tightenNulls` CREATE refusal) — C1-Q-001. Pin:
  `wide_lazy_view_union_without_tighten_is_not_refused`.
- SAF-001 remediating: walk also recurses `TableSource::get_logical_plan`
  (lazy `into_view` / ViewTable). Pin:
  `lazy_view_of_derived_plan_is_visible_to_the_create_walk`.
- Residual (L-003 + parquet, accepted — SQM-ruled): provenance ends at any
  plan-less remint that keeps Arrow `nullable: false` and drops tags. Named
  members: parquet path-write (physically required); `to_arrow()` /
  `register_record_batches_as_temp_view` / `register_arrow_stream_as_temp_view`;
  `mapInArrow` remint (declared schema is typically optional; SQL after
  register is untagged). Facade CREATE on the still-marked handle refuses.
  R-A closes collect-once remints only (`register_collected_memtable`).
  `session.rs` is at its 1650 ceiling — no third remint door this round.

## Pre-PR critic report (/repark-harden)

Engine: critic-octo N=3 × CCC findings-only → fix — tier high
(`session.rs` / CREATE refuse / remint in the diff). Actor phase was
R-A..R-D; octo attacked the post-seam tree.

Critic-1 (quality/parity): attacked crates contract, pin discrimination,
two-doors, CLOSED surfaces, walk budget, remint tagging. Cycle-1 S1
C1-Q-001 (64-visit width cap + `tightenNulls` lie) remediating (4096
generic overflow + 65-wide UNION pin). Cycle-2 S1 C2-Q-001 (skip-by-name
CREATE leak on `ts+1 AS symbol`) remediating (skip removed; remint tags
every reminted required field). Cycle-3 S0/S1 CLEAN; leftover S2s
(Python map strip rebuild, dead skip param, Python strip depth) remediating.
Null-report: unwrap/expect, session.rs 1650, CLOSED set
(`function_dispatch.rs` / facade `functions*.py` / `repark-ta`).

Critic-2 (security/safety): attacked CREATE bypass, remint atomicity,
INSERT over-refusal, export leak, walk DoS. C1-SAF-001 remediating with
Q-001. C2-SAF-001 (unbounded remint/restore/strip) remediating (depth 32
+ deep-struct pin). C1-SAF-002 `replace_view` two-step ACCEPTED_FLAGGED
S3 (pre-existing). Cycle-3 CLEAN. Null-report: mid-collect cannot leave
a half-stamped view; no secrets; no CLOSED writes.

Critic-3 (logic): C1-L-001 nested remint+hint remediating (recursive
tag/restore + pin). C2 skip-by-name leftovers remediating by deletion.
Originally-required widen on remint+hint ACCEPTED conservative (Iceberg
writes stay nullable). Cycle-3 CLEAN — no new CREATE allow/refuse invert.

Critic-4 (claims): Q-002 keep-stamp dual-home remediating (restore removes
stamp). EXISTS-not-scalar comment remediating. Serving-shape docstring
remediating. Literal pin docstring remediating. ~~"13 nodes" remediating
to "hint-mode" / 11.~~ **(STRUCK, Z-4: the file still collects 13 nodes —
measured round 5; see the Pins heading.)** Cycle-3 CLEAN. CL-IDENTITY: `%ae`/`%ce` byte-exact
`64240326+TRO-Wolf@users.noreply.github.com` on all three branch commits.

Signature table: n/a (no PySpark function wrap).
Oracle probes: n/a (no Spark twin; tighten is a repark extension).
Pin audit: delete-facade-layer (saveAsTable + writeTo create +
createOrReplace) live; right-side combinators live; cache remint live
(SQL half isolates remint); EXISTS + lazy view live; R-D allowed +
INSERT live; remint+hint restore + name-colliding computed + nested
child + 65-wide UNION + deep-struct strip live. Accepted residual pins
do not claim to kill parquet remint.

## Finder-battery report

Target: `origin/main (b628b0f)..working tree` | dimensions: 6 then quiet-1
(3 dims) | findings: 20+ raw → ~12 deduped
Survivors (0 S0/S1 after 3-vote):
  (none)
Refuted (S1 candidates, 3-vote):
  C-stream batch.schema leak — 3× REFUTED (declared stream schema is
  stripped; FFI rebuilds batches under that schema)
  ListView required-item persist — persist REFUTED (fork conversion
  fail-closed); refuse-message gap only
  depth-33 remint+hint CREATE — 2× REFUTED as hostile/non-shipping
  (facade cannot hint after cache)
Null attestations: R-A/R-B/R-C engine wiring; INSERT allowed; CLOSED
surfaces; two-doors CTAS twins; removed-behavior R-D all-nullable skip
is the ruled refinement.
Quiet-1: CLEAN (wiring / pins / CREATE domain) — 0 new S0/S1.
Two consecutive quiet: first battery 0 S0/S1 survivors + quiet-1.
Verdict: CLEAN on S0/S1 (not a substitute for gates).

Convergence: **OCTO-CONVERGED** (3 cycles; cycle 3 S0/S1 CLEAN;
cycle-3 leftover S2s remediating). `make verify` 0 on the remediating
tree; `make preflight` 0 on the seam tree before the last critic
polish (`preflight_exit=0`, 3343 passed / 70 skipped).

# PR-D1 round 4 — post-rebase integration + the Y battery (2026-08-17)

Branch rebased onto merged `main` (which carries the DF-2 `dynamicFlatten` unit). Round 4 is
an Actor–Critic fix round over a tier-2 review battery (Y-1..Y-8) plus one integration defect
found at the rebase (CEIL-1). No architecture change: the source-walk design from round 3
stands; this round closes two real bypasses, makes three pin claims honest, runs two NOT-RUN
verifiers, and pays back a file-size ceiling the rebase overran.

**Every table below is MEASURED on this tree** (mutant in place vs fixed). BASE-of-round =
`fe742a6` as checked out. Struck claims are struck visibly.

## CEIL-1 — `core.py` overran the lib-py ceiling at the rebase

D1 and DF-2 each fit the 7350-line ceiling alone; together `core.py` measured **7380**. Fixed
by a MOVE-ONLY extract on the T0b precedent — the six remaining module-level tail helpers
(`_is_native_pure_global_aggregate`, `_parse_count_distinct_simple_names`,
`_global_agg_sql_parts`, `_pandas_udf_window_frame_bounds`, `_reject_partition_transform`,
`_reject_aggregate_in_with_column`) moved VERBATIM to `plan_collapse.py`. Not one line of
their bodies changed; `core.py` re-exports all six from its existing hand-ordered tail bind
block, so `joins_columns.py`'s `from …core import _global_agg_sql_parts` and every other
import path is unchanged (Q7 freeze).

| | before | after (at 6b08081) | **at the composed head (675a413), round-5 measurement** |
|---|---|---|---|
| `core.py` | 7380 (**RED**, ceiling 7350) | ~~7257~~ | **7250** |
| `plan_collapse.py` | 1237 | ~~1373~~ | **1432** |
| ceiling | 7350 | 7350 — not raised | 7350 — not raised |

**Z-5 truth-up (round 5):** the "after" column above was measured on commit `6b08081`, not on
the composed head. `wc -l` on `675a413` as checked out reports `core.py` **7250** and
`plan_collapse.py` **1432** — the octo remediation commit moved more. The conclusion (ceiling
held, not raised) is unchanged; the numbers are.

`make check-lib-py`: `lib-py: 67 files clean (ceilings held; no-stub rule held)`.

## Y-3 / Y-4 — the DDL-sink bypass (S2 behavior, both doors)

**CONFIRMED on BASE.** `CREATE VIEW ice.ns.v AS SELECT * FROM tight LIMIT 0` and
`SELECT * INTO ice.ns.t FROM tight LIMIT 0` both fell into the routers' catch-all
(`_ => execute_passthrough` on the Spark door, `_ => delegate` on the ANSI door), so the CTAS
tighten refuse never saw them. Measured on BASE via a scratch probe:

| statement (BASE) | result | what the sink left behind |
|---|---|---|
| `CREATE VIEW ice.sales.v AS SELECT * FROM tight LIMIT 0` | **Ok** | `SELECT * FROM ice.sales.v` → `symbol` / `ts` **non-nullable**, fields carry `PARQUET:field_id` |
| `SELECT * INTO ice.sales.t FROM tight LIMIT 0` | **Ok** | `SELECT * FROM ice.sales.t` → same, required `symbol` / `ts` |
| `CREATE VIEW ice.sales.vp AS SELECT * FROM plain LIMIT 0` (untightened) | **Ok** | table persisted — see payload finding below |

**Fix.** New public `repark_core::refuse_iceberg_create_of_tightened_ddl(plan, catalogs)`
(`sorted_view.rs`) matches `LogicalPlan::Ddl(CreateView | CreateMemoryTable)`, requires the
target to be a three-part name in a **registered Iceberg catalog**, and applies the existing
R-D predicate to the DDL body. Wired next to each door's SEC-02 plan guard:
`repark_spark::spark_ast::execute_passthrough` and `repark_sql::router::delegate`. NOT a
blanket refuse — a session-scoped one-part `CREATE VIEW` / `SELECT INTO` persists nothing and
stays allowed (the round-3 lazy-view pins depend on that).

MEASURED mutants:

| mutant | Spark-door pins | ANSI-door pins | facade pins |
|---|---|---|---|
| fixed tree | green | green | green |
| delete the `refuse_…_ddl` call in that door | `create_view_…_refuses` **RED**, `select_into_…_refuses` **RED** | (door-local) | (door-local) |
| delete the `refuse_…_ddl` call in `router::delegate` | — | `ansi_create_view_…` **RED**, `ansi_select_into_…` **RED** | — |
| keep only the `CreateView` arm (drop `CreateMemoryTable`) | `select_into_…_refuses` **RED**, `create_view_…` green | — | — |
| stale (pre-fix) native module, fixed facade tests | — | — | `test_create_view_into_catalog_…` **DID NOT RAISE**, `test_select_into_catalog_…` **DID NOT RAISE** |

The `CreateView`-only mutant is the one that proves Y-3 and Y-4 are independent statements,
not one finding written twice.

**PAYLOAD FINDING (predates this branch, NOT fixed here).** `CREATE VIEW cat.ns.v AS …`
against a registered Iceberg catalog persists a **format-v2 Iceberg TABLE**, not a view —
measured on BASE with an *untightened* source (`plain`), where nothing about `tightenNulls` is
in play. This round deliberately fixes only the tighten leak; the untightened statement behaves
exactly as it did on BASE and is pinned that way on both doors
(`*_create_view_in_iceberg_catalog_over_untightened_source_stays_allowed`) so a later fix to the
payload class has to move a pin rather than land silently.

## Y-1 — the S1 pin-honesty finding (relabelled, no facade-only discriminator exists)

`test_literal_over_tightened_source_is_refused` claimed *"Kills: dropping the facade R-D half"*.
MEASURED both directions:

| mutant | this node |
|---|---|
| facade `_refuse_tightened_iceberg_create` no-oped | **green** (the engine source walk refuses) |
| engine `refuse_iceberg_create_of_tightened_plan` no-oped | **green** (the facade marker refuses) |

~~Kills: dropping the facade R-D half.~~ **Struck.** Two independent layers see that statement
(a tightened `MemTable` scan *and* the `_tighten_derived` marker with a non-nullable `lit(1)`
output), so no single-layer mutant reaches it. A genuinely facade-only discriminator needs a
write plan with the marker but **no** tagged scan; that shape exists and is already pinned by
`test_facade_layer_refuses_when_engine_source_walk_is_silent` — the only node the facade no-op
killed. The literal node was relabelled in place as a belt-and-suspenders end-to-end guard with
the measurement in its docstring (DF-2 V-1 relabel precedent).

## Y-2 — the `get_logical_plan` recurse was dead under every pin

MEASURED: deleting the `TableSource::get_logical_plan` recurse in ~~`collect_tighten_sources`~~
(**Z-5:** that walk was rewritten by the octo integration commit — `collect_tighten_sources` is
now a two-line adapter over the ITERATIVE, visit-budgeted `walk_tighten_sources` /
`visit_tighten_sources` pair, which is where the follow actually lives)
left **all four** Q-001 lazy-view pins green — `lazy_view_of_derived_plan_is_visible_to_the_create_walk`
(core), `iceberg_create_from_lazy_view_of_derived_plan_refuses` (Spark),
`ansi_ctas_from_lazy_view_of_derived_plan_refuses` (ANSI),
`test_sql_derived_write_and_lazy_view_create_refuse` (facade). Cause: DataFusion 54.1
`LogicalPlanBuilder::scan` **inlines** a source that has a logical plan
(datafusion-expr `builder.rs` L517-539), so every SQL-door `SELECT * FROM <view>` puts the
tightened `MemTable`'s `TableScan` directly in the outer plan.

The recurse is **not** unreachable code: the same function skips the inline when
`table_scan.filters` is non-empty, leaving a real `TableScan` whose source still carries the
view's plan. New pin
`filtered_scan_of_a_view_source_exercises_the_get_logical_plan_recurse` builds exactly that
shape through the public `LogicalPlanBuilder` / `ViewTable` API against the public
`refuse_iceberg_create_of_tightened_plan` entry point, and asserts the shape (retained
`TableScan`, source still carrying a plan) before asserting the behavior.

| mutant | new pin | the four old pins |
|---|---|---|
| recurse deleted | **RED** | all green |
| fixed | green | green |

Recorded honestly: no SQL-door *statement* reaches that branch on DF 54.1. The pin makes the
branch live so a DataFusion release that stops inlining, or a future filter-carrying scan,
cannot silently reopen the hole.

## Y-5 — prose drift (S3)

`iceberg_create_from_subquery_over_tightened_source_refuses` (Spark door) said
*"scalar-subquery"*; its SQL is and always was `WHERE EXISTS (SELECT 1 FROM tight)`. Corrected.
Twins checked: the core node says "expression-subquery scans", the ANSI node says
"expression-subquery scans on the ANSI door" — both already accurate, no change. (Round 3's
"CL-003 EXISTS not scalar remediating" line landed on two of the three doors.)

## Y-6 — the export-strip claim (extended, not just narrowed)

`export_strip_drops_tighten_tags_and_keeps_non_nullability` claimed it killed
*"leaking repark.tighten_nulls into user-visible to_arrow()/df.schema export"* while unit-testing
only the helper. MEASURED, three ways:

| mutant | core unit node | facade `to_arrow()` metadata asserts | new facade export node |
|---|---|---|---|
| facade `_strip_internal_tighten_metadata` no-oped | green | **green** | green |
| Rust `strip_tighten_export_metadata` no-oped | **RED** | **green** | **RED** |
| fixed | green | green | green |

Two facts fell out. (a) The `to_arrow()` metadata assertions are **non-discriminating**: with
both strips no-oped the collected schema's field metadata is already empty — DataFusion drops
field metadata across physical execution. (b) The Rust helper *is* load-bearing, but for the
binding surface `PyDataFrame::analyzed_arrow_schema` (`crates/repark-python/src/dataframe.rs`),
which with the helper no-oped reported `{b'repark.tighten_nulls': b'1'}` on both keys — probed
directly. So coverage was **extended**: new facade node
`test_analyzed_schema_export_carries_no_tighten_tag` pins that boundary and is the node the
helper mutant kills there. The core node's `Kills:` line now states its unit scope and strikes
the old export claim; the `to_arrow()` asserts carry an inline MEASURED note.

## Y-7 — verifier P-3 (was NOT-RUN): the cache `saveAsTable` cell

RUN. Mutant: `apply_tighten_provenance_on_materialize` returns the schema/batches unstamped
(R-A deleted).

| node | under the R-A mutant |
|---|---|
| `materialize_of_derived_plan_restamps_tighten_provenance` (core) | **RED** |
| `remint_hint_restore_does_not_leave_required_untagged_fields` (core) | **RED** |
| `iceberg_create_from_cached_derived_frame_refuses` (Spark door) | **RED** |
| `ansi_ctas_from_cached_derived_frame_refuses` (ANSI door) | **RED** |
| `test_cache_of_derived_still_refuses_iceberg_create` — `cached.write.saveAsTable(…)` half | **still refuses** (facade marker survives cache) |
| `test_cache_of_derived_still_refuses_iceberg_create` — SQL CTAS half | **DID NOT RAISE** ⇒ the discriminator |

**Verdict: the cell is genuinely green, with a correction to what it proves.** The
`saveAsTable` statement specifically is guarded by the FACADE `_tighten_derived` marker, not by
the engine remint; the engine R-A half is discriminated by the SQL half of that same node and by
the two Rust twins. The facade node's docstring now carries this table.

## Y-8 — verifier P-5 (was NOT-RUN): List / Map CHILD requiredness

RUN. Question: does `field_or_child_is_non_nullable` see a required child inside List/Map
element fields? **Yes** — measured through the public
`refuse_iceberg_create_of_tightened_schema` entry point (the helper is crate-private), and
pinned by `list_and_map_child_requiredness_is_seen_by_the_r_d_output_walk`:

| shape (outer field nullable, schema tighten-derived) | verdict |
|---|---|
| `List<item: Int64 NOT NULL>` | refuses |
| `LargeList<item: Int64 NOT NULL>` | refuses |
| `FixedSizeList<item: Int64 NOT NULL, 2>` | refuses |
| `Map<key NOT NULL, value: Int64 NOT NULL>` | refuses |
| `Map<key NOT NULL, value: Int64 NULL>` | **allowed** (accepted scope) |
| `List<item: Int64 NULL>` | **allowed** |

The two Map rows together pin the accepted scope deliberately: Iceberg map KEYS are
spec-required, so only a required VALUE persists a nested required Iceberg field. Mutants:
deleting the List/LargeList/FixedSizeList arm → **RED**; deleting the Map arm → **RED**; fixed
→ green. No walk fix was needed — P-5's suspicion is refuted with measurement, and the scope is
now pinned instead of assumed.

## Round-4 test counts (all green on the fixed tree)

| gate | result |
|---|---|
| `cargo test -p repark-core -p repark-spark -p repark-sql` | ~~**1023 passed + 1 doc-test**~~ — **Z-5: 1028 passed on the composed head** (repark-core lib 132 + `declared_sorted` **30**, not 26 — both counted firsthand on `675a413`); round 4's row describes `6b08081`, not `675a413` |
| `pytest test_declare_sorted_tighten.py test_dynamic_flatten.py` | ~~**45 passed** (tighten file 17 nodes)~~ — **Z-5: 46 passed on the composed head, tighten file 18 nodes** (the octo commit added one); round 4's row was measured before composition |
| `make check-lib-py` | `lib-py: 67 files clean (ceilings held; no-stub rule held)` |
| whole facade suite (CEIL-1 move-only regression check) | **3354 passed, 70 skipped** |
| `make rust-clippy` / `make rust-fmt-check` / `make py-lint` / `make py-format-check` | clean |
| `make check-manifest` / `make check-rust-file-size` | clean |

New nodes this round: 2 core (`filtered_scan_of_a_view_source_exercises_the_get_logical_plan_recurse`,
`list_and_map_child_requiredness_is_seen_by_the_r_d_output_walk`), 4 Spark-door, 4 ANSI-door,
4 facade. Three existing nodes were relabelled in place (Y-1, Y-5, Y-6/Y-7 docstrings) — no
test NAME changed, so the relocation-discipline identity gate is untouched.

## NOT-RUN after round 4

- `make preflight` (orchestrator runs it after this round, by instruction).
- The parity-live / tier-2 live-oracle tier (needs a JVM + the env arm; unchanged by this round
  — `tightenNulls` is a repark extension with no PySpark twin, so no parity-corpus row).
- The payload class "`CREATE VIEW` in an Iceberg catalog persists a TABLE" is measured and
  recorded above but deliberately NOT fixed here.

---

# Round 5 (SQM r5) — the altitude fix: one pre-execute belt, one choke point

BASE-of-this-round = **`675a413`** as checked out (the composed head). **Every table below is
MEASURED on this tree** — each mutant applied in place, the named pins re-run, then reverted.
Anything not run is listed under "NOT-RUN after round 5". Struck claims are struck visibly.

## Z-2 (S1) — the native door had no guard at all, and per-door wiring is why

`DataFusionDialect::execute` was `cx.ctx.sql(query)`. That is the door behind
~~`ReparkSession::sql` / `session.context().sql`~~ → **`ReparkSession::sql` only** (round-6
R6-2 correction: the belt closed `ReparkSession::sql` on `DataFusionDialect`; it does **not**
and cannot close `session.context().sql`, which is the raw DataFusion context and bypasses
every product guard — see "R6-2" in round 6 below) — every `repark-core` embedder — and rounds 3
and 4 wired the tighten refuses into the Spark door and then the ANSI door, twice missing it.

MEASURED on BASE (`675a413`), native door, tightened temp view `tight` in an `ice` memory
catalog with namespace `ice.sales`:

| statement (native door, BASE) | result | persisted? |
|---|---|---|
| `CREATE VIEW ice.sales.v_limit AS SELECT * FROM tight LIMIT 0` | **Ok** | `table_exists` **true**, columns `symbol`/`ts`/`close` all `required` |
| `CREATE VIEW ice.sales.v_false AS SELECT * FROM tight WHERE false` | **Ok** | idem |
| `SELECT * INTO ice.sales.t_limit FROM tight LIMIT 0` | **Ok** | idem |
| `SELECT * INTO ice.sales.t_false FROM tight WHERE false` | **Ok** | idem |

**Fix — the altitude, not the site.** New `crates/repark-core/src/pre_execute.rs`:
`PreExecute` = `plan` (`create_logical_plan`, no execution) → `guard` (the ONE choke point for
pre-execute refusals) → `execute`, plus `run` = all three. Every door now passes its planned
statement through `guard`:

| door | belt use |
|---|---|
| native (`DataFusionDialect::execute`) | `PreExecute::run` |
| ANSI (`repark_sql::router::delegate`) | `plan` → SEC-02 local-fs guard (door-specific) → `guard` → `execute` |
| Spark (`repark_spark::spark_ast::execute_passthrough`) | `guard` on the planned statement |
| ANSI CTAS derivation (`repark_sql::create_table::derive_ctas_query`) | `plan` → `guard` → tighten source-walk refuse → `execute` |

`refuse_iceberg_create_of_tightened_ddl` is no longer called from any door directly.

| mutant | native pins | Spark-door pins | ANSI-door pins |
|---|---|---|---|
| BASE `675a413` (no belt) | **RED** ×2 | green (round-4 wiring) | green (round-4 wiring) |
| `PreExecute::run`'s `guard` deleted | **RED** ×2 | green | green |
| `spark_ast`'s `guard` deleted | green | **RED** ×4 | green |
| `router::delegate`'s `guard` deleted | green | green | **RED** ×3 |
| fixed | green | green | green |

Belt unit pins (`crates/repark-core/src/pre_execute/tests.rs`): `plan` does not publish a
`SELECT … INTO` target while `SessionContext::sql` does (the contrast half — this is the
property that lets a guard sit between planning and execution), `run` returns rows, `guard` is
a no-op for a plain SELECT.

## Z-1 (S1) — the gate was the SPELLING, not the resolved catalog

`refuse_iceberg_create_of_tightened_ddl` returned `Ok` for any name that was not
`TableReference::Full`. DataFusion resolves a Bare/Partial name against
`datafusion.catalog.default_catalog` / `default_schema`, so a `SET` moves one- and two-part DDL
straight into the Iceberg catalog.

MEASURED on BASE, after `SET datafusion.catalog.default_catalog = 'ice'` +
`SET datafusion.catalog.default_schema = 'sales'` (source named as `datafusion.public.tight`
because the SET moves default resolution away from the temp-view schema):

| statement (BASE) | native | ANSI | Spark |
|---|---|---|---|
| `CREATE VIEW v_bare AS SELECT * FROM datafusion.public.tight LIMIT 0` | **Ok** — persisted as `ice.sales.v_bare`, all columns `required` | **Ok** | **Ok** |
| `SELECT * INTO t_bare FROM datafusion.public.tight LIMIT 0` | **Ok** | **Ok** | **Ok** |
| `CREATE VIEW sales.v_partial AS …` (two-part) | **Ok** | **Ok** | **Ok** |
| `CREATE VIEW ice.sales.v_full AS …` (three-part) | refuses | refuses | refuses |

**Fix:** resolve the planned target with `TableReference::resolve` against the session's
`config_options().catalog` defaults, then look the RESOLVED catalog up in the registry. The
function now takes the `SessionContext` (supplied by the belt).

| mutant | Z-1 pins (3 doors) | session-scoped allowed pins |
|---|---|---|
| BASE (gate on `TableReference::Full`) | **RED** on all three doors | green |
| fixed | green | green (`default_catalog_pointing_away_from_iceberg_keeps_session_ddl_allowed`: with the default catalog left at `datafusion`, a bare `CREATE VIEW` still persists nothing and stays allowed) |

## Z-3 (S2) — the ANSI CTAS derivation executed the body before refusing

`create_table.rs` derived the CTAS schema with `cx.ctx.sql(&query.to_string())`, which plans
**and executes**.

MEASURED on BASE, ANSI door,
`CREATE TABLE ice.sales.wrap AS SELECT * INTO ice.sales.wrap_inner FROM tight LIMIT 0`:

| observable (BASE) | value |
|---|---|
| statement result | ~~"errors after publishing"~~ → **`Ok(frame)`** — it did not refuse at all |
| `ice.sales.wrap_inner` exists | **true**, schema `[(symbol, required), (ts, required)]` |
| `ice.sales.wrap` exists | **true** |

Root cause, measured: the eagerly-executed inner DDL hands back its **own** (empty) result, so
the next line's `refuse_iceberg_create_of_tightened_plan` walked an `EmptyRelation` and saw no
tightened source. The brief's "publishes x BEFORE the next-line refuse sees the plan" understates
it — the refuse never fired.

**Fix:** `derive_ctas_query` plans without executing, runs `belt.guard` and the tighten
source-walk refuse on that plan, then executes.

| mutant | `ansi_ctas_wrapping_a_ddl_sink_…` | `ansi_plain_ctas_still_derives_and_writes` | the 6 older ANSI CTAS pins |
|---|---|---|---|
| BASE / derivation back to eager `ctx.sql` | **RED** | green | green |
| `belt.guard` deleted from `derive_ctas_query` only | green (see below) | green | green |
| fixed | green | green | green |

**Honest note on that middle row:** with plan-before-execute in place, the *existing* source-walk
refuse already fires on the planned `CreateMemoryTable` (its input carries the tightened scan),
so deleting `belt.guard` here alone is **not discriminating**. The discriminating mutant is the
eager `ctx.sql` revert. `belt.guard` stays as the choke-point invariant (one door, one call),
recorded as defense-in-depth rather than claimed as pinned-by-deletion.

The Spark door reached the opposite outcome on BASE — that wrap already refused, because its
CTAS body is planned through `execute_passthrough`, which carries round 4's DDL refuse. Both
doors are now pinned to the same outcome (refuse + inner table NOT published).

## Z-6 (S2, verify-or-fix) — the walk's visit-budget overflow: RUN, and the reachability measured

Question: can the `MAX_VIEW_VISITS = 4096` overflow arm be reached, and is its error generic?

MEASURED, three shapes:

| shape | inner-plan visits | outcome |
|---|---|---|
| 5 stacked 8-way `UNION ALL` lazy views (SQL), untightened | **0** | no overflow — SQL planning INLINES a `ViewTable` as `SubqueryAlias` (plan printed and inspected) |
| 4100-deep `into_view` chain reached via `ctx.table(name)` | **0** | no overflow — `ctx.table` inlines identically (`SubqueryAlias: hop2 / hop1 / hop0 / TableScan: plain`) |
| 4100 retained `TableScan`s (`LogicalPlanBuilder::scan_with_filters` over `ViewTable`s) | 4100 | **overflow**, `Error::Analysis` naming "view-visit budget", message does **not** contain `tightenNulls` |

So the budget is reachable exactly where the `get_logical_plan` follow is live — the
non-inlined, filter-carrying scan shape that round 4's Y-2 pin already identified — and is
unreachable from any SQL statement on DF 54.1. Both sides are now pinned:
`view_visit_budget_overflow_is_a_generic_error_not_a_tighten_refusal` (overflow, generic
message, no `tightenNulls`) and `view_hop_chain_under_the_visit_budget_still_walks_clean`
(64 hops walk clean — the budget is a safety net, not a feature). No code change was needed;
the verifier is closed with measurement plus pins.

## Z-7 (S2, verify-or-fix) — nested export strip: FixedSizeList and every other walked container

RUN. The three nested walkers — the tagger (`remint_annotate_field_at`), the detector
(`field_or_descendant_is_tagged`), and the strip (`strip_field_export_at`) — were read side by
side and exercised. MEASURED through the public `strip_tighten_export_metadata` /
`schema_is_tighten_derived` pair:

| container (tag on the nested leaf) | detector sees it | strip removes it | requiredness preserved |
|---|---|---|---|
| `FixedSizeList<item NOT NULL, 3>` | yes | yes | yes |
| `List<item NOT NULL>` | yes | yes | yes |
| `LargeList<item NOT NULL>` | yes | yes | yes |
| `Struct<child NOT NULL>` | yes | yes | yes |
| `Map<key, value NOT NULL>` (VALUE) | yes | yes | yes |

**Measured scope, recorded rather than assumed:** Union, Dictionary, RunEndEncoded and the
`*View` list types are walked by **none** of the three, so nothing can be tagged inside one and
the tagger/detector/strip stay symmetric — no export leak, and no refusal gap beyond the one
Y-8 already recorded. Pin: `nested_export_strip_covers_every_container_the_tagger_walks`
(asserts detector-sees → strip-removes → requiredness unchanged, per container). No code change
was needed.

## Z-4 (S3) — the "13 → 11" node-count claim was false

Counted firsthand on this head:
`pytest python/repark/tests/test_declare_sorted.py --collect-only -q` → **13 tests collected**.
Round 4's critic line claimed that count was remediated to 11; it was not. The Pins heading and
the critic line are corrected in place, both struck visibly. (Round 5 adds no node to that file.)

## Z-5 (S3) — the round-4 MEASURED tables described `6b08081`, not the composed head

Re-measured on `675a413` as checked out:

| claim (round 4) | round-4 value | **measured on `675a413`** |
|---|---|---|
| `core.py` lines after CEIL-1 | ~~7257~~ | **7250** |
| `plan_collapse.py` lines | ~~1373~~ | **1432** |
| `cargo test -p repark-core -p repark-spark -p repark-sql` | ~~1023 + 1 doc-test~~ | **1028 passed** |
| `declared_sorted` nodes | ~~26~~ | **30** |
| `pytest test_declare_sorted_tighten.py test_dynamic_flatten.py` | ~~45 (tighten 17)~~ | **46 (tighten 18)** |
| Y-2 prose names `collect_tighten_sources` as the walk | — | that name is now a two-line adapter; the follow lives in the iterative `walk_tighten_sources` / `visit_tighten_sources` pair (prose corrected in place) |

Conclusions of round 4 are unchanged (the ceiling held, the mutants were real); the numbers and
the one function name were not true to the composed head and now are.

## Round-5 test counts (all green on the fixed tree)

| gate | result |
|---|---|
| `cargo test -p repark-core -p repark-spark -p repark-sql` | **1043 passed** (the total includes the 1 doc-test), 0 failed, 0 ignored, across 25 test binaries (repark-core lib **135** + `declared_sorted` **37**; repark-spark lib 473; repark-sql lib 251) — BASE was 1028 |
| `pytest python/repark/tests/test_declare_sorted_tighten.py` | **18 passed** (unchanged — round 5 adds no facade node) |
| `pytest test_declare_sorted_tighten.py test_dynamic_flatten.py` | **46 passed** |
| `pytest python/repark/tests/test_declare_sorted.py` | **13 passed** (the Z-4 count) |
| `make check-lib-py` (the lib-py guard step) | `lib-py: 67 files clean (ceilings held; no-stub rule held)` |
| `make rust-clippy` / `make rust-fmt-check` | clean |
| `make check-rust-file-size` / `make check-lib-rs` / `make check-crate-dag` / `make rust-panic-ban` / `make check-manifest` | clean |

New nodes this round: **7 core** (4 native-door/resolved-catalog, 2 visit-budget, 1 nested-strip)
+ **3 belt unit** (`pre_execute/tests.rs`) + **3 ANSI-door** + **2 Spark-door** = 15. No existing
test NAME changed (relocation-discipline identity gate untouched).

## NOT-RUN after round 5

- `make preflight` (the orchestrator runs it after this round, by instruction) — and with it
  `py-test-facade`, the audit and the workflow lint.
- The whole-facade pytest suite (round 4's 3354/70 regression sweep): round 5 changes no Python
  file, and the facade rides the Spark door whose pins are green. Not re-run.
- The parity-live / tier-2 live-oracle tier (needs a JVM + the env arm): unchanged —
  `tightenNulls` is a repark extension with no PySpark twin.
- Facade-level pins for Z-1/Z-2: deliberately none. The facade reaches the engine through the
  Spark door only (the native `DataFusionDialect` door is a Rust-embedder surface), and the
  Spark door's Z-1 behaviour is pinned in Rust. Recorded as a scope decision, not an oversight.
- The payload class "`CREATE VIEW` in an Iceberg catalog persists a TABLE at all" — measured in
  round 4, still deliberately NOT fixed here. Its allowed-side pins (untightened `CREATE VIEW`
  stays Ok) remain green on all three doors.
- One measured boundary worth naming: the Iceberg schema provider's `register_table` refuses a
  NON-EMPTY body ("register_table does not support tables with data"), which is why the whole
  DDL-sink family is pinned on `LIMIT 0` / `WHERE false` bodies. Measured this round, recorded.

---

# Round 6 (SQM r6) — the temp-view API was a third write door

BASE-of-this-round = **`68e98f4`** as checked out. **Every table below is MEASURED on this tree.**
Anything not run is listed under "NOT-RUN after round 6". Struck claims are struck visibly.

## R6-1 (S1) — `createOrReplaceTempView` registered INTO the Iceberg catalog

`ReparkSession::create_or_replace_temp_view_from` (and every sibling) forwarded the caller's raw
name to `replace_view` → `SessionContext::register_table`, which parses a `&str` into a
`TableReference` and resolves it. Two consequences, both measured, neither of which any guard
could see — **no statement is ever planned on this path**, so the round-5 `PreExecute` belt is
structurally out of the picture.

MEASURED on BASE (`68e98f4`), memory catalog `ice` + namespace `ice.sales`, tightened temp view
`tight` (`declare_temp_view_sorted(..., tighten_nulls = true)`):

| call | BASE result | BASE `table_exists` |
|---|---|---|
| `register_record_batches_as_temp_view("ice.sales.vempty", <tightened schema>, [])` | **Ok** | **true** |
| `create_or_replace_temp_view_from("ice.sales.vlazy", <tightened `LIMIT 0` frame>)` | **Ok** | **true** |
| `materialize_dataframe_as_temp_view("ice.sales.vmat", <tightened `LIMIT 0` frame>)` | **Ok** | **true** |
| the persisted `ice.sales.vmat` provider schema | `symbol` / `ts` / `close` all **required**, `PARQUET:field_id` 1/2/3 — a real format-v2 table | — |
| `create_or_replace_temp_view("ice.sales.v3", <NON-empty batches>)` | Err — Iceberg "register_table does not support tables with data" | false |
| `create_or_replace_temp_view("vbare", <non-empty>)` **after** `SET datafusion.catalog.default_catalog='ice'` + `default_schema='sales'` | Err — the SAME Iceberg provider error: the ONE-part registration had left the session | false |

So the `tightenNulls` `required: true` payload the belt refuses on all three SQL doors persisted
freely through the temp-view API, and `SET default_catalog` broke one-part `createOrReplaceTempView`
outright. (The non-empty rows error only because the Iceberg provider refuses data — the same
boundary round 5 recorded. The empty/lazy rows are the leak.)

**Fix — the shared choke point, not the callers.** New `crates/repark-core/src/temp_view.rs`:
`TempViewHome` (the `catalog.schema` a session's temp views live in, snapshotted ONCE at
`build()` from the final config) + `temp_view_ref(home, name)`:

- a **qualified** name refuses `Error::Analysis` (→ facade `AnalysisException`);
- a **one-part** name is built `Full` against the home, so a later `SET` cannot move it;
- parsing is DataFusion's own `TableReference::parse_str` — the same parse `register_table(&str)`
  performed on BASE — so identifier normalization is byte-identical (`MyView` → `myview`,
  `"MyView"` → `MyView`).

MEASURED sub-finding that shaped the predicate: `TableReference::parse_str("a.b.c.d")` returns
**`Bare { table: "a.b.c.d" }`** — past three parts DataFusion falls back to "one identifier". A
`Bare` check alone would therefore have let a four-part spelling through as one oddly-named view,
so the predicate is `Bare` **and** (quoted **or** no embedded dot). Pinned by
`qualified_names_refuse_at_every_arity` / `identifier_normalization_matches_datafusions_own_parse`.

Every sibling path was audited and routes through the one seam (`replace_view` for registration;
`temp_view_ref` directly where the path does not register):

| path | before | after |
|---|---|---|
| `create_or_replace_temp_view` (batches) | raw name → `replace_view` | `replace_view` → `temp_view_ref` |
| `create_or_replace_temp_view_from` (plan) | idem | idem |
| `register_record_batches_as_temp_view` | idem | idem |
| `materialize_dataframe_as_temp_view` / `materialize_dataframe_as_cache_view` | via `register_collected_memtable` → `replace_view` | idem |
| `declare_temp_view_sorted` | `ctx.table_provider(name)` / `ctx.table(name)` / `replace_view` | all three on the pinned ref |
| `drop_temp_view` | `ctx.deregister_table(name)` | pinned ref (a name that cannot be created cannot be dropped) |
| `table_exists`, one-part arm | `ctx.table_exist(segment)` — live default catalog | pinned ref |
| `list_temp_view_names` | read the LIVE `default_catalog`/`default_schema` (after a `SET` it listed the Iceberg catalog's tables as "temp views") | reads the pinned home |

MEASURED on the fixed tree:

| observable | fixed |
|---|---|
| 3-part / 2-part / 4-part `createOrReplaceTempView` | `Error::Analysis`, message says SESSION-LOCAL |
| `table_exists` for every refused name | **false** |
| `create_or_replace_temp_view("vbare", …)` after `SET default_catalog='ice'` | **Ok** |
| `table_exists("ice.sales.vbare")` after that | **false** |
| `table_exists("vbare")` / `list_temp_view_names()` after that | **true** / contains `vbare` |
| one-part `createOrReplaceTempView` + `SELECT count(*)` (facade) | unchanged, reads back |

**This table is the FIRST pass and is incomplete.** It measures a home pinned to the configured
default-catalog NAME; that home is still an Iceberg catalog when `default_catalog` is set at BUILD
time. See "R6-1 second pass" below, where the leak is measured open on this very tree and closed.

**Recorded honestly, scoped OUT:** while `SET datafusion.catalog.default_catalog='ice'` is in
force, `SELECT * FROM vbare` does NOT resolve the temp view (DataFusion resolves a bare name in a
SQL body against the live default too, and has no temp-view namespace to search first);
`SELECT * FROM datafusion.public.vbare` finds it. Spark would find it either way. R6-1 is about the
API never WRITING a catalog; making READ resolution Spark-shaped would mean a resolver in front of
DataFusion. ~~Measured and asserted as the current behaviour in
`set_default_catalog_cannot_move_a_temp_view_into_a_catalog`, not left as a belief.~~ **STRUCK
(round-6 second pass, critic S3):** calling it "the current behaviour" implied nothing moved. It
moved. The WRITE side is now pinned to the home, so a create-then-read-by-bare-name round trip that
worked on BASE under a `SET` to **any** other catalog — including a plain non-Iceberg one — now
misses. Measured both sides and pinned in
`set_to_a_plain_catalog_keeps_the_write_home_and_moves_only_the_read`; see "R6-1 second pass"
below.

**File-size consequence, disclosed.** The fix pushed `session.rs` from 1579 to 1724 lines, over its
1650 exception. Sanctioned out (1) taken: the temp-view family moved to
`crates/repark-core/src/session/temp_views.rs` (311 lines after the second pass), leaving
`session.rs` at **1477** — under the DEFAULT 1500, so its EXCEPTIONS row was **deleted**, not
raised (ceilings ratchet down only). (First-pass counts were 276/1465; trued after the
round-6 critic's second-pass additions.)
`tests/declared_sorted.rs` hit the same ceiling and round 6's new nodes went to
`tests/temp_view_doors.rs` rather than into it.

## R6-2 (RULED: documentation, not a guard) — `context()` is a raw hatch

(a) `ReparkSession::context`'s rustdoc now states that the returned `SessionContext` bypasses
**every** product guard — the pre-execute belt and the tighten DDL-sink refuse, the door dialects
and their routers, and the R6-1 temp-view choke point — that closing it would mean wrapping
DataFusion, and that embedders own the risk.

(b) The round-5 Z-2 claim is **struck in place**: ~~"the door behind `ReparkSession::sql` /
`session.context().sql`"~~ → the belt closed `ReparkSession::sql` on `DataFusionDialect`;
`context().sql` is raw and remains so.

(c) MEASURED and pinned as a KNOWN HATCH (`context_sql_is_a_known_unguarded_hatch`):

| statement | via `context().sql` | via `session.sql` (the product door) |
|---|---|---|
| `CREATE VIEW ice.sales.v_* AS SELECT * FROM tight LIMIT 0` | **Ok**, `table_exists` **true** | refuses naming `tightenNulls`, `table_exists` **false** |

The pin asserts the hatch's CURRENT behaviour, so a future guard moves a pin instead of passing
silently.

## R6-3 (S2) — the Z-1 Spark-door docstring was false for one row

`crates/repark-spark/tests/declared_sorted_tighten.rs` claimed "MEASURED on BASE (675a413): every
statement below returned Ok on this door". ~~every statement~~ → true per ROW: Bare and Partial
returned Ok (the round-5 reds); the **Full** row already refused on BASE (round 4 wired the
three-part spelling on this door) and is a regression fence, not a red. Corrected per row on the
Spark door, and the same per-row truth written on the ANSI door (identical shape) and the NATIVE
door (where the Full row DID return Ok on BASE, because that door had no guard at all — the
existing comment's "round-4 behaviour is not traded away" did not hold for it).

## R6-4 (S3) — the refuse pins asserted the message only

Every Z-1 / Z-2 refuse row on all three doors now also asserts `table_exists(<resolved name>) ==
false`. A refusal that still persisted would have passed the old message-only assertion. Rows
covered: native Z-2 ×4, native Z-1 ×5, ANSI Z-1 ×4, Spark Z-1 ×4.

## R6-5 (S3) — PREPARE: measured inert, pinned, documented

MEASURED on THIS head, native door:

| step | result | `table_exists("ice.sales.v_prepared")` |
|---|---|---|
| `PREPARE p_sink AS CREATE VIEW ice.sales.v_prepared AS SELECT * FROM tight LIMIT 0` | **Ok** | **false** |
| `.collect()` on the PREPARE | **Ok**, 0 batches | **false** |
| `EXECUTE p_sink` | Ok (plans) | — |
| `.collect()` on the EXECUTE | **Err** — `NotImplemented`: "Unsupported logical plan: CreateView" | **false** |

So the class is inert on DataFusion 54.1: a prepared DDL cannot execute at all. No guard added
(there is nothing to guard today); the floor is pinned by
`prepare_of_a_tightened_ddl_sink_is_inert_today` and the class is named in `pre_execute.rs`'s
module docs as measured-inert-today, with the condition that would make it live.

## R6-6 (S3) — the Y-2 `Kills:` comment named the adapter

`crates/repark-core/tests/declared_sorted.rs` still said the recurse lives in
~~`collect_tighten_sources`~~ → it lives in the iterative `walk_tighten_sources` /
`visit_tighten_sources` pair; `collect_tighten_sources` has been a two-line adapter since the
round-4 octo rewrite. Z-5 corrected the ledger's copy of this and missed the comment; corrected.

## R6-7 (S3) — `map.md` stated the three-part gate and then its negation

`crates/repark-core/src/map.md` said the DDL refuse fires "ONLY when the target is a three-part
name in a registered Iceberg catalog", immediately before the Z-1 paragraph saying the gate is the
RESOLVED name. The stale clause is deleted; the map now reads as one truth. The same entry was
trued for R6-1 (temp-view family re-homed; `table_exists`'s one-part arm asks the pinned home), and
`temp_view.rs` / `session/temp_views.rs` / `tests/temp_view_doors.rs` were added to their
directories' maps in lockstep.

## R6-1 second pass (round-6 critic S1) — the home NAME was not the home

The first pass pinned the temp-view home to the **configured** `datafusion.catalog.default_catalog`
/ `default_schema`. That defends against a runtime `SET` and nothing else. `datafusion.*` is a
first-class BUILDER prefix (`DATAFUSION_CONFIG_PREFIX`, `session.rs`), so a session can be built
with `default_catalog = ice` — and `register_memory_catalog("ice")` then REPLACES the provider that
name resolves to with the Iceberg one. The fix had pinned the leak IN.

MEASURED on the name-only fix (core, `ReparkSession::builder().config("datafusion.catalog.default_catalog","ice").config("datafusion.catalog.default_schema","sales")` + `register_memory_catalog("ice")` + `create_namespace("ice","sales")`):

| observable | name-only fix | after this pass |
|---|---|---|
| `register_record_batches_as_temp_view("vempty", <required schema>, [])` | **Ok** | **Err(Analysis)**, says SESSION-LOCAL |
| `create_or_replace_temp_view("vbatch", <rows>)` | (same door) | **Err(Analysis)** |
| `drop_temp_view` / `declare_temp_view_sorted` | (same door) | **Err(Analysis)** |
| `table_exists("ice.sales.vempty")` | **true** | **false** |
| persisted provider schema | `[("symbol", nullable=false), ("ts", true), ("close", false)]` — the `required: true` tighten payload PERSISTED via the temp-view API | nothing persisted |
| `list_temp_view_names()` | `["vempty"]` — simultaneously reported as a session temp view | **Err(Analysis)** (it will not report a CATALOG's tables as temp views) |
| `table_exists("vempty")` (one-part) | — | **Err(Analysis)** |

MEASURED facade half (same conf via `spark.sql.catalog.ice.type=memory` + the two `datafusion.*`
confs): name-only fix → `createDataFrame([], schema)` Ok, `df.createOrReplaceTempView("v_leak")`
Ok, `spark.catalog.tableExists("ice.sales.v_leak")` **True**. After this pass → the first temp-view
mint (`createDataFrame` itself) raises `AnalysisException` naming SESSION-LOCAL, and
`tableExists("ice.sales.v_leak")` is **False**.

**The mechanism.** `TempViewHome` now carries the schema **provider handle** snapshotted at build
alongside the name, and `temp_view::assert_home_intact` re-checks — at EVERY temp-view entry point,
via the same `temp_view_ref` seam — that the live provider under the home name is still that same
object. Identity (`Arc::ptr_eq`), not a type check: no downcast, and it also catches a plain
re-registration of the default catalog. MEASURED discrimination: `Arc::ptr_eq(before, after)` is
**false** across `register_memory_catalog("ice")` and **true** for repeated lookups of an untouched
home (so the check cannot false-refuse an ordinary session — the 3359-node facade suite and the
1054-node Rust suite are the wide evidence for that).

Collateral this closes, MEASURED by the critic and confirmed here: under the same conf an ordinary
non-empty `createDataFrame` used to hard-fail with the Iceberg engine's own
"register_table does not support tables with data". That confusing engine error is now a loud
product refusal that names the cause and the fix.

**Also this pass:**

- **`table_exists` on the allowed quoted-dotted spelling.** `"a.b"` is a legitimate ONE-identifier
  temp-view name (C2-L-006 quote rules) — it could be created, listed and dropped, but
  `table_exists` re-parsed the ALREADY-stripped segment `a.b`, saw an embedded dot, and refused it
  as "qualified". MEASURED: `create` Ok / `list` `["a.b"]` / `drop` Ok(true) /
  ~~`table_exists` Err(Analysis "… is qualified …")~~ → now **Ok(true)**. (On BASE it was Ok(false)
  — also wrong, but not an error.) Fixed with a segment overload
  (`temp_view_ref_from_segment`) that normalizes like `TableReference::parse_str` instead of
  re-parsing: quoted verbatim, unquoted ASCII-folded — MEASURED `table_exists("MyView")` and
  `table_exists("myview")` both true, so BASE's case fold is not traded away. Pinned by
  `a_quoted_dotted_temp_view_name_round_trips_through_table_exists` (integration) and
  `the_segment_overload_normalizes_like_parse_str` (unit).
- **The read/write divergence is a CHANGE, and is now pinned as one.** MEASURED both sides with a
  plain non-Iceberg second catalog (`mem`) and `SET datafusion.catalog.default_catalog = 'mem'`;
  the BASE side is BASE's own call replayed byte-for-byte
  (`context().register_table(<raw &str>, provider)` — what BASE's `replace_view` did):

  | | BASE mechanism | fixed |
  |---|---|---|
  | registration | Ok — landed in `mem.public` (`table_names() == ["v2"]`) | Ok — landed in `datafusion.public`, `mem.public` stays **empty** |
  | `SELECT * FROM v2` | **Ok** | **Err(Analysis "table 'mem.public.v2' not found")** |
  | `SELECT * FROM datafusion.public.v2` | — | **Ok** |
  | `table_exists("v2")` | true | **true** |

  So the WRITE side is immune to `SET` (the point of R6-1) and the READ side is still DataFusion's
  live-default resolution, which means a create-then-read-by-bare-name round trip that worked on
  BASE now misses under such a `SET`. Reachability is low — the facade's
  `currentCatalog`/`setCurrentCatalog` is facade-only state and never issues this SET
  (`python/repark/src/repark/spark/catalog.py`), so only a raw `spark.sql("SET datafusion...")`
  reaches it — but it is a real regression class and is pinned as such by
  `set_to_a_plain_catalog_keeps_the_write_home_and_moves_only_the_read`, not narrated as
  "unchanged behaviour" (that wording is struck in R6-1 above).
- **Two stale citations struck.** `session.rs`'s `context()` rustdoc and `pre_execute.rs`'s module
  docs pointed at `tests/declared_sorted.rs` for `context_sql_is_a_known_unguarded_hatch` /
  `prepare_of_a_tightened_ddl_sink_is_inert_today`; both live in `tests/temp_view_doors.rs`
  (round 6 put them there because `declared_sorted.rs` was at its ceiling). Both now resolve.
- **The `# Errors` doc-contract gap closed.** `register_record_batches_as_temp_view`,
  `materialize_dataframe_as_temp_view`, `materialize_dataframe_as_cache_view` and
  `declare_temp_view_sorted` documented only `DataFusion`/`Config` while refusing
  `Error::Analysis` for a qualified name (and the pins assert that they do). All four now document
  it, matching their `create_or_replace_temp_view*` / `drop_temp_view` siblings.

## Round-6 test counts (all green on the fixed tree)

| gate | result |
|---|---|
| `cargo test -p repark-core -p repark-spark -p repark-sql` | **1054 passed**, 0 failed, 0 ignored (round-5 BASE: 1043). repark-core lib **139** (+4 `temp_view::tests`), `declared_sorted` **37**, `temp_view_doors` **7** (new binary), repark-spark lib 473, repark-sql lib 251 |
| `pytest python/repark/tests/test_declare_sorted_tighten.py` | **22 passed** (18 on BASE + 4 new R6-1 facade nodes) |
| `pytest test_declare_sorted_tighten.py test_declare_sorted.py test_cache_persist.py test_create_dataframe_materialize.py test_catalog_flow.py test_catalog_surface.py` | **121 passed** (the temp-view-adjacent facade sweep for the R6-1 behaviour change) |
| `pytest python/repark/tests` (the WHOLE facade suite — run because R6-1 changes engine behaviour every temp-view caller rides) | **3359 passed, 70 skipped**, 0 failed (round 4's sweep: 3354/70) |
| `make check-lib-py` (the lib-py guard step) | `lib-py: 67 files clean (ceilings held; no-stub rule held)` |
| `make rust-clippy` / `make rust-fmt-check` | clean |
| `make check-rust-file-size` | `238 files clean (default ceiling 1500; 12 exceptions)` — one exception fewer than round 5 |
| `make check-lib-rs` / `make check-crate-dag` / `make check-manifest` / `make check-map-md` / `make rust-panic-ban` | clean |

New nodes across round 6: **7 core integration** (`temp_view_doors.rs`) + **4 core unit**
(`temp_view/tests.rs`) + **4 facade** = 15. Seventeen existing refuse rows gained the
`table_exists` half (R6-4) and four docstrings were trued (R6-3/R6-6) — no test NAME changed, so
the relocation-discipline identity gate is untouched.

## NOT-RUN after round 6

- `make preflight` (the orchestrator runs it, by instruction) — and with it `py-test-facade`, the
  audit and the workflow lint. (The facade suite it wraps was run directly this round, above.)
- **The exact PySpark error text for a qualified `createOrReplaceTempView`.** No JVM in this tier,
  so the message was NOT measured against real PySpark. What is mirrored is the **class**
  (`AnalysisException`); repark's message text is its own and says so in `temp_view.rs`'s rustdoc.
  Recorded as a scope decision, not as parity evidence.
- The parity-live / tier-2 live-oracle tier (JVM + env arm): unchanged — `tightenNulls` has no
  PySpark twin.
- Spark-shaped temp-view READ resolution under `SET default_catalog` (see the scoped-out note in
  R6-1) — measured on both sides and deliberately not changed. It is a MEASURED CHANGE from BASE,
  not "unchanged current behaviour" (that wording is struck above).
- Whether the R6-1 second-pass home check behaves the same for a **Glue / S3 Tables** catalog type
  as for the memory catalog: NOT-RUN (no AWS in this tier). The mechanism is provider identity
  under the home name, which is catalog-type-agnostic, but that is reasoning, not a measurement.
- The payload class "`CREATE VIEW` in an Iceberg catalog persists a TABLE at all" — measured in
  round 4, still deliberately not fixed.

## Payload finding — time-travel ephemeral view rides the raw context (round-6 critic, S3)

`crates/repark-core/src/time_travel.rs` `read_table_at` registers its ephemeral
`__repark_tt_<n>` view with a raw BARE name via `ctx.register_table` — the same
resolution hazard class R6-1 closed for the user temp-view API, in a sibling
OUTSIDE that finding's scope. Under `SET datafusion.catalog.default_catalog =
<iceberg catalog>` the bare registration resolves into that catalog: a
snapshot WITH data fails loud today (fork `register_table` refuses tables with
data), an EMPTY snapshot could persist a junk table. NOT fixed this round —
the clean fix threads `TempViewHome` through `EngineContext` (crate-boundary
surgery) and is out of proportion for an S3 on an internal short-lived view.
Recorded here so the next tighten/temp-view unit picks it up; not pinned
(a pin needs a time-travel fixture under SET, deferred with the fix).
