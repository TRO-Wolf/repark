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

## Pins (`python/repark/tests/test_declare_sorted.py`, 13 nodes)

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
- Pins: existing 13 `test_declare_sorted.py` nodes untouched. Facade file
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
  `MemTable` (Q-001 / L-1); `PolarsFrame.join` ORs the marker (L-2); hint
  restore keeps the schema-level remint stamp (Q-002).
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

Engine: ACC review-only high (Critic-1 quality + Critic-2 safety + Critic-3
logic + Critic-4 claims) — tier high (`session.rs` / CREATE refuse in the
diff). Actor phase was the R-A..R-D seam close; critics attacked the
post-remediation tree.

Critic-1 (quality/parity): attacked crates contract, pin discrimination,
two-doors, CLOSED surfaces, docstring examples — 1 finding (S3 Q-001
createOrReplace unpinned) remediating (pin added). Null-report: unwrap/
expect, session.rs 1650, CLOSED set, hop-drops struck.

Critic-2 (security/safety): attacked CREATE bypass, ViewTable hop, INSERT
over-refusal, metadata leak, injection, recursion — SAF-001 (ViewTable)
already remediating via `TableSource::get_logical_plan`. Null-report:
atomicity of remint-before-register, size-guard, INSERT allowed, no
secrets, no CLOSED writes.

Critic-3 (logic): L-001 remint+hint (REMEDIATED: remint field-tags
non-null outputs); L-002 nested R-D (REMEDIATED: `field_or_child`);
L-003 MIA remint (ACCEPTED_FLAGGED: all-nullable remint, ledger residual);
L-004 already-non-null stamp (REMEDIATED: schema-level stamp).

Critic-4 (claims): CL-001/002 comment honesty remediating; CL-003 EXISTS
not scalar remediating; CL-004 conservative pin docstring remediating.
CL-IDENTITY: existing commits `%ae`/`%ce` byte-exact
`64240326+TRO-Wolf@users.noreply.github.com`.

Signature table: n/a (no PySpark function wrap).
Oracle probes: n/a (no Spark twin; tighten is a repark extension).
Pin audit: delete-facade-layer (saveAsTable + writeTo create +
createOrReplace) live; right-side combinators live; cache remint live;
EXISTS + lazy view live; R-D allowed + INSERT live; remint hint restore
+ already-non-null stamp + nested schema helper live. Accepted residual
pins do not claim to kill parquet remint.

Finder-battery: 6 dimensions spawned (wiring, pins, fence, removed-behavior,
cross-file, domain). 5 reports in; wiring still in-flight at ready.
S1 candidates 3-vote: parquet residual REFUTED (SQM-accepted); to_arrow
remint REFUTED (L-003 class); MIA REQUIRED persist REFUTED (declared
schema always nullable). Quiet-1 spawned (3 dims); quiet-2 NOT-RUN.
Verdict: FIX-REQUIRED closed on S0/S1; battery not two-quiet CLEAN.

Convergence: ACC-CONVERGED on S0/S1 (residuals accepted-flagged below
floor or SQM-ruled). Not OCTO-CONVERGED (ACC high, not 8-cycle octo).
