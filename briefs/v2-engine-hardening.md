# BRIEF — the V2 Engine Hardening campaign (execution slate)

**Status:** ACTIVE — the campaign opens with this slate · **Date:** 2026-08-10 · **Design:**
[../docs/design/v2-engine-hardening.md](../docs/design/v2-engine-hardening.md)

This is the per-unit slate. Every unit is independently mergeable and leaves `main` green. Unlike
the Front-Door campaign, **most units here change engine code**, so the testing contract below is
binding on all of them — no unit is exempt by being "small".

The design holds the goal, the definition of done, the ground truth and the dated decisions
(D1–D9); this brief holds only what each unit must do and how it is accepted. Where the two appear
to disagree, the design wins and the disagreement is raised, not silently resolved.

---

## Orchestration (standing rules)

- Delegated work follows [../AGENTS.md](../AGENTS.md) "Delegated work" and "Delegated-agent
  standing rules" — this brief **references** those rules and never restates or relaxes them. It
  may narrow them for a unit.
- **Capability-tier choices are tool mechanics**, not project rules: they live in the tool adapters
  ([../.agents/](../.agents/map.md)), never here and never in the design.
- **Verification panel per unit:** FULL (adversarial review with the lenses the unit names) for any
  unit that touches engine code, harness code, or a mechanical gate; SLIM (one adversarial verifier)
  for documentation-only units. Each unit below declares its panel.
- **Actors draft in isolated worktrees**; the orchestrating agent assembles; the owner merges every
  PR. A dirty worktree is not a delivered unit.
- **One ledger per unit** at `task/<unit>-ledger.md`, linked from [../task/map.md](../task/map.md)
  in the same commit. Ledger presence is a gate item; provocation proofs for any new gate live in
  it and are never committed as code.
- **Public-repo hygiene applies to every unit:** the two-pass grep (added-line content + commit
  metadata) against the forbidden-content list before anything leaves a worktree. The W1
  sanitization rule (design §5) is part of that list for this campaign.
- **Delegated external lanes G-1…G-3** run beside this slate under the design's §6 policy: G-1 is
  delivered (#30), G-2 activates at H-3 (workload authoring against the harness contract), G-3 is a
  separate product lane. H-1 correctness fixes, H-4 optimizations, and the predecessor-side fix
  under D1 are **not** delegated to those lanes.

## The testing contract is binding — every engine unit, no exceptions

[../docs/testing.md](../docs/testing.md) is the contract; the clauses this campaign leans on
hardest:

1. **Tests land in the same commit as the code.** No "later". A unit whose behavior change ships
   without its pins is not a deliverable.
2. **Divergence-class claims pin every class they name, per entry point, on the Arrow path** —
   `collect` / `to_arrow`, value **and** Arrow type (and nullability where the class touches it).
   One representative case is not the claim; `show` proves nothing about export paths.
3. **A pin is valid only if reverting the fix turns it red**, and every branch a fix adds needs a
   nameable input where it changes the output.
4. **The calibration idiom matches the failure mode**: decimal128 bit-exact fixtures for
   `DECIMAL(p,s)` arithmetic, row-order fixtures for null/sort/window semantics, `f64::to_bits` for
   float aggregation across partitions, schema-equality for evolution.
5. **Hand-computed expectations are not an oracle.** A golden is authored in record mode against
   real Spark; a divergence is recorded as a **disclosure** the live tier re-asserts, so a silent
   convergence goes RED instead of being laundered into "parity".
6. **Every new mechanical gate ships provocation proofs** (must-FAIL and must-PASS, captured
   verbatim in the ledger, never committed).
7. **Test moves follow the relocation discipline** — move-only diffs pass the identity gate;
   anything that changes a test path is a declared-rename unit that ships alone with a name map.

Standing acceptance items every unit inherits, and which the per-unit gates below do not repeat:
`make ci` green (`make verify` before hand-off, `make preflight` before the PR), `map.md` lockstep
for every touched directory, both hygiene passes zero, and the unit ledger linked from
`task/map.md`.

---

## Sequencing

```
H-1d  the divergence registry
  │
  ├── H-1a  session timezone (FIX)           ┐
  ├── H-1b  time-travel view leak (re-port)  ├──▶ H-2 ──▶ H-3 ──▶ H-4 ──▶ H-5
  └── H-1c  $-metadata (decide in unit)      ┘    depth   baseline  evidence  close
```

- **H-1d lands first.** It is the home every DECLARE outcome writes into, and H-1c may produce one.
- **H-1a, H-1b, H-1c are mutually independent** and may run in parallel.
- **H-2 may start beside H-1** on any gap that does not touch an H-1 surface; its timezone rows
  depend on H-1a, and its golden-corpus re-derivation builds on the live tier's recorded nightly
  runs (green twice on merged `main` as of 2026-08-10).
- **H-4 is gated on H-3 evidence** by rule, not by convenience.
- Preparatory chores (design §2.4) land before the phase they protect: the census output-convention
  chore before H-3, the facade-gate extras chore before H-2.

---

# H-1 — Correctness: fix or declare

## H-1d — the divergence registry · panel: FULL (registry-completeness + citation lenses)

**Goal.** This repository gains a discoverable list of its known divergences, and its dead
citations resolve.

**The fact this unit closes.** `docs/spark-sql-iceberg-parity.md` is cited **16 times** from live
sources and does not exist: eight sites in `crates/repark-spark/src/` (`router.rs` ×3,
`normalize.rs`, `metadata_tables.rs`, `ref_ddl.rs` ×2, `insert_overwrite.rs`), two in the facade's
session package (`session_core.py`, `builder_conf.py`), three in facade tests
(`test_sql_passthrough_parity.py`, `test_errors.py`, `test_dropin_disclosure.py`), and three in
`python/repark/tests/map.md`. The cited *content* — the per-surface gap sections, the known-parity-gaps
backlog, and the drop-in disclosure rationale table — has no home here, so today there is no
discoverable list of known divergences in this repository.

**Edits**
- **Author the registry at the cited path** so all sixteen citations resolve without touching the
  citing code. Section structure follows what the citations name (the gap sections, the backlog
  section, the disclosure-rationale table).
- **Every row carries four things:** the behavior repark has, the behavior Spark has, the
  `path::test_name` that pins it, and the rationale (why it is declared rather than fixed, or the
  ticketed intent to fix).
- **Seed the registry** with: the case-folding divergence (decision D3) as its first declared row;
  the cast-failure class (repark raises where non-ANSI Spark yields NULL) as a backlog row; the
  disclosures the live registry already carries; and a row per STATUS known-issue as each resolves.
- **Promote the case-folding cross-door pin** to a declared-divergence test that names the registry
  section it defends.
- **Point [../STATUS.md](../STATUS.md) at the registry** rather than growing a second authoritative
  list; STATUS keeps the *state* of each issue, the registry holds the *semantics*.
- **Decide, and record, whether the live-tier `DISCLOSURES` list becomes the machine-checked
  mirror** of the registry's declared rows (the repo's SSOT-plus-checked-mirror pattern). If yes,
  the check lands with this unit; if no, the reason lands in the unit ledger.

**Acceptance gate**
- Each of the sixteen citation sites resolves: the document exists and every cited section number
  exists in it (verified by re-running the citation grep and reading each target section).
- Every registry row names a live test; a row with no pin is a gate failure, not a TODO.
- The case-folding pin is a declared-divergence test and reds if the divergence disappears.
- A parity/divergence grep finds no second authoritative list (STATUS and the crate maps link, they
  do not restate).
- If the registry joins the `[documentation]` index in `repo-manifest.toml`, `make check-manifest`
  covers its existence.

## H-1a — session-timezone semantics (FIX, decision D7) · panel: FULL (semantics + oracle lenses)

**Goal.** A session timezone exists as a real configuration, timestamp extraction honors it, and
the oracle can see the class at all.

**The fact this unit closes.** Timestamp fields are extracted **in the stored zone, not a session
zone**; `spark.sql.session.timeZone` is not a session configuration here (the only `timeZone`
string in the facade is a reader-option name in the session package); and the live oracle session
is pinned to `spark.sql.session.timeZone=UTC`, so all 27 recorded scenarios are **structurally
incapable** of catching a session-timezone divergence. The census independently measured a
four-hour silent offset in this class. Severity: CRITICAL.

**Edits**
- **The configuration surface:** a session-timezone conf on the session builder and its facade
  spelling, resolved once at session construction (no environment reads at query time).
- **Extraction semantics:** the extractor family in the function shims resolves its field against
  the session zone; the coercion path is explicit, not incidental.
- **Oracle re-derivation:** the live scenario registry grows a **per-scenario session-conf
  override** so scenarios can run under a non-UTC oracle session; the registry's size pin is
  updated deliberately in the same diff.
- **Divergence-class pins per entry point** — native DataFrame, ANSI door, Spark door, facade
  (this campaign splits docs/testing.md's matrix row 3 into the Rust Spark door and the Python
  facade: four cells, not three — a deliberate narrowing, stricter than the contract) —
  value AND Arrow type on the Arrow path.

**Pin budget** (the ranked gap list, gap G1)
- **10–14 differential rows vs real PySpark** (record mode): `year`/`month`/`day`/`hour` over a
  tz-aware timestamp column under two non-UTC session zones; `current_timestamp` type and zone;
  `to_timestamp` of a zone-suffixed string; a DST spring-forward instant and a fall-back instant;
  a tz-aware/tz-naive round trip; `date_trunc('day', …)` across a zone boundary.
- **6–8 Rust unit pins** on the extractor coercion path, one per extractor family.
- **2 live-tier scenarios** running under a **non-UTC** oracle session (this is what the
  per-scenario conf override exists for).
- **Fold in gap G16** (epoch / DST / temporal edges — pre-1970, year-bound, leap-day): **6–8**
  additional differential rows, appended here rather than given their own unit.

**Split rule.** If the harness change and the engine change do not fit one reviewable PR, split:
unit A = the conf surface + the registry override + the differential rows; unit B = the extraction
fix + its Rust pins. Neither half lands without its own tests.

**Acceptance gate**
- Every class the unit names is pinned at **all four** entry points on the Arrow path, value and
  type.
- Reverting the extraction change reds at least one pin per named class (contract rule 3).
- At least two live-tier scenarios run under a non-UTC oracle; with the live gate unset they SKIP
  with a visible reason, never a silent pass.
- The live scenario registry's size pin and its scenario-name uniqueness pin are updated in the
  same diff as the registry change itself.
- A session-conf grep shows exactly one authoritative spelling of the timezone key.

## H-1b — the Spark-door time-travel view leak (decision D1, v1-first) · panel: FULL

**The v1-first rule, stated.** [../STATUS.md](../STATUS.md) "Current milestone" carries the
standing decision: the private v1 predecessor is **bugfix-only**, this repository is the sole
forward target, and **a defect both engines share is fixed there and re-ported rather than patched
only here**. This defect is inherited verbatim from v1, so it is exactly that case. This unit
therefore has a precondition it does not itself satisfy: the predecessor's fix exists first.

**Edits**
- **Re-port the predecessor's fix** into `crates/repark-spark/src/time_travel.rs`, following the
  ANSI door's template — `PinnedViews` in `crates/repark-sql/src/time_travel.rs`, released on
  **every** exit path, including the error paths.
  > **Annotation, 2026-08-11 (H-1b delivery — briefs are versioned, so this narrows rather than
  > rewrites).** "Every exit path, including the error paths" is not what either door delivers or
  > intends. The guarantee is the map's wording, **every `?` / `return` path**; unwind and
  > future-drop are deliberately outside it (`PinnedViews` carries no `Drop` impl — it would have
  > to own a `SessionContext` clone — and neither source exists today: panics are banned in prod
  > and the PyO3 facade drives this via `block_on`). The error-path half of the sentence IS
  > delivered and pinned. Decision D-1 + evidence: [../task/h1b-ledger.md](../docs/history/hardening-h1/h1b-ledger.md).
- **Pins on both doors and the facade**: an ephemeral view created by a time-travel rewrite is gone
  after the statement completes, and gone after the statement *fails*.
- **Move the issue's STATUS row** from "Known correctness issues" to fixed in the same PR, and
  handle the registry side: **create the registry row — H-1d seeds only *disposed* issues, and
  this defect has no disposition and no pin, so H-1d deliberately left it rowless — and update
  STATUS**. (Brief-internal conflict, resolved 2026-08-10 at H-1d's fix pass: this line as first
  written presupposed a row H-1d's own rule forbade. See `task/h1d-ledger.md` D-2 and the fix-pass
  section.) If the re-ported fix closes the defect outright, the honest outcome is **no row at
  all** — a fixed defect is not a divergence; the row exists only if the re-port leaves a residual,
  declared difference, and then it lands with the pin that holds it.

**Acceptance gate**
- The unit ledger names the predecessor commit the fix was re-ported from, and records that the
  re-port was verified against v2's structure rather than diff-applied.
- A test fails without the fix and passes with it, per door and at the facade — including the
  error-path case (a failing statement must not leave a view behind).
- No unrelated behavior change in `time_travel.rs` (the diff is the leak fix and its pins).
- STATUS and the registry agree; neither is left claiming an open defect.

## H-1c — the `$`-metadata introspection rider (decide in unit, decision D2) · panel: FULL

**Goal.** End the open question with a recorded decision and a deliberate pin update.

**The two admissible outcomes**, and what each must ship:
- **(a) Filter** — metadata tables stop enumerating as ordinary tables, decided at the catalog
  layer (`SchemaProvider::table_names`), never in a door parser. Ships with: the behavior pinned on
  both doors and the bare core session, and the reason the filter belongs at that layer.
- **(b) Keep and declare** — ships with: a registry row (H-1d), a live-tier disclosure so a silent
  convergence reds, and the rationale.

**The pins go red on purpose.** Current behavior is pinned at `crates/repark-sql/tests/introspection.rs`
and `crates/repark-core/src/session/tests.rs`. Whichever way the decision goes, both sites are
updated **in the same diff**, and the diff shows the intent — a pin that changes without a stated
reason is indistinguishable from a regression.

**Acceptance gate**
- The decision, its rationale and its rejected alternative are recorded (an ADR if the engine's
  behavior changes; the unit ledger plus a registry row if it does not).
- Both existing pin sites are updated in the same commit as the behavior; neither is deleted.
- Outcome (a): the behavior is pinned at both doors and the facade, and the introspection surface
  is checked for the twin path (`information_schema`).
- Outcome (b): the registry row and the live-tier disclosure both exist, and the disclosure asserts
  non-equality with Spark's shape.

---

# H-2 — Parity deepening (runs until the red list is empty)

**Exit condition (decision D5).** No red-ranked gap remains without either a new pin or an
owner-accepted deferral row. There is **no unit-count cap**; the list is the gate.

**Rules for every H-2 unit**
- Rows go through the parity comparator on the Arrow path: schema signature (name, type,
  nullability), row count, then bit-exact values.
- A golden is authored in **record mode** against real PySpark. Where the honest outcome is
  non-equality, the row is a **disclosure** asserting the two engines still differ.
- The calibration idiom matches the domain (contract clause 4).
- A unit names its gap ID, meets that gap's pin budget or records a deviation with a reason, and
  closes or converts that gap's row in the divergence registry (H-1d).

## The seed — the head of the ranked list

| Gap | Severity | What can be silently wrong | Pin budget | Units |
|---|---|---|---|---|
| **G1** session timezone / tz-aware timestamps | **CRITICAL** | extraction in the stored zone rather than a session zone; a measured four-hour offset; the whole golden corpus recorded under a UTC-pinned oracle | 10–14 differential rows · 6–8 Rust extractor pins · 2 non-UTC live scenarios | **H-1a** (promoted into H-1 by D7) |
| **G2** decimal128 arithmetic bit-exactness | **CRITICAL** | money and quantity columns: Spark's result precision/scale rules, the 38-digit clamp, and decimal literal inference are unpinned; only type-promotion evidence exists | 20–26 differential rows (value **and** exact `decimal128(p,s)`) · 8–10 Rust bit-exact `Decimal128` fixture pins · 2 cross-door rows · 3 CTAS write-back rows | 2 |
| **G3** `MERGE INTO` has no Spark-parity differential | **HIGH** | mechanics are well tested, but the result set has never been compared to Spark: duplicate source keys, `WHEN MATCHED AND …` arm ordering, NULL merge keys | 8–10 differential rows · 4 Rust pins (duplicate-source-key detection, arm ordering) · 2 live scenarios | 2 — unit A grows the live engine abstraction to cover a table lifecycle (create → merge → read); unit B is the parity rows |
| **G4** join semantics: NULL keys, duplicate keys, missing join types | **HIGH** | facade join coverage is about naming and ambiguity, not values; duplicate-key row multiplication is unpinned; the binding accepts only `inner`/`left`/`right`/`full` | 10–12 differential rows (join type × {NULL key, dup one side, dup both sides}, value + type + nullability) · 3 Rust binding pins for the widened join-type set · 2 cross-door rows | 1–2 — the missing join types are implemented **in** the unit that pins them (tests-with-code) |
| **G5** window frame semantics | **HIGH** | coverage is dominated by refusal pins; one real frame-semantics value case exists; a temporal `RANGE` frame over a timestamp order key is rejected outright | 12–16 differential rows (`ROWS` × five bound kinds; `RANGE` over numeric and temporal order keys; ties under `rank`/`dense_rank`/`row_number`; NULL in partition and order keys; empty frame) · 4 live scenarios · 4 Rust pins for the temporal-`RANGE` path | 2 — one for the value matrix, one for the temporal-`RANGE` implementation and its pins |

> **Correction, 2026-08-12 (L-1 landing-truth — briefs are versioned, so this narrows rather than
> rewrites).** The G5 cell's "a temporal `RANGE` frame over a timestamp order key is rejected
> outright" was **untested, not rejected**. O-2's §0 recon
> (`task/g5b-temporal-range-ledger.md`) found interval-bounded temporal `RANGE` already matched
> Spark 4.1.2 at the frozen base. The real defect was the **unit-less** offset envelope (silently
> read as MONTHS): #62 refuses it on `TIMESTAMP` and means days on `DATE`, as Spark does. Five
> residuals remain open as G5b-R (negative-offset `count(*)`=-1 is HIGH).

## The tail — the rest of the ranked list

Thirteen further gaps (G6–G18), carried in the same list and cleared in the same way. Named here so
the exit condition is checkable rather than rhetorical:

- **G6** (HIGH) cast-failure semantics — repark raises where non-ANSI Spark yields NULL. Its
  *registry* half is H-1d; the remaining work is 8–10 differential rows asserting repark's actual
  behavior plus **non-equality** with the Spark golden, and 4–6 live-tier disclosures.
- **G7** (MEDIUM-HIGH) float aggregation determinism — 6–8 `f64::to_bits` Rust pins over a
  catastrophic-cancellation fixture at three partition counts, plus 2 differential rows (the honest
  outcome may be a declared tolerance, not equality).
- **G8** (MEDIUM-HIGH, structural) the capability vocabulary gains a value-semantics family —
  7 new surface IDs × 2 doors = 14 matrix rows, each citing a real test or an honest declared
  absence, plus a test-name liveness gate that closes the audit's stated hole. **Sequenced LAST**:
  it reds both doors until the rows exist, which is the point, and is worthless before G1–G7 have
  produced tests to cite.
- **G9** (MEDIUM-HIGH, process) the hand-computed corpora become live-tier scenarios — roughly
  +18–24 scenarios, taking the registry from 27 toward ~45–50. **Builds on the live tier's
  recorded nightly runs (its former dispatch gate was discharged 2026-08-10).**
- **G10** (MEDIUM) facade-boundary container-shape divergences (map/struct/binary/array shapes,
  pandas timestamp unit) — 8–10 pins plus a ruling on extending the census cohorts.
- **G11** (MEDIUM) the ANSI door has no value-semantics coverage — 6–8 cross-door rows asserting
  the *intended* divergences with reasons, plus 4–6 ANSI-door value pins. The unit first rules on
  whether Spark is even the right oracle for a deliberately Trino-flavoured door; a ruling of "not
  parity, correctness" closes the gap without pretending otherwise.
- **G12** (MEDIUM) three-valued logic — 10–12 differential rows across the truth table at two
  expression entry points, plus 2 cross-door rows.
- **G13** (MEDIUM) expression-level arithmetic overflow — 6–8 differential rows plus 2 Rust pins;
  folds into G2's Rust unit.
- **G14** (MEDIUM, process) the local gate is parity-blind — `make test` is Rust-only behind a
  comment that has been stale since the facade landed, and `make py-test-facade` is in neither
  `make verify` nor `make preflight` (CI does cover the facade suite through the wheel workflow).
  Zero tests: one Makefile decision plus the corrected comment; folds into the first unit that
  touches the Makefile.
- **G15** (MEDIUM) collation is unimplemented and silently wrong-count — 2–3 refusal pins plus a
  disclosure if the ruling is "refuse loudly"; a larger unit if the ruling is "implement".
- **G16** (LOW-MEDIUM) epoch / DST / temporal edge values — folded into H-1a.
- **G17** (LOW-MEDIUM) the namespace-scoped catalog wrapper forwards only the required trait
  methods; **16 defaulted methods** fall through with no omission comments and one is flagged HIGH
  because it can swallow a real inner-catalog override. Not a parity gap, but a silent-behavior gap
  on the write path: 16 explicit forwards or stated omissions, plus 3–4 wrapper tests.
- **G18** (LOW-MEDIUM, structural) nested-type parity is capped by the comparator's flat-schema
  ordering — one comparator enhancement with its own unit tests, then 4–6 nested rows that were
  previously impossible. An enabler for G10.

**Per-unit acceptance gate (H-2, generic)**
- The unit names its gap ID and meets that gap's budget, or records the deviation and its reason in
  the ledger.
- Every new row runs through the comparator on the Arrow path, value and type (and nullability
  where the class touches it); no row asserts through a display path.
- Any row whose honest outcome is non-equality is a disclosure the live tier re-asserts.
- The gap's row in the divergence registry is closed, or converted into a declared divergence with
  a rationale.
- Where a gap's budget names live-tier scenarios, the live scenario registry's size pin moves in
  the same diff.

---

# H-3 — Performance baseline (measure before touching)

**Binding constraint.** H-3 **extends** [../python/repark-parity/bench/](../python/repark-parity/bench/map.md)
— the existing TPC-H/TPC-DS scoreboards, the write-path matrix, the fuzz harness, and the committed
ratio baseline with its fail-closed comparator. **A second measurement convention is a campaign
failure.** The `profiling` build profile already exists with a full rationale and no consumer; H-3
is its first consumer and never edits it.

**The rule that makes the rest work:** *a number whose environment was not recorded is not a
baseline.*

## H-3a — the instrument · panel: FULL (harness-correctness + fail-closed lenses)

- **Tier 1, criterion ratio micro-benches**: a subject and a baseline work unit measured
  back-to-back, the ratio asserted against a committed ceiling, the measured ratio printed pass or
  fail. Criterion stays a **crate-level** dev-dependency. Ceilings may be tightened freely; a
  loosening needs a dated ledger note naming the reason.
- **Tier 2, end-to-end timed runs** through each entry point (native/core, ANSI door, Spark door,
  facade), measuring build-sync and build-async wall separately from plan wall and execute wall,
  plus per-stage timings, peak RSS, spill bytes and file count, warehouse bytes and data-file count
  for write cells, and integrity oracles.
- **Isolation rule (hard):** one cell per subprocess, JSON config in / JSON result out — process
  lifetime peak-RSS in a shared process is not an independent sample, and a cell that cannot be
  isolated records a null with a named reason rather than a misleading number.
- **Release-build proof (hard):** the runner probes the build profile and refuses to write a
  baseline artifact it cannot prove was release-built.
- **Deterministic seeded generators**, synthetic only: same (shape, scale, seed) ⇒ byte-identical
  input, asserted by content hash; a self-describing shape manifest per dataset; a private cache,
  never committed; **positional column names only**; and a provocation test proving the generator
  refuses every plausible "use this real dataset" flag and environment variable.
- **The workloads:** W1, the north-star (design §5), plus four generic shapes — scan → filter →
  aggregate; a wide star join; a window-heavy indicator pipeline; and facade round-trip overhead
  reported as a ratio against its own Rust twin. Each is defined at three scales
  (production-representative / amplified / stress); the **scale name, never a raw row count**, is
  what the baseline directory and the comparator key on, and a cell too small to measure is raised
  in scale rather than given a tighter threshold.
- **Harness code is code**: unit pins land in the same commit, validation fires before any heavy or
  optional import, `map.md` lockstep with a populated `## Debug` section per new directory.

**Acceptance gate** — tier-1 groups authored and ratio-gated with committed ceilings; the tier-2
runner's argument validation, threshold arithmetic, band classification, manifest gating and
cell-coverage fail-closed behavior all unit-pinned; both provocation tests (no-real-data,
no-undeclared-exclusion) present and proven in the ledger; a path-filtered benches workflow runs
tier 1 only; `make help` lists every new target.

## H-3b — the baseline · panel: FULL (evidence-integrity lens)

- **Manifest first:** the environment manifest is written *before any cell runs*, with hard-gated
  keys (machine identity, toolchain, engine and fork versions, generator version, seed, harness
  version), required keys, and recorded-but-not-gated keys. An external manifest may fill a missing
  key or restate one identically; a contradiction is a loud failure, never a merge.
- **Noise floor before thresholds:** the whole suite runs three times back-to-back on unchanged
  code; per-cell, per-metric noise is committed; no threshold may be tighter than measured noise ×
  1.5; a cell noisier than its own band is quarantined **by name** in a checked-in ledger, which is
  the only way a cell leaves the gate.
- **The comparator** gates in order: manifests, then integrity oracles (a changed result set is a
  correctness failure wearing a performance costume, not a delta), then quarantine subtraction
  echoed by name, then fail-closed cell coverage, then per-cell deltas against the noise-floored
  band, refusing non-finite ratios. Zero cells checked can never report green.
- **The baseline directory is evidence, not source**: never hand-edited, replaced wholesale in one
  commit, each refresh naming its cause, and self-verifying — the comparator diffing the baseline
  against itself exits 0.

**Acceptance gate** — the committed baseline parses, its environment freeze is non-empty, its
quarantine ledger exists (an empty file is a result; a missing file is a failure), the self-diff
exits 0, and the comparator's own self-tests cover every loud-failure path.

## H-3c — the spill-coverage matrix (the never-OOM spike) · panel: FULL

Discharges the deferred never-OOM item's trigger question. It is a **measurement spike**: its
deliverable is a matrix, a named defect list, and a ruling recommendation — not an engine change.

- **The grid:** memory budget × workload cell — an unbounded control (what the workload actually
  needs), the session builder's default 8 GiB spill pool, then a descent to a small floor — run
  against stress-scale cells chosen so the unbounded run's peak clearly exceeds the smaller
  budgets: the near-unique group-by, a non-broadcast join, a skewed join, a window over many
  partitions, the two MERGE arms, the full six-entity run, and one large export.
- **The claim, stated precisely:** under a configured memory budget a workload either completes
  (spilling as required) or fails cleanly with a resource error that names the budget; the process
  is never killed by the kernel and never dies without a diagnosable error.
- **Outcome enum per cell**, from completed-without-spill through completed-with-spill (the target
  state under pressure), clean resource exhaustion (an acceptable outcome), other failure (a
  finding), kernel kill (the defect the spike exists to find), timeout, and
  not-spillable-by-construction (declared, not pretended to be a gap).
- **Detection mechanism matters**: kernel-reported OOM via a per-cell memory-limited scope where
  available; a disclosed weaker fallback (subprocess exit-signal classification) otherwise;
  address-space rlimits are **not** used — they measure the wrong failure mode.
- **Deliverables:** the committed matrix, the named defect list (each entry becomes an H-4
  candidate or a declared limitation), and a recommendation on the never-OOM claim's wording.

**Acceptance gate** — every matrix cell carries an outcome from the enum plus peak RSS, spill
bytes, wall and, for failures, the verbatim error text; every kernel-kill and other-failure cell is
named in the defect list; the detection mechanism actually used is recorded per cell; the ruling
recommendation is stated with the matrix as its evidence.

## Owner gates that touch H-2 and H-3 (design §7)

- ~~The first live-parity dispatch~~ — **discharged 2026-08-10** (the armed nightly ran green
  twice on merged `main` before this slate landed); **G9** proceeds on those recorded runs.
- **The reference-host choice** blocks **committing an H-3 baseline** (H-3b), because the
  hard-gated machine keys bind a baseline to one machine by design. It does **not** block H-3a's
  harness, its unit pins, the generators, the comparator, or the noise-floor procedure.

---

# H-4 — Optimization (evidence-driven only)

**A unit exists only because an H-3 artifact named a cost.** There is no intuition path into this
phase.

**Every H-4 unit ships, in this order:** the profile evidence (the flamegraph or baseline delta,
cited by artifact path) → the change → the measured delta against the declared threshold → a
regression pin that reds if the improvement is lost.

**Rules**
- A `profiling`-profile wall is **never** a baseline number; every flamegraph is paired with the
  release wall for the same cell so the profile's distortion is visible rather than assumed small.
- A cell that got faster **and** moved its integrity oracle is a correctness regression found by
  the performance harness — it fails loudly and is not reportable as a win.
- A win lands its **baseline refresh in the same PR**; otherwise the improvement silently becomes
  headroom for a future regression back to the stale ceiling.
- Candidate areas are to be validated, not assumed: batch-size and partition defaults, MERGE write
  amplification, facade-boundary copies, function-kernel hot loops, and whatever the spill matrix's
  defect list names.

**Acceptance gate (per unit)** — the cited evidence artifact exists and is reachable; the delta
meets the declared threshold on the reference host; the regression pin fails when the change is
reverted; the baseline refresh (if any) names its cause; no unrelated behavior change rides along.

---

# H-5 — Verification close · panel: FULL (definition-of-done + honesty lenses)

**Edits**
- Demonstrate the design's five done-criteria **item by item**, each with its evidence, including
  the items that turn out PARTIAL — a campaign close that cannot fail an item is not a check.
- Confirm the registry has no undeclared divergence and no row without a live pin.
- Confirm the performance baseline self-verifies and that every landed optimization is pinned.
- Append the campaign's process metrics to `task/metrics.md` (a new section; earlier sections are
  never rewritten) and land the retrospective.
- Advance [../STATUS.md](../STATUS.md): step 2 → done, step 3 (the cutover inventory) becomes the
  live front; archive this brief and the design with the campaign's record, per the archive rules.
- Produce the **go/no-go evidence pack** for cutover: what is verified, what is declared, what is
  measured, and what remains open.

**Acceptance gate**
- Each done-criterion has a verdict with named evidence; anything short of TRUE carries a dated
  correction and a discharge path, not a softened claim.
- No active rule survives only in this brief or the design once they are archived (the promotion
  check the Front-Door close-out established).
- `make preflight` green; the structural manifest and the status source of truth agree.

---

## What "done" means for the campaign

All phases delivered; the design's five done-criteria demonstrably true; no undeclared divergence
anywhere in the engine; a committed, self-verifying performance baseline with a fail-closed
comparator any later PR can be measured against; every optimization carrying its evidence trail;
the retrospective and its metrics landed; and STATUS advanced to the cutover inventory with a
go/no-go evidence pack behind it.
