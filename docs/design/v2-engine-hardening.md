# DESIGN — the V2 Engine Hardening campaign

**Status:** RULED — settled 2026-08-09, addendum 2026-08-10, landed in-repo at kickoff
2026-08-10 · **Execution slate:** [../../briefs/v2-engine-hardening.md](../../briefs/v2-engine-hardening.md)
**Prerequisite:** the Agent-Agnostic Front-Door campaign is closed
([../history/frontdoor/README.md](../history/frontdoor/README.md)) — this campaign starts on a
repository whose status, structural manifest and history are mechanically truthful.
**Charge:** full optimization *and* the verification that proves it, before any production
workload moves onto this engine. The cutover inventory and the first tagged release follow this
campaign ([../../STATUS.md](../../STATUS.md) "Current milestone", steps 3–4).

This is the second post-milestone campaign and the **first to touch engine code** since the port.
It therefore runs under the full testing contract ([../testing.md](../testing.md)), not the
documentation-only posture the Front-Door campaign could take.

---

## 1. Goal and definition of done

The engine is **verified** — its Spark-parity claims are demonstrated at adversarial depth, and
every known divergence is either fixed or formally declared with a pinned test — and **optimized**
— a committed performance baseline exists, the hot paths have been profiled, and every improvement
carries a regression-pinned benchmark. "Done" is these five, each checkable:

1. **Zero undeclared divergences.** Every known correctness issue is either FIXED (with the
   divergence-class pins [../testing.md](../testing.md) demands — every class the claim names, per
   entry point, on the Arrow path, value AND type) or DECLARED (a documented divergence with a
   pinned test and a rationale — never a silent behavior).
2. **A committed, reproducible performance baseline + a profiling procedure** — the performance
   analogue of the census: recorded environment, generated numbers, comparator-style regression
   detection, evidence committed as evidence rather than as source.
3. **Every optimization shows its evidence trail** — profile → change → benchmark delta →
   regression pin. No speculative optimization lands.
4. **The predecessor carry assessment is closed** — every improvement from the private
   predecessor's final pre-port work round is CARRIED, MOOT or REJECTED with a reason.
   *(Complete — see §2.4; the carries land as preparatory chores outside the phase slate.)*
5. **Campaign retrospective**, and [../../STATUS.md](../../STATUS.md)'s forward sequence advances:
   step 2 flips to done and step 3 (the cutover inventory) becomes the live front.

---

## 2. Ground truth going in

Recorded so the campaign is scoped against what is actually true here, not against an assumption.

### 2.1 The three known correctness issues

Carried debt from the port, stated authoritatively in
[../../STATUS.md](../../STATUS.md) "Known correctness issues". Their campaign dispositions are
§4's decisions D1–D3:

| Issue | Where it lives | Disposition |
|---|---|---|
| Spark-door time-travel view leak | `crates/repark-spark/src/time_travel.rs` registers an ephemeral temp view per rewritten relation and does not release it on every exit path | **FIX**, in the predecessor first, then re-ported (D1) |
| The `$`-metadata introspection rider | metadata tables enumerate as ordinary tables; pinned on the ANSI door (`crates/repark-sql/tests/introspection.rs`) and the bare core session (`crates/repark-core/src/session/tests.rs`) | **DECIDE IN UNIT** (D2) |
| Quoted-identifier case folding vs Apache Spark | inherited engine-wide, pinned by the cross-door case-folding test | **DECLARE** (D3) |

The ANSI door already solves the first class the right way — `PinnedViews` in
`crates/repark-sql/src/time_travel.rs`, released on every exit path — so the fix has a template
rather than a design question.

### 2.2 The verification surface, as it actually is

Five facts that shape H-1 and H-2:

- **The parity corpus is the facade suite.** `python/repark-parity` ships a comparison *function*
  — schema signature including nullability, row count, then bit-exact Arrow values — not a corpus.
  Every Spark-parity case lives in the facade test tree.
- **The live oracle tier is armed and firing.** The nightly live-parity workflow has run green
  twice on merged `main` (2026-08-09 and 2026-08-10, scheduled runs): every mandated golden
  re-derived from live Spark, repark == pinned golden == live Spark on value and Arrow-path
  type. Golden drift and oracle drift are both **actively detected** as of those runs. (An
  earlier recon draft, reading the status checklist instead of the run history, recorded the
  tier as never-fired — the stale-checkbox lesson, once more.)
- **Most non-live parity pins are hand-computed by explicit admission**, while
  [../../AGENTS.md](../../AGENTS.md) "Delegated-agent standing rules" says hand-computed
  expectations are not an oracle. That is a live contract tension, not a nit — H-2's
  golden-corpus conversion (G9) is what resolves it, building on the live tier's recorded runs.
- **The capability vocabulary cannot express a value-semantics gap.** `crates/repark-common/src/surfaces.rs`
  declares **43** surface IDs across four families (statement forms, table-creation options, guard
  rails, ergonomics + seams); not one is a value-semantics surface. The build-enforced typed-absence
  machinery is therefore structurally blind to every dimension the gap list below ranks.
- **There is no discoverable divergence registry.** `docs/spark-sql-iceberg-parity.md` is cited
  **16 times** from live sources — eight in `crates/repark-spark/src/`, two in the facade's session
  package, three in facade tests, three in `python/repark/tests/map.md` — and **does not exist in
  this repository**. The cited content (the gap sections, the known-parity-gaps backlog, the
  drop-in disclosure rationale table) has no home here.

**The ranked gap list.** Recon produced 18 ranked gaps — **2 CRITICAL, 4 HIGH, 3 MEDIUM-HIGH,
6 MEDIUM, 3 LOW-MEDIUM** — each with a proposed pin budget. The head of the list (G1–G5) seeds H-2,
with G1 promoted into H-1 by decision D7; the full list with budgets is in the slate. Ranking
weights, in order: silently-wrong results
first, then the shapes a high-volume incremental-ingest pipeline exercises (multi-entity ingest,
`MERGE INTO`, joins, window aggregations), exotic corners last.

### 2.3 The performance surface — richer than assumed

Recon corrected this design's first assumption: the repository is **not** starting from zero.
[../../python/repark-parity/bench/](../../python/repark-parity/bench/map.md) already carries
TPC-H and TPC-DS scoreboards with subprocess-isolated query workers, a write-path bench matrix, a
seeded differential SQL fuzzer, and — closest to what H-3 needs — a **committed ratio baseline with
a fail-closed comparator** (`bench/tpch/baseline-ratios.json` + `check_baseline_ratios.py`). One
criterion ratio bench group exists (`crates/repark-functions/benches/ratio_string_datetime.rs`).

What is missing is the wall-clock half: no committed wall-clock baseline, no comparator for wall or
peak RSS, no measured noise floor, no flamegraph procedure, no spill matrix, no benches workflow,
no `make` entry point for any of it — and the `profiling` build profile is declared in the root
`Cargo.toml` **with no consumer at all**.

**Consequence (binding on H-3): extend the existing convention; do not fork a second one.**

### 2.4 The predecessor carry assessment

Complete, and its premise inverted: the port's source pin **was** the predecessor's final pre-port
work round, so the port did not skip that round — it started from it. Of 14 assessed improvements,
**4 CARRY · 9 MOOT · 1 REJECT**. Nothing in that round touched correctness or performance (it was
structure, gates and process), so the assessment contributes **no engine units**: H-1, H-2 and H-4
inputs come from the parity gap list and the performance harness spec. The four carries land as
preparatory chores outside the phase slate, together with a forward-only, comment-and-fixture-only
naming pass in the ported test tree (approved 2026-08-10).

---

## 3. Campaign structure — six phases

Each phase is a slate of independently mergeable units; each unit leaves `main` green.

### H-0 — Recon (COMPLETE, no engine change)

Three strands, all delivered before kickoff: a **parity-coverage gap map** (the ranked list in
§2.2), a **performance-harness spec** (the instrument H-3 builds), and the **predecessor carry
assessment** (§2.4). Their conclusions are distilled into this design and the slate; the campaign
carries no separate recon artifacts in this repository.

### H-1 — Correctness: fix or declare

The three known divergences plus the session-timezone class promoted out of the gap list by D7,
plus the divergence registry that gives every DECLARE outcome a home. Units, gates and pin budgets:
the slate.

### H-2 — Parity deepening (verification at adversarial depth)

Widen the differential harness where the gap map found thin cohorts: adversarial inputs against
real PySpark on the Arrow path (value AND type, never `show`), decimal128 bit-exact where the domain
demands it, row-order fixtures for null/sort/window semantics, `f64::to_bits` for float aggregation
across partitions. **Exit: the ranked list has no red gap without either a new pin or an
owner-accepted deferral row** (D5 — no unit-count cap).

### H-3 — Performance baseline (measure before touching)

Build the instrument and take the baseline: two tiers (criterion **ratio** micro-benches for
kernels; end-to-end timed runs through each entry point for whole operations), subprocess-isolated
cells, release-build proof, recorded environment as part of the pin, a measured **noise floor**
before any threshold exists, a committed baseline directory that self-verifies, a fail-closed
comparator, and the flamegraph procedure as the `profiling` profile's first consumer. The
**spill-coverage matrix** lives here and discharges the never-OOM deferral's trigger question.
**Exit: a committed baseline any later PR can be compared against mechanically.**

### H-4 — Optimization (evidence-driven only)

Units are created **from** H-3 artifacts — a flamegraph or a baseline delta that named a cost —
never from intuition. Each carries: profile evidence → change → benchmark delta ≥ its declared
threshold → regression pin. Candidate areas are to be *validated, not assumed*: batch-size and
partition defaults, MERGE write amplification, facade-boundary copies, function-kernel hot loops.
**Exit: no known hot path with unexplained cost; every landed optimization pinned.**

### H-5 — Verification close

The definition of done demonstrated item by item; the campaign retrospective with its process
metrics; STATUS advanced to the cutover inventory; the go/no-go evidence pack for cutover.

---

## 4. Decisions (dated)

Owner rulings, recorded here as the campaign's decision record. A decision changes by a new dated
entry, never by an in-place edit.

**2026-08-09**

- **D1 — Time-travel view leak: FIX, in the predecessor first, then re-ported.** This follows the
  standing shared-defect rule in [../../STATUS.md](../../STATUS.md) "Current milestone": the private
  predecessor is bugfix-only, and a defect both engines share is fixed there and re-ported rather
  than patched only here. The ANSI door's `PinnedViews` release-on-every-exit-path is the template.
- **D2 — The `$`-metadata rider: decide in the unit.** Either filter the metadata tables out of the
  catalog's `SchemaProvider::table_names`, or keep the behavior and declare it. Whichever way it
  goes, the existing pins go red *on purpose* and are updated in the same diff — that is the
  instrument working, and the diff must show the intent.
- **D3 — Quoted-identifier case folding: DECLARE.** A documented divergence with a rationale, and
  the existing cross-door pin promoted to a declared-divergence test. Making quoted resolution
  case-insensitive would touch identifier resolution engine-wide for marginal cutover value;
  revisit only if the cutover inventory surfaces a workload that actually depends on it.
- **D4 — The performance north-star is W1**, and its sanitization rule is absolute (§5).
- **D5 — H-2 depth: run until the red list is empty.** No unit-count cap; the exit condition is the
  empty list, with each unit's PR reviewed as usual.
- **D6 — Delegated external lanes: all three, at the suggested timing** (§6).

**Addendum, 2026-08-10**

- **D7 — Session timezone (gap G1): FIX, not declare-UTC-only.** The full path — a session-timezone
  configuration, extraction semantics resolved against it, and oracle re-derivation under a non-UTC
  zone. H-1 gains the fix unit(s); H-2's timezone rows build on them. This was the single most
  consequential open ruling in the campaign: the two paths differ by roughly an order of magnitude
  in cost, and the severity is CRITICAL either way.
- **D8 — The preparatory chores land outside the phase slate** — the carry items from §2.4,
  together with a forward-only, comment-and-fixture-only naming pass in the ported test tree
  (approved 2026-08-10). None of them is a phase unit, and none gates a phase it does not precede.
- **D9 — One owner gate is held** (§7): the reference-host choice. (A second — the first
  live-parity dispatch — was discharged by the evidence before this design landed: the armed
  nightly had already run green twice on merged `main`.) Nothing else in the campaign blocks
  on it.

---

## 5. W1 — the performance north-star, and the sanitization rule

**W1 is the sanitized *shape* of a production ingest pipeline.** It was chosen because it exercises
almost the whole write path in one workload:

- **Six entities (A–F)** in two stages: extraction — two full snapshots, two predicate windows, one
  one-hop join, one two-hop join, run as a **2-then-4 fan-out** — then a publish stage of six
  sequential cycles in **one long-lived session**.
- **Both write regimes in a single run.** The two full-snapshot entities re-match every existing
  row, so their `MERGE` is ~100 % matched — a full copy-on-write rewrite each run. The four
  incremental-slice entities are ~0 % matched — almost pure insert. One workload therefore covers
  both the write-amplification-dominated and the append-dominated arms of the MERGE executor.
- **A create-then-merge bootstrap**: the first run of an entity is CTAS, every later run is MERGE.
- **A dedup window whose ordering key is constant across the slice** — a full sort at a 100 % tie
  rate, a shape ordinary benchmarks never generate.
- **A computed surrogate key** on one entity, so the window cannot be satisfied from a
  source-ordered column; **bulk multi-column projection rebuilds** (5–14 expressions applied as one
  node, twice per entity); **six `decimal(10,4)` columns** on one entity; and **entity sizes
  spanning four orders of magnitude** against the same session and the same knobs.

**Sanitization rule — HARD, and structural rather than procedural.** The public repository receives
the shape only: operator graph, entity roles, column counts, type mixes, cardinalities, predicates,
and write regimes. **No employer, pipeline, schedule, table or column business name ever appears** —
here, in any harness artifact, in any fixture, or in any commit message. The harness enforces it by
construction: generated columns are named positionally, and the generator has no flag and no
environment variable that accepts a path to an existing dataset (a provocation test proves the
refusal). The mapping from W1 to its real-world source is held by the owner, is not reproduced
anywhere in this repository, and its location is not part of any repository artifact.

The four supporting workloads are generic by construction: scan → filter → aggregate; a wide star
join; a window-heavy indicator pipeline; and facade round-trip overhead measured as a ratio against
its own Rust twin.

---

## 6. Delegated external lanes

Part of the campaign's work runs in **delegated external lanes** (G-1…G-3) beside the main slate,
coordinated under the multi-workstream rules already proven on the fork: worktree-only, a claim
board, tagged PRs, and content verified before anything is called merged. The lanes are vendor- and
tool-neutral by policy; capability-tier choices are tool mechanics and live in the tool adapters,
never in an authoritative document ([../../AGENTS.md](../../AGENTS.md) "Delegated work").

| Lane | Scope | State |
|---|---|---|
| **G-1** | The dead doc-pointer sweep in ported sources — grep-checkable, zero behavior diff | **DELIVERED** — merged as #30, 2026-08-10 |
| **G-2** | Benchmark workload authoring against the harness contract H-3 defines | Activates at H-3 |
| **G-3** | The parked dbt-adapter lane — product breadth in parallel, zero engine-file overlap | Activates when its track opens |

**Not delegated:** H-1 correctness fixes, H-4 optimizations (both need the full testing contract and
touch the engine's riskiest surfaces), and the predecessor-side fix under D1 (single-writer
discipline on the shared pin).

---

## 7. Owner gates currently held

One, blocking a precisely bounded thing:

1. ~~The first live-parity workflow dispatch~~ — **discharged 2026-08-10**: the armed nightly
   had already fired green twice on merged `main` (2026-08-09, 2026-08-10), so the live tier's
   first-run evidence exists; H-2's golden-corpus conversion (G9) proceeds on those recorded
   runs. Recorded here because an earlier draft held it as a gate.
2. **The reference-host choice** — blocks *committing* an H-3 baseline. Because the machine
   identity keys are hard-gated, a baseline is bound to one machine by design; until the host is
   declared there is nothing to bind to. It does **not** block authoring the harness, its unit
   pins, the generators, the comparator, or the noise-floor procedure.

---

## 8. Non-goals

- **No distribution work.** The `ExecutionBackend` seam stays where it is; distribution stays
  deferred by decision.
- **No `ReparkSession` decomposition.** It remains driver-gated under
  [../adr/0005-defer-session-decomposition.md](../adr/0005-defer-session-decomposition.md); this
  campaign is not a driver, and performance evidence that *would* be one is recorded, not acted on
  ahead of the ADR's own rule.
- **No release action.** Tagging is a separate, owner-held step in the STATUS sequence.
- **No speculative optimization.** An H-4 unit that cannot name the H-3 artifact that motivated it
  does not exist.
- **No second measurement convention.** H-3 extends
  [../../python/repark-parity/bench/](../../python/repark-parity/bench/map.md); a parallel harness
  is a campaign failure, not an implementation detail.

---

## 9. Sequencing

```
H-0 (complete) ──▶ H-1 ─┐
                        ├──▶ H-3 ──▶ H-4 ──▶ H-5
                   H-2 ─┘
        (independent surfaces; H-4 gated on H-3 evidence)
```

Preparatory chores (§2.4) run beside H-1/H-2 and land before the phase they protect: the census
output-convention chore before H-3, the facade-gate extras chore before H-2. G-2 runs beside H-3;
G-3 beside anything.
