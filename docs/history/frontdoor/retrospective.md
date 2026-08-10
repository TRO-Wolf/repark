# Retrospective — the Agent-Agnostic Front-Door campaign

> **ARCHIVED 2026-08-10** (Front-Door close-out) — a historical record of the Agent-Agnostic
> Front-Door campaign, kept for provenance and **not a source of live rules**: every rule still in
> force lives in a current document (§8, "Promotion check"). Current state:
> [STATUS.md](../../../STATUS.md).

**Status:** FILED · **Campaign:** Front-Door (FD-1…FD-5) · **Filed:** 2026-08-10 ·
**Design:** [agent-agnostic-frontdoor.md](agent-agnostic-frontdoor.md) · **Slate:**
[frontdoor-campaign.md](frontdoor-campaign.md) · **SEPMO state:** 6 (RETROSPECTIVE), entered via T7
— every PR unit accepted.

The quantitative half of this record — the eight-metric ledger — is **not** in this file. It lives
in the appendable ledger [task/metrics.md](../../../task/metrics.md), which later campaigns add to;
§4 below carries the headline numbers and the caveats that govern how they are read, and points
there for the rows themselves. Two homes for one set of numbers is the exact drift shape this
campaign existed to remove.

---

## 1. Campaign summary

**Goal.** Make the repository legible and safely modifiable by *any* contributor — human or
automated — without depending on a tool name, a model name, or porting-era vocabulary. The first
post-milestone campaign: documentation and mechanical-gate work only, no engine-behavior change.

**Shape.** Five sequenced units, each independently mergeable, each leaving `main` green. The
design was settled 2026-08-08 (owner ruling: single neutral authoritative contract; the universal
hand-written `map.md` discipline preserved against the proposal's generator recommendation). All
five units were opened and merged inside a single 23-hour window on 2026-08-09 (UTC).

| Unit | PR | Opened → merged (UTC) | What it did | Diff |
|---|---|---|---|---|
| FD-1 — one truthful front door | #24 | 00:44 → 00:53 | `STATUS.md` created as the status SSOT; stale phase/crate claims across `PROJECT.md` / the contracts / the maps replaced by pointers; the campaign design + brief landed in-repo | 12 files, +538/−82 |
| FD-2 — the neutral contributor interface | #25 | 17:50 → 18:03 | Option A executed: `AGENTS.md` becomes the single vendor-neutral authoritative contract (precedence chain moved here as its single home); the tool adapter demoted to a thin pointer file carrying zero authoritative facts; `ARCHITECTURE.md` + `DEVELOPMENT.md` created; `.agent/` adapters; a `## Component contract` section on all nine crate-root maps | 26 files, +705/−258 |
| FD-3 — mechanize structural truth | #26 | 19:08 → 20:51 | `repo-manifest.toml` + `scripts/check_manifest.py` (nine rules, every field proven against a real artifact); `check_crate_dag.py` upgraded to an explicit allowed-edge table with dependency kinds; wired into `make ci`, the ci.yml `guards` job, and pre-commit | 27 files, +1,433/−142 |
| FD-4 — reduce active documentation weight | #28 | 22:08 → 22:32 | 22 files `git mv`'d to `docs/history/port-v2/` behind a lossless promotion audit (126 rules classified, 13 distinct promotions, 11 of them landed *before* the moves); the live backlog condensed into `STATUS.md` | 64 files, +1,296/−583 |
| FD-5 — seam honesty + the deferred refactor | #29 | 23:05 → 23:52 | The `ExecutionBackend` overclaim corrected in the code's own doc-comments; ADR-0005 records the `ReparkSession` decomposition as deferred with four precisely stated driver-gates | 11 files, +197/−43 |

**Totals:** 5 PRs · 140 files changed · +4,169 / −1,108. Zero non-comment `.rs` lines changed
across the whole campaign. Every figure in the table above was re-read from the merged PR records
while filing this retrospective, not carried from the unit narratives.

PR #27 (a dependency-bump PR) opened mid-campaign at 20:55 on 2026-08-09, belongs to no unit, and
was still open at close-out — the numbering gap is not a missing unit. PR #30 (the G-1 dead
doc-pointer sweep, merged 2026-08-10) is likewise **not** a campaign unit: it is the first unit of
the V2 Engine Hardening lane, and it discharged a deferral this campaign had recorded. It appears
below twice — once because it closed FD-4's deferred rider, and once because its squash reproduced
the campaign's own ED-2 defect one unit after the campaign ended.

**One process note on timings.** Open-to-merge understates review effort for FD-1 and FD-2, whose
adversarial passes ran *before* the PR was opened; FD-3 and FD-4 ran theirs in-unit, which is why
their windows are longer. This is a reporting artifact, not a discipline difference.

**Closed 2026-08-10.** The campaign's own "What done means" clause was not satisfied at the fifth
merge — two of the eight acceptance items were unmet. The close-out unit discharged both, archived
the campaign's record here, and filed this retrospective and the metrics ledger; §7 records what it
did and what it found.

---

## 2. Per-unit scorecard — each unit against its own acceptance gate

Each unit is scored against the gate its own slate entry declares, not against a common rubric.

### FD-1 — verification FULL · **PASS**

| Gate clause | Verdict | Evidence |
|---|---|---|
| Phase/milestone grep over `*.md docs/ crates/**/map.md` → every hit inside `STATUS.md` or a link to it; no second authoritative statement | **PARTIAL** | The unit's own consistency sweep reported zero survivors, and the front-door files it touched are clean. Three sites outside the unit's change set still carried stale current-state claims at the fifth merge — ED-3 in §4, closed at close-out. This was the campaign's largest residual. |
| Cargo workspace member list agrees with what `STATUS.md` / `crates/map.md` claim (9 crates) | PASS | Nine members on disk and in the root `Cargo.toml`; independently re-proven at FD-3 by `check_manifest`'s two-way inventory rule. |
| `make ci` green; both hygiene passes zero over the branch range; touched-dir `map.md` lockstep | PASS | Recorded in the PR; `check_map_md` / `check_crate_dag` / `check_lib_rs` / `check_workflows_parse` green. |

**Score: 2 of 3 clean, 1 partial.** The unit delivered its object (one status SSOT, in use by every
later unit) and its own adversarial pass caught a surviving front-door claim before merge. The
partial is a population-scoping failure, not a correctness failure — the gate named the whole
tracked surface and the sweep ran the lane.

### FD-2 — verification FULL, gated on the §4 authority ruling · **PASS**

| Gate clause | Verdict | Evidence |
|---|---|---|
| Deleting any one vendor adapter loses **no** project knowledge | PASS | The adapter is a pointer file with no authoritative facts; every fact it references resolves in the neutral spine. |
| Tier/vendor grep over `AGENTS.md` → no authoritative rule matches | PASS | Re-verified for this retrospective: the only matches in `AGENTS.md`, `ARCHITECTURE.md`, `DEVELOPMENT.md`, `STATUS.md`, `README.md`, `PROJECT.md` are cross-references *to* the adapter files. No authoritative rule names a tool or model. |
| Precedence chain in exactly one file; others point, never restate | PASS | `AGENTS.md` `## Precedence` declares itself the single home; `PROJECT.md`, the adapter, and the SEPMO binding manifest all point at it. |
| Every crate-root `map.md` carries the component-contract section | PASS | Re-counted at close-out: 9 of 9 crate maps carry `## Component contract`. |
| `make ci` green; hygiene passes zero | PASS | Recorded in the PR. |

**Score: 5 of 5.** The adversarial pass additionally verified all three new runtime flows and the
component dependency claims *against the actual source* — the strongest attestation in the campaign,
because `ARCHITECTURE.md` is exactly the kind of hand-written document that invents plausible
behavior. It also stopped a private-repo ADR number from being cited in a public document.

### FD-3 — verification FULL · **PASS**

| Gate clause | Verdict | Evidence |
|---|---|---|
| Adding a Cargo member without a manifest entry → CI red | PASS | [fd3-ledger.md](fd3-ledger.md) P-1/P-2, failing run captured verbatim, reverted, re-proven green. |
| A new same-layer `normal` edge → CI red until added to the policy | PASS | P-3, plus P-4 (kind + door↔door) and P-5, the declaration audit — writing the forbidden row into the policy is itself red. |
| Flipping `status = "planned"` to a real path with code absent → CI red | PASS | P-6, proven in **both** directions. |
| `make ci` / `make preflight` green with the new gate; `check_workflows_parse` green | **PASS with a declared flag** | `make ci` rc=0 with `check-manifest` in the chain; 11 workflows parse; zizmor no findings. `make preflight`'s security leg was not run by the actor and is declared as O-3 in the ledger with its justification (no Rust dependency touched). |

**Score: 3 clean + 1 flagged.** Ten provocation proofs, three of them mandated by the slate. The
adversarial pass returned REWORK and closed two live bypasses of the gate's own guarantee — the
campaign's single highest-value review event; see §5.

### FD-4 — verification FULL · **PASS**

| Gate clause | Verdict | Evidence |
|---|---|---|
| The promotion ledger accounts for **every** rule in every archived file; no active rule reachable only through archived material | PASS | [../port-v2/promotion-ledger.md](../port-v2/promotion-ledger.md): 126 rows classified HOMED / PROMOTED / SUPERSEDED / HISTORICAL, counts generated from the disposition column; 13 distinct promotions, 11 landed *before* the `git mv` and 2 found and landed at the unit's own adversarial-review stage. |
| A cold-read contributor can answer "what should happen next?" from `STATUS.md` alone | PASS | `STATUS.md` "Current milestone" carries the standing decision and a numbered four-step forward sequence — independently confirmed by the close-out cold-read trial (§3 item 8), which answered that question from `STATUS.md` and named its source section. |
| All internal links resolve post-move; `map.md` present in every new `docs/history/**` dir | PASS | Repo-wide sweep: every relative target in every tracked `.md` resolves; the archive carries maps. |
| `make ci` green; hygiene passes zero | PASS | `make ci` rc=0, `make test` 1,267 passed, deferred-ledger binding suite 8/8. |

**Score: 4 of 4.** The largest unit by file count and the one whose adversarial pass earned the most
— it recounted the ledger mechanically (the headline counts were hand-written and wrong), stopped
two false claims from entering the status SSOT, and restored a rider wrongly recorded as discharged.

### FD-5 — verification SLIM (one adversarial verifier) · **PASS**

| Gate clause | Verdict | Evidence |
|---|---|---|
| No `ExecutionBackend` / `ReparkSession` *code* change in the diff | PASS | Proven two independent ways: line classification (58 changed `.rs` lines, all `//!`/`///`, zero non-comment) **and** comment-stripped file identity vs base for all three files. |
| ADR-0005 exists, status *Deferred*, linked both ways with `STATUS.md` | PASS | [docs/adr/0005-defer-session-decomposition.md](../../adr/0005-defer-session-decomposition.md) present; `STATUS.md` links it from both the architectural-risk row and the deferred-capabilities row. |
| `make ci` green; hygiene passes zero | PASS | `make ci` / `make test` (1,268) / map + manifest guards green. |

**Score: 3 of 3**, verdict ACCEPT-WITH-NITS. The two-way doc-only proof is the model for any future
"this change cannot affect behavior" claim: one proof over the diff's *lines*, one over the
*artifact* with comments stripped. Neither alone is sufficient; together they are close to
conclusive.

**Campaign scorecard: 5 of 5 units PASS**, with one partial clause (FD-1) and one declared flag
(FD-3 O-3).

---

## 3. Definition of success — §6 items 1–8, assessed

Each item is assessed against evidence available in the tree, not against the PR that claimed it.
Each carries two verdicts where they differ: **at the fifth merge** (2026-08-09, the campaign's own
units) and **at close-out** (2026-08-10, after the close-out unit).

**1. No authoritative rule names a model tier or agent vendor. — TRUE.**
A tier/vendor grep across `AGENTS.md`, `ARCHITECTURE.md`, `DEVELOPMENT.md`, `STATUS.md`,
`README.md`, `CONTRIBUTING.md`, `PROJECT.md` returns only cross-references to the clearly-labelled
adapter files. No authoritative rule matches. The neutral contract's `## Read first` path is written
identically for "a human and an agent."

**2. Current status has one source of truth: `STATUS.md`. — PARTIALLY TRUE at the fifth merge;
TRUE at close-out (2026-08-10).**
`STATUS.md` exists, is authoritative, is pointed at from `README.md`, `PROJECT.md`, `AGENTS.md`, the
maps and the SEPMO binding manifest, and no *competing* status document exists. But three files
still carried current-state claims that contradicted it (ED-3, §4). All three were verified against
the tree and corrected by the close-out unit:

| Site | The stale claim | What it says now |
|---|---|---|
| `CONTRIBUTING.md` | The port "is in progress"; external PRs closed "during the port"; the policy "will be revisited after the port's milestone one" | Contribution-gated because the engine is **pre-alpha and nothing is released** — the port is named as complete, the gate is the pre-alpha state, and the current-state facts are ceded to `STATUS.md` rather than restated |
| `docs/skills/map.md` | `trait-wrapping.md` is "not ported yet (returns with phase 1) … lives in the private v1 repository until crate code lands here" | It was never ported and no longer needs to be: the audit duty it carried is a rule in force in `AGENTS.md` "Version-pin contract", and the one open instance is a named limitation in `crates/repark-iceberg/map.md` |
| `AGENTS.md` | "the bindings crate arrives in phase 3" (future tense, delivered crate) | The bindings crate is delivered; the section keeps the rules and drops the phase framing |

The `CONTRIBUTING.md` site was the material one — linked directly from the README's own Status
section, and the first thing an outside contributor reads. **This item was the campaign's one open
acceptance failure, and it is now closed.**

*Five further stale sites beyond the draft punch list were found and corrected in the same change —
in two rounds that are themselves the lesson.* Round one, the unit's own widened sweep: `AGENTS.md`
"When crates exist (phase 1), `[patch.crates-io]` → the owned fork" and "recorded in the workspace
`Cargo.toml` once phase 1 lands crates" (both future-tense for a delivered state), plus the
out-of-scope row "External code PRs **during the port**", realigned with the rewritten
`CONTRIBUTING.md`. Round two, this unit's adversarial verification — after the widened sweep had
stopped at `AGENTS.md`: **`SECURITY.md`**, squarely inside the declared population, carrying an
audit-gates sentence that was factually wrong about the live CI surface (cargo-audit and pip-audit
already run; only CodeQL is absent) and a future-tense AWS-handling heading. Eight in-population
sites against three known ones — the §6 lesson about sweep populations demonstrated a **third**
time on the same gate, the last round caught only by the adversarial pass. Three same-class sites
just outside the declared population (`scripts/map.md`, `.github/workflows/map.md` ×2) were
corrected alongside.

**3. Structural facts are machine-readable and CI-validated. — TRUE.**
`repo-manifest.toml` + `scripts/check_manifest.py`, wired into the `make ci` chain (verified in the
`Makefile`) and dual-wired as the `repo-manifest guard` step inside the ci.yml `guards` job
(verified in the workflow). Each of the four failure modes the item names is proven red by a
captured provocation: an unclassified Cargo member (P-1), a stale phase field (P-7a), a missing
declared doc (P-7b), a dead command (P-7b).

**4. Dependency constraints are explicit edges with kind. — TRUE.**
20 internal edges declared with kinds (15 `normal`, 4 `dev`, 1 `optional`) and a reason each. A new
same-layer `normal` edge fails until declared (P-3), and the door-to-door edge is expressible as
`dev` while red as `normal` (P-4). The property is stronger than the item asks: the structural rules
are applied to the *declared* table as well as the observed edges (P-5), so writing the forbidden
row down is not a way to make the gate green.

**5. Runtime flows and per-crate component contracts are readable without reconstructing them from
source. — TRUE.**
`ARCHITECTURE.md` carries the three flows as named sections (session construction; query execution
per door; write/commit). All 9 crate-root maps carry `## Component contract`. The flows' factual
claims were verified against the source by FD-2's adversarial pass — the check that separates a
useful architecture document from a plausible one.

**6. Port evidence preserved under `docs/history/port-v2/`, off the normal read path; no active
engineering rule lives only in archived material. — TRUE.**
22 files archived with dated ARCHIVED banners, repaired links, and true final statuses. The
promotion ledger discharges the design's §7 reconciliation identity over 126 classified rules with
13 promotions. Nothing on the `README → STATUS → ARCHITECTURE → DEVELOPMENT → AGENTS` path routes
into the archive except where provenance is the point. The same discipline was applied to this
campaign's own record at close-out — §8.

**7. `map.md` remains universal and hand-written; component contracts live inside crate-root maps. —
TRUE.**
104 `map.md` files in the tree at close-out (103 at the fifth merge, plus this directory's); no
generator exists; `check_map_md.sh` enforces lockstep and `check_manifest.py`'s map rule *checks*
hand-written maps and writes none — the only sanctioned `map.md` automation, and it was deliberately
built as a checker. The owner's REJECT of the generator recommendation survived the campaign intact.

**8. A new capable agent, never having seen this repo, can locate, implement, verify, and hand off a
bounded change starting from `README → STATUS → ARCHITECTURE → DEVELOPMENT → AGENTS.md`. —
UNDEMONSTRATED at the fifth merge; DEMONSTRATED at close-out (2026-08-10).**

At the fifth merge the path existed and was declared, but two things were true. First, no cold-start
trial had been run: no unit's evidence included a fresh contributor or agent executing a bounded
change from the path alone, so the claim rested on inspection. Second, the path's *first hop was
weak*: `README.md` linked `STATUS.md` and `CONTRIBUTING.md` but named neither `ARCHITECTURE.md`,
`DEVELOPMENT.md`, nor `AGENTS.md`. The canonical read order was declared inside `AGENTS.md`
`## Read first` — which a newcomer reached only by already having found `AGENTS.md`.

The close-out unit did both things the item needed. `README.md` gained a `## Where to start` block
naming the five hops in order. Then **one cold-read trial was run and is recorded below verbatim**.

**Trial design.** A reviewer with no prior exposure to this repository was given the repository root
and two questions — *what should happen next* and *how do I make a bounded change* — under a hard
protocol: start at `README.md`; read only documents a document already read explicitly links to or
names; record every hop and which document sent them there; no grepping, no directory listing, no
opening a file nothing pointed at, no builds. Any temptation to search was to be recorded as a
finding rather than acted on. The trial ran mid-close-out — the `## Where to start` block and the
ED-3 corrections in place, but before the STATUS advance and the archival moves (the transcript's
hop 8 reads the slate at its pre-move path).

**Honesty note on the transcript.** It is reproduced as returned, including the three failures it
found and the one place it reports being unable to confirm an inference. Its verdict is its own;
nothing in it was edited to be kinder. Two mechanical changes were made and are the only ones: it is
quoted and re-wrapped to this file's line width, and one tool-name string inside a commit-trailer
example is redacted, marked inline as `[redacted]` — this repository is public and its authoritative
surface names no tool. One of its findings — that `STATUS.md` called this campaign "in flight" while
the campaign's artifacts were visibly finished — is a defect *this very unit* was in the middle of
fixing when the trial ran; it is left in the transcript rather than excised, because a trial edited
to remove the finding it caught is not a trial.

> ### Hop trace
>
> | # | Document read | Sent there by | What it gave me |
> |---|---|---|---|
> | 1 | `README.md` | (start) | What repark is; declared 5-hop read order (README→STATUS→ARCHITECTURE→DEVELOPMENT→AGENTS); pre-alpha status one-liner; pointer to `CONTRIBUTING.md` (not followed — not on declared path, not needed for either question) |
> | 2 | `STATUS.md` | README.md "Where to start" step 2 | Release state (pre-alpha, no tag yet); delivered-capabilities table (9 crates, milestone one COMPLETE 2026-08-08); ordered "what happens next" list (1–4); active workstream (Front-Door campaign); known correctness issues; architectural risks; deferred capabilities; release blockers |
> | 3 | `ARCHITECTURE.md` | README.md step 3 | Crate-tier diagram + dependency rules; 3 runtime flows (session construction, per-door query execution, write/commit); honest `ExecutionBackend` writeup; pointer to `repo-manifest.toml` and to each crate's `map.md` |
> | 4 | `DEVELOPMENT.md` | README.md step 4 (also linked from ARCHITECTURE.md "Onward") | Prerequisites; the `make` target table (`ci`/`test`/`verify`/`preflight`/`develop`/`build-wheel`/`install-hooks`); test-command discipline (`cargo test --workspace`, never `--all-features`); CI surface (tier-1 vs tier-2); troubleshooting table |
> | 5 | `AGENTS.md` | README.md step 5 | Precedence chain; **crate map** ("you will want to change… → home" table) — the locate mechanism; hard rules (fork ownership, two-doors rule, server-prep, tests-in-same-commit, `map.md` lockstep, mechanical gates, unsafe ban, style); verify-before-done; safety/destructive-ops boundaries; delegated-agent rules; pointer to `docs/testing.md` as read-first step 6 |
> | 6 | `docs/testing.md` | AGENTS.md "Read first" step 6 (foreshadowed by README.md step 5: "…starting with docs/testing.md") | The two hard testing rules; entry-point matrix (DataFrame/ANSI SQL/Spark facade); calibration-idiom table; Spark-parity differential harness; divergence-class-claim rules; relocation discipline; forbidden patterns |
> | 7 | `crates/map.md` | AGENTS.md "Crate map" section (names it for navigation) and ARCHITECTURE.md "Crate-by-crate responsibilities" | Fine-grained "I want to… → go to" routing table per crate; full DAG detail; each crate's one-line responsibility; Debug table |
> | 8 | `briefs/frontdoor-campaign.md` | STATUS.md "Active workstreams" section (direct link) | The concrete FD-1…FD-5 slate behind the "in flight" campaign, each unit's edits + acceptance gate, and — notably — its own "what done means" criterion, which is verbatim this same 5-hop cold-read test |
>
> ### Q1 — What should happen next?
>
> Per `STATUS.md` "Current milestone" (explicit ordered list): (1) finish the **Agent-Agnostic
> Front-Door campaign** (documentation + mechanical-gate only, no engine-behavior change —
> `STATUS.md` "Active workstreams"), (2) **V2 Engine Hardening** — full optimization + verification
> across all three entry points, not yet drawn up, (3) **production-pipeline cutover inventory**
> (single-writer-per-table migration plan, carried from `docs/port/PLAN.md`), (4) **the first tagged
> release** — owner-held, not an engineering blocker (`STATUS.md` "Release blockers": "None
> technical").
>
> Following the link `STATUS.md` gives for item (1) into `briefs/frontdoor-campaign.md`, I found
> something the entry path itself doesn't flag: the five units (FD-1…FD-5) the campaign was scoped
> to produce — a new `STATUS.md`, `ARCHITECTURE.md`, `DEVELOPMENT.md`, a rewritten `AGENTS.md`,
> `repo-manifest.toml`, `docs/history/port-v2/`, and ADR-0005 — all **already exist and match their
> acceptance-gate descriptions**, including the FD-5 `ExecutionBackend` honesty fix, which
> `STATUS.md` itself dates to 2026-08-09 in the past tense ("**The docs now say so**"). Yet
> `STATUS.md`, dated the same day, still lists the campaign as "in flight." The likely explanation,
> inferable from the brief's own closing section ("this brief + design archived under
> `docs/history/`… campaign retrospective appended"), is that only the final archival/retrospective
> step is outstanding — but I cannot confirm that without leaving the declared path (checking
> `docs/history/port-v2/README.md` contents or `repo-manifest.toml` directly), so I record it as a
> genuine ambiguity rather than a fact.
>
> ### Q2 — How do I make a bounded change?
>
> **Locate** — `AGENTS.md` "Crate map — where a change will go" gives a coarse table (e.g.
> Session/planning → `crates/repark-core`; Iceberg DML/catalogs → `crates/repark-iceberg`; ANSI door
> → `crates/repark-sql`; Spark door → `crates/repark-spark`; PyO3 → `crates/repark-python`).
> `crates/map.md`'s "I want to…" table (reached via AGENTS.md's own pointer) refines this to
> crate-level routing (e.g. "add a `ReparkSession` method" → `repark-core/map.md`). Per
> `README.md`/AGENTS.md's `map.md` rule, the next step is always to read the `map.md` of every
> directory the change touches before editing.
>
> **Implement** — Obey `AGENTS.md` "Hard rules": no `unsafe` outside `repark-python`; new SQL
> surface needs both door spellings plus a test row per door (`docs/adr/0002`); everything routes
> through `ReparkSession` (no global mutable state, no query-time env reads — `docs/adr/0004`); Rust
> house style (91-`=` banner doc blocks, `thiserror`/`anyhow`, no `unwrap`/`expect`); tests land in
> the **same commit** as the code (`docs/testing.md` rule 1) — pick the test idiom per the
> calibration table (decimal128 bit-exact, row-order fixtures, `f64::to_bits`, schema-equality, or
> Spark-parity differential as applicable); a new facade DataFrame op needs a parity case in the
> same commit; PyO3-boundary changes need a real-wheel-artifact test, not just an in-process one.
> Update the touched directories' `map.md` in the same change (mechanically gated).
>
> **Verify** — `DEVELOPMENT.md`'s command table, confirmed by `AGENTS.md`: `make ci`
> (fmt/clippy/panic-ban/crate-DAG/thinness/manifest/`cargo check`/ruff/uv-lock/toml/spell), `make
> test` (`cargo test --locked --workspace`, **never** `--all-features`), `make verify` = `ci` +
> `test` — "a change is not done until `make verify` is green and touched `map.md` files are
> current" — and, before opening a PR, `make preflight` (`verify` + security audits + workflow
> lint), which mirrors CI exactly.
>
> **Hand off** — `AGENTS.md` "Safety": commit or push only when the user asks; AWS writes only
> through `repark-iceberg`'s sanctioned paths; never drop/delete Glue/S3-Tables data or touch IAM
> without explicit user action. If the work is delegated to sub-agents, "Delegated-agent standing
> rules" requires one `task/<unit>-ledger.md` per unit (linked from `task/map.md`, a location
> AGENTS.md names but that I did not open, staying inside the declared-path restriction) and
> public-repo hygiene per the frontdoor brief's own orchestration notes. No document in the 5-hop
> path gives PR-mechanics specifics (branch naming, commit-message format, `gh pr create` usage) —
> that's assumed generic tooling, not spelled out here.
>
> ### Where the path failed me
>
> - **STATUS.md's headline active item is ambiguous on inspection.** It calls the Front-Door
>   campaign "in flight," but the artifacts the campaign's own brief (which STATUS.md links to)
>   describes producing already exist in finished form as of the same date the status doc carries.
>   STATUS.md bills itself as "the single source of truth for current state," but does not tell me
>   concretely what remains of its own top-listed active item — I had to infer (not confirm) that
>   only archival/retrospective is left, by reading the brief's closing section, and I could not
>   verify even that without stepping off the declared path.
> - **No map.md template is given anywhere in the 5-doc path.** AGENTS.md mandates a `map.md` in
>   every directory, updated in lockstep, but never states the required sections. I only saw the
>   actual template (Purpose/Contents/"I want to…"/Pointers/Debug + "Component contract") by reading
>   `crates/map.md` as an example — that hop was legitimate (AGENTS.md named it), but the template
>   itself is asserted only by convention/example, not declared as a spec anywhere on the path.
> - **No PR/git mechanics.** Branch naming, commit message conventions, and how to actually open and
>   land a PR are not specified in any of README/STATUS/ARCHITECTURE/DEVELOPMENT/AGENTS/testing.md.
>   `DEVELOPMENT.md` says "if `preflight` is green, the PR checks are green" but not how to create
>   the PR. (The only such detail I saw — an `Authored-By: [redacted] (<model>)` trailer,
>   squash-merge-only-by-owner — lives in `briefs/frontdoor-campaign.md`, which is
>   campaign-specific process, not the general contract.)
> - Everything else on the declared path was internally consistent and cross-linked correctly (no
>   dead links, no contradictions between STATUS/ARCHITECTURE/DEVELOPMENT/AGENTS on gates or crate
>   names).
>
> ### Verdict
>
> **PASS.** From the five declared hops alone (plus the two AGENTS.md-named/linked follow-ons,
> `docs/testing.md` and `crates/map.md`), I could answer both questions concretely: STATUS.md gives
> an explicit, ordered "what's next," and AGENTS.md + crates/map.md + DEVELOPMENT.md +
> docs/testing.md together give a complete locate → implement → verify loop with real commands
> (`make ci`/`make test`/`make verify`/`make preflight`) and real gates (map.md lockstep,
> entry-point matrix, tests-in-same-commit). I did not need to grep or list directories to get a
> working answer. The failures found are real but narrow: a template gap (map.md structure), a
> mechanics gap (PR/git process), and one substantive documentation-honesty gap where the "current
> state" SSOT's headline claim ("in flight") is hard to reconcile with the evidence the same
> document set otherwise shows — worth fixing, but it didn't block me from producing a correct,
> actionable answer to either question.

**Disposition of the trial's three findings.**

1. *The "in flight" ambiguity* — **CLOSED in this same change.** `STATUS.md`'s "Current milestone"
   step 1 is now done and dated, and the Active-workstreams row for this campaign is replaced by
   the campaign that follows it. The finding is exactly right about the underlying rule, though: a
   status SSOT that lists an item as active owes the reader what remains of it, not only its name.
2. *No `map.md` template on the path* — **OPEN, recorded.** The rule is mandatory and mechanically
   enforced (`check_map_md.sh`, and `check_manifest.py`'s crate-map rule), but the required sections
   are conveyed only by example. A contributor cannot satisfy a mandatory, gated convention from the
   declared path alone. This is a real gap in the front door this campaign built; it is not
   remediated here because it needs a decision about *where* the template lives (a section in the
   contract, or a spec the guard cites), which is a contract change, not a close-out edit.
3. *No PR/git mechanics on the path* — **OPEN, recorded.** Narrower than it looks, because external
   PRs are not accepted yet and the internal process is owner-run; but the same argument as (2)
   applies the moment contributions open.

Both open findings are handed forward in §7 rather than left in a transcript.

**Item tally at close-out: 8 of 8 TRUE** — 6 true throughout, item 4 true with the campaign's
strongest evidence, and items 2 and 8 closed by the close-out unit. Per the slate's own "What done
means," the campaign is **closed**.

---

## 4. Metrics — headline, and how to read them

The full eight-metric ledger is **[task/metrics.md](../../../task/metrics.md)**, section
`ML-RETRO-1`. This is the first retrospective in this repository, so that file was CREATE per the
binding manifest's `metrics_ledger_location` row. All eight metrics are present; an empty population
is recorded there as `0` with its reason, never as an absent row.

**Headline.** 19 findings caught pre-merge · 4 defects escaped · **83% pre-merge catch rate**
(19 / 23) · 1.8 cycles to convergence · 0 environment drift events · 0 LIGHT-path escapes over an
empty population · 4 flags shipped: 2 still accepted, 1 closed at close-out, 1 still open.

**Reconstruction caveat — the most important thing about these numbers.** The campaign ran
adversarial passes on all five units but did **not** record SEPMO-shaped finding ledgers: no
`F-<unit>-<n>` identifiers, no severity labels, no explicit dispositions. Severities in the ledger
were assigned *by this retrospective* from the PR bodies and [fd3-ledger.md](fd3-ledger.md), and
FD-5's individual nits are unrecoverable. **That gap is itself a finding**, it is the single largest
threat to these numbers' meaning, and it drives FF-1 in §7. A reader should treat the severity
distribution as an interpretation and the counts as sound.

**The four escaped defects, in one line each** (full rows, origins and evidence in the ledger):

| Id | What escaped | Origin | State |
|---|---|---|---|
| ED-1 | The FD-1 squash's attribution trailer names a model that was not the one running | missed clause | CLOSED forward — rule landed at FD-3 (`task/lessons.md`, 2026-08-09); merged history left as-is |
| ED-2 | Squash commits landing with **no attribution trailer at all** — empty commit bodies, the branch-side commits correctly stamped | execution defect | **OPEN.** 2 of 5 campaign squashes (#25, #26) — and a **third occurrence after the campaign**: #30, merged 2026-08-10, the very next unit. Not remediable backwards; detector proposed as FF-2 |
| ED-3 | Three stale current-state claims outside the status SSOT, contradicting it | execution defect | **CLOSED 2026-08-10** at close-out — the three sites plus three more the re-run sweep found (§3 item 2) |
| ED-4 | FD-3's ledger stated the campaign brief and design were not yet in the repository; both had landed two units earlier | execution defect | CLOSED at FD-4 (#28), which restored the links and corrected the status |

ED-2's third occurrence is the finding that matters most here. It landed **after** the campaign
ended, in a unit that was not part of it, with the attribution problem known and two lessons already
written about it. Prose did not prevent the third instance any more than it prevented the second.
The merge-commit surface is outside every unit's diff by construction, and a rule that only lives in
a document cannot reach it.

ED-4 is **counted as escaped** even though the following unit caught it. It shipped in #26 as a
false statement of fact and was not caught by its own unit's review; "caught by the next unit" is
still "not caught by its own unit," and this metric measures the review loop's catch rate, not the
campaign's eventual self-healing — grading it otherwise would let a sequenced campaign launder every
miss into the following PR. It is recorded at the lowest severity and as closed, so it does not read
as an open defect. The distinction that matters: ED-4 cost one extra review pass, ED-3 cost an
acceptance item.

**One flag correction against the draft of this record.** FD-5 recorded four pre-existing rustdoc
intra-link warnings in untouched `session.rs` regions (proven present on base) and handed them to "a
tracked doc-hygiene chore." At close-out that lane was searched for and **does not exist**: no
`STATUS.md` entry, no ledger, no tracked item anywhere in the tree names it. The flag is therefore
recorded as **STILL OPEN and, until this ledger, untracked** — a handoff to a named lane is not a
handoff if the lane is not an artifact. It is now tracked in `task/metrics.md`, which is the honest
minimum, not a substitute for a home in `STATUS.md`. The neighbouring FD-4 rider — eight comment
sites citing a v1 crate name, deferred rather than swept — **was** properly tracked (it went into
`STATUS.md` "Deferred capabilities" as promotion #7) and was **closed on 2026-08-10 by #30**, which
swept them and deleted the deferral. The contrast between the two is the whole lesson: the one
recorded in an artifact got done, the one recorded in a sentence did not.

---

## 5. What the adversarial pass caught — quantified

This is the campaign's strongest evidence, and it is worth stating precisely because the units were
*documentation* units, which is exactly where a review pass is usually treated as optional.

**Coverage.** 5 of 5 units ran an adversarial pass — four FULL, one SLIM. Three returned a recorded
**REWORK** verdict (FD-1, FD-3, FD-4); one returned **ACCEPT-WITH-NITS** (FD-5); FD-2's verdict is
not named in its record, but two findings were caught and fixed before the PR opened.

**Volume and disposition.** 19 enumerable findings, **100% remediated inside the originating unit**
— none deferred to a follow-on, none shipped as an unfixed flag, none disputed away.

**What it caught, by class** (illustrative, not exhaustive — the rows below total 10 of the 19
enumerable findings):

| Class | Count | The specific catches |
|---|---|---|
| **Mechanical-gate bypasses** — a new gate that would have shipped silently permissive | **2** | FD-3 B-1: `ROLES` values were never validated and were read with a default, so a **one-character typo in the role table disabled every structural rule** — the declaration audit included — while the gate still printed its green line. FD-3 B-2: "internal" was spelled as the `repark-` name prefix, so a genuine workspace member named otherwise was **unpoliced in both directions**. Both closed by construction and re-proven (P-8, P-9). |
| **False claims stopped before they became authoritative** | **3** | FD-4: a housekeeping claim headed into `STATUS.md` that *inverted* its archived source; a "pinned in both doors" claim whose second pin is the bare core session; and the promotion ledger's hand-written headline counts (96) against the mechanically generated truth (126) — in the very instrument whose job is to prove the archival lossless. |
| **Public-repo hygiene leaks** | **1** | FD-2: a private-repo ADR number cited in a public document. |
| **Silently-skipped validation** | **1** | FD-3 N1: a non-string `[project]` field skipped the entire STATUS agreement rule *while the success line still claimed agreement*. |
| **Riders wrongly recorded as discharged** | **1** | FD-4: twelve stale v1 crate references believed swept — four map sites corrected outright (one pointed at the wrong door), eight named for the deferred sweep (closed 2026-08-10 by #30). |
| **Obligations the unit did not know it owed** | **1** | FD-4: `docs/release.md` still described the pending-publisher flow for a package name that already exists — a defect that would have failed a real release attempt, found by a unit whose subject was archival. |
| **Front-door status survivors** | **1** | FD-1: a surviving README status claim, caught against the unit's own gate. |
| **Bulk consistency** | **~180 sites** | FD-4: 174 stale link labels displaying paths that do not exist, the census-baseline map's DEFECTIVE note, archived-date consistency. |
| **Attestations returning clean (the pass working and finding nothing)** | — | FD-2 verified all three runtime flows and the component dependency claims against source; FD-5 verified the trait's one-item surface, 28 concrete-context call sites, all five precedent modules named by the ADR, and 124 relative links. |

**The single most valuable finding.** FD-3's B-1. The unit's own prose asserted that the
declaration audit made the adversarial-bypass path closed *by construction* — and the review
demonstrated a one-character diff that defeated exactly that property. A gate that claims to be
unbypassable and is not is worse than no gate, because it retires the vigilance that would have
caught the drift by hand. This is the campaign's proof that the pass must attack **the property the
unit asserts about itself**, not just the code the unit wrote.

**The honest counterweight.** Four defects escaped anyway, and **three of the four are in the same
class the passes were strongest at** — documentation truth. That is not a contradiction; it is the
diagnosis. The passes attacked each unit's *own diff* extremely well and the repository's
*untouched surface* poorly. `CONTRIBUTING.md` was never opened by any of the five units, so nothing
attacked it — even though FD-1's acceptance gate named the whole tracked surface as its population,
and even though FD-1's own PR pointed the README at it. Likewise the merge-commit surface (ED-2) is
outside every unit's diff by construction, which is precisely why it went unchecked twice *during*
the campaign and a third time *after* it. The lesson the ledger supports is narrow and actionable:
**a gate whose declared population is the repository must be attacked over the repository, and a
surface no unit's diff can reach needs a detector that is not attached to a unit.**

The close-out unit is a small confirmation of the first half: re-running FD-1's declared sweep over
the declared population, rather than over the three sites already known, doubled the hit count
(§3 item 2).

---

## 6. Lessons filed to `task/lessons.md`

Checked against every existing entry (2026-08-06 carried-from-v1, 2026-08-07 phase 1, 2026-08-08
phases 2 and 3, 2026-08-09 front-door). Only genuinely new rules were filed; each names the failure
class, the artifact it slipped through, and the rule that now catches it. Per the retrospective
contract, a trap-class lesson needs a **detector in an enforced gate**, not a paragraph — the
detector is stated for each, and where none is proposed the residual risk is recorded explicitly.

Filed under `## 2026-08-10 — front-door close-out` in
[task/lessons.md](../../../task/lessons.md), alongside the one rule the promotion check had to
rescue (§8):

- **DO verify the attribution trailer on the *merge* commit, not only on the branch commits.**
  Extends (does not supersede) the 2026-08-09 entry: that rule fixes *which name* the trailer
  carries; it does not require the trailer to survive the squash. Two of the campaign's five squashes
  (#25, #26) landed with **empty commit bodies**, and a third (#30) did so the day after the campaign
  ended — the branch-side commits were correctly stamped and the merge step dropped them. All three
  landed after the attribution problem was known and under attention, which is exactly why prose did
  not protect them: no unit's own gate can see a squash the unit does not perform. *Detector:* a
  check over `main`'s commit messages (a `push: main` CI job, or a `make` target run at close-out)
  asserting every commit message ends with the trailer and nothing else. *Residual risk while the
  squash stays owner-manual:* the detector can only report after the fact — it prevents the fourth
  occurrence, not the third.

- **DO NOT scope a consistency sweep to the files the unit is editing.** A gate's population is
  whatever the gate's own wording declares. FD-1's acceptance gate named a grep over
  `*.md docs/ crates/**/map.md`; the sweep ran over the unit's change set, and `CONTRIBUTING.md` — a
  root `*.md`, linked from the README's own Status section, and the first document an outside
  contributor reads — went on telling the world the port was in progress for a further day. If a
  file is out of scope for *editing*, it is not out of scope for *checking*: a hit outside the lane
  is a finding to route, not a hit to skip. *Detector:* fold a stale-tense current-state sweep into
  `check_manifest.py`, which already reads `STATUS.md`'s phase words and already owns the
  "structural facts must agree" rule.

- **DO give every unit a unit ledger, not only the units that ship mechanical gates.** One of five
  units (FD-3) filed a `task/<unit>-ledger.md`, which the SEPMO binding manifest names as the
  active-plan home. The other four exist only as PR bodies — which is why this retrospective had to
  reconstruct severities and cycle counts from prose for four units and could not recover FD-5's nit
  count at all. A PR body is not an addressable artifact: it is not in the tree, no gate reads it,
  and it cannot be corrected forward without rewriting the narrative of a merged PR. The ledger is
  what a later audit, an Invariant-V pass, or a retrospective cites. *Detector:* the close-out gate
  for any multi-unit campaign checks that each unit id resolves to a ledger file.

- **DO re-verify a cross-reference's *premise* against the tree, not just its target, when sibling
  units have landed since the citation was drafted.** FD-3's ledger deliberately wrote non-links,
  stating that the campaign brief and design were not yet in the repository and would arrive with
  the closing archival — both had landed at FD-1, two units earlier. The failure mode is specific to
  sequenced campaigns: a citation written against the campaign's *plan* rather than against the tree
  at the moment it lands, which a link checker cannot catch because a deliberate non-link is not a
  broken link. *Detector:* a grep for prose that names a repository path in backticks without
  linking it — cheap, and it surfaces exactly this shape.

- **DO attack a mechanical gate's lookup tables, not only its rules.** Both of FD-3's demonstrated
  bypasses were in the *lookup* layer rather than the rule layer: an unvalidated role vocabulary,
  and an unvalidated definition of "internal." A rule that reads a hand-maintained table with a
  default silently returns *permitted* for every key the table does not know — so a one-character
  typo disables the rule set while the gate still prints its green line. Every gate that keys
  behavior off a hand-maintained table must validate that table's key space as its own rule.
  *Detector:* already landed for this gate (`ROLE_NAMES` / `TIER_NAMES` vocabularies +
  workspace-membership scope, proven by P-8/P-9); the generalization is the standing rule for the
  next gate.

**Considered and rejected as duplicates.** The branch-protection-context rule (2026-08-07) was
correctly *applied* at FD-3 (D-8: the `guards` job name deliberately unchanged) rather than
re-learned — the lesson system working as intended, so no new entry. The forward-only-scrub and
added-lines-only hygiene rules (2026-08-08) likewise held without incident.

---

## 7. What the close-out unit did, and what it hands forward

### 7.1 The close-out unit

The campaign was not closed by its own "What done means" clause at the fifth merge. The close-out
unit is what closed it. Each item below was **verified before being acted on**; where a claim about
the tree turned out to be right, it was fixed, and where the sweep found more than the claim
predicted, the extra sites are named.

| # | What it did | Outcome |
|---|---|---|
| 1 | Correct the ED-3 sites — `CONTRIBUTING.md`, `docs/skills/map.md`, `AGENTS.md`'s future-tense bindings line | All three claims verified TRUE and fixed. Re-running the sweep over the gate's *declared* population found three more (§3 item 2); all six corrected. Closes acceptance item 2. |
| 2 | Add a `## Where to start` block to `README.md` naming the five hops, and run one recorded cold-read trial | Both done. Trial verdict **PASS**, transcript verbatim in §3 item 8, with three findings — one closed here, two handed forward below. Closes acceptance item 8. |
| 3 | Archive the campaign's own record under `docs/history/frontdoor/` with dated ARCHIVED banners, repaired links and final statuses, behind a promotion check | Done — the design, the slate and the FD-3 ledger, with this retrospective and a hand-written `README.md` + `map.md`. The promotion check is §8. The slate's tool/model orchestration labels were re-read and confirmed to be marked as historical process notes, never project rules. |
| 4 | File the metrics ledger as `task/metrics.md` (CREATE, per the binding manifest) and file the §6 lessons | Both done. The ledger is the single home for the numbers; this record points at it rather than duplicating it. |
| 5 | Advance `STATUS.md`'s forward sequence to the V2 Engine Hardening campaign | Done — step 1 marked done and dated with a link to this archive; the campaign row leaves Active workstreams and V2 Engine Hardening enters it. |

### 7.2 Handed forward

Two findings from the cold-read trial, unremediated here because each needs a decision rather than
an edit:

- **The `map.md` template is not stated anywhere on the declared read path.** The convention is
  mandatory and mechanically gated, but a contributor learns its required sections only by copying
  an example. Either the contract states the sections, or the guard cites a spec that does. This is
  a contract change.
- **PR/git mechanics are absent from the path.** Narrow while external contributions are closed;
  load-bearing the day they open.

And one flag with no home:

- **The four pre-existing rustdoc intra-link warnings** (FD-5) are still open and were never tracked
  anywhere but a PR body. They are recorded in `task/metrics.md` under `flags_shipped`; a real home
  in `STATUS.md` is the owner's call.

### 7.3 Feed-forward proposals

Filed under the asymmetric rule: **bar-raising proposals may land immediately, stamped with date and
provenance; bar-lowering or neutral changes wait for the project boundary.** Every proposal is
grounded in this campaign's own ledger evidence. **None is self-applied** — each targets the SEPMO
binding manifest, and the close-out unit deliberately did not edit that manifest on its own
authority. FF-1 and FF-2 are recommended for immediate landing; that is the owner's call, and until
it is made their status is `PROPOSED`.

```yaml
FEED_FORWARD_PROPOSALS:
  - id: FF-1
    targets: binding_row
    manifest_row: "Active plan tracking (task/<unit>-ledger.md) + metrics_ledger_location"
    evidence: >
      findings_per_cycle severities were reconstructed from prose for 4 of 5 units, and FD-5's
      nit count is unrecoverable — an unledgered-claim condition on the retrospective's own
      inputs. 1 of 5 units filed the ledger the manifest already names as the active-plan home.
    proposal: >
      Every PR unit files task/<unit>-ledger.md carrying an addressable finding table —
      F-<unit>-<n> id, severity, disposition, remediation — regardless of whether the unit ships
      code, a gate, or documentation only. The PR body summarizes the ledger; it does not
      replace it.
    direction: RAISES
    status: PROPOSED        # recommend LANDED_IMMEDIATE

  - id: FF-2
    targets: binding_row
    manifest_row: "green_commands (the R7 pre-merge gate / local CI mirror)"
    evidence: >
      ED-2 — 2 of 5 campaign squashes landed with no attribution trailer at all, and a third
      occurrence landed on 2026-08-10 in the next unit after the campaign, all AFTER the
      attribution problem was known. No unit-scoped gate can see the squash step.
    proposal: >
      Add a merge-commit attribution check over main's history (a push:main CI job or a make
      target run at campaign close-out), and record the residual gap explicitly: while the
      squash remains an owner-manual step, the check detects and never prevents.
    direction: RAISES
    status: PROPOSED        # recommend LANDED_IMMEDIATE

  - id: FF-3
    targets: taxonomy_category
    manifest_row: "taxonomy_extensions (currently: None — the ten spine categories)"
    evidence: >
      ED-3 — three live stale current-state claims outside the status SSOT, against a unit whose
      acceptance gate named this exact grep and whose PR attested it clean; the close-out sweep
      over the declared population found three more. 3 of 4 escaped defects are in this one class.
    proposal: >
      Extend the attack taxonomy with "stale current-state claim outside the status SSOT,"
      whose attestation population is explicitly the whole tracked markdown surface rather than
      the unit's diff. Attesting it clean requires citing the sweep's population.
    direction: RAISES
    status: PROPOSED
    note: >
      An extension widens the Critic's attestation duty on EVERY subsequent unit — deliberate,
      and the cost is real. Accept only if the owner wants status truth policed campaign-wide.

  - id: FF-4
    targets: light_thresholds
    manifest_row: "light_thresholds (≤150 changed lines and ≤5 files + the six spine criteria)"
    evidence: "light_path_escapes = 0 over an EMPTY POPULATION — no unit ran LIGHT."
    proposal: NONE — recorded so a later pass does not read the zero as vindication.
    direction: NEUTRAL
    status: NOT_PROPOSED

  - id: FF-5
    targets: severity_floor
    manifest_row: "severity_floor (S1)"
    evidence: >
      No finding was disputed, none was accepted below the floor, and nothing in the ledger
      indicts the floor in either direction. The open product question in the manifest row
      (raising to S2 for the write/commit path) is untouched by this campaign — a documentation
      campaign generates no evidence about data-integrity paths.
    proposal: NONE — examined and unchanged.
    direction: NEUTRAL
    status: NOT_PROPOSED
```

---

## 8. Promotion check — what this archival had to promote first

The same reconciliation identity FD-4 applied to the port ([design §7](agent-agnostic-frontdoor.md))
applies to this campaign's own record: *every rule these three files carry either already lives in a
current authoritative document, is promoted to one before the move, or is a dated historical claim.*
All three archived files were read end to end at close-out.

| Source | Rules it carries | Disposition |
|---|---|---|
| [frontdoor-campaign.md](frontdoor-campaign.md) — orchestration standing rules | Owner squash-merges every PR; nothing reaches the remote until the owner rules | **HOMED** — `AGENTS.md` "Safety": commit or push only when the user asks |
| | Single-agent-in-main is the default; fan-out only on explicit owner go-ahead | **HOMED** — `AGENTS.md` "Delegated work" |
| | A brief references the standing rules rather than restating them; a brief may narrow, never relax | **HOMED** — `AGENTS.md` "Delegated-agent standing rules" + `briefs/map.md` "Pointers" |
| | Public-repo hygiene: two-pass grep (added-lines content + commit metadata) against the forbidden-patterns list | **HOMED** — `task/lessons.md` 2026-08-08 (added-lines content pass) and 2026-08-09 (metadata/identity pass); the forbidden-pattern classes are enumerated in `briefs/map.md` "Import gate" |
| | The attribution trailer's shape and the model it must name | **HOMED** — `task/lessons.md` 2026-08-06 (shape) and 2026-08-09 (which name) |
| | **Repo-local git identity in every worktree** | **PROMOTED** — this was the one rule with **no** home outside the slate. Landed in `task/lessons.md` under `## 2026-08-10 — front-door close-out`, **before** the `git mv`. |
| | Model-tier / role labels for orchestrator, actors and critics | **HISTORICAL** — process notes about how this campaign was drawn up, marked as such in the slate itself; never project rules. Confirmed at close-out, per the archival brief. |
| | Per-unit edits, acceptance gates, sequencing, "what done means" | **HISTORICAL** — execution record; the acceptance clause is discharged and dated in the slate itself |
| [agent-agnostic-frontdoor.md](agent-agnostic-frontdoor.md) | Authority lives in one neutral contract; tool files are adapters carrying no facts (§4) | **HOMED** — `AGENTS.md` states it in its own first paragraphs and owns `## Precedence` as the chain's single home |
| | `map.md` stays universal and hand-written; no generator (§2.3, §3 rec 4 REJECT) | **HOMED** — `AGENTS.md` "Hard rules", including the explicit statement that the one piece of map automation only *checks* |
| | The lossless-archival reconciliation identity (§7) | **HOMED** — `docs/history/map.md` states the rule and the procedure for archiving a campaign; this section is its second application |
| | The non-goals (§5) and the ten dispositions (§3) | **HISTORICAL** — campaign-scoped; the two that outlived it (no history rewrite; no map generator) are homed above and in `task/lessons.md` 2026-08-08 |
| | The eight success items (§6) | **HISTORICAL** — campaign acceptance, discharged and dated in §3 above and in the design's own OUTCOME note |
| [fd3-ledger.md](fd3-ledger.md) | D-1…D-7: how the manifest and edge-table gates work | **HOMED** — the gates themselves are the SSOT (`scripts/check_manifest.py`, `scripts/check_crate_dag.py`), summarized in `AGENTS.md` "Mechanical structure gates", which points at them and never restates them |
| | D-8 / O-2: a CI job rename moves the branch-protection required context in the same change | **HOMED** — `task/lessons.md` 2026-08-07 |
| | Mechanical gate scripts are proven by provocation proofs, not unit tests | **HOMED** — `docs/testing.md` "Gate provocation proofs" |
| | O-1, O-3: declared open items | **HISTORICAL**, with dated dispositions in the ledger itself; O-1 also carried live in `task/metrics.md` `flags_shipped` |
| | R-1: the lessons rider | **HISTORICAL** — discharged; the entry is live in `task/lessons.md` 2026-08-09 |

**Result: one promotion, landed before the move.** Nothing else these files carry is reachable only
through them. The rest is either already owned by a current document or is a dated historical claim.

**One defect found in the neighbouring archive while performing this check**, recorded rather than
buried: `docs/history/port-v2/map.md` described "the eleven promotions FD-4 landed," where the
promotion ledger it summarizes says thirteen distinct promotions, eleven of them landed before the
moves. Corrected in place as a dated correction, which is what that directory's own rules permit.

---

## 9. Verdict

```yaml
RETROSPECTIVE_RECORD:
  id: RETRO-1
  pr_units_covered: [ FD-1, FD-2, FD-3, FD-4, FD-5 ]
  learning_pass:
    promoted:
      - { lesson: "Verify the attribution trailer on the merge commit, not only the branch commits",
          destination: "task/lessons.md 2026-08-10 + detector proposed as FF-2" }
      - { lesson: "Never scope a consistency sweep to the unit's own change set",
          destination: "task/lessons.md 2026-08-10 + detector proposed in check_manifest.py" }
      - { lesson: "Every unit files a unit ledger, not only gate-shipping units",
          destination: "task/lessons.md 2026-08-10 + binding manifest via FF-1" }
      - { lesson: "Re-verify a cross-reference's premise when sibling units have landed",
          destination: "task/lessons.md 2026-08-10" }
      - { lesson: "Attack a mechanical gate's lookup tables, not only its rules",
          destination: "task/lessons.md 2026-08-10 (detector already landed at FD-3)" }
      - { lesson: "Set a repo-local git identity in every worktree before the first commit",
          destination: "task/lessons.md 2026-08-10 — promotion check (§8), landed before the archival move" }
    kept:
      - "FD-5's two-way doc-only proof as the pattern for any 'cannot affect behavior' claim —
         recent, not yet generalized into a rule."
      - "The provocation-proof convention for mechanical gates — already canon in docs/testing.md;
         FD-3 exercised it at ten proofs without amending it."
    archived:
      - "Per-unit gate command outputs and the campaign's PR-by-PR narrative — retained in the PR
         records and in fd3-ledger.md; not promoted."
  metrics_ledger: ML-RETRO-1                 # task/metrics.md
  feed_forward_proposals: [ FF-1, FF-2, FF-3, FF-4, FF-5 ]
  verdict: FILED
```

**The campaign delivered its object.** The repository now has one status source of truth, one
neutral authoritative contract that names no tool, a machine-validated structural manifest, an
explicit dependency-edge policy, hand-written runtime flows and component contracts, an archived
port record behind a lossless promotion audit, and honest doc-comments on its most-overclaimed seam
— across 140 files and zero non-comment lines of engine code.

**And it is closed.** Both acceptance items that were unmet at the fifth merge — item 2's three
residual stale claims and item 8's undemonstrated, unsignposted read path — were closed by the
close-out unit, with a cold-read trial recorded verbatim rather than asserted. The campaign's own
brief and design are off the live read path, where this record now sits with them.

**The finding worth carrying forward** is not any individual catch. It is the shape of the four
escapes: the adversarial pass was excellent at attacking what each unit *wrote* and blind to what
each unit *claimed jurisdiction over*. Every escaped defect lives outside some unit's diff and
inside some unit's declared population — a file no unit opened, a commit no unit authored. The
campaign ended and the pattern continued: the very next unit's squash reproduced ED-2 for the third
time, two lessons after it was first written down. That gap does not close with more review effort
per unit; it closes with detectors that are attached to the repository rather than to a diff. FF-2
and FF-3 are those detectors, and they are the campaign's most valuable output that has not yet
landed.
