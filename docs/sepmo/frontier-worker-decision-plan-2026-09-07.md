# Frontier preparation and bounded worker execution — decision plan

Date: 2026-09-07. Class: campaign. State: **proposal for later review**.
This file closes when the owner records its adoption, revision, or rejection and the
efficiency pilot's E-7 outcome records the disposition. Archive it with that campaign.

## Decision requested

Decide whether frontier models should prepare and review more precise implementation
briefs, so bounded work can be evaluated on Terra and GLM 5.3 Flash. The hypothesis is
that spending more reasoning on scope and contracts can reduce total repair and review
cost. It is not a measured saving or a claim that a particular model is qualified.

This PR documents the option. It does not install skills, run a pilot, change routing,
amend contributor rules, or grant workers new permissions. Current authority remains
in [AGENTS.md](../../AGENTS.md); current orchestration remains in
[SEPMO](../../.agents/skills/sepmo/SKILL.md) and its
[binding manifest](../../.agents/skills/sepmo/binding-manifest.md).

Capability planning and release numbering remain separate. This proposal does not
reserve a package version. Product milestones stay in the
[capability roadmap](../../task/roadmap/epic-term/release-roadmap-2026-08-29.md).

## Existing foundation and measured limits

Inspection baseline: RePark `2019b737`, 2026-09-07. Recheck these facts before implementation.

| Existing home | What the proposal can reuse | Limit to resolve |
|---|---|---|
| [E-2 packet format](packets/packet-format.md) | Markdown plus a JSON sidecar; source identity, stable instructions, scope, implementation context, verification, permissions, and handoff groups. | Structural validation cannot prove that the task is technically complete or correct. |
| [Packet extraction](../../scripts/sepmo_packet_extract.py) | `context_group` extracts referenced files, decisions, and stop conditions. | It returns empty `callers` and `interfaces` lists and takes at most 40 relevant paths. It does not derive a complete dependency graph. |
| [Packet command](../../scripts/sepmo_packet.py) | Build and check commands; consistent rendering and adapter trailers. | Its adapter choices are `muse`, `grok`, `glm`, and `opus`. A Terra/Codex route needs an explicit adapter decision. |
| [Adoption proposal](packets/adoption.md) | A proposed wrapper-consumption path. | A document describing wrapper changes is not evidence that those changes have shipped. |
| [Packet baseline](packets/baseline.md) | Three source-to-packet comparisons and stable-prefix measurements. | Some packets grow. Byte or word reduction does not establish billed-token or total-task savings. |
| [Telemetry](telemetry/map.md) | Existing inventory and usage schema. | Missing cost or cache fields must remain unavailable; they cannot count as zero. |

Extend this foundation only where a pilot proves a missing requirement. Do not build
a second packet framework. Recheck copied stable-prefix rules against their authoritative
homes before wider adoption; a cached instruction must not silently preserve an obsolete rule.

## Proposed skills

Two focused skills are candidates. Their names and location remain decisions, not files
created by this PR. A likely home is `.agents/skills/`, with directory maps and supporting
templates. Each entry point should remain short; load references only when the task needs them.
This follows the progressive-disclosure approach in the
[official skills guide](https://learn.chatgpt.com/docs/build-skills).

### `prepare-worker-brief`

Input: an authorized scope, a pinned source tree, acceptance evidence, and a worker's actual
execution capabilities. The frontier model reads the relevant implementation and callers,
resolves architectural choices, and separates independent work from dependent work.

Output: one bounded worker brief using the E-2 groups, plus explicit unresolved decisions.
The brief distinguishes required behavior, a preferred implementation approach, and choices
the worker can make. Routine naming or local implementation choices should not require a
return to the orchestrator. Cross-component ownership and product semantics should be settled
before dispatch.

The preparer must not turn guesses into requirements. It must cite source evidence for
existing behavior and name the oracle for a requested behavioral change. If the task is still
design work, assign design work explicitly instead of disguising it as mechanical execution.

### `review-worker-brief`

Input: the proposed brief, original requirements, pinned sources, and required evidence.
The reviewer tries to find a worker implementation that satisfies the words while violating
the intended behavior. It checks missing callers, wrong assumptions, ineffective tests,
unsafe permissions, and work that cannot fit the proposed boundary.

Output: `READY`, `REVISE`, or `DECISION REQUIRED`, with concrete findings. `READY` means the
brief can be executed within existing authorization; it does not supply that authorization.
Review may run as a distinct phase under current single-session rules. Do not call an
in-session context break an independent agent review.

Review the completed patch separately. Give that Critic the requirements, source, diff, and
verification artifacts before the Actor's explanation. A well-written brief can still encode
the wrong premise. Patch review must challenge that premise, not only compliance with the plan.

## Minimum worker brief

Use these sections inside the existing packet groups. Detail should follow the task's risk.
Do not expand every small edit into a design document.

| Section | Required content | Failure it prevents |
|---|---|---|
| Outcome and exclusions | One observable outcome, finite scope, and explicit adjacent work excluded. | A narrow change grows into an unreviewable refactor. |
| Source and ownership | Revision, relevant paths and symbols, actual callers, interfaces, dependency direction, and authoritative rules. | The worker edits the wrong layer or follows a stale signature. |
| Behavioral contract | Values, types, nullability, ordering, errors, ownership, and atomicity where relevant. State which dimensions do not apply. | A patch passes a happy-path example while changing a public contract. |
| Implementation steps | Ordered steps, existing abstractions to reuse, and pseudocode only for difficult logic. | Repeated discovery or an unnecessary framework consumes the worker budget. |
| Acceptance matrix | Public entry points, edge classes, oracle source, required commands, and evidence that tests detect the defect. | Tests mirror the patch or exercise only a display path. |
| Decision latitude | Required behavior, preferred approach, free choices, and unresolved decisions. | The worker either improvises semantics or escalates every minor choice. |
| Execution boundary | Allowed edits, forbidden actions, resource budget, dependencies on other work, and stop conditions. | A prompt is mistaken for permission to push, deploy, or mutate unrelated state. |
| Handback | Diff or patch identity, changed paths, commands and real exit codes, evidence, unresolved issues, and resource cleanup. | A persuasive completion message hides missing work. |

For example, a nested-data brief must settle independent-array expansion semantics, null and
empty-container behavior, output naming collisions, and the expected schema. It must not ask
a worker to infer those choices from the name `dynamicFlatten`. Supply the relevant existing
contract or a decision request; do not invent new behavior in the brief.

Record the full affected-file set before applying any context limit. If the required context
does not fit, split the unit at a real interface or keep it at the frontier tier. Silent
truncation is not scope reduction. A source revision or interface mismatch invalidates the
brief; return the contradictory evidence for reconciliation.

## Routing hypothesis and safeguards

These are candidate pilot assignments, not new model bindings:

| Work | Candidate assignment | Qualification needed |
|---|---|---|
| Architecture, scoping, difficult debugging, sensitive implementation, and final risk decisions | Frontier model | Evidence review under the current binding and engineering contract. |
| A bounded feature or coherent refactor with settled interfaces and acceptance criteria | Terra | Held-out examples show correct implementation with acceptable total review and repair cost. |
| Patterned edits, narrow investigations, and precisely specified fixes | GLM 5.3 Flash | Measured success on that task family; uncertainty returns to the orchestrator. |
| Supplemental narrow critique | A qualified lower-cost worker | Findings help the required Critic; they do not replace a mandated frontier review. |

Current SEPMO critical-path requirements continue to apply. Any proposal to replace a required
Actor or Critic tier needs an explicit, reviewed binding decision before use. Do not qualify a
model by name alone or relabel it to bypass a requirement. Keep model-specific mechanics in
the adapters and engineering rules in their existing authoritative homes.

Classify risk before dispatch. Unsafe Rust, write/commit atomicity, catalog changes, security
boundaries, and subtle compatibility semantics require deliberate treatment under current
rules. This proposal does not authorize unsafe Rust or relax existing prohibitions.

Token and time budgets are checkpoints. At a checkpoint the worker reports progress and
remaining evidence; it must not declare completion because a budget expired. Escalation must
also use observable triggers: repeated failed repair, contradictory source evidence, missing
oracle coverage, or a required edit outside scope. Model confidence alone is insufficient.

Markdown is not a security boundary. The runner must enforce the approved execution limits,
and the orchestrator must inspect the returned patch. Tool deny lists are not an operating-system
sandbox when shell commands can reach the same files. Assess actual runner isolation before a
pilot; do not infer it from an adapter's label or from a brief's forbidden-actions section.

## Pilot design for later approval

Separate brief quality from model choice so the result explains what changed.

1. Hold the worker and task family constant. Compare the current brief process with frontier
   preparation and brief review. Measure preparation and review costs as part of each run.
2. Hold the accepted brief process constant. Compare candidate worker assignments on matched
   tasks, with the same tools, context access, acceptance criteria, and review standard.
3. Include held-out tasks, clean changes, known defects, and plausible incorrect patches.
   Blind replay tasks to their later fixes. Treat repeated trials on one issue as correlated
   evidence, not many independent successes.
4. Assess correctness independently of worker self-report. Use real entry-point results,
   relevant oracles, and tests that reject seeded incorrect behavior. Distinguish brief defects,
   implementation defects, reviewer misses, and infrastructure failures.
5. Admit only the task families supported by the results. A small clean pilot cannot establish
   a rare-defect safety rate or qualify a worker for unrelated sensitive work.

Before execution, record the task set, risk exclusions, stop criteria, budget, minimum quality
bar, and acceptable cost/latency tradeoff. Do not choose thresholds after seeing which model
wins. If sample size cannot support a claim, report that limit and keep routing unchanged.

| Measure | Accounting rule |
|---|---|
| Accepted-change cost | Include frontier preparation, worker execution, brief review, patch critique, repairs, and failed attempts. |
| Token usage | Record uncached input, cached input, and output separately where provided; retain provider/model/settings. |
| Wall time | Measure elapsed time to an accepted patch, including dependencies and retries. Report parallel compute separately. |
| Correctness | Record defect severity, missed defects, false alarms, and independent acceptance results. A cheap wrong patch is a failure. |
| Brief usefulness | Count clarification rounds, source mismatches, missing acceptance cases, and unnecessary context. |
| Review burden | Record actual reviewer work and repair cycles; do not equate worker completion with saved engineering time. |

Keep stable contract references and reusable instructions separate from task-specific context.
Load the smallest sufficient source slice rather than full conversation history. Cache-hit
counts and context size are useful diagnostics; the decision depends on cost per accepted
change and quality, not token reduction alone.

## Decision register

All rows are **OPEN for owner review**. They are decisions for a later implementation unit,
not unresolved obligations in this documentation-only unit.

| ID | Decision | Recommended starting point | Evidence needed before adoption |
|---|---|---|---|
| FW-1 | Skill boundary and home | Two short repo-local skills with a shared brief template and conditional references. | Review duplication against SEPMO and existing worker adapters. |
| FW-2 | Packet representation | Fill existing implementation-context fields first; version the schema only for a demonstrated missing contract. | A representative brief passes structural checks and a human completeness review. |
| FW-3 | Terra execution route | Specify an explicit adapter mapping and its real permission/handback behavior. | A bounded integration check; no claim that an existing adapter already supports it. |
| FW-4 | Eligible worker and reviewer roles | Preserve current bindings during evaluation; start with low-risk bounded work. | Task-family qualification and an explicit binding amendment for any critical-path substitution. |
| FW-5 | Brief-review depth | A focused check for small work; full adversarial brief review where semantics or boundaries are difficult. | Matched-task evidence that extra planning reduces downstream work. |
| FW-6 | Pilot budget and success criteria | Approve them before selecting or running trials. | A written task set, thresholds, budget, and accounting method. |
| FW-7 | Isolation and patch admission | Inspect runner enforcement and returned changes under existing authority. | Demonstrated scope enforcement or a documented restricted pilot that avoids unsupported isolation claims. |

## Follow-up sequence

After owner review, an implementation charter should name the accepted FW decisions and
their evidence. A first unit can add the smallest skill/template pair and validate it on
representative briefs without changing worker routing. A separate approved pilot can measure
brief quality. Adapter or packet changes follow only if those briefs expose a concrete gap.
Routing adoption follows qualification and any required policy amendment.

Each unit retains the existing scope audit, Actor–Critic review, verification, and PR process.
The final efficiency-pilot report records accepted and declined options, total measured cost,
quality limits, and the next review trigger. Rejection remains a useful outcome; it avoids
maintaining an orchestration layer that does not pay for itself.
