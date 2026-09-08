# Capability roadmap planning — 2026-09-07

Class: ledger. Retires from staging in this documentation unit's departure commit;
the next pickup archives it after merge. Base: `f46384c8`.

## Scope and authorization

The owner approved separating capability milestones from package versions and requested a
planning-only PR on 2026-09-07. This is one documentation unit. Worker-skill design remains
discussion; no model routing, agent permissions, engine code, release tag, or API contract changes.
The previous branch's flattening and Iceberg reports are outside this PR.

## PROPOSITION LEDGER

| Clause | Proposition (checkable) | Proof obligation | Verdict | Evidence / open question |
|---|---|---|---|---|
| C-001 | Capability names replace future package versions as planning identifiers. | Map each existing roadmap slot to a stable capability ID; retain historical lookup. | PROVEN | Owner-approved scope; finite domain is the roadmap's 0.6, 1.0, 1.1–1.13, two 2.0 rows, 2.1–2.9, 3.0 and conditional format-v4 items. |
| C-002 | Package versions are assigned during release assembly. | Point to the existing compatibility policy; preserve its versioning table byte-for-byte. | PROVEN | `docs/release.md` owns versioning; no fixed calendar or next release number is requested. |
| C-003 | Shipped subsets do not imply milestone completion. | Reconcile example, spill, dbt, and API-freeze planning references to their status/evidence homes. | PROVEN | `STATUS.md`, `docs/release.md`, and `docs/perf/spill-matrix-baseline.md` distinguish these scopes. |
| C-004 | The PR contains planning Markdown only. | Inspect changed paths; compare source, dependency, workflow, skill, and release-version trees to base. | PROVEN | Allowed homes: roadmap, release documentation, PROJECT's roadmap pointer, their maps, this unit's ledger/navigation, and documentation-verification citations in scripts/map.md. |
| C-005 | Prior scope and dependency decisions remain addressable. | Preserve original headings/anchors and dated decisions; distinguish explicit supersession from historical records. | PROVEN | Existing roadmap and design-card paths remain; unrelated campaigns and historical ledgers stay outside the diff. |

Scope verdict: PASS (5/5 defined propositions, zero OPEN or REJECTED). Execution evidence follows.

## Pre-execution review

`PER-capability-roadmap-planning`: PROCEED. One PR covers C-001–C-005. Authorization is the
owner's acceptance and PR request in this task. All six LIGHT rubric criteria pass: one
planning component, Markdown only, one-commit reversibility, no new dependency or runtime
pattern, no runtime-sensitive path, and no unresolved scope decision. The manifest's prose-only
size rule applies. Actor and Critic run sequentially in this session at frontier tier.

`SLR-1`: edit only the named planning homes. Preconditions: clean isolated worktree at the
verified base; shared hooks executable; 670 GiB free before checkout. Success: scoped diff and
preserved policy/scope references. Risk: historical version references become misleading;
handled by an explicit legacy-label interpretation and capability index. No material open risk.

Pickup inspected the latest merged delta: no roadmap departure edit applies. Unrelated ledger
archival would widen this planning unit and is left to its owning pickup, following the
contract's narrow-change rule.

## Execution evidence

Pending implementation, review, and required gates. No convergence or delivery claim yet.

## Documentation review

Context break executed; attacking artifacts, not memory. This is a procedural in-session
break, not a fresh context. Review inputs: C-001–C-005, the base-to-working-tree diff,
source documents, and the ten-category taxonomy. No runtime behavior is changed.

The review checked legacy consumers, original headings, the Q&A rows, and compatibility-policy
text independently against `f46384c8`. A Python assertion check found 27 unique capability IDs
in the index, retained every original section heading and decision row, confirmed the versioning
policy table byte-identical, and resolved 166 local links across the changed tracked documents.
The first check accidentally counted the separate pickup table too; restricting the count to
its declared index domain made the check valid. No document was changed to satisfy that error.

| Clause | Execution evidence |
|---|---|
| `C-001` | Roadmap capability index covers the complete legacy domain; the two original 2.0 rows share CAP-SERVER-ACCESS. |
| `C-002` | Release-assembly section names version selection at assembly; the existing compatibility table is byte-identical. |
| `C-003` | Roadmap pickup table points to examples, spill, dbt, and existing API-freeze evidence without asserting full completion. |
| `C-004` | `git diff --name-only f46384c8` contains Markdown only; no skills or dependency files change. |
| `C-005` | Every original `##` / `###` heading and Q&A table row remains; the dated interpretation supersedes future tag assignments explicitly. |

```yaml
COVERAGE_ATTESTATION:
  pr_unit: capability-roadmap-planning
  categories:
    - id: AT-1
      status: ATTACKED
      evidence: Compared each scoped proposition with the document diff and capability index.
      artifacts: [task/roadmap/epic-term/release-roadmap-2026-08-29.md, docs/release.md]
    - id: AT-2
      status: ATTACKED
      evidence: Checked partial capability delivery, mixed-scope releases, legacy references, and the two 2.0 rows.
      artifacts: [task/roadmap/epic-term/release-roadmap-2026-08-29.md]
    - id: AT-3
      status: ATTACKED
      evidence: Checked that an old tag or queued row cannot certify remaining acceptance or authorize implementation.
      artifacts: [task/roadmap/epic-term/release-roadmap-2026-08-29.md, docs/release.md]
    - id: AT-4
      status: N/A
      justification: No runtime state or concurrent execution changes; edits are isolated from the owner's dirty branch.
    - id: AT-5
      status: ATTACKED
      evidence: Verified that later shared-trust planning grants no permission to expose an unprotected server and changes no agent authority.
      artifacts: [task/roadmap/epic-term/release-roadmap-2026-08-29.md]
    - id: AT-6
      status: ATTACKED
      evidence: Compared original headings and dated decisions with base and checked local document links.
      artifacts: [task/roadmap/epic-term/release-roadmap-2026-08-29.md, task/roadmap/epic-term/roadmap-design-plan-2026-08-29.md]
    - id: AT-7
      status: N/A
      justification: Documentation changes add no engine work, allocation, or service cost.
    - id: AT-8
      status: ATTACKED
      evidence: Compared the versioning policy table byte-for-byte and preserved the engineering-contract precedence.
      artifacts: [docs/release.md]
    - id: AT-9
      status: N/A
      justification: No runtime failure path, logging, or diagnostic interface changes.
    - id: AT-10
      status: ATTACKED
      evidence: Executed source-identity, legacy-domain, heading, decision, policy-table, link, and path-scope assertions; whitespace check is clean.
      artifacts: [scripts/map.md]
  reattested: []
  complete: true
```

No S0–S3 product or planning findings remain. Required PR gates are still pending at this
review checkpoint; the final validation record must precede publication. No new runtime test
is needed for this prose change. The existing structural gates and direct document assertions
verify its changed surface; no lint, test, or review policy is amended.

The initial preflight stopped at ledger grammar: the evidence table resembled a second clause
table, and documentation verification lacked citations under the guard's scanned roots.
The evidence references now use inline-code clause names; `scripts/map.md` points to this
record with the required citations. The ledger stays in staging until validation passes.
AT-6, AT-8, and AT-10 were rechecked after correcting the remaining future-freeze sentence
and the documentation evidence wiring. No runtime assertion or gate implementation changed.

## Final validation and disposition — 2026-09-07

`make preflight` exited **0** on the documentation branch at base `f46384c8`, using the shared
Cargo build cache and a task-owned Python environment. Its `verify` prerequisite passed.

| Check | Recorded outcome |
|---|---|
| Rust workspace suite | 2,830 passed; 0 failed; 5 ignored across 48 result groups |
| Python facade suite | 5,805 passed; 368 skipped; 44 warnings; 700.73 seconds |
| dbt adapter suite | 59 passed; 1 skipped; 386 warnings; 38.45 seconds |
| Cargo and Python dependency audits | Passed; no known Python vulnerabilities reported |
| Workflow checks | 13 workflows parse; security lint passed |
| Documentation assertions | 27 unique IDs; original section headings and decision rows retained; compatibility table byte-identical; Markdown-only scope |

No live AWS acceptance or live JVM oracle was run for this planning change. No new runtime
behavior is claimed. The test skip and warning counts above remain part of the evidence.

`SLR-2` / final Critic re-attestation: AT-1, AT-6, AT-8, and AT-10 were checked over the final
planning diff, including PROJECT's corrected pointer. All other attack categories remain
applicable as recorded. **CONVERGED:** zero open findings at or above S1 and all ten categories
accounted for. Readiness: required preflight green, C-001–C-005 traced, historical references
preserved, no runtime or permission change. Commit and push are authorized by the owner's
request to open this planning PR; merge and release publishing remain owner actions.

Disk checks: 670 GiB before checkout, 669 GiB before broad validation, 666 GiB during the build,
and 680 GiB after preflight. Shared caches are retained. The isolated review worktree is kept
for PR review. Task-owned environment and raw temporary logs can be removed after publication;
the measurements and commands needed for pickup are recorded here.
