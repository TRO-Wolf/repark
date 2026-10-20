# Production grade (repark)

What "production grade" means in this repository, and how a reviewer tests a claim to it. Filed
2026-10-13 (PROD-GRADE-DEF-1). This page sits under [PROJECT.md](../PROJECT.md) in the
[precedence chain](../AGENTS.md#precedence): the evidence list in PROJECT.md
["What production-grade means"](../PROJECT.md#what-production-grade-means) is the floor this page
builds on, and on any conflict PROJECT.md wins. The page is permanent and is amended deliberately,
never as a passenger on another change; an owner ruling that moves the definition elsewhere
retires it.

## Purpose

The repository uses "production grade" in goals, cards and reviews, but until now it had only an
evidence checklist in PROJECT.md and no definition of the term itself. This page is the
definition agents and reviewers apply when a capability claims the label, and a rubric for
reviewing that claim. **It is a definition and a review rubric, not a gate.** No new ledger, no
new check and no required artifact comes from this page: it adds no `make` target, no script, no
CI job and no document a unit must produce. The answers it asks for already live in existing
contracts and tests.

## Definition

> A capability is production grade when it delivers its documented behavior within a declared
> operating envelope, protects data during failures, makes its condition observable, and can be
> maintained and upgraded with justified confidence, and those claims are backed by repeatable
> tests and operational evidence.

The compressed heuristic: *the number of things you must know that are not written down is close
to zero.*

The review form of the same idea:

- the contract is written;
- the failure paths are tested as thoroughly as the happy path;
- a stranger reads the code without a guide;
- a change is cheap because the tests catch what matters;
- the limits are stated rather than discovered.

## The operating envelope

Production grade is relative to a declared scope: the supported workloads, data sizes,
concurrency, storage systems and deployment shapes. A capability is production grade *inside* its
envelope; outside it, the honest behavior is a refusal.

RePark is an embedded, single-node, no-JVM engine. Refusing distributed execution or a server
mode is inside the envelope, not a gap in it — see AGENTS.md
["Explicitly out of scope"](../AGENTS.md#explicitly-out-of-scope-do-not-reintroduce-without-a-decision)
and PROJECT.md ["Not now, and why"](../PROJECT.md#not-now-and-why). A clear refusal with a hint
beats a plausible wrong answer. That is already the repository's posture: PROJECT.md's
"no silent degradation" rule in
["What production-grade means"](../PROJECT.md#what-production-grade-means) and the "LOUD
unsupported errors" rule in AGENTS.md
["Delegated-agent standing rules"](../AGENTS.md#delegated-agent-standing-rules). This page does
not restate either.

## Assessed per capability, never averaged

Local analytical queries, Iceberg writes, the Spark facade, CDC and a future warehouse service
have different failure modes and need different evidence. Each is assessed on its own; there is
no single score for the engine. Strength in one attribute does not offset weakness in another:
excellent documentation does not offset a possible silent data loss.

Part of production grade is exposure to real workloads, and that cannot be bought. The honest
claim for a young project is therefore "production grade for the workloads we have exercised",
with the list of those workloads as the evidence. A claim without that list is a claim about
workloads nobody has run.

## Attributes and the evidence that earns each

| Attribute | In practice | Evidence |
|---|---|---|
| Correctness | The capability answers as its contract says: the live Spark oracle on the Spark door, the door's own contract on the native door. | Pinned tests per entry point plus adversarial testing — fuzzing and randomized differential testing against the Spark oracle, not only hand-written happy paths. |
| Data integrity | A failed or interrupted write leaves the table at its last committed snapshot, with no partial commit visible and no acknowledged data lost. | Tests that fail commits, kill writers and race concurrent writers, then read the table back and check every row. |
| Predictable failure | Every unsupported path refuses by name with a hint, and every error carries a documented class. | Refusal tests that assert the error class and message on every entry point the path is reachable from. |
| Determinism | Same input, same plan, same output, same error. | Repeat-run tests that compare plans, output bytes and error text across runs, thread counts and partition counts. |
| Resource discipline | Memory, disk and file handles stay inside a declared bound; the engine spills where it can and refuses cleanly where it cannot. | The spill-coverage matrix and torture runs at multiples of the memory limit, indexed from [perf/map.md](perf/map.md). |
| Performance consistency | Each reference workload stays within its card's stated ceiling of the committed baseline, release after release. | Committed baselines under [perf/](perf/map.md) measured as like-for-like medians against that ceiling. |
| Operability | An operator sees what the engine did, how far it got and why it stopped, without a debugger. | Tests that assert on emitted progress, metrics or log events and on the state left behind after a stop. |
| Security | Credentials never reach logs, errors or files, and a request the engine cannot honor safely refuses rather than downgrades. | Redaction tests on every error and log path, the dependency audit the pre-PR gate runs, and refusal tests for unsafe requests. |
| Maintainability and small surface | Few ways to do a thing, few abstractions, and a public API that fits in one head. | The mechanical structure gates in AGENTS.md ["Hard rules"](../AGENTS.md#hard-rules-non-negotiable) green, and a frozen-surface register that names every public name. |
| Compatibility and delivery | Data an earlier release wrote reads and writes correctly on this one, and the public surface changes only under the versioning policy in [release.md](release.md). | Compatibility tests against tables the shipped engine wrote, the API-freeze register, and wheels built and smoke-tested on every platform. |
| Documentation as product surface | Every public name and every limit is documented where a user looks first. | Executable examples that fail on drift, and guide snippets executed against a built module ([guide/](guide/map.md)). |
| Operational maturity | The capability has run on real workloads, and each incident ends in a fix, a pinned test and a recorded lesson. | A dated list of exercised workloads and a regression pin for each incident found in use. |

## Anti-patterns

Each of these disqualifies a capability from the label however well it is tested:

- **Silent fallbacks** — a path that quietly takes a slower, looser or different route instead of
  refusing.
- **Swallowed errors** — an error caught and dropped, or downgraded to a log line the caller never
  sees.
- **Panics in library code** — enforced by `make rust-panic-ban`; see the panic-ban bullet in
  AGENTS.md ["Hard rules"](../AGENTS.md#hard-rules-non-negotiable).
- **Hidden global state** — held by the everything-through-Session discipline in
  [adr/0004-server-prep-disciplines.md](adr/0004-server-prep-disciplines.md).
- **Undocumented environment variables** — any behavior switch a user cannot find in the
  configuration guides.
- **Nondeterministic or flaky tests** — a test that can pass and fail on the same input proves
  nothing; the testing contract's [forbidden patterns](testing.md#forbidden-patterns) name the
  related escape hatches.
- **Knowledge that lives only in one head** — a step, a limit or a reason that exists only in a
  conversation; the comment and prose rule in AGENTS.md
  ["Write for the eventual reader"](../AGENTS.md#write-for-the-eventual-reader--comments-docstrings-and-prose)
  says where durable reasons go.
- **A claim of "supported" with no named test** — the
  [testing contract](testing.md) requires the test in the same change.

## Values for directing agents and reviewers

- **Evidence over assertion.** A claim stands on a named test or a recorded measurement, never on
  agreement.
- **Explicitness over surprise.** A user learns a limit from a refusal or a document, never from a
  wrong answer.
- **Simplicity with purpose.** Every abstraction earns its place with a second real caller.
- **Failure is ordinary.** Crashes, conflicts and bad input are designed for and tested, not
  treated as rare.
- **Compatibility is stewardship.** Data and pipelines that worked yesterday keep working, or the
  change says so first.
- **Honest limits.** "Not supported" and "not yet measured" are valid, written answers.
- **Useful observability.** Emit what an operator needs to act on, not everything that happened.
- **Proportionate assurance.** The depth of evidence matches what a failure would cost the user.
- **Learning compounds.** Every incident leaves a pinned test and a written lesson behind.

## Five review questions

For any capability that claims the label:

1. What exactly is promised, and under which conditions?
2. What can go wrong, and what happens to the user's data and work when it does?
3. What evidence demonstrates the guarantees?
4. How does someone diagnose, recover and upgrade?
5. What remains unproven or unsupported?

The answers live in the existing contracts and tests — the capability's guide, its registry rows
in [spark-sql-iceberg-parity.md](spark-sql-iceberg-parity.md), its pinned tests and its committed
baselines. They are not a new artifact, and a reviewer asks them rather than requiring a document
that answers them.

## Reference projects

Each reference stands for exactly one practice we copy and the RePark capability it governs; a
project that cannot fill all three columns is not on the list.

| Project | The one practice we copy | RePark capability it governs |
|---|---|---|
| DuckDB | Continuous fuzzing and randomized differential testing of the engine against other engines | SQL and DataFrame correctness; the parity suite against the Spark oracle |
| Polars | Strict typing, an explicit lazy versus eager split, an error-message culture and a small public surface | The native DataFrame door and its API |
| Apache Kafka | A written design proposal before any significant change, and published protocol and API compatibility guarantees | Change process; facade and API compatibility; release policy |
| SQLite | A published testing doctrine (one page that says how the product is tested) and a long-term file-format and API promise | The testing contract; the "how we test" page this repo should eventually carry |
| ClickHouse | Per-pull-request performance tests and fuzzers in CI, with regressions blocking merge | The performance gates and their like-for-like medians |
| TigerBeetle | A written engineering style guide (assertions, limits, allocation, control flow) and deterministic simulation of failures | The engineering method and the briefs agents work from |
| PostgreSQL | Complete documentation, error codes as a stable interface, boring release and deprecation discipline | The user guides and the error-class contract |
| Apache DataFusion | sqllogictest as the test format, benchmark tracking, written design discussion before large changes | Test format and adjacent review practice; the engine RePark builds on |
| RocksDB | Crash and stress tests that kill the process mid-write and verify recovery | Iceberg write data integrity; the CDC sink |
| The Apache Iceberg specification | Spec first, versioned format, explicit compatibility rules | Iceberg parity as a product claim; format-version support |

## Next step

A per-capability maturity label (three levels, each citing the tests that justify it) belongs in
[STATUS.md](../STATUS.md) and is a separate unit; this page does not create it.
