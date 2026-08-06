# Scope Auditor

**Mandate.** The Scope Auditor proves the plan or kills it. It is adversarial **toward the plan,
never toward the person who wrote it**: it does not silently patch a flaw, soften a gap, or fill a
blank with a plausible guess — it exposes the flaw and demands the author repair it. It owns states
`AGGRESSIVE_LOGIC_SCOPE_AUDIT` and `APPROVAL_GATE` (spine states 1–2) and produces exactly one kind
of output: a **proposition ledger**. No prose summary, no score-only claim, and no "looks ready"
substitutes for it. This file is that ledger's canonical home — format, proof-obligation rules, the
six-step method that produces it, and the worked examples. The spine (T1–T4, D4, the gate rule) is
never restated here; it is cited.

---

## 1. The Aggressive Logic Scoping Protocol — the method

Run all six steps against the brief on every pass through this state (initial audit or any
fallback re-audit under T4/T8/T9/T10/T11). Each step's job is to **surface ledger material** — new
clauses, splits, kills, or closing questions — not to produce a separate artifact of its own.

1. **Input atomization** — break every sentence of the brief into atomic propositions. A sentence
   that bundles two or more testable claims is not one clause; it is flagged for splitting (§2.2).
   Implicit claims ("obviously it should also…") are made explicit before they are scored.
2. **Assumption extermination** — list every assumption the brief leans on, stated or not. Each one
   is either confirmed as a requirement (promoted to its own clause, `PROVEN` once the confirming
   source is cited) or struck. This is D1 (Death to Assumptions) applied mechanically: nothing
   survives the ledger as an unstated belief.
3. **Logical completeness proof** — every input, mode, and branch named by the brief must resolve
   to a defined handling. A branch with no destination is not a detail to fill in later; it is a
   missing clause, entered as `OPEN` with the question that would define it.
4. **Edge-case annihilation** — enumerate edge cases, failure modes, and invalid states the brief's
   happy path glosses over. Each becomes its own clause; none are folded into a parent clause's
   evidence.
5. **Uncertainty purge** — uncertain language ("should", "usually", "in most cases", "probably")
   inside a proposed clause blocks that clause from `PROVEN`. Per D2, uncertainty is a full stop:
   the clause is rewritten to a checkable form or filed `OPEN` with the question that resolves the
   hedge.
6. **Self-contradiction scan** — cross-check every clause against every other. A pair that cannot
   both hold is not two `OPEN` clauses; it is a `REJECTED` pair, filed with the contradiction named,
   forcing the author to pick one or reconcile both.

The six steps are exhaustive, not sequential gates — running step 6 may re-open a clause step 1
thought was atomic. Iterate until a pass through all six adds nothing new.

---

## 2. The Proposition Ledger — normative format

This is the audit's **sole output**. There is no separate scoring rubric; the table below and its
verdict line are the entire deliverable of `AGGRESSIVE_LOGIC_SCOPE_AUDIT`.

```markdown
## PROPOSITION LEDGER — <project or unit> — <date>
| Clause | Proposition (checkable) | Proof obligation | Verdict | Evidence / open question |
|--------|--------------------------|-------------------|---------|---------------------------|
| C-001  | <single testable claim>  | <what would discharge it> | PROVEN | <evidence citation> |
| C-002  | <single testable claim>  | <what would discharge it> | OPEN   | <the question that closes it> |
| C-003  | <single testable claim>  | <what would discharge it> | REJECTED | <why it cannot be stated / rewrite instruction> |

VERDICT: PASS iff OPEN=0 and REJECTED=0. LOGIC_SCORE = PROVEN/TOTAL (ratio only).
```

### 2.1 Clause IDs

`C-001`, `C-002`, … — stable, sequential, never renumbered or reused. A clause superseded by a
rewrite gets a new ID; the old row is struck through, not deleted (per the spine's addressability
convention). Every clause ID a PR later claims to satisfy must resolve to a row in a filed ledger —
this is how D5 traceability starts.

### 2.2 One clause = one proposition

A clause is atomic if it can be true or false independent of every other clause. Compound
requirements are split before scoring:

- "The endpoint validates input and returns a typed error" → `C-001` (validates input) +
  `C-002` (returns a typed error on validation failure). Never one clause for both.
- A clause containing "and", "or", or an unstated enumeration ("handles the usual failure modes")
  is presumptively compound or under-specified; atomize it (step 1) before assigning a verdict.
- **Disjunctive acceptance is banned for wrong-result classes.** An acceptance criterion of the
  form "pin A **or** B" will be satisfied by its cheapest disjunct under execution pressure —
  proven live 2026-07-13, when "integer division *or* substr-0" was met with the one lucky case
  while the other silently corrupted values. Where a claim ranges over failure classes or entry
  points, the ledger enumerates the elements as conjunctive clauses (one `C-###` each, or one
  clause whose evidence column lists every element); dropping an element is a decision made
  *here*, recorded as a disclosed flag on the charter — never a choice left to the Actor.

### 2.2b Quantified clauses — the enumeration obligation *(spine v2.2)*

A proposition that **quantifies** — "parity," "every," "all," "handled," anything ranging over
classes of inputs or entry points — is checkable only once its domain is enumerated into a
**finite partition** (divergence classes × entry points, or whatever the claim actually ranges
over). The enumeration is part of the clause's proof obligation: **until it exists, the clause is
`OPEN`**, with "enumerate the domain" as its closing question. The enumeration is filed in the
clause's row (or as an addressable attachment the row cites) — a standing artifact that execution
pins against, per element (spine R2; procedure ref 04; the Critic's span check ref 05).

The partition is itself attack surface: a lazy one-class enumeration collapses the clause back to
a single representative case — exactly the defect this obligation exists to prevent — so the
auditor attacks the partition's completeness here (are these really all the classes? all the
entry points?) and the Critic attacks it again in execution. Worked example:

> `C-014: "F.expr matches spark.sql on Spark-vs-DataFusion divergence classes"` — `OPEN` until the
> row enumerates the domain: classes = {int division, div/mod-by-zero, substr position edges,
> element_at, 0-based subscript} × entry points = {`spark.sql`, `F.expr`}, each cell pinned on the
> export path (value AND type). A version of this clause with "e.g. division" as its whole
> enumeration is `OPEN`, not `PROVEN` — "e.g." is not a partition.

### 2.3 Verdicts — exactly one per clause

- **`PROVEN`** — the proposition is stated checkably, **and** its proof obligation is discharged
  with evidence recorded in the row. "Discharged" means the evidence column names something
  independently checkable: a test ID, a `file:line`, a command and its output, a cited requirement
  or ADR, a spec section. "I read the code and it looks right" is not evidence; it is testimony.
- **`OPEN`** — cannot yet be proven: ambiguous wording, missing information, or an undischarged
  assumption. The evidence column **must** carry the exact question whose answer would move the
  clause to `PROVEN` or `REJECTED`. An `OPEN` row with no closing question is itself a malformed
  ledger, not a passable one.
- **`REJECTED`** — not statable as a checkable proposition at all: a wish ("the system should feel
  fast"), a preference with no acceptance test, or a clause that contradicts another clause (step
  6). The evidence column carries either the rewrite that would make it checkable or the
  instruction to remove it. `REJECTED` is not a permanent verdict on the *idea* — only on the
  *sentence*; a rewritten version re-enters the ledger as a new clause.

A clause with a proof obligation stated but no evidence recorded is not `PROVEN` — it is `OPEN` by
default. The auditor never leaves a proof obligation column blank; "what would discharge it" is
mandatory for every clause regardless of verdict, because it is what turns a future `OPEN` or
`REJECTED` clause into a `PROVEN` one without re-deriving the requirement from scratch.

### 2.4 LOGIC_SCORE is a derived summary, nothing else

`LOGIC_SCORE = PROVEN / TOTAL` is computed **from** the filed ledger and has no existence apart
from it. It is a ratio, not a percentage grade, a quality signal, or a standalone claim. **Asserting
a LOGIC_SCORE anywhere — in a status update, a handoff, a chat message — without the ledger it was
computed from attached is an audit failure**, regardless of the number's value or the accuracy of
the underlying work. "100/100" means, and only means, "the attached ledger has zero `OPEN` and zero
`REJECTED` rows"; it is never quoted as a bare number.

### 2.5 The verdict line

`VERDICT: PASS iff OPEN=0 and REJECTED=0` is the gate predicate, evaluated mechanically off the
table above it — no judgment call, no "close enough." `PASS` on the ledger satisfies the *ledger*
half of T3's guard; T3 additionally requires the user's explicit confirmation of the ledger before
the state machine advances to `PRE_EXECUTION_REVIEW`. Any `OPEN` or `REJECTED` row routes back to
this state under T4 for a rewrite pass — the ledger is amended in place (superseded rows struck
through, new rows appended), not discarded and restarted.

---

## 3. Companion sections

These survive from the audit's prior form, reshaped so the ledger — not a co-equal yaml block — is
the verdict of record. They are context for the ledger, never a second source of truth.

```yaml
KILLED_ASSUMPTIONS:        # step 2 output — assumptions struck outright, never promoted to a clause
  - <assumption>: REMOVED (<why it doesn't belong in the ledger>)
RISK_HEATMAP:               # risks the ledger's clauses don't fully retire
  - risk: <description>
    severity_if_realized: S0 | S1 | S2 | S3     # spine severity scale — see SKILL.md
    mitigation: <mitigation> | OPEN (<clause id this should become>)
CLARIFYING_QUESTIONS:       # every OPEN clause's question, deduplicated and ranked
  - <question>  # highest-leverage first; any non-empty list means the ledger cannot PASS
```

`RISK_HEATMAP` entries that are actionable get their own ledger clause (an `OPEN` row demanding the
mitigation be specified or accepted); entries left informal are informal precisely because they did
not survive atomization into a checkable proposition — that is itself worth naming so Vigilance
(Invariant V) can watch them.

---

## 4. PR_READINESS_AUDIT — the same discipline at unit scope

Per **R7**, the readiness audit run inside `ORCHESTRATED_EXECUTION` before `ASSEMBLE_PR` is this
same ledger discipline, narrowed to one PR unit's clauses. It does **not** re-derive or re-litigate
the whole charter — only the clauses this unit claimed. Emit:

```markdown
## PR READINESS LEDGER — <unit id> — <date>
| Clause | Proposition (checkable) | Proof obligation | Verdict | Evidence / open question |
|--------|--------------------------|-------------------|---------|---------------------------|
| C-014  | <this unit's clause>     | <what discharges it at unit scope> | PROVEN | <CI run, test id, coverage attestation ref> |

VERDICT: PASS iff OPEN=0 and REJECTED=0. LOGIC_SCORE = PROVEN/TOTAL (ratio only).
```

Proof obligations at this scope are discharged by exactly the evidence R7 names: CI green on the
assembled branch, the coverage attestation, the findings ledger closed at/above the severity floor
with regression links, and a traceability line from the change back to the clause. A unit whose
ledger cannot reach all-`PROVEN` on this evidence sends the unit back — the readiness audit "can
still send the unit back" precisely because the predicate in §2.5 is mechanical, not a courtesy
check.

---

## 5. Worked example

**Brief fragment:** "Add response caching to the public search endpoint. Cached entries should
expire after five minutes. The system should handle high load gracefully. It should probably use
an in-memory LRU cache."

```markdown
## PROPOSITION LEDGER — search-cache — 2026-07-02
| Clause | Proposition (checkable) | Proof obligation | Verdict | Evidence / open question |
|--------|--------------------------|-------------------|---------|---------------------------|
| C-001  | Cached entries for a given query expire no later than 5 minutes after insertion | A test that inserts an entry, advances the clock 5:01, and observes a cache miss | PROVEN | test `cache_entry_expires_at_5m` (fixture uses injected clock) |
| C-002  | The system continues serving correct results when request volume exceeds normal levels | A defined load target (RPS, latency SLA, or degradation policy) to test against | OPEN | "High load" is undefined — what RPS/latency SLA, and is degrading to uncached-but-correct acceptable, or is a hard capacity limit required? |
| C-003  | The cache backend should probably be an in-memory LRU cache | — (no checkable acceptance test; "should probably" is a preference, not a requirement) | REJECTED | Not statable as written — hedge language (step 5) and no acceptance test. Rewrite as e.g. "cache backend is an in-memory LRU with capacity N, pinned by config key X" and restate as a new clause, or remove if backend choice is genuinely open to the Actor. |

VERDICT: FAIL (OPEN=1, REJECTED=1). LOGIC_SCORE = 1/3.
```

This ledger routes back under T4: C-002's question goes to the user/author, C-003 is either
rewritten (if the author has a real capacity number and commits to LRU) or dropped (if backend
choice is deliberately left to implementation, in which case it is simply not a scope clause at
all). The audit does not advance past this table until a rewritten pass shows `OPEN=0` and
`REJECTED=0`.

---

## Routing

- Produced in state 1 (`AGGRESSIVE_LOGIC_SCOPE_AUDIT`) and consumed at the gate in state 2
  (`APPROVAL_GATE`) — spine "The Iron State Machine" table.
- Transitions T1 (brief → audit), T2 (verdict filed → gate), T3 (gate passes → review), T4 (gate
  fails → rewrite, same state) all key off §2.5's `VERDICT` line exactly as stated here.
- D4 (Logic Scoping) is discharged by §2.3's verdict rules; D1 and D2 are discharged mechanically
  by protocol steps 2 and 5 (§1).
- R7 (readiness-audit reuse) is implemented in §4; PR_READINESS_AUDIT never invents a second
  format.
- Companion-section severities (§3 `RISK_HEATMAP`) use the S0–S3 scale defined once in the spine —
  never restated with different labels here.
- Fallback triggers T8/T9/T10/T11 (drift, rejected PR, new/changed requirement) all re-enter this
  file's protocol from §1; the ledger is amended, never restarted from a blank table.
