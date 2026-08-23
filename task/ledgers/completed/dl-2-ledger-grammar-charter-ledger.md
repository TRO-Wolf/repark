# Charter ledger — DL-2 · the ledger grammar, checked by a script

**Date:** 2026-08-23 · **Branch:** `feat/dl-2-ledger-grammar` (stacked on `feat/dl-1-ledger-lifecycle`,
PR #221) · **Base:** `3f1aa22` · **Policy:** [../../../AGENTS.md](../../../AGENTS.md) "Markdown document
lifecycle" · **Governs:** [../../../skills/sepmo/SKILL.md](../../../skills/sepmo/SKILL.md) "The gate
is a ledger, not a score" and [../../../skills/sepmo/references/05-critic.md](../../../skills/sepmo/references/05-critic.md)
"Coverage attestation format"

**Retires:** this ledger moves to `../completed/` in the unit's last commit; the first pickup after
the merge archives it.

The owner's SEPMO architecture note (2026-08) asked for deterministic, regex-extractable ledgers and
scripts that take over the mechanical half of the Scope Auditor's and Critic's work. The assessment
that ruled this unit's shape (same day) accepted a declared grammar, a pin-citation binding and a
checked attestation form, and **declined XML** as the carrier. This ledger records what was measured,
what the grammar is, and what the script enforces.

## 1. Measured (2026-08-23, at `3f1aa22`)

| Measure | Value |
|---|---|
| live charters in `staging/` with a clause table | 3 (`fnp-0`, `mw-0`, `sem-0`); `v3-0` and `dl-1` carry none |
| clauses in them | **32**: 31 `PROVEN`, 1 `OPEN` (`sem-0` C-110, the ceiling-risk clause; `fnp-0` C-007 was closed by ruling D-7, which `staging/map.md` still called open) |
| distinct clause-table layouts in use | **3** — `ID · Clause · Proof obligation · Verdict` (fnp-0), `# · Clause · State · Evidence` (mw-0, sem-0), `Clause · Proposition · Proof obligation · Verdict · Evidence` (SEPMO-run unit ledgers, e.g. A13) |
| archived ledgers with a clause table | 36 (immutable; out of scope by the DL-1 rule) |
| ledgers that ever filed a `COVERAGE_ATTESTATION` block | **2 of 127** (`r6`, `w3-g3e8-pr2`); the Octo mode records the Critic as prose |
| test files citing a charter clause by id | **0** — no convention exists |
| verdict cells that are not a bare token | 12 of 32 — `**PROVEN** (enumeration complete)` and the like; a ruled charter annotates its verdicts |
| `OPEN` rows without a `?` | 1 of 1 — the closing condition is stated, not asked |

Three conclusions. The column *names* differ but the row shape does not: every row begins with a
`C-NNN` id, carries exactly one verdict token and at least one evidence cell — so the grammar is
declared on the row, not the header, and no live charter needs rewriting. The attestation format
already exists in ref 05; what is missing is anything that checks it, which is why it is filed in
2 ledgers of 127. And two sub-rules drafted before measuring — "an `OPEN` row carries a `?`" and
"a quantified `PROVEN` clause names its enumeration" — were **declined on the live input**: the one
`OPEN` row states its closing condition as a sentence, and whether a domain is enumerated is the
Scope Auditor's reading, not a token a regex can find. A form rule that fakes a meaning is worse
than none.

## 2. Rulings (owner, 2026-08-23)

1. **Markdown stays the carrier; XML declined.** Every gate in the repository is markdown-aware
   (link validity, the DL-1 lifecycle and its link rewriter, map lockstep, typos); XML ledgers would
   sit outside all of them, double the bytes, and be unreadable in PR review. The "LLMs were trained
   on XML" argument is about prompt structure, not storage; a `<proposition>` tag in a prompt and a
   table row in a file are not in tension. Recorded here as measured-and-declined.
2. **Scripts check bindings; they do not generate tests.** No script turns "the eleven higher-order
   functions evaluate correctly" into a test. What a script can prove is that every `PROVEN` clause
   is cited by a test and every citation names a clause that exists.
3. **Property-based testing is a technique the Actor chooses** where a domain is enumerable, not a
   derivation from prose; it is not part of this unit.

## 3. The grammar — `scripts/check_ledger_grammar.py`, `make check-ledger-grammar`

Scope: every tracked `*-ledger.md` under `task/ledgers/staging/` **and** `completed/` — the live
bins; a ledger retires into `completed/` in its own departure commit, so CI meets it there (the
archive is immutable and is read for citations only). Three rules:

**A. Clause rows.** A clause table is any markdown table whose data rows begin with `| C-NNN |`.
Each row: the id is unique within the ledger; exactly one cell is a verdict — `PROVEN`, `OPEN` or
`REJECTED`, bold allowed, optionally followed by a parenthetical note; at least one other cell
besides the clause text is non-empty (the evidence or proof obligation). A staging ledger not in
`EXCEPTIONS` must carry at least one clause row: scope is a ledger of propositions before any work
(SEPMO "The gate is a ledger, not a score"), now mechanically.

**B. Pin binding.** A test cites a clause with the token `pins: <unit>/C-NNN` (comma-separated
further ids inherit the unit), where `<unit>` is the ledger's filename without `-ledger.md` and, in
the archive, without its date prefix — `pins: a13-shared-ctas-fallback/C-003, C-004`. The checker
scans every tracked file under `crates/`, `python/` and `scripts/`. Every `PROVEN` clause in a
staging ledger must be cited at least once; every citation anywhere must resolve to a clause that
exists in any bin (`staging/`, `completed/`, the archive). The measured floor — 31 unpinned `PROVEN` clauses across three
charters — is seeded as a per-ledger ceiling in the script's `EXCEPTIONS` table (the PYC-6 pattern:
down only, a row is deleted when it reaches zero, a ledger not in the table allows zero).

**C. Attestation form.** A `COVERAGE_ATTESTATION:` block (ref 05's YAML shape, inside a fenced
block) must list every category `AT-1` … `AT-10` exactly once; each has `status: ATTACKED` with a
non-empty `artifacts:` list, or `status: N/A` with a `justification:`; `complete:` is `true` iff
every category satisfies that. Presence is required for a staging ledger **not** in the
`EXCEPTIONS` table once none of its clauses is `OPEN` — the attestation is the Critic's artifact
and comes after the Actor's work, so a charter whose clauses are still `OPEN` is not asked for one
(the four live staging ledgers predate the rule and are listed; this ledger is not).
A `FINDING:` record, where present, carries `id`, `severity` in `S0..S3`, `category` in
`AT-1..AT-10`, at least one `clause`, and a `disposition` from ref 05's enumeration.

Exit 0 clean / 1 findings / 2 usage. Armed in `make ci`; the ci.yml half is an owner-scoped
`.github/` change. Provocation proofs per [../../../docs/testing.md](../../../docs/testing.md) on a
scratch tree, one per rule and direction.

## 4. What the skill stops describing

The Scope Auditor reference and the Critic reference keep the *meaning* of the verdicts, the
taxonomy and the attestation; the *shape* is now the script's, and both references point at it. The
pin-citation convention is homed in [../../../docs/testing.md](../../../docs/testing.md) (it is a
testing-discipline fact), and AGENTS.md's ledger row gains the pointer to the gate.

## 5. Commits, in order

1. This charter + the staging map row.
2. `scripts/check_ledger_grammar.py` + provocation proofs + `make check-ledger-grammar` in `make ci`
   (the tree passes at arming: the floor is seeded, not hidden), `scripts/map.md`.
3. docs/testing.md "Pinning a charter clause" + AGENTS.md pointer (contract edit, alone).
4. SEPMO ref 01 / ref 05 / SKILL.md pointers + the declined XML line.
5. Departure: this ledger's clause table pinned by the unit's own tests, its attestation filed,
   `move`d to `completed/`; STATUS and the slate trued up.

Size **S/M**: one script (~300 lines), one gate, four doc edits, no migration.

## 6. Proposition ledger — DL-2

| Clause | Proposition (checkable) | Proof obligation | Verdict | Evidence |
|---|---|---|---|---|
| C-001 | A clause row whose verdict cell is not one of `PROVEN` / `OPEN` / `REJECTED` (bold or a parenthetical allowed) fails the gate; a duplicate id fails it. | Provocation: plant a row with verdict `DONE`, and a repeated id; red. | PROVEN | `test_verdict_cell_and_duplicate_id_go_red`, `test_row_without_evidence_goes_red` |
| C-002 | Every live staging charter passes rule A unchanged (no rewrite of a ruled charter). | Run on the tree at arming: 32 clauses, 0 rule-A findings. | PROVEN | `0635cd8`: `ledger-grammar: 5 staging ledgers clean (38 clauses, 5 pinned clause ids, 4 exception rows)` — no charter edited; `test_clean_shape_is_green_and_counts` |
| C-003 | A `PROVEN` clause in a staging ledger with no `pins:` citation fails the gate once its ledger's ceiling is exhausted; a citation naming a nonexistent clause fails it always. | Provocation both directions. | PROVEN | `test_unpinned_proven_clause_and_dead_citation_go_red`, `test_archived_and_completed_clauses_can_be_cited` |
| C-004 | A `COVERAGE_ATTESTATION` block missing a category, or `ATTACKED` without artifacts, or `N/A` without justification, or `complete: true` over an incomplete list, fails the gate; a staging ledger outside `EXCEPTIONS` with no `OPEN` clause and no block fails; one with no clause table fails. | Provocation per case. | PROVEN | `test_attestation_required_once_no_clause_is_open`, `test_attestation_shape_defects_go_red`, `test_ledger_without_a_clause_table_goes_red`, `test_finding_record_fields_are_checked`, `test_refuses_to_pass_closed_with_no_ledgers` |
| C-005 | The `EXCEPTIONS` ceilings only ratchet down: a ceiling above the measured count fails the gate, and a row for a ledger no longer in `staging/` fails it. | Provocation: raise a ceiling; name a gone ledger; red. | PROVEN | `test_exceptions_table_ratchets_down_only` (against the real tree) |
| C-006 | Every clause in this table is cited by a test (`pins: dl-2-ledger-grammar-charter/C-NNN`) and the attestation is filed, so the unit passes its own rules B and C with no `EXCEPTIONS` row. | `make check-ledger-grammar` green with this ledger in `staging/`. | PROVEN | this commit's run, §7; every test above carries its `pins:` line |

The verdicts were `OPEN` at charter time by construction — a proposition is `PROVEN` when its
evidence exists — and flipped in the departure commit, which is also when the attestation below
became required of this ledger and was filed. VERDICT: PASS (OPEN=0, REJECTED=0).

## 7. Execution record (2026-08-23, same day)

Five commits on `feat/dl-2-ledger-grammar`, stacked on DL-1's `3f1aa22`: `866b1b6` charter,
`0635cd8` script + 11 provocation proofs + `make check-ledger-grammar` in `make ci` (the `ci:`
list verified this time, not only the comment), `1db4b4b` docs/testing.md "Pinning a charter
clause" + the AGENTS.md pointer, `e96288e` the binding-manifest row (the SEPMO references are
portable and were not edited), then this departure. Tree at departure: **6 staging ledgers
clean (44 clauses, 6 pinned clause ids, 4 exception rows)** — the six pinned ids are this
ledger's, cited from the tests that prove them. Two rules drafted before measuring were declined
on the live input (§1). No live charter was edited. The gate's first catch was this ledger: at the
first departure run C-006 was `PROVEN` with no citation — exactly the defect rule B exists for —
and the clean-shape test now cites it. Its second catch was the departure `move` itself: a ledger in
`completed/` was not a place a citation could resolve, so every `pins:` line went red the moment the
unit retired its own ledger — `completed/` now counts, and the citation test covers it. Which
raised the third: the departure commit is the one CI sees, and in it the ledger is already in
`completed/` — so the rules run over both live bins, not `staging/` alone.

**The Critic's pass** ran in-session under the procedural break (binding manifest
`context_break_mechanics`); the S0 class here is *a gate passing closed*, so the fresh input was
the empty-tree run (`test_refuses_to_pass_closed_with_no_ledgers`, exit 2), absent from the
Actor's first test set.

```yaml
COVERAGE_ATTESTATION:
  pr_unit: DL-2
  categories:
    - id: AT-1
      status: ATTACKED
      evidence: >
        Walked C-001..C-006 against the script's behaviour; each clause's provocation is a named
        test and the tests cite the clause they pin, so the walk is mechanical from here on.
      artifacts: [python/repark-parity/tests/test_dl_2_ledger_grammar.py, scripts/check_ledger_grammar.py]
    - id: AT-2
      status: ATTACKED
      evidence: >
        Annotated verdict cells (12 of 32 live), a bare-token cell, a duplicate id, a row with no
        evidence cell, a fenced block containing a clause-shaped line, a citation to an archived
        unit (date prefix stripped), a citation to a nonexistent clause, an empty tree.
      artifacts: [test_verdict_cell_and_duplicate_id_go_red, test_row_without_evidence_goes_red, test_archived_and_completed_clauses_can_be_cited, test_refuses_to_pass_closed_with_no_ledgers]
    - id: AT-3
      status: ATTACKED
      evidence: >
        The one failure path that matters for a gate is passing closed: no ledgers at all exits 2;
        an unreadable or binary file under the citation roots is skipped, not fatal.
      artifacts: [test_refuses_to_pass_closed_with_no_ledgers, "scripts/check_ledger_grammar.py citations()"]
    - id: AT-4
      status: N/A
      justification: stateless, single-process, reads only; inputs are git's sorted ls-files.
    - id: AT-5
      status: N/A
      justification: reads tracked files, writes nothing, runs no subprocess but git ls-files.
    - id: AT-6
      status: ATTACKED
      evidence: >
        The live charters are the compatibility surface: three header layouts and annotated
        verdicts were measured before the grammar was declared, and the arming run edited none
        of them.
      artifacts: ["0635cd8 ledger-grammar: 5 staging ledgers clean (38 clauses ...)", "ledger §1"]
    - id: AT-7
      status: N/A
      justification: one pass over ~170 text files under the citation roots; sub-second, measured in `make ci`.
    - id: AT-8
      status: ATTACKED
      evidence: >
        The attestation and finding shapes are ref 05's verbatim; the EXCEPTIONS contract follows
        check_docstring_presence (down only, stale row red); a ledger outside EXCEPTIONS with no
        table is red, so the SEPMO gate rule is honoured and not merely described.
      artifacts: [test_exceptions_table_ratchets_down_only, test_ledger_without_a_clause_table_goes_red, test_finding_record_fields_are_checked]
    - id: AT-9
      status: ATTACKED
      evidence: >
        Every finding names the file, the line for a row, the clause id and the rule; the summary
        line carries the counts a retrospective will want.
      artifacts: ["scripts/check_ledger_grammar.py run()"]
    - id: AT-10
      status: ATTACKED
      evidence: >
        Each clause is pinned by at least one named test and the gate itself checks that binding
        on this ledger (C-006); the two declined sub-rules are recorded so their absence is a
        decision, not a gap.
      artifacts: [test_clean_shape_is_green_and_counts, "ledger §1"]
  reattested: []
  complete: true
```
