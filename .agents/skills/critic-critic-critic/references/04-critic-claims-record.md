# Critic-4 — Claims & Record (fourth critic)

**Charter:** attack every CLAIM the change makes about itself — in ledgers, maps, STATUS-class
docs, docstrings, doc comments, commit messages, author/trailer claims, and reports — against
the TREE, by re-execution where possible. Critic-1/2/3 attack the code; Critic-4 attacks the
paper. A change whose code is right and whose record is wrong is NOT clean: the record is what
every future session acts on.

**Finding prefix:** `CL` (e.g. `W1-CL-001`). Same context-break and evidence doctrines as the
other critics. Spawn as `explore` (needs `git log`); never `capability_mode: read-only` if the
prompt orders git. Default **on** for ledger-bearing units (COMPLETE, unit ledger, map.md
claim, STATUS-class record, §6 registry row). Opt-out only by explicit `claims_critic=false`.
When on, joins every findings triad as a quad under the same mutual-exclusion rules.

## Method — claims are guilty until evidenced

1. **Inventory the claims.** Grep the diff + its ledger/report for claim verbs and quantifiers:
   "fixed", "corrected", "every", "all", "never", "unchanged", "green", "done", "no longer",
   "is now", "verified", counts ("nine files", "352 tests"), and file/path citations. Each is a
   row in your worklist.
2. **Verify each against the tree, not the narrative.** A claim that file X was corrected →
   `git diff --name-only` must CONTAIN X, and the correction must be in it. A claim of a green
   gate → re-run it (or a targeted subset) and match the claimed counts. A claimed transcript →
   re-execute a sample and compare exit codes, counts, and key lines. A claim of **author
   identity** (or “identity PASS”) → `git log --format='%ae'` across the **unit branch**, not
   `%an` and not the commit-message text. Name-only PASS is incomplete (CL-IDENTITY).
3. **File the divergence, not a paraphrase.** Quote the claim, quote the tree, name the gap.

## The taxonomy (observed classes, each with its real shape)

- **CL-MANDATE — mandated item claimed done, tree untouched.** The charter/map orders an edit to
  file X; the ledger says "corrected in this diff"; X is not in the diff. (Observed: a mandated
  wording fix in a second crate's router doc — three separate documents claimed it landed; the
  file was never edited. The correction block itself was the fourth false claim.)
- **CL-QUANT — quantifier broader than the mechanism.** "every exit path" where unwind/drop are
  not covered; "all rows" where a filter exists; "always" where one arm returns early. Demand
  the honest quantifier ("every `?` / `return` path").
- **CL-STALE — present-tense claims outlived the fix.** STATUS/maps/design docs still describing
  a fixed defect as open (or an open one as fixed), a "known limitations" row for a closed
  limitation, an archived template endorsed as clean when it was not.
- **CL-RATIONALE — a flagged deviation with an invented reason.** Flagging a deviation is
  honest; justifying it with a constraint that does not exist is not. Re-derive the claimed
  constraint from the code; if it does not hold, file it even when the deviant CODE is correct —
  the escalation deserves a true reason. (Observed: an ordering constraint invoked to justify a
  design choice; the constraint was not real, the choice was defensible for other reasons.)
- **CL-TRANSCRIPT — evidence that does not replay.** Claimed gate counts, mutation transcripts,
  or provocation outputs that cannot be reproduced from the shipped tree; restore-proof hashes
  that do not match; a docstring claiming "both assertions red" when a panic can only ever show
  one.
- **CL-COUNT — arithmetic drift.** "nine files" vs a ten-row table; test counts that do not
  match collection; a budget claim vs the measured set.
- **CL-DUALHOME — one fact, two authoritative homes, diverging.** The same divergence/decision
  described in two places with different wording or disposition; a correction filed in one home
  while the other still carries the old claim. Includes corrections that are themselves
  overclaims ("both halves are now fixed" while a stated residual remains).
- **CL-VACUOUS — a test/assertion presented as proof that cannot fail the claimed way.**
  Refusal tests that pass for the wrong reason (missing config vs the actual refusal), coverage
  pins satisfiable by a control row, non-vacuity sourced from the very bug under test.
- **CL-GHOST — citations to files/anchors that do not exist** on any ref (never-written ledgers,
  moved paths with no redirect, wrong section anchors).
- **CL-IDENTITY — author/trailer claim checked at the wrong resolution.** “Author — PASS” that
  grepped `%an` (or commit-message text) but not `%ae`. The claim is clean only when every
  commit on the unit branch has `git log --format='%ae'` equal to the identity the repository's
  own git configuration sets (`git config user.email` at the repo root) **byte-exact**. Also
  verify that any attribution trailer the repository's hooks permit names what it claims to
  **as read at commit time** (not a skill/role literal) — and that no trailer the hooks forbid
  is present. Observed: a completeness critic passed name-only while two lanes carried two
  different emails, one of them a machine-local form.

Conductor / completeness hard-check: grep **author emails** (`%ae`) across the branch, not
just names. A name-only row is not a null report for this class.

## Null report

Attest per class what was inventoried and how verified (grep patterns, re-runs performed,
sample sizes). "No claims checked" is not a null report; it is an incomplete pass.
