# metrics — the process metrics ledger

The quantitative half of every retrospective. One section per retrospective, appended; an earlier
section is never rewritten, only superseded by a dated note. This file is the **single home** for
these numbers — a retrospective narrates them and links here, it does not restate them.

The metric set is fixed at **eight** by the SEPMO retrospective contract
([../.agents/skills/sepmo/references/08-retrospective.md](../.agents/skills/sepmo/references/08-retrospective.md)
§3), and this file is the location the binding manifest's `metrics_ledger_location` row names
([../.agents/skills/sepmo/binding-manifest.md](../.agents/skills/sepmo/binding-manifest.md)). No metric is optional
and none is dropped for having no events: an empty population is recorded as `0` **with its reason**,
never as an absent row. `status: COMPLETE` on a section asserts exactly that.

---

## ML-RETRO-1 — the Agent-Agnostic Front-Door campaign (2026-08-10)

**Covers:** FD-1…FD-5, merged 2026-08-09 (#24, #25, #26, #28, #29). **Retrospective:**
[../docs/history/frontdoor/retrospective.md](../docs/history/frontdoor/retrospective.md).
**Filed:** 2026-08-10, at the campaign's close-out. This is the first section in this file, so the
file was CREATE at this retrospective.

> **Reconstruction caveat — read the severities with this in mind.** The campaign ran adversarial
> passes on all five units but did **not** record SEPMO-shaped finding ledgers: no `F-<unit>-<n>`
> identifiers, no severity labels, no explicit dispositions. Severities below were assigned *by the
> retrospective* from the PR bodies and the one unit ledger the campaign filed
> ([../docs/history/frontdoor/fd3-ledger.md](../docs/history/frontdoor/fd3-ledger.md)); FD-5's
> individual nits are unrecoverable. The counts are sound; the severity distribution is an
> interpretation. The gap is itself a finding and drives FF-1 in the retrospective's §7.

```yaml
METRICS_LEDGER:
  id: ML-RETRO-1
  covers: [ FD-1, FD-2, FD-3, FD-4, FD-5 ]
  note: >
    Severities reconstructed by the retrospective from PR bodies and the FD-3 unit ledger;
    the units did not record severity labels. See the reconstruction caveat above.

  findings_per_cycle:
    - pr_unit: FD-1                       # verification FULL
      cycles: [ { cycle: 1, S0: 0, S1: 1, S2: 0, S3: 0 },
                { cycle: 2, S0: 0, S1: 0, S2: 0, S3: 0 } ]
      detail: "S1 — a surviving front-door status claim that defeated the unit's own
               one-authoritative-statement gate; fixed pre-open."
    - pr_unit: FD-2                       # verification FULL
      cycles: [ { cycle: 1, S0: 0, S1: 1, S2: 1, S3: 0 },
                { cycle: 2, S0: 0, S1: 0, S2: 0, S3: 0 } ]
      detail: "S1 — a private-repo ADR number cited in a public document (hygiene leak);
               S2 — a stale Status-SSOT row. Both fixed pre-open. Attested clean with no
               findings: all three runtime flows and the component dependency claims,
               verified against source."
    - pr_unit: FD-3                       # verification FULL
      cycles: [ { cycle: 1, S0: 0, S1: 2, S2: 2, S3: 4 },
                { cycle: 2, S0: 0, S1: 0, S2: 0, S3: 0 } ]
      detail: "S1 x2 — the two demonstrated gate bypasses (B-1 unvalidated role vocabulary,
               B-2 name-prefix scope); S2 x2 — the [project] type check that silently skipped
               the whole STATUS agreement rule, and the glob-member filter; S3 x4 — tier
               wording, two deliberate tier clauses, debug-table error strings, ledger wording.
               All closed in-unit; B-1/B-2/N1 re-proven by P-8/P-9/P-10."
    - pr_unit: FD-4                       # verification FULL
      cycles: [ { cycle: 1, S0: 0, S1: 3, S2: 3, S3: 2 },
                { cycle: 2, S0: 0, S1: 0, S2: 0, S3: 0 } ]
      detail: "S1 x3 — hand-written headline counts wrong in the lossless-archival instrument
               (96 -> 126, now generated); a housekeeping claim headed into STATUS.md that
               inverted its archived source; a '$-metadata pinned in both doors' claim whose
               second pin is the bare core session. S2 x3 — a rider wrongly recorded as
               discharged (12 stale v1 crate references); the release doc still describing the
               pending-publisher flow for a name that already exists; the census-baseline map
               still marked DEFECTIVE for a directory STATUS cites as evidence. S3 x2 — 174
               stale link labels; archived-date consistency."
    - pr_unit: FD-5                       # verification SLIM (1 verifier)
      cycles: [ { cycle: 1, S0: 0, S1: 0, S2: 0, S3: "nits, count not recorded" } ]
      detail: "Verdict ACCEPT-WITH-NITS. Individual nits were not enumerated addressably in
               the record; the count is unrecoverable. One out-of-lane pre-existing defect was
               found and tracked rather than fixed (four rustdoc intra-link warnings in
               untouched regions, proven present on base)."
  totals:
    enumerable_findings: 19               # S0: 0 · S1: 7 · S2: 6 · S3: 6 (+ FD-5 nits, uncounted)
    remediated_in_unit: 19                # 100% — none deferred, none shipped unfixed

  cycles_to_convergence:
    - { pr_unit: FD-1, cycles: 2 }
    - { pr_unit: FD-2, cycles: 2 }
    - { pr_unit: FD-3, cycles: 2 }
    - { pr_unit: FD-4, cycles: 2 }
    - { pr_unit: FD-5, cycles: 1 }
    mean: 1.8
    note: "No unit needed a third cycle. Severity fell to zero in one remediation pass on every
           unit that opened with findings — the signature of genuine convergence rather than
           attrition."

  noise_ratio:
    - critic: "the adversarial pass (all five units)"
      withdrawn: 0
      filed: 19
      ratio: 0.00
      note: "No dispute (R6) and no WITHDRAWN disposition appears anywhere in the campaign
             record. Every finding named a concrete artifact — a file, a count, or a
             provocation that reproduced it — which supports a genuinely low ratio. But 0.00
             is not independently corroborated: with no dispute channel exercised, a finding
             accepted without challenge and a finding that was correct look identical in this
             record. Diagnostic, not a target."

  coverage_misses:
    - defect: ED-1 (attribution trailer names a model that was not the one running)
      pr_unit: FD-1
      category: process/commit-metadata hygiene
      attestation_was: ATTACKED_clean      # "both hygiene passes ... clean"
    - defect: ED-2 (squash commits carry no attribution trailer at all)
      pr_unit: FD-2, FD-3
      category: process/commit-metadata hygiene
      attestation_was: ATTACKED_clean      # same attestation, on the branch side only
    - defect: ED-3 (residual stale current-state claims outside the status SSOT)
      pr_unit: FD-1 (origin), FD-2 (AGENTS.md site)
      category: documentation truth / status de-duplication
      attestation_was: ATTACKED_clean      # "zero surviving stale current-state claims"
    - defect: ED-4 (unit ledger cited the brief and design as not-yet-in-repo)
      pr_unit: FD-3
      category: documentation truth / cross-reference integrity
      attestation_was: ATTACKED_clean
    count: 4

  escaped_defects_by_origin:
    - defect: ED-1
      description: >
        The FD-1 squash commit's Authored-By trailer names a model that was not the one
        actually running. Discovered post-merge.
      origin: missed_clause
      evidence: >
        No proposition required the trailer be read from the live session at commit time; the
        rule in force named a constant. Audit failure, not execution failure.
      remediation: >
        Rule landed at FD-3 (task/lessons.md, 2026-08-09). Merged history left as-is under the
        forward-only rule. CLOSED forward.
    - defect: ED-2
      description: >
        The FD-2 (#25) and FD-3 (#26) squash commits carry no attribution trailer at all —
        empty commit bodies. The branch-side commits were correctly stamped; the trailer did
        not survive the squash step. A THIRD occurrence landed 2026-08-10 in #30, the first
        unit after this campaign, with two lessons on the subject already in force.
      origin: execution_defect
      evidence: >
        The clause existed (task/lessons.md 2026-08-06: end every commit message with exactly
        the trailer, and nothing else). All three landed AFTER ED-1 was already known, so the
        attribution surface was under active attention and the merge-commit surface still went
        unchecked. The unit's own pre-merge gate cannot see the squash it does not perform.
      remediation: >
        Not remediable backwards (history stands, forward-only). Detector proposed as FF-2.
        OPEN — and the post-campaign recurrence is the evidence that prose alone will not
        close it.
    - defect: ED-3
      description: >
        Current-state claims outside STATUS.md that contradicted it: (a) CONTRIBUTING.md — the
        port "is in progress"; external PRs closed "during the port"; the policy "will be
        revisited after the port's milestone one". Linked directly from README's Status
        section. (b) docs/skills/map.md — a skill "not ported yet (returns with phase 1) ...
        lives in the private v1 repository until crate code lands here", after phase 1
        delivered. (c) AGENTS.md — "the bindings crate arrives in phase 3", future tense for a
        delivered crate.
      origin: execution_defect
      evidence: >
        FD-1's acceptance gate names this exact grep over the whole tracked markdown surface,
        and its PR attests "zero surviving stale current-state claims outside STATUS.md." The
        clause existed and was proven; the sweep's population was the unit's lane, not the
        surface the gate named. FD-2 rewrote AGENTS.md and left site (c).
      severity_note: >
        (a) is S1-class — on the outward contributor path, and the only one an external reader
        will hit. (b) is S2. (c) is S3 and the weakest of the three.
      remediation: >
        CLOSED 2026-08-10 at close-out. All three corrected; re-running the gate over its
        DECLARED population (rather than over the three known hits) surfaced three more
        future-tense sites in AGENTS.md, all corrected in the same change — the miss was
        twice the size the known list implied.
    - defect: ED-4
      description: >
        The FD-3 ledger shipped in #26 stating that the campaign brief and design "are
        execution records, not live rules — they land in-repo with the campaign's closing
        archival", and therefore wrote deliberate non-links where working links existed. FD-1
        had landed both files two units earlier.
      origin: execution_defect
      evidence: >
        FD-3's own scope section cites the design and slate by section number; the AC cycle did
        not re-check the premise of its own citation against the tree. Caught post-merge by
        FD-4's adversarial pass and corrected in FD-4's diff (links restored, status corrected
        to DELIVERED, merged-PR number added).
      escaped_judgment: >
        COUNTED AS ESCAPED. It shipped in #26 as a false statement of fact and was not caught
        by its own unit's review. "Caught by the next unit" is still "not caught by its own
        unit", and this metric measures the AC loop's catch rate, not the campaign's eventual
        self-healing — grading it otherwise would let a sequenced campaign launder every miss
        into the following PR. Recorded at the lowest severity (S3, documentation accuracy, no
        user-facing surface, no rule made wrong) and CLOSED at FD-4 (#28), so it does not read
        as an open defect. The distinction that matters: ED-4 cost one extra review pass, while
        ED-3 cost an acceptance item.
    count: 4
    by_origin: { missed_clause: 1, execution_defect: 3, novel_scope: 0, environment: 0 }

  light_path_escapes:
    entries: []
    count: 0
    note: >
      EMPTY POPULATION, with reason — no unit ran the LIGHT path. The smallest unit (FD-5, 11
      files / 240 changed lines) exceeds the <=150-line <=5-file threshold, and the slate assigned
      it SLIM (one adversarial verifier), which is not LIGHT (no adversarial pass). All five
      units ran an adversarial pass. This zero therefore says NOTHING about whether the LIGHT
      thresholds are calibrated — it must not be read as vindicating them.

  flags_shipped:
    - finding: "O-1 (FD-3) — 'planned' vs 'deferred' vocabulary split between repo-manifest.toml
                and AGENTS.md/STATUS.md for the same three crate homes"
      pr_unit: FD-3
      r8_visible: yes                      # declared in the PR body's Reviewer notes
      outcome: STILL_ACCEPTED
      note: "Bridged in prose; the one-line rename was offered and not taken. Harmless so far,
             but it is a second vocabulary for one fact — the exact drift shape this campaign
             set out to remove. Worth closing at the next manifest touch. Re-checked at
             close-out 2026-08-10: unchanged."
    - finding: "O-3 (FD-3) — make preflight's security leg (make audit) not run by the actor"
      pr_unit: FD-3
      r8_visible: yes
      outcome: CLOSED
      note: "Justified: the unit touched no Rust dependency. No advisory materialized. CLOSED
             at close-out 2026-08-10."
    - finding: "FD-4 'kept live by design' — docs/port/{PLAN,census}.md stay live rather than
                being archived with the rest of the port record"
      pr_unit: FD-4
      r8_visible: yes                      # flagged as a judgment call for the reviewer
      outcome: STILL_ACCEPTED
      note: "Both are cited by live procedure (census.md by scripts; PLAN.md by STATUS's
             cutover item), so archiving them would have stranded a live reference."
    - finding: "FD-5 — four pre-existing rustdoc intra-link warnings in untouched session.rs
                regions, proven present on base"
      pr_unit: FD-5
      r8_visible: yes
      outcome: STILL_OPEN
      note: >
        Recorded at FD-5 as handed to a tracked doc-hygiene lane. At close-out 2026-08-10 that
        lane was looked for and DOES NOT EXIST: no STATUS.md entry, no ledger, no tracked item
        anywhere in the tree names it. A handoff to a named lane is not a handoff if the lane is
        not an artifact. Tracked here from 2026-08-10 — the honest minimum, not a substitute for
        a home in STATUS.md. Contrast the neighbouring FD-4 rider (eight comment sites citing a
        v1 crate name), which WAS recorded as an artifact in STATUS.md "Deferred capabilities"
        and was closed on 2026-08-10 by #30. The one written into an artifact got done; the one
        written into a sentence did not.
    count: 4

  environment_drift_events:
    entries: []
    count: 0
    note: >
      EMPTY POPULATION, with reason. No gate went red on any of the five units for environmental
      reasons, so no R10 base-ref test was triggered by a red gate. The one base-ref comparison
      actually performed — FD-5 proving the four rustdoc warnings pre-existed — returned base-red
      for a WARNING STREAM THAT NO GATE COVERS; per the metric's definition (red *gates* proven
      environmental) that is a pre-existing-defect finding, not a drift event, and it is filed
      under flags_shipped above. The dependency-bump PR that opened mid-campaign (#27) was still
      unmerged at close-out and perturbed nothing. Nothing indicts the local-mirrors-CI binding:
      every gate the units ran locally matched CI.

  status: COMPLETE
```

**Headline.** 19 findings caught pre-merge · 4 defects escaped · **83% pre-merge catch rate**
(19 / 23) · 1.8 cycles to convergence · 0 environment drift events · 0 LIGHT-path escapes over an
empty population · 4 flags shipped (2 still accepted, 1 closed, 1 still open).

**What these numbers are evidence for, and what they are not.** They are evidence that an
adversarial pass earns its cost on *documentation* units — the two mechanical-gate bypasses alone
(a one-character typo that would have disabled every structural rule while the gate still printed
green) justify the campaign's whole review budget. They are **not** evidence that the review loop
covers the repository: three of the four escapes are in a single class — a claim about the tree that
no unit's diff touched — and one of them recurred *after* the campaign closed. The catch rate
measures attacks on diffs. Nothing here measures the surface no diff reaches.
