# Overnight report — run 10 of 2026-09-13 (COMMENT-CORE-1)

**Session:** one Opus orchestrator (overnight-10), 2026-09-13 05:49 → 07:45 local.
**Grants:** G-1 (squash-merge on green), G-2 (bounded decisions), G-3 (stop when the unit merges or by 09:30),
G-4 **Grok** actor and Grok critic (no Devin, Muse or GLM), G-5 no. Release pipeline, tags and STATUS.md untouched.
**Procedure:** [overnight-orchestrator-runbook-2026-09-08.md](overnight-orchestrator-runbook-2026-09-08.md).

## 1. What landed

| Unit | PR | Merged | Rounds |
|---|---|---|---|
| COMMENT-CORE-1: in-code comments removed from `python/repark/src/repark/spark/dataframe/core.py`, no code change | #561 | `893e72be` | Grok actor 2 ($2.67), Grok critic-logic 1 ($0.33) |

Ledger: [comment-core-1-ledger.md](../../ledgers/completed/comment-core-1-ledger.md). The unit is complete and nothing is parked.

## 2. What was removed and what moved

- The owner measured about 60 trailing comments on main. A `tokenize` count found 347 non-pragma comments:
  **341 full-line and 6 trailing**. The rest of that estimate were the **74 `# noqa` / `# type:` pragmas**, which stay unchanged
  (same sequence on both sides). Docstrings are untouched.
- **312 moved to map.md, 35 deleted as narration.** Moved reasons live in
  [dataframe/map.md](../../../python/repark/src/repark/spark/dataframe/map.md) under
  `core.py rationale (COMMENT-CORE-1)`, one bullet per function (66 anchors). Examples: the GIL hazard around `mapInArrow`,
  the display-name overlay and origin-map rules, the plan-collapse identity notes, the cache-versus-checkpoint lineage rules,
  and the three explode kinds. The 35 deletions are section banners, restatements of the next line, or shorter duplicates of
  a comment that moved.
- The ledger table has one row per removed comment: base line, enclosing function, verbatim text, disposition, map anchor.

## 3. Counts

| Measure | Base `4f121ab9` | Branch |
|---|---|---|
| `core.py` lines | 4468 | **4117** |
| non-pragma comments | 347 | 0 |
| pragmas | 74 | 74 |
| `ast.dump` equality | — | **True** |
| `check_lib_py.py` / CAP-1 ceiling | 4468 / 4468 | 4117 / 4117 |
| facade suite, same clone | 5985 passed, 369 skipped | 5985 passed, 369 skipped |
| parity suite | — | 756 passed, 1 skipped, 12 xfailed |

`make preflight` and `make verify` passed on the branch; CI was green before the squash, and the merged tree equals the branch tree.

## 4. The Critic's verdict

One Grok critic-logic round (read-only, a fresh clone of the branch at `c2beb6b7`) re-derived the comment inventory with
`tokenize`. The **347/347 table was complete**: no missing, extra or duplicate rows. Code identity, pragmas, ceilings and scope were clean.
Verdict **FINDINGS (11)**, all adopted and fixed on markdown only before the PR:

- 2 LOST-RATIONALE: `is_empty` and `to_local_iterator` are snake_case aliases that are not PySpark names. The disclosure was deleted
  as narration, but the same kind of disclosure was kept for `declareSorted` and `toArrowBatches`.
- 9 DISTORTED map sentences: `localCheckpoint` called `eager` the signature-parity argument when it is `storageLevel`
  (`eager` is live); `declareSorted`'s disclosure was pinned on `dynamicFlatten`; `explode_keep_null`'s null-versus-empty rule
  was stated for all explode kinds; and six sentences dropped a qualifier. Those qualifiers are: WriterV2 options ignored beyond
  `tableProperty`; bare `joined["b"]` stays `AMBIGUOUS`; checkpoint keeps the VALUES seam; do not resolve the outer type
  for `explode`; `_mia_temp_views` dropped at finalization; and `grouping_sets` v1 includes the grand-total `()`.

Lesson: moving a comment into map.md loses more through paraphrase than through deletion. Nine of the eleven findings were
condensations that changed a name or dropped a qualifier. A comment-removal unit needs a critic that reads each moved sentence
against the original comment, not just a row count.

## 5. Decisions under §6

- R10-D-1: the critic launched on the branch after the actor's second commit, before the third. The third commit only added gate
  evidence and removed one import blank line (4118 → 4117), and the orchestrator audited it separately.
- R10-D-2: all 11 critic findings adopted after checking F-003 and F-005 against base. The fix round was told not to run
  builds while the orchestrator's preflight used the clone. Its diff is markdown only.
- R10-D-3: the branch was rebased onto `0a1a3d70` (#560, docs only) instead of merged, per the lockstep-hook lesson from run 9.
