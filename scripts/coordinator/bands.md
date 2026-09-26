# Executor bands — Terminal-Bench 4.0 (owner ruling 2026-09-26)

An executor engine's work-order shape follows its Terminal-Bench 4.0 score. The orchestrating
session re-pulls the scores at least weekly (the pull date is the table's heading; a table older
than seven days is refreshed before the next dispatch). Artificial Analysis is the reference source;
llm-stats and Snorkel differ by a few points and are noted where used. A score is never invented:
an unscored engine runs guided.

| Band | Score | Work-order shape |
|---|---|---|
| self-directed | 50 and above | the ordinary work order (the handbook's "A work order always states") |
| guided | 25 to 49 | the guided-execution checklist below, complete before launch |
| clerk | below 25 | mechanical work only: rebases, red lint or docs or ledger checks, bump mechanics |

## Scores pulled 2026-09-26

| Engine | TB 4.0 | Source | Band |
|---|---|---|---|
| Claude Opus 5.5 | 59.6 (AA) / 66.4 (llm-stats) | AA, llm-stats | self-directed |
| Claude Mythos 5.1 | 60.9 | llm-stats | self-directed |
| GPT-6 Astra | 59.6 (AA) / 58.2 (Snorkel) | AA, Snorkel | self-directed |
| Claude Fable 5.1 | 55.1 (AA) / 57.9 (Snorkel) | AA, Snorkel | self-directed |
| Claude Opus 5 | 51.8 (AA) / 53.9 (Snorkel) | AA, Snorkel | self-directed |
| GLM-5.3 | 41.8 | llm-stats, Snorkel | guided |
| Grok 4.7 | 38.0 / 37.6 | llm-stats, Snorkel | guided |
| GPT-5.6 Sol | 37.3 | llm-stats, Snorkel | guided |
| Muse Spark 1.3 | 33.3 | AA (Intelligence Index v4.3 note) | guided |
| Devin SWE-2 | 27.3 | benchlm.ai | guided |
| GPT-5.6 Terra | 21.5 | llm-stats, Snorkel | clerk |
| Grok 4.6 | 20.3 | llm-stats, Snorkel | clerk |
| GPT-5.6 Luna | 17.3 | llm-stats, Snorkel | clerk |
| Claude Sonnet 5 | 12.4 | llm-stats, Snorkel | banned by the owner regardless |
| GLM 5.3 Flash | unscored | — | guided (clerk in practice) |

Sonnet and Haiku stay banned as executors and orchestrators whatever they score (owner, 2026-09-20).
The Opus 5.5 medium verifier and fixer rule on every lane is unchanged by the band.

## Guided execution — the checklist a work order passes before a guided engine launches

1. **Every ruling is made before dispatch.** The work order carries no open question; anything the
   executor might have to decide (a default, a refusal text, a residue vs fix call) is decided in the
   order and cites where the decision came from (claims line, owner ruling, measured Spark answer).
2. **One cell per round.** The order names one cell (or one behaviour of the same size) and pastes
   the recorded Spark answer for it verbatim, with the probe key, so the executor pins against text
   it did not have to measure.
3. **A design sketch for anything spanning more than one crate.** The orchestrating session writes
   the files, functions and approach (a self-directed engine's sketch, not the guided engine's) into
   the order before launch.
4. **The file-ceiling escape is pre-named.** For every file the order touches that sits at its
   ceiling, the order names the new module (path, declaring line, `map.md` row) the added code goes to.
5. **Halt within the first hour on any open question.** The order says so, and the hand-back schema's
   `questions[]` is the only place a question goes; a round that runs on past a question is a failed
   round, not a partial success.
6. **A smaller step budget** than a self-directed round (the launcher's default halves), sized for
   the one cell.
7. **The engine's addendum carries the pattern.** Each guided engine's `addendum-<engine>.md` is where
   the lessons ledger's evidence for that engine becomes an instruction, applied at the next run's
   start, never mid-run.

Evidence for the rule (the row in `lessons.md` dated 2026-09-26): the Muse Spark round on U11 (edge
resolution, 2026-09-25) stopped on rulings the work order had left open and produced nothing that
could be merged in six hours; the salvage was an Opus triage round on top. Every Opus round on the
same unit produced merged commits. Muse is a flat subscription and Opus spends the weekly quota, so a
guided Muse round that lands is the cheapest merged commit the campaign has.
