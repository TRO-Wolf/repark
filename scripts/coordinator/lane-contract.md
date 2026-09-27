# Lane contract — the uniform lane interface

Purpose: the one work-order / hand-back / resume interface every lane and worker family shares.

Source: work order ORCHESTRATOR-DOCTRINE-1 (owner-accepted proposal, 2026-09-26); `handbook.md`
for the work-order fields; the muse-worker, devin-worker, grok-worker, and codex-worker skill
files for the hand-back shape (residue table below). No skill file is edited by this contract.

## Work order

The fields are those `handbook.md` already lists under "A work order always states" — cited,
not copied. This contract adds no field: the goal, the files and functions, the decided design
ruling and its source, the cells or tests that must turn green, the gate command, what not to
touch, the comment ban, and "commit after every step; write the hand-back file last". A guided
engine's order additionally passes the checklist in `bands.md` before launch.

## Hand-back envelope

One JSON object at the clone root, written LAST, after all commits and gates:

| Field | Shape |
|---|---|
| `status` | `CONCLUDED` or `HALT`. |
| `summary` | One paragraph: what the round did and what it proves. |
| `commits` | Array of commit lines (sha + subject). |
| `gates` | Array of `{name, result}` gate records. |
| `questions` | Array of `{id, class, question, premise, lean}`; `class` is `RULING` (the orchestrator may rule) or `FACT` (a measurement or tree lookup is owed). Empty unless `HALT`. |
| `out_of_scope_observed` | Array of strings: real findings outside the work order, for the orchestrator to re-measure, never to absorb silently. |

This is the shape the five worker families already emit. The worker writes the file at the
clone root (git-excluded) and echoes the same JSON as its final text; the launcher moves it
into the run directory, or reconstructs it from the terminal text.

### Residue — where the families differ today

Read 2026-09-26 from the skill files; the skills themselves are unchanged.

| Residue | Detail |
|---|---|
| No opus-worker skill file exists | Opus rounds go through the handbook Toolbox launcher (`r7-launch.sh … opus`); their hand-back has no skill-pinned schema. |
| `BLOCKED` status | The grok-worker's schema and this lane's own hand-back contract carry `CONCLUDED \| HALT \| BLOCKED`; the envelope above standardises on `CONCLUDED \| HALT`. A transport-level stop arrives as `HALT` with the obstacle in `summary`. |
| Gate record shape | The grok-worker schema pins `{command, exit}`; the envelope above standardises on `{name, result}`. |
| Question classes | The grok-worker schema pins `RULING \| OWNER`; the envelope above standardises on `RULING \| FACT`. |
| Schema pinning | Only the grok-worker family pins its shape in a schema file with a reader script; the other families state the contract in the brief text. |

## Q&A protocol

A `HALT` with `questions[]` is answered by a RULING file passed as `--followup` on the same
session id; the ruling is repeated in the ledger. The orchestrator verifies each question's
premise against the tree before ruling, adopts or overrides each lean explicitly, and never
invents a ruling it cannot source. A HALT whose lean stays inside the work order is Tier-1
latitude (`latitude-ladder.md`); anything wider batches for owner approval.

## Resume semantic

- Exhausted step budget or transport fault, tree intact: resume with a finish order on the same
  session id, a smaller step cap, and "stop exploring, finish".
- Crash before any edit: relaunch fresh on a clean tree.

A round that ends with no hand-back means the work order was too big: split it and relaunch
the pieces — never resume or re-run the same oversized order (the handbook's WORK-ORDER SIZE
rule agrees).

## State file template — `state/orchestrator.md`

The cold-start file (`orchestrator-adapter-muse.md`): the orchestrator re-reads it at every
tick and writes it before ending the tick. Layout, copied from the work order:

- Mode in force (which latitude tier and usage posture apply).
- Lanes table, one row per lane: lane, clone, branch + base, current round + work order,
  session id, watcher, next action on fire.
- After-list (what happens once each in-flight item lands).
- Monitor registry (what is watched, and what firing means).
- Standing rules (the rules in force for this run, with pointers, never pasted).
