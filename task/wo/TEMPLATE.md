# Work order template — three grades (owner, 2026-10-01)

A work order is the unit of dispatch to an engine. Its **grade** says what the order carries, and the
engine's band (self-directed ≥ 50, guided 25–49, clerk < 25 on Terminal-Bench 4.0; the band table is
owner-gated) says the lowest grade it may run. A lower-tier engine fails on decisions and judgment,
not on typing, so a grade is reached by removing decisions, never by adding prose.

| grade | carries | runs on |
|---|---|---|
| **A · self-directed** | the charter clause, the pins to write, the gate commands, the hand-back conditions | self-directed engines |
| **B · guided** | A + a design sketch naming types and signatures, one slice per round with one commit each, every ruling pre-made, a halt list, the oracle per slice (the Spark answer, the measured bar) | guided engines; ATTR-ID-1's order is the template |
| **C · clerk** | B + the exact file list with ceilings, skeleton code for each new file with signatures that compile, the test that must go green named before its body exists, the expected output of every gate, a numbered step list in which the commit is its own step | clerk engines |

**The test of a grade-C order:** a fresh engine starts it without asking a question and halts on the
first ambiguity. If the first run fails that test, the fix is in the order, not the engine.

## Skeleton

```
# <UNIT> — <one line>                       grade: A | B | C   engine band: …   release: …
## 0. Why (two sentences) and what is out of scope, named
## 1. Rulings already made (ids and one line each; nothing is decided inside the round)
## 2. Files (exact paths; new / moved / edited; ceilings from scripts/check_rust_file_size.py)
## 3. Skeletons (grade C only: signatures that compile, the test names)
## 4. Steps (numbered; one gate per step where one applies; the commit is its own step)
## 5. Gates and their expected output (the exact command and the line that means green)
## 6. Halt rules (what ends the round early, and what the hand-back must contain)
## 7. Hand-back (the JSON or the ledger row the orchestrator reads)
```

Standing rules every order inherits: no code comments from Anthropic models (docstrings are not
comments); relocated code sheds its comments; map.md lockstep for every `.rs` / `.py` / `.toml`;
commit-first briefs with a provisional hand-back for engines that cannot see their step budget;
the scratch clone, never the live worktree; the TRO-Wolf identity and the `Authored-By:` trailer.
