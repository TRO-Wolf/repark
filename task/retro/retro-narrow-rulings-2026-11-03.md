# Retrospective: why rulings kept being narrower than the measured surface, and how to get to one round

Date: 2026-11-03. Scope: the three audit-3 closeout units run 2026-10-31 to 2026-11-03 (AUD-015 SPARK-DOOR-TEMP-VIEW-DDL-1, AUD-016 MAP-STRUCT-KEY-SUBSCRIPT-1, fork F-IDLESS-TOPLEVEL-POSITIONAL-1). Written by the orchestrator at the owner's request after the second halt. Owner usage: 41 % of the week on 2026-11-02 morning, 48 % by the halt.

## The record

| unit | build rounds | verification rounds | how each later round failed |
|---|---|---|---|
| AUD-015 | 5 | 5 | r2 new spellings of the target; r3 quoted spellings; r4 inner backtick, literal tokens, multi-name DROP; r5 no-view write heads, then qualified quoted DROP and TRUNCATE |
| AUD-016 | 5 | 5 | r4 ANSI-off cells; r5 case-insensitive field names, then DECIMAL array-element cells |
| fork F-IDLESS | 6 | 4 | r2 four filter cells; r3 two cells of the ruled matrix wrong; r4 pass |

Zero of the three closed on the first verification. Every later catch was real (an S1 or S2 the owner would not have accepted on main), so the Critics did their job. The rounds came from upstream: what each round asked the builder to make true was smaller than what the clause claimed.

## Causes, in order of weight

**1. Rulings were written from the cells a verifier happened to find, and generalised by inference.** The fork's filter matrix was ruled from one unmeasured cell (STARTS WITH on a string read as binary) and the ledger said so in plain words; I ruled over the word "unmeasured". The AUD-015 round-4 ruling said "any target Spark cannot parse refuses, with or without a view", and the builder pinned the spellings the verifier had listed: unqualified ones. Nobody enumerated the target grammar (head, qualification depth, quote kind per segment, literal-like tokens, separators, multiple targets, view in scope or not, catalog), so the next verifier found the next spelling. AUD-016's ANSI-off rule was ruled per the cells of one probe and failed on the DECIMAL pairs that probe did not hold.

**2. The claim set an unprovable bar, then every verifier proved it unprovable one counterexample at a time.** "Every spelling Spark refuses, RePark refuses" is a property of the SQL dialect, not of a temp-view DML unit. A unit scoped to one surface cannot prove an engine-wide clause; it can only prove a table. Writing the clause as a universal invited a new counterexample per round, and each counterexample was then argued in or out of scope after the fact (F17 carded, L10 carded then re-raised as S1).

**3. Attack coverage was attested per category, never per domain.** AT-1 "input domain" was marked ATTACKED every round, truthfully, because the Critic attacked inputs. But with no enumerated domain, ATTACKED means "tried some spellings", which is a findings count in disguise. The spine's rule that convergence is coverage, not a count, had nothing to measure coverage against.

**4. Pins were examples, not the property.** Red-then-green on the cells found proves those cells. The invariant was never written as one table-driven test over the grammar with Spark's recorded answer per cell. Muse ran a 156-cell matrix as a probe on the fork and a 490-row matrix on AUD-015; those matrices were thrown away after the verdict instead of becoming the pin.

**5. Two independent verifiers with free choice of probes guarantee two different edges.** The union rule caught real defects (Muse found the S1 multi-name DROP and the qualified deletes; Opus found the regression Muse passed). But run sequentially, each on its own probes, they produce one new round each by construction.

**6. Lead versus finding was never ruled.** L10 was carded by the round-4 Opus verifier; Muse re-raised the same class as S1 in round 5 because the narrowed clause still covered it. Both were right by their own reading. A card has to be a ruling with a domain, not a parking note.

**7. The orchestrator ruled ahead of measurement, twice, against a standing memory ("measure before ruling", 2026-09).** Two Spark measurements I ran myself today took minutes each and would have settled the fork matrix and the no-view cells before any fix brief. I chose a ruling over a measurement to save a step, and paid a round each time.

What was not the cause: builder quality (every fix implemented its ruling faithfully, red-first, gates green), verifier quality (every later finding reproduced), or the delta-packet change (today's delta rounds cost a quarter to a third of a full round).

## The one-round design

The Actor–Critic loop closes in one round only when the Critic cannot discover the claim's domain for the first time during the critique. So the domain is produced before the build, and the Critic's job becomes checking it, not finding it.

1. **Measurement before ruling, as a gate.** No fix brief and no clause until a domain table exists: the enumerated input classes for the invariant, with Spark's measured answer per cell. For a DDL-target invariant that is heads × spellings × qualification × view presence × catalog. The measurement is a Spark run by the orchestrator or a Haiku clerk, costs no frontier tokens, and today took under ten minutes per table. A cell that cannot be measured is ruled fail-closed (refuse), never inferred.

2. **The clause is the table.** The ledger clause claims the table and names what is outside it. Everything outside is carded on day one with its measured answers, before the build, not discovered later. Engine-wide dialect properties (quoted identifiers, ANSI-off implicit casts, Unicode identifiers) are their own units with their own tables; a surface unit never claims them.

3. **The pin is the table.** One table-driven test per door reads the table and asserts every cell against Spark's recorded answer. Red before the fix shows the failing cells; green after shows the whole table. Hand-picked example pins become controls, not the proof. Muse's and the verifiers' matrices are promoted into that test instead of being probes.

4. **One Critic pass, both verifiers on the same head and the same table, in parallel.** Each verifier confirms the table through the public door and attacks only outside it. Findings are merged by class before anything goes back to the builder. A finding inside the table is a builder defect and goes to one delta fix; a finding outside the table is a card by rule, not a FAIL, and does not open a round.

5. **Cap: one build, one critique, one delta fix, one delta re-check, then park.** Not two or three. The delta re-check verifies only the cells that moved and the boundary claim. A second critique that finds a new class outside the table is evidence the table was wrong, which is a scoping failure to record and card, not a reason for round three.

6. **Leads are rulings.** A card written during a round states its domain and its severity reading, and the clause text is narrowed in the same commit so no later verifier can re-raise it as a finding against the clause.

7. **Orchestrator rules only from tables.** The word "unmeasured" anywhere in a ledger or a verdict blocks a ruling on that cell. This is the standing memory restated as a gate on me.

## What this costs and saves

Per unit, the table costs one measurement session before the build and one table-driven pin in the build. Against today's record it replaces three to four build rounds, four verification rounds, and the records rounds between them. At today's prices a full Opus verification round was about twenty dollars and a builder round between six and seventy; a Spark measurement is zero on the meter. The usage curve of this week, 41 % to 48 % in two days with three units still open, is the argument.

## What happens to the three open units

AUD-015 and AUD-016 should each get one measured table before anything else moves: the DDL-target table and the ANSI-off cast table, both already half measured in the verdict files. If the owner restarts them, the next round is a table-driven pin round, not another spelling fix. The fork unit is merged; its matrix test already exists in Muse's probe and should become a pin in the repin unit rather than a new fork round.
