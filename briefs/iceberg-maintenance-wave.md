# Slate — the Iceberg write-path maintenance wave (MW)

**Chartered 2026-08-21**, green-lit by the owner for immediate start. Design:
[../docs/design/iceberg-maintenance-wave.md](../docs/design/iceberg-maintenance-wave.md).
Charter and approval gate: [../task/mw-0-charter-ledger.md](../task/ledgers/staging/mw-0-charter-ledger.md).

## The one invariant

**No refusal becomes silent.** This campaign's subject is loud refusals that are being lifted or
replaced. Each one either stays refused with its text pinned, or is replaced by something equally
visible at the point of use: a documented failure mode, a dry-run default, a declared registry
row. Removing a refusal is fine. Removing it and leaving nothing in its place is not, because the
condition the refusal described does not go away when the message does.

## Per-unit contract

Standing repo discipline, restated because this campaign touches a destructive surface:

1. **Reproduce first.** The behavior is demonstrated on this tree before anything is edited.
2. **Write the pin and watch it go red.** A pin that was never red proves nothing.
3. **Fix narrowly.** One knob, named, with the reason in the code where the knob lives.
4. **Gate alone.** `make preflight` runs by itself and its own exit code is read immediately —
   never through a pipe into `grep` or `head`, which reports the pipeline's status, not the gate's.
5. **`map.md` in lockstep, in the same commit.** Not a follow-up.
6. **A ledger at `task/mw-<n>-…-ledger.md`, indexed in `task/map.md`.**

## The testing contract, restated because it is a hard block

Every value a pin asserts is measured against the oracle **first** — including the incidental
controls, the ones that look too obvious to check. The SEM campaign shipped a draft assertion
that read the engine's own answer back as if it were Spark's, and it was caught one step before
it would have been pinned as truth. That is the failure this rule exists for, and a green pin
asserting a divergence as parity is the most expensive wrong test in the repo.

Where the oracle cannot execute a procedure, the expected value comes from the shipping Iceberg
jar's own constant and **the ledger says which**. Never from documentation, and never inferred.

## Destructive-surface discipline (MW-3 especially)

`remove_orphan_files` deletes files with no rollback. It is the only unit in this campaign that
can destroy data, so it inverts the usual defaults:

- **`older_than` is required.** No default that could eat an in-flight commit's files.
- **Dry-run is the default.** Deletion requires an explicit opt-in — stricter than Spark, and
  therefore a declared registry divergence rather than a silent improvement.
- **The floor is enforced, not documented.** The fork already defaults `older_than` to
  `now − 3 days`; a caller-supplied value below the floor refuses rather than being honoured.
- **The fixture proves both directions.** Dry-run lists exactly the orphans, and the armed run
  deletes them and provably not one live file. "It deleted the orphans" is half a test.

## Sequencing

MW-0 → MW-1 → MW-2 → MW-3 → MW-4 → MW-5, and the order is load-bearing:

- **MW-1 before MW-2/MW-3** because it establishes the catalog policy those two inherit. Wiring a
  new procedure against a fence that is about to move means writing its policy twice.
- **MW-2 before MW-3** because compaction is the reversible one. If position-delete rewrite is
  wrong you rewrite again; if orphan removal is wrong the files are gone.
- **MW-4 after all wiring** because it is the only unit that proves the campaign on a real
  catalog, and it needs every procedure it exercises to exist.
- **MW-5 last** because it re-measures the MW-0 baseline, and the delta is the campaign's result.

## Unit notes that are easy to get wrong

- **MW-1's obligation is documentation, not machinery.** The owner ruled the fence lifts for
  *both* catalog policies. What the fence guarded against turns out to be a commit conflict the
  fork's own `validate_data_files_exist` already catches, so the commit fails loudly and the table
  is not damaged. MW-1 has to say so — in the guide and on the procedure surface — because an
  operator whose maintenance command fails for a reason unrelated to their command has no way to
  tell routine from broken. See design §6.
- **MW-1 carries a nullability fix.** Spark declares `expire_snapshots`'s result columns nullable;
  the engine pins them non-nullable. The other two procedures agree with Spark. Fix or register —
  do not leave it unremarked a second time.
- **MW-2 gets its result schema for free** — the measured Spark schema already matches the fork
  result type's four accessors exactly. Resist adding a filter or a sort; `rewrite_data_files` is
  binpack-only and the austerity is deliberate.
- **MW-3's Spark result is one row per orphan**, not a summary count — so the dry-run listing IS
  the Spark-shaped result, not a second surface bolted on.
- **MW-5 inherits three registry rows, two of them pre-existing.** The `expire_snapshots` column
  funnel and the `rewrite_data_files` omitted column are already disclosed in `call.rs` doc
  tables with sound reasoning, but the divergence registry has no row and no pin for either. They
  are not new defects. They are correctly-decided divergences filed in the wrong place.

## Delivery

One branch, one PR per unit, manual owner merge — matching the last three campaigns. The
orchestrator prepares and gates; the owner merges.
