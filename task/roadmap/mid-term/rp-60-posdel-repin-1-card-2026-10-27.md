# RP-60-POSDEL-REPIN-1: repin the fork to the position-delete batch fix, with one Spark-door pin

**Filed:** 2026-10-27. **Filed by:** the POSDEL docs clerk round (`claude-haiku-5-5`), from the orchestrator's measured facts of 2026-10-27.

**Status:** dispatched 2026-10-27 on an Opus 5.5 builder lane, repin target `ea0b7741`.

**Status, 2026-10-28:** merged as PR #1200 at `57e37a72`, before the format-v2 label date; the label carries no fifth carve-out. Closed.

**Depends on:** F-POSDEL-BATCH-1 landed on the fork (landed 2026-10-27, fork PR #374) ([the fork card](f-posdel-batch-1-card-2026-10-27.md)). No RePark step starts before the fix is on fork main and fork main is green (fork-sync rule 2).

**Grade:** clerk-grade repin, plus one Spark-door pin. Nothing changes in the Python facade.

**Why:** RePark runs the metadata-delete door first. That door declines a ten-day range inside a month partition, so the statement reaches the fork's merge-on-read delete plan, which panics at pin `d80a2884` on a partition of 10 to 15 million rows. The fix lives in the fork. RePark needs the repin, a pin that shows the Spark door reaches the fixed writer, and the records that keep the pin honest.

## Steps

1. **The repin.** Move the five `rev` lines in the root `Cargo.toml` to the fixed fork commit, through `make bump-fork-pin REV=<sha>` (fork-sync rule 1). `Cargo.lock` moves only its six fork source lines. Check the result with a clean merge plus `cargo metadata --locked`. Never use `cargo update -p ... --precise`, which re-points duplicate edges and churns the lock.
2. **A Spark-door pin.** A merge-on-read `DELETE` on a partitioned v2 table, with `write.delete.position-delete-batch-rows` set to a tiny value, reads back the surviving rows and asserts the delete-file count. The pin proves the Spark door reaches the batched writer.
3. **A dated registry row** in [docs/spark-sql-iceberg-parity.md](../../../docs/spark-sql-iceberg-parity.md) for a bulk merge-on-read `DELETE` that matched more rows than one batch, named to the pin of step 2.
4. **A release-notes line** in the next release's notes draft. It states the behaviour change (a bulk merge-on-read `DELETE` that panicked now completes) and the new table property.
5. **The unit ledger.** It records the pin commit, the result of each gate, and the step-2 measurement.

## Records

The repin PR has the shape of RP-59 (`git show --stat cfb20a16`): four files, `Cargo.toml`, `Cargo.lock`, a pin-history row in [docs/fork-sync.md](../../../docs/fork-sync.md) and the root `map.md` line for the pin. The last three pin-history rows of that file are the form to copy. The registry row, the release-notes line and the ledger of steps 3 to 5 join those four files in the same change.

## Sequencing

- Land RP-60 before the format-v2 label date, 2026-11-10 (owner, 2026-10-27).
- If it does not land by then, the label carries it as a fifth dated carve-out, beside ENC-1 items 1 and 5, ENC-CTAS-REPLACE-1 and NESTED-TEXT-LEAF-STORE-1.
- Until it lands, the workarounds recorded on the fork card apply.

## Gates

The repin PR's CI, plus `make rust-panic-ban`.

## Out of scope

The memory side of the fault, which the fork card names as its follow-up. The copy-on-write delete path. The Python facade.

Up: [map.md](map.md).
