# F-APPEND-WINDOW-FAILLOUD-1: an opt-in fail-loud mode on the incremental append scan for a window holding a non-append snapshot (fork card)

**Filed:** 2026-10-05, the micro-batch sink's first fork request. The owner said "Fork requests file today" on 2026-10-05.

**Target:** the owned `iceberg-rust` fork, then an RP-N repin.

**Consumer:** MB-1, the micro-batch batch source (see [the plan](../epic-term/microbatch-cdc-sink-plan-2026-10-04.md), D-2, and §5's *Fork* row).

**Severity:** P2. Without it, MB-1 has to walk snapshots a second time in RePark just to refuse.

## What the fork does today (read at the pin `e1d74bef`)

`IncrementalAppendScan` collects the snapshots in `(from, to]` by walking parents from `to`, and keeps a snapshot only when `snapshot.summary().operation == Operation::Append` (`crates/iceberg/src/scan/incremental.rs:531`). An `overwrite`, `delete` or `replace` snapshot inside the window is skipped without a word.

That matches Java's batch `IncrementalAppendScan` (`ancestorsBetween` filtered to appends), and RePark's batch `start-snapshot-id` / `end-snapshot-id` read keeps it (ICE-CHANGELOG-1 C-003).

## What the streaming source needs

Spark's micro-batch Iceberg source does not skip silently:
- it refuses a non-append snapshot in the stream unless `streaming-skip-overwrite-snapshots` or `streaming-skip-delete-snapshots` is set;
- under the owner's ruling O-5 (2026-10-04: "No deletes in bronze"), a delete or overwrite inside a Bronze window is a contract violation, and the streaming query refuses.

MB-0 records Spark's exact error class and text before this card's pin is written.

## The request

- **The option.** Add an opt-in builder option to `IncrementalAppendScanBuilder`, for example `fail_on_non_append(bool)`. The default stays `false`, so today's skip and Java's batch behaviour are unchanged.
- **When it is set.** If the window contains a snapshot whose operation is not `append`, `plan_files()` returns a typed error. The error names:
  - the snapshot id;
  - its operation;
  - the window bounds.
- **The skip opt-ins.** A separate pair of options mirrors Spark's two skip properties, so a caller can skip `overwrite` only or `delete` only, while the other still fails loud. Whether RePark exposes them is ruled in the packet; under O-5 the default for a Bronze stream is to refuse both.
- **Unchanged.** Snapshot order, the `(from, to]` exclusivity and the empty-range case (`from == to`) stay exactly as they are.

## Pins (fork side, red first)

- A window holding `append`, `overwrite`, `append`, with the option set, errors on the overwrite's id. With the option unset, it yields the two appends, which is today's answer.
- The same for a `delete` snapshot (a row-level delete), and for `replace` (compaction).
  - Whether a `replace` (rewrite) snapshot counts as non-append for the stream is decided by MB-0's Spark cell. Spark's source skips `replace` silently, so the fork follows that cell.
- Each skip opt-in skips only its own kind.
- Mutation: dropping the new check turns the first pin red.

## Then

RP-N repins RePark with the scan unchanged by default. MB-1 consumes the option.

Up: [map.md](map.md).
