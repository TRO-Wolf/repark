# Card CSV-WRITER-DOOR-1: the csv write door's differences from Spark, as FA-5 recorded them

**Date:** 2026-10-08. **Filed by:** Claude (Haiku 5.5), docs lane, from the orchestrator's brief.

**Status:** open. Not scheduled.

**Retires:** when each difference below is fixed and pinned, or the owner declines it with a dated
ruling in this card.

## Why

FA-5 ([fa-5-6-ledger.md](../../ledgers/staging/fa-5-6-ledger.md)) made a csv write of a frame with
exact-duplicate display names write instead of refuse. Its verifier's grid (`a_csv.py`, 195 cells
against live Spark 4.1.2) then found byte differences from Spark. Each one reproduces on a
unique-name frame, so each is the csv door's existing behaviour. FA-5 did not change them. The
ledger recorded them and moved them out of its scope.

## Measured

The differences, copied from the FA-5 ledger's "Fold 1 — FA-5 as it ships" section, "The csv
door's own differences". The ledger records them as measured by the verifier's grid, not by this
card.

| Where | repark | Spark 4.1.2 |
|---|---|---|
| `header` option absent | header written | no header |
| Header or body value with leading or trailing space | written as is | trimmed (`lead,lead,v`) |
| Empty column name; empty string value | written as nothing | `""` |
| A name starting `#` | unquoted | `"#c"` in the first cell |
| A quote in a name or value | escaped as `""` | escaped as `\"` |
| A backslash under `quoteAll` | not doubled | doubled |
| Empty frame (`limit(0)`) | header never quoted and `quoteAll` ignored, so a name holding the separator or a newline splits | header quoted as for a non-empty frame |
| Binary column | hex (`61`) | `[61]` |
| A union frame after `coalesce(1)` | two part files | one |
| Empty partitioned write | a header-only file at the root | no file |

One refusal difference is recorded in the ledger's "Refusal text (S2)" table, and it stays a
refusal in FA-5:

| Shape | repark | Spark 4.1.2 |
|---|---|---|
| `select('id','id','s').write.partitionBy('id')` | a declared repark sentence naming `id` (pinned `_divergence`) | writes `id=1/`, `id=2/` with header `s` |

The ledger says writing this shape "needs both copies treated as the partition column, which is
not a small change to the `COPY`."

The verifier's S2, from its reverify verdict (`/tmp/oc-worker/direct/wo/fa-5-6/reverify/verdict.json`,
finding 2, orchestrator scratch, not in the repo). Its title: "caseSensitive=true: partitionBy on a
unique name that has a case twin partitions a duplicate-name frame by the twin (inherited from the
door; identical on a unique-name frame)."

- Repro: columns `a, A, a, b, B` (`b = v*10`, `B = upper(s)`), `spark.sql.caseSensitive=true`,
  `.write.option('header', True).partitionBy('b').csv(p)`.
- repark (head) writes `B=A/` and `B=B/` with header `a,A,a,b`, and raises no error.
- Spark writes `b=100/` and `b=200/` with header `a,A,a,B`.
- The verdict says the case-sensitive lookup reaches a frame main refused, and that the orchestrator's
  ruling Q2 sends the case-sensitive lookup to this card. Its expected line also records a second cell: `partitionBy('T')` writes `t=x/` where Spark refuses
  `_LEGACY_ERROR_TEMP_1155`.

The brief describes the S2 as "`partitionBy("B")` partitions by `b`". The verdict's repro is the
reverse: `partitionBy('b')` partitions by `B`. This card uses the verdict's wording. The reverse
shape is not in any file the card cites, so this card leaves it out.

Not in this card's ask, recorded so the owner can route it. The verdict's S3, "Loud residues on the
duplicate csv door, declared or inherited": a timestamp partition column refuses "not yet supported
to write to hive partitions"; a missing partition column refuses with repark's own text rather than
`_LEGACY_ERROR_TEMP_1155`; `partitionBy('T')` names the directory `t=x` where Spark writes `T=x`;
and an empty partitioned write leaves a header-only root file. The verdict says all five are the
same for unique-name frames or sit on registry row FA-5.

## Step 0

On live Spark 4.1.2 and on main `6c04b218`:

1. Re-run each row of the table above, with `header` set explicitly and without it. Record the
   bytes and the file list.
2. Re-run the verifier's S2 shape and the reverse shape. Record Spark's and repark's answers.
3. Say in this card which rows still differ. A row that no longer differs is closed with its date.

## The ask

Decide, per row, whether repark matches Spark, and fix the rows the owner rules to fix. A row the
owner keeps as a difference is recorded here with the ruling and its date. The S2 case-sensitive
partition lookup is the one row that writes to a wrong column, not just a different byte. Lean: take
it first.

## Gates

- Each row the owner rules to fix equals Spark's recorded answer, bytes and file list.
- A pin per row, failing for that row's reason under a mutation.
- A unique-name csv write costs what it costs on main. The FA-5 ledger's bar was 3%.
- Parquet, json and orc keep their refusals on duplicate-name frames, as FA-5 pinned them.
- The `map.md` of every directory the change touches is current.

## Pointers

- [fa-5-6-ledger.md](../../ledgers/staging/fa-5-6-ledger.md), "Fold 1 — FA-5 as it ships", and
  its clauses C-002 and C-003.
- The verifier's verdict and rulings, as scratch: `/tmp/oc-worker/direct/wo/fa-5-6/reverify/verdict.json`
  and `/tmp/oc-worker/direct/wo/fa-5-6/rulings-r1.md`. Orchestrator scratch, not in the repo.
- The FA-6 card, which carries the view half of the same unit: [fa-6-duplicate-view-schemas-card-2026-10-08.md](fa-6-duplicate-view-schemas-card-2026-10-08.md).
