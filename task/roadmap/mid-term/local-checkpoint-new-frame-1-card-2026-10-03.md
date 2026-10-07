# Card LOCAL-CHECKPOINT-NEW-FRAME-1 — `localCheckpoint()` returns a new frame

**Date:** 2026-10-03 · **Filed by:** ATTR-ID-1 SJ-6 (card only, no implementation) · **Source:**
the self-join design's §5.3 residue 2 and ruling R-SJ4-1 (SJ-4 hand-back Q1, accepted
2026-10-02; [attr-id-1-ledger.md](../../ledgers/staging/attr-id-1-ledger.md), SJ-6 residues).

## Why

`localCheckpoint()` returns the same frame object (`return frame`); Spark returns a new
Dataset. That is a pre-existing API deviation, and under option A it flips one cell:
both selects below carry identical attribute and frame tokens, so no funnel-side fix can
split them. Measured (`sj2.spark.json`, `d = [(1,10),(2,20),(3,30)]`,
`c = d.localCheckpoint()`):

| Cell | Shape | Spark answer | RePark today |
|---|---|---|---|
| `I_checkpoint_sel_d` | `d.join(c, d.id == c.id).select(d.v)` | rows `[[10],[20],[30]]` | 1182 (strict xfail, R-SJ4-1) |
| `I_checkpoint_sel_c` | `d.join(c, d.id == c.id).select(c.v)` | 1182 | 1182 |

`I_checkpoint_sel_d` is the strict-xfail pin `test_sj4_i_checkpoint_sel_d` (reason `card
LOCAL-CHECKPOINT-NEW-FRAME-1`); it is the one probe-cell regression SJ-4 carried, and it
is not in the 842-cell inventory. The same-object semantics are pinned (out-is-frame, eager
repr materialization, cache lineage tests), so object separation breaks pins on purpose.

## Decisions (proposed)

| id | decision |
|---|---|
| D-1 | `localCheckpoint()` returns a new frame object; `I_checkpoint_sel_d` answers Spark's rows and `I_checkpoint_sel_c` still refuses 1182. |
| D-2 | The strict xfail un-xfails, and the same-object pins are re-measured against live Spark and rewritten to the new semantics. |
| D-3 | Measure first: checkpoint of a joined frame and checkpoint between register and read, before ruling what lineage the new frame carries. |

## Steps

| step | worker | what |
|---|---|---|
| 0 | clerk | Measure (live Spark): the D-3 shapes, recorded beside the probe. No product code. |
| 1 | executor | D-1 separation; D-2 pin rewrites. |

**Home:** `localCheckpoint` in the surface-A module, `test_attr_id_1_sj4.py`, the
checkpoint pin files, a new oracle pin file. **Gates:** `make verify`, `make preflight`,
the parity suite, the S0 replay with zero moves away from Spark.

## Pointers

- Up: [map.md](map.md)
