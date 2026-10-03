# Card VIEW-LINEAGE-SELFJOIN-1 — a view read keeps its frame's lineage for self-join checks

**Date:** 2026-10-03 · **Filed by:** ATTR-ID-1 SJ-6 (card only, no implementation) · **Source:**
the self-join design's §5.3 residue 1
([attr-id-1-ledger.md](../../ledgers/staging/attr-id-1-ledger.md), SJ-6 residues).

## Why

A view read keeps its frame's lineage in Spark, not in RePark. Spark's view stores the
plan with the registering frame's dataset tag, so a self-join against a view of the same
frame refuses; RePark roots `S.table` and `S.sql` frames, cutting the lineage, so the
option-A detector answers. Measured on live Spark 4.1.2 (`sj3.spark.json`,
`d = [(1,10),(2,20),(3,30)]`, `t = S.table(view of d)`, `q = spark.sql("SELECT * FROM
view")`):

| Cell | Shape | Spark answer | RePark today |
|---|---|---|---|
| `J_view_sel_d` | `d.join(t, …).select(d.v)` | 1182, `Column v#5L are ambiguous` | answers the left `v` |
| `J_sql_sel_d` | `d.join(q, …).select(d.v)` | 1182, `Column v#13L are ambiguous` | answers the left `v` |
| `J_view_sel_t` | `d.join(t, …).select(t.v)` | 1182 | 1182 (their ids are `d`'s) |
| `J_sql_sel_q` | `d.join(q, …).select(q.v)` | 1182 | 1182 |
| `J_view_left_sel_t` | the left-join twin | rows `[[10],[20],[30]]` | answers (over-fire guard) |

No pin covers `J_view_sel_d` or `J_sql_sel_d` yet; `J_view_sel_t`, `J_sql_sel_q` and
`J_view_left_sel_t` are pinned in `test_attr_id_1_sj4.py`.

## Decisions (proposed)

| id | decision |
|---|---|
| D-1 | The temp-view registry stores the registering frame's lineage node; `S.table` and `S.sql` build from it instead of rooting. Rust-first, like the join nodes. |
| D-2 | `J_view_sel_d` and `J_sql_sel_d` refuse 1182 with Spark's names; `J_view_sel_t` and `J_sql_sel_q` stay 1182; `J_view_left_sel_t` still answers. |
| D-3 | Measure first: views over joined frames, views over views, and `cache()`/`checkpoint()` between register and read, before ruling what the stored node covers. |

## Steps

| step | worker | what |
|---|---|---|
| 0 | clerk | Measure (live Spark): the D-3 shapes, recorded beside the probe. No product code. |
| 1 | executor | D-1 wiring at view register/read; D-2 pins. |

**Home:** the temp-view registry and the `S.table`/`S.sql` frame constructors,
`test_attr_id_1_sj4.py`, a new oracle pin file. **Gates:** `make verify`, `make preflight`,
the parity suite, the S0 replay with zero moves away from Spark.

## Pointers

- Up: [map.md](map.md)
