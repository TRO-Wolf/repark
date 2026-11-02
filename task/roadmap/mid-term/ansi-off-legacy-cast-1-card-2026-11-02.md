# Card ANSI-OFF-LEGACY-CAST-1: with ANSI off, an array element of a map key is cast by RePark's ANSI-on rule, where Spark uses its legacy implicit cast

**Date:** 2026-11-02. **Filed by:** `claude-haiku-5-5` (records clerk) for the orchestrator, from the MAP-STRUCT-KEY-SUBSCRIPT-1 round-5 Opus verdict (its F2), the round-5 Muse hand-back (its R5-1) and the domain table's reading 2.

**Status:** filed; grade B; not dispatched. Carded out of MAP-STRUCT-KEY-SUBSCRIPT-1 under its one-round ruling (ledger clause C-055). Against the base: the verdict's base wheel refuses the cells of classes (a) to (c) below with `Can't compare arrays of different types`; Muse's base `80c529fc` answers the four R5-1 cells as the head does. The base was not measured for the other table cells.

**Why:** The round-5 verifier reads the cause as follows. Under `spark.sql.ansi.enabled=false`, RePark keeps the ANSI-on accept set and the ANSI-on cast for the elements of an array key (`accepts` and `leaf_accepts`), where Spark uses its legacy implicit cast. Spark's legacy cast nullability (`Cast.forceNullable`) refuses a cast that can yield NULL into a NOT NULL element, as class (c) shows. The round-4 reading (the nullable-element class of C-040 (1)) does not hold for these cells.

**The clause (the domain table, reading 2):** the table's array-element group measures a literal map, `map(array(CAST(v AS T1)), 7)[array(CAST(v AS T2))]`, for every pair of 14 element types, under ANSI on and off. Under ANSI off, of 196 cells, 62 answer equal, 94 refuse equal, 39 differ (RePark answers where Spark refuses `UNEXPECTED_INPUT_TYPE`), and 1 differs the other way. Under ANSI on, all 196 are equal. The 39 cells are:

- (a) an integer-family key element (`tinyint`, `smallint`, `int`, `bigint`) against a `float`, `double`, `decimal(5,2)`, `decimal(6,3)` or `decimal(2,1)` probe element: 20 cells. RePark answers `[["7"]]`.
- (b) a decimal key element (`decimal(5,2)`, `decimal(6,3)`, `decimal(2,1)`) against a `float`, `double` or decimal probe element (9 cells), or against an integer-family probe element (10 cells): 19 cells. RePark answers `[["7"]]` or `[["None"]]`.

The one cell that differs the other way is a `binary` key element against a `string` probe element: Spark answers `[["7"]]` and RePark refuses. Copied from the table:

```text
key int            probe float          Spark ERR DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE   RePark rows [["7"]]
key decimal(5,2)   probe int            Spark ERR DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE   RePark rows [["None"]]
key binary         probe string         Spark rows [["7"]]                                 RePark ERR DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE
```

**Stored maps (outside the table, measured by the round-5 verdict):** against nullable elements (a stored map), with ANSI off, Spark 4.1.2 against the head:

```text
(a) SELECT m[array('1')] FROM p_str_int             MAP<ARRAY<INT>, INT>        Spark [[7]]    RePark refuses
(a) m[array('1.5')] on ARRAY<DOUBLE>, m[array('2024-01-01')] on ARRAY<DATE>,
    m[array('2024-01-01 00:00:00')] on ARRAY<TIMESTAMP>, m[array('a')] on ARRAY<BINARY>
                                                                                    Spark [[7]] each    RePark refuses each
(b) SELECT m[array(CAST(5000000001 AS BIGINT))] FROM p_bigint_int_ovf (entry array(705032705))
                                                                                    Spark [[7]]    RePark Arrow cast error
```

**R5-1 (Muse, round 5), literal map, ANSI off:** `arrdec_round`, `arrdec_round_low`, `arrdec_neg_round` and `arrint_dec_1_5` answer `[[8]]`, `[[7]]`, `[[8]]` and `[[7]]` on RePark, in two stable runs. Spark refuses `UNEXPECTED_INPUT_TYPE` (Muse names `ARRAY<DECIMAL(5,2)>` against `ARRAY<DECIMAL(6,3)>` and `ARRAY<INT>` against `ARRAY<DECIMAL(2,1)>`).

**Fix (proposed):** under ANSI off, match Spark's legacy implicit cast for array elements, so that the 39 table cells and the binary-to-string cell answer as Spark does, and keep the ANSI-on cells as they are. The proof is one table-driven pin per door (the Spark door and the ANSI door), over the table as the clause. Muse's measured caution: a blanket exact-elements rule would newly diverge on `arr_bigint`, which Spark answers with ANSI off, so the rule must be pair-sensitive.

**Retires:** the card when the 39 table cells, the binary-to-string cell, the stored-map cells and the R5-1 cells above answer as Spark does, pinned table-driven per door.

**Relation:** [MAP-STRUCT-KEY-SUBSCRIPT-1](../../ledgers/staging/map-struct-key-subscript-1-ledger.md) clauses C-048 (rewritten, out of scope, carded) and C-055; its [domain table](../../ledgers/staging/map-struct-key-subscript-1-domain-table.md). Up: [map.md](map.md).
