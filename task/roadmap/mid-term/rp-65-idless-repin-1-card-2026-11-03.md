# RP-65-IDLESS-REPIN-1: repin the fork to the top-level positional fix, with the no-mapping pins through both doors and the facade

**Filed:** 2026-11-03. **Filed by:** `claude-haiku-5-5` (the RP-65 records round), from the Opus 5.5 builder's hand-back and the landed line of the fork card.

**Status:** closed 2026-11-03 at `bbd87737` (the RP-65 PR #1246, squashed to main as `63bbb1b6`) after the two verifiers. The unit ledger is [rp-65-idless-repin-1-ledger.md](../../ledgers/completed/rp-65-idless-repin-1-ledger.md). Lead L1 stays open on its own card, and lead L2 is recorded below against the fork's S-40.

**Target pin:** `66901f02a2df26098cfe073605152aeb518808f5`, the squash of fork PR `#381` (F-IDLESS-TOPLEVEL-POSITIONAL-1, merged 2026-11-02), moved from `56ff5545` (fork PR `#380`). The fork card is [f-idless-toplevel-positional-1-card-2026-11-01.md](f-idless-toplevel-positional-1-card-2026-11-01.md).

**Why:** At `56ff5545`, an id-less Parquet file admitted by `add_files` with no name mapping (the state that `ALTER TABLE … UNSET TBLPROPERTIES ('schema.name-mapping.default')` reaches) read a top-level string/binary pair as silent NULL on both doors, and every filter on the pair answered `[]`. Eight of the nine Rust pins and both facade pins were red there. The fork's round-4 head named a blanket filter refusal; RePark never pinned that state, so the measured answers below are the silent-NULL ones.

**Measured (builder, 2026-11-03; both doors identical unless noted):**

| Cell | At `56ff5545` | At `66901f02` |
|---|---|---|
| string column read as binary, `SELECT k, v` | NULL on every row | `616263`, `78797a`, NULL, `c3a961` |
| string read as binary: `IS NOT NULL`, `!= X'616263'`, `LIKE 'a%'` | `[]` each | `[1, 2, 4]`, `[2, 4]`, `[1]` |
| string read as binary: `=`, `<`, `IN` | `[]` each | refuse with DataInvalid (`IN` names `=`, the pushdown's OR of equalities) |
| binary read as string, `SELECT k, v` | NULL on every row | `abc`, `xyz`, NULL |
| binary read as string: `IS NULL` | `[3]` | `[3]` |
| binary read as string: `!= 'abc'`, `LIKE 'a%'`, `NOT LIKE 'a%'` | `[]` each | `[2]`, `[1]`, `[2]` |
| binary read as string: `=`, `>` | `[]` each | refuse with DataInvalid |
| reordered type-changing file `(v STRING, k INT)` under `(k INT, v STRING)`, no mapping | `["NULL\|1", "NULL\|2"]` | refuses by position: `data file column 'v' at position 0 has type string and cannot be read as table field 'k' (id 1) of type int` |
| same file with a name mapping | `1\|abc`, `2\|NULL` (read by name at both pins, the control row) | `1\|abc`, `2\|NULL`; `v = 'abc'` gives `[1]` |
| the string/binary pair under a name mapping, `SELECT k, v` (pin `the_same_files_with_a_name_mapping_read_the_pair_by_name`) | NULL on every row of both files | `1\|616263`, `2\|78797a`, `3\|NULL`, `4\|c3a961` (string read as binary); `1\|abc`, `2\|xyz`, `3\|NULL` (binary read as string); `IS NOT NULL` `[1, 2, 4]`; binary read as string `LIKE 'a%'` `[1]` |
| DECIMAL(12,2) file under DECIMAL(10,2), overflowing value, column bounds set to `counts` | `["1\|NULL", "2\|NULL"]`; `d > 0` and `IS NOT NULL` `[]` | the read, `d > 0` and `IS NOT NULL` refuse with `cannot read a data file column of type Decimal128(12, 2) as Decimal128(10, 2)` |
| reordered same-type file with a mapping (control) | green | green |
| facade pins (two tests) | red: `[None, None, None] == [b'abc', b'xyz', None]` and `[(1, None), …]` | green |

The overflowing decimal file is refused at `add_files` when the table keeps column bounds; the pin sets `write.metadata.metrics.default` to `counts` so the file is admitted (lead L1 below).

**Spark parity:** the Spark 4.1.2 answers for the string/binary, decimal and raw-bytes pins come from the fork ledger at `66901f02` (S-23, S-27, S-33 to S-36 and S-45), not from the RePark fork card, which holds only the reorder and evolution shapes. Every answered and refused cell in the table matches those cells; the mapping-route `=` and `>` on a binary column read as string are a neighbour outside the pins where Spark raises (lead L2). The `binary LIKE 'a%'` prefix pin, `NOT LIKE`, and the mapping-route `IS NOT NULL` and `LIKE` cells follow the measured rule (S-45 measured `startswith(v, X'61')`); the `LIKE` spelling is unmeasured on Spark. `IN` on the pair refuses naming `=` on both doors: the pushdown hands the fork an OR of equalities. Spark also refuses `IN`, so the refusal is Spark-equal and only the operator name differs from the fork's own `IN` pin. `STARTS WITH` on a string column read as binary never reaches the fork's predicate from either door (the planner casts the column and the scan is unpushed), so RePark answers it above the scan with Spark's answer `[1]`; the fork's S-45 cell is unreachable from RePark. `!=` excludes the NULL row, because DataFusion re-applies the filter above the scan, so the S-43 nulls-first lead does not surface through either door.

**Lead L1 (Spark unmeasured):** RePark's `add_files` refuses an id-less DECIMAL(12,2) file holding the unscaled value 123456789012 under a DECIMAL(10,2) table when column-bounds metrics are on: `DataInvalid => Decimal value 123456789012 is too large for precision 10`, from the fork's bound conversion in `add_files_datafile.rs`. The fork ledger's S-27 record says Spark's `add_files` admitted the shape. Carded as [add-files-idless-decimal-bounds-1-card-2026-11-03.md](add-files-idless-decimal-bounds-1-card-2026-11-03.md).

**Lead L2 (S2, fork-side, OPEN):** on the mapping route a binary column read as string answers `=` and `>` where Spark raises: `SELECT k FROM ice.ns.t WHERE v = 'abc'` answers k = 1 and `v > 'b'` answers k = 2; a string column read as binary answers `v = X'616263'` with k = 1, where Spark raises ClassCastException (the fork's S-40 class). The neighbours sit outside the pin span, and the repin moved them from no rows to these answers. The fork records S-40 OPEN upstream; there is no RePark card until the fork fixes it. Found by the Muse verifier (ledger clause C-011).

**Pins:** `crates/repark-sql/tests/idless_toplevel_door.rs` (nine tests through the Spark door and an adopted native session; ledger clause C-001) and `python/repark/tests/iceberg/test_ice_idless_toplevel_1.py` (two facade tests; ledger clause C-002). The clause numbers are the unit ledger's: C-001 and C-002, bound in its pins sidecar.

**Gates:** the builder's run was green: `cargo metadata`, build and test (repark-sql 654, repark-iceberg 1061 and repark-spark 3141 passed), fmt, clippy, `make rust-panic-ban`, the file-size, model-ban, lib-rs, audit and deny gates, ruff, and the facade idless suites (21 passed). The repin PR's CI is the merge gate, plus `make rust-panic-ban`.

**Out of scope:** the decimal `add_files` admission (L1); the nested by-name reads on the no-mapping route, which stay the kept divergence in RePark's favour (fork card); the re-added top-level name and the top-level partial-mapping shapes, which RP-65 does not pin (the Muse differential found both Spark-equal at `66901f02`; ledger clause C-010, OPEN until pinned).

**Retires:** met in part. The repin merged (PR #1246, `63bbb1b6`) with its pins green in CI. L1 is still open on its own card, so the L1 condition is not yet met.

Up: [map.md](map.md).
