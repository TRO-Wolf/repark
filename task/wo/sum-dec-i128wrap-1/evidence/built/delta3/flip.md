# The four moved revision cells: main's #1362, measured (2026-11-19)

`rev/dfview_flip/g{5,7}/c38_0` (`run_revision.py`): a DataFrame temp view `v_dfsum` (`T.groupBy("g").agg(sf.sum(...))`)
created under one ANSI setting and read after `spark.sql.ansi.enabled` flips; its SQL twin `rev/view_flip` reads the
SQL view `v_sum` the same way. Spark answers each under the view's captured setting. RePark's answers for `g5` on four
builds (`run_revision.py repark <ansi>`, cores 48–57; `g7` is the same):

| build | created with ANSI | `view_flip` (SQL view) | `dfview_flip` (DataFrame view) |
|---|---|---|---|
| main before #1362, `pkg-main-6286ba19` | on / off | `100000000000000000000000000000000000000` / the same | the same / the same |
| main now, `pkg-main-4c365814` (#1362) | on / off | `100000000000000000000000000000000000000` / the same | the same / the same |
| delta 2's head, `pkg-sum-fcb0c888` | on / off | `ARITHMETIC_OVERFLOW` / NULL | **NULL / `ARITHMETIC_OVERFLOW`** |
| delta 3's head, `pkg-sum-845766fd` | on / off | `ARITHMETIC_OVERFLOW` / NULL | `ARITHMETIC_OVERFLOW` / NULL |

On delta 2's head the DataFrame view answered under the flipped session setting (TEMP-VIEW-SESSION-SETTINGS-1 part 2,
then open; part 1, the SQL view, was already in `6286ba19`). Main `4c365814` (#1362) closed part 2 and reached this
branch in the merge `de471f60`; on the head both views answer under their captured setting, as Spark does. Both mains
sum `c38_0` with a wrap, so their answer is the same 39-digit value in either mode and cannot show the setting.
