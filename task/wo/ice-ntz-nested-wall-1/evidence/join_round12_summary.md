# Round 12 joined-head measurement

Head: the joined branch release module, measured by join_round12. Spark: live Spark 4.1.2 with Iceberg 1.11, measured by join_round12. Main: taken from the two verify1 verdicts' recorded outputs (the coercion per-cell main rows; the F1 wall and instant rows from the F1 finding, which are zone-independent).

## America/New_York

| cell | row | head | spark | main | verdict |
|---|---|---|---|---|---|
| f1/insert-by-name-union | 1 | [1783149753654321, 1] | [1783149753654321, 1] | [1783164153654321, 1] | same |
| f1/insert-by-name-union | 2 | [1767323045123456, 2] | [1767323045123456, 2] | [1767323045123456, 2] | same |
| f1/insert-case-subquery | 1 | [1783149753654321, 1] | [1783149753654321, 1] | [1783164153654321, 1] | same |
| f1/insert-case-subquery | 2 | [1767323045123456, 2] | [1767323045123456, 2] | [1767323045123456, 2] | same |
| f1/insert-union | 1 | [1783149753654321, 1] | [1783149753654321, 1] | [1783164153654321, 1] | same |
| f1/insert-union | 2 | [1767323045123456, 2] | [1767323045123456, 2] | [1767323045123456, 2] | same |
| f1/merge-update-case | 1 | [1783149753654321, 1] | [1783149753654321, 1] | [1783164153654321, 1] | same |
| f1/merge-update-case | 2 | [1767323045123456, 2] | [1767323045123456, 2] | [1767323045123456, 2] | same |
| f1/update-case | 1 | [1783149753654321, 100] | [1783149753654321, 100] | [1783164153654321, 100] | same |
| f1/update-case | 2 | [1767323045123456, 2] | [1767323045123456, 2] | [1767323045123456, 2] | same |
| f1/update-coalesce | 1 | [1783149753654321, 100] | [1783149753654321, 100] | [1783164153654321, 100] | same |
| f1/update-coalesce | 2 | [1767323045123456, 2] | [1767323045123456, 2] | [1767323045123456, 2] | same |
| ntz-target/df-append | 1 | [1767323045123456, 7] | [1767323045123456, 7] | [1767323045123456, 7] | same |
| ntz-target/df-append | 11 | [1767323045123456, 7] | [1767323045123456, 7] | [1767341045123456, 7] | same |
| ntz-target/df-append | 12 | [1767305045123456, 7] | [1767305045123456, 7] | [1767323045123456, 7] | same |
| ntz-target/df-append | 13 | [1783149753654321, 7] | [1783149753654321, 7] | [1783164153654321, 7] | same |
| ntz-target/df-append | 14 | [None, None] | [None, None] | [None, None] | same |
| ntz-target/df-append | 2 | [None, None] | [None, None] | [None, None] | same |
| ntz-target/df-append | 3 | [None, 8] | [None, 8] | [None, 8] | same |
| ntz-target/df-append | 4 | [1783149753654321, 7] | [1783149753654321, 7] | [1783149753654321, 7] | same |
| ntz-target/insert-case | 1 | [1767323045123456, 7] | [1767323045123456, 7] | [1767323045123456, 7] | same |
| ntz-target/insert-case | 2 | [1767305045123456, 7] | [1767305045123456, 7] | [1767323045123456, 7] | same |
| ntz-target/insert-case | 3 | [None, 8] | [None, 8] | [None, 8] | same |
| ntz-target/insert-case | 4 | [1783149753654321, 7] | [1783149753654321, 7] | [1783149753654321, 7] | same |
| ntz-target/insert-coalesce | 1 | [1767323045123456, 7] | [1767323045123456, 7] | [1767323045123456, 7] | same |
| ntz-target/insert-coalesce | 2 | [1767305045123456, 7] | [1767305045123456, 7] | [1767323045123456, 7] | same |
| ntz-target/insert-coalesce | 3 | [None, 8] | [None, 8] | [None, 8] | same |
| ntz-target/insert-coalesce | 4 | [1783149753654321, 7] | [1783149753654321, 7] | [1783149753654321, 7] | same |
| ntz-target/insert-overwrite-union | 1 | [1767323045123456, 7] | [1767323045123456, 7] | [1767323045123456, 7] | same |
| ntz-target/insert-overwrite-union | 11 | [1767323045123456, 7] | [1767323045123456, 7] | [1767341045123456, 7] | same |
| ntz-target/insert-overwrite-union | 12 | [1767305045123456, 7] | [1767305045123456, 7] | [1767323045123456, 7] | same |
| ntz-target/insert-overwrite-union | 13 | [1783149753654321, 7] | [1783149753654321, 7] | [1783164153654321, 7] | same |
| ntz-target/insert-overwrite-union | 14 | [None, None] | [None, None] | [None, None] | same |
| ntz-target/insert-overwrite-union | 2 | [None, None] | [None, None] | [None, None] | same |
| ntz-target/insert-overwrite-union | 3 | [None, 8] | [None, 8] | [None, 8] | same |
| ntz-target/insert-overwrite-union | 4 | [1783149753654321, 7] | [1783149753654321, 7] | [1783149753654321, 7] | same |
| ntz-target/merge-insert-coalesce | 1 | [1767323045123456, 7] | [1767323045123456, 7] | [1767323045123456, 7] | same |
| ntz-target/merge-insert-coalesce | 101 | [1767323045123456, 7] | [1767323045123456, 7] | [1767323045123456, 7] | same |
| ntz-target/merge-insert-coalesce | 102 | [1767305045123456, 7] | [1767305045123456, 7] | [1767323045123456, 7] | same |
| ntz-target/merge-insert-coalesce | 103 | [None, 8] | [None, 8] | [None, 8] | same |
| ntz-target/merge-insert-coalesce | 104 | [1783149753654321, 7] | [1783149753654321, 7] | [1783149753654321, 7] | same |
| ntz-target/merge-insert-coalesce | 2 | [1767323045123456, 7] | [1767323045123456, 7] | [1767323045123456, 7] | same |
| ntz-target/merge-insert-coalesce | 3 | [None, None] | [None, None] | [None, None] | same |
| ntz-target/merge-source-union | 1 | [1767323045123456, 7] | [1767323045123456, 7] | [1767323045123456, 7] | same |
| ntz-target/merge-source-union | 101 | [1767323045123456, 7] | [1767323045123456, 7] | [1767323045123456, 7] | same |
| ntz-target/merge-source-union | 102 | [None, None] | [None, None] | [None, None] | same |
| ntz-target/merge-source-union | 103 | [None, 8] | [None, 8] | [None, 8] | same |
| ntz-target/merge-source-union | 104 | [1783149753654321, 7] | [1783149753654321, 7] | [1783149753654321, 7] | same |
| ntz-target/merge-source-union | 111 | [1767323045123456, 7] | [1767323045123456, 7] | [1767341045123456, 7] | same |
| ntz-target/merge-source-union | 112 | [1767305045123456, 7] | [1767305045123456, 7] | [1767323045123456, 7] | same |
| ntz-target/merge-source-union | 113 | [1783149753654321, 7] | [1783149753654321, 7] | [1783164153654321, 7] | same |
| ntz-target/merge-source-union | 114 | [None, None] | [None, None] | [None, None] | same |
| ntz-target/merge-source-union | 2 | [1767323045123456, 7] | [1767323045123456, 7] | [1767323045123456, 7] | same |
| ntz-target/merge-source-union | 3 | [None, None] | [None, None] | [None, None] | same |
| ntz-target/merge-update-case | 1 | [1767323045123456, 7] | [1767323045123456, 7] | [1767341045123456, 7] | same |
| ntz-target/merge-update-case | 2 | [1767323045123456, 7] | [1767323045123456, 7] | [1767323045123456, 7] | same |
| ntz-target/merge-update-case | 3 | [None, None] | [None, None] | [None, None] | same |
| ntz-target/update-case-where | 1 | [1783149753654321, 7] | [1783149753654321, 7] | [1783164153654321, 7] | same |
| ntz-target/update-case-where | 2 | [1767323045123456, 7] | [1767323045123456, 7] | [1767323045123456, 7] | same |
| ntz-target/update-case-where | 3 | [None, None] | [None, None] | [None, None] | same |

## Australia/Lord_Howe

| cell | row | head | spark | main | verdict |
|---|---|---|---|---|---|
| f1/insert-by-name-union | 1 | [1783201953654321, 1] | [1783201953654321, 1] | [1783164153654321, 1] | same |
| f1/insert-by-name-union | 2 | [1767323045123456, 2] | [1767323045123456, 2] | [1767323045123456, 2] | same |
| f1/insert-case-subquery | 1 | [1783201953654321, 1] | [1783201953654321, 1] | [1783164153654321, 1] | same |
| f1/insert-case-subquery | 2 | [1767323045123456, 2] | [1767323045123456, 2] | [1767323045123456, 2] | same |
| f1/insert-union | 1 | [1783201953654321, 1] | [1783201953654321, 1] | [1783164153654321, 1] | same |
| f1/insert-union | 2 | [1767323045123456, 2] | [1767323045123456, 2] | [1767323045123456, 2] | same |
| f1/merge-update-case | 1 | [1783201953654321, 1] | [1783201953654321, 1] | [1783164153654321, 1] | same |
| f1/merge-update-case | 2 | [1767323045123456, 2] | [1767323045123456, 2] | [1767323045123456, 2] | same |
| f1/update-case | 1 | [1783201953654321, 100] | [1783201953654321, 100] | [1783164153654321, 100] | same |
| f1/update-case | 2 | [1767323045123456, 2] | [1767323045123456, 2] | [1767323045123456, 2] | same |
| f1/update-coalesce | 1 | [1783201953654321, 100] | [1783201953654321, 100] | [1783164153654321, 100] | same |
| f1/update-coalesce | 2 | [1767323045123456, 2] | [1767323045123456, 2] | [1767323045123456, 2] | same |
| ntz-target/df-append | 1 | [1767323045123456, 7] | [1767323045123456, 7] | [1767323045123456, 7] | same |
| ntz-target/df-append | 11 | [1767380645123456, 7] | [1767380645123456, 7] | [1767341045123456, 7] | same |
| ntz-target/df-append | 12 | [1767362645123456, 7] | [1767362645123456, 7] | [1767323045123456, 7] | same |
| ntz-target/df-append | 13 | [1783201953654321, 7] | [1783201953654321, 7] | [1783164153654321, 7] | same |
| ntz-target/df-append | 14 | [None, None] | [None, None] | [None, None] | same |
| ntz-target/df-append | 2 | [None, None] | [None, None] | [None, None] | same |
| ntz-target/df-append | 3 | [None, 8] | [None, 8] | [None, 8] | same |
| ntz-target/df-append | 4 | [1783149753654321, 7] | [1783149753654321, 7] | [1783149753654321, 7] | same |
| ntz-target/insert-case | 1 | [1767323045123456, 7] | [1767323045123456, 7] | [1767323045123456, 7] | same |
| ntz-target/insert-case | 2 | [1767362645123456, 7] | [1767362645123456, 7] | [1767323045123456, 7] | same |
| ntz-target/insert-case | 3 | [None, 8] | [None, 8] | [None, 8] | same |
| ntz-target/insert-case | 4 | [1783149753654321, 7] | [1783149753654321, 7] | [1783149753654321, 7] | same |
| ntz-target/insert-coalesce | 1 | [1767323045123456, 7] | [1767323045123456, 7] | [1767323045123456, 7] | same |
| ntz-target/insert-coalesce | 2 | [1767362645123456, 7] | [1767362645123456, 7] | [1767323045123456, 7] | same |
| ntz-target/insert-coalesce | 3 | [None, 8] | [None, 8] | [None, 8] | same |
| ntz-target/insert-coalesce | 4 | [1783149753654321, 7] | [1783149753654321, 7] | [1783149753654321, 7] | same |
| ntz-target/insert-overwrite-union | 1 | [1767323045123456, 7] | [1767323045123456, 7] | [1767323045123456, 7] | same |
| ntz-target/insert-overwrite-union | 11 | [1767380645123456, 7] | [1767380645123456, 7] | [1767341045123456, 7] | same |
| ntz-target/insert-overwrite-union | 12 | [1767362645123456, 7] | [1767362645123456, 7] | [1767323045123456, 7] | same |
| ntz-target/insert-overwrite-union | 13 | [1783201953654321, 7] | [1783201953654321, 7] | [1783164153654321, 7] | same |
| ntz-target/insert-overwrite-union | 14 | [None, None] | [None, None] | [None, None] | same |
| ntz-target/insert-overwrite-union | 2 | [None, None] | [None, None] | [None, None] | same |
| ntz-target/insert-overwrite-union | 3 | [None, 8] | [None, 8] | [None, 8] | same |
| ntz-target/insert-overwrite-union | 4 | [1783149753654321, 7] | [1783149753654321, 7] | [1783149753654321, 7] | same |
| ntz-target/merge-insert-coalesce | 1 | [1767323045123456, 7] | [1767323045123456, 7] | [1767323045123456, 7] | same |
| ntz-target/merge-insert-coalesce | 101 | [1767323045123456, 7] | [1767323045123456, 7] | [1767323045123456, 7] | same |
| ntz-target/merge-insert-coalesce | 102 | [1767362645123456, 7] | [1767362645123456, 7] | [1767323045123456, 7] | same |
| ntz-target/merge-insert-coalesce | 103 | [None, 8] | [None, 8] | [None, 8] | same |
| ntz-target/merge-insert-coalesce | 104 | [1783149753654321, 7] | [1783149753654321, 7] | [1783149753654321, 7] | same |
| ntz-target/merge-insert-coalesce | 2 | [1767323045123456, 7] | [1767323045123456, 7] | [1767323045123456, 7] | same |
| ntz-target/merge-insert-coalesce | 3 | [None, None] | [None, None] | [None, None] | same |
| ntz-target/merge-source-union | 1 | [1767323045123456, 7] | [1767323045123456, 7] | [1767323045123456, 7] | same |
| ntz-target/merge-source-union | 101 | [1767323045123456, 7] | [1767323045123456, 7] | [1767323045123456, 7] | same |
| ntz-target/merge-source-union | 102 | [None, None] | [None, None] | [None, None] | same |
| ntz-target/merge-source-union | 103 | [None, 8] | [None, 8] | [None, 8] | same |
| ntz-target/merge-source-union | 104 | [1783149753654321, 7] | [1783149753654321, 7] | [1783149753654321, 7] | same |
| ntz-target/merge-source-union | 111 | [1767380645123456, 7] | [1767380645123456, 7] | [1767341045123456, 7] | same |
| ntz-target/merge-source-union | 112 | [1767362645123456, 7] | [1767362645123456, 7] | [1767323045123456, 7] | same |
| ntz-target/merge-source-union | 113 | [1783201953654321, 7] | [1783201953654321, 7] | [1783164153654321, 7] | same |
| ntz-target/merge-source-union | 114 | [None, None] | [None, None] | [None, None] | same |
| ntz-target/merge-source-union | 2 | [1767323045123456, 7] | [1767323045123456, 7] | [1767323045123456, 7] | same |
| ntz-target/merge-source-union | 3 | [None, None] | [None, None] | [None, None] | same |
| ntz-target/merge-update-case | 1 | [1767380645123456, 7] | [1767380645123456, 7] | [1767341045123456, 7] | same |
| ntz-target/merge-update-case | 2 | [1767323045123456, 7] | [1767323045123456, 7] | [1767323045123456, 7] | same |
| ntz-target/merge-update-case | 3 | [None, None] | [None, None] | [None, None] | same |
| ntz-target/update-case-where | 1 | [1783201953654321, 7] | [1783201953654321, 7] | [1783164153654321, 7] | same |
| ntz-target/update-case-where | 2 | [1767323045123456, 7] | [1767323045123456, 7] | [1767323045123456, 7] | same |
| ntz-target/update-case-where | 3 | [None, None] | [None, None] | [None, None] | same |

## UTC

| cell | row | head | spark | main | verdict |
|---|---|---|---|---|---|
| f1/insert-by-name-union | 1 | [1783164153654321, 1] | [1783164153654321, 1] | [1783164153654321, 1] | same |
| f1/insert-by-name-union | 2 | [1767323045123456, 2] | [1767323045123456, 2] | [1767323045123456, 2] | same |
| f1/insert-case-subquery | 1 | [1783164153654321, 1] | [1783164153654321, 1] | [1783164153654321, 1] | same |
| f1/insert-case-subquery | 2 | [1767323045123456, 2] | [1767323045123456, 2] | [1767323045123456, 2] | same |
| f1/insert-union | 1 | [1783164153654321, 1] | [1783164153654321, 1] | [1783164153654321, 1] | same |
| f1/insert-union | 2 | [1767323045123456, 2] | [1767323045123456, 2] | [1767323045123456, 2] | same |
| f1/merge-update-case | 1 | [1783164153654321, 1] | [1783164153654321, 1] | [1783164153654321, 1] | same |
| f1/merge-update-case | 2 | [1767323045123456, 2] | [1767323045123456, 2] | [1767323045123456, 2] | same |
| f1/update-case | 1 | [1783164153654321, 100] | [1783164153654321, 100] | [1783164153654321, 100] | same |
| f1/update-case | 2 | [1767323045123456, 2] | [1767323045123456, 2] | [1767323045123456, 2] | same |
| f1/update-coalesce | 1 | [1783164153654321, 100] | [1783164153654321, 100] | [1783164153654321, 100] | same |
| f1/update-coalesce | 2 | [1767323045123456, 2] | [1767323045123456, 2] | [1767323045123456, 2] | same |
| ntz-target/df-append | 1 | [1767323045123456, 7] | [1767323045123456, 7] | [1767323045123456, 7] | same |
| ntz-target/df-append | 11 | [1767341045123456, 7] | [1767341045123456, 7] | [1767341045123456, 7] | same |
| ntz-target/df-append | 12 | [1767323045123456, 7] | [1767323045123456, 7] | [1767323045123456, 7] | same |
| ntz-target/df-append | 13 | [1783164153654321, 7] | [1783164153654321, 7] | [1783164153654321, 7] | same |
| ntz-target/df-append | 14 | [None, None] | [None, None] | [None, None] | same |
| ntz-target/df-append | 2 | [None, None] | [None, None] | [None, None] | same |
| ntz-target/df-append | 3 | [None, 8] | [None, 8] | [None, 8] | same |
| ntz-target/df-append | 4 | [1783149753654321, 7] | [1783149753654321, 7] | [1783149753654321, 7] | same |
| ntz-target/insert-case | 1 | [1767323045123456, 7] | [1767323045123456, 7] | [1767323045123456, 7] | same |
| ntz-target/insert-case | 2 | [1767323045123456, 7] | [1767323045123456, 7] | [1767323045123456, 7] | same |
| ntz-target/insert-case | 3 | [None, 8] | [None, 8] | [None, 8] | same |
| ntz-target/insert-case | 4 | [1783149753654321, 7] | [1783149753654321, 7] | [1783149753654321, 7] | same |
| ntz-target/insert-coalesce | 1 | [1767323045123456, 7] | [1767323045123456, 7] | [1767323045123456, 7] | same |
| ntz-target/insert-coalesce | 2 | [1767323045123456, 7] | [1767323045123456, 7] | [1767323045123456, 7] | same |
| ntz-target/insert-coalesce | 3 | [None, 8] | [None, 8] | [None, 8] | same |
| ntz-target/insert-coalesce | 4 | [1783149753654321, 7] | [1783149753654321, 7] | [1783149753654321, 7] | same |
| ntz-target/insert-overwrite-union | 1 | [1767323045123456, 7] | [1767323045123456, 7] | [1767323045123456, 7] | same |
| ntz-target/insert-overwrite-union | 11 | [1767341045123456, 7] | [1767341045123456, 7] | [1767341045123456, 7] | same |
| ntz-target/insert-overwrite-union | 12 | [1767323045123456, 7] | [1767323045123456, 7] | [1767323045123456, 7] | same |
| ntz-target/insert-overwrite-union | 13 | [1783164153654321, 7] | [1783164153654321, 7] | [1783164153654321, 7] | same |
| ntz-target/insert-overwrite-union | 14 | [None, None] | [None, None] | [None, None] | same |
| ntz-target/insert-overwrite-union | 2 | [None, None] | [None, None] | [None, None] | same |
| ntz-target/insert-overwrite-union | 3 | [None, 8] | [None, 8] | [None, 8] | same |
| ntz-target/insert-overwrite-union | 4 | [1783149753654321, 7] | [1783149753654321, 7] | [1783149753654321, 7] | same |
| ntz-target/merge-insert-coalesce | 1 | [1767323045123456, 7] | [1767323045123456, 7] | [1767323045123456, 7] | same |
| ntz-target/merge-insert-coalesce | 101 | [1767323045123456, 7] | [1767323045123456, 7] | [1767323045123456, 7] | same |
| ntz-target/merge-insert-coalesce | 102 | [1767323045123456, 7] | [1767323045123456, 7] | [1767323045123456, 7] | same |
| ntz-target/merge-insert-coalesce | 103 | [None, 8] | [None, 8] | [None, 8] | same |
| ntz-target/merge-insert-coalesce | 104 | [1783149753654321, 7] | [1783149753654321, 7] | [1783149753654321, 7] | same |
| ntz-target/merge-insert-coalesce | 2 | [1767323045123456, 7] | [1767323045123456, 7] | [1767323045123456, 7] | same |
| ntz-target/merge-insert-coalesce | 3 | [None, None] | [None, None] | [None, None] | same |
| ntz-target/merge-source-union | 1 | [1767323045123456, 7] | [1767323045123456, 7] | [1767323045123456, 7] | same |
| ntz-target/merge-source-union | 101 | [1767323045123456, 7] | [1767323045123456, 7] | [1767323045123456, 7] | same |
| ntz-target/merge-source-union | 102 | [None, None] | [None, None] | [None, None] | same |
| ntz-target/merge-source-union | 103 | [None, 8] | [None, 8] | [None, 8] | same |
| ntz-target/merge-source-union | 104 | [1783149753654321, 7] | [1783149753654321, 7] | [1783149753654321, 7] | same |
| ntz-target/merge-source-union | 111 | [1767341045123456, 7] | [1767341045123456, 7] | [1767341045123456, 7] | same |
| ntz-target/merge-source-union | 112 | [1767323045123456, 7] | [1767323045123456, 7] | [1767323045123456, 7] | same |
| ntz-target/merge-source-union | 113 | [1783164153654321, 7] | [1783164153654321, 7] | [1783164153654321, 7] | same |
| ntz-target/merge-source-union | 114 | [None, None] | [None, None] | [None, None] | same |
| ntz-target/merge-source-union | 2 | [1767323045123456, 7] | [1767323045123456, 7] | [1767323045123456, 7] | same |
| ntz-target/merge-source-union | 3 | [None, None] | [None, None] | [None, None] | same |
| ntz-target/merge-update-case | 1 | [1767341045123456, 7] | [1767341045123456, 7] | [1767341045123456, 7] | same |
| ntz-target/merge-update-case | 2 | [1767323045123456, 7] | [1767323045123456, 7] | [1767323045123456, 7] | same |
| ntz-target/merge-update-case | 3 | [None, None] | [None, None] | [None, None] | same |
| ntz-target/update-case-where | 1 | [1783164153654321, 7] | [1783164153654321, 7] | [1783164153654321, 7] | same |
| ntz-target/update-case-where | 2 | [1767323045123456, 7] | [1767323045123456, 7] | [1767323045123456, 7] | same |
| ntz-target/update-case-where | 3 | [None, None] | [None, None] | [None, None] | same |

Decision: continue. 0 rows differ from Spark where main equalled Spark; 0 named-hole rows differ where main also differed.
