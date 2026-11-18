# Headline counts (written by analyze.py)

Scope: cells where the created and read settings differ; `avg` from #1355's package, every other body from main;
the twin-column bodies `cs_twin` and `cs_lower` are left out (Spark refuses the relation itself under
`caseSensitive=false` at every door, a held frame included). A view gap is a wrong cell whose two direct
queries (the body under the created value and under the read value) agree with Spark (both raising counts as
agreeing, whatever the error text).

| family | phase | door | cells | EQUAL or same raise | view gap, silent | view gap, loud | direct gap |
|---|---|---|---|---|---|---|---|
| ansi | nest | nest-df-df | 4 | 4 | 0 | 0 | 0 |
| ansi | nest | nest-df-sql | 4 | 3 | 0 | 1 | 0 |
| ansi | nest | nest-sql-df | 4 | 3 | 1 | 0 | 0 |
| ansi | nest | nest-sql-sql | 4 | 2 | 1 | 1 | 0 |
| ansi | read | sql | 136 | 52 | 24 | 36 | 24 |
| ansi | read | df | 136 | 92 | 12 | 8 | 24 |
| ansi | read | held | 34 | 28 | 0 | 0 | 6 |
| ansi | read | tsql | 34 | 18 | 4 | 6 | 6 |
| ansi | read | tdf | 34 | 28 | 0 | 0 | 6 |
| ansi | read | vss | 34 | 13 | 6 | 9 | 6 |
| ansi | read | vsd | 34 | 23 | 3 | 2 | 6 |
| ansi | read | vds | 34 | 13 | 6 | 9 | 6 |
| ansi | read | ren | 34 | 13 | 6 | 9 | 6 |
| ansi | read | heldf | 34 | 28 | 0 | 0 | 6 |
| ansi | read | tsqlf | 34 | 18 | 4 | 6 | 6 |
| ansi | read | tdff | 34 | 28 | 0 | 0 | 6 |
| ansi | read | dff | 34 | 23 | 3 | 2 | 6 |
| ansi | read | vdsf | 34 | 13 | 6 | 9 | 6 |
| ansi | read | cat | 32 | 24 | 0 | 0 | 8 |
| ansi | reader | sql | 6 | 6 | 0 | 0 | 0 |
| ansi | reader | df | 6 | 6 | 0 | 0 | 0 |
| ansi | reader | direct | 6 | 6 | 0 | 0 | 0 |
| ansi | write | w-ctas-df | 8 | 4 | 2 | 1 | 1 |
| ansi | write | w-ctas-sql | 8 | 1 | 3 | 3 | 1 |
| ansi | write | w-insert-sql | 8 | 1 | 3 | 3 | 1 |
| ansi | write | w-parquet-held | 8 | 4 | 2 | 1 | 1 |
| ansi | write | w-writeto-held | 8 | 4 | 2 | 1 | 1 |
| ansi | write | w-writeto-tsql | 8 | 1 | 3 | 3 | 1 |
| cs | read | sql | 2 | 1 | 0 | 1 | 0 |
| cs | read | df | 2 | 2 | 0 | 0 | 0 |
| cs | read | held | 2 | 2 | 0 | 0 | 0 |
| cs | read | tsql | 2 | 1 | 0 | 1 | 0 |
| cs | read | heldf | 2 | 2 | 0 | 0 | 0 |
| cs | read | tsqlf | 2 | 1 | 0 | 1 | 0 |
| cs | read | dff | 2 | 2 | 0 | 0 | 0 |
| dec | read | sql | 4 | 2 | 0 | 2 | 0 |
| dec | read | df | 4 | 4 | 0 | 0 | 0 |
| dec | read | held | 4 | 4 | 0 | 0 | 0 |
| dec | read | tsql | 4 | 2 | 0 | 2 | 0 |
| dec | read | heldf | 4 | 4 | 0 | 0 | 0 |
| dec | read | tsqlf | 4 | 2 | 0 | 2 | 0 |
| dec | read | dff | 4 | 4 | 0 | 0 | 0 |
| ns | read | sql | 2 | 2 | 0 | 0 | 0 |
| ns | read | df | 2 | 2 | 0 | 0 | 0 |
| ns | read | held | 2 | 2 | 0 | 0 | 0 |
| ns | read | tsql | 2 | 2 | 0 | 0 | 0 |
| ns | read | heldf | 2 | 2 | 0 | 0 | 0 |
| ns | read | tsqlf | 2 | 2 | 0 | 0 | 0 |
| ns | read | dff | 2 | 2 | 0 | 0 | 0 |
| tpp | read | sql | 4 | 2 | 0 | 0 | 2 |
| tpp | read | df | 4 | 2 | 0 | 0 | 2 |
| tpp | read | held | 4 | 2 | 0 | 0 | 2 |
| tpp | read | tsql | 4 | 2 | 0 | 0 | 2 |
| tpp | read | heldf | 4 | 2 | 0 | 0 | 2 |
| tpp | read | tsqlf | 4 | 2 | 0 | 0 | 2 |
| tpp | read | dff | 4 | 2 | 0 | 0 | 2 |
| tst | read | sql | 4 | 2 | 0 | 0 | 2 |
| tst | read | df | 4 | 2 | 0 | 0 | 2 |
| tst | read | held | 4 | 2 | 0 | 0 | 2 |
| tst | read | tsql | 4 | 2 | 0 | 0 | 2 |
| tst | read | heldf | 4 | 2 | 0 | 0 | 2 |
| tst | read | tsqlf | 4 | 2 | 0 | 0 | 2 |
| tst | read | dff | 4 | 2 | 0 | 0 | 2 |
| tz | nest | nest-df-df | 4 | 2 | 2 | 0 | 0 |
| tz | nest | nest-df-sql | 4 | 2 | 2 | 0 | 0 |
| tz | nest | nest-sql-df | 4 | 2 | 2 | 0 | 0 |
| tz | nest | nest-sql-sql | 4 | 2 | 2 | 0 | 0 |
| tz | read | sql | 80 | 8 | 72 | 0 | 0 |
| tz | read | df | 80 | 16 | 64 | 0 | 0 |
| tz | read | held | 20 | 20 | 0 | 0 | 0 |
| tz | read | tsql | 20 | 18 | 2 | 0 | 0 |
| tz | read | tdf | 20 | 20 | 0 | 0 | 0 |
| tz | read | vss | 20 | 2 | 18 | 0 | 0 |
| tz | read | vsd | 20 | 4 | 16 | 0 | 0 |
| tz | read | vds | 20 | 2 | 18 | 0 | 0 |
| tz | read | ren | 20 | 2 | 18 | 0 | 0 |
| tz | read | heldf | 20 | 18 | 2 | 0 | 0 |
| tz | read | tsqlf | 20 | 16 | 4 | 0 | 0 |
| tz | read | tdff | 20 | 18 | 2 | 0 | 0 |
| tz | read | dff | 20 | 4 | 16 | 0 | 0 |
| tz | read | vdsf | 20 | 2 | 18 | 0 | 0 |
| tz | read | cat | 32 | 32 | 0 | 0 | 0 |
| tz | reader | sql | 4 | 0 | 4 | 0 | 0 |
| tz | reader | df | 4 | 0 | 4 | 0 | 0 |
| tz | reader | direct | 4 | 4 | 0 | 0 | 0 |
| tz | write | w-ctas-df | 8 | 2 | 6 | 0 | 0 |
| tz | write | w-ctas-sql | 8 | 0 | 8 | 0 | 0 |
| tz | write | w-insert-sql | 8 | 0 | 8 | 0 | 0 |
| tz | write | w-parquet-held | 8 | 2 | 6 | 0 | 0 |
| tz | write | w-writeto-held | 8 | 8 | 0 | 0 | 0 |
| tz | write | w-writeto-tsql | 8 | 0 | 8 | 0 | 0 |
| **all** | | | **1414** | **740** | **393** | **119** | **162** |

