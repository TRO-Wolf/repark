# Summary (written by analyze.py)

Cells paired: 3616; measured reads, writes and directs: 2609.

## Verdicts by build, family and phase

| build | family | phase | cells | EQUAL | BOTH_RAISE | BOTH_RAISE_OTHER | wrong | wrong, direct agrees |
|---|---|---|---|---|---|---|---|---|
| head-1355 | ansi | direct | 2 | 1 | 1 | 0 | 0 | 0 |
| head-1355 | ansi | read | 70 | 26 | 18 | 2 | 24 | 24 |
| head-1355 | ansi | write | 12 | 3 | 0 | 0 | 9 | 9 |
| main | ansi | direct | 34 | 14 | 7 | 6 | 7 | 0 |
| main | ansi | nest | 16 | 6 | 3 | 3 | 4 | 4 |
| main | ansi | read | 1254 | 404 | 80 | 370 | 400 | 146 |
| main | ansi | reader | 22 | 13 | 9 | 0 | 0 | 0 |
| main | ansi | write | 48 | 3 | 3 | 12 | 30 | 18 |
| main | cs | direct | 6 | 3 | 3 | 0 | 0 | 0 |
| main | cs | read | 66 | 18 | 4 | 33 | 11 | 11 |
| main | dec | direct | 4 | 4 | 0 | 0 | 0 | 0 |
| main | dec | read | 44 | 38 | 0 | 0 | 6 | 6 |
| main | ns | direct | 2 | 2 | 0 | 0 | 0 | 0 |
| main | ns | read | 22 | 22 | 0 | 0 | 0 | 0 |
| main | tpp | direct | 4 | 1 | 1 | 0 | 2 | 0 |
| main | tpp | read | 44 | 11 | 11 | 0 | 22 | 0 |
| main | tst | direct | 4 | 2 | 0 | 0 | 2 | 0 |
| main | tst | read | 44 | 22 | 0 | 0 | 22 | 0 |
| main | tz | direct | 20 | 20 | 0 | 0 | 0 | 0 |
| main | tz | nest | 24 | 16 | 0 | 0 | 8 | 8 |
| main | tz | now | 8 | 8 | 0 | 0 | 0 | 0 |
| main | tz | read | 764 | 494 | 0 | 20 | 250 | 250 |
| main | tz | reader | 16 | 8 | 0 | 0 | 8 | 8 |
| main | tz | write | 48 | 12 | 0 | 0 | 36 | 36 |
| main | unset | unset | 31 | 8 | 11 | 7 | 5 | 5 |

## Wrong reads by door and direction (main; avg from #1355's package), direct agrees

| family | door | created → read | cells | wrong | Spark follows (creation/reader/same) | RePark follows (creation/reader/same/neither) |
|---|---|---|---|---|---|---|
| ansi | cat | off → on | 16 | 0 | 0/16/0 | 0/8/8/0 |
| ansi | cat | on → off | 16 | 0 | 0/12/0 | 0/8/8/0 |
| ansi | df | off → on | 68 | 8 | 52/0/8 | 24/8/32/4 |
| ansi | df | on → off | 68 | 12 | 56/0/8 | 16/12/32/8 |
| ansi | dff | off → on | 17 | 2 | 13/0/2 | 6/2/8/1 |
| ansi | dff | on → off | 17 | 3 | 14/0/2 | 4/3/8/2 |
| ansi | held | off → on | 17 | 0 | 13/0/2 | 8/0/8/1 |
| ansi | held | on → off | 17 | 0 | 14/0/2 | 7/0/8/2 |
| ansi | heldf | off → on | 17 | 0 | 13/0/2 | 8/0/8/1 |
| ansi | heldf | on → off | 17 | 0 | 14/0/2 | 7/0/8/2 |
| ansi | ren | off → on | 17 | 8 | 13/0/2 | 0/8/8/1 |
| ansi | ren | on → off | 17 | 7 | 14/0/2 | 0/7/8/2 |
| ansi | sql | off → on | 68 | 32 | 52/0/8 | 0/32/32/4 |
| ansi | sql | on → off | 68 | 28 | 56/0/8 | 0/28/32/8 |
| ansi | tdf | off → on | 17 | 0 | 13/0/2 | 8/0/8/1 |
| ansi | tdf | on → off | 17 | 0 | 14/0/2 | 7/0/8/2 |
| ansi | tdff | off → on | 17 | 0 | 13/0/2 | 8/0/8/1 |
| ansi | tdff | on → off | 17 | 0 | 14/0/2 | 7/0/8/2 |
| ansi | tsql | off → on | 17 | 5 | 13/0/2 | 3/5/8/1 |
| ansi | tsql | on → off | 17 | 5 | 14/0/2 | 2/5/8/2 |
| ansi | tsqlf | off → on | 17 | 5 | 13/0/2 | 3/5/8/1 |
| ansi | tsqlf | on → off | 17 | 5 | 14/0/2 | 2/5/8/2 |
| ansi | vds | off → on | 17 | 8 | 13/0/2 | 0/8/8/1 |
| ansi | vds | on → off | 17 | 7 | 14/0/2 | 0/7/8/2 |
| ansi | vdsf | off → on | 17 | 8 | 13/0/2 | 0/8/8/1 |
| ansi | vdsf | on → off | 17 | 7 | 14/0/2 | 0/7/8/2 |
| ansi | vsd | off → on | 17 | 2 | 13/0/2 | 6/2/8/1 |
| ansi | vsd | on → off | 17 | 3 | 14/0/2 | 4/3/8/2 |
| ansi | vss | off → on | 17 | 8 | 13/0/2 | 0/8/8/1 |
| ansi | vss | on → off | 17 | 7 | 14/0/2 | 0/7/8/2 |
| cs | df | ci → cs | 3 | 0 | 1/0/0 | 1/0/0/2 |
| cs | df | cs → ci | 3 | 2 | 0/2/0 | 2/0/0/1 |
| cs | dff | ci → cs | 3 | 0 | 1/0/0 | 1/0/0/2 |
| cs | dff | cs → ci | 3 | 2 | 0/2/0 | 2/0/0/1 |
| cs | held | ci → cs | 3 | 0 | 1/0/0 | 1/0/0/2 |
| cs | held | cs → ci | 3 | 0 | 2/0/0 | 2/0/0/1 |
| cs | heldf | ci → cs | 3 | 0 | 1/0/0 | 1/0/0/2 |
| cs | heldf | cs → ci | 3 | 2 | 0/2/0 | 2/0/0/1 |
| cs | sql | ci → cs | 3 | 1 | 1/0/0 | 0/1/0/2 |
| cs | sql | cs → ci | 3 | 0 | 0/2/0 | 0/2/0/1 |
| cs | tsql | ci → cs | 3 | 1 | 1/0/0 | 0/1/0/2 |
| cs | tsql | cs → ci | 3 | 2 | 2/0/0 | 0/2/0/1 |
| cs | tsqlf | ci → cs | 3 | 1 | 1/0/0 | 0/1/0/2 |
| cs | tsqlf | cs → ci | 3 | 0 | 0/2/0 | 0/2/0/1 |
| dec | df | dflt → leg | 2 | 0 | 1/0/1 | 1/0/1/0 |
| dec | df | leg → dflt | 2 | 0 | 1/0/1 | 1/0/1/0 |
| dec | dff | dflt → leg | 2 | 0 | 1/0/1 | 1/0/1/0 |
| dec | dff | leg → dflt | 2 | 0 | 1/0/1 | 1/0/1/0 |
| dec | held | dflt → leg | 2 | 0 | 1/0/1 | 1/0/1/0 |
| dec | held | leg → dflt | 2 | 0 | 1/0/1 | 1/0/1/0 |
| dec | heldf | dflt → leg | 2 | 0 | 1/0/1 | 1/0/1/0 |
| dec | heldf | leg → dflt | 2 | 0 | 1/0/1 | 1/0/1/0 |
| dec | sql | dflt → leg | 2 | 1 | 1/0/1 | 0/0/1/1 |
| dec | sql | leg → dflt | 2 | 1 | 1/0/1 | 0/0/1/1 |
| dec | tsql | dflt → leg | 2 | 1 | 1/0/1 | 0/0/1/1 |
| dec | tsql | leg → dflt | 2 | 1 | 1/0/1 | 0/0/1/1 |
| dec | tsqlf | dflt → leg | 2 | 1 | 1/0/1 | 0/0/1/1 |
| dec | tsqlf | leg → dflt | 2 | 1 | 1/0/1 | 0/0/1/1 |
| ns | df | n1 → n2 | 1 | 0 | 1/0/0 | 1/0/0/0 |
| ns | df | n2 → n1 | 1 | 0 | 1/0/0 | 1/0/0/0 |
| ns | dff | n1 → n2 | 1 | 0 | 1/0/0 | 1/0/0/0 |
| ns | dff | n2 → n1 | 1 | 0 | 1/0/0 | 1/0/0/0 |
| ns | held | n1 → n2 | 1 | 0 | 1/0/0 | 1/0/0/0 |
| ns | held | n2 → n1 | 1 | 0 | 1/0/0 | 1/0/0/0 |
| ns | heldf | n1 → n2 | 1 | 0 | 1/0/0 | 1/0/0/0 |
| ns | heldf | n2 → n1 | 1 | 0 | 1/0/0 | 1/0/0/0 |
| ns | sql | n1 → n2 | 1 | 0 | 1/0/0 | 1/0/0/0 |
| ns | sql | n2 → n1 | 1 | 0 | 1/0/0 | 1/0/0/0 |
| ns | tsql | n1 → n2 | 1 | 0 | 1/0/0 | 1/0/0/0 |
| ns | tsql | n2 → n1 | 1 | 0 | 1/0/0 | 1/0/0/0 |
| ns | tsqlf | n1 → n2 | 1 | 0 | 1/0/0 | 1/0/0/0 |
| ns | tsqlf | n2 → n1 | 1 | 0 | 1/0/0 | 1/0/0/0 |
| tpp | df | cor → leg | 2 | 0 | 2/0/0 | 0/0/2/0 |
| tpp | df | leg → cor | 2 | 0 | 2/0/0 | 0/0/2/0 |
| tpp | dff | cor → leg | 2 | 0 | 0/2/0 | 0/0/2/0 |
| tpp | dff | leg → cor | 2 | 0 | 0/2/0 | 0/0/2/0 |
| tpp | held | cor → leg | 2 | 0 | 2/0/0 | 0/0/2/0 |
| tpp | held | leg → cor | 2 | 0 | 2/0/0 | 0/0/2/0 |
| tpp | heldf | cor → leg | 2 | 0 | 0/2/0 | 0/0/2/0 |
| tpp | heldf | leg → cor | 2 | 0 | 0/2/0 | 0/0/2/0 |
| tpp | sql | cor → leg | 2 | 0 | 0/2/0 | 0/0/2/0 |
| tpp | sql | leg → cor | 2 | 0 | 0/2/0 | 0/0/2/0 |
| tpp | tsql | cor → leg | 2 | 0 | 2/0/0 | 0/0/2/0 |
| tpp | tsql | leg → cor | 2 | 0 | 2/0/0 | 0/0/2/0 |
| tpp | tsqlf | cor → leg | 2 | 0 | 0/2/0 | 0/0/2/0 |
| tpp | tsqlf | leg → cor | 2 | 0 | 0/2/0 | 0/0/2/0 |
| tst | df | ltz → ntz | 2 | 0 | 2/0/0 | 0/0/2/0 |
| tst | df | ntz → ltz | 2 | 0 | 2/0/0 | 0/0/2/0 |
| tst | dff | ltz → ntz | 2 | 0 | 2/0/0 | 0/0/2/0 |
| tst | dff | ntz → ltz | 2 | 0 | 2/0/0 | 0/0/2/0 |
| tst | held | ltz → ntz | 2 | 0 | 2/0/0 | 0/0/2/0 |
| tst | held | ntz → ltz | 2 | 0 | 2/0/0 | 0/0/2/0 |
| tst | heldf | ltz → ntz | 2 | 0 | 2/0/0 | 0/0/2/0 |
| tst | heldf | ntz → ltz | 2 | 0 | 2/0/0 | 0/0/2/0 |
| tst | sql | ltz → ntz | 2 | 0 | 2/0/0 | 0/0/2/0 |
| tst | sql | ntz → ltz | 2 | 0 | 2/0/0 | 0/0/2/0 |
| tst | tsql | ltz → ntz | 2 | 0 | 2/0/0 | 0/0/2/0 |
| tst | tsql | ntz → ltz | 2 | 0 | 2/0/0 | 0/0/2/0 |
| tst | tsqlf | ltz → ntz | 2 | 0 | 2/0/0 | 0/0/2/0 |
| tst | tsqlf | ntz → ltz | 2 | 0 | 2/0/0 | 0/0/2/0 |
| tz | cat | la → utc | 16 | 0 | 0/16/0 | 0/16/0/0 |
| tz | cat | utc → la | 16 | 0 | 0/16/0 | 0/16/0/0 |
| tz | df | la → utc | 40 | 32 | 36/4/0 | 4/36/0/0 |
| tz | df | utc → la | 40 | 32 | 36/4/0 | 4/36/0/0 |
| tz | dff | la → utc | 10 | 8 | 9/1/0 | 1/9/0/0 |
| tz | dff | utc → la | 10 | 8 | 9/1/0 | 1/9/0/0 |
| tz | held | la → utc | 10 | 0 | 10/0/0 | 10/0/0/0 |
| tz | held | utc → la | 10 | 0 | 10/0/0 | 10/0/0/0 |
| tz | heldf | la → utc | 10 | 1 | 9/1/0 | 10/0/0/0 |
| tz | heldf | utc → la | 10 | 1 | 9/1/0 | 10/0/0/0 |
| tz | ren | la → utc | 10 | 9 | 9/1/0 | 0/10/0/0 |
| tz | ren | utc → la | 10 | 9 | 9/1/0 | 0/10/0/0 |
| tz | sql | la → utc | 40 | 36 | 36/4/0 | 0/40/0/0 |
| tz | sql | utc → la | 40 | 36 | 36/4/0 | 0/40/0/0 |
| tz | tdf | la → utc | 10 | 0 | 10/0/0 | 10/0/0/0 |
| tz | tdf | utc → la | 10 | 0 | 10/0/0 | 10/0/0/0 |
| tz | tdff | la → utc | 10 | 1 | 9/1/0 | 10/0/0/0 |
| tz | tdff | utc → la | 10 | 1 | 9/1/0 | 10/0/0/0 |
| tz | tsql | la → utc | 10 | 1 | 10/0/0 | 9/1/0/0 |
| tz | tsql | utc → la | 10 | 1 | 10/0/0 | 9/1/0/0 |
| tz | tsqlf | la → utc | 10 | 2 | 9/1/0 | 9/1/0/0 |
| tz | tsqlf | utc → la | 10 | 2 | 9/1/0 | 9/1/0/0 |
| tz | vds | la → utc | 10 | 9 | 9/1/0 | 0/10/0/0 |
| tz | vds | utc → la | 10 | 9 | 9/1/0 | 0/10/0/0 |
| tz | vdsf | la → utc | 10 | 9 | 9/1/0 | 0/10/0/0 |
| tz | vdsf | utc → la | 10 | 9 | 9/1/0 | 0/10/0/0 |
| tz | vsd | la → utc | 10 | 8 | 9/1/0 | 1/9/0/0 |
| tz | vsd | utc → la | 10 | 8 | 9/1/0 | 1/9/0/0 |
| tz | vss | la → utc | 10 | 9 | 9/1/0 | 0/10/0/0 |
| tz | vss | utc → la | 10 | 9 | 9/1/0 | 0/10/0/0 |

## Follows grid (plain position): body by door; letters Spark/RePark

C creation, R reader, = not discriminating (the two directs agree), N neither, x missing. Two letters per created → read direction.

**ansi**

| body | sql | df | held | tsql | tdf | vss | vsd | vds | ren | heldf | tsqlf | tdff | dff | vdsf | cat |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| add_ovf | C/R! C/R! | C/R! C/R! | C/C C/C | C/C C/C | C/C C/C | C/R! C/R! | C/R! C/R! | C/R! C/R! | C/R! C/R! | C/C C/C | C/C C/C | C/C C/C | C/R! C/R! | C/R! C/R! | R/R R/R |
| arr_elt | C/= C/=! | C/= C/=! | C/= C/=! | C/= C/=! | C/= C/=! | C/= C/=! | C/= C/=! | C/= C/=! | C/= C/=! | C/= C/=! | C/= C/=! | C/= C/=! | C/= C/=! | C/= C/=! | - |
| arr_sub | C/= C/=! | C/= C/=! | C/= C/=! | C/= C/=! | C/= C/=! | C/= C/=! | C/= C/=! | C/= C/=! | C/= C/=! | C/= C/=! | C/= C/=! | C/= C/=! | C/= C/=! | C/= C/=! | - |
| avg | C/R! C/R! | C/C C/R! | C/C C/C | C/C C/R! | C/C C/C | C/R! C/R! | C/C C/R! | C/R! C/R! | C/R! C/R! | C/C C/C | C/C C/R! | C/C C/C | C/C C/R! | C/R! C/R! | - |
| bool_dec | C/R! C/R! | C/C C/C | C/C C/C | C/R! C/R! | C/C C/C | C/R! C/R! | C/C C/C | C/R! C/R! | C/R! C/R! | C/C C/C | C/R! C/R! | C/C C/C | C/C C/C | C/R! C/R! | - |
| cast_date | C/=! C/= | C/=! C/= | C/=! C/= | C/=! C/= | C/=! C/= | C/=! C/= | C/=! C/= | C/=! C/= | C/=! C/= | C/=! C/= | C/=! C/= | C/=! C/= | C/=! C/= | C/=! C/= | R/= R/=! |
| cast_dbl | C/R! C/R! | C/R! C/R! | C/C C/C | C/C C/C | C/C C/C | C/R! C/R! | C/R! C/R! | C/R! C/R! | C/R! C/R! | C/C C/C | C/C C/C | C/C C/C | C/R! C/R! | C/R! C/R! | - |
| cast_int | C/=! C/= | C/=! C/= | C/=! C/= | C/=! C/= | C/=! C/= | C/=! C/= | C/=! C/= | C/=! C/= | C/=! C/= | C/=! C/= | C/=! C/= | C/=! C/= | C/=! C/= | C/=! C/= | - |
| cast_ovf | C/=! C/= | C/=! C/= | C/=! C/= | C/=! C/= | C/=! C/= | C/=! C/= | C/=! C/= | C/=! C/= | C/=! C/= | C/=! C/= | C/=! C/= | C/=! C/= | C/=! C/= | C/=! C/= | R/= N/=! |
| dbl_lit | C/R! C/N | C/C C/N | C/C C/N | C/R! C/N | C/C C/N | C/R! C/N | C/C C/N | C/R! C/N | C/R! C/N | C/C C/N | C/R! C/N | C/C C/N | C/C C/N | C/R! C/N | - |
| div | C/R! C/R! | C/C C/C | C/C C/C | C/R! C/R! | C/C C/C | C/R! C/R! | C/C C/C | C/R! C/R! | C/R! C/R! | C/C C/C | C/R! C/R! | C/C C/C | C/C C/C | C/R! C/R! | R/R R/R |
| int_bin | C/R! N/N | C/C N/N | C/C N/N | C/R! N/N | C/C N/N | C/R! N/N | C/C N/N | C/R! N/N | C/R! N/N | C/C N/N | C/R! N/N | C/C N/N | C/C N/N | C/R! N/N | - |
| map_elt | =/= =/= | =/= =/= | =/= =/= | =/= =/= | =/= =/= | =/= =/= | =/= =/= | =/= =/= | =/= =/= | =/= =/= | =/= =/= | =/= =/= | =/= =/= | =/= =/= | - |
| map_struct | N/N C/R! | N/N C/C | N/N C/C | N/N C/R! | N/N C/C | N/N C/R! | N/N C/C | N/N C/R! | N/N C/R! | N/N C/C | N/N C/R! | N/N C/C | N/N C/C | N/N C/R! | - |
| map_sub | =/= =/= | =/= =/= | =/= =/= | =/= =/= | =/= =/= | =/= =/= | =/= =/= | =/= =/= | =/= =/= | =/= =/= | =/= =/= | =/= =/= | =/= =/= | =/= =/= | - |
| mod | C/R! C/R! | C/C C/C | C/C C/C | C/R! C/R! | C/C C/C | C/R! C/R! | C/C C/C | C/R! C/R! | C/R! C/R! | C/C C/C | C/R! C/R! | C/C C/C | C/C C/C | C/R! C/R! | - |
| seq | N/= C/=! | N/= C/=! | N/= C/=! | N/= C/=! | N/= C/=! | N/= C/=! | N/= C/=! | N/= C/=! | N/= C/=! | N/= C/=! | N/= C/=! | N/= C/=! | N/= C/=! | N/= C/=! | - |

**tz**

| body | sql | df | held | tsql | tdf | vss | vsd | vds | ren | heldf | tsqlf | tdff | dff | vdsf | cat |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| curtz | R/R R/R | R/R R/R | C/C C/C | C/C C/C | C/C C/C | R/R R/R | R/R R/R | R/R R/R | R/R R/R | R/C! R/C! | R/C! R/C! | R/C! R/C! | R/R R/R | R/R R/R | R/R R/R |
| date | C/R! C/R! | C/R! C/R! | C/C C/C | C/C C/C | C/C C/C | C/R! C/R! | C/R! C/R! | C/R! C/R! | C/R! C/R! | C/C C/C | C/C C/C | C/C C/C | C/R! C/R! | C/R! C/R! | - |
| date_ts | C/R! C/R! | C/R! C/R! | C/C C/C | C/C C/C | C/C C/C | C/R! C/R! | C/R! C/R! | C/R! C/R! | C/R! C/R! | C/C C/C | C/C C/C | C/C C/C | C/R! C/R! | C/R! C/R! | - |
| fmt | C/R! C/R! | C/R! C/R! | C/C C/C | C/C C/C | C/C C/C | C/R! C/R! | C/R! C/R! | C/R! C/R! | C/R! C/R! | C/C C/C | C/C C/C | C/C C/C | C/R! C/R! | C/R! C/R! | - |
| fromunix | C/R! C/R! | C/R! C/R! | C/C C/C | C/C C/C | C/C C/C | C/R! C/R! | C/R! C/R! | C/R! C/R! | C/R! C/R! | C/C C/C | C/C C/C | C/C C/C | C/R! C/R! | C/R! C/R! | - |
| hour | C/R! C/R! | C/R! C/R! | C/C C/C | C/C C/C | C/C C/C | C/R! C/R! | C/R! C/R! | C/R! C/R! | C/R! C/R! | C/C C/C | C/C C/C | C/C C/C | C/R! C/R! | C/R! C/R! | R/R R/R |
| lit | C/R! C/R! | C/C C/C | C/C C/C | C/R! C/R! | C/C C/C | C/R! C/R! | C/C C/C | C/R! C/R! | C/R! C/R! | C/C C/C | C/R! C/R! | C/C C/C | C/C C/C | C/R! C/R! | R/R R/R |
| parse | C/R! C/R! | C/R! C/R! | C/C C/C | C/C C/C | C/C C/C | C/R! C/R! | C/R! C/R! | C/R! C/R! | C/R! C/R! | C/C C/C | C/C C/C | C/C C/C | C/R! C/R! | C/R! C/R! | - |
| str | C/R! C/R! | C/R! C/R! | C/C C/C | C/C C/C | C/C C/C | C/R! C/R! | C/R! C/R! | C/R! C/R! | C/R! C/R! | C/C C/C | C/C C/C | C/C C/C | C/R! C/R! | C/R! C/R! | R/R R/R |
| trunc | C/R! C/R! | C/R! C/R! | C/C C/C | C/C C/C | C/C C/C | C/R! C/R! | C/R! C/R! | C/R! C/R! | C/R! C/R! | C/C C/C | C/C C/C | C/C C/C | C/R! C/R! | C/R! C/R! | - |

**cs**

| body | sql | df | held | tsql | heldf | tsqlf | dff |
|---|---|---|---|---|---|---|---|
| ci_upper | C/R! N/N | C/C N/N | C/C N/N | C/R! N/N | C/C N/N | C/R! N/N | C/C N/N |
| cs_lower | N/N R/R | N/N R/C! | N/N C/C | N/N C/R! | N/N R/C! | N/N R/R | N/N R/C! |
| cs_twin | N/N R/R | N/N R/C! | N/N C/C | N/N C/R! | N/N R/C! | N/N R/R | N/N R/C! |

**tpp**

| body | sql | df | held | tsql | heldf | tsqlf | dff |
|---|---|---|---|---|---|---|---|
| tpp_parse | R/=! R/= | C/= C/=! | C/= C/=! | C/= C/=! | R/=! R/= | R/=! R/= | R/=! R/= |
| tpp_try | R/=! R/= | C/= C/=! | C/= C/=! | C/= C/=! | R/=! R/= | R/=! R/= | R/=! R/= |

**dec**

| body | sql | df | held | tsql | heldf | tsqlf | dff |
|---|---|---|---|---|---|---|---|
| dec_ct | C/N! C/N! | C/C C/C | C/C C/C | C/N! C/N! | C/C C/C | C/N! C/N! | C/C C/C |
| dec_dbl | =/= =/= | =/= =/= | =/= =/= | =/= =/= | =/= =/= | =/= =/= | =/= =/= |

**tst**

| body | sql | df | held | tsql | heldf | tsqlf | dff |
|---|---|---|---|---|---|---|---|
| tst_cast | C/= C/=! | C/= C/=! | C/= C/=! | C/= C/=! | C/= C/=! | C/= C/=! | C/= C/=! |
| tst_lit | C/= C/=! | C/= C/=! | C/= C/=! | C/= C/=! | C/= C/=! | C/= C/=! | C/= C/=! |

**ns**

| body | sql | df | held | tsql | heldf | tsqlf | dff |
|---|---|---|---|---|---|---|---|
| ns_read | C/C C/C | C/C C/C | C/C C/C | C/C C/C | C/C C/C | C/C C/C | C/C C/C |

`!` marks a wrong cell (RePark differs from Spark).

## Every write cell

| build | cell | verdict | Spark | RePark |
|---|---|---|---|---|
| main | `write/ctas-sql/on/off/div` | REPARK_ANSWERS | RAISE DIVIDE_BY_ZERO | [["null"], ["double"]] |
| main | `write/ctas-df/on/off/div` | BOTH_RAISE | RAISE DIVIDE_BY_ZERO | RAISE DIVIDE_BY_ZERO |
| main | `write/insert-sql/on/off/div` | REPARK_ANSWERS | RAISE DIVIDE_BY_ZERO | [["null"], ["double"]] |
| main | `write/writeto-held/on/off/div` | BOTH_RAISE | RAISE DIVIDE_BY_ZERO | RAISE DIVIDE_BY_ZERO |
| main | `write/writeto-tsql/on/off/div` | REPARK_ANSWERS | RAISE DIVIDE_BY_ZERO | [["null"], ["double"]] |
| main | `write/parquet-held/on/off/div` | BOTH_RAISE | RAISE DIVIDE_BY_ZERO | RAISE DIVIDE_BY_ZERO |
| main | `write/ctas-sql/on/off/add_ovf` | REPARK_ANSWERS | RAISE ARITHMETIC_OVERFLOW | [["-2147483648"], ["int"]] |
| main | `write/ctas-df/on/off/add_ovf` | REPARK_ANSWERS | RAISE ARITHMETIC_OVERFLOW | [["-2147483648"], ["int"]] |
| main | `write/insert-sql/on/off/add_ovf` | REPARK_ANSWERS | RAISE ARITHMETIC_OVERFLOW | [["-2147483648"], ["int"]] |
| main | `write/writeto-held/on/off/add_ovf` | REPARK_ANSWERS | RAISE ARITHMETIC_OVERFLOW | [["-2147483648"], ["int"]] |
| main | `write/writeto-tsql/on/off/add_ovf` | REPARK_ANSWERS | RAISE ARITHMETIC_OVERFLOW | [["-2147483648"], ["int"]] |
| main | `write/parquet-held/on/off/add_ovf` | REPARK_ANSWERS | RAISE ARITHMETIC_OVERFLOW | [["-2147483648"], ["int"]] |
| main | `write/ctas-sql/on/off/cast_ovf` | BOTH_RAISE_OTHER | RAISE CAST_OVERFLOW | RAISE datafusion engine error: Arrow error: Cast error: Can't cast |
| main | `write/ctas-df/on/off/cast_ovf` | BOTH_RAISE_OTHER | RAISE CAST_OVERFLOW | RAISE datafusion engine error: Arrow error: Cast error: Can't cast |
| main | `write/insert-sql/on/off/cast_ovf` | BOTH_RAISE_OTHER | RAISE CAST_OVERFLOW | RAISE datafusion engine error: Arrow error: Cast error: Can't cast |
| main | `write/writeto-held/on/off/cast_ovf` | BOTH_RAISE_OTHER | RAISE CAST_OVERFLOW | RAISE datafusion engine error: Arrow error: Cast error: Can't cast |
| main | `write/writeto-tsql/on/off/cast_ovf` | BOTH_RAISE_OTHER | RAISE CAST_OVERFLOW | RAISE datafusion engine error: Arrow error: Cast error: Can't cast |
| main | `write/parquet-held/on/off/cast_ovf` | BOTH_RAISE_OTHER | RAISE CAST_OVERFLOW | RAISE datafusion engine error: Arrow error: Cast error: Can't cast |
| main | `write/ctas-sql/on/off/avg` | BOTH_RAISE_OTHER | RAISE ARITHMETIC_OVERFLOW | RAISE datafusion engine error: Execution error: Arithmetic Overflo |
| main | `write/ctas-df/on/off/avg` | BOTH_RAISE_OTHER | RAISE ARITHMETIC_OVERFLOW | RAISE datafusion engine error: Execution error: Arithmetic Overflo |
| main | `write/insert-sql/on/off/avg` | BOTH_RAISE_OTHER | RAISE ARITHMETIC_OVERFLOW | RAISE datafusion engine error: Execution error: Arithmetic Overflo |
| main | `write/writeto-held/on/off/avg` | REPARK_RAISES | [["null"], ["decimal(38,4)"]] | RAISE datafusion engine error: Execution error: Arithmetic Overflo |
| main | `write/writeto-tsql/on/off/avg` | BOTH_RAISE_OTHER | RAISE ARITHMETIC_OVERFLOW | RAISE datafusion engine error: Execution error: Arithmetic Overflo |
| main | `write/parquet-held/on/off/avg` | BOTH_RAISE_OTHER | RAISE ARITHMETIC_OVERFLOW | RAISE datafusion engine error: Execution error: Arithmetic Overflo |
| main | `write/ctas-sql/off/on/div` | REPARK_RAISES | [["null"], ["double"]] | RAISE DIVIDE_BY_ZERO |
| main | `write/ctas-df/off/on/div` | EQUAL | [["null"], ["double"]] | [["null"], ["double"]] |
| main | `write/insert-sql/off/on/div` | REPARK_RAISES | [["null"], ["double"]] | RAISE DIVIDE_BY_ZERO |
| main | `write/writeto-held/off/on/div` | EQUAL | [["null"], ["double"]] | [["null"], ["double"]] |
| main | `write/writeto-tsql/off/on/div` | REPARK_RAISES | [["null"], ["double"]] | RAISE DIVIDE_BY_ZERO |
| main | `write/parquet-held/off/on/div` | EQUAL | [["null"], ["double"]] | [["null"], ["double"]] |
| main | `write/ctas-sql/off/on/add_ovf` | REPARK_RAISES | [["-2147483648"], ["int"]] | RAISE ARITHMETIC_OVERFLOW |
| main | `write/ctas-df/off/on/add_ovf` | REPARK_RAISES | [["-2147483648"], ["int"]] | RAISE ARITHMETIC_OVERFLOW |
| main | `write/insert-sql/off/on/add_ovf` | REPARK_RAISES | [["-2147483648"], ["int"]] | RAISE ARITHMETIC_OVERFLOW |
| main | `write/writeto-held/off/on/add_ovf` | REPARK_RAISES | [["-2147483648"], ["int"]] | RAISE ARITHMETIC_OVERFLOW |
| main | `write/writeto-tsql/off/on/add_ovf` | REPARK_RAISES | [["-2147483648"], ["int"]] | RAISE ARITHMETIC_OVERFLOW |
| main | `write/parquet-held/off/on/add_ovf` | REPARK_RAISES | [["-2147483648"], ["int"]] | RAISE ARITHMETIC_OVERFLOW |
| main | `write/ctas-sql/off/on/cast_ovf` | REPARK_RAISES | [["-1"], ["int"]] | RAISE datafusion engine error: Arrow error: Cast error: Can't cast |
| main | `write/ctas-df/off/on/cast_ovf` | REPARK_RAISES | [["-1"], ["int"]] | RAISE datafusion engine error: Arrow error: Cast error: Can't cast |
| main | `write/insert-sql/off/on/cast_ovf` | REPARK_RAISES | [["-1"], ["int"]] | RAISE datafusion engine error: Arrow error: Cast error: Can't cast |
| main | `write/writeto-held/off/on/cast_ovf` | REPARK_RAISES | [["-1"], ["int"]] | RAISE datafusion engine error: Arrow error: Cast error: Can't cast |
| main | `write/writeto-tsql/off/on/cast_ovf` | REPARK_RAISES | [["-1"], ["int"]] | RAISE datafusion engine error: Arrow error: Cast error: Can't cast |
| main | `write/parquet-held/off/on/cast_ovf` | REPARK_RAISES | [["-1"], ["smallint"]] | RAISE datafusion engine error: Arrow error: Cast error: Can't cast |
| main | `write/ctas-sql/off/on/avg` | REPARK_RAISES | [["null"], ["decimal(38,4)"]] | RAISE datafusion engine error: Execution error: Arithmetic Overflo |
| main | `write/ctas-df/off/on/avg` | REPARK_RAISES | [["null"], ["decimal(38,4)"]] | RAISE datafusion engine error: Execution error: Arithmetic Overflo |
| main | `write/insert-sql/off/on/avg` | REPARK_RAISES | [["null"], ["decimal(38,4)"]] | RAISE datafusion engine error: Execution error: Arithmetic Overflo |
| main | `write/writeto-held/off/on/avg` | BOTH_RAISE_OTHER | RAISE ARITHMETIC_OVERFLOW | RAISE datafusion engine error: Execution error: Arithmetic Overflo |
| main | `write/writeto-tsql/off/on/avg` | REPARK_RAISES | [["null"], ["decimal(38,4)"]] | RAISE datafusion engine error: Execution error: Arithmetic Overflo |
| main | `write/parquet-held/off/on/avg` | REPARK_RAISES | [["null"], ["decimal(38,4)"]] | RAISE datafusion engine error: Execution error: Arithmetic Overflo |
| main | `write/ctas-sql/utc/la/hour` | VALUE | [["2"], ["int"]] | [["18"], ["int"]] |
| main | `write/ctas-df/utc/la/hour` | VALUE | [["2"], ["int"]] | [["18"], ["int"]] |
| main | `write/insert-sql/utc/la/hour` | VALUE | [["2"], ["int"]] | [["18"], ["int"]] |
| main | `write/writeto-held/utc/la/hour` | EQUAL | [["18"], ["int"]] | [["18"], ["int"]] |
| main | `write/writeto-tsql/utc/la/hour` | VALUE | [["2"], ["int"]] | [["18"], ["int"]] |
| main | `write/parquet-held/utc/la/hour` | VALUE | [["2"], ["int"]] | [["18"], ["int"]] |
| main | `write/ctas-sql/utc/la/str` | VALUE | [["2024-01-01 02:30:00"], ["string"]] | [["2023-12-31 18:30:00"], ["string"]] |
| main | `write/ctas-df/utc/la/str` | VALUE | [["2024-01-01 02:30:00"], ["string"]] | [["2023-12-31 18:30:00"], ["string"]] |
| main | `write/insert-sql/utc/la/str` | VALUE | [["2024-01-01 02:30:00"], ["string"]] | [["2023-12-31 18:30:00"], ["string"]] |
| main | `write/writeto-held/utc/la/str` | EQUAL | [["2023-12-31 18:30:00"], ["string"]] | [["2023-12-31 18:30:00"], ["string"]] |
| main | `write/writeto-tsql/utc/la/str` | VALUE | [["2024-01-01 02:30:00"], ["string"]] | [["2023-12-31 18:30:00"], ["string"]] |
| main | `write/parquet-held/utc/la/str` | VALUE | [["2024-01-01 02:30:00"], ["string"]] | [["2023-12-31 18:30:00"], ["string"]] |
| main | `write/ctas-sql/utc/la/date` | VALUE | [["2024-01-01"], ["date"]] | [["2023-12-31"], ["date"]] |
| main | `write/ctas-df/utc/la/date` | VALUE | [["2024-01-01"], ["date"]] | [["2023-12-31"], ["date"]] |
| main | `write/insert-sql/utc/la/date` | VALUE | [["2024-01-01"], ["date"]] | [["2023-12-31"], ["date"]] |
| main | `write/writeto-held/utc/la/date` | EQUAL | [["2023-12-31"], ["date"]] | [["2023-12-31"], ["date"]] |
| main | `write/writeto-tsql/utc/la/date` | VALUE | [["2024-01-01"], ["date"]] | [["2023-12-31"], ["date"]] |
| main | `write/parquet-held/utc/la/date` | VALUE | [["2024-01-01"], ["date"]] | [["2023-12-31"], ["date"]] |
| main | `write/ctas-sql/utc/la/lit` | VALUE | [["1704076200"], ["bigint"]] | [["1704105000"], ["bigint"]] |
| main | `write/ctas-df/utc/la/lit` | EQUAL | [["1704076200"], ["bigint"]] | [["1704076200"], ["bigint"]] |
| main | `write/insert-sql/utc/la/lit` | VALUE | [["1704076200"], ["bigint"]] | [["1704105000"], ["bigint"]] |
| main | `write/writeto-held/utc/la/lit` | EQUAL | [["1704076200"], ["bigint"]] | [["1704076200"], ["bigint"]] |
| main | `write/writeto-tsql/utc/la/lit` | VALUE | [["1704076200"], ["bigint"]] | [["1704105000"], ["bigint"]] |
| main | `write/parquet-held/utc/la/lit` | EQUAL | [["1704076200"], ["bigint"]] | [["1704076200"], ["bigint"]] |
| main | `write/ctas-sql/la/utc/hour` | VALUE | [["18"], ["int"]] | [["2"], ["int"]] |
| main | `write/ctas-df/la/utc/hour` | VALUE | [["18"], ["int"]] | [["2"], ["int"]] |
| main | `write/insert-sql/la/utc/hour` | VALUE | [["18"], ["int"]] | [["2"], ["int"]] |
| main | `write/writeto-held/la/utc/hour` | EQUAL | [["2"], ["int"]] | [["2"], ["int"]] |
| main | `write/writeto-tsql/la/utc/hour` | VALUE | [["18"], ["int"]] | [["2"], ["int"]] |
| main | `write/parquet-held/la/utc/hour` | VALUE | [["18"], ["int"]] | [["2"], ["int"]] |
| main | `write/ctas-sql/la/utc/str` | VALUE | [["2023-12-31 18:30:00"], ["string"]] | [["2024-01-01 02:30:00"], ["string"]] |
| main | `write/ctas-df/la/utc/str` | VALUE | [["2023-12-31 18:30:00"], ["string"]] | [["2024-01-01 02:30:00"], ["string"]] |
| main | `write/insert-sql/la/utc/str` | VALUE | [["2023-12-31 18:30:00"], ["string"]] | [["2024-01-01 02:30:00"], ["string"]] |
| main | `write/writeto-held/la/utc/str` | EQUAL | [["2024-01-01 02:30:00"], ["string"]] | [["2024-01-01 02:30:00"], ["string"]] |
| main | `write/writeto-tsql/la/utc/str` | VALUE | [["2023-12-31 18:30:00"], ["string"]] | [["2024-01-01 02:30:00"], ["string"]] |
| main | `write/parquet-held/la/utc/str` | VALUE | [["2023-12-31 18:30:00"], ["string"]] | [["2024-01-01 02:30:00"], ["string"]] |
| main | `write/ctas-sql/la/utc/date` | VALUE | [["2023-12-31"], ["date"]] | [["2024-01-01"], ["date"]] |
| main | `write/ctas-df/la/utc/date` | VALUE | [["2023-12-31"], ["date"]] | [["2024-01-01"], ["date"]] |
| main | `write/insert-sql/la/utc/date` | VALUE | [["2023-12-31"], ["date"]] | [["2024-01-01"], ["date"]] |
| main | `write/writeto-held/la/utc/date` | EQUAL | [["2024-01-01"], ["date"]] | [["2024-01-01"], ["date"]] |
| main | `write/writeto-tsql/la/utc/date` | VALUE | [["2023-12-31"], ["date"]] | [["2024-01-01"], ["date"]] |
| main | `write/parquet-held/la/utc/date` | VALUE | [["2023-12-31"], ["date"]] | [["2024-01-01"], ["date"]] |
| main | `write/ctas-sql/la/utc/lit` | VALUE | [["1704105000"], ["bigint"]] | [["1704076200"], ["bigint"]] |
| main | `write/ctas-df/la/utc/lit` | EQUAL | [["1704105000"], ["bigint"]] | [["1704105000"], ["bigint"]] |
| main | `write/insert-sql/la/utc/lit` | VALUE | [["1704105000"], ["bigint"]] | [["1704076200"], ["bigint"]] |
| main | `write/writeto-held/la/utc/lit` | EQUAL | [["1704105000"], ["bigint"]] | [["1704105000"], ["bigint"]] |
| main | `write/writeto-tsql/la/utc/lit` | VALUE | [["1704105000"], ["bigint"]] | [["1704076200"], ["bigint"]] |
| main | `write/parquet-held/la/utc/lit` | EQUAL | [["1704105000"], ["bigint"]] | [["1704105000"], ["bigint"]] |
| head-1355 | `write/ctas-sql/on/off/avg` | REPARK_ANSWERS | RAISE ARITHMETIC_OVERFLOW | [["null"], ["decimal(38,4)"]] |
| head-1355 | `write/ctas-df/on/off/avg` | REPARK_ANSWERS | RAISE ARITHMETIC_OVERFLOW | [["null"], ["decimal(38,4)"]] |
| head-1355 | `write/insert-sql/on/off/avg` | REPARK_ANSWERS | RAISE ARITHMETIC_OVERFLOW | [["null"], ["decimal(38,4)"]] |
| head-1355 | `write/writeto-held/on/off/avg` | EQUAL | [["null"], ["decimal(38,4)"]] | [["null"], ["decimal(38,4)"]] |
| head-1355 | `write/writeto-tsql/on/off/avg` | REPARK_ANSWERS | RAISE ARITHMETIC_OVERFLOW | [["null"], ["decimal(38,4)"]] |
| head-1355 | `write/parquet-held/on/off/avg` | REPARK_ANSWERS | RAISE ARITHMETIC_OVERFLOW | [["null"], ["decimal(38,4)"]] |
| head-1355 | `write/ctas-sql/off/on/avg` | REPARK_RAISES | [["null"], ["decimal(38,4)"]] | RAISE ARITHMETIC_OVERFLOW |
| head-1355 | `write/ctas-df/off/on/avg` | EQUAL | [["null"], ["decimal(38,4)"]] | [["null"], ["decimal(38,4)"]] |
| head-1355 | `write/insert-sql/off/on/avg` | REPARK_RAISES | [["null"], ["decimal(38,4)"]] | RAISE ARITHMETIC_OVERFLOW |
| head-1355 | `write/writeto-held/off/on/avg` | REPARK_ANSWERS | RAISE ARITHMETIC_OVERFLOW | [["null"], ["decimal(38,4)"]] |
| head-1355 | `write/writeto-tsql/off/on/avg` | REPARK_RAISES | [["null"], ["decimal(38,4)"]] | RAISE ARITHMETIC_OVERFLOW |
| head-1355 | `write/parquet-held/off/on/avg` | EQUAL | [["null"], ["decimal(38,4)"]] | [["null"], ["decimal(38,4)"]] |

## Every nest cell

| build | cell | verdict | Spark | RePark |
|---|---|---|---|---|
| main | `nest/create-inner/sqlsql` | EQUAL | [[], []] | [[], []] |
| main | `nest/create-outer/sqlsql` | EQUAL | [[], []] | [[], []] |
| main | `nest/read/sqlsql/off/m` | REPARK_ANSWERS | RAISE 'NoneType' object has no attribute 'contextType' | [["null"], ["double"]] |
| main | `nest/read/sqlsql/off/m2` | EQUAL | [["null"], ["double"]] | [["null"], ["double"]] |
| main | `nest/read/sqlsql/on/m` | BOTH_RAISE_OTHER | RAISE 'NoneType' object has no attribute 'contextType' | RAISE DIVIDE_BY_ZERO |
| main | `nest/read/sqlsql/on/m2` | REPARK_RAISES | [["null"], ["double"]] | RAISE DIVIDE_BY_ZERO |
| main | `nest/create-inner/dfsql` | EQUAL | OK | OK |
| main | `nest/create-outer/dfsql` | EQUAL | [[], []] | [[], []] |
| main | `nest/read/dfsql/off/m` | BOTH_RAISE_OTHER | RAISE 'NoneType' object has no attribute 'contextType' | RAISE DIVIDE_BY_ZERO |
| main | `nest/read/dfsql/off/m2` | EQUAL | [["null"], ["double"]] | [["null"], ["double"]] |
| main | `nest/read/dfsql/on/m` | BOTH_RAISE_OTHER | RAISE 'NoneType' object has no attribute 'contextType' | RAISE DIVIDE_BY_ZERO |
| main | `nest/read/dfsql/on/m2` | REPARK_RAISES | [["null"], ["double"]] | RAISE DIVIDE_BY_ZERO |
| main | `nest/create-inner/sqldf` | EQUAL | [[], []] | [[], []] |
| main | `nest/create-outer/sqldf` | EQUAL | OK | OK |
| main | `nest/read/sqldf/off/m` | REPARK_ANSWERS | RAISE DIVIDE_BY_ZERO | [["null"], ["double"]] |
| main | `nest/read/sqldf/off/m2` | EQUAL | [["null"], ["double"]] | [["null"], ["double"]] |
| main | `nest/read/sqldf/on/m` | BOTH_RAISE | RAISE DIVIDE_BY_ZERO | RAISE DIVIDE_BY_ZERO |
| main | `nest/read/sqldf/on/m2` | EQUAL | [["null"], ["double"]] | [["null"], ["double"]] |
| main | `nest/create-inner/dfdf` | EQUAL | OK | OK |
| main | `nest/create-outer/dfdf` | EQUAL | OK | OK |
| main | `nest/read/dfdf/off/m` | BOTH_RAISE | RAISE DIVIDE_BY_ZERO | RAISE DIVIDE_BY_ZERO |
| main | `nest/read/dfdf/off/m2` | EQUAL | [["null"], ["double"]] | [["null"], ["double"]] |
| main | `nest/read/dfdf/on/m` | BOTH_RAISE | RAISE DIVIDE_BY_ZERO | RAISE DIVIDE_BY_ZERO |
| main | `nest/read/dfdf/on/m2` | EQUAL | [["null"], ["double"]] | [["null"], ["double"]] |
| main | `nest/read/sqlsql/la/m` | VALUE | [["2"], ["int"]] | [["18"], ["int"]] |
| main | `nest/read/sqlsql/la/m2` | EQUAL | [["18"], ["int"]] | [["18"], ["int"]] |
| main | `nest/read/sqlsql/utc/m` | EQUAL | [["2"], ["int"]] | [["2"], ["int"]] |
| main | `nest/read/sqlsql/utc/m2` | VALUE | [["18"], ["int"]] | [["2"], ["int"]] |
| main | `nest/read/dfsql/la/m` | VALUE | [["2"], ["int"]] | [["18"], ["int"]] |
| main | `nest/read/dfsql/la/m2` | EQUAL | [["18"], ["int"]] | [["18"], ["int"]] |
| main | `nest/read/dfsql/utc/m` | EQUAL | [["2"], ["int"]] | [["2"], ["int"]] |
| main | `nest/read/dfsql/utc/m2` | VALUE | [["18"], ["int"]] | [["2"], ["int"]] |
| main | `nest/read/sqldf/la/m` | VALUE | [["2"], ["int"]] | [["18"], ["int"]] |
| main | `nest/read/sqldf/la/m2` | EQUAL | [["18"], ["int"]] | [["18"], ["int"]] |
| main | `nest/read/sqldf/utc/m` | EQUAL | [["2"], ["int"]] | [["2"], ["int"]] |
| main | `nest/read/sqldf/utc/m2` | VALUE | [["18"], ["int"]] | [["2"], ["int"]] |
| main | `nest/read/dfdf/la/m` | VALUE | [["2"], ["int"]] | [["18"], ["int"]] |
| main | `nest/read/dfdf/la/m2` | EQUAL | [["18"], ["int"]] | [["18"], ["int"]] |
| main | `nest/read/dfdf/utc/m` | EQUAL | [["2"], ["int"]] | [["2"], ["int"]] |
| main | `nest/read/dfdf/utc/m2` | VALUE | [["18"], ["int"]] | [["2"], ["int"]] |

## Every reader cell

| build | cell | verdict | Spark | RePark |
|---|---|---|---|---|
| main | `reader/create-sql/on` | EQUAL | [[], []] | [[], []] |
| main | `reader/create-df/on` | EQUAL | OK | OK |
| main | `reader/sql/on/off/filter` | EQUAL | [["0"], ["int"]] | [["0"], ["int"]] |
| main | `reader/df/on/off/filter` | EQUAL | [["0"], ["int"]] | [["0"], ["int"]] |
| main | `reader/direct/on/off/filter` | EQUAL | [["0"], ["int"]] | [["0"], ["int"]] |
| main | `reader/sql/on/off/proj_div` | EQUAL | [["null"], ["double"]] | [["null"], ["double"]] |
| main | `reader/df/on/off/proj_div` | EQUAL | [["null"], ["double"]] | [["null"], ["double"]] |
| main | `reader/direct/on/off/proj_div` | EQUAL | [["null"], ["double"]] | [["null"], ["double"]] |
| main | `reader/sql/on/off/proj_add` | EQUAL | [["-2147483648"], ["int"]] | [["-2147483648"], ["int"]] |
| main | `reader/df/on/off/proj_add` | EQUAL | [["-2147483648"], ["int"]] | [["-2147483648"], ["int"]] |
| main | `reader/direct/on/off/proj_add` | EQUAL | [["-2147483648"], ["int"]] | [["-2147483648"], ["int"]] |
| main | `reader/create-sql/off` | EQUAL | [[], []] | [[], []] |
| main | `reader/create-df/off` | EQUAL | OK | OK |
| main | `reader/sql/off/on/filter` | BOTH_RAISE | RAISE DIVIDE_BY_ZERO | RAISE DIVIDE_BY_ZERO |
| main | `reader/df/off/on/filter` | BOTH_RAISE | RAISE DIVIDE_BY_ZERO | RAISE DIVIDE_BY_ZERO |
| main | `reader/direct/off/on/filter` | BOTH_RAISE | RAISE DIVIDE_BY_ZERO | RAISE DIVIDE_BY_ZERO |
| main | `reader/sql/off/on/proj_div` | BOTH_RAISE | RAISE DIVIDE_BY_ZERO | RAISE DIVIDE_BY_ZERO |
| main | `reader/df/off/on/proj_div` | BOTH_RAISE | RAISE DIVIDE_BY_ZERO | RAISE DIVIDE_BY_ZERO |
| main | `reader/direct/off/on/proj_div` | BOTH_RAISE | RAISE DIVIDE_BY_ZERO | RAISE DIVIDE_BY_ZERO |
| main | `reader/sql/off/on/proj_add` | BOTH_RAISE | RAISE ARITHMETIC_OVERFLOW | RAISE ARITHMETIC_OVERFLOW |
| main | `reader/df/off/on/proj_add` | BOTH_RAISE | RAISE ARITHMETIC_OVERFLOW | RAISE ARITHMETIC_OVERFLOW |
| main | `reader/direct/off/on/proj_add` | BOTH_RAISE | RAISE ARITHMETIC_OVERFLOW | RAISE ARITHMETIC_OVERFLOW |
| main | `reader/create-sql/utc` | EQUAL | [[], []] | [[], []] |
| main | `reader/create-df/utc` | EQUAL | OK | OK |
| main | `reader/sql/utc/la/filter` | VALUE | [["2/18"], ["int", "int"]] | [["18/18"], ["int", "int"]] |
| main | `reader/df/utc/la/filter` | VALUE | [["2/18"], ["int", "int"]] | [["18/18"], ["int", "int"]] |
| main | `reader/direct/utc/la/filter` | EQUAL | [["18/18"], ["int", "int"]] | [["18/18"], ["int", "int"]] |
| main | `reader/sql/utc/la/proj` | VALUE | [["2/18/2023-12-31 18:30:00"], ["int", "int", "string"]] | [["18/18/2023-12-31 18:30:00"], ["int", "int", "string"]] |
| main | `reader/df/utc/la/proj` | VALUE | [["2/18/2023-12-31 18:30:00"], ["int", "int", "string"]] | [["18/18/2023-12-31 18:30:00"], ["int", "int", "string"]] |
| main | `reader/direct/utc/la/proj` | EQUAL | [["18/18/2023-12-31 18:30:00"], ["int", "int", "string"]] | [["18/18/2023-12-31 18:30:00"], ["int", "int", "string"]] |
| main | `reader/create-sql/la` | EQUAL | [[], []] | [[], []] |
| main | `reader/create-df/la` | EQUAL | OK | OK |
| main | `reader/sql/la/utc/filter` | VALUE | [["18/2"], ["int", "int"]] | [["2/2"], ["int", "int"]] |
| main | `reader/df/la/utc/filter` | VALUE | [["18/2"], ["int", "int"]] | [["2/2"], ["int", "int"]] |
| main | `reader/direct/la/utc/filter` | EQUAL | [["2/2"], ["int", "int"]] | [["2/2"], ["int", "int"]] |
| main | `reader/sql/la/utc/proj` | VALUE | [["18/2/2024-01-01 02:30:00"], ["int", "int", "string"]] | [["2/2/2024-01-01 02:30:00"], ["int", "int", "string"]] |
| main | `reader/df/la/utc/proj` | VALUE | [["18/2/2024-01-01 02:30:00"], ["int", "int", "string"]] | [["2/2/2024-01-01 02:30:00"], ["int", "int", "string"]] |
| main | `reader/direct/la/utc/proj` | EQUAL | [["2/2/2024-01-01 02:30:00"], ["int", "int", "string"]] | [["2/2/2024-01-01 02:30:00"], ["int", "int", "string"]] |

## Every now cell

| build | cell | verdict | Spark | RePark |
|---|---|---|---|---|
| main | `now/create-sql/utc` | EQUAL | [[], []] | [[], []] |
| main | `now/create-df/utc` | EQUAL | OK | OK |
| main | `now/sql/utc/la` | EQUAL | [["America/Los_Angeles/America/Los_Angeles/true"], ["string", "string", "boolean"]] | [["America/Los_Angeles/America/Los_Angeles/true"], ["string", "string", "boolean"]] |
| main | `now/df/utc/la` | EQUAL | [["America/Los_Angeles/America/Los_Angeles/true"], ["string", "string", "boolean"]] | [["America/Los_Angeles/America/Los_Angeles/true"], ["string", "string", "boolean"]] |
| main | `now/create-sql/la` | EQUAL | [[], []] | [[], []] |
| main | `now/create-df/la` | EQUAL | OK | OK |
| main | `now/sql/la/utc` | EQUAL | [["UTC/UTC/true"], ["string", "string", "boolean"]] | [["UTC/UTC/true"], ["string", "string", "boolean"]] |
| main | `now/df/la/utc` | EQUAL | [["UTC/UTC/true"], ["string", "string", "boolean"]] | [["UTC/UTC/true"], ["string", "string", "boolean"]] |

## Every unset cell

| build | cell | verdict | Spark | RePark |
|---|---|---|---|---|
| main | `unset/ansi_at_start` | VALUE | [["spark.sql.ansi.enabled/true"], ["string", "string"]] | [["spark.sql.ansi.enabled/<undefined>"], ["string", "string"]] |
| main | `unset/create-sql/div` | EQUAL | [[], []] | [[], []] |
| main | `unset/create-df/div` | EQUAL | OK | OK |
| main | `unset/create-sql/add_ovf` | EQUAL | [[], []] | [[], []] |
| main | `unset/create-df/add_ovf` | EQUAL | OK | OK |
| main | `unset/create-sql/cast_ovf` | EQUAL | [[], []] | [[], []] |
| main | `unset/create-df/cast_ovf` | EQUAL | OK | OK |
| main | `unset/read/sql/unset/div` | BOTH_RAISE | RAISE DIVIDE_BY_ZERO | RAISE DIVIDE_BY_ZERO |
| main | `unset/read/df/unset/div` | BOTH_RAISE | RAISE DIVIDE_BY_ZERO | RAISE DIVIDE_BY_ZERO |
| main | `unset/read/held/unset/div` | BOTH_RAISE | RAISE DIVIDE_BY_ZERO | RAISE DIVIDE_BY_ZERO |
| main | `unset/direct/unset/div` | BOTH_RAISE | RAISE DIVIDE_BY_ZERO | RAISE DIVIDE_BY_ZERO |
| main | `unset/read/sql/unset/add_ovf` | BOTH_RAISE | RAISE ARITHMETIC_OVERFLOW | RAISE ARITHMETIC_OVERFLOW |
| main | `unset/read/df/unset/add_ovf` | BOTH_RAISE | RAISE ARITHMETIC_OVERFLOW | RAISE ARITHMETIC_OVERFLOW |
| main | `unset/read/held/unset/add_ovf` | BOTH_RAISE | RAISE ARITHMETIC_OVERFLOW | RAISE ARITHMETIC_OVERFLOW |
| main | `unset/direct/unset/add_ovf` | BOTH_RAISE | RAISE ARITHMETIC_OVERFLOW | RAISE ARITHMETIC_OVERFLOW |
| main | `unset/read/sql/unset/cast_ovf` | BOTH_RAISE_OTHER | RAISE CAST_OVERFLOW | RAISE Arrow error: Cast error: Can't cast value 2147483647 to type |
| main | `unset/read/df/unset/cast_ovf` | BOTH_RAISE_OTHER | RAISE CAST_OVERFLOW | RAISE Arrow error: Cast error: Can't cast value 2147483647 to type |
| main | `unset/read/held/unset/cast_ovf` | BOTH_RAISE_OTHER | RAISE CAST_OVERFLOW | RAISE Arrow error: Cast error: Can't cast value 2147483647 to type |
| main | `unset/direct/unset/cast_ovf` | BOTH_RAISE_OTHER | RAISE CAST_OVERFLOW | RAISE Arrow error: Cast error: Can't cast value 2147483647 to type |
| main | `unset/read/sql/off/div` | REPARK_ANSWERS | RAISE DIVIDE_BY_ZERO | [["null"], ["double"]] |
| main | `unset/read/df/off/div` | BOTH_RAISE | RAISE DIVIDE_BY_ZERO | RAISE DIVIDE_BY_ZERO |
| main | `unset/read/held/off/div` | BOTH_RAISE | RAISE DIVIDE_BY_ZERO | RAISE DIVIDE_BY_ZERO |
| main | `unset/direct/off/div` | EQUAL | [["null"], ["double"]] | [["null"], ["double"]] |
| main | `unset/read/sql/off/add_ovf` | REPARK_ANSWERS | RAISE ARITHMETIC_OVERFLOW | [["-2147483648"], ["int"]] |
| main | `unset/read/df/off/add_ovf` | REPARK_ANSWERS | RAISE ARITHMETIC_OVERFLOW | [["-2147483648"], ["int"]] |
| main | `unset/read/held/off/add_ovf` | BOTH_RAISE | RAISE ARITHMETIC_OVERFLOW | RAISE ARITHMETIC_OVERFLOW |
| main | `unset/direct/off/add_ovf` | EQUAL | [["-2147483648"], ["int"]] | [["-2147483648"], ["int"]] |
| main | `unset/read/sql/off/cast_ovf` | BOTH_RAISE_OTHER | RAISE CAST_OVERFLOW | RAISE Arrow error: Cast error: Can't cast value 2147483647 to type |
| main | `unset/read/df/off/cast_ovf` | BOTH_RAISE_OTHER | RAISE CAST_OVERFLOW | RAISE Arrow error: Cast error: Can't cast value 2147483647 to type |
| main | `unset/read/held/off/cast_ovf` | BOTH_RAISE_OTHER | RAISE CAST_OVERFLOW | RAISE Arrow error: Cast error: Can't cast value 2147483647 to type |
| main | `unset/direct/off/cast_ovf` | REPARK_RAISES | [["-1"], ["smallint"]] | RAISE Arrow error: Cast error: Can't cast value 2147483647 to type |

