cells: spark 75032, base 75032, head 75032

| scope | build | EQUAL | WRONG | RAISE | MISSING | total |
|---|---|---|---|---|---|---|
| all | base | 30758 | 13786 | 30488 | 0 | 75032 |
| all | head | 34159 | 12521 | 28352 | 0 | 75032 |
| ltz hour | base | 128 | 384 | 512 | 0 | 1024 |
| ltz hour | head | 1014 | 10 | 0 | 0 | 1024 |
| ltz minute | base | 128 | 384 | 512 | 0 | 1024 |
| ltz minute | head | 1024 | 0 | 0 | 0 | 1024 |
| ltz other | base | 24410 | 9964 | 22522 | 0 | 56896 |
| ltz other | head | 24410 | 9964 | 22522 | 0 | 56896 |
| ltz second | base | 256 | 256 | 512 | 0 | 1024 |
| ltz second | head | 1024 | 0 | 0 | 0 | 1024 |
| multi-row hour | base | 0 | 0 | 72 | 0 | 72 |
| multi-row hour | head | 72 | 0 | 0 | 0 | 72 |
| multi-row minute | base | 0 | 0 | 72 | 0 | 72 |
| multi-row minute | head | 72 | 0 | 0 | 0 | 72 |
| multi-row second | base | 0 | 0 | 72 | 0 | 72 |
| multi-row second | head | 72 | 0 | 0 | 0 | 72 |
| ntz hour | base | 32 | 96 | 128 | 0 | 256 |
| ntz hour | head | 251 | 5 | 0 | 0 | 256 |
| ntz minute | base | 32 | 96 | 128 | 0 | 256 |
| ntz minute | head | 256 | 0 | 0 | 0 | 256 |
| ntz other | base | 5708 | 2542 | 5830 | 0 | 14080 |
| ntz other | head | 5708 | 2542 | 5830 | 0 | 14080 |
| ntz second | base | 64 | 64 | 128 | 0 | 256 |
| ntz second | head | 256 | 0 | 0 | 0 | 256 |

moved cells (head answer differs from base): 3419
{"hour": 1195, "minute": 1192, "second": 1032}
gained EQUAL: 3404
{"hour": 1180, "minute": 1192, "second": 1032}
lost EQUAL: 3
a0-la	L	date	fexpr	hour	0	0	16	EQUAL	WRONG
a1-la	L	date	fexpr	hour	0	0	16	EQUAL	WRONG
a1-la-ntz	L	date	fexpr	hour	0	0	16	EQUAL	WRONG

hms cells not EQUAL on head: 15
a0-la	L	date	fexpr	hour	0	0	16	EQUAL	WRONG
a0-la	L	frac	fexpr	hour	3	0	19	WRONG	WRONG
a0-la	L	full	fexpr	hour	3	0	19	WRONG	WRONG
a0-la	L	gap	fexpr	hour	3	0	18	WRONG	WRONG
a0-la	L	tsep	fexpr	hour	23	0	16	WRONG	WRONG
a1-la	L	date	fexpr	hour	0	0	16	EQUAL	WRONG
a1-la	L	frac	fexpr	hour	3	0	19	WRONG	WRONG
a1-la	L	full	fexpr	hour	3	0	19	WRONG	WRONG
a1-la	L	gap	fexpr	hour	3	0	18	WRONG	WRONG
a1-la	L	tsep	fexpr	hour	23	0	16	WRONG	WRONG
a1-la-ntz	L	date	fexpr	hour	0	0	16	EQUAL	WRONG
a1-la-ntz	L	frac	fexpr	hour	3	0	19	WRONG	WRONG
a1-la-ntz	L	full	fexpr	hour	3	0	19	WRONG	WRONG
a1-la-ntz	L	gap	fexpr	hour	3	0	18	WRONG	WRONG
a1-la-ntz	L	tsep	fexpr	hour	23	0	16	WRONG	WRONG
