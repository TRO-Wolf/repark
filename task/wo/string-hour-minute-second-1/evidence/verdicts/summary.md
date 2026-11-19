cells: 60184 (LTZ configurations), 14848 (NTZ)
missing in main: 0

## All LTZ cells
| key | EQUAL | WRONG | RAISE | total |
|---|---|---|---|---|
| all | 24922 | 10988 | 24274 | 60184 |

## By configuration
| key | EQUAL | WRONG | RAISE | total |
|---|---|---|---|---|
| a0-la | 6231 | 3036 | 5779 | 15046 |
| a0-utc | 6689 | 2578 | 5779 | 15046 |
| a1-la | 5772 | 2910 | 6364 | 15046 |
| a1-utc | 6230 | 2464 | 6352 | 15046 |

## By family
| key | EQUAL | WRONG | RAISE | total |
|---|---|---|---|---|
| control | 4524 | 84 | 512 | 5120 |
| date_field | 3584 | 512 | 4240 | 8336 |
| date_part | 1920 | 722 | 11766 | 14408 |
| date_trunc | 5600 | 4712 | 0 | 10312 |
| extract | 7104 | 2856 | 5544 | 15504 |
| format | 1678 | 1078 | 460 | 3216 |
| hms | 512 | 1024 | 1752 | 3288 |

## By door
| key | EQUAL | WRONG | RAISE | total |
|---|---|---|---|---|
| fcol | 8016 | 3240 | 3592 | 14848 |
| fexpr | 5604 | 2512 | 6732 | 14848 |
| g:group_f | 12 | 42 | 78 | 132 |
| g:group_sql | 6 | 42 | 84 | 132 |
| g:order_f | 12 | 42 | 78 | 132 |
| g:order_sql | 6 | 42 | 84 | 132 |
| g:where_f | 12 | 42 | 78 | 132 |
| g:where_sql | 6 | 42 | 84 | 132 |
| sexpr | 5624 | 2492 | 6732 | 14848 |
| sql | 5624 | 2492 | 6732 | 14848 |

## By input (projection doors)
| key | EQUAL | WRONG | RAISE | total |
|---|---|---|---|---|
| I | 6222 | 2679 | 5947 | 14848 |
| L | 6202 | 2699 | 5947 | 14848 |
| P | 6222 | 2679 | 5947 | 14848 |
| V | 6222 | 2679 | 5947 | 14848 |

## By function (projection doors)
| key | EQUAL | WRONG | RAISE | total |
|---|---|---|---|---|
| cast_date | 448 | 64 | 512 | 1024 |
| cast_hour | 1014 | 10 | 0 | 1024 |
| cast_minute | 1024 | 0 | 0 | 1024 |
| cast_second | 1024 | 0 | 0 | 1024 |
| cast_ts | 1014 | 10 | 0 | 1024 |
| date_format_HH | 512 | 512 | 0 | 1024 |
| date_format_full | 160 | 416 | 448 | 1024 |
| date_part_DAY | 184 | 48 | 792 | 1024 |
| date_part_DAYOFWEEK | 24 | 0 | 1000 | 1024 |
| date_part_DAYOFWEEK_ISO | 24 | 0 | 1000 | 1024 |
| date_part_DOY | 184 | 48 | 792 | 1024 |
| date_part_HOUR | 200 | 32 | 792 | 1024 |
| date_part_MINUTE | 208 | 24 | 792 | 1024 |
| date_part_MONTH | 184 | 48 | 792 | 1024 |
| date_part_QUARTER | 184 | 48 | 792 | 1024 |
| date_part_S | 64 | 168 | 792 | 1024 |
| date_part_SECOND | 64 | 168 | 792 | 1024 |
| date_part_WEEK | 184 | 48 | 792 | 1024 |
| date_part_YEAR | 184 | 48 | 792 | 1024 |
| date_part_YEAROFWEEK | 24 | 0 | 1000 | 1024 |
| date_part_min | 208 | 24 | 792 | 1024 |
| date_trunc_DAY | 576 | 448 | 0 | 1024 |
| date_trunc_HOUR | 544 | 480 | 0 | 1024 |
| date_trunc_MICROSECOND | 544 | 480 | 0 | 1024 |
| date_trunc_MILLISECOND | 544 | 480 | 0 | 1024 |
| date_trunc_MINUTE | 544 | 480 | 0 | 1024 |
| date_trunc_MONTH | 576 | 448 | 0 | 1024 |
| date_trunc_QUARTER | 576 | 448 | 0 | 1024 |
| date_trunc_SECOND | 544 | 480 | 0 | 1024 |
| date_trunc_WEEK | 576 | 448 | 0 | 1024 |
| date_trunc_YEAR | 576 | 448 | 0 | 1024 |
| dayofmonth | 448 | 64 | 512 | 1024 |
| dayofweek | 448 | 64 | 512 | 1024 |
| dayofyear | 448 | 64 | 512 | 1024 |
| extract_DAY | 736 | 192 | 96 | 1024 |
| extract_DAYOFWEEK | 96 | 0 | 928 | 1024 |
| extract_DAYOFWEEK_ISO | 24 | 0 | 1000 | 1024 |
| extract_DOY | 736 | 192 | 96 | 1024 |
| extract_H | 200 | 32 | 792 | 1024 |
| extract_HOUR | 800 | 128 | 96 | 1024 |
| extract_HRS | 200 | 32 | 792 | 1024 |
| extract_MINUTE | 832 | 96 | 96 | 1024 |
| extract_MONTH | 736 | 192 | 96 | 1024 |
| extract_QUARTER | 736 | 192 | 96 | 1024 |
| extract_SECOND | 256 | 672 | 96 | 1024 |
| extract_SECONDS | 256 | 672 | 96 | 1024 |
| extract_WEEK | 736 | 192 | 96 | 1024 |
| extract_YEAR | 736 | 192 | 96 | 1024 |
| extract_YEAROFWEEK | 24 | 0 | 1000 | 1024 |
| hour | 128 | 384 | 512 | 1024 |
| minute | 128 | 384 | 512 | 1024 |
| month | 448 | 64 | 512 | 1024 |
| quarter | 448 | 64 | 512 | 1024 |
| second | 256 | 256 | 512 | 1024 |
| unix_timestamp | 952 | 60 | 12 | 1024 |
| weekday | 448 | 64 | 512 | 1024 |
| weekofyear | 448 | 64 | 512 | 1024 |
| year | 448 | 64 | 512 | 1024 |

## Not-EQUAL projection cells by function and shape (each of 64 = 4 cfg x 4 in x 4 doors)
| function | date | full | tsep | frac | off | zulu | lead | trail | empty | null | bad | nosec | tonly | badtime | gap | yonly |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| hour | . | W64 | W64 | W64 | W64 | W64 | R64 | R64 | R64 | . | R64 | R64 | R64 | R64 | W64 | R64 |
| minute | . | W64 | W64 | W64 | W64 | W64 | R64 | R64 | R64 | . | R64 | R64 | R64 | R64 | W64 | R64 |
| second | . | W64 | . | W64 | W64 | W64 | R64 | R64 | R64 | . | R64 | R64 | R64 | R64 | . | R64 |
| year | . | . | . | . | W64 | . | R64 | R64 | R64 | . | R64 | R64 | R64 | R64 | . | R64 |
| quarter | . | . | . | . | W64 | . | R64 | R64 | R64 | . | R64 | R64 | R64 | R64 | . | R64 |
| month | . | . | . | . | W64 | . | R64 | R64 | R64 | . | R64 | R64 | R64 | R64 | . | R64 |
| dayofmonth | . | . | . | . | W64 | . | R64 | R64 | R64 | . | R64 | R64 | R64 | R64 | . | R64 |
| dayofweek | . | . | . | . | W64 | . | R64 | R64 | R64 | . | R64 | R64 | R64 | R64 | . | R64 |
| dayofyear | . | . | . | . | W64 | . | R64 | R64 | R64 | . | R64 | R64 | R64 | R64 | . | R64 |
| weekofyear | . | . | . | . | W64 | . | R64 | R64 | R64 | . | R64 | R64 | R64 | R64 | . | R64 |
| weekday | . | . | . | . | W64 | . | R64 | R64 | R64 | . | R64 | R64 | R64 | R64 | . | R64 |
| extract_YEAR | . | . | . | . | W64 | W32 | . | . | . | . | . | W32R32 | . | W32R32 | . | W32R32 |
| extract_YEAROFWEEK | R64 | R64 | R64 | R64 | R64 | R64 | R64 | R64 | R56 | R64 | R56 | R64 | R56 | R64 | R64 | R64 |
| extract_QUARTER | . | . | . | . | W64 | W32 | . | . | . | . | . | W32R32 | . | W32R32 | . | W32R32 |
| extract_MONTH | . | . | . | . | W64 | W32 | . | . | . | . | . | W32R32 | . | W32R32 | . | W32R32 |
| extract_WEEK | . | . | . | . | W64 | W32 | . | . | . | . | . | W32R32 | . | W32R32 | . | W32R32 |
| extract_DAY | . | . | . | . | W64 | W32 | . | . | . | . | . | W32R32 | . | W32R32 | . | W32R32 |
| extract_DAYOFWEEK | R64 | R64 | R64 | R64 | R64 | R64 | R64 | R64 | R32 | R64 | R32 | R64 | R32 | R64 | R64 | R64 |
| extract_DAYOFWEEK_ISO | R64 | R64 | R64 | R64 | R64 | R64 | R64 | R64 | R56 | R64 | R56 | R64 | R56 | R64 | R64 | R64 |
| extract_DOY | . | . | . | . | W64 | W32 | . | . | . | . | . | W32R32 | . | W32R32 | . | W32R32 |
| extract_HOUR | . | . | . | . | . | . | . | . | . | . | . | W32R32 | W32R32 | . | W32 | W32R32 |
| extract_MINUTE | . | . | . | . | . | . | . | . | . | . | . | W32R32 | W32R32 | . | . | W32R32 |
| extract_SECOND | W64 | W64 | W64 | W64 | W64 | W64 | W64 | W64 | . | . | . | W32R32 | W32R32 | . | W64 | W32R32 |
| date_part_YEAR | R48 | R48 | R48 | R48 | W16R48 | W8R48 | R48 | R48 | R48 | R48 | R48 | W8R56 | R48 | W8R56 | R48 | W8R56 |
| date_part_YEAROFWEEK | R64 | R64 | R64 | R64 | R64 | R64 | R64 | R64 | R56 | R64 | R56 | R64 | R56 | R64 | R64 | R64 |
| date_part_QUARTER | R48 | R48 | R48 | R48 | W16R48 | W8R48 | R48 | R48 | R48 | R48 | R48 | W8R56 | R48 | W8R56 | R48 | W8R56 |
| date_part_MONTH | R48 | R48 | R48 | R48 | W16R48 | W8R48 | R48 | R48 | R48 | R48 | R48 | W8R56 | R48 | W8R56 | R48 | W8R56 |
| date_part_WEEK | R48 | R48 | R48 | R48 | W16R48 | W8R48 | R48 | R48 | R48 | R48 | R48 | W8R56 | R48 | W8R56 | R48 | W8R56 |
| date_part_DAY | R48 | R48 | R48 | R48 | W16R48 | W8R48 | R48 | R48 | R48 | R48 | R48 | W8R56 | R48 | W8R56 | R48 | W8R56 |
| date_part_DAYOFWEEK | R64 | R64 | R64 | R64 | R64 | R64 | R64 | R64 | R56 | R64 | R56 | R64 | R56 | R64 | R64 | R64 |
| date_part_DAYOFWEEK_ISO | R64 | R64 | R64 | R64 | R64 | R64 | R64 | R64 | R56 | R64 | R56 | R64 | R56 | R64 | R64 | R64 |
| date_part_DOY | R48 | R48 | R48 | R48 | W16R48 | W8R48 | R48 | R48 | R48 | R48 | R48 | W8R56 | R48 | W8R56 | R48 | W8R56 |
| date_part_HOUR | R48 | R48 | R48 | R48 | R48 | R48 | R48 | R48 | R48 | R48 | R48 | W8R56 | W8R56 | R48 | W8R48 | W8R56 |
| date_part_MINUTE | R48 | R48 | R48 | R48 | R48 | R48 | R48 | R48 | R48 | R48 | R48 | W8R56 | W8R56 | R48 | R48 | W8R56 |
| date_part_SECOND | W16R48 | W16R48 | W16R48 | W16R48 | W16R48 | W16R48 | W16R48 | W16R48 | R48 | R48 | R48 | W8R56 | W8R56 | R48 | W16R48 | W8R56 |
| extract_H | R48 | R48 | R48 | R48 | R48 | R48 | R48 | R48 | R48 | R48 | R48 | W8R56 | W8R56 | R48 | W8R48 | W8R56 |
| extract_HRS | R48 | R48 | R48 | R48 | R48 | R48 | R48 | R48 | R48 | R48 | R48 | W8R56 | W8R56 | R48 | W8R48 | W8R56 |
| date_part_min | R48 | R48 | R48 | R48 | R48 | R48 | R48 | R48 | R48 | R48 | R48 | W8R56 | W8R56 | R48 | R48 | W8R56 |
| date_part_S | W16R48 | W16R48 | W16R48 | W16R48 | W16R48 | W16R48 | W16R48 | W16R48 | R48 | R48 | R48 | W8R56 | W8R56 | R48 | W16R48 | W8R56 |
| extract_SECONDS | W64 | W64 | W64 | W64 | W64 | W64 | W64 | W64 | . | . | . | W32R32 | W32R32 | . | W64 | W32R32 |
| date_trunc_YEAR | . | . | . | . | . | W32 | W64 | W64 | W32 | . | W32 | W64 | W64 | W32 | . | W64 |
| date_trunc_QUARTER | . | . | . | . | . | W32 | W64 | W64 | W32 | . | W32 | W64 | W64 | W32 | . | W64 |
| date_trunc_MONTH | . | . | . | . | . | W32 | W64 | W64 | W32 | . | W32 | W64 | W64 | W32 | . | W64 |
| date_trunc_WEEK | . | . | . | . | . | W32 | W64 | W64 | W32 | . | W32 | W64 | W64 | W32 | . | W64 |
| date_trunc_DAY | . | . | . | . | . | W32 | W64 | W64 | W32 | . | W32 | W64 | W64 | W32 | . | W64 |
| date_trunc_HOUR | . | . | . | . | W32 | W32 | W64 | W64 | W32 | . | W32 | W64 | W64 | W32 | . | W64 |
| date_trunc_MINUTE | . | . | . | . | W32 | W32 | W64 | W64 | W32 | . | W32 | W64 | W64 | W32 | . | W64 |
| date_trunc_SECOND | . | . | . | . | W32 | W32 | W64 | W64 | W32 | . | W32 | W64 | W64 | W32 | . | W64 |
| date_trunc_MILLISECOND | . | . | . | . | W32 | W32 | W64 | W64 | W32 | . | W32 | W64 | W64 | W32 | . | W64 |
| date_trunc_MICROSECOND | . | . | . | . | W32 | W32 | W64 | W64 | W32 | . | W32 | W64 | W64 | W32 | . | W64 |
| unix_timestamp | . | . | . | . | . | . | W48 | . | . | . | . | . | . | . | W12R12 | . |
| date_format_HH | . | . | . | . | W32 | W32 | W64 | W64 | W32 | . | W32 | W64 | W64 | W32 | W32 | W64 |
| date_format_full | R64 | R64 | R64 | R64 | R64 | R64 | W64 | W64 | W32 | . | W32 | W64 | W64 | W32 | R64 | W64 |
| cast_hour | W2 | W2 | W2 | W2 | . | . | . | . | . | . | . | . | . | . | W2 | . |
| cast_minute | . | . | . | . | . | . | . | . | . | . | . | . | . | . | . | . |
| cast_second | . | . | . | . | . | . | . | . | . | . | . | . | . | . | . | . |
| cast_ts | W2 | W2 | W2 | W2 | . | . | . | . | . | . | . | . | . | . | W2 | . |
| cast_date | . | . | . | . | W64 | . | R64 | R64 | R64 | . | R64 | R64 | R64 | R64 | . | R64 |

## Spark: implicit cast equals the explicit cast (hour/minute/second vs cast_*)
{"ltz:True": 3072, "ntz:False": 96, "ntz:True": 672}

## Seat forecast: main's cast_* answer against Spark's answer, per cell
## forecast by function
| key | EQUAL | WRONG | RAISE | total |
|---|---|---|---|---|
| date_part_HOUR | 1014 | 10 | 0 | 1024 |
| date_part_HOUR ntz | 251 | 5 | 0 | 256 |
| date_part_MINUTE | 1024 | 0 | 0 | 1024 |
| date_part_MINUTE ntz | 256 | 0 | 0 | 256 |
| date_part_min | 1024 | 0 | 0 | 1024 |
| date_part_min ntz | 256 | 0 | 0 | 256 |
| extract_H | 1014 | 10 | 0 | 1024 |
| extract_H ntz | 251 | 5 | 0 | 256 |
| extract_HOUR | 1014 | 10 | 0 | 1024 |
| extract_HOUR ntz | 251 | 5 | 0 | 256 |
| extract_HRS | 1014 | 10 | 0 | 1024 |
| extract_HRS ntz | 251 | 5 | 0 | 256 |
| extract_MINUTE | 1024 | 0 | 0 | 1024 |
| extract_MINUTE ntz | 256 | 0 | 0 | 256 |
| hour | 1014 | 10 | 0 | 1024 |
| hour ntz | 251 | 5 | 0 | 256 |
| minute | 1024 | 0 | 0 | 1024 |
| minute ntz | 256 | 0 | 0 | 256 |
| second | 1024 | 0 | 0 | 1024 |
| second ntz | 256 | 0 | 0 | 256 |

residue (hms): 15
a0-la L date fexpr hour spark=('v', '0') proxy=('v', '16')
a0-la L frac fexpr hour spark=('v', '3') proxy=('v', '19')
a0-la L full fexpr hour spark=('v', '3') proxy=('v', '19')
a0-la L gap fexpr hour spark=('v', '3') proxy=('v', '18')
a0-la L tsep fexpr hour spark=('v', '23') proxy=('v', '16')
a1-la L date fexpr hour spark=('v', '0') proxy=('v', '16')
a1-la L frac fexpr hour spark=('v', '3') proxy=('v', '19')
a1-la L full fexpr hour spark=('v', '3') proxy=('v', '19')
a1-la L gap fexpr hour spark=('v', '3') proxy=('v', '18')
a1-la L tsep fexpr hour spark=('v', '23') proxy=('v', '16')
a1-la-ntz L date fexpr hour spark=('v', '0') proxy=('v', '16')
a1-la-ntz L frac fexpr hour spark=('v', '3') proxy=('v', '19')
a1-la-ntz L full fexpr hour spark=('v', '3') proxy=('v', '19')
a1-la-ntz L gap fexpr hour spark=('v', '3') proxy=('v', '18')
a1-la-ntz L tsep fexpr hour spark=('v', '23') proxy=('v', '16')

## NTZ configurations (spark.sql.timestampType=TIMESTAMP_NTZ, ANSI on, L and I)
## NTZ by function
| key | EQUAL | WRONG | RAISE | total |
|---|---|---|---|---|
| cast_date | 112 | 16 | 128 | 256 |
| cast_hour | 204 | 52 | 0 | 256 |
| cast_minute | 224 | 32 | 0 | 256 |
| cast_second | 240 | 16 | 0 | 256 |
| cast_ts | 204 | 52 | 0 | 256 |
| date_format_HH | 104 | 152 | 0 | 256 |
| date_format_full | 16 | 128 | 112 | 256 |
| date_part_DAY | 46 | 6 | 204 | 256 |
| date_part_DAYOFWEEK | 12 | 0 | 244 | 256 |
| date_part_DAYOFWEEK_ISO | 12 | 0 | 244 | 256 |
| date_part_DOY | 46 | 6 | 204 | 256 |
| date_part_HOUR | 50 | 2 | 204 | 256 |
| date_part_MINUTE | 52 | 0 | 204 | 256 |
| date_part_MONTH | 46 | 6 | 204 | 256 |
| date_part_QUARTER | 46 | 6 | 204 | 256 |
| date_part_S | 16 | 36 | 204 | 256 |
| date_part_SECOND | 16 | 36 | 204 | 256 |
| date_part_WEEK | 46 | 6 | 204 | 256 |
| date_part_YEAR | 46 | 6 | 204 | 256 |
| date_part_YEAROFWEEK | 12 | 0 | 244 | 256 |
| date_part_min | 52 | 0 | 204 | 256 |
| date_trunc_DAY | 120 | 136 | 0 | 256 |
| date_trunc_HOUR | 112 | 144 | 0 | 256 |
| date_trunc_MICROSECOND | 112 | 144 | 0 | 256 |
| date_trunc_MILLISECOND | 112 | 144 | 0 | 256 |
| date_trunc_MINUTE | 112 | 144 | 0 | 256 |
| date_trunc_MONTH | 120 | 136 | 0 | 256 |
| date_trunc_QUARTER | 120 | 136 | 0 | 256 |
| date_trunc_SECOND | 112 | 144 | 0 | 256 |
| date_trunc_WEEK | 120 | 136 | 0 | 256 |
| date_trunc_YEAR | 120 | 136 | 0 | 256 |
| dayofmonth | 112 | 16 | 128 | 256 |
| dayofweek | 112 | 16 | 128 | 256 |
| dayofyear | 112 | 16 | 128 | 256 |
| extract_DAY | 184 | 24 | 48 | 256 |
| extract_DAYOFWEEK | 48 | 0 | 208 | 256 |
| extract_DAYOFWEEK_ISO | 12 | 0 | 244 | 256 |
| extract_DOY | 184 | 24 | 48 | 256 |
| extract_H | 50 | 2 | 204 | 256 |
| extract_HOUR | 200 | 8 | 48 | 256 |
| extract_HRS | 50 | 2 | 204 | 256 |
| extract_MINUTE | 208 | 0 | 48 | 256 |
| extract_MONTH | 184 | 24 | 48 | 256 |
| extract_QUARTER | 184 | 24 | 48 | 256 |
| extract_SECOND | 64 | 144 | 48 | 256 |
| extract_SECONDS | 64 | 144 | 48 | 256 |
| extract_WEEK | 184 | 24 | 48 | 256 |
| extract_YEAR | 184 | 24 | 48 | 256 |
| extract_YEAROFWEEK | 12 | 0 | 244 | 256 |
| hour | 32 | 96 | 128 | 256 |
| minute | 32 | 96 | 128 | 256 |
| month | 112 | 16 | 128 | 256 |
| quarter | 112 | 16 | 128 | 256 |
| second | 64 | 64 | 128 | 256 |
| unix_timestamp | 238 | 12 | 6 | 256 |
| weekday | 112 | 16 | 128 | 256 |
| weekofyear | 112 | 16 | 128 | 256 |
| year | 112 | 16 | 128 | 256 |

## hms not-EQUAL cells by shape and configuration (each of 16 = 4 in x 4 doors)
| shape | a0-utc | a0-la | a1-utc | a1-la |
|---|---|---|---|---|
| date | . | . | . | . |
| full | W48 | W48 | W48 | W48 |
| tsep | W32 | W32 | W32 | W32 |
| frac | W48 | W48 | W48 | W48 |
| off | W48 | W48 | W48 | W48 |
| zulu | W48 | W48 | W48 | W48 |
| lead | R48 | R48 | R48 | R48 |
| trail | R48 | R48 | R48 | R48 |
| empty | R48 | R48 | R48 | R48 |
| null | . | . | . | . |
| bad | R48 | R48 | R48 | R48 |
| nosec | R48 | R48 | R48 | R48 |
| tonly | R48 | R48 | R48 | R48 |
| badtime | R48 | R48 | R48 | R48 |
| gap | W32 | W32 | W32 | W32 |
| yonly | R48 | R48 | R48 | R48 |

## Literal SQL answers, Spark / main (hour, minute, second, extract_SECOND, year)
### a0-utc
| shape | hour | minute | second | extract_SECOND | year | cast_hour |
|---|---|---|---|---|---|---|
| date | 0 / 0 | 0 / 0 | 0 / 0 | 0.000000 / 0 | 2024 / 2024 | 0 / 0 |
| full | 3 / 0 | 4 / 0 | 5 / 0 | 5.000000 / 5 | 2024 / 2024 | 3 / 3 |
| tsep | 23 / 0 | 30 / 0 | 0 / 0 | 0.000000 / 0 | 2024 / 2024 | 23 / 23 |
| frac | 3 / 0 | 4 / 0 | 5 / 0 | 5.123456 / 5 | 2024 / 2024 | 3 / 3 |
| off | 21 / 0 | 34 / 0 | 5 / 0 | 5.000000 / 5 | 2024 / 2023 | 21 / 21 |
| zulu | 3 / 0 | 4 / 0 | 5 / 0 | 5.000000 / 5 | 2024 / 2024 | 3 / 3 |
| lead | 3 / !datafusion engine error: | 4 / !datafusion engine error: | 5 / !datafusion engine error: | 5.000000 / 5 | 2024 / !datafusion engine error: | 3 / 3 |
| trail | 3 / !datafusion engine error: | 4 / !datafusion engine error: | 5 / !datafusion engine error: | 5.000000 / 5 | 2024 / !datafusion engine error: | 3 / 3 |
| empty | None / !datafusion engine error: | None / !datafusion engine error: | None / !datafusion engine error: | None / None | None / !datafusion engine error: | None / None |
| null | None / None | None / None | None / None | None / None | None / None | None / None |
| bad | None / !datafusion engine error: | None / !datafusion engine error: | None / !datafusion engine error: | None / None | None / !datafusion engine error: | None / None |
| nosec | 3 / !datafusion engine error: | 4 / !datafusion engine error: | 0 / !datafusion engine error: | 0.000000 / None | 2024 / !datafusion engine error: | 3 / 3 |
| tonly | 3 / !datafusion engine error: | 4 / !datafusion engine error: | 5 / !datafusion engine error: | 5.000000 / None | None / !datafusion engine error: | 3 / 3 |
| badtime | None / !datafusion engine error: | None / !datafusion engine error: | None / !datafusion engine error: | None / None | 2024 / !datafusion engine error: | None / None |
| gap | 2 / 0 | 30 / 0 | 0 / 0 | 0.000000 / 0 | 2024 / 2024 | 2 / 2 |
| yonly | 0 / !datafusion engine error: | 0 / !datafusion engine error: | 0 / !datafusion engine error: | 0.000000 / None | 2024 / !datafusion engine error: | 0 / 0 |

### a0-la
| shape | hour | minute | second | extract_SECOND | year | cast_hour |
|---|---|---|---|---|---|---|
| date | 0 / 0 | 0 / 0 | 0 / 0 | 0.000000 / 0 | 2024 / 2024 | 0 / 0 |
| full | 3 / 0 | 4 / 0 | 5 / 0 | 5.000000 / 5 | 2024 / 2024 | 3 / 3 |
| tsep | 23 / 0 | 30 / 0 | 0 / 0 | 0.000000 / 0 | 2024 / 2024 | 23 / 23 |
| frac | 3 / 0 | 4 / 0 | 5 / 0 | 5.123456 / 5 | 2024 / 2024 | 3 / 3 |
| off | 13 / 0 | 34 / 0 | 5 / 0 | 5.000000 / 5 | 2024 / 2023 | 13 / 13 |
| zulu | 19 / 0 | 4 / 0 | 5 / 0 | 5.000000 / 5 | 2024 / 2024 | 19 / 19 |
| lead | 3 / !datafusion engine error: | 4 / !datafusion engine error: | 5 / !datafusion engine error: | 5.000000 / 5 | 2024 / !datafusion engine error: | 3 / 3 |
| trail | 3 / !datafusion engine error: | 4 / !datafusion engine error: | 5 / !datafusion engine error: | 5.000000 / 5 | 2024 / !datafusion engine error: | 3 / 3 |
| empty | None / !datafusion engine error: | None / !datafusion engine error: | None / !datafusion engine error: | None / None | None / !datafusion engine error: | None / None |
| null | None / None | None / None | None / None | None / None | None / None | None / None |
| bad | None / !datafusion engine error: | None / !datafusion engine error: | None / !datafusion engine error: | None / None | None / !datafusion engine error: | None / None |
| nosec | 3 / !datafusion engine error: | 4 / !datafusion engine error: | 0 / !datafusion engine error: | 0.000000 / None | 2024 / !datafusion engine error: | 3 / 3 |
| tonly | 3 / !datafusion engine error: | 4 / !datafusion engine error: | 5 / !datafusion engine error: | 5.000000 / None | None / !datafusion engine error: | 3 / 3 |
| badtime | None / !datafusion engine error: | None / !datafusion engine error: | None / !datafusion engine error: | None / None | 2024 / !datafusion engine error: | None / None |
| gap | 3 / 0 | 30 / 0 | 0 / 0 | 0.000000 / 0 | 2024 / 2024 | 3 / 3 |
| yonly | 0 / !datafusion engine error: | 0 / !datafusion engine error: | 0 / !datafusion engine error: | 0.000000 / None | 2024 / !datafusion engine error: | 0 / 0 |

### a1-utc
| shape | hour | minute | second | extract_SECOND | year | cast_hour |
|---|---|---|---|---|---|---|
| date | 0 / 0 | 0 / 0 | 0 / 0 | 0.000000 / 0 | 2024 / 2024 | 0 / 0 |
| full | 3 / 0 | 4 / 0 | 5 / 0 | 5.000000 / 5 | 2024 / 2024 | 3 / 3 |
| tsep | 23 / 0 | 30 / 0 | 0 / 0 | 0.000000 / 0 | 2024 / 2024 | 23 / 23 |
| frac | 3 / 0 | 4 / 0 | 5 / 0 | 5.123456 / 5 | 2024 / 2024 | 3 / 3 |
| off | 21 / 0 | 34 / 0 | 5 / 0 | 5.000000 / 5 | 2024 / 2023 | 21 / 21 |
| zulu | 3 / 0 | 4 / 0 | 5 / 0 | 5.000000 / 5 | 2024 / 2024 | 3 / 3 |
| lead | 3 / !datafusion engine error: | 4 / !datafusion engine error: | 5 / !datafusion engine error: | 5.000000 / 5 | 2024 / !datafusion engine error: | 3 / 3 |
| trail | 3 / !datafusion engine error: | 4 / !datafusion engine error: | 5 / !datafusion engine error: | 5.000000 / 5 | 2024 / !datafusion engine error: | 3 / 3 |
| empty | !CAST_INVALID_INPUT / !datafusion engine error: | !CAST_INVALID_INPUT / !datafusion engine error: | !CAST_INVALID_INPUT / !datafusion engine error: | !CAST_INVALID_INPUT / !CAST_INVALID_INPUT | !CAST_INVALID_INPUT / !datafusion engine error: | !CAST_INVALID_INPUT / !CAST_INVALID_INPUT |
| null | None / None | None / None | None / None | None / None | None / None | None / None |
| bad | !CAST_INVALID_INPUT / !datafusion engine error: | !CAST_INVALID_INPUT / !datafusion engine error: | !CAST_INVALID_INPUT / !datafusion engine error: | !CAST_INVALID_INPUT / !CAST_INVALID_INPUT | !CAST_INVALID_INPUT / !datafusion engine error: | !CAST_INVALID_INPUT / !CAST_INVALID_INPUT |
| nosec | 3 / !datafusion engine error: | 4 / !datafusion engine error: | 0 / !datafusion engine error: | 0.000000 / !CAST_INVALID_INPUT | 2024 / !datafusion engine error: | 3 / 3 |
| tonly | 3 / !datafusion engine error: | 4 / !datafusion engine error: | 5 / !datafusion engine error: | 5.000000 / !CAST_INVALID_INPUT | !CAST_INVALID_INPUT / !datafusion engine error: | 3 / 3 |
| badtime | !CAST_INVALID_INPUT / !datafusion engine error: | !CAST_INVALID_INPUT / !datafusion engine error: | !CAST_INVALID_INPUT / !datafusion engine error: | !CAST_INVALID_INPUT / !CAST_INVALID_INPUT | 2024 / !datafusion engine error: | !CAST_INVALID_INPUT / !CAST_INVALID_INPUT |
| gap | 2 / 0 | 30 / 0 | 0 / 0 | 0.000000 / 0 | 2024 / 2024 | 2 / 2 |
| yonly | 0 / !datafusion engine error: | 0 / !datafusion engine error: | 0 / !datafusion engine error: | 0.000000 / !CAST_INVALID_INPUT | 2024 / !datafusion engine error: | 0 / 0 |

### a1-la
| shape | hour | minute | second | extract_SECOND | year | cast_hour |
|---|---|---|---|---|---|---|
| date | 0 / 0 | 0 / 0 | 0 / 0 | 0.000000 / 0 | 2024 / 2024 | 0 / 0 |
| full | 3 / 0 | 4 / 0 | 5 / 0 | 5.000000 / 5 | 2024 / 2024 | 3 / 3 |
| tsep | 23 / 0 | 30 / 0 | 0 / 0 | 0.000000 / 0 | 2024 / 2024 | 23 / 23 |
| frac | 3 / 0 | 4 / 0 | 5 / 0 | 5.123456 / 5 | 2024 / 2024 | 3 / 3 |
| off | 13 / 0 | 34 / 0 | 5 / 0 | 5.000000 / 5 | 2024 / 2023 | 13 / 13 |
| zulu | 19 / 0 | 4 / 0 | 5 / 0 | 5.000000 / 5 | 2024 / 2024 | 19 / 19 |
| lead | 3 / !datafusion engine error: | 4 / !datafusion engine error: | 5 / !datafusion engine error: | 5.000000 / 5 | 2024 / !datafusion engine error: | 3 / 3 |
| trail | 3 / !datafusion engine error: | 4 / !datafusion engine error: | 5 / !datafusion engine error: | 5.000000 / 5 | 2024 / !datafusion engine error: | 3 / 3 |
| empty | !CAST_INVALID_INPUT / !datafusion engine error: | !CAST_INVALID_INPUT / !datafusion engine error: | !CAST_INVALID_INPUT / !datafusion engine error: | !CAST_INVALID_INPUT / !CAST_INVALID_INPUT | !CAST_INVALID_INPUT / !datafusion engine error: | !CAST_INVALID_INPUT / !CAST_INVALID_INPUT |
| null | None / None | None / None | None / None | None / None | None / None | None / None |
| bad | !CAST_INVALID_INPUT / !datafusion engine error: | !CAST_INVALID_INPUT / !datafusion engine error: | !CAST_INVALID_INPUT / !datafusion engine error: | !CAST_INVALID_INPUT / !CAST_INVALID_INPUT | !CAST_INVALID_INPUT / !datafusion engine error: | !CAST_INVALID_INPUT / !CAST_INVALID_INPUT |
| nosec | 3 / !datafusion engine error: | 4 / !datafusion engine error: | 0 / !datafusion engine error: | 0.000000 / !CAST_INVALID_INPUT | 2024 / !datafusion engine error: | 3 / 3 |
| tonly | 3 / !datafusion engine error: | 4 / !datafusion engine error: | 5 / !datafusion engine error: | 5.000000 / !CAST_INVALID_INPUT | !CAST_INVALID_INPUT / !datafusion engine error: | 3 / 3 |
| badtime | !CAST_INVALID_INPUT / !datafusion engine error: | !CAST_INVALID_INPUT / !datafusion engine error: | !CAST_INVALID_INPUT / !datafusion engine error: | !CAST_INVALID_INPUT / !CAST_INVALID_INPUT | 2024 / !datafusion engine error: | !CAST_INVALID_INPUT / !CAST_INVALID_INPUT |
| gap | 3 / 0 | 30 / 0 | 0 / 0 | 0.000000 / 0 | 2024 / 2024 | 3 / 3 |
| yonly | 0 / !datafusion engine error: | 0 / !datafusion engine error: | 0 / !datafusion engine error: | 0.000000 / !CAST_INVALID_INPUT | 2024 / !datafusion engine error: | 0 / 0 |

### a1-utc-ntz
| shape | hour | minute | second | extract_SECOND | year | cast_hour |
|---|---|---|---|---|---|---|
| date | 0 / 0 | 0 / 0 | 0 / 0 | 0.000000 / 0 | 2024 / 2024 | 0 / 0 |
| full | 3 / 0 | 4 / 0 | 5 / 0 | 5.000000 / 5 | 2024 / 2024 | 3 / 3 |
| tsep | 23 / 0 | 30 / 0 | 0 / 0 | 0.000000 / 0 | 2024 / 2024 | 23 / 23 |
| frac | 3 / 0 | 4 / 0 | 5 / 0 | 5.123456 / 5 | 2024 / 2024 | 3 / 3 |
| off | 21 / 0 | 34 / 0 | 5 / 0 | 5.000000 / 5 | 2024 / 2023 | 3 / 21 |
| zulu | 3 / 0 | 4 / 0 | 5 / 0 | 5.000000 / 5 | 2024 / 2024 | 3 / 3 |
| lead | 3 / !datafusion engine error: | 4 / !datafusion engine error: | 5 / !datafusion engine error: | 5.000000 / 5 | 2024 / !datafusion engine error: | 3 / 3 |
| trail | 3 / !datafusion engine error: | 4 / !datafusion engine error: | 5 / !datafusion engine error: | 5.000000 / 5 | 2024 / !datafusion engine error: | 3 / 3 |
| empty | !CAST_INVALID_INPUT / !datafusion engine error: | !CAST_INVALID_INPUT / !datafusion engine error: | !CAST_INVALID_INPUT / !datafusion engine error: | !CAST_INVALID_INPUT / !CAST_INVALID_INPUT | !CAST_INVALID_INPUT / !datafusion engine error: | !CAST_INVALID_INPUT / !CAST_INVALID_INPUT |
| null | None / None | None / None | None / None | None / None | None / None | None / None |
| bad | !CAST_INVALID_INPUT / !datafusion engine error: | !CAST_INVALID_INPUT / !datafusion engine error: | !CAST_INVALID_INPUT / !datafusion engine error: | !CAST_INVALID_INPUT / !CAST_INVALID_INPUT | !CAST_INVALID_INPUT / !datafusion engine error: | !CAST_INVALID_INPUT / !CAST_INVALID_INPUT |
| nosec | 3 / !datafusion engine error: | 4 / !datafusion engine error: | 0 / !datafusion engine error: | 0.000000 / !CAST_INVALID_INPUT | 2024 / !datafusion engine error: | 3 / 3 |
| tonly | 3 / !datafusion engine error: | 4 / !datafusion engine error: | 5 / !datafusion engine error: | 5.000000 / !CAST_INVALID_INPUT | !CAST_INVALID_INPUT / !datafusion engine error: | !CAST_INVALID_INPUT / 3 |
| badtime | !CAST_INVALID_INPUT / !datafusion engine error: | !CAST_INVALID_INPUT / !datafusion engine error: | !CAST_INVALID_INPUT / !datafusion engine error: | !CAST_INVALID_INPUT / !CAST_INVALID_INPUT | 2024 / !datafusion engine error: | !CAST_INVALID_INPUT / !CAST_INVALID_INPUT |
| gap | 2 / 0 | 30 / 0 | 0 / 0 | 0.000000 / 0 | 2024 / 2024 | 2 / 2 |
| yonly | 0 / !datafusion engine error: | 0 / !datafusion engine error: | 0 / !datafusion engine error: | 0.000000 / !CAST_INVALID_INPUT | 2024 / !datafusion engine error: | 0 / 0 |

### a1-la-ntz
| shape | hour | minute | second | extract_SECOND | year | cast_hour |
|---|---|---|---|---|---|---|
| date | 0 / 0 | 0 / 0 | 0 / 0 | 0.000000 / 0 | 2024 / 2024 | 0 / 0 |
| full | 3 / 0 | 4 / 0 | 5 / 0 | 5.000000 / 5 | 2024 / 2024 | 3 / 3 |
| tsep | 23 / 0 | 30 / 0 | 0 / 0 | 0.000000 / 0 | 2024 / 2024 | 23 / 23 |
| frac | 3 / 0 | 4 / 0 | 5 / 0 | 5.123456 / 5 | 2024 / 2024 | 3 / 3 |
| off | 13 / 0 | 34 / 0 | 5 / 0 | 5.000000 / 5 | 2024 / 2023 | 3 / 13 |
| zulu | 19 / 0 | 4 / 0 | 5 / 0 | 5.000000 / 5 | 2024 / 2024 | 3 / 19 |
| lead | 3 / !datafusion engine error: | 4 / !datafusion engine error: | 5 / !datafusion engine error: | 5.000000 / 5 | 2024 / !datafusion engine error: | 3 / 3 |
| trail | 3 / !datafusion engine error: | 4 / !datafusion engine error: | 5 / !datafusion engine error: | 5.000000 / 5 | 2024 / !datafusion engine error: | 3 / 3 |
| empty | !CAST_INVALID_INPUT / !datafusion engine error: | !CAST_INVALID_INPUT / !datafusion engine error: | !CAST_INVALID_INPUT / !datafusion engine error: | !CAST_INVALID_INPUT / !CAST_INVALID_INPUT | !CAST_INVALID_INPUT / !datafusion engine error: | !CAST_INVALID_INPUT / !CAST_INVALID_INPUT |
| null | None / None | None / None | None / None | None / None | None / None | None / None |
| bad | !CAST_INVALID_INPUT / !datafusion engine error: | !CAST_INVALID_INPUT / !datafusion engine error: | !CAST_INVALID_INPUT / !datafusion engine error: | !CAST_INVALID_INPUT / !CAST_INVALID_INPUT | !CAST_INVALID_INPUT / !datafusion engine error: | !CAST_INVALID_INPUT / !CAST_INVALID_INPUT |
| nosec | 3 / !datafusion engine error: | 4 / !datafusion engine error: | 0 / !datafusion engine error: | 0.000000 / !CAST_INVALID_INPUT | 2024 / !datafusion engine error: | 3 / 3 |
| tonly | 3 / !datafusion engine error: | 4 / !datafusion engine error: | 5 / !datafusion engine error: | 5.000000 / !CAST_INVALID_INPUT | !CAST_INVALID_INPUT / !datafusion engine error: | !CAST_INVALID_INPUT / 3 |
| badtime | !CAST_INVALID_INPUT / !datafusion engine error: | !CAST_INVALID_INPUT / !datafusion engine error: | !CAST_INVALID_INPUT / !datafusion engine error: | !CAST_INVALID_INPUT / !CAST_INVALID_INPUT | 2024 / !datafusion engine error: | !CAST_INVALID_INPUT / !CAST_INVALID_INPUT |
| gap | 3 / 0 | 30 / 0 | 0 / 0 | 0.000000 / 0 | 2024 / 2024 | 2 / 3 |
| yonly | 0 / !datafusion engine error: | 0 / !datafusion engine error: | 0 / !datafusion engine error: | 0.000000 / !CAST_INVALID_INPUT | 2024 / !datafusion engine error: | 0 / 0 |

## G doors by function and door (each of 12 = 4 cfg x 3 in)
## G doors
| key | EQUAL | WRONG | RAISE | total |
|---|---|---|---|---|
| date_format_HH group_f | 0 | 12 | 0 | 12 |
| date_format_HH group_sql | 0 | 12 | 0 | 12 |
| date_format_HH order_f | 0 | 12 | 0 | 12 |
| date_format_HH order_sql | 0 | 12 | 0 | 12 |
| date_format_HH where_f | 0 | 12 | 0 | 12 |
| date_format_HH where_sql | 0 | 12 | 0 | 12 |
| date_part_MINUTE group_f | 0 | 6 | 6 | 12 |
| date_part_MINUTE group_sql | 0 | 0 | 12 | 12 |
| date_part_MINUTE order_f | 0 | 6 | 6 | 12 |
| date_part_MINUTE order_sql | 0 | 0 | 12 | 12 |
| date_part_MINUTE where_f | 0 | 6 | 6 | 12 |
| date_part_MINUTE where_sql | 0 | 0 | 12 | 12 |
| date_trunc_HOUR group_f | 0 | 12 | 0 | 12 |
| date_trunc_HOUR group_sql | 0 | 12 | 0 | 12 |
| date_trunc_HOUR order_f | 0 | 12 | 0 | 12 |
| date_trunc_HOUR order_sql | 0 | 12 | 0 | 12 |
| date_trunc_HOUR where_f | 0 | 12 | 0 | 12 |
| date_trunc_HOUR where_sql | 0 | 12 | 0 | 12 |
| dayofmonth group_f | 0 | 0 | 12 | 12 |
| dayofmonth group_sql | 0 | 0 | 12 | 12 |
| dayofmonth order_f | 0 | 0 | 12 | 12 |
| dayofmonth order_sql | 0 | 0 | 12 | 12 |
| dayofmonth where_f | 0 | 0 | 12 | 12 |
| dayofmonth where_sql | 0 | 0 | 12 | 12 |
| extract_HOUR group_f | 0 | 6 | 6 | 12 |
| extract_HOUR group_sql | 0 | 6 | 6 | 12 |
| extract_HOUR order_f | 0 | 6 | 6 | 12 |
| extract_HOUR order_sql | 0 | 6 | 6 | 12 |
| extract_HOUR where_f | 0 | 6 | 6 | 12 |
| extract_HOUR where_sql | 0 | 6 | 6 | 12 |
| extract_SECOND group_f | 0 | 6 | 6 | 12 |
| extract_SECOND group_sql | 0 | 6 | 6 | 12 |
| extract_SECOND order_f | 0 | 6 | 6 | 12 |
| extract_SECOND order_sql | 0 | 6 | 6 | 12 |
| extract_SECOND where_f | 0 | 6 | 6 | 12 |
| extract_SECOND where_sql | 0 | 6 | 6 | 12 |
| hour group_f | 0 | 0 | 12 | 12 |
| hour group_sql | 0 | 0 | 12 | 12 |
| hour order_f | 0 | 0 | 12 | 12 |
| hour order_sql | 0 | 0 | 12 | 12 |
| hour where_f | 0 | 0 | 12 | 12 |
| hour where_sql | 0 | 0 | 12 | 12 |
| minute group_f | 0 | 0 | 12 | 12 |
| minute group_sql | 0 | 0 | 12 | 12 |
| minute order_f | 0 | 0 | 12 | 12 |
| minute order_sql | 0 | 0 | 12 | 12 |
| minute where_f | 0 | 0 | 12 | 12 |
| minute where_sql | 0 | 0 | 12 | 12 |
| second group_f | 0 | 0 | 12 | 12 |
| second group_sql | 0 | 0 | 12 | 12 |
| second order_f | 0 | 0 | 12 | 12 |
| second order_sql | 0 | 0 | 12 | 12 |
| second where_f | 0 | 0 | 12 | 12 |
| second where_sql | 0 | 0 | 12 | 12 |
| unix_timestamp group_f | 12 | 0 | 0 | 12 |
| unix_timestamp group_sql | 6 | 6 | 0 | 12 |
| unix_timestamp order_f | 12 | 0 | 0 | 12 |
| unix_timestamp order_sql | 6 | 6 | 0 | 12 |
| unix_timestamp where_f | 12 | 0 | 0 | 12 |
| unix_timestamp where_sql | 6 | 6 | 0 | 12 |
| year group_f | 0 | 0 | 12 | 12 |
| year group_sql | 0 | 0 | 12 | 12 |
| year order_f | 0 | 0 | 12 | 12 |
| year order_sql | 0 | 0 | 12 | 12 |
| year where_f | 0 | 0 | 12 | 12 |
| year where_sql | 0 | 0 | 12 | 12 |

## Door spread: (cfg, in, shape, fn) groups whose main answers differ across doors
- cast_hour: 10; e.g. a0-la L date fcol=0 fexpr=16 sexpr=0 sql=0
- cast_ts: 10; e.g. a0-la L date fcol=2024-01-01 00:00:00 fexpr=2023-12-31 16:00:00 sexpr=2024-01-01 00:00:00 sql=2024-01-01 00:00:00
- date_part_DAY: 256; e.g. a0-utc L date fcol=1 fexpr=!Error during planni sexpr=!Error during planni sql=!Error during planni
- date_part_DAYOFWEEK: 256; e.g. a0-utc L date fcol=!Execution error: Da fexpr=!Error during planni sexpr=!Error during planni sql=!Error during planni
- date_part_DAYOFWEEK_ISO: 256; e.g. a0-utc L date fcol=!Execution error: Da fexpr=!Error during planni sexpr=!Error during planni sql=!Error during planni
- date_part_DOY: 256; e.g. a0-utc L date fcol=1 fexpr=!Error during planni sexpr=!Error during planni sql=!Error during planni
- date_part_HOUR: 256; e.g. a0-utc L date fcol=0 fexpr=!Error during planni sexpr=!Error during planni sql=!Error during planni
- date_part_MINUTE: 256; e.g. a0-utc L date fcol=0 fexpr=!Error during planni sexpr=!Error during planni sql=!Error during planni
- date_part_MONTH: 256; e.g. a0-utc L date fcol=1 fexpr=!Error during planni sexpr=!Error during planni sql=!Error during planni
- date_part_QUARTER: 256; e.g. a0-utc L date fcol=1 fexpr=!Error during planni sexpr=!Error during planni sql=!Error during planni
- date_part_S: 256; e.g. a0-utc L date fcol=0 fexpr=!Error during planni sexpr=!Error during planni sql=!Error during planni
- date_part_SECOND: 256; e.g. a0-utc L date fcol=0 fexpr=!Error during planni sexpr=!Error during planni sql=!Error during planni
- date_part_WEEK: 256; e.g. a0-utc L date fcol=1 fexpr=!Error during planni sexpr=!Error during planni sql=!Error during planni
- date_part_YEAR: 256; e.g. a0-utc L date fcol=2024 fexpr=!Error during planni sexpr=!Error during planni sql=!Error during planni
- date_part_YEAROFWEEK: 256; e.g. a0-utc L date fcol=!Execution error: Da fexpr=!Error during planni sexpr=!Error during planni sql=!Error during planni
- date_part_min: 256; e.g. a0-utc L date fcol=0 fexpr=!Error during planni sexpr=!Error during planni sql=!Error during planni
- extract_DAYOFWEEK_ISO: 256; e.g. a0-utc L date fcol=!Execution error: Da fexpr=!SQL error: ParserEr sexpr=!SQL error: ParserEr sql=!SQL error: ParserEr
- extract_H: 256; e.g. a0-utc L date fcol=0 fexpr=!SQL error: ParserEr sexpr=!SQL error: ParserEr sql=!SQL error: ParserEr
- extract_HRS: 256; e.g. a0-utc L date fcol=0 fexpr=!SQL error: ParserEr sexpr=!SQL error: ParserEr sql=!SQL error: ParserEr
- extract_YEAROFWEEK: 256; e.g. a0-utc L date fcol=!Execution error: Da fexpr=!SQL error: ParserEr sexpr=!SQL error: ParserEr sql=!SQL error: ParserEr
- unix_timestamp: 24; e.g. a0-utc L lead fcol=None fexpr=1704078245 sexpr=1704078245 sql=1704078245

