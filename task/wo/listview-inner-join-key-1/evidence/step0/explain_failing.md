### join|LV_I64|df_str|inner|base (CROSS-PRODUCT)
```
== Optimized Logical Plan ==
SubqueryAlias: datafusion.public.__repark_explain_4656b177d790416e83a3d4df4eba6e52
  Projection: _repark_jl_53a32418e594.lid AS lid, _repark_jr_b900cba64796.rid AS rid
    Cross Join:
      SubqueryAlias: _repark_jl_53a32418e594
        SubqueryAlias: datafusion.public.lv_l
          TableScan: ?table? projection=[lid]
      SubqueryAlias: _repark_jr_b900cba64796
        SubqueryAlias: datafusion.public.lv_r
          TableScan: ?table? projection=[rid]

== Physical Plan ==
ProjectionExec: expr=[lid@1 as lid, rid@0 as rid]
  CrossJoinExec
    CoalescePartitionsExec
      UnionExec
        ProjectionExec: expr=[10 as rid]
          PlaceholderRowExec
        ProjectionExec: expr=[11 as rid]
          PlaceholderRowExec
        ProjectionExec: expr=[12 as rid]
          PlaceholderRowExec
        ProjectionExec: expr=[13 as rid]
          PlaceholderRowExec
        ProjectionExec: expr=[14 as rid]
          PlaceholderRowExec
        ProjectionExec: expr=[15 as rid]
          PlaceholderRowExec
        ProjectionExec: expr=[16 as rid]
          PlaceholderRowExec
    UnionExec
      ProjectionExec: expr=[1 as lid]
        PlaceholderRowExec
      ProjectionExec: expr=[2 as lid]
        PlaceholderRowExec
      ProjectionExec: expr=[3 as lid]
        PlaceholderRowExec
      ProjectionExec: expr=[4 as lid]
        PlaceholderRowExec
      ProjectionExec: expr=[5 as lid]
        PlaceholderRowExec
      ProjectionExec: expr=[6 as lid]
        PlaceholderRowExec
      ProjectionExec: expr=[7 as lid]
        PlaceholderRowExec
      ProjectionExec: expr=[8 as lid]
        PlaceholderRowExec

```

### join|LLV_I64|df_str|inner|base (CROSS-PRODUCT)
```
== Optimized Logical Plan ==
SubqueryAlias: datafusion.public.__repark_explain_c9795e00d2014eb4bf81a4b06184813d
  Projection: _repark_jl_facc7ee69bc5.lid AS lid, _repark_jr_22cd90f0ba43.rid AS rid
    Cross Join:
      SubqueryAlias: _repark_jl_facc7ee69bc5
        SubqueryAlias: datafusion.public.lv_l
          TableScan: ?table? projection=[lid]
      SubqueryAlias: _repark_jr_22cd90f0ba43
        SubqueryAlias: datafusion.public.lv_r
          TableScan: ?table? projection=[rid]

== Physical Plan ==
ProjectionExec: expr=[lid@1 as lid, rid@0 as rid]
  CrossJoinExec
    CoalescePartitionsExec
      UnionExec
        ProjectionExec: expr=[10 as rid]
          PlaceholderRowExec
        ProjectionExec: expr=[11 as rid]
          PlaceholderRowExec
        ProjectionExec: expr=[12 as rid]
          PlaceholderRowExec
        ProjectionExec: expr=[13 as rid]
          PlaceholderRowExec
        ProjectionExec: expr=[14 as rid]
          PlaceholderRowExec
        ProjectionExec: expr=[15 as rid]
          PlaceholderRowExec
        ProjectionExec: expr=[16 as rid]
          PlaceholderRowExec
    UnionExec
      ProjectionExec: expr=[1 as lid]
        PlaceholderRowExec
      ProjectionExec: expr=[2 as lid]
        PlaceholderRowExec
      ProjectionExec: expr=[3 as lid]
        PlaceholderRowExec
      ProjectionExec: expr=[4 as lid]
        PlaceholderRowExec
      ProjectionExec: expr=[5 as lid]
        PlaceholderRowExec
      ProjectionExec: expr=[6 as lid]
        PlaceholderRowExec
      ProjectionExec: expr=[7 as lid]
        PlaceholderRowExec
      ProjectionExec: expr=[8 as lid]
        PlaceholderRowExec

```

### join|LV_UTF8|df_str|inner|base (CROSS-PRODUCT)
```
== Optimized Logical Plan ==
SubqueryAlias: datafusion.public.__repark_explain_e879c0a02cd549c6a0e8a53efdc0f42a
  Projection: _repark_jl_bd28d7a055b7.lid AS lid, _repark_jr_7c99a603ff3c.rid AS rid
    Cross Join:
      SubqueryAlias: _repark_jl_bd28d7a055b7
        SubqueryAlias: datafusion.public.lv_l
          TableScan: ?table? projection=[lid]
      SubqueryAlias: _repark_jr_7c99a603ff3c
        SubqueryAlias: datafusion.public.lv_r
          TableScan: ?table? projection=[rid]

== Physical Plan ==
ProjectionExec: expr=[lid@1 as lid, rid@0 as rid]
  CrossJoinExec
    CoalescePartitionsExec
      UnionExec
        ProjectionExec: expr=[10 as rid]
          PlaceholderRowExec
        ProjectionExec: expr=[11 as rid]
          PlaceholderRowExec
        ProjectionExec: expr=[12 as rid]
          PlaceholderRowExec
        ProjectionExec: expr=[13 as rid]
          PlaceholderRowExec
        ProjectionExec: expr=[14 as rid]
          PlaceholderRowExec
        ProjectionExec: expr=[15 as rid]
          PlaceholderRowExec
        ProjectionExec: expr=[16 as rid]
          PlaceholderRowExec
    UnionExec
      ProjectionExec: expr=[1 as lid]
        PlaceholderRowExec
      ProjectionExec: expr=[2 as lid]
        PlaceholderRowExec
      ProjectionExec: expr=[3 as lid]
        PlaceholderRowExec
      ProjectionExec: expr=[4 as lid]
        PlaceholderRowExec
      ProjectionExec: expr=[5 as lid]
        PlaceholderRowExec
      ProjectionExec: expr=[6 as lid]
        PlaceholderRowExec
      ProjectionExec: expr=[7 as lid]
        PlaceholderRowExec
      ProjectionExec: expr=[8 as lid]
        PlaceholderRowExec

```

### join|LLV_UTF8|df_str|inner|base (CROSS-PRODUCT)
```
== Optimized Logical Plan ==
SubqueryAlias: datafusion.public.__repark_explain_2b8b56d3429643a79c27de9c2fa1fd82
  Projection: _repark_jl_802692aa4b85.lid AS lid, _repark_jr_df6ef4004abf.rid AS rid
    Cross Join:
      SubqueryAlias: _repark_jl_802692aa4b85
        SubqueryAlias: datafusion.public.lv_l
          TableScan: ?table? projection=[lid]
      SubqueryAlias: _repark_jr_df6ef4004abf
        SubqueryAlias: datafusion.public.lv_r
          TableScan: ?table? projection=[rid]

== Physical Plan ==
ProjectionExec: expr=[lid@1 as lid, rid@0 as rid]
  CrossJoinExec
    CoalescePartitionsExec
      UnionExec
        ProjectionExec: expr=[10 as rid]
          PlaceholderRowExec
        ProjectionExec: expr=[11 as rid]
          PlaceholderRowExec
        ProjectionExec: expr=[12 as rid]
          PlaceholderRowExec
        ProjectionExec: expr=[13 as rid]
          PlaceholderRowExec
        ProjectionExec: expr=[14 as rid]
          PlaceholderRowExec
        ProjectionExec: expr=[15 as rid]
          PlaceholderRowExec
        ProjectionExec: expr=[16 as rid]
          PlaceholderRowExec
    UnionExec
      ProjectionExec: expr=[1 as lid]
        PlaceholderRowExec
      ProjectionExec: expr=[2 as lid]
        PlaceholderRowExec
      ProjectionExec: expr=[3 as lid]
        PlaceholderRowExec
      ProjectionExec: expr=[4 as lid]
        PlaceholderRowExec
      ProjectionExec: expr=[5 as lid]
        PlaceholderRowExec
      ProjectionExec: expr=[6 as lid]
        PlaceholderRowExec
      ProjectionExec: expr=[7 as lid]
        PlaceholderRowExec
      ProjectionExec: expr=[8 as lid]
        PlaceholderRowExec

```

### join|LV_STRUCT|df_str|inner|base (CROSS-PRODUCT)
```
== Optimized Logical Plan ==
SubqueryAlias: datafusion.public.__repark_explain_5909b62b074246788eee5caa9712edc4
  Projection: _repark_jl_f34d4a05f0aa.lid AS lid, _repark_jr_7a6a22a799c7.rid AS rid
    Cross Join:
      SubqueryAlias: _repark_jl_f34d4a05f0aa
        SubqueryAlias: datafusion.public.lv_l
          TableScan: ?table? projection=[lid]
      SubqueryAlias: _repark_jr_7a6a22a799c7
        SubqueryAlias: datafusion.public.lv_r
          TableScan: ?table? projection=[rid]

== Physical Plan ==
ProjectionExec: expr=[lid@1 as lid, rid@0 as rid]
  CrossJoinExec
    CoalescePartitionsExec
      UnionExec
        ProjectionExec: expr=[10 as rid]
          PlaceholderRowExec
        ProjectionExec: expr=[11 as rid]
          PlaceholderRowExec
        ProjectionExec: expr=[12 as rid]
          PlaceholderRowExec
        ProjectionExec: expr=[13 as rid]
          PlaceholderRowExec
        ProjectionExec: expr=[14 as rid]
          PlaceholderRowExec
        ProjectionExec: expr=[15 as rid]
          PlaceholderRowExec
        ProjectionExec: expr=[16 as rid]
          PlaceholderRowExec
    UnionExec
      ProjectionExec: expr=[1 as lid]
        PlaceholderRowExec
      ProjectionExec: expr=[2 as lid]
        PlaceholderRowExec
      ProjectionExec: expr=[3 as lid]
        PlaceholderRowExec
      ProjectionExec: expr=[4 as lid]
        PlaceholderRowExec
      ProjectionExec: expr=[5 as lid]
        PlaceholderRowExec
      ProjectionExec: expr=[6 as lid]
        PlaceholderRowExec
      ProjectionExec: expr=[7 as lid]
        PlaceholderRowExec
      ProjectionExec: expr=[8 as lid]
        PlaceholderRowExec

```

### join|LLV_STRUCT|df_str|inner|base (CROSS-PRODUCT)
```
== Optimized Logical Plan ==
SubqueryAlias: datafusion.public.__repark_explain_0c30f572ed1e490ea6ca89f25163f682
  Projection: _repark_jl_6c2aad6dbf43.lid AS lid, _repark_jr_0f059fd42aa9.rid AS rid
    Cross Join:
      SubqueryAlias: _repark_jl_6c2aad6dbf43
        SubqueryAlias: datafusion.public.lv_l
          TableScan: ?table? projection=[lid]
      SubqueryAlias: _repark_jr_0f059fd42aa9
        SubqueryAlias: datafusion.public.lv_r
          TableScan: ?table? projection=[rid]

== Physical Plan ==
ProjectionExec: expr=[lid@1 as lid, rid@0 as rid]
  CrossJoinExec
    CoalescePartitionsExec
      UnionExec
        ProjectionExec: expr=[10 as rid]
          PlaceholderRowExec
        ProjectionExec: expr=[11 as rid]
          PlaceholderRowExec
        ProjectionExec: expr=[12 as rid]
          PlaceholderRowExec
        ProjectionExec: expr=[13 as rid]
          PlaceholderRowExec
        ProjectionExec: expr=[14 as rid]
          PlaceholderRowExec
        ProjectionExec: expr=[15 as rid]
          PlaceholderRowExec
        ProjectionExec: expr=[16 as rid]
          PlaceholderRowExec
    UnionExec
      ProjectionExec: expr=[1 as lid]
        PlaceholderRowExec
      ProjectionExec: expr=[2 as lid]
        PlaceholderRowExec
      ProjectionExec: expr=[3 as lid]
        PlaceholderRowExec
      ProjectionExec: expr=[4 as lid]
        PlaceholderRowExec
      ProjectionExec: expr=[5 as lid]
        PlaceholderRowExec
      ProjectionExec: expr=[6 as lid]
        PlaceholderRowExec
      ProjectionExec: expr=[7 as lid]
        PlaceholderRowExec
      ProjectionExec: expr=[8 as lid]
        PlaceholderRowExec

```

### join|NEST_LV|df_str|inner|base (CROSS-PRODUCT)
```
== Optimized Logical Plan ==
SubqueryAlias: datafusion.public.__repark_explain_b6faa6a2d2ba4dfe990070e6ead5030a
  Projection: _repark_jl_5fc0381fea58.lid AS lid, _repark_jr_a77fa36a8fd6.rid AS rid
    Cross Join:
      SubqueryAlias: _repark_jl_5fc0381fea58
        SubqueryAlias: datafusion.public.lv_l
          TableScan: ?table? projection=[lid]
      SubqueryAlias: _repark_jr_a77fa36a8fd6
        SubqueryAlias: datafusion.public.lv_r
          TableScan: ?table? projection=[rid]

== Physical Plan ==
ProjectionExec: expr=[lid@1 as lid, rid@0 as rid]
  CrossJoinExec
    CoalescePartitionsExec
      UnionExec
        ProjectionExec: expr=[10 as rid]
          PlaceholderRowExec
        ProjectionExec: expr=[11 as rid]
          PlaceholderRowExec
        ProjectionExec: expr=[12 as rid]
          PlaceholderRowExec
        ProjectionExec: expr=[13 as rid]
          PlaceholderRowExec
        ProjectionExec: expr=[14 as rid]
          PlaceholderRowExec
        ProjectionExec: expr=[15 as rid]
          PlaceholderRowExec
        ProjectionExec: expr=[16 as rid]
          PlaceholderRowExec
    UnionExec
      ProjectionExec: expr=[1 as lid]
        PlaceholderRowExec
      ProjectionExec: expr=[2 as lid]
        PlaceholderRowExec
      ProjectionExec: expr=[3 as lid]
        PlaceholderRowExec
      ProjectionExec: expr=[4 as lid]
        PlaceholderRowExec
      ProjectionExec: expr=[5 as lid]
        PlaceholderRowExec
      ProjectionExec: expr=[6 as lid]
        PlaceholderRowExec
      ProjectionExec: expr=[7 as lid]
        PlaceholderRowExec
      ProjectionExec: expr=[8 as lid]
        PlaceholderRowExec

```

