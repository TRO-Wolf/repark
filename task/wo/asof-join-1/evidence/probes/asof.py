from decimal import Decimal
from typing import Any

LEFT_DDL = "lid INT, k1 STRING, k2 INT, s STRING, lv STRING"
RIGHT_DDL = "rid INT, k1 STRING, k2 INT, s STRING, rv STRING"

LEFT = [
    (1, "a", 1, 2),
    (2, "a", 1, 8),
    (3, "a", 1, 9),
    (4, "a", 1, 11),
    (5, "a", 1, 12),
    (6, "a", 1, 16),
    (7, "a", 1, 20),
    (8, "a", 1, None),
    (9, "b", 1, 5),
    (10, "b", 1, 7),
    (11, "a", 2, 9),
    (12, None, 1, 10),
    (13, "c", 1, 10),
    (14, "a", None, 11),
]
RIGHT = [
    (1, "a", 1, 4, "r4"),
    (2, "a", 1, 8, "r8"),
    (3, "a", 1, 10, "r10"),
    (4, "a", 1, 13, "r13"),
    (5, "a", 1, 16, "r16a"),
    (6, "a", 1, 16, "r16b"),
    (7, "a", 1, None, "rnull"),
    (8, "b", 1, 5, "rb5"),
    (9, "a", 2, 9, "ra2"),
    (10, None, 1, 10, "rnk"),
    (11, "a", None, 11, "rnk2"),
]


def text(typ: str, p: Any) -> str | None:
    if p is None:
        return None
    if typ.startswith("TIMESTAMP"):
        return f"2024-01-01 00:00:{p:02d}"
    if typ == "DATE":
        return f"2024-01-{p:02d}"
    return str(p)


def frames(s: Any, f: Any, ltyp: str, rtyp: str, lrows: Any = None, rrows: Any = None) -> Any:
    lrows = LEFT if lrows is None else lrows
    rrows = RIGHT if rrows is None else rrows
    ldata = [(i, a, b, text(ltyp, p), "l" + str(i)) for i, a, b, p in lrows]
    rdata = [(i, a, b, text(rtyp, p), v) for i, a, b, p, v in rrows]
    left = s.createDataFrame(ldata, LEFT_DDL)
    right = s.createDataFrame(rdata, RIGHT_DDL)
    left = left.select("lid", "k1", "k2", f.col("s").cast(ltyp).alias("t"), "lv")
    right = right.select("rid", "k1", "k2", f.col("s").cast(rtyp).alias("t"), "rv")
    return left, right


def tol_lit(f: Any, typ: str, n: Any) -> Any:
    if n is None:
        return None
    if typ.startswith("TIMESTAMP"):
        return f.expr(f"INTERVAL {n} SECONDS")
    if typ == "DOUBLE":
        return f.lit(float(n))
    if typ.startswith("DECIMAL"):
        return f.lit(Decimal(n))
    return f.lit(n)


def asof_cond(f: Any, lt: Any, rt: Any, tol: Any, exact: bool, direction: str) -> Any:
    if direction == "backward":
        base = lt >= rt if exact else lt > rt
    elif direction == "forward":
        base = lt <= rt if exact else lt < rt
    else:
        base = f.lit(True) if exact else ~(lt == rt)
    if tol is None:
        return base
    if direction == "backward":
        return base & ((rt >= lt - tol) if exact else (rt > lt - tol))
    if direction == "forward":
        return base & ((rt <= lt + tol) if exact else (rt < lt + tol))
    if exact:
        return (rt >= lt - tol) & (rt <= lt + tol)
    return base & ((rt > lt - tol) & (rt < lt + tol))


def direct(left: Any, right: Any, on: Any, how: str, tol: Any, exact: bool, direction: str) -> Any:
    return left._joinAsOf(
        right,
        leftAsOfColumn="t",
        rightAsOfColumn="t",
        on=on,
        how=how,
        tolerance=tol,
        allowExactMatches=exact,
        direction=direction,
    )


def renamed(f: Any, right: Any) -> Any:
    return right.select(*[f.col(c).alias("r_" + c) for c in right.columns])


def w1(
    f: Any,
    window: Any,
    left: Any,
    right: Any,
    keys: list[str],
    how: str,
    tol: Any,
    exact: bool,
    direction: str,
) -> Any:
    names = list(left.columns) + list(right.columns)
    rgt = renamed(f, right)
    lt, rt = f.col("t"), f.col("r_t")
    cond = asof_cond(f, lt, rt, tol, exact, direction) & lt.isNotNull() & rt.isNotNull()
    for k in keys:
        cond = cond & (f.col(k) == f.col("r_" + k))
    joined = left.join(rgt, cond, how)
    if direction == "backward":
        order = rt.desc_nulls_last()
    elif direction == "forward":
        order = rt.asc_nulls_last()
    else:
        order = f.when(lt > rt, lt - rt).otherwise(rt - lt).asc_nulls_last()
    spec = window.partitionBy("lid").orderBy(order)
    ranked = joined.withColumn("__rn", f.row_number().over(spec))
    return ranked.where(f.col("__rn") == 1).drop("__rn").toDF(*names)


def fill_window(window: Any, keys: list[str], order: str, following: bool) -> Any:
    spec = window.partitionBy(*keys).orderBy("__t", order) if keys else window.orderBy("__t", order)
    if following:
        return spec.rowsBetween(window.currentRow, window.unboundedFollowing)
    return spec.rowsBetween(window.unboundedPreceding, window.currentRow)


def w2(
    f: Any,
    window: Any,
    left: Any,
    right: Any,
    keys: list[str],
    how: str,
    tol: Any,
    exact: bool,
    direction: str,
) -> Any:
    names = list(left.columns) + list(right.columns)
    rgt = renamed(f, right).withColumn("__ri", f.col("r_rid").cast("bigint"))
    lcols = [f.col(c).alias("l_" + c) for c in left.columns]
    nulls = [f.lit(None).cast(kind).alias("l_" + c) for c, kind in left.dtypes]
    kl = [f.col(k).alias("__k_" + k) for k in keys]
    kr = [f.col("r_" + k).alias("__k_" + k) for k in keys]
    lpart = left.select(
        *lcols,
        *kl,
        f.col("t").alias("__t"),
        f.lit(None).cast("bigint").alias("__ri"),
        f.lit(0).alias("__side"),
    )
    keep = f.col("r_t").isNotNull()
    for k in keys:
        keep = keep & f.col("r_" + k).isNotNull()
    rpart = rgt.where(keep).select(
        *nulls, *kr, f.col("r_t").alias("__t"), "__ri", f.lit(1).alias("__side")
    )
    side = f.col("__side")
    u = lpart.unionByName(rpart)
    u = u.withColumn("__sb", (1 - side) if exact else side).withColumn(
        "__sf", side if exact else (1 - side)
    )
    pk = ["__k_" + k for k in keys]
    rri = f.when(side == 1, f.col("__ri"))
    rtt = f.when(side == 1, f.col("__t"))
    wb = fill_window(window, pk, "__sb", False)
    wf = fill_window(window, pk, "__sf", True)
    u = u.withColumn("__bri", f.last(rri, ignorenulls=True).over(wb)).withColumn(
        "__bt", f.last(rtt, ignorenulls=True).over(wb)
    )
    u = u.withColumn("__fri", f.first(rri, ignorenulls=True).over(wf)).withColumn(
        "__ft", f.first(rtt, ignorenulls=True).over(wf)
    )
    t, bt, ft = f.col("__t"), f.col("__bt"), f.col("__ft")
    ok_b = bt.isNotNull()
    ok_f = ft.isNotNull()
    if tol is not None:
        ok_b = ok_b & ((bt >= t - tol) if exact else (bt > t - tol))
        ok_f = ok_f & ((ft <= t + tol) if exact else (ft < t + tol))
    if direction == "backward":
        pick = f.when(ok_b, f.col("__bri"))
    elif direction == "forward":
        pick = f.when(ok_f, f.col("__fri"))
    else:
        near_b = ok_b & (~ok_f | ((t - bt) <= (ft - t)))
        pick = f.when(near_b, f.col("__bri")).when(ok_f, f.col("__fri"))
    pick = f.when(t.isNotNull(), pick)
    lefts = u.where(side == 0).select(*["l_" + c for c in left.columns], pick.alias("__pick"))
    if how == "inner":
        lefts = lefts.where(f.col("__pick").isNotNull())
    out = lefts.join(rgt, f.col("__pick") == f.col("__ri"), "left")
    return out.select(*["l_" + c for c in left.columns], *["r_" + c for c in right.columns]).toDF(
        *names
    )
