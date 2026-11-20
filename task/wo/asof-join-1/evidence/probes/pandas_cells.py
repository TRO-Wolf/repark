import json
import sys
import time
from functools import reduce
from typing import Any

import asof
from common import Rec, condition, modules, session, text_of

DIRS = ("backward", "forward", "nearest")


def collect_ps(frame: Any) -> dict[str, Any]:
    sdf = frame.to_spark()
    rows = sdf.collect()
    return {
        "rows": sorted(json.dumps([text_of(v) for v in row]) for row in rows),
        "names": list(sdf.columns),
        "types": [kind for _, kind in sdf.dtypes],
    }


def run_ps(rec: Rec, key: dict[str, Any], build: Any) -> None:
    start = time.perf_counter()
    try:
        got = collect_ps(build())
    except Exception as error:
        got = {"error": condition(error)}
    got["ms"] = round((time.perf_counter() - start) * 1000, 1)
    rec.emit({**key, **got})


def joined(
    f: Any,
    left: Any,
    right: Any,
    lkeys: list[str],
    rkeys: list[str],
    tol: Any,
    exact: bool,
    direction: str,
    rt: str = "t",
) -> Any:
    on = None
    if lkeys:
        on = reduce(
            lambda a, b: a & b, [left[a] == right[b] for a, b in zip(lkeys, rkeys, strict=True)]
        )
    return left._joinAsOf(
        right,
        leftAsOfColumn=left["t"],
        rightAsOfColumn=right[rt],
        on=on,
        how="left",
        tolerance=tol,
        allowExactMatches=exact,
        direction=direction,
    )


def cells(rec: Rec, s: Any, ansi: str) -> None:
    import pandas as pd
    import pyspark.pandas as ps

    f, _ = modules("spark")
    left, right = asof.frames(s, f, "BIGINT", "BIGINT")
    rby = right.withColumnRenamed("k1", "rk1")
    rrt = right.withColumnRenamed("t", "rt")
    pl, pr, prby, prrt = left.pandas_api(), right.pandas_api(), rby.pandas_api(), rrt.pandas_api()
    bys: dict[str, tuple[dict[str, Any], Any, Any, list[str], list[str]]] = {
        "none": ({}, pr, right, [], []),
        "by_k1": ({"by": "k1"}, pr, right, ["k1"], ["k1"]),
        "by_k1k2": ({"by": ["k1", "k2"]}, pr, right, ["k1", "k2"], ["k1", "k2"]),
        "left_right_by": ({"left_by": "k1", "right_by": "rk1"}, prby, rby, ["k1"], ["rk1"]),
    }
    for bname, (kw, psr, sr, lk, rk) in bys.items():
        for direction in DIRS:
            for exact in (True, False):
                for tn in (None, 2):
                    key = {
                        "g": "P",
                        "ansi": ansi,
                        "case": bname,
                        "dir": direction,
                        "exact": exact,
                        "tol": tn,
                    }
                    run_ps(
                        rec,
                        {**key, "surf": "P"},
                        lambda kw=kw, psr=psr, d=direction, e=exact, t=tn: ps.merge_asof(
                            pl, psr, on="t", tolerance=t, allow_exact_matches=e, direction=d, **kw
                        ),
                    )
                    tol = None if tn is None else f.lit(tn)
                    rec.run(
                        {**key, "surf": "PD"},
                        lambda sr=sr, lk=lk, rk=rk, tol=tol, d=direction, e=exact: joined(
                            f, left, sr, lk, rk, tol, e, d
                        ),
                    )
    for sname, suffixes in (("default", ("_x", "_y")), ("custom", ("_l", "_r"))):
        key = {
            "g": "P",
            "ansi": ansi,
            "case": "suffix_" + sname,
            "dir": "backward",
            "exact": True,
            "tol": None,
        }
        run_ps(
            rec,
            {**key, "surf": "P"},
            lambda sfx=suffixes: ps.merge_asof(pl, pr, on="t", by="k1", suffixes=sfx),
        )
    for direction in DIRS:
        key = {
            "g": "P",
            "ansi": ansi,
            "case": "left_on_right_on",
            "dir": direction,
            "exact": True,
            "tol": None,
        }
        run_ps(
            rec,
            {**key, "surf": "P"},
            lambda d=direction: ps.merge_asof(
                pl, prrt, left_on="t", right_on="rt", by="k1", direction=d
            ),
        )
        rec.run(
            {**key, "surf": "PD"},
            lambda d=direction: joined(f, left, rrt, ["k1"], ["k1"], None, True, d, "rt"),
        )
    tl, tr = asof.frames(s, f, "TIMESTAMP", "TIMESTAMP")
    ptl, ptr = tl.pandas_api(), tr.pandas_api()
    for direction in DIRS:
        for exact in (True, False):
            key = {
                "g": "P",
                "ansi": ansi,
                "case": "timestamp_timedelta",
                "dir": direction,
                "exact": exact,
                "tol": 2,
            }
            run_ps(
                rec,
                {**key, "surf": "P"},
                lambda d=direction, e=exact: ps.merge_asof(
                    ptl,
                    ptr,
                    on="t",
                    by="k1",
                    tolerance=pd.Timedelta(seconds=2),
                    allow_exact_matches=e,
                    direction=d,
                ),
            )
            rec.run(
                {**key, "surf": "PD"},
                lambda d=direction, e=exact: joined(
                    f, tl, tr, ["k1"], ["k1"], f.expr("INTERVAL 2 SECONDS"), e, d
                ),
            )
    errors: dict[str, Any] = {
        "on_and_left_on": lambda: ps.merge_asof(pl, pr, on="t", left_on="t", right_on="t"),
        "by_and_left_by": lambda: ps.merge_asof(
            pl, pr, on="t", by="k1", left_by="k1", right_by="k1"
        ),
        "left_by_only": lambda: ps.merge_asof(pl, prby, on="t", left_by="k1"),
        "two_on_keys": lambda: ps.merge_asof(pl, pr, on=["t", "k2"]),
        "tolerance_negative": lambda: ps.merge_asof(pl, pr, on="t", by="k1", tolerance=-2),
        "tolerance_string": lambda: ps.merge_asof(pl, pr, on="t", by="k1", tolerance="2"),
        "direction_bad": lambda: ps.merge_asof(pl, pr, on="t", by="k1", direction="sideways"),
        "no_common_columns": lambda: ps.merge_asof(pl[["lid"]], pr[["rid"]]),
    }
    for name, build in errors.items():
        run_ps(
            rec,
            {
                "g": "P",
                "ansi": ansi,
                "case": name,
                "dir": "",
                "exact": True,
                "tol": None,
                "surf": "P",
            },
            build,
        )


def main() -> None:
    out, ansi = sys.argv[1:3]
    s = session("spark", ansi)
    rec = Rec(out)
    cells(rec, s, ansi)
    if ansi == "true":
        import pyspark.pandas as ps

        ps.set_option("compute.fail_on_ansi_mode", False)
        left, right = asof.frames(s, modules("spark")[0], "BIGINT", "BIGINT")
        key = {
            "g": "P",
            "ansi": ansi,
            "case": "fail_on_ansi_mode_off",
            "dir": "backward",
            "exact": True,
            "tol": None,
            "surf": "P",
        }
        run_ps(
            rec, key, lambda: ps.merge_asof(left.pandas_api(), right.pandas_api(), on="t", by="k1")
        )
    print("DONE P", flush=True)


if __name__ == "__main__":
    main()
