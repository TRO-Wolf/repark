from __future__ import annotations

import argparse
import json
import re
import shutil
from collections.abc import Callable
from pathlib import Path
from typing import Any

LAYOUTS: dict[str, list[tuple[str, Any]]] = {
    "L01_sub1_sub2": [
        ("sub1/a=1/part-0.parquet", [(1, "a")]),
        ("sub2/a=2/part-0.parquet", [(2, "b")]),
    ],
    "L02_root_and_sub": [
        ("a=1/part-0.parquet", [(1, "a")]),
        ("sub/a=2/part-0.parquet", [(2, "b")]),
    ],
    "L03_part_sub_file": [("p=1/good.parquet", [(1, "a")]), ("sub/x.parquet", [(99, "s")])],
    "L04_part_root_file": [("p=1/good.parquet", [(1, "a")]), ("root.parquet", [(99, "r")])],
    "L05_part_sub_hidden": [("p=1/good.parquet", [(1, "a")]), ("sub/_hidden.parquet", [(99, "h")])],
    "L06_part_temporary": [("p=1/good.parquet", [(1, "a")]), ("_temporary/x.parquet", [(99, "t")])],
    "L07_part_temporary_deep": [
        ("p=1/good.parquet", [(1, "a")]),
        ("_temporary/0/x.parquet", [(99, "t")]),
    ],
    "L08_a_sub_b_alone": [("a=1/sub/b=2/part-0.parquet", [(1, "a")])],
    "L09_a_sub_b_same": [
        ("a=1/sub/b=2/part-0.parquet", [(1, "a")]),
        ("a=1/sub/b=3/part-0.parquet", [(2, "b")]),
    ],
    "L10_a_sub_b_diff": [
        ("a=1/sub/b=2/part-0.parquet", [(1, "a")]),
        ("a=2/sub/b=3/part-0.parquet", [(2, "b")]),
    ],
    "L11_deep_same": [
        ("s1/s2/a=1/part-0.parquet", [(1, "a")]),
        ("s1/s2/a=2/part-0.parquet", [(2, "b")]),
    ],
    "L12_deep_diff_depth": [
        ("s1/s2/a=1/part-0.parquet", [(1, "a")]),
        ("s1/a=2/part-0.parquet", [(2, "b")]),
    ],
    "L13_deep_trailing": [
        ("a=1/s1/s2/part-0.parquet", [(1, "a")]),
        ("a=2/s1/part-0.parquet", [(2, "b")]),
    ],
    "L14_deep_mid": [
        ("a=1/s1/b=1/s2/part-0.parquet", [(1, "a")]),
        ("a=1/s1/b=2/s2/part-0.parquet", [(2, "b")]),
    ],
    "L15_base_file": [("s1/a=1/part-0.parquet", [(1, "a")]), ("s1/x.parquet", [(99, "x")])],
    "L16_base_sub_file": [("s1/a=1/part-0.parquet", [(1, "a")]), ("s1/s2/x.parquet", [(99, "x")])],
    "L17_case_base": [
        ("Sub/a=1/part-0.parquet", [(1, "a")]),
        ("sub/a=2/part-0.parquet", [(2, "b")]),
    ],
    "L18_names_vs_dirs": [
        ("a=1/part-0.parquet", [(1, "a")]),
        ("sub/b=2/part-0.parquet", [(2, "b")]),
    ],
    "L19_mixed_depth": [("a=1/b=1/part-0.parquet", [(1, "a")]), ("a=2/part-0.parquet", [(2, "b")])],
    "L20_sub_only_parts": [
        ("sub/a=1/part-0.parquet", [(1, "a")]),
        ("sub/a=2/part-0.parquet", [(2, "b")]),
    ],
    "L21_part_sub_txt": [("p=1/good.parquet", [(1, "a")]), ("sub/notes.txt", b"hello")],
    "L22_part_sub_zero": [("p=1/good.parquet", [(1, "a")]), ("sub/empty.parquet", b"")],
    "L23_three_bases": [
        ("x/a=1/part-0.parquet", [(1, "a")]),
        ("y/a=2/part-0.parquet", [(2, "b")]),
        ("z/a=3/part-0.parquet", [(3, "c")]),
    ],
    "L24_part_trailing_sub_and_root": [
        ("a=1/part-0.parquet", [(1, "a")]),
        ("a=2/sub/part-0.parquet", [(2, "b")]),
    ],
    "L25_hidden_dot_sub": [("p=1/good.parquet", [(1, "a")]), (".sub/x.parquet", [(99, "d")])],
    "L26_unpart_sub1_sub2_root": [
        ("root.parquet", [(1, "r")]),
        ("sub1/a.parquet", [(2, "s")]),
        ("sub2/b.parquet", [(3, "t")]),
    ],
    "Z01_root_first": [("empty.parquet", b""), ("p=1/good.parquet", [(1, "a")])],
    "Z02_root_last": [("zzz.parquet", b""), ("p=1/good.parquet", [(1, "a")])],
    "Z03_root_nonempty_first": [
        ("a.parquet", [(99, "r")]),
        ("empty.parquet", b""),
        ("p=1/good.parquet", [(1, "a")]),
    ],
    "Z04_part_later": [("p=1/good.parquet", [(1, "a")]), ("p=2/empty.parquet", b"")],
    "Z05_part_same_later": [
        ("p=1/a.parquet", [(1, "a")]),
        ("p=1/z.parquet", b""),
        ("p=2/b.parquet", [(2, "b")]),
    ],
    "Z06_part_first": [("p=1/empty.parquet", b""), ("p=2/good.parquet", [(2, "b")])],
    "Z07_case_order": [("p=1/Z.parquet", b""), ("p=1/a.parquet", [(1, "a")])],
    "Z08_unpart_last": [("good.parquet", [(1, "a")]), ("zzz.parquet", b"")],
    "Z09_unpart_first": [("empty.parquet", b""), ("good.parquet", [(1, "a")])],
    "Z10_unpart_nested_zero_first": [("a/x.parquet", b""), ("b.parquet", [(1, "a")])],
    "Z11_unpart_hidden_last": [
        ("_h.parquet", [(99, "h")]),
        ("good.parquet", [(1, "a")]),
        ("zzz.parquet", b""),
    ],
    "Z12_part_all_zero": [("p=1/empty.parquet", b"")],
    "Z13_root_nonempty_part_zero": [("a.parquet", [(99, "r")]), ("p=1/empty.parquet", b"")],
    "Z14_unpart_case_order": [("Z.parquet", b""), ("a.parquet", [(1, "a")])],
    "Z15_two_parts_zero_mid": [
        ("p=1/a.parquet", [(1, "a")]),
        ("p=1/m.parquet", b""),
        ("p=2/a.parquet", [(2, "b")]),
    ],
    "Z16_part_upper_root": [("A.parquet", b""), ("p=1/good.parquet", [(1, "a")])],
    "Z17_unpart_hidden_first": [
        ("_h.parquet", [(99, "h")]),
        ("empty.parquet", b""),
        ("good.parquet", [(1, "a")]),
    ],
    "Z18_zero_leaf_types": [("p=1/good.parquet", [(1, "a")]), ("p=x/empty.parquet", b"")],
    "Z19_names_before_zero": [("p=1/empty.parquet", b""), ("q=2/good.parquet", [(2, "b")])],
    "Z20_dirs_before_zero": [("a=2/empty.parquet", b""), ("sub/a=1/good.parquet", [(1, "a")])],
    "Z21_zero_leaf_only_value": [
        ("p=1/good.parquet", [(1, "a")]),
        ("p=2/empty.parquet", b""),
        ("p=3/good.parquet", [(3, "c")]),
    ],
}

PRUNE_FIXTURES: dict[str, list[tuple[str, Any]]] = {
    "s1x": [("p=1/part-0.parquet", [(1, "one")]), ("p=x/part-0.parquet", [])],
    "s1xn": [("p=1/part-0.parquet", [(1, "one")]), ("p=x/part-0.parquet", [(2, "bad")])],
    "i10": [("p=1/part-0.parquet", [(1, "one")]), ("p=0/part-0.parquet", [])],
    "iovf": [("p=1/part-0.parquet", [(1, "one")]), ("p=2147483647/part-0.parquet", [])],
    "dbad": [("p=2024-01-02/part-0.parquet", [(1, "ok")]), ("p=notadate/part-0.parquet", [])],
    "ab": [
        ("a=1/b=1/part-0.parquet", [(1, "one")]),
        ("a=x/b=1/part-0.parquet", []),
        ("a=1/b=2/part-0.parquet", [(3, "three")]),
    ],
    "sub_parts": [("sub/a=1/part-0.parquet", [(1, "a")]), ("sub/a=2/part-0.parquet", [(2, "b")])],
}

STR_PREDICATES: list[tuple[str, list[str]]] = [
    ("P01_cast_and_eq", ["CAST(p AS INT) = 1 AND p = '1'"]),
    ("P02_chain_cast_then_eq", ["CAST(p AS INT) = 1", "p = '1'"]),
    ("P03_cast_and_eq99", ["CAST(p AS INT) = 1 AND p = '99'"]),
    ("P04_nullsafe", ["CAST(p AS INT) = 1 AND p <=> '1'"]),
    ("P05_nullsafe_flipped", ["CAST(p AS INT) = 1 AND '1' <=> p"]),
    ("P06_in_single", ["CAST(p AS INT) = 1 AND p IN ('1')"]),
    ("P07_not_ne", ["CAST(p AS INT) = 1 AND NOT (p <> '1')"]),
    ("P08_not_eq_x", ["CAST(p AS INT) = 1 AND NOT (p = 'x')"]),
    ("P09_binding_under_or", ["CAST(p AS INT) = 1 AND (p = '1' OR p = '2')"]),
    ("P10_and_inside_or", ["(CAST(p AS INT) = 1 AND p = '1') OR p = '2'"]),
    ("P11_two_bindings_last_2", ["CAST(p AS INT) = 1 AND p = '1' AND p = '2'"]),
    ("P12_two_bindings_last_1", ["p = '2' AND p = '1' AND CAST(p AS INT) = 1"]),
    ("P13_upper_cast_col", ["CAST(P AS INT) = 1 AND p = '1'"]),
    ("P14_upper_binding_col", ["CAST(p AS INT) = 1 AND P = '1'"]),
    ("P15_case_sibling", ["CASE WHEN CAST(p AS INT) = 1 THEN true ELSE false END AND p = '1'"]),
    ("P16_fold_raise_no_match", ["CAST(p AS INT) = 1 AND p = 'y'"]),
    ("P17_fold_raise_after_binding", ["p = 'y' AND CAST(p AS INT) = 1"]),
    ("P18_in_list_before_binding", ["CAST(p AS INT) IN (1, 2) AND p = '1'"]),
    ("P19_false_prefix_then_raise", ["p <> 'x' AND CAST(p AS INT) = 1 AND p = 'x'"]),
    ("P20a_guard_ne", ["CAST(p AS INT) = 1 AND p != 'x'"]),
    ("P20b_guard_in_two", ["CAST(p AS INT) = 1 AND p IN ('1','2')"]),
    ("P20c_guard_like", ["CAST(p AS INT) = 1 AND p LIKE '1%'"]),
    ("P21_eq_then_cast", ["p = '1' AND CAST(p AS INT) = 1"]),
    ("P22_cast_alone", ["CAST(p AS INT) = 1"]),
    ("P23_cast_bool_sibling", ["CAST(p AS BOOLEAN) AND p = '1'"]),
    ("P24_chain_eq_then_cast", ["p = '1'", "CAST(p AS INT) = 1"]),
    ("P25_chain_three", ["CAST(p AS INT) = 1", "id >= 0", "p = '1'"]),
    ("P26_eq_then_cast_with_data", ["CAST(p AS INT) = 1 AND id >= 0 AND p = '1'"]),
    ("P27_explicit_not_eq_prefix", ["NOT (p = 'x') AND CAST(p AS INT) = 1 AND p = 'x'"]),
    ("P28_cast_bool_cmp_sibling", ["CAST(p AS BOOLEAN) = true AND p = '1'"]),
    ("P29_data_binding_into_part", ["CAST(p AS INT) = id AND id = 1"]),
    ("P60_nullsafe_ne_prefix", ["NOT (p <=> 'x') AND CAST(p AS INT) = 1 AND p <=> 'x'"]),
]

INT_PREDICATES: list[tuple[str, str, list[str]]] = [
    ("P30_div_and_eq", "i10", ["1 / p = 1 AND p = 1"]),
    ("P31_div_and_eq_strlit", "i10", ["1 / p = 1 AND p = '1'"]),
    ("P32_ovf_and_eq", "iovf", ["p + 1 > 0 AND p = 1"]),
    ("P33_div_alone", "i10", ["1 / p = 1"]),
    ("P34_div_and_in_two", "i10", ["1 / p = 1 AND p IN (1, 2)"]),
    ("P35_eq_then_div", "i10", ["p = 1 AND 1 / p = 1"]),
    ("P36_div_and_eq_flipped", "i10", ["1 / p = 1 AND 1 = p"]),
    ("P40_date_and_eq", "dbad", ["CAST(p AS DATE) = DATE '2024-01-02' AND p = '2024-01-02'"]),
    ("P41_date_alone", "dbad", ["CAST(p AS DATE) = DATE '2024-01-02'"]),
    ("P50_guard_two_cols", "ab", ["CAST(a AS INT) = 1 AND b = 1"]),
    ("P51_two_col_bindings", "ab", ["CAST(a AS INT) = 1 AND a = '1' AND b = 1"]),
    ("P52_two_col_sum", "ab", ["CAST(a AS INT) + b = 2 AND a = '1' AND b = 1"]),
    ("P53_two_col_bindings_reorder", "ab", ["CAST(a AS INT) = 1 AND b = 1 AND a = '1'"]),
    ("P54_partial_substitution", "ab", ["CAST(a AS INT) = b AND a = '1'"]),
    ("P55_sub_base_prune", "sub_parts", ["a = 2"]),
]


def write_parquet(path: Path, rows: list[tuple[int, str]]) -> None:
    import pyarrow as pa
    import pyarrow.parquet as pq

    path.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(
        pa.table(
            {
                "id": pa.array([r[0] for r in rows], type=pa.int64()),
                "s": pa.array([r[1] for r in rows], type=pa.string()),
            }
        ),
        path,
    )


def make_fixtures(root: Path) -> None:
    if root.exists():
        shutil.rmtree(root)
    root.mkdir(parents=True)
    for group in (LAYOUTS, PRUNE_FIXTURES):
        for name, entries in group.items():
            for rel, data in entries:
                target = root / name / rel
                if isinstance(data, bytes):
                    target.parent.mkdir(parents=True, exist_ok=True)
                    target.write_bytes(data)
                else:
                    write_parquet(target, data)


def enc_err(exc: BaseException) -> dict[str, Any]:
    text = str(exc)
    first = re.sub(r"calling o\d+\.", "calling oN.", text.splitlines()[0] if text else "")
    match = re.search(r"\[([A-Z0-9_.]+)\]", text)
    cond = match.group(1) if match else None
    if hasattr(exc, "getCondition"):
        try:
            got = exc.getCondition()
            if got:
                cond = got
        except Exception:
            pass
    full = re.sub(r"/[^ '\n]*/fx/", "<fx>/", text)
    return {
        "status": "err",
        "type": type(exc).__name__,
        "condition": cond,
        "message": first[:300],
        "full": full[:900],
    }


def norm(value: Any) -> Any:
    return [
        type(value).__name__,
        value if isinstance(value, (int, float, str, bool, type(None))) else str(value),
    ]


def run_cell(fn: Callable[[], Any]) -> dict[str, Any]:
    try:
        df = fn()
        rows = sorted(([norm(v) for v in r] for r in df.collect()), key=repr)
        return {"status": "ok", "schema": df.schema.simpleString(), "rows": rows}
    except BaseException as exc:
        return enc_err(exc)


def new_session(engine: str, name: str, ansi: str) -> Any:
    if engine == "spark":
        from pyspark.sql import SparkSession

        prev = SparkSession.getActiveSession()
        if prev is not None:
            prev.stop()
        return (
            SparkSession.builder.master("local[2]")
            .appName(name)
            .config("spark.ui.enabled", "false")
            .config("spark.sql.shuffle.partitions", "2")
            .config("spark.sql.ansi.enabled", ansi)
            .config("spark.sql.session.timeZone", "UTC")
            .getOrCreate()
        )
    from repark.spark import ReparkSession

    prev = ReparkSession.getActiveSession()
    if prev is not None:
        prev.stop()
    return (
        ReparkSession.builder.appName(name)
        .config("spark.sql.ansi.enabled", ansi)
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )


def read_door(spark: Any, door: str, path: str) -> Any:
    return (
        spark.read.parquet(path) if door == "parquet" else spark.read.format("parquet").load(path)
    )


def chain(df: Any, preds: list[str]) -> Any:
    for pred in preds:
        df = df.where(pred)
    return df


def run_views(spark: Any, fixtures: Path, ansi: str, res: dict[str, Any]) -> None:
    for fx in ("s1x", "s1xn"):
        for vname, inner, outer in (
            ("V1_view_cast_outer_eq", "CAST(p AS INT) = 1", "p = '1'"),
            ("V2_view_eq_outer_cast", "p = '1'", "CAST(p AS INT) = 1"),
            ("V3_view_cast_outer_eq99", "CAST(p AS INT) = 1", "p = '99'"),
        ):
            key = f"VIEW/{vname}/{fx}/ansi={ansi}"
            try:
                spark.read.parquet(str(fixtures / fx)).createOrReplaceTempView("t_d2")
                spark.sql(f"CREATE OR REPLACE TEMP VIEW v_d2 AS SELECT * FROM t_d2 WHERE {inner}")
                res[key] = run_cell(lambda o=outer: spark.sql(f"SELECT * FROM v_d2 WHERE {o}"))
            except BaseException as exc:
                res[key] = enc_err(exc)
        key = f"VIEW/V4_df_view_cast_outer_eq/{fx}/ansi={ansi}"
        try:
            spark.read.parquet(str(fixtures / fx)).where(
                "CAST(p AS INT) = 1"
            ).createOrReplaceTempView("w_d2")
            res[key] = run_cell(lambda: spark.sql("SELECT * FROM w_d2 WHERE p = '1'"))
        except BaseException as exc:
            res[key] = enc_err(exc)


def run_once(engine: str, fixtures: Path) -> dict[str, Any]:
    res: dict[str, Any] = {}
    for ansi in ("true", "false"):
        spark = new_session(engine, f"probe-d2-{ansi}", ansi)
        for door in ("parquet", "load"):
            for name in LAYOUTS:
                res[f"LAYOUT/{name}/{door}/ansi={ansi}"] = run_cell(
                    lambda d=door, n=name, sp=spark: read_door(sp, d, str(fixtures / n))
                )
            for fx in ("s1x", "s1xn"):
                for pname, preds in STR_PREDICATES:
                    res[f"PRUNE/{pname}/{fx}/{door}/ansi={ansi}"] = run_cell(
                        lambda d=door, f=fx, p=preds, sp=spark: chain(
                            read_door(sp, d, str(fixtures / f)), p
                        )
                    )
            for pname, fx, preds in INT_PREDICATES:
                res[f"PRUNE/{pname}/{fx}/{door}/ansi={ansi}"] = run_cell(
                    lambda d=door, f=fx, p=preds, sp=spark: chain(
                        read_door(sp, d, str(fixtures / f)), p
                    )
                )
        res[f"SINGLE/zero_file/ansi={ansi}"] = run_cell(
            lambda sp=spark: sp.read.parquet(
                str(fixtures / "Z12_part_all_zero" / "p=1" / "empty.parquet")
            )
        )
        run_views(spark, fixtures, ansi, res)
        spark.stop()
    return res


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--engine", choices=["make-fixtures", "spark", "repark"], required=True)
    parser.add_argument("--fixtures", required=True)
    parser.add_argument("--out")
    parser.add_argument("--runs", type=int, default=1)
    args = parser.parse_args()
    fixtures = Path(args.fixtures)
    if args.engine == "make-fixtures":
        make_fixtures(fixtures)
        print("PROBE-D2-FIXTURES-OK")
        return
    if args.engine == "repark":
        import repark._native

        print("REPARK_NATIVE:", repark._native.__file__)
    runs = [run_once(args.engine, fixtures) for _ in range(args.runs)]
    nondet = sorted(k for k in runs[0] if any(r.get(k) != runs[0][k] for r in runs[1:]))
    Path(args.out).write_text(
        json.dumps({"nondet": nondet, "run1": runs[0]}, indent=2, sort_keys=True), encoding="utf-8"
    )
    print(f"DONE engine={args.engine} cells={len(runs[0])} nondet={len(nondet)} {nondet[:5]}")


if __name__ == "__main__":
    main()
