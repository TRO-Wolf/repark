import argparse
import importlib.metadata
import json
import re
import shutil
from pathlib import Path
from typing import Any

RAW = (
    Path(__file__).resolve().parents[3]
    / "followup-dml-conjunct-and-signed-zero/evidence/raw/repark-main-signed-zero-extension.jsonl"
)
ROWS = {"E1", "E3", "E6", "E7", "R1", "R2", "R3"}
COLS = "id BIGINT, i INT, d DOUBLE, flag STRING"
T = (
    "(1, 1, CAST('1.5' AS DOUBLE), 'f1')",
    "(2, NULL, NULL, 'f2')",
    "(3, NULL, CAST('NaN' AS DOUBLE), 'f3')",
    "(4, NULL, CAST('-0.0' AS DOUBLE), 'f4')",
    "(5, 2147483647, CAST('Infinity' AS DOUBLE), 'f5')",
    "(6, -1, CAST('0.0' AS DOUBLE), 'f6')",
    "(7, NULL, NULL, 'f7')",
    "(8, 1, CAST('1.5' AS DOUBLE), 'f8')",
)
U = ("(10, NULL, CAST('0.0' AS DOUBLE), 'u0')", "(11, NULL, CAST('-0.0' AS DOUBLE), 'u1')")
PROPS = {
    "v2cow": ("2", "copy-on-write"),
    "v2mor": ("2", "merge-on-read"),
    "v3": ("3", "copy-on-write"),
}


def create(door: Any, name: Any, slug: Any, extra: Any = "") -> Any:
    if door["name"] == "native":
        return f"CREATE TABLE {name} ({COLS})"
    version, mode = PROPS[slug]
    props = (
        f"'format-version' = '{version}', 'write.delete.mode' = '{mode}', "
        f"'write.update.mode' = '{mode}', 'write.merge.mode' = '{mode}'{extra}"
    )
    return f"CREATE TABLE {name} ({COLS}) USING iceberg TBLPROPERTIES ({props})"


def run(door: Any, text: Any) -> Any:
    frame = door["sql"](text)
    table = frame.toArrow() if hasattr(frame, "toArrow") else frame.to_arrow()
    return [
        list(r)
        for r in zip(*[table.column(i).to_pylist() for i in range(table.num_columns)], strict=True)
    ]


def show(value: Any) -> Any:
    if value is None:
        return None
    if isinstance(value, float):
        if value != value:
            return "NaN"
        if value in (float("inf"), float("-inf")):
            return "Infinity" if value > 0 else "-Infinity"
    return str(value)


def two_file(door: Any, name: Any, slug: Any) -> Any:
    run(door, create(door, name, slug))
    run(door, f"INSERT INTO {name} VALUES {T[3]}")
    run(door, f"INSERT INTO {name} VALUES {T[5]}")


def main() -> Any:
    parser = argparse.ArgumentParser()
    parser.add_argument("--label", required=True)
    parser.add_argument("--warehouse", required=True)
    parser.add_argument("--out", required=True)
    args = parser.parse_args()
    import repark
    from repark import ReparkSession
    from repark import functions as repark_functions
    from repark._native import PyReparkSession

    wh = Path(args.warehouse)
    if wh.exists():
        shutil.rmtree(wh)
    (wh / "native").mkdir(parents=True)
    (wh / "facade").mkdir(parents=True)
    native = PyReparkSession.native(config={"repark.sql.allowCreateFormatVersion3": "true"})
    native.register_memory_catalog("ice", str(wh / "native"))
    native.create_namespace("ice", "ns", str(wh / "native" / "ns"))
    repark._ANSI_NATIVE = native
    facade = (
        ReparkSession.builder.appName("sz-ext")
        .config("repark.sql.allowCreateFormatVersion3", "true")
        .config("spark.sql.session.timeZone", "America/New_York")
        .getOrCreate()
    )
    facade.register_memory_catalog("ice", str(wh / "facade"))
    facade_door = {"name": "spark-sql", "sql": facade.sql}
    native_door = {"name": "native", "sql": repark.sql}
    run(facade_door, f"CREATE NAMESPACE ice.ns LOCATION '{wh / 'facade' / 'ns'}'")
    for door in (facade_door, native_door):
        run(door, create(door, "ice.ns.x_t", "v2cow"))
        run(door, f"INSERT INTO ice.ns.x_t VALUES {', '.join(T)}")
        run(door, create(door, "ice.ns.x_u", "v2cow"))
        run(door, f"INSERT INTO ice.ns.x_u VALUES {', '.join(U)}")
        slugs = ("v2cow", "v2mor", "v3") if door is facade_door else ("v2cow",)
        for slug in slugs:
            short = {"v2cow": "cow", "v2mor": "mor", "v3": "v3"}[slug]
            two_file(door, f"ice.ns.x_two_{short}", slug)
    page = ", 'write.parquet.page-row-limit' = '1000', 'write.parquet.page-size-bytes' = '2048'"
    run(facade_door, create(facade_door, "ice.ns.x_pi", "v2cow", page))
    for lo, hi in ((1, 10001), (10001, 20001)):
        run(
            facade_door,
            "INSERT INTO ice.ns.x_pi SELECT id, CAST(NULL AS INT), CASE WHEN id <= 1000 THEN "
            "CAST('-0.0' AS DOUBLE) WHEN id > 19000 THEN CAST('0.0' AS DOUBLE) ELSE "
            f"CAST('1.5' AS DOUBLE) END, 'p' FROM range({lo}, {hi}) AS r(id)",
        )
    old = [json.loads(line) for line in RAW.read_text().splitlines() if line.strip()]
    out = []
    for rec in old:
        if rec.get("record") != "cell" or rec.get("row") not in ROWS:
            continue
        route, mode, text = rec["route"], rec["mode"], rec["text"]
        door = native_door if route == "native" else facade_door
        answer, error = None, None
        try:
            if mode == "dml":
                table = re.search(r"(ice\.ns\.x_dml_\w+)", text).group(1)
                slug = re.search(r"_(v2cow|v2mor|v3)_", table).group(1)
                two_file(door, table, slug)
                run(door, text)
                answer = run(door, f"SELECT id, i, d, flag FROM {table} ORDER BY id")
            elif route in ("sql", "native"):
                answer = run(door, text)
            else:
                frame = facade.table("ice.ns.x_t")
                if route == "filter-str":
                    result = frame.filter(text).select("id")
                elif route == "f-expr":
                    result = frame.filter(repark_functions.expr(text)).select("id")
                else:
                    result = frame.filter(eval(text, {"F": repark_functions})).select("id")
                table = result.toArrow()
                answer = [
                    list(r)
                    for r in zip(
                        *[table.column(i).to_pylist() for i in range(table.num_columns)],
                        strict=True,
                    )
                ]
        except BaseException as exc:
            error = {
                "class": type(exc).__name__,
                "message": str(exc).replace(str(wh), "<warehouse>")[:300],
            }
        rows = None if answer is None else [[show(v) for v in r] for r in answer]
        out.append(
            {
                "record": "cell",
                "cid": rec["cid"],
                "row": rec["row"],
                "route": route,
                "mode": mode,
                "text": text,
                "outcome": "ANSWERED" if error is None else "REFUSED",
                "answer": rows,
                "error": error,
                "old_outcome": rec["outcome"],
                "old_answer": rec["answer"],
            }
        )
    with Path(args.out).open("w") as handle:
        handle.write(
            json.dumps(
                {
                    "record": "leg",
                    "label": args.label,
                    "repark_version": importlib.metadata.version("repark"),
                }
            )
            + "\n"
        )
        for r in out:
            handle.write(json.dumps(r, default=str) + "\n")
    print("cells", len(out))


if __name__ == "__main__":
    main()
