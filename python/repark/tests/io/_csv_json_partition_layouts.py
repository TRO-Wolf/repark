from __future__ import annotations

import datetime
import json
from collections.abc import Callable
from pathlib import Path
from typing import Any
from urllib.parse import unquote

FORMATS = ("csvh", "csvn", "json")
EXTENSION = {"csvh": ".csv", "csvn": ".csv", "json": ".json"}
ROWS = {"p=1": [(1, "a"), (2, "b")], "p=2": [(3, "c"), (4, "d")]}
TWO_LEVEL = {
    "year=2024/month=1": [(1, "a")],
    "year=2024/month=2": [(2, "b")],
    "year=2025/month=1": [(3, "c")],
}
INFER_KEY = "spark.sql.sources.partitionColumnTypeInference.enabled"


def _body(fmt: str, rows: list[tuple[int, str]]) -> str:
    if fmt == "json":
        return "".join(json.dumps({"id": key, "s": text}) + "\n" for key, text in rows)
    header = "id,s\n" if fmt == "csvh" else ""
    return header + "".join(f"{key},{text}\n" for key, text in rows)


def _place(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")


def _table(root: Path, fmt: str, dirs: dict[str, list[tuple[int, str]]]) -> None:
    for index, (rel, rows) in enumerate(dirs.items()):
        _place(root / rel / f"part-{index:05d}{EXTENSION[fmt]}", _body(fmt, rows))
    _place(root / "_SUCCESS", "")


def build(base: Path) -> None:
    for fmt in FORMATS:
        root = base / fmt
        ext = EXTENSION[fmt]
        _table(root / "one_level", fmt, ROWS)
        _table(root / "two_level", fmt, TWO_LEVEL)
        _table(
            root / "hive_default",
            fmt,
            {"p=__HIVE_DEFAULT_PARTITION__": [(1, "a")], "p=1": [(2, "b")]},
        )
        _table(root / "dates", fmt, {"p=2024-01-02": [(1, "a")], "p=2024-07-15": [(2, "b")]})
        _table(root / "strings", fmt, {"p=alpha": [(1, "a")], "p=beta": [(2, "b")]})
        _table(root / "conflict", fmt, {"a=1": [(1, "a")], "b=2": [(2, "b")]})
        _table(root / "mixed_case", fmt, {"p=1": [(1, "a")], "P=2": [(2, "b")]})
        _table(root / "hidden", fmt, ROWS)
        _table(root / "zero", fmt, ROWS)
        _place(root / "zero" / "p=3" / f"empty{ext}", "")
        _place(root / "hidden" / "_temporary" / "0" / f"part-t{ext}", _body(fmt, [(9, "z")]))
        _place(root / "hidden" / f".hidden{ext}", _body(fmt, [(8, "y")]))
        _place(root / "hidden" / "p=1" / f"_under{ext}", _body(fmt, [(7, "x")]))
        _place(root / "hidden" / "p=1" / f".dot{ext}", _body(fmt, [(6, "w")]))
        _place(root / "flat" / f"part-00000{ext}", _body(fmt, ROWS["p=1"]))
        _place(root / "flat" / f"part-00001{ext}", _body(fmt, ROWS["p=2"]))
        _place(root / "eq_root" / "tag=foo" / f"part-00000{ext}", _body(fmt, ROWS["p=1"]))


def _reader(spark: Any, fmt: str, conf: dict[str, str]) -> Any:
    reader = spark.read
    if fmt != "json":
        reader = reader.option("header", "true" if fmt == "csvh" else "false")
    if "schema" in conf:
        reader = reader.schema(conf["schema"])
    return reader


def read(spark: Any, base: Path, case: dict[str, Any]) -> Any:
    fmt = case["fmt"]
    path = str(base / fmt / case["path"])
    reader = _reader(spark, fmt, case.get("conf", {}))
    kind = "json" if fmt == "json" else "csv"
    if case.get("door") == "load":
        return reader.format(kind).load(path)
    return reader.json(path) if kind == "json" else reader.csv(path)


def _value(value: Any) -> Any:
    if isinstance(value, datetime.date):
        return value.isoformat()
    return value


def observe(frame: Any) -> dict[str, Any]:
    schema = [[field.name, field.dataType.simpleString()] for field in frame.schema.fields]
    rows = sorted(([_value(cell) for cell in row] for row in frame.collect()), key=repr)
    return {"schema": schema, "rows": rows}


def run(spark: Any, base: Path, work: Path, case: dict[str, Any]) -> dict[str, Any]:
    spark.conf.set("spark.sql.ansi.enabled", case["ansi"])
    for key, value in case.get("conf", {}).get("session", {}).items():
        spark.conf.set(key, value)
    try:
        frame = read(spark, base, case)
        query = case["query"]
        if query == "where":
            return observe(frame.where("p = 2"))
        if query == "count":
            return {"count": frame.count()}
        if query == "files":
            names = [Path(unquote(name)) for name in frame.inputFiles()]
            return {"files": sorted(f"{name.parent.name}/{name.name}" for name in names)}
        if query == "roundtrip":
            out = work / case["id"].replace("/", "_")
            frame.write.mode("overwrite").parquet(str(out))
            return observe(spark.read.parquet(str(out)))
        return observe(frame)
    except Exception as error:
        getter: Callable[[], Any] | None = getattr(error, "getCondition", None)
        return {"error": getter() if getter is not None else type(error).__name__}
    finally:
        spark.conf.set(INFER_KEY, "true")
        spark.conf.set("spark.sql.caseSensitive", "false")
        spark.conf.set("spark.sql.ansi.enabled", "true")


def cases() -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    target_paths = (
        "one_level",
        "two_level",
        "hive_default",
        "dates",
        "strings",
        "hidden",
        "two_level/year=2024",
    )
    for ansi in ("true", "false"):
        for fmt in FORMATS:
            for path in target_paths:
                for query in ("read", "where", "roundtrip"):
                    out.append(
                        {"fmt": fmt, "path": path, "query": query, "ansi": ansi, "kind": "target"}
                    )
            out.append(
                {
                    "fmt": fmt,
                    "path": "one_level",
                    "query": "read",
                    "door": "load",
                    "ansi": ansi,
                    "kind": "target",
                }
            )
            out.append(
                {"fmt": fmt, "path": "conflict", "query": "read", "ansi": ansi, "kind": "target"}
            )
            for path in ("zero", "hidden"):
                out.append(
                    {"fmt": fmt, "path": path, "query": "files", "ansi": ansi, "kind": "target"}
                )
            out.append(
                {
                    "fmt": fmt,
                    "path": "one_level",
                    "query": "read",
                    "ansi": ansi,
                    "kind": "target",
                    "conf": {"session": {INFER_KEY: "false"}},
                }
            )
            out.append(
                {
                    "fmt": fmt,
                    "path": "mixed_case",
                    "query": "read",
                    "ansi": ansi,
                    "kind": "target",
                    "conf": {"session": {"spark.sql.caseSensitive": "true"}},
                }
            )
            for path in ("flat", "one_level/p=1", "eq_root/tag=foo"):
                for query in ("read", "count", "roundtrip"):
                    out.append(
                        {
                            "fmt": fmt,
                            "path": path,
                            "query": query,
                            "ansi": ansi,
                            "kind": "protected",
                        }
                    )
            out.append(
                {
                    "fmt": fmt,
                    "path": "flat",
                    "query": "read",
                    "door": "load",
                    "ansi": ansi,
                    "kind": "protected",
                }
            )
            out.append(
                {
                    "fmt": fmt,
                    "path": "one_level",
                    "query": "count",
                    "ansi": ansi,
                    "kind": "protected",
                }
            )
        for ddl in (
            "p INT, id BIGINT, s STRING",
            "id BIGINT, s STRING",
            "id BIGINT, s STRING, p STRING",
        ):
            out.append(
                {
                    "fmt": "json",
                    "path": "one_level",
                    "query": "read",
                    "ansi": ansi,
                    "kind": "target",
                    "conf": {"schema": ddl},
                }
            )
    for case in out:
        parts = [
            case["kind"],
            case["fmt"],
            case.get("door", "read"),
            case["path"],
            case["query"],
            f"ansi={case['ansi']}",
        ]
        conf = case.get("conf", {})
        if "schema" in conf:
            parts.append("schema=" + conf["schema"].replace(" ", "_"))
        parts.extend(
            f"{key.rsplit('.', 1)[-1]}={value}" for key, value in conf.get("session", {}).items()
        )
        case["id"] = "/".join(parts)
    return out
