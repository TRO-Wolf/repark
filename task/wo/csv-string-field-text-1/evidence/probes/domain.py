import sys
from collections.abc import Callable
from pathlib import Path
from typing import Any

from common import Rec, condition, session

SHAPES: dict[str, list[str]] = {
    "plain": ["42"],
    "lead0": ["007"],
    "plus": ["+5"],
    "frac": ["1.50"],
    "exp": ["1e3"],
    "expneg": ["1E-2"],
    "dot": ["5."],
    "upper": ["TRUE"],
    "title": ["True"],
    "yes": ["yes"],
    "space": [" 7 "],
    "empty": [""],
    "nullv": ["NA", "007"],
    "quoted": ['"007"'],
    "bad": ["abc"],
    "ovf32": ["2147483648"],
    "ovf64": ["99999999999999999999"],
    "date": ["2024-01-15"],
    "ts": ["2024-01-15 10:30:00"],
}
N1_ROWS = ["1e3,2.0", "1.5,7", "3,4"]
TYPES = [
    "STRING",
    "INT",
    "BIGINT",
    "DECIMAL(10,2)",
    "DOUBLE",
    "BOOLEAN",
    "DATE",
    "TIMESTAMP",
    "TIMESTAMP_NTZ",
]
TS_TEXT = "2024-01-01 00:00:00"
COMPS = ("alone", "beside")
HDRS = ("on", "off")
LABEL_TYPES = ["STRING", "INT", "DECIMAL(10,2)", "DOUBLE", "BOOLEAN", "DATE"]


def companion(comp: str) -> tuple[str, str, str]:
    if comp == "beside":
        return "ts", TS_TEXT, "TIMESTAMP"
    return "k", "k", "STRING"


def gen(data: Path) -> None:
    data.mkdir(parents=True, exist_ok=True)
    for comp in COMPS:
        name, text, _ = companion(comp)
        for hdr in HDRS:
            head = [f"x,{name}"] if hdr == "on" else []
            for shape, values in SHAPES.items():
                lines = head + [f"{value},{text}" for value in values]
                (data / f"{shape}-{comp}-{hdr}.csv").write_text("\n".join(lines) + "\n")
            n1_head = [f"a,b,{name}"] if hdr == "on" else []
            n1 = n1_head + [f"{row},{text}" for row in N1_ROWS]
            (data / f"n1-{comp}-{hdr}.csv").write_text("\n".join(n1) + "\n")


def schema_for(shape: str, kind: str, comp: str) -> str:
    name, _, ctype = companion(comp)
    if shape == "n1":
        return f"a INT, b BIGINT, {name} {ctype}"
    return f"x {kind}, {name} {ctype}"


def opts_for(read: str, hdr: str, shape: str, mode: str | None = None) -> dict[str, str]:
    opts: dict[str, str] = {}
    if hdr == "on":
        opts["header"] = "true"
    if read == "F":
        opts["inferSchema"] = "false"
    if read == "T":
        opts["inferSchema"] = "true"
    if shape == "nullv":
        opts["nullValue"] = "NA"
    if mode is not None:
        opts["mode"] = mode
    return opts


def frame_for(s: Any, door: str, path: str, schema: str | None, opts: dict[str, str]) -> Any:
    if door == "sql":
        return s.sql(f"SELECT * FROM csv.`{path}`")
    reader = s.read.format("csv") if door == "load" else s.read
    if schema is not None:
        reader = reader.schema(schema)
    for key, value in opts.items():
        reader = reader.option(key, value)
    frame = reader.load(path) if door == "load" else reader.csv(path)
    if door == "view":
        frame.createOrReplaceTempView("dv")
        return s.sql("SELECT * FROM dv")
    return frame


def builder(s: Any, door: str, path: str, schema: str | None, opts: dict[str, str]) -> Callable:
    return lambda: frame_for(s, door, path, schema, opts)


def all_shapes() -> list[str]:
    return [*SHAPES, "n1"]


def part_s(r: Rec, data: Path, ansi: str) -> None:
    for door in ("csv", "load", "view"):
        for hdr in HDRS:
            for comp in COMPS:
                for shape in all_shapes():
                    kinds = ["N1"] if shape == "n1" else TYPES
                    for kind in kinds:
                        path = str(data / f"{shape}-{comp}-{hdr}.csv")
                        schema = schema_for(shape, kind, comp)
                        opts = opts_for("S", hdr, shape)
                        tag = f"S/{ansi}/{door}/{hdr}/{comp}/{kind}/{shape}"
                        r.run(tag, builder(r.s, door, path, schema, opts))


def part_n(r: Rec, data: Path, ansi: str, read: str) -> None:
    doors = ("csv", "load", "view", "sql") if read == "N" else ("csv", "load", "view")
    for door in doors:
        for hdr in HDRS:
            for comp in COMPS:
                for shape in all_shapes():
                    path = str(data / f"{shape}-{comp}-{hdr}.csv")
                    opts = opts_for(read, hdr, shape)
                    if door == "sql" and (opts.keys() - {"header"} or hdr == "on"):
                        continue
                    tag = f"{read}/{ansi}/{door}/{hdr}/{comp}/-/{shape}"
                    r.run(tag, builder(r.s, door, path, None, opts))


def part_m(r: Rec, data: Path, ansi: str) -> None:
    for mode in ("DROPMALFORMED", "FAILFAST"):
        for comp in COMPS:
            for shape in all_shapes():
                kinds = ["N1"] if shape == "n1" else TYPES
                for kind in kinds:
                    path = str(data / f"{shape}-{comp}-off.csv")
                    schema = schema_for(shape, kind, comp)
                    opts = opts_for("S", "off", shape, mode)
                    tag = f"M/{ansi}/{mode}/off/{comp}/{kind}/{shape}"
                    r.run(tag, builder(r.s, "csv", path, schema, opts))


def label_reads() -> list[tuple[str, str, str | None]]:
    reads: list[tuple[str, str, str | None]] = [(f"S{kind}", "alone", kind) for kind in LABEL_TYPES]
    reads += [("SSTRING", "beside", "STRING"), ("N", "alone", None), ("F", "alone", None)]
    return reads


def store_type(kind: str | None) -> str:
    return "STRING" if kind is None else kind


def write_one(r: Rec, tag: str, frame: Any, how: str, table: str, ddl: str) -> None:
    s = r.s
    try:
        s.sql(f"DROP TABLE IF EXISTS {table}").collect()
        if how == "create":
            frame.writeTo(table).tableProperty("format-version", "2").create()
        else:
            props = "TBLPROPERTIES ('format-version'='2')"
            s.sql(f"CREATE TABLE {table} ({ddl}) USING iceberg {props}").collect()
            frame.createOrReplaceTempView("lv")
            s.sql(f"INSERT INTO {table} SELECT * FROM lv").collect()
    except Exception as error:
        r.emit({"tag": tag, "error": "write: " + condition(error)})
        return
    r.run(tag, lambda: s.table(table))
    try:
        s.sql(f"DROP TABLE IF EXISTS {table} PURGE").collect()
    except Exception as error:
        r.emit({"tag": tag + "/drop", "error": condition(error)})


def part_l(r: Rec, data: Path, ansi: str) -> None:
    r.s.sql("CREATE NAMESPACE IF NOT EXISTS lc.db").collect()
    for label, comp, kind in label_reads():
        read = "N" if label == "N" else ("F" if label == "F" else "S")
        name, _, ctype = companion(comp)
        for shape in SHAPES:
            path = str(data / f"{shape}-{comp}-on.csv")
            schema = None if kind is None else schema_for(shape, kind, comp)
            opts = opts_for(read, "on", shape)
            ddl = f"x {store_type(kind)}, {name} {ctype}"
            for how in ("create", "insert"):
                tag = f"L/{ansi}/{how}/on/{comp}/{label}/{shape}"
                try:
                    frame = frame_for(r.s, "csv", path, schema, opts)
                except Exception as error:
                    r.emit({"tag": tag, "error": "read: " + condition(error)})
                    continue
                write_one(r, tag, frame, how, "lc.db.t_" + how, ddl)


def part_e(r: Rec, data: Path, ansi: str) -> None:
    plans = [
        ("string", "lead0", "alone", "STRING"),
        ("int", "plain", "alone", "INT"),
        ("string_ts", "lead0", "beside", "STRING"),
        ("int_ts", "plain", "beside", "INT"),
    ]
    for key, shape, comp, kind in plans:
        path = str(data / f"{shape}-{comp}-on.csv")
        frame = frame_for(r.s, "csv", path, schema_for(shape, kind, comp), {"header": "true"})
        frame.createOrReplaceTempView("ev")
        rows = r.s.sql("EXPLAIN SELECT * FROM ev WHERE x IS NOT NULL").collect()
        text = "\n".join(" | ".join(str(cell) for cell in row) for row in rows)
        r.emit({"tag": f"E/{ansi}/{key}", "plan": text.replace(str(data).lstrip("/"), "<data>")})
    for key, read in (("none", "N"), ("infer_false", "F"), ("infer_true", "T")):
        path = str(data / "lead0-alone-on.csv")
        frame = frame_for(r.s, "csv", path, None, opts_for(read, "on", "lead0"))
        frame.createOrReplaceTempView("ev")
        rows = r.s.sql("EXPLAIN SELECT * FROM ev").collect()
        text = "\n".join(" | ".join(str(cell) for cell in row) for row in rows)
        r.emit({"tag": f"E/{ansi}/{key}", "plan": text.replace(str(data).lstrip("/"), "<data>")})


def main() -> None:
    engine, parent, data_dir, out, part = sys.argv[1:6]
    data = Path(data_dir)
    if engine == "gen":
        gen(data)
        return
    name, ansi = part.split(":")
    s, _ = session(engine, parent)
    r = Rec(s, out)
    r.conf(zone="UTC", ansi=ansi)
    if name == "S":
        part_s(r, data, ansi)
    elif name in {"N", "F", "T"}:
        part_n(r, data, ansi, name)
    elif name == "M":
        part_m(r, data, ansi)
    elif name == "L":
        part_l(r, data, ansi)
    elif name == "E":
        part_e(r, data, ansi)
    s.stop()


if __name__ == "__main__":
    main()
