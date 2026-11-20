import gzip
import sys
from collections.abc import Callable
from pathlib import Path
from typing import Any

from common import Rec, condition, session
from seat_emu import emulate

ORACLE_TYPES = [
    "TINYINT",
    "SMALLINT",
    "INT",
    "BIGINT",
    "FLOAT",
    "DOUBLE",
    "DECIMAL(10,2)",
    "BOOLEAN",
    "DATE",
    "TIMESTAMP",
    "TIMESTAMP_NTZ",
    "STRING",
    "BINARY",
    "INTERVAL YEAR TO MONTH",
    "INTERVAL DAY TO SECOND",
    "VARIANT",
]
TEXTS = [
    "0",
    "-0",
    "007",
    "+5",
    "-007",
    "127",
    "128",
    "-129",
    "200",
    "32767",
    "32768",
    "40000",
    "2147483647",
    "2147483648",
    "9223372036854775807",
    "9223372036854775808",
    "99999999999999999999",
    " 7",
    "7 ",
    "\t7",
    " 7 ",
    "1.50",
    "1.5",
    "5.",
    ".5",
    "1e3",
    "1E-2",
    "1.555",
    "-1.555",
    "1.554",
    "99999999.995",
    "123456789012",
    "1e400",
    "1,5",
    "1,000.5",
    "1d",
    "1f",
    "0x10",
    "0x1p3",
    "1_000",
    "NaN",
    "nan",
    "Inf",
    "+Inf",
    "-Inf",
    "inf",
    "Infinity",
    "-Infinity",
    "true",
    "TRUE",
    "True",
    "false",
    "t",
    "1",
    "y",
    "yes",
    " true",
    "2024",
    "2024-1",
    "2024-1-5",
    "2024-01-15",
    "20240115",
    " 2024-01-15",
    "2024-01-15T10:30:00",
    "2024-01-15 10:30:00",
    "2024-01-15 10:30",
    "2024-01-15T10:30:00Z",
    "2024-01-15 10:30:00+02:00",
    "2024-01-15 10:30:00.123456",
    "1705314600",
    "1-2",
    "1 02:03:04",
    "INTERVAL '1-2' YEAR TO MONTH",
    "abc",
]
ZONES = ["America/Los_Angeles", "America/New_York", "Asia/Tokyo"]
ZONE_TEXTS = [
    "2024-01-15",
    "2024-01-15 10:30:00",
    "2024-01-15T10:30:00Z",
    "2024-01-15 10:30:00+02:00",
    "2024-03-10 02:30:00",
    "2024-11-03 01:30:00",
    "007",
]
ZONE_TYPES = ["STRING", "DATE", "TIMESTAMP", "TIMESTAMP_NTZ"]
TS = "2024-01-01 00:00:00"


def field(text: str) -> str:
    if any(char in text for char in ',"'):
        return '"' + text.replace('"', '""') + '"'
    return text


def write(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text)


def gen(data: Path) -> None:
    for index, text in enumerate(TEXTS):
        write(data / "oracle" / f"o{index}.csv", f"{field(text)},k\n")
    for index, text in enumerate(ZONE_TEXTS):
        write(data / "zone" / f"z{index}-alone.csv", f"{text},k\n")
        write(data / "zone" / f"z{index}-beside.csv", f"{text},{TS}\n")
    topo = data / "topo"
    write(topo / "one.csv", "x,k\n007,k1\n")
    write(topo / "glob" / "a.csv", "x,k\n007,k1\n")
    write(topo / "glob" / "b.csv", "x,k\n1.50,k2\n")
    write(topo / "two" / "a.csv", "x,k\n007,k1\n")
    write(topo / "two" / "b.csv", "k,x\nk2,1e3\nk3,TRUE\n")
    (topo / "gz").mkdir(parents=True, exist_ok=True)
    (topo / "gz" / "a.csv.gz").write_bytes(gzip.compress(b"x,k\n007,k1\n", mtime=0))
    write(topo / "sep.csv", "x;k\n007;k1\n")
    write(topo / "spaces.csv", " x , k \n007,k1\n")
    write(topo / "emptyname.csv", ",k\n007,k1\n")
    write(topo / "bom.csv", "﻿x,k\n007,k1\n")
    pair = data / "pair"
    write(pair / "swap.csv", "k,x\nabc,007\n")
    write(pair / "part.csv", "x,z\n007,abc\n")
    write(pair / "case.csv", "X,K\n007,abc\n")
    write(pair / "plain.csv", "x,k\n007,abc\n")
    extra = data / "extra"
    write(extra / "lead0.csv", "x,k\n007,abc\n")
    write(extra / "bslash.csv", 'x,k\n"a\\"b",k1\n')
    write(extra / "dquote.csv", 'x,k\n"a""b",k1\n')
    write(extra / "qempty.csv", 'x,k\n"",k1\n')


def read(s: Any, path: str, schema: str | None, opts: dict[str, str]) -> Any:
    reader = s.read if schema is None else s.read.schema(schema)
    for key, value in opts.items():
        reader = reader.option(key, value)
    return reader.csv(path)


def scan0(s: Any, path: str, schema: str | None, opts: dict[str, str]) -> Any:
    lowered = {key.lower(): value for key, value in opts.items()}
    native = {key: lowered[key] for key in ("header", "sep", "compression") if key in lowered}
    native.setdefault("header", "false")
    local = Path(path)
    if local.exists():
        path = local.absolute().as_uri() + "/" * local.is_dir()
    first = s.read_csv(path, native)
    native["utf8_columns"] = ",".join(first.columns)
    frame = s.read_csv(path, native)
    reader = s.read if schema is None else s.read.schema(schema)
    for key, value in opts.items():
        reader = reader.option(key, value)
    reader._format = "csv"
    header = lowered.get("header") == "true"
    return reader._apply_reader_schema_semantics(
        frame, infer_schema=False, header=header, path=path
    )


def text_cast(s: Any, path: str, schema: str | None, opts: dict[str, str]) -> Any:
    lowered = {key.lower(): value for key, value in opts.items()}
    header = lowered.get("header", "false")
    frame = s.read_csv(path, {"header": header, "utf8_columns": "x"})
    reader = s.read if schema is None else s.read.schema(schema)
    for key, value in opts.items():
        reader = reader.option(key, value)
    reader._format = "csv"
    return reader._apply_reader_schema_semantics(
        frame, infer_schema=False, header=header == "true", path=path
    )


def cell(r: Rec, tag: str, build: Callable[[], Any]) -> None:
    r.run(tag, build)


def part_oracle(r: Rec, data: Path, engine: str) -> None:
    variants = ("spark",) if engine == "spark" else ("base", "text_try")
    for kind in ORACLE_TYPES:
        schema = f"x {kind}, k STRING"
        for index in range(len(TEXTS)):
            path = str(data / "oracle" / f"o{index}.csv")
            for variant in variants:
                tag = f"O/{variant}/{kind}/{index}"
                if variant == "text_try":
                    cell(
                        r,
                        tag,
                        lambda p=path, sc=schema: emulate(r.s, "text_try", p, "off", sc, "S"),
                    )
                else:
                    cell(r, tag, lambda p=path, sc=schema: read(r.s, p, sc, {}))


def zone_view(frame: Any, kind: str) -> Any:
    exprs = ["CAST(x AS STRING) AS xs"]
    if kind == "TIMESTAMP":
        exprs.append("unix_timestamp(x) AS ux")
    return frame.selectExpr(*exprs)


def zone_frame(s: Any, variant: str, path: str, schema: str, kind: str) -> Any:
    if variant == "text_try":
        return zone_view(emulate(s, "text_try", path, "off", schema, "S"), kind)
    return zone_view(read(s, path, schema, {}), kind)


def part_zones(r: Rec, data: Path, engine: str) -> None:
    variants = ("spark",) if engine == "spark" else ("base", "text_try")
    for zone in ZONES:
        r.conf(zone=zone)
        for comp, ctype in (("alone", "k STRING"), ("beside", "ts TIMESTAMP")):
            for kind in ZONE_TYPES:
                schema = f"x {kind}, {ctype}"
                for index in range(len(ZONE_TEXTS)):
                    path = str(data / "zone" / f"z{index}-{comp}.csv")
                    for variant in variants:
                        tag = f"Z/{variant}/{zone}/{comp}/{kind}/{index}"
                        cell(
                            r,
                            tag,
                            lambda v=variant, p=path, sc=schema, k=kind: zone_frame(
                                r.s, v, p, sc, k
                            ),
                        )
    r.conf(zone="UTC")


def topo_cells(data: Path) -> list[tuple[str, str, str | None, dict[str, str]]]:
    topo = data / "topo"
    hdr = {"header": "true"}
    url = (topo / "one.csv").absolute().as_uri()
    return [
        ("url_schema", url, "x STRING, k STRING", hdr),
        ("url_none", url, None, hdr),
        ("glob_schema", str(topo / "glob" / "*.csv"), "x STRING, k STRING", hdr),
        ("glob_none", str(topo / "glob" / "*.csv"), None, hdr),
        ("two_schema", str(topo / "two"), "x STRING, k STRING", hdr),
        ("two_none", str(topo / "two"), None, hdr),
        (
            "gz_schema",
            str(topo / "gz" / "a.csv.gz"),
            "x STRING, k STRING",
            {**hdr, "compression": "gzip"},
        ),
        ("gz_none", str(topo / "gz" / "a.csv.gz"), None, {**hdr, "compression": "gzip"}),
        ("sep_schema", str(topo / "sep.csv"), "x STRING, k STRING", {**hdr, "sep": ";"}),
        ("sep_none", str(topo / "sep.csv"), None, {**hdr, "sep": ";"}),
        ("spaces_schema", str(topo / "spaces.csv"), "x STRING, k STRING", hdr),
        ("spaces_none", str(topo / "spaces.csv"), None, hdr),
        ("emptyname_none", str(topo / "emptyname.csv"), None, hdr),
        ("emptyname_schema", str(topo / "emptyname.csv"), "x STRING, k STRING", hdr),
        ("bom_schema", str(topo / "bom.csv"), "x STRING, k STRING", hdr),
        ("bom_none", str(topo / "bom.csv"), None, hdr),
    ]


def part_topo(r: Rec, data: Path, engine: str) -> None:
    variants = ("spark",) if engine == "spark" else ("base", "scan0")
    for key, path, schema, opts in topo_cells(data):
        for variant in variants:
            tag = f"P/{variant}/{key}"
            if variant == "scan0":
                cell(r, tag, lambda p=path, sc=schema, o=opts: scan0(r.s, p, sc, o))
            else:
                cell(r, tag, lambda p=path, sc=schema, o=opts: read(r.s, p, sc, o))


def pair_cells(data: Path) -> list[tuple[str, str, str | None, dict[str, str]]]:
    pair = data / "pair"
    hdr = {"header": "true"}
    return [
        ("swap_int", str(pair / "swap.csv"), "x INT, k STRING", hdr),
        ("swap_string", str(pair / "swap.csv"), "x STRING, k STRING", hdr),
        ("part_int", str(pair / "part.csv"), "x INT, k STRING", hdr),
        ("part_string", str(pair / "part.csv"), "x STRING, k STRING", hdr),
        ("case_string", str(pair / "case.csv"), "x STRING, k STRING", hdr),
        ("plain_string", str(pair / "plain.csv"), "x STRING, k STRING", hdr),
        ("swap_none", str(pair / "swap.csv"), None, hdr),
        ("part_none", str(pair / "part.csv"), None, hdr),
        ("swap_off", str(pair / "swap.csv"), "x STRING, k STRING", {}),
    ]


def part_pair(r: Rec, data: Path, engine: str, ansi: str) -> None:
    r.conf(ansi=ansi)
    variants = ("spark",) if engine == "spark" else ("base", "text_cast")
    for key, path, schema, opts in pair_cells(data):
        for variant in variants:
            tag = f"Q/{ansi}/{variant}/{key}"
            if variant == "text_cast":
                cell(r, tag, lambda p=path, sc=schema, o=opts: text_cast(r.s, p, sc, o))
            else:
                cell(r, tag, lambda p=path, sc=schema, o=opts: read(r.s, p, sc, o))


def part_extra(r: Rec, data: Path, engine: str) -> None:
    extra = data / "extra"
    lead0 = str(extra / "lead0.csv")
    hdr = {"header": "true"}
    infer = {**hdr, "inferSchema": "true"}
    reads = [
        ("cond_string", lead0, "x STRING, k STRING", infer),
        ("cond_int", lead0, "x INT, k INT", infer),
        ("bslash", str(extra / "bslash.csv"), "x STRING, k STRING", hdr),
        ("dquote", str(extra / "dquote.csv"), "x STRING, k STRING", hdr),
        (
            "empty_value",
            str(extra / "qempty.csv"),
            "x STRING, k STRING",
            {**hdr, "emptyValue": "E"},
        ),
        (
            "qempty_nullv",
            str(extra / "qempty.csv"),
            "x STRING, k STRING",
            {**hdr, "nullValue": "NA"},
        ),
        ("qempty", str(extra / "qempty.csv"), "x STRING, k STRING", hdr),
    ]
    variants = ("spark",) if engine == "spark" else ("base", "text_cast")
    for key, path, schema, opts in reads:
        for variant in variants:
            tag = f"X/{variant}/{key}"
            if variant == "text_cast":
                cell(r, tag, lambda p=path, sc=schema, o=opts: text_cast(r.s, p, sc, o))
            else:
                cell(r, tag, lambda p=path, sc=schema, o=opts: read(r.s, p, sc, o))
    using = [
        ("using", f"CREATE OR REPLACE TEMPORARY VIEW uv USING csv OPTIONS (path '{lead0}')"),
        (
            "using_cols",
            f"CREATE OR REPLACE TEMPORARY VIEW uv (x STRING, k STRING) USING csv "
            f"OPTIONS (path '{lead0}', header 'true')",
        ),
    ]
    for key, ddl in using:
        try:
            r.s.sql(ddl).collect()
        except Exception as error:
            r.emit({"tag": f"X/{engine}/{key}", "error": condition(error)})
            continue
        cell(r, f"X/{engine}/{key}", lambda: r.s.sql("SELECT * FROM uv"))


def main() -> None:
    engine, parent, data_dir, out, part = sys.argv[1:6]
    data = Path(data_dir)
    if engine == "gen":
        gen(data)
        return
    s, _ = session(engine, parent)
    r = Rec(s, out)
    r.conf(zone="UTC", ansi="true")
    name, _, ansi = part.partition(":")
    if name == "oracle":
        r.conf(ansi=ansi)
        part_oracle(r, data, engine)
    elif name == "zones":
        part_zones(r, data, engine)
    elif name == "topo":
        part_topo(r, data, engine)
    elif name == "pair":
        part_pair(r, data, engine, ansi)
    elif name == "extra":
        part_extra(r, data, engine)
    s.stop()


if __name__ == "__main__":
    main()
