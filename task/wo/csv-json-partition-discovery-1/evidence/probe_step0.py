from __future__ import annotations

import argparse
import datetime
import decimal
import json
import re
import shutil
from pathlib import Path
from typing import Any

FORMATS = ("csvh", "csvn", "json")
EXT = {"csvh": ".csv", "csvn": ".csv", "json": ".json"}
INFER_KEY = "spark.sql.sources.partitionColumnTypeInference.enabled"


def spark_session(work: Path) -> Any:
    from pyspark.sql import SparkSession

    s = (
        SparkSession.builder.master("local[2]")
        .appName("csvjson-step0")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.warehouse.dir", str(work / "wh"))
        .getOrCreate()
    )
    s.sparkContext.setLogLevel("ERROR")
    return s


def repark_session(work: Path) -> Any:
    from repark.spark.session import ReparkSession

    return (
        ReparkSession.builder.appName("csvjson-step0")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.warehouse.dir", str(work / "wh"))
        .config("repark.local_fs.allow_ddl", "true")
        .getOrCreate()
    )


def write_fmt(df: Any, fmt: str, out: Path, parts: list[str]) -> None:
    w = df.coalesce(1).write.mode("overwrite")
    if parts:
        w = w.partitionBy(*parts)
    if fmt == "csvh":
        w.option("header", "true").csv(str(out))
    elif fmt == "csvn":
        w.option("header", "false").csv(str(out))
    else:
        w.json(str(out))


def data_files(d: Path, ext: str) -> list[Path]:
    return sorted(
        p for p in d.rglob(f"*{ext}") if p.is_file() and not p.name.startswith((".", "_"))
    )


def place(src: Path, dst: Path) -> None:
    dst.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy2(src, dst)


def make_fixtures(fx: Path, work: Path) -> None:
    from pyspark.sql import types as st

    if fx.exists():
        shutil.rmtree(fx)
    fx.mkdir(parents=True)
    spark = spark_session(work)
    base = st.StructType(
        [
            st.StructField("id", st.LongType()),
            st.StructField("p", st.IntegerType()),
            st.StructField("s", st.StringType()),
        ]
    )
    rows = [(1, 1, "a"), (2, 1, "b"), (3, 2, "c"), (4, 2, "d")]
    two = st.StructType(
        [
            st.StructField("id", st.LongType()),
            st.StructField("year", st.IntegerType()),
            st.StructField("month", st.IntegerType()),
            st.StructField("s", st.StringType()),
        ]
    )
    sp = st.StructType(
        [
            st.StructField("id", st.LongType()),
            st.StructField("p", st.StringType()),
            st.StructField("s", st.StringType()),
        ]
    )
    dp = st.StructType(
        [
            st.StructField("id", st.LongType()),
            st.StructField("p", st.DateType()),
            st.StructField("s", st.StringType()),
        ]
    )
    manifest: dict[str, Any] = {}
    for fmt in FORMATS:
        root = fx / fmt
        ext = EXT[fmt]
        write_fmt(spark.createDataFrame(rows, base), fmt, root / "one_level", ["p"])
        write_fmt(
            spark.createDataFrame([(1, 2024, 1, "a"), (2, 2024, 2, "b"), (3, 2025, 1, "c")], two),
            fmt,
            root / "two_level",
            ["year", "month"],
        )
        write_fmt(
            spark.createDataFrame([(1, None, "a"), (2, 1, "b")], base),
            fmt,
            root / "hive_default",
            ["p"],
        )
        write_fmt(
            spark.createDataFrame(
                [(1, "a b", "w"), (2, "a/b", "x"), (3, "50%", "y"), (4, "c=d", "z")], sp
            ),
            fmt,
            root / "escaped",
            ["p"],
        )
        write_fmt(
            spark.createDataFrame([(1, "alpha", "a"), (2, "beta", "b")], sp),
            fmt,
            root / "str_vals",
            ["p"],
        )
        write_fmt(
            spark.createDataFrame(
                [(1, datetime.date(2024, 1, 2), "a"), (2, datetime.date(2024, 7, 15), "b")], dp
            ),
            fmt,
            root / "date_vals",
            ["p"],
        )
        fa = data_files(root / "one_level" / "p=1", ext)[0]
        fb = data_files(root / "one_level" / "p=2", ext)[0]
        place(fa, root / "conflict" / "a=1" / f"part-0{ext}")
        place(fb, root / "conflict" / "b=2" / f"part-1{ext}")
        place(fa, root / "mixed_depth" / "a=1" / f"part-0{ext}")
        place(fb, root / "mixed_depth" / "a=1" / "b=2" / f"part-1{ext}")
        shutil.copytree(root / "one_level", root / "hidden")
        place(fa, root / "hidden" / "_temporary" / "0" / f"part-temp{ext}")
        place(fa, root / "hidden" / f".hidden{ext}")
        place(fa, root / "hidden" / "p=1" / f".dot{ext}")
        place(fa, root / "hidden" / "p=1" / f"_under{ext}")
        place(fa, root / "hidden" / "_hdir" / f"part-h{ext}")
        place(fa, root / "hidden" / ".ddir" / f"part-d{ext}")
        place(fa, root / "flat" / f"part-0{ext}")
        place(fb, root / "flat" / f"part-1{ext}")
        place(fa, root / "eq_root" / "tag=foo" / f"part-0{ext}")
        place(fb, root / "eq_root" / "tag=foo" / f"part-1{ext}")
        place(fa, root / "eq_value" / "k=a=b" / f"part-0{ext}")
        write_fmt(spark.createDataFrame([(1, 99, "x")], base), fmt, work / f"ov_{fmt}", [])
        place(data_files(work / f"ov_{fmt}", ext)[0], root / "overlap" / "p=10" / f"part-0{ext}")
        place(fa, root / "case_mixed" / "p=1" / f"part-0{ext}")
        place(fb, root / "case_mixed" / "P=2" / f"part-1{ext}")
        shutil.copytree(root / "one_level", root / "zero")
        (root / "zero" / "p=3").mkdir()
        (root / "zero" / "p=3" / f"empty{ext}").write_bytes(b"")
        shutil.copytree(root / "one_level", root / "stray")
        place(fa, root / "stray" / "p=1" / "README")
        place(fa, root / "empty_val" / "p=" / f"part-0{ext}")
        place(fb, root / "empty_val" / "p=1" / f"part-1{ext}")
        manifest[fmt] = {"fa": str(fa.relative_to(fx)), "fb": str(fb.relative_to(fx))}
    (fx / "manifest.json").write_text(json.dumps(manifest, indent=2), encoding="utf-8")
    spark.stop()


def enc_value(v: Any) -> Any:
    if v is None:
        return None
    if isinstance(v, bool):
        return ["bool", v]
    if isinstance(v, int):
        return ["int", v]
    if isinstance(v, float):
        return ["float", repr(v)]
    if isinstance(v, decimal.Decimal):
        return ["dec", str(v)]
    if isinstance(v, datetime.datetime):
        return ["ts", v.isoformat()]
    if isinstance(v, datetime.date):
        return ["date", v.isoformat()]
    return ["str", str(v)]


def enc_rows(rows: list[Any]) -> list[Any]:
    out = [[enc_value(c) for c in r] for r in rows]
    out.sort(key=lambda r: json.dumps(r, sort_keys=True))
    return out


def enc_schema(df: Any) -> list[Any]:
    return [[f.name, f.dataType.simpleString(), f.nullable] for f in df.schema.fields]


def enc_err(exc: BaseException) -> dict[str, Any]:
    cond = None
    getter = getattr(exc, "getCondition", None)
    if callable(getter):
        try:
            cond = getter()
        except Exception:
            cond = None
    text = str(exc)
    m = re.search(r"\[([A-Z0-9_.]+)\]", text)
    return {
        "status": "err",
        "condition": cond,
        "cond_msg": m.group(1) if m else None,
        "type": type(exc).__name__,
        "message": text.splitlines()[0][:200] if text else "",
    }


def rel_files(files: list[str], fx: Path, engine: str) -> list[str]:
    from urllib.parse import unquote

    root = str(fx.resolve())
    out = []
    for f in files:
        t = re.sub(r"^file:(//)?", "", unquote(f) if engine == "spark" else f)
        t = "/" + t.lstrip("/")
        out.append(t[len(root) + 1 :] if t.startswith(root) else t)
    return sorted(out)


class Probe:
    def __init__(self, engine: str, fx: Path, work: Path) -> None:
        self.engine = engine
        self.fx = fx
        self.work = work
        self.spark = spark_session(work) if engine == "spark" else repark_session(work)
        self.manifest = json.loads((fx / "manifest.json").read_text(encoding="utf-8"))
        self.res: dict[str, Any] = {}
        self.n = 0

    def cell(self, key: str, fn: Any) -> None:
        try:
            self.res[key] = fn()
        except Exception as exc:
            self.res[key] = enc_err(exc)

    def reader(self, fmt: str, door: str, opts: dict[str, str], schema: str | None) -> Any:
        r = self.spark.read
        if fmt == "csvh":
            r = r.option("header", "true")
        elif fmt == "csvn":
            r = r.option("header", "false")
        for k, v in opts.items():
            r = r.option(k, v)
        if schema is not None:
            r = r.schema(schema)
        return r

    def read(
        self,
        fmt: str,
        door: str,
        path: Any,
        opts: dict[str, str] | None = None,
        schema: str | None = None,
    ) -> Any:
        r = self.reader(fmt, door, opts or {}, schema)
        kind = "json" if fmt == "json" else "csv"
        if door == "load":
            return r.format(kind).load(path)
        return r.json(path) if kind == "json" else r.csv(path)

    def frame_obs(self, df: Any) -> dict[str, Any]:
        return {"status": "ok", "schema": enc_schema(df), "rows": enc_rows(df.collect())}

    def roundtrip(self, df: Any) -> dict[str, Any]:
        self.n += 1
        out = self.work / "rt" / f"c{self.n}"
        df.write.mode("overwrite").parquet(str(out))
        return self.frame_obs(self.spark.read.parquet(str(out)))

    def paths(self, fmt: str) -> dict[str, Any]:
        root = self.fx / fmt
        ext = EXT[fmt]
        fa = str(self.fx / self.manifest[fmt]["fa"])
        fb = str(self.fx / self.manifest[fmt]["fb"])
        return {
            "one_level": str(root / "one_level"),
            "two_level": str(root / "two_level"),
            "hive_default": str(root / "hive_default"),
            "escaped": str(root / "escaped"),
            "str_vals": str(root / "str_vals"),
            "date_vals": str(root / "date_vals"),
            "conflict": str(root / "conflict"),
            "mixed_depth": str(root / "mixed_depth"),
            "hidden": str(root / "hidden"),
            "leaf_p1": str(root / "one_level" / "p=1"),
            "two_leaf": str(root / "two_level" / "year=2024"),
            "flat": str(root / "flat"),
            "file": fa,
            "files_list": [fa, fb],
            "glob_dirs": str(root / "one_level" / "p=*"),
            "glob_files": str(root / "one_level" / "p=*" / f"*{ext}"),
            "eq_root": str(root / "eq_root" / "tag=foo"),
            "eq_value": str(root / "eq_value"),
            "overlap": str(root / "overlap"),
            "case_mixed": str(root / "case_mixed"),
            "zero": str(root / "zero"),
            "stray": str(root / "stray"),
            "empty_val": str(root / "empty_val"),
        }

    def primary(self, fmt: str, ansi: str) -> None:
        load_layouts = {
            "one_level",
            "two_level",
            "flat",
            "hidden",
            "leaf_p1",
            "file",
            "conflict",
            "date_vals",
        }
        for name, path in self.paths(fmt).items():
            doors = ["read", "load"] if name in load_layouts else ["read"]
            for door in doors:
                k = f"P/{fmt}/{door}/{name}/ansi={ansi}"
                self.cell(f"{k}/read", lambda d=door, p=path: self.frame_obs(self.read(fmt, d, p)))
                self.cell(
                    f"{k}/where_p2",
                    lambda d=door, p=path: self.frame_obs(self.read(fmt, d, p).where("p = 2")),
                )
                self.cell(
                    f"{k}/count",
                    lambda d=door, p=path: {"status": "ok", "count": self.read(fmt, d, p).count()},
                )
                self.cell(
                    f"{k}/files",
                    lambda d=door, p=path: {
                        "status": "ok",
                        "files": rel_files(self.read(fmt, d, p).inputFiles(), self.fx, self.engine),
                    },
                )
                self.cell(f"{k}/rt", lambda d=door, p=path: self.roundtrip(self.read(fmt, d, p)))

    def trigger_b(self, fmt: str, ansi: str) -> None:
        paths = self.paths(fmt)
        for name in ("one_level", "two_level", "date_vals"):
            path = paths[name]
            k = f"TB/{fmt}/{name}/ansi={ansi}"

            def fromjson(p: str = path) -> dict[str, Any]:
                df = self.read(fmt, "read", p)
                back = type(df.schema).fromJson(json.loads(df.schema.json()))
                return {
                    "status": "ok",
                    "equal": back == df.schema,
                    "schema": [
                        [f.name, f.dataType.simpleString(), f.nullable] for f in back.fields
                    ],
                }

            def create(p: str = path) -> dict[str, Any]:
                df = self.read(fmt, "read", p)
                return self.frame_obs(self.spark.createDataFrame(df.collect(), df.schema))

            def describe(p: str = path) -> dict[str, Any]:
                return self.frame_obs(self.read(fmt, "read", p).describe())

            def ctas(p: str = path) -> dict[str, Any]:
                self.n += 1
                t = f"tb_ctas_{self.n}"
                self.read(fmt, "read", p).createOrReplaceTempView("tb_src")
                self.spark.sql(f"DROP TABLE IF EXISTS {t}")
                self.spark.sql(f"CREATE TABLE {t} USING parquet AS SELECT * FROM tb_src")
                try:
                    return self.frame_obs(self.spark.table(t))
                finally:
                    self.spark.sql(f"DROP TABLE IF EXISTS {t}")

            self.cell(f"{k}/fromjson", fromjson)
            self.cell(f"{k}/create", create)
            self.cell(f"{k}/describe", describe)
            self.cell(f"{k}/ctas", ctas)

    def canaries(self, fmt: str, ansi: str) -> None:
        paths = self.paths(fmt)
        one = paths["one_level"]
        k = f"C/{fmt}/ansi={ansi}"
        for name in ("one_level", "date_vals", "str_vals"):
            self.spark.conf.set(INFER_KEY, "false")
            try:
                self.cell(
                    f"{k}/noinfer/{name}/read",
                    lambda p=paths[name]: self.frame_obs(self.read(fmt, "read", p)),
                )
                self.cell(
                    f"{k}/noinfer/{name}/where",
                    lambda p=paths[name]: self.frame_obs(
                        self.read(fmt, "read", p).where("p = '2'")
                    ),
                )
            finally:
                self.spark.conf.set(INFER_KEY, "true")
        schemas = {
            "with_p": "id BIGINT, s STRING, p STRING",
            "with_p_int": "id BIGINT, s STRING, p INT",
            "p_first": "p INT, id BIGINT, s STRING",
            "without_p": "id BIGINT, s STRING",
        }
        for sname, ddl in schemas.items():
            self.cell(
                f"{k}/schema_{sname}/read",
                lambda d=ddl: self.frame_obs(self.read(fmt, "read", one, schema=d)),
            )
            self.cell(
                f"{k}/schema_{sname}/where",
                lambda d=ddl: self.frame_obs(self.read(fmt, "read", one, schema=d).where("p = 2")),
            )
        self.cell(
            f"{k}/schema_with_p/flat",
            lambda: self.frame_obs(
                self.read(fmt, "read", paths["flat"], schema="id BIGINT, s STRING")
            ),
        )
        self.cell(
            f"{k}/recursive/read",
            lambda: self.frame_obs(
                self.read(fmt, "read", one, opts={"recursiveFileLookup": "true"})
            ),
        )
        self.cell(
            f"{k}/basepath_leaf/read",
            lambda: self.frame_obs(
                self.read(fmt, "read", paths["leaf_p1"], opts={"basePath": one})
            ),
        )
        self.cell(
            f"{k}/basepath_root/read",
            lambda: self.frame_obs(self.read(fmt, "read", one, opts={"basePath": one})),
        )
        if fmt != "json":
            self.cell(
                f"{k}/infer_schema/read",
                lambda: self.frame_obs(self.read(fmt, "read", one, opts={"inferSchema": "true"})),
            )
            self.cell(
                f"{k}/infer_schema/flat",
                lambda: self.frame_obs(
                    self.read(fmt, "read", paths["flat"], opts={"inferSchema": "true"})
                ),
            )
            self.cell(
                f"{k}/nullvalue/read",
                lambda: self.frame_obs(self.read(fmt, "read", one, opts={"nullValue": "b"})),
            )
        self.spark.conf.set("spark.sql.caseSensitive", "true")
        try:
            self.cell(
                f"{k}/case_sensitive/case_mixed",
                lambda: self.frame_obs(self.read(fmt, "read", paths["case_mixed"])),
            )
            self.cell(
                f"{k}/case_sensitive/one_level", lambda: self.frame_obs(self.read(fmt, "read", one))
            )
        finally:
            self.spark.conf.set("spark.sql.caseSensitive", "false")
        self.cell(
            f"{k}/select_p/read", lambda: self.frame_obs(self.read(fmt, "read", one).select("p"))
        )
        self.cell(
            f"{k}/groupby_p/read",
            lambda: self.frame_obs(self.read(fmt, "read", one).groupBy("p").count()),
        )

    def run(self) -> dict[str, Any]:
        for ansi in ("true", "false"):
            self.spark.conf.set("spark.sql.ansi.enabled", ansi)
            for fmt in FORMATS:
                self.primary(fmt, ansi)
                self.trigger_b(fmt, ansi)
                self.canaries(fmt, ansi)
        self.spark.conf.set("spark.sql.ansi.enabled", "true")
        return self.res


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--engine", choices=["make-fixtures", "spark", "repark"], required=True)
    ap.add_argument("--fixtures", required=True)
    ap.add_argument("--work", required=True)
    ap.add_argument("--out")
    a = ap.parse_args()
    fx = Path(a.fixtures)
    work = Path(a.work)
    if work.exists():
        shutil.rmtree(work)
    work.mkdir(parents=True)
    if a.engine == "make-fixtures":
        make_fixtures(fx, work)
        print("FIXTURES-OK")
        return
    if a.engine == "repark":
        import repark._native

        print("REPARK_NATIVE:", repark._native.__file__)
    res = Probe(a.engine, fx, work).run()
    Path(a.out).write_text(
        json.dumps({"run1": res}, indent=1, sort_keys=True, default=str), encoding="utf-8"
    )
    print(
        f"DONE engine={a.engine} cells={len(res)} "
        f"errs={sum(1 for v in res.values() if v.get('status') == 'err')}"
    )


if __name__ == "__main__":
    main()
