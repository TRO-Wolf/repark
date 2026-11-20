import sys
from pathlib import Path
from typing import Any

from common import Rec, session
from domain import COMPS, HDRS, TYPES, all_shapes, opts_for, schema_for


def text_frame(s: Any, path: str, hdr: str) -> Any:
    header = "true" if hdr == "on" else "false"
    return s.read_csv(path, {"header": header, "utf8_columns": "x"})


def emulate(s: Any, variant: str, path: str, hdr: str, schema: str | None, read: str) -> Any:
    from repark.spark import functions

    opts = opts_for(read, hdr, Path(path).name.split("-")[0])
    reader = s.read if schema is None else s.read.schema(schema)
    for key, value in opts.items():
        reader = reader.option(key, value)
    reader._format = "csv"
    frame = text_frame(s, path, hdr)
    if variant == "text_cast" or schema is None:
        return reader._apply_reader_schema_semantics(
            frame, infer_schema=False, header=hdr == "on", path=path
        )
    token = opts.get("nullValue")
    fields = [part.strip().split(" ", 1) for part in schema.split(", ")]
    selects = []
    for column, (name, kind) in zip(frame.columns, fields, strict=True):
        col = functions.col(column)
        if token is not None:
            col = functions.when(col == functions.lit(token), functions.lit(None)).otherwise(col)
        selects.append(col.try_cast(kind).alias(name))
    return frame.select(*selects)


def main() -> None:
    engine, parent, data_dir, out, part = sys.argv[1:6]
    variant, ansi = part.split(":")
    data = Path(data_dir)
    s, _ = session(engine, parent)
    r = Rec(s, out)
    r.conf(zone="UTC", ansi=ansi)
    for read in ("S", "N", "F"):
        for hdr in HDRS:
            for comp in COMPS:
                for shape in all_shapes():
                    kinds = ["-"] if read != "S" else (["N1"] if shape == "n1" else TYPES)
                    for kind in kinds:
                        path = str(data / f"{shape}-{comp}-{hdr}.csv")
                        schema = None if read != "S" else schema_for(shape, kind, comp)
                        tag = f"{read}/{ansi}/csv/{hdr}/{comp}/{kind}/{shape}"
                        r.run(
                            tag,
                            lambda p=path, h=hdr, sc=schema, rd=read: emulate(
                                s, variant, p, h, sc, rd
                            ),
                        )
    s.stop()


if __name__ == "__main__":
    main()
