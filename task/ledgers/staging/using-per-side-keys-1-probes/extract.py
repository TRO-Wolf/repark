from __future__ import annotations

import contextlib
import io
import json
import runpy
import sys
import types
from pathlib import Path
from typing import Any

EVENTS: list[list[Any]] = []


class Frame:
    def __init__(self, origin: list[Any] | None) -> None:
        self.origin = origin
        self.columns: list[str] = []

    def collect(self) -> list[Any]:
        return []

    def create_view(self, name: str) -> None:
        if self.origin is None:
            EVENTS.append(["skip", name])
        elif self.origin[0] == "rows":
            EVENTS.append(["rows", name, self.origin[1], self.origin[2]])
        else:
            EVENTS.append(["sqlview", name, self.origin[1]])

    def __getattr__(self, _name: str) -> Any:
        return Chain()

    def __getitem__(self, _name: str) -> Any:
        return Chain()


class Chain:
    columns: tuple[str, ...] = ()

    def __call__(self, *_args: Any, **_kwargs: Any) -> Any:
        return Frame(None)

    def __getattr__(self, _name: str) -> Any:
        return Chain()

    def __getitem__(self, _name: str) -> Any:
        return Chain()

    def __iter__(self) -> Any:
        return iter(())

    def _binary(self, _other: Any) -> Any:
        return Chain()

    __gt__ = __lt__ = __ge__ = __le__ = __add__ = _binary

    def simple_string(self) -> str:
        return ""


class Session:
    conf = Chain()
    catalog = Chain()

    def sql(self, text: str) -> Frame:
        EVENTS.append(["sql", text])
        return Frame(["sql", text])

    def create_frame(self, rows: Any, schema: Any = None) -> Frame:
        plain = json.loads(json.dumps([list(row) for row in rows], default=str))
        return Frame(["rows", plain, schema])

    def table(self, _name: str) -> Frame:
        return Frame(None)

    def stop(self) -> None:
        return None


class Builder:
    def __getattr__(self, _name: str) -> Any:
        return lambda *_args, **_kwargs: self

    def session(self) -> Session:
        return Session()


def stub_modules() -> None:
    camel = (
        (Frame, "createOrReplaceTempView", Frame.create_view),
        (Session, "createDataFrame", Session.create_frame),
        (Session, "sparkContext", Chain()),
        (Builder, "getOrCreate", Builder.session),
        (Chain, "simpleString", Chain.simple_string),
    )
    for owner, name, value in camel:
        setattr(owner, name, value)
    repark = types.ModuleType("repark")
    repark.ReparkSession = type("ReparkSession", (), {"builder": Builder()})
    spark = types.ModuleType("repark.spark")
    spark.functions = Chain()
    functions = types.ModuleType("repark.spark.functions")
    functions.__getattr__ = lambda _name: Chain()
    errors = types.ModuleType("repark.errors")
    errors.__getattr__ = lambda _name: Exception
    sys.modules.update(
        {
            "repark": repark,
            "repark.spark": spark,
            "repark.spark.functions": functions,
            "repark.errors": errors,
        }
    )


def main() -> None:
    out = Path(sys.argv[1])
    stub_modules()
    scenarios: dict[str, list[list[Any]]] = {}
    for script in sys.argv[2:]:
        EVENTS.clear()
        sys.argv = [script, "repark", "/dev/null"]
        with contextlib.redirect_stdout(io.StringIO()):
            runpy.run_path(script, run_name="__main__")
        seen: set[str] = set()
        kept = []
        for event in EVENTS:
            key = json.dumps(event)
            if event[0] == "sql" and key in seen:
                continue
            seen.add(key)
            kept.append(list(event))
        scenarios[Path(script).stem] = kept
    out.write_text(json.dumps(scenarios, indent=0, sort_keys=True) + "\n")
    print(
        {
            name: sum(1 for event in events if event[0] == "sql")
            for name, events in scenarios.items()
        }
    )


main()
