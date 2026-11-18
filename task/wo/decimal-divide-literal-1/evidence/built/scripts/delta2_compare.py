import json
import re
import sys
from collections import Counter
from pathlib import Path
from typing import Any

CONDITION = re.compile(r"\[([A-Z_]+)")
CARDED = {"REMAINDER_BY_ZERO": "DIVIDE_BY_ZERO"}


def verdict(found: Any, want: Any) -> str:
    if isinstance(want, str):
        if not isinstance(found, str):
            return "SPARK_RAISES"
        spark = CONDITION.search(want)
        ours = CONDITION.search(found)
        spark_class = spark.group(1) if spark else None
        ours_class = ours.group(1) if ours else None
        if ours_class == CARDED.get(spark_class, spark_class):
            return "BOTH_RAISE"
        return "BOTH_RAISE_OTHER_CLASS"
    if isinstance(found, str):
        return "REPARK_RAISES"
    if found == want:
        return "EQUAL"
    if found.get("types") != want.get("types"):
        return "TYPE"
    return "VALUE"


def flatten(doc: dict[str, Any]) -> dict[tuple[str, str], Any]:
    out: dict[tuple[str, str], Any] = {}
    for mode, cells in doc["pmod"].items():
        for key, value in cells.items():
            out[(key, mode)] = value
    for built, modes in doc["runtime_set"].items():
        for mode, cells in modes.items():
            for key, value in cells.items():
                out[(f"{built}:{key}", mode)] = value
    return out


def brief(value: Any) -> str:
    if isinstance(value, str):
        found = CONDITION.search(value)
        return "RAISE " + (found.group(1) if found else value[4:60])
    types = ",".join(value.get("types", [])[1:]) or ",".join(value.get("types", []))
    return f"{types} {';'.join(value.get('rows', []))}"


spark = flatten(json.loads(Path(sys.argv[1]).read_text()))
names = sys.argv[2::2]
packages = [flatten(json.loads(Path(p).read_text())) for p in sys.argv[3::2]]
counts = {name: Counter() for name in names}
for cell in sorted(spark):
    verdicts = [verdict(pkg[cell], spark[cell]) for pkg in packages]
    for name, v in zip(names, verdicts, strict=True):
        counts[name][v] += 1
    line = " | ".join(f"{n}={v}" for n, v in zip(names, verdicts, strict=True))
    print(f"{cell[0]} [{cell[1]}] | {line} | spark: {brief(spark[cell])}")
    for name, pkg, v in zip(names, packages, verdicts, strict=True):
        if v != "EQUAL":
            print(f"    {name}: {brief(pkg[cell])}")
for name in names:
    print("COUNTS", name, dict(sorted(counts[name].items())))
