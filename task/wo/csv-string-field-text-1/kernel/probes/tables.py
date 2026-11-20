import ast
import gzip
import json
import sys
from pathlib import Path

raw_path, added_path, out_dir = sys.argv[1:4]
out = Path(out_dir)

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
]
ADDED_TYPES = [*ORACLE_TYPES, "DECIMAL(38,10)", "DECIMAL(3,0)", "DECIMAL(4,4)"]
REFUSED = ["INTERVAL YEAR TO MONTH", "INTERVAL DAY TO SECOND", "VARIANT"]


def days_from_civil(y: int, m: int, d: int) -> int:
    y -= m <= 2
    era = y // 400
    yoe = y - era * 400
    doy = (153 * (m + (-3 if m > 2 else 9)) + 2) // 5 + d - 1
    doe = yoe * 365 + yoe // 4 - yoe // 100 + doy
    return era * 146097 + doe - 719468


def split_date(text: str) -> int:
    sign = 1
    if text[0] in "+-":
        sign = -1 if text[0] == "-" else 1
        text = text[1:]
    y, m, d = text.split("-")
    return days_from_civil(sign * int(y), int(m), int(d))


def date_days(text: str) -> str:
    return str(split_date(text))


def stamp_micros(text: str) -> str:
    day, _, clock = text.partition(" ")
    days = split_date(day)
    hh, mm, ss = clock.split(":")
    whole, _, frac = ss.partition(".")
    micros = int((frac + "000000")[:6]) if frac else 0
    return str(((days * 86400 + int(hh) * 3600 + int(mm) * 60 + int(whole)) * 1_000_000) + micros)


def decimal_units(text: str, scale: int) -> str:
    neg = text.startswith("-")
    body = text.lstrip("-")
    whole, _, frac = body.partition(".")
    assert len(frac) == scale, (text, scale)
    value = int(whole + frac)
    return str(-value if neg else value)


def canon(kind: str, text: str, value: str | None) -> str:
    if value is None or value == "None":
        return "N"
    if kind in ("TINYINT", "SMALLINT", "INT", "BIGINT"):
        return str(int(value))
    if kind in ("FLOAT", "DOUBLE"):
        mapped = {"Infinity": "inf", "-Infinity": "-inf", "NaN": "nan"}.get(value, value)
        return repr(float(mapped))
    if kind.startswith("DECIMAL"):
        scale = int(kind.split(",")[1].rstrip(")"))
        return decimal_units(value, scale)
    if kind == "BOOLEAN":
        return {"True": "true", "False": "false", "true": "true", "false": "false"}[value]
    if kind == "DATE":
        return date_days(value)
    if kind in ("TIMESTAMP", "TIMESTAMP_NTZ"):
        return stamp_micros(value)
    if kind == "STRING":
        assert value == text, (kind, text, value)
        return "="
    if kind == "BINARY":
        assert value in (repr(text.encode()), text), (kind, text, value)
        return "="
    raise ValueError(kind)


spark = {}
zones = {}
with gzip.open(raw_path, "rt") as handle:
    for line in handle:
        record = json.loads(line)
        parts = record["tag"].split("/")
        if parts[0] == "O" and parts[1] == "false" and parts[2] == "spark":
            spark[(parts[3], int(parts[4]))] = record
        if parts[0] == "Z" and parts[1] == "spark":
            zone = parts[2] + "/" + parts[3]
            zones[(zone, parts[4], parts[5], int(parts[6]))] = record

with Path(added_path).open() as handle:
    added = [json.loads(line) for line in handle]
added_by = {(r["text"], r["type"]): r for r in added}

revision = ast.parse((Path(raw_path).parents[1] / "probes" / "revision.py").read_text())
names = {}
for node in revision.body:
    target = node.targets[0] if isinstance(node, ast.Assign) else None
    if isinstance(target, ast.Name) and target.id in ("TEXTS", "ZONE_TEXTS"):
        names[target.id] = ast.literal_eval(node.value)
TEXTS = names["TEXTS"]
ZONE_TEXTS = names["ZONE_TEXTS"]


def rust_text(text: str) -> str:
    out = []
    for ch in text:
        if ch == "\\":
            out.append("\\\\")
        elif ch == '"':
            out.append('\\"')
        elif ch == "\t":
            out.append("\\t")
        elif 32 <= ord(ch) < 127:
            out.append(ch)
        else:
            out.append(f"\\u{{{ord(ch):x}}}")
    return '"' + "".join(out) + '"'


oracle_rows = []
refused_ok = 0
for index, text in enumerate(TEXTS):
    cells = []
    for kind in ORACLE_TYPES:
        record = spark[(kind, index)]
        if "error" in record:
            fallback = added_by[(text, kind)]
            cells.append(canon(kind, text, fallback["xs"]))
        else:
            cells.append(canon(kind, text, record["cols"][0][0]))
    for kind in REFUSED:
        assert (kind, index) in spark
        refused_ok += 1
    oracle_rows.append((text, " ".join(cells)))

added_rows = []
seen = set(TEXTS)
for text in dict.fromkeys(r["text"] for r in added):
    if text in seen:
        continue
    cells = []
    for kind in ADDED_TYPES:
        r = added_by[(text, kind)]
        if kind in ("FLOAT", "DOUBLE"):
            assert r["isnull"] or "py" in r, r
            cells.append(canon(kind, text, None if r["isnull"] else r["py"]))
        elif kind == "BOOLEAN":
            cells.append(canon(kind, text, None if r["isnull"] else r["xs"]))
        else:
            cells.append(canon(kind, text, None if r["isnull"] else r["xs"]))
    added_rows.append((text, " ".join(cells)))

extra_old = []
for text in TEXTS:
    if (text, "DECIMAL(38,10)") in added_by:
        cells = [
            canon(k, text, None if added_by[(text, k)]["isnull"] else added_by[(text, k)]["xs"])
            for k in ("DECIMAL(38,10)", "DECIMAL(3,0)", "DECIMAL(4,4)")
        ]
        extra_old.append((text, " ".join(cells)))

zone_rows = []
for (zone, comp, kind, index), record in sorted(zones.items()):
    if comp != "alone":
        continue
    beside = zones[(zone, "beside", kind, index)]
    assert beside.get("cols") == record.get("cols"), (zone, kind, index)
    text = ZONE_TEXTS[index]
    values = record["cols"][0]
    if kind == "TIMESTAMP":
        ux = values[1]
        expected = "N" if ux == "None" else str(int(ux) * 1_000_000)
    elif kind == "DATE":
        expected = "N" if values[0] == "None" else date_days(values[0])
    elif kind == "TIMESTAMP_NTZ":
        expected = "N" if values[0] == "None" else stamp_micros(values[0])
    else:
        assert values[0] == text
        expected = "="
    zone_rows.append((zone, kind, text, expected))

lines = ["const ORACLE: &[(&str, &str)] = &["]
lines += [f'    ({rust_text(t)}, "{c}"),' for t, c in oracle_rows]
lines += ["];", "", "const ADDED: &[(&str, &str)] = &["]
lines += [f'    ({rust_text(t)}, "{c}"),' for t, c in added_rows]
lines += ["];", "", "const ADDED_DECIMALS: &[(&str, &str)] = &["]
lines += [f'    ({rust_text(t)}, "{c}"),' for t, c in extra_old]
lines += ["];", "", "const ZONE_CELLS: &[(&str, &str, &str, &str)] = &["]
lines += [f'    ("{z}", "{k.lower()}", {rust_text(t)}, "{e}"),' for z, k, t, e in zone_rows]
lines += ["];"]
(out / "tables.rs.txt").write_text("\n".join(lines) + "\n")


def py_cells(cells: str) -> str:
    if len(cells) <= 60:
        return repr(cells)
    words = cells.split(" ")
    half = len(words) // 2
    return repr(" ".join(words[:half]) + " ") + " " + repr(" ".join(words[half:]))


py = ["ORACLE = ["]
py += [f"    ({t!a}, {py_cells(c)})," for t, c in oracle_rows]
py += ["]", "ADDED = ["]
py += [f"    ({t!a}, {py_cells(c)})," for t, c in added_rows]
py += ["]", "ZONE_CELLS = ["]
py += [f"    ({z!r}, {k.lower()!r}, {t!a}, {e!r})," for z, k, t, e in zone_rows]
py += ["]"]
(out / "tables.py.txt").write_text("\n".join(py) + "\n")
print(
    "oracle",
    len(oracle_rows),
    "x",
    len(ORACLE_TYPES),
    "refused cells",
    refused_ok,
    "added",
    len(added_rows),
    "x",
    len(ADDED_TYPES),
    "extra decimals",
    len(extra_old),
    "zones",
    len(zone_rows),
)
