import json
import sys
from pathlib import Path

P, head, out = Path(sys.argv[1]), sys.argv[2], Path(sys.argv[3])
spark = json.loads((P / "spark-cells.json").read_text())
found = json.loads((P / f"{head}-cells.json").read_text())
RULED = ("filter/df/or_mixed/on_off", "filter/df/agg_key_sens/on_off", "filter/df/add_sens/on_off")
CLASSES = {
    "ruled": "TEMP-VIEW-SESSION-SETTINGS-1 ruled class (question 11): a settings-sensitive "
    "conjunct kept above a raising body",
    "and_guard": "AND-GUARD-ANSI-1: the AND evaluates the guarded division on the dropped row",
    "cast_text": "direct gap: a string-to-integer cast raises Arrow's text in both modes",
    "mod": "direct gap: 10 % 0 raises DIVIDE_BY_ZERO where Spark raises REMAINDER_BY_ZERO",
    "suggestion": "direct gap: NUMERIC_VALUE_OUT_OF_RANGE without Spark's WITH_SUGGESTION subclass",
    "native_join": "door not at the seat: crossJoin's native exact join plans both sides "
    "under the live state",
    "main": "kept as main answers: the walker reads through the closed frame as it read the "
    "inlined one",
}


def same(a: dict, b: dict) -> bool:
    if "error" in a or "error" in b:
        return "error" in a and "error" in b and (not a["error"] or a["error"] == b["error"])
    return a.get("rows") == b.get("rows")


def klass(key: str) -> str | None:
    if key.startswith(
        ("filter/df/or_mixed/on_off", "filter/df/agg_key_sens/on_off", "filter/df/add_sens/on_off")
    ):
        return "ruled"
    if key.startswith("filter/df/and_free/"):
        return "and_guard"
    if key.startswith(("filter/df/cast_null/", "filter/df/numcast/")):
        return "cast_text"
    if "/ansi/mod/true>false" in key:
        return "mod"
    if "/ansi/bool_dec/true>false" in key:
        return "suggestion"
    if key.startswith("derive/crossjoin/"):
        return "native_join"
    if key.startswith("walker/"):
        return "main"
    return None


table = {"classes": CLASSES}
missing = []
for mode in ("true", "false"):
    table[mode] = {}
    for key, want in sorted(spark[mode].items()):
        got = found[mode][key]
        if same(want, got):
            continue
        k = klass(key)
        if k is None:
            missing.append((mode, key))
            continue
        answer = {"error": got["error"]} if "error" in got else {"rows": got["rows"]}
        table[mode][key] = {"class": k, "answer": answer}
out.write_text(json.dumps(table, indent=1, sort_keys=True) + "\n")
print("standing", {m: len(table[m]) for m in ("true", "false")}, "unclassed", missing)
