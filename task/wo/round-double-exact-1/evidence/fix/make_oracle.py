import json
import sys
from pathlib import Path

raw, values_path, out = sys.argv[1:4]
values = json.loads(Path(values_path).read_text())
sp = {
    json.loads(x)["key"]: json.loads(x)
    for x in Path(raw, "spark-ansi-true.jsonl").read_text().splitlines()
}
sf = {
    json.loads(x)["key"]: json.loads(x)
    for x in Path(raw, "spark-ansi-false.jsonl").read_text().splitlines()
}
oracle = {
    "spark": sp["provenance"]["version"],
    "jar": sp["provenance"]["jar"],
    "values": {},
    "stores": {},
}
for src, key in (("DOUBLE", "double"), ("FLOAT", "float")):
    texts = sp[f"source/{src}"]["text" if src == "DOUBLE" else "dtext"]
    oracle["values"][src] = [
        [r["id"], r["text"], texts[str(r["id"])], r["class"]] for r in values[key]
    ]
    for target, fn, scale in (
        ("DECIMAL(10,2)", "round", 2),
        ("DECIMAL(10,2)", "bround", 2),
        ("DECIMAL(38,18)", "round", 18),
    ):
        k = f"store/sql/{src}/{target}/{fn}/{scale}"
        rows = sp[k]["rows"]
        assert rows == sf[k]["rows"]
        oracle["stores"][f"{src}/{target}/{fn}/{scale}"] = {i: v[4:] for i, v in rows.items()}
Path(out).write_text(json.dumps(oracle, separators=(",", ":"), sort_keys=True) + "\n")
print(len(Path(out).read_text()))
