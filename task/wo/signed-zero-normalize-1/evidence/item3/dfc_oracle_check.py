import argparse
import json
import re
from pathlib import Path


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--rust", type=Path, required=True)
    parser.add_argument("--oracle", type=Path, required=True)
    arguments = parser.parse_args()
    source = arguments.rust.read_text(encoding="utf-8")
    start = source.index("async fn signed_zero_comparisons_answer_like_spark")
    block = re.sub(r"\s+", " ", source[start : source.index("];", start)])
    pattern = r'\(\s*"([^"]+)",\s*vec!\[([^\]]*)\],\s*vec!\[([^\]]*)\],?\s*\)'
    cases = re.findall(pattern, block)
    oracle = json.loads(arguments.oracle.read_text(encoding="utf-8"))
    recorded = {}
    for key, cell in oracle["cells"].items():
        match = re.match(r"SELECT id FROM t WHERE (.*)$", cell.get("sql") or "")
        rows = json.loads(cell["rows"]) if isinstance(cell["rows"], str) else cell["rows"]
        if match:
            recorded[match.group(1)] = (key, sorted(row[0] for row in rows))
    mismatches = 0
    for predicate, spark, _ in cases:
        ids = sorted(int(item) for item in spark.split(",") if item.strip())
        hit = recorded.get(predicate)
        status = "missing" if hit is None else ("equal" if hit[1] == ids else f"differs {hit[1]}")
        mismatches += status != "equal"
        print(predicate, ids, hit[0] if hit else "-", status)
    print(f"cases {len(cases)} spark {oracle['spark_version']} mismatches {mismatches}")


if __name__ == "__main__":
    main()
