import json
import sys
from collections import Counter
from pathlib import Path


def want(name: str, precision: int, files: int, mode: str, partitions: int, query: str) -> str:
    function, shape = query.split("/")
    multi = partitions > 1 and (shape == "grouped" or files == 2)
    if multi and precision == 19 and name != "fit":
        return "PAST_DECLARED_PRECISION"
    if mode == "true" and function == "sum":
        return "ARITHMETIC_OVERFLOW"
    return "NULL"


def outcome(answer: list[list[str]] | str) -> str:
    if isinstance(answer, list):
        return "NULL" if answer[0][-1] == "None" else "VALUE " + answer[0][-1]
    if "past its declared precision" in answer:
        return "PAST_DECLARED_PRECISION"
    if "ARITHMETIC_OVERFLOW" in answer:
        return "ARITHMETIC_OVERFLOW"
    return answer[:120]


tally: Counter[tuple[bool, str]] = Counter()
with Path(sys.argv[1]).open() as fh:
    for line in fh:
        r = json.loads(line)
        if r["query"].split("/")[0] not in ("sum", "try_sum"):
            continue
        stem = r["file"].split("_")[0]
        files = 2 if "_two" in r["file"] else 1
        precision = int(r["file"].rsplit("_p", 1)[1])
        got = outcome(r["answer"])
        expected = want(stem, precision, files, r["ansi"], r["tp"], r["query"])
        tally[(got == expected, "" if got == expected else got)] += 1
        if got != expected:
            print("DIFF", r["ansi"], r["tp"], r["file"], r["query"], "want", expected, "got", got)
print(dict(tally))
