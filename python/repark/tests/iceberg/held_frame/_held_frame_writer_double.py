from __future__ import annotations

import json
import sys

TAG = "ICE-HELD-WRITER"
ACCOUNT = "0" * 12
RESOURCE = ":".join(["arn", "aws", "glue", "us-east-2", ACCOUNT, "table/x"])


def reply(fields: dict[str, object]) -> None:
    print(TAG, json.dumps(fields), flush=True)


def main() -> int:
    json.loads(sys.stdin.readline())
    print(f"starting under {RESOURCE} for {ACCOUNT}", file=sys.stderr, flush=True)
    reply({"ready": True})
    for line in sys.stdin:
        request = json.loads(line)
        if request["values"] == "refuse":
            message = f"[TABLE_OR_VIEW_NOT_FOUND] {request['table']} in {RESOURCE}"
            reply({"kind": "AnalysisException", "message": message})
        elif request["values"] == "exit":
            return 4
        else:
            reply({"count": 3})
    return 0


if __name__ == "__main__":
    sys.exit(main())
