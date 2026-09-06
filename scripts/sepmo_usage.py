#!/usr/bin/env python3
"""Collect normalized SEPMO worker usage records from local run directories."""

from __future__ import annotations

import argparse
import json
import re
import sys
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

ADAPTERS: tuple[str, ...] = ("muse", "opencode", "grok", "claude")
RECORD_FIELDS: tuple[str, ...] = (
    "unit",
    "lane",
    "adapter",
    "model",
    "effort",
    "role",
    "started_utc",
    "finished_utc",
    "wall_s",
    "steps",
    "tool_calls",
    "tokens_in",
    "tokens_out",
    "tokens_cached",
    "tokens_reasoning",
    "cost_usd",
    "exit",
    "commits",
)
FIELD_UNITS: dict[str, str] = {
    "wall_s": "seconds",
    "steps": "count",
    "tool_calls": "count",
    "tokens_in": "provider_tokens",
    "tokens_out": "provider_tokens",
    "tokens_cached": "provider_tokens",
    "tokens_reasoning": "provider_tokens",
    "cost_usd": "usd",
    "exit": "process_exit_code",
    "commits": "count",
}
STAMP_NAME: re.Pattern[str] = re.compile(r"^(\d{8}T\d{6}Z)$")
FLAG_VALUE: re.Pattern[str] = re.compile(
    r"--(model|reasoning-effort|agent|role|variant|cli)\s+(\S+)"
)
USAGE_KEYS: dict[str, tuple[str, ...]] = {
    "tokens_in": ("tokens_in", "input_tokens", "prompt_tokens", "input"),
    "tokens_out": ("tokens_out", "output_tokens", "completion_tokens", "output"),
    "tokens_cached": (
        "tokens_cached",
        "cache_read_tokens",
        "cached_tokens",
        "cache_read",
    ),
    "tokens_reasoning": ("tokens_reasoning", "reasoning_tokens", "reasoning"),
    "cost_usd": ("cost_usd", "total_cost_usd", "cost"),
}

MUSE_NO_MODEL_TOKENS: str = (
    "muse out.jsonl has no model token or cost fields; "
    "tool.result original_output_tokens is tool-output size, not billed usage"
)
GROK_NO_TOKENS: str = (
    "grok --output-format json wrapper fields are sessionId, stopReason, "
    "num_turns, total_cost_usd; token keys were not present on the live object"
)
CLAUDE_UNAVAILABLE: str = (
    "claude sub-agent transcripts are not in a worker run-dir layout and "
    "are not accessible to this collector"
)
OPENCODE_NO_RUN_STREAM: str = "opencode out.ndjson has no step-finish usage events"


class UsageError(Exception):
    """A collect or index input that must fail loudly."""


def collect_run(run_directory: Path) -> dict[str, Any]:
    """Build one usage record from a worker run directory."""
    resolved: Path = run_directory.resolve()
    _reject_remote(resolved)
    if not resolved.is_dir():
        raise UsageError(f"not a directory: {run_directory}")
    command_path: Path = resolved / "cmd.txt"
    if not command_path.is_file():
        raise UsageError(f"missing cmd.txt: {run_directory}")
    command_text: str = _read_text(command_path)
    if not command_text.strip():
        raise UsageError(f"empty cmd.txt: {run_directory}")
    adapter: str = _detect_adapter(resolved, command_text)
    record: dict[str, Any] = _blank_record(resolved)
    record["adapter"] = adapter
    record["lane"] = _lane_name(resolved)
    record["unit"] = record["lane"]
    _apply_command_flags(record, command_text)
    _apply_timestamps(record, resolved)
    _apply_exit(record, resolved)
    _apply_commits(record, resolved)
    source: list[str] = ["cmd.txt"]
    if adapter == "muse":
        source.extend(_parse_muse(record, resolved))
    elif adapter == "grok":
        source.extend(_parse_grok(record, resolved))
    elif adapter == "opencode":
        source.extend(_parse_opencode(record, resolved))
    else:
        source.extend(_parse_claude(record, resolved))
    record["source"] = source
    _fill_missing(record)
    findings: list[str] = validate_record(record)
    if findings:
        joined: str = "; ".join(findings)
        raise UsageError(f"record failed schema checks: {joined}")
    return record


def discover_run_dirs(root: Path) -> list[Path]:
    """Return run directories under root (direct, or lane/stamp)."""
    resolved: Path = root.resolve()
    _reject_remote(resolved)
    if not resolved.is_dir():
        raise UsageError(f"not a directory: {root}")
    found: list[Path] = []
    if _is_run_dir(resolved):
        return [resolved]
    for child in sorted(resolved.iterdir()):
        if not child.is_dir():
            continue
        if _is_run_dir(child):
            found.append(child)
            continue
        for grandchild in sorted(child.iterdir()):
            if grandchild.is_dir() and _is_run_dir(grandchild):
                found.append(grandchild)
    if not found:
        raise UsageError(f"no run directories under {root}")
    return found


def index_runs(root: Path, as_jsonl: bool) -> str:
    """Collect every run directory under root and render a table or JSONL."""
    records: list[dict[str, Any]] = [collect_run(path) for path in discover_run_dirs(root)]
    if as_jsonl:
        lines: list[str] = [json.dumps(record, sort_keys=True) for record in records]
        return "\n".join(lines) + "\n"
    return _markdown_table(records)


def validate_record(record: dict[str, Any]) -> list[str]:
    """Return schema findings for one usage record; empty means valid."""
    findings: list[str] = []
    for field_name in RECORD_FIELDS:
        if field_name not in record:
            findings.append(f"missing field {field_name}")
    if "missing_reason" not in record or not isinstance(record["missing_reason"], dict):
        findings.append("missing_reason must be an object")
    if "units" not in record or not isinstance(record["units"], dict):
        findings.append("units must be an object")
    if "source" not in record or not isinstance(record["source"], list):
        findings.append("source must be an array")
    adapter: Any = record.get("adapter")
    if adapter is not None and adapter not in ADAPTERS:
        findings.append(f"adapter {adapter!r} is not a known adapter")
    integer_fields: tuple[str, ...] = (
        "steps",
        "tool_calls",
        "tokens_in",
        "tokens_out",
        "tokens_cached",
        "tokens_reasoning",
        "exit",
        "commits",
    )
    number_fields: tuple[str, ...] = ("wall_s", "cost_usd")
    string_fields: tuple[str, ...] = (
        "unit",
        "lane",
        "model",
        "effort",
        "role",
        "started_utc",
        "finished_utc",
    )
    for field_name in integer_fields:
        value: Any = record.get(field_name)
        if value is not None and type(value) is not int:
            findings.append(f"{field_name} must be integer or null")
    for field_name in number_fields:
        value = record.get(field_name)
        if value is not None and not isinstance(value, (int, float)):
            findings.append(f"{field_name} must be number or null")
    for field_name in string_fields:
        value = record.get(field_name)
        if value is not None and not isinstance(value, str):
            findings.append(f"{field_name} must be string or null")
    missing_reason: Any = record.get("missing_reason")
    if isinstance(missing_reason, dict):
        for key, reason in missing_reason.items():
            if not isinstance(key, str) or not isinstance(reason, str) or not reason:
                findings.append("missing_reason values must be non-empty strings")
                break
            if record.get(key) is not None:
                findings.append(f"{key} is set but also listed in missing_reason")
    return findings


def main(argv: list[str] | None = None) -> int:
    """Run collect or index on local run directories."""
    parser: argparse.ArgumentParser = argparse.ArgumentParser(
        description="Collect SEPMO worker usage records from local run directories."
    )
    subparsers = parser.add_subparsers(dest="command", required=True)
    collect_parser = subparsers.add_parser("collect", help="one run directory")
    collect_parser.add_argument("run_dir", type=Path)
    index_parser = subparsers.add_parser("index", help="a directory of run directories")
    index_parser.add_argument("root", type=Path)
    index_parser.add_argument("--jsonl", action="store_true")
    arguments: argparse.Namespace = parser.parse_args(argv)
    try:
        if arguments.command == "collect":
            record: dict[str, Any] = collect_run(arguments.run_dir)
            sys.stdout.write(json.dumps(record, indent=2, sort_keys=True) + "\n")
            return 0
        rendered: str = index_runs(arguments.root, arguments.jsonl)
        sys.stdout.write(rendered)
        if not rendered.endswith("\n"):
            sys.stdout.write("\n")
        return 0
    except UsageError as error:
        print(f"sepmo-usage: {error}", file=sys.stderr)
        return 1
    except OSError as error:
        print(f"sepmo-usage: {error}", file=sys.stderr)
        return 1


def _reject_remote(path: Path) -> None:
    text: str = str(path)
    if text.startswith(("http://", "https://", "ftp://")):
        raise UsageError("refusing a remote path; collector is local-only")


def _read_text(path: Path) -> str:
    return path.read_text(encoding="utf-8")


def _is_run_dir(path: Path) -> bool:
    if not (path / "cmd.txt").is_file():
        return False
    markers: tuple[str, ...] = (
        "out.jsonl",
        "out.json",
        "out.ndjson",
        "exit",
        "config.json",
        "prompt.md",
    )
    return any((path / name).exists() for name in markers)


def _lane_name(run_directory: Path) -> str:
    if STAMP_NAME.match(run_directory.name) and run_directory.parent.name:
        return run_directory.parent.name
    return run_directory.name


def _blank_record(run_directory: Path) -> dict[str, Any]:
    record: dict[str, Any] = dict.fromkeys(RECORD_FIELDS)
    record["missing_reason"] = {}
    record["units"] = dict(FIELD_UNITS)
    record["source"] = []
    record["unit"] = _lane_name(run_directory)
    record["lane"] = _lane_name(run_directory)
    return record


def _mark(record: dict[str, Any], field_name: str, reason: str) -> None:
    record[field_name] = None
    record["missing_reason"][field_name] = reason


def _set_if(record: dict[str, Any], field_name: str, value: Any) -> None:
    if value is None:
        return
    record[field_name] = value
    record["missing_reason"].pop(field_name, None)


def _detect_adapter(run_directory: Path, command_text: str) -> str:
    lowered: str = command_text.lower()
    if (run_directory / "out.jsonl").exists() or "muse exec" in lowered:
        return "muse"
    if (run_directory / "out.ndjson").exists() or (run_directory / "config.json").exists():
        return "opencode"
    if "opencode" in lowered or "kilo" in lowered:
        return "opencode"
    if (run_directory / "out.json").exists() or re.search(r"\bgrok\b", lowered):
        return "grok"
    if "claude" in lowered:
        return "claude"
    raise UsageError(f"cannot detect adapter from {run_directory / 'cmd.txt'}")


def _apply_command_flags(record: dict[str, Any], command_text: str) -> None:
    flags: dict[str, str] = {}
    for match in FLAG_VALUE.finditer(command_text):
        flags[match.group(1)] = match.group(2).strip("'\"")
    _set_if(record, "model", flags.get("model"))
    _set_if(record, "effort", flags.get("reasoning-effort") or flags.get("variant"))
    role: str | None = flags.get("agent") or flags.get("role")
    if role:
        _set_if(record, "role", role)


def _apply_timestamps(record: dict[str, Any], run_directory: Path) -> None:
    command_path: Path = run_directory / "cmd.txt"
    exit_path: Path = run_directory / "exit"
    started: datetime | None = _mtime_utc(command_path)
    finished: datetime | None = _mtime_utc(exit_path) if exit_path.exists() else None
    stamp_match: re.Match[str] | None = STAMP_NAME.match(run_directory.name)
    if started is None and stamp_match:
        started = datetime.strptime(stamp_match.group(1), "%Y%m%dT%H%M%SZ").replace(tzinfo=UTC)
    if started is not None:
        record["started_utc"] = _iso(started)
    if finished is not None:
        record["finished_utc"] = _iso(finished)
    if started is not None and finished is not None:
        wall: float = max(0.0, (finished - started).total_seconds())
        record["wall_s"] = wall


def _apply_exit(record: dict[str, Any], run_directory: Path) -> None:
    exit_path: Path = run_directory / "exit"
    if not exit_path.is_file():
        return
    text: str = _read_text(exit_path).strip()
    if not text:
        return
    try:
        record["exit"] = int(text.split()[0])
    except ValueError as error:
        raise UsageError(f"malformed exit file: {exit_path}") from error


def _apply_commits(record: dict[str, Any], run_directory: Path) -> None:
    handback_path: Path = run_directory / "handback.json"
    if not handback_path.is_file():
        return
    try:
        payload: Any = json.loads(_read_text(handback_path))
    except json.JSONDecodeError as error:
        raise UsageError(f"malformed handback.json: {handback_path}") from error
    if not isinstance(payload, dict):
        raise UsageError(f"handback.json must be an object: {handback_path}")
    commits: Any = payload.get("commits")
    if isinstance(commits, list):
        record["commits"] = len(commits)


def _parse_muse(record: dict[str, Any], run_directory: Path) -> list[str]:
    jsonl_path: Path = run_directory / "out.jsonl"
    source: list[str] = []
    prompt_path: Path = run_directory / "prompt.md"
    if prompt_path.is_file() and record.get("role") is None:
        prompt_head: str = _read_text(prompt_path)[:400].lower()
        if "critic" in prompt_head:
            record["role"] = "critic"
        elif "build lane" in prompt_head or "worker" in prompt_head:
            record["role"] = "worker"
        source.append("prompt.md")
    if not jsonl_path.is_file():
        _mark(record, "steps", "muse out.jsonl is absent")
        _mark(record, "tool_calls", "muse out.jsonl is absent")
        for field_name in (
            "tokens_in",
            "tokens_out",
            "tokens_cached",
            "tokens_reasoning",
            "cost_usd",
        ):
            _mark(record, field_name, MUSE_NO_MODEL_TOKENS)
        return source
    source.append("out.jsonl")
    events: list[dict[str, Any]] = _load_jsonl(jsonl_path)
    started_tasks: set[str] = set()
    tool_calls: int = 0
    for event in events:
        payload: dict[str, Any] = _as_object(event.get("payload"))
        payload_type: str = str(event.get("payload_type") or "")
        kind: str = str(payload.get("kind") or "")
        if payload_type == "run.model.configured":
            _set_if(record, "model", payload.get("model_id"))
        if payload_type == "task.lifecycle.started":
            task_id: Any = payload.get("task_id")
            if isinstance(task_id, str) and task_id:
                started_tasks.add(task_id)
        if payload_type == "tool.result" or kind == "tool_result":
            tool_calls += 1
    record["steps"] = len(started_tasks)
    record["tool_calls"] = tool_calls
    for field_name in (
        "tokens_in",
        "tokens_out",
        "tokens_cached",
        "tokens_reasoning",
        "cost_usd",
    ):
        _mark(record, field_name, MUSE_NO_MODEL_TOKENS)
    return source


def _parse_grok(record: dict[str, Any], run_directory: Path) -> list[str]:
    json_path: Path = run_directory / "out.json"
    source: list[str] = []
    if not json_path.is_file():
        _mark(record, "steps", "grok out.json is absent")
        _mark(record, "tool_calls", "grok out.json has no tool-call field")
        for field_name in (
            "tokens_in",
            "tokens_out",
            "tokens_cached",
            "tokens_reasoning",
            "cost_usd",
        ):
            _mark(record, field_name, GROK_NO_TOKENS)
        return source
    source.append("out.json")
    raw: str = _read_text(json_path)
    if not raw.strip():
        _mark(record, "steps", "grok out.json is empty (round still running or failed)")
        _mark(record, "tool_calls", "grok out.json is empty")
        for field_name in (
            "tokens_in",
            "tokens_out",
            "tokens_cached",
            "tokens_reasoning",
            "cost_usd",
        ):
            _mark(record, field_name, "grok out.json is empty")
        return source
    try:
        payload: Any = json.loads(raw)
    except json.JSONDecodeError as error:
        raise UsageError(f"malformed out.json: {json_path}") from error
    if not isinstance(payload, dict):
        raise UsageError(f"out.json must be an object: {json_path}")
    turns: Any = payload.get("num_turns")
    if isinstance(turns, int):
        record["steps"] = turns
    tool_calls: int | None = _grok_tool_calls(payload)
    if tool_calls is not None:
        record["tool_calls"] = tool_calls
    else:
        _mark(record, "tool_calls", "grok out.json has no tool-call count")
    _apply_usage_object(record, payload)
    if record.get("cost_usd") is None:
        cost: Any = payload.get("total_cost_usd")
        if isinstance(cost, (int, float)):
            record["cost_usd"] = float(cost)
    for field_name in ("tokens_in", "tokens_out", "tokens_cached", "tokens_reasoning"):
        if record.get(field_name) is None:
            _mark(record, field_name, GROK_NO_TOKENS)
    if record.get("cost_usd") is None:
        _mark(record, "cost_usd", GROK_NO_TOKENS)
    if record.get("steps") is None:
        _mark(record, "steps", "grok out.json has no num_turns")
    return source


def _parse_opencode(record: dict[str, Any], run_directory: Path) -> list[str]:
    source: list[str] = []
    config_path: Path = run_directory / "config.json"
    if config_path.is_file():
        source.append("config.json")
        try:
            config: Any = json.loads(_read_text(config_path))
        except json.JSONDecodeError as error:
            raise UsageError(f"malformed config.json: {config_path}") from error
        if isinstance(config, dict):
            model: Any = config.get("model")
            if isinstance(model, str):
                _set_if(record, "model", model)
            if record.get("effort") is None:
                _set_if(record, "effort", config.get("variant"))
    ndjson_path: Path = run_directory / "out.ndjson"
    if not ndjson_path.is_file():
        _mark(record, "steps", "opencode out.ndjson is absent")
        _mark(record, "tool_calls", "opencode out.ndjson is absent")
        for field_name in (
            "tokens_in",
            "tokens_out",
            "tokens_cached",
            "tokens_reasoning",
            "cost_usd",
        ):
            _mark(record, field_name, OPENCODE_NO_RUN_STREAM)
        return source
    source.append("out.ndjson")
    events: list[dict[str, Any]] = _load_jsonl(ndjson_path)
    steps: int = 0
    tool_calls: int = 0
    tokens_in: int = 0
    tokens_out: int = 0
    tokens_cached: int = 0
    tokens_reasoning: int = 0
    cost_usd: float = 0.0
    saw_usage: bool = False
    for event in events:
        if _is_step_finish(event):
            steps += 1
            usage: dict[str, Any] | None = _event_usage(event)
            if usage is not None:
                saw_usage = True
                tokens_in += int(usage.get("tokens_in") or 0)
                tokens_out += int(usage.get("tokens_out") or 0)
                tokens_cached += int(usage.get("tokens_cached") or 0)
                tokens_reasoning += int(usage.get("tokens_reasoning") or 0)
                cost_usd += float(usage.get("cost_usd") or 0.0)
        if _is_tool_event(event):
            tool_calls += 1
    record["steps"] = steps
    record["tool_calls"] = tool_calls
    if saw_usage:
        record["tokens_in"] = tokens_in
        record["tokens_out"] = tokens_out
        record["tokens_cached"] = tokens_cached
        record["tokens_reasoning"] = tokens_reasoning
        record["cost_usd"] = cost_usd
    else:
        for field_name in (
            "tokens_in",
            "tokens_out",
            "tokens_cached",
            "tokens_reasoning",
            "cost_usd",
        ):
            _mark(record, field_name, OPENCODE_NO_RUN_STREAM)
    return source


def _parse_claude(record: dict[str, Any], run_directory: Path) -> list[str]:
    del run_directory
    for field_name in (
        "steps",
        "tool_calls",
        "tokens_in",
        "tokens_out",
        "tokens_cached",
        "tokens_reasoning",
        "cost_usd",
        "role",
    ):
        if record.get(field_name) is None:
            _mark(record, field_name, CLAUDE_UNAVAILABLE)
    return []


def _fill_missing(record: dict[str, Any]) -> None:
    reasons: dict[str, str] = {
        "unit": "unit is the lane directory name when no sidecar is present",
        "lane": "lane directory name could not be derived",
        "adapter": "adapter could not be detected",
        "model": "model flag and event stream had no model id",
        "effort": "no --reasoning-effort or --variant flag",
        "role": "no --agent/--role flag and no prompt role marker",
        "started_utc": "cmd.txt mtime and stamp name were both unavailable",
        "finished_utc": "exit file was absent",
        "wall_s": "started or finished timestamp was absent",
        "steps": "adapter event stream has no step or turn count",
        "tool_calls": "adapter event stream has no tool-call count",
        "tokens_in": "adapter does not report input tokens in the run dir",
        "tokens_out": "adapter does not report output tokens in the run dir",
        "tokens_cached": "adapter does not report cached input tokens in the run dir",
        "tokens_reasoning": "adapter does not report reasoning tokens in the run dir",
        "cost_usd": "adapter does not report cost in the run dir",
        "exit": "exit file was absent or empty",
        "commits": "handback.json was absent or had no commits list",
    }
    for field_name in RECORD_FIELDS:
        if record.get(field_name) is None and field_name not in record["missing_reason"]:
            record["missing_reason"][field_name] = reasons[field_name]


def _load_jsonl(path: Path) -> list[dict[str, Any]]:
    lines: list[str] = [line for line in _read_text(path).splitlines() if line.strip()]
    if not lines:
        return []
    events: list[dict[str, Any]] = []
    failures: int = 0
    for line in lines:
        try:
            payload: Any = json.loads(line)
        except json.JSONDecodeError:
            failures += 1
            continue
        if isinstance(payload, dict):
            events.append(payload)
        else:
            failures += 1
    if failures and failures * 2 >= len(lines):
        raise UsageError(f"malformed JSONL (majority of lines failed): {path}")
    return events


def _as_object(value: Any) -> dict[str, Any]:
    if isinstance(value, dict):
        return value
    return {}


def _mtime_utc(path: Path) -> datetime | None:
    if not path.exists():
        return None
    return datetime.fromtimestamp(path.stat().st_mtime, tz=UTC)


def _iso(value: datetime) -> str:
    return value.astimezone(UTC).strftime("%Y-%m-%dT%H:%M:%S.%fZ")


def _grok_tool_calls(payload: dict[str, Any]) -> int | None:
    for key in ("num_tool_calls", "tool_calls", "toolCallCount"):
        value: Any = payload.get(key)
        if isinstance(value, int):
            return value
        if isinstance(value, list):
            return len(value)
    return None


def _apply_usage_object(record: dict[str, Any], payload: dict[str, Any]) -> None:
    holders: list[dict[str, Any]] = [payload]
    usage: Any = payload.get("usage")
    if isinstance(usage, dict):
        holders.append(usage)
    tokens: Any = payload.get("tokens")
    if isinstance(tokens, dict):
        holders.append(tokens)
        cache: Any = tokens.get("cache")
        if isinstance(cache, dict):
            holders.append(cache)
    for field_name, keys in USAGE_KEYS.items():
        if record.get(field_name) is not None:
            continue
        for holder in holders:
            extracted: int | float | None = _first_number(holder, keys)
            if extracted is None:
                continue
            if field_name == "cost_usd":
                record[field_name] = float(extracted)
            else:
                record[field_name] = int(extracted)
            break


def _first_number(holder: dict[str, Any], keys: tuple[str, ...]) -> int | float | None:
    for key in keys:
        value: Any = holder.get(key)
        if isinstance(value, bool):
            continue
        if isinstance(value, int):
            return value
        if isinstance(value, float):
            return value
    return None


def _is_step_finish(event: dict[str, Any]) -> bool:
    type_name: str = str(event.get("type") or "")
    if "step" in type_name and "finish" in type_name:
        return True
    part: dict[str, Any] = _event_part(event)
    part_type: str = str(part.get("type") or "")
    return part_type == "step-finish" or ("step" in part_type and "finish" in part_type)


def _is_tool_event(event: dict[str, Any]) -> bool:
    type_name: str = str(event.get("type") or "").lower()
    if "tool" in type_name and "result" in type_name:
        return True
    if type_name in {"tool_call", "tool.use", "tool-call"}:
        return True
    part: dict[str, Any] = _event_part(event)
    part_type: str = str(part.get("type") or "").lower()
    return part_type in {"tool", "tool-call", "tool_call"} or (
        "tool" in part_type and "start" in part_type
    )


def _event_part(event: dict[str, Any]) -> dict[str, Any]:
    part: Any = event.get("part")
    if isinstance(part, dict):
        return part
    properties: dict[str, Any] = _as_object(event.get("properties"))
    nested: Any = properties.get("part")
    if isinstance(nested, dict):
        return nested
    return _as_object(event.get("payload"))


def _event_usage(event: dict[str, Any]) -> dict[str, Any] | None:
    holders: list[dict[str, Any]] = [event, _event_part(event)]
    properties: dict[str, Any] = _as_object(event.get("properties"))
    holders.append(properties)
    usage: dict[str, Any] = {
        "tokens_in": 0,
        "tokens_out": 0,
        "tokens_cached": 0,
        "tokens_reasoning": 0,
        "cost_usd": 0.0,
    }
    found: bool = False
    for holder in holders:
        if not holder:
            continue
        cost: Any = holder.get("cost")
        if isinstance(cost, (int, float)):
            usage["cost_usd"] = float(cost)
            found = True
        tokens: Any = holder.get("tokens")
        token_holder: dict[str, Any] = tokens if isinstance(tokens, dict) else holder
        extracted_in: int | float | None = _first_number(token_holder, USAGE_KEYS["tokens_in"])
        extracted_out: int | float | None = _first_number(token_holder, USAGE_KEYS["tokens_out"])
        cache: Any = token_holder.get("cache")
        cache_holder: dict[str, Any] = cache if isinstance(cache, dict) else token_holder
        extracted_cached: int | float | None = _first_number(
            cache_holder, ("read",) + USAGE_KEYS["tokens_cached"]
        )
        extracted_reasoning: int | float | None = _first_number(
            token_holder, USAGE_KEYS["tokens_reasoning"]
        )
        if extracted_in is not None:
            usage["tokens_in"] = int(extracted_in)
            found = True
        if extracted_out is not None:
            usage["tokens_out"] = int(extracted_out)
            found = True
        if extracted_cached is not None:
            usage["tokens_cached"] = int(extracted_cached)
            found = True
        if extracted_reasoning is not None:
            usage["tokens_reasoning"] = int(extracted_reasoning)
            found = True
    if not found:
        return None
    return usage


def _markdown_table(records: list[dict[str, Any]]) -> str:
    headers: tuple[str, ...] = (
        "unit",
        "lane",
        "adapter",
        "model",
        "effort",
        "role",
        "wall_s",
        "steps",
        "tool_calls",
        "tokens_in",
        "tokens_out",
        "tokens_cached",
        "cost_usd",
        "exit",
        "commits",
    )
    lines: list[str] = [
        "| " + " | ".join(headers) + " |",
        "|" + "|".join("---" for _ in headers) + "|",
    ]
    for record in records:
        cells: list[str] = []
        for header in headers:
            value: Any = record.get(header)
            if value is None:
                cells.append("—")
            elif isinstance(value, float):
                cells.append(f"{value:.1f}")
            else:
                cells.append(str(value))
        lines.append("| " + " | ".join(cells) + " |")
    return "\n".join(lines) + "\n"


if __name__ == "__main__":
    raise SystemExit(main())
