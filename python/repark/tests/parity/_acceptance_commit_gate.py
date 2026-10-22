from __future__ import annotations

import enum
import http.client
import json
import re
import socket
import socketserver
import ssl
import struct
import threading
from dataclasses import dataclass, field
from http.server import BaseHTTPRequestHandler
from typing import Any
from urllib.parse import parse_qs, unquote, urlsplit

from _acceptance_sigv4 import Credentials, amz_date, sign

GLUE = "glue"
S3TABLES = "s3tables"
GLUE_COMMIT_TARGET = "AWSGlue.UpdateTable"
S3TABLES_COMMIT_SUFFIX = "/metadata-location"
REGIONAL_HOST = re.compile(r"^(glue|s3tables)\.([a-z]{2}(?:-[a-z]+)+-\d)\.amazonaws\.com$")
GLUE_DELETES = frozenset(
    {
        "DeleteTable",
        "BatchDeleteTable",
        "DeleteDatabase",
        "DeleteTableVersion",
        "BatchDeleteTableVersion",
        "DeletePartition",
        "BatchDeletePartition",
    }
)
SIGNED_PASSTHROUGH = ("content-type", "x-amz-target")
REPLY_PASSTHROUGH = ("content-type", "x-amzn-requestid", "x-amzn-errortype", "x-amz-request-id")
THROTTLE_REPLIES: dict[str, tuple[int, str, str, bytes]] = {
    GLUE: (
        400,
        "application/x-amz-json-1.1",
        "ThrottlingException",
        b'{"__type":"ThrottlingException","Message":"Rate exceeded"}',
    ),
    S3TABLES: (
        429,
        "application/json",
        "TooManyRequestsException",
        b'{"message":"Rate exceeded"}',
    ),
}


class Script(enum.Enum):
    FORWARD = "U-C0"
    LANDED_READS_OPEN = "U-1"
    LANDED_READS_CUT = "U-2"
    ABSENT_READS_CUT = "U-3"
    ABSENT_READS_OPEN = "U-4"
    LANDED_RESENDS_FORWARDED = "U-5"
    LANDED_THROTTLED_RESEND = "U-5r"
    HOLD = "hold"


@dataclass(frozen=True)
class Upstream:
    host: str
    port: int
    tls: bool
    service: str
    region: str

    def host_header(self) -> str:
        default = 443 if self.tls else 80
        return self.host if self.port == default else f"{self.host}:{self.port}"


def regional_upstream(service: str, region: str) -> Upstream:
    host = f"{service}.{region}.amazonaws.com"
    match = REGIONAL_HOST.match(host)
    if match is None or match.group(1) != service or match.group(2) != region:
        msg = f"the gate forwards only to a region's Glue and S3 Tables endpoints, not {host!r}"
        raise ValueError(msg)
    return Upstream(host=host, port=443, tls=True, service=service, region=region)


def loopback_upstream(port: int, service: str, region: str) -> Upstream:
    return Upstream(host="127.0.0.1", port=port, tls=False, service=service, region=region)


@dataclass
class Exchange:
    method: str
    path: str
    operation: str
    commit: bool
    forwarded: bool
    answer: str
    status: int | None = None
    metadata_location: str | None = None


@dataclass
class Request:
    method: str
    path: str
    query: str
    headers: list[tuple[str, str]]
    body: bytes = b""

    def header(self, name: str) -> str | None:
        for key, value in self.headers:
            if key.lower() == name:
                return value
        return None


@dataclass
class Reply:
    status: int
    headers: list[tuple[str, str]]
    body: bytes


@dataclass
class GateState:
    script: Script = Script.FORWARD
    cutting: bool = False
    commits_seen: int = 0
    exchanges: list[Exchange] = field(default_factory=list)
    refusals: list[str] = field(default_factory=list)


def glue_operation(request: Request) -> str:
    target = request.header("x-amz-target") or ""
    return target.removeprefix("AWSGlue.")


def s3tables_operation(request: Request) -> str:
    segments = [unquote(segment) for segment in request.path.split("/") if segment]
    head = segments[0] if segments else ""
    return f"{request.method} {head}"


def is_commit(service: str, request: Request) -> bool:
    if service == GLUE:
        return (request.header("x-amz-target") or "") == GLUE_COMMIT_TARGET
    return request.method == "PUT" and request.path.endswith(S3TABLES_COMMIT_SUFFIX)


def json_body(request: Request) -> dict[str, Any]:
    if not request.body:
        return {}
    try:
        parsed = json.loads(request.body)
    except ValueError:
        return {}
    return parsed if isinstance(parsed, dict) else {}


def glue_refusal(request: Request, namespace: str) -> str | None:
    operation = glue_operation(request)
    if operation in GLUE_DELETES:
        return f"refused Glue {operation}: the drills never delete"
    body = json_body(request)
    names = [body.get("DatabaseName"), (body.get("DatabaseInput") or {}).get("Name")]
    for name in names:
        if name is not None and name != namespace:
            return f"refused Glue {operation}: database {name!r} is not the scratch namespace"
    return None


def s3tables_names(request: Request) -> tuple[list[str], list[str]]:
    segments = [unquote(segment) for segment in request.path.split("/") if segment]
    query = parse_qs(request.query)
    arns = [segment for segment in segments if segment.startswith("arn:")]
    arns += query.get("tableBucketARN", [])
    namespaces = query.get("namespace", [])
    if segments and segments[0] in ("tables", "namespaces") and len(segments) >= 3:
        namespaces.append(segments[2])
    return arns, namespaces


def s3tables_refusal(request: Request, namespace: str, table_bucket_arn: str | None) -> str | None:
    if request.method == "DELETE":
        return "refused S3 Tables DELETE: the drills never delete"
    arns, namespaces = s3tables_names(request)
    for arn in arns:
        if table_bucket_arn is not None and arn != table_bucket_arn:
            return "refused S3 Tables request: a table bucket other than the configured one"
    for name in namespaces:
        if name != namespace:
            return f"refused S3 Tables request: namespace {name!r} is not the scratch namespace"
    return None


def commit_metadata_location(service: str, request: Request) -> str | None:
    body = json_body(request)
    if service == GLUE:
        parameters = (body.get("TableInput") or {}).get("Parameters") or {}
        return parameters.get("metadata_location")
    return body.get("metadataLocation")


def resign(request: Request, upstream: Upstream, credentials: Credentials) -> list[tuple[str, str]]:
    kept = [(name, value) for name, value in request.headers if name.lower() in SIGNED_PASSTHROUGH]
    headers = [("Host", upstream.host_header()), *kept]
    signed = sign(
        method=request.method,
        path=request.path,
        query=request.query,
        headers=headers,
        body=request.body,
        credentials=credentials,
        region=upstream.region,
        service=upstream.service,
        stamp=amz_date(),
        normalize=False,
    )
    return list(signed.headers)


def forward(request: Request, upstream: Upstream, credentials: Credentials) -> Reply:
    headers = resign(request, upstream, credentials)
    target = request.path + (f"?{request.query}" if request.query else "")
    connection: http.client.HTTPConnection
    if upstream.tls:
        connection = http.client.HTTPSConnection(
            upstream.host, upstream.port, timeout=60, context=ssl.create_default_context()
        )
    else:
        connection = http.client.HTTPConnection(upstream.host, upstream.port, timeout=60)
    try:
        connection.putrequest(request.method, target, skip_host=True, skip_accept_encoding=True)
        for name, value in headers:
            connection.putheader(name, value)
        connection.putheader("Content-Length", str(len(request.body)))
        connection.endheaders(request.body)
        response = connection.getresponse()
        body = response.read()
        kept = [
            (name, value)
            for name, value in response.getheaders()
            if name.lower() in REPLY_PASSTHROUGH
        ]
        return Reply(response.status, kept, body)
    finally:
        connection.close()


class GateServer(socketserver.ThreadingMixIn, socketserver.TCPServer):
    daemon_threads = True
    allow_reuse_address = True

    def __init__(self, gate: CommitGate) -> None:
        self.gate = gate
        self.reset_sockets: set[int] = set()
        super().__init__(("127.0.0.1", 0), GateHandler)

    def shutdown_request(self, request: Any) -> None:
        if id(request) in self.reset_sockets:
            self.reset_sockets.discard(id(request))
            request.close()
            return
        super().shutdown_request(request)


class GateHandler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"
    server: GateServer

    def log_message(self, format: str, *args: Any) -> None:
        return

    def _request(self) -> Request:
        split = urlsplit(self.path)
        length = int(self.headers.get("Content-Length") or 0)
        body = self.rfile.read(length) if length else b""
        return Request(
            method=self.command,
            path=split.path or "/",
            query=split.query,
            headers=list(self.headers.items()),
            body=body,
        )

    def reset(self) -> None:
        self.connection.setsockopt(socket.SOL_SOCKET, socket.SO_LINGER, struct.pack("ii", 1, 0))
        self.server.reset_sockets.add(id(self.connection))
        self.close_connection = True

    def reply(self, reply: Reply) -> None:
        self.send_response(reply.status)
        for name, value in reply.headers:
            self.send_header(name, value)
        self.send_header("Content-Length", str(len(reply.body)))
        self.send_header("Connection", "close")
        self.end_headers()
        self.wfile.write(reply.body)
        self.close_connection = True

    def handle_any(self) -> None:
        self.server.gate.handle(self, self._request())


for _verb in ("GET", "POST", "PUT", "DELETE", "HEAD"):
    setattr(GateHandler, f"do_{_verb}", GateHandler.handle_any)


class CommitGate:
    def __init__(
        self,
        *,
        upstream: Upstream,
        credentials: Credentials,
        namespace: str,
        table_bucket_arn: str | None = None,
    ) -> None:
        self.upstream = upstream
        self.credentials = credentials
        self.namespace = namespace
        self.table_bucket_arn = table_bucket_arn
        self.state = GateState()
        self.lock = threading.Lock()
        self.held = threading.Event()
        self.release_hold = threading.Event()
        self.server = GateServer(self)
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)

    def __enter__(self) -> CommitGate:
        self.thread.start()
        return self

    def __exit__(self, *exc: object) -> None:
        self.release_hold.set()
        self.server.shutdown()
        self.server.server_close()

    def __repr__(self) -> str:
        return f"CommitGate(service={self.upstream.service!r}, script={self.state.script.value!r})"

    @property
    def url(self) -> str:
        return f"http://127.0.0.1:{self.server.server_address[1]}"

    def arm(self, script: Script) -> None:
        with self.lock:
            self.state = GateState(script=script)
        self.held.clear()
        self.release_hold.clear()

    def stop_cutting(self) -> None:
        with self.lock:
            self.state.cutting = False
        self.release_hold.set()

    def commits(self) -> list[Exchange]:
        with self.lock:
            return [exchange for exchange in self.state.exchanges if exchange.commit]

    def exchanges(self) -> list[Exchange]:
        with self.lock:
            return list(self.state.exchanges)

    def refusals(self) -> list[str]:
        with self.lock:
            return list(self.state.refusals)

    def operation(self, request: Request) -> str:
        if self.upstream.service == GLUE:
            return glue_operation(request)
        return s3tables_operation(request)

    def refusal(self, request: Request) -> str | None:
        if self.upstream.service == GLUE:
            return glue_refusal(request, self.namespace)
        return s3tables_refusal(request, self.namespace, self.table_bucket_arn)

    def handle(self, handler: GateHandler, request: Request) -> None:
        operation = self.operation(request)
        refused = self.refusal(request)
        if refused is not None:
            with self.lock:
                self.state.refusals.append(refused)
            handler.reply(
                Reply(
                    403,
                    [("x-amzn-ErrorType", "AccessDeniedException")],
                    json.dumps({"__type": "AccessDeniedException", "message": refused}).encode(),
                )
            )
            return
        commit = is_commit(self.upstream.service, request)
        exchange = Exchange(
            method=request.method,
            path=request.path,
            operation=operation,
            commit=commit,
            forwarded=False,
            answer="",
            metadata_location=commit_metadata_location(self.upstream.service, request)
            if commit
            else None,
        )
        with self.lock:
            self.state.exchanges.append(exchange)
            ordinal = self.state.commits_seen
            if commit:
                self.state.commits_seen += 1
            script = self.state.script
            cutting = self.state.cutting
        if commit:
            self.commit(handler, request, exchange, script, ordinal)
        elif cutting:
            exchange.answer = "reset"
            handler.reset()
        else:
            self.pass_through(handler, request, exchange)

    def pass_through(self, handler: GateHandler, request: Request, exchange: Exchange) -> None:
        reply = forward(request, self.upstream, self.credentials)
        exchange.forwarded = True
        exchange.status = reply.status
        exchange.answer = "forwarded"
        handler.reply(reply)

    def landed_then_reset(self, handler: GateHandler, request: Request, exchange: Exchange) -> None:
        reply = forward(request, self.upstream, self.credentials)
        exchange.forwarded = True
        exchange.status = reply.status
        exchange.answer = "forwarded, reply reset"
        handler.reset()

    def commit(
        self,
        handler: GateHandler,
        request: Request,
        exchange: Exchange,
        script: Script,
        ordinal: int,
    ) -> None:
        if script is Script.FORWARD:
            self.pass_through(handler, request, exchange)
            return
        if ordinal > 0:
            self.later_commit(handler, request, exchange, script)
            return
        if script is Script.HOLD:
            self.held.set()
            self.release_hold.wait()
            exchange.answer = "held, then reset"
            handler.reset()
            return
        if script in (Script.ABSENT_READS_CUT, Script.ABSENT_READS_OPEN):
            exchange.answer = "dropped, reset"
            handler.reset()
        elif script is Script.LANDED_THROTTLED_RESEND:
            forward(request, self.upstream, self.credentials)
            exchange.forwarded = True
            status, content_type, code, body = THROTTLE_REPLIES[self.upstream.service]
            exchange.status = status
            exchange.answer = f"forwarded, answered {code}"
            handler.reply(
                Reply(status, [("Content-Type", content_type), ("x-amzn-ErrorType", code)], body)
            )
        else:
            self.landed_then_reset(handler, request, exchange)
        if script in (Script.LANDED_READS_CUT, Script.ABSENT_READS_CUT):
            with self.lock:
                self.state.cutting = True

    def later_commit(
        self, handler: GateHandler, request: Request, exchange: Exchange, script: Script
    ) -> None:
        if script in (Script.LANDED_RESENDS_FORWARDED, Script.LANDED_THROTTLED_RESEND):
            self.pass_through(handler, request, exchange)
            return
        exchange.answer = "re-send reset unforwarded"
        handler.reset()
