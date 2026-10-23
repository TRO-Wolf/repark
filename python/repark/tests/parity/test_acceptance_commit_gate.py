from __future__ import annotations

import http.client
import json
import threading
from collections.abc import Callable, Iterator
from dataclasses import dataclass
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any
from urllib.parse import quote

import pytest
from _acceptance_commit_gate import (
    GLUE,
    S3TABLES,
    CommitGate,
    Request,
    Script,
    is_commit,
    loopback_upstream,
    regional_upstream,
)
from _acceptance_sigv4 import Credentials, canonical_uri, sign

NAMESPACE = "testing_repark_acceptance"
REGION = "us-east-1"
ARN = "arn:aws:s3tables:us-east-1:000000000000:bucket/testing-gate"
SECRET = "gate-secret-never-logged"
TOKEN = "gate-token-never-logged"
CLIENT_SIGNATURE = "AWS4-HMAC-SHA256 client-signature-must-not-pass"
DUMMY = Credentials("AKIDGATETEST", SECRET, TOKEN)
VECTORS = json.loads(Path(__file__).with_name("sigv4_v4_vectors.json").read_text(encoding="utf-8"))


@dataclass
class Seen:
    method: str
    path: str
    headers: dict[str, str]
    body: bytes


class FakeUpstream:
    def __init__(self, answer: Callable[[Seen, int], tuple[int, dict[str, str], bytes]]) -> None:
        self.seen: list[Seen] = []
        self.lock = threading.Lock()
        self.answer = answer
        upstream = self

        class Handler(BaseHTTPRequestHandler):
            protocol_version = "HTTP/1.1"

            def log_message(self, format: str, *args: Any) -> None:
                return

            def handle_any(self) -> None:
                length = int(self.headers.get("Content-Length") or 0)
                seen = Seen(
                    self.command,
                    self.path,
                    {key.lower(): value for key, value in self.headers.items()},
                    self.rfile.read(length) if length else b"",
                )
                with upstream.lock:
                    upstream.seen.append(seen)
                    ordinal = sum(1 for item in upstream.seen if item.method == seen.method)
                status, headers, body = upstream.answer(seen, ordinal)
                self.send_response(status)
                for key, value in headers.items():
                    self.send_header(key, value)
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)

        for verb in ("GET", "POST", "PUT"):
            setattr(Handler, f"do_{verb}", Handler.handle_any)
        self.server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()

    @property
    def port(self) -> int:
        return int(self.server.server_address[1])

    def commits(self, service: str) -> list[Seen]:
        with self.lock:
            return [
                seen
                for seen in self.seen
                if is_commit(
                    service,
                    Request(seen.method, seen.path.split("?")[0], "", list(seen.headers.items())),
                )
            ]

    def close(self) -> None:
        self.server.shutdown()
        self.server.server_close()


def ok_answer(seen: Seen, ordinal: int) -> tuple[int, dict[str, str], bytes]:
    if seen.headers.get("x-amz-target") == "AWSGlue.GetDatabases":
        return (
            200,
            {"Content-Type": "application/x-amz-json-1.1"},
            json.dumps({"DatabaseList": [{"Name": NAMESPACE}]}).encode(),
        )
    return 200, {"Content-Type": "application/json"}, b"{}"


def stale_second_commit(seen: Seen, ordinal: int) -> tuple[int, dict[str, str], bytes]:
    if ordinal > 1:
        return 409, {"x-amzn-ErrorType": "ConflictException"}, b'{"message":"stale"}'
    return ok_answer(seen, ordinal)


@pytest.fixture
def upstream() -> Iterator[FakeUpstream]:
    fake = FakeUpstream(ok_answer)
    yield fake
    fake.close()


def gate_for(service: str, fake: FakeUpstream) -> CommitGate:
    return CommitGate(
        upstream=loopback_upstream(fake.port, service, REGION),
        credentials=DUMMY,
        namespace=NAMESPACE,
        table_bucket_arn=ARN,
    )


def call(gate: CommitGate, method: str, path: str, headers: dict[str, str], body: bytes) -> str:
    connection = http.client.HTTPConnection("127.0.0.1", gate.server.server_address[1], timeout=10)
    try:
        connection.request(
            method,
            path,
            body=body,
            headers={
                **headers,
                "Authorization": CLIENT_SIGNATURE,
                "X-Amz-Date": "20260101T000000Z",
                "X-Amz-Security-Token": "client-token-must-not-pass",
            },
        )
        response = connection.getresponse()
        response.read()
        return f"reply {response.status}"
    except (ConnectionError, http.client.HTTPException):
        return "reset"
    finally:
        connection.close()


def glue_call(gate: CommitGate, operation: str, body: dict[str, Any]) -> str:
    return call(
        gate,
        "POST",
        "/",
        {"Content-Type": "application/x-amz-json-1.1", "X-Amz-Target": f"AWSGlue.{operation}"},
        json.dumps(body).encode(),
    )


def glue_commit_body(namespace: str = NAMESPACE) -> dict[str, Any]:
    return {
        "DatabaseName": namespace,
        "VersionId": "1",
        "TableInput": {
            "Name": "testing_t",
            "Parameters": {"metadata_location": "s3://warehouse/t/metadata/00002-a.metadata.json"},
        },
    }


def s3tables_path(namespace: str = NAMESPACE, arn: str = ARN, tail: str = "") -> str:
    return f"/tables/{quote(arn, safe='')}/{namespace}/testing_t{tail}"


def s3tables_commit(gate: CommitGate, namespace: str = NAMESPACE, arn: str = ARN) -> str:
    body = {"metadataLocation": "s3://bucket/metadata/00002-b.metadata.json", "versionToken": "v1"}
    return call(
        gate,
        "PUT",
        s3tables_path(namespace, arn, "/metadata-location"),
        {"Content-Type": "application/json"},
        json.dumps(body).encode(),
    )


def s3tables_read(gate: CommitGate) -> str:
    query = f"tableBucketARN={quote(ARN, safe='')}&namespace={NAMESPACE}&name=testing_t"
    return call(gate, "GET", f"/get-table?{query}", {}, b"")


def read(gate: CommitGate, service: str) -> str:
    if service == GLUE:
        return glue_call(gate, "GetTable", {"DatabaseName": NAMESPACE, "Name": "testing_t"})
    return s3tables_read(gate)


def commit(gate: CommitGate, service: str) -> str:
    if service == GLUE:
        return glue_call(gate, "UpdateTable", glue_commit_body())
    return s3tables_commit(gate)


SCRIPT_TABLE: dict[Script, tuple[list[str], list[bool], int]] = {
    Script.FORWARD: (
        ["reply 200", "reply 200", "reply 200", "reply 200", "reply 200"],
        [True, True],
        2,
    ),
    Script.LANDED_READS_OPEN: (
        ["reply 200", "reset", "reply 200", "reset", "reply 200"],
        [True, False],
        1,
    ),
    Script.LANDED_READS_CUT: (
        ["reply 200", "reset", "reset", "reset", "reply 200"],
        [True, False],
        1,
    ),
    Script.ABSENT_READS_CUT: (
        ["reply 200", "reset", "reset", "reset", "reply 200"],
        [False, False],
        0,
    ),
    Script.ABSENT_READS_OPEN: (
        ["reply 200", "reset", "reply 200", "reset", "reply 200"],
        [False, False],
        0,
    ),
    Script.LANDED_RESENDS_FORWARDED: (
        ["reply 200", "reset", "reply 200", "reply 200", "reply 200"],
        [True, True],
        2,
    ),
}


@pytest.mark.parametrize("vector", VECTORS["vectors"], ids=lambda vector: vector["name"])
def test_signer_matches_the_published_aws_vector(vector: dict[str, Any]) -> None:
    lines = vector["canonical_request"].split("\n")
    if "signature" not in vector:
        assert canonical_uri(vector["path"]) == lines[1]
        return
    signed = sign(
        method=vector["method"],
        path=vector["path"],
        query=vector["query"],
        headers=[(key, value) for key, value in vector["headers"] if key.lower() != "x-amz-date"],
        body=vector["body"].encode(),
        credentials=Credentials(vector["access_key"], vector["secret_key"], vector["token"]),
        region=vector["region"],
        service=vector["service"],
        stamp=vector["stamp"],
    )
    assert signed.canonical_request == vector["canonical_request"]
    assert signed.string_to_sign == vector["string_to_sign"]
    assert signed.signature == vector["signature"]


def test_the_vector_set_covers_the_gate_paths() -> None:
    names = {vector["name"] for vector in VECTORS["vectors"]}
    assert len(names) == 28
    assert {
        "double-encode-path",
        "get-vanilla-with-session-token",
        "post-vanilla",
        "post-x-www-form-urlencoded",
        "post-x-www-form-urlencoded-parameters",
    } <= names


@pytest.mark.parametrize(
    ("service", "region"),
    [
        (GLUE, "us-east-1"),
        (S3TABLES, "us-east-2"),
        (GLUE, "eu-central-1"),
        (S3TABLES, "ap-southeast-2"),
    ],
)
def test_the_regional_endpoint_is_allowed(service: str, region: str) -> None:
    upstream = regional_upstream(service, region)
    assert upstream.host == f"{service}.{region}.amazonaws.com"
    assert upstream.tls
    assert upstream.port == 443


@pytest.mark.parametrize(
    ("service", "region"),
    [
        ("s3", "us-east-1"),
        ("sts", "us-east-1"),
        (GLUE, "127.0.0.1"),
        (GLUE, "us-east-1.evil.example"),
        (S3TABLES, "localhost"),
        (GLUE, ""),
    ],
)
def test_every_other_endpoint_is_refused(service: str, region: str) -> None:
    with pytest.raises(ValueError, match="forwards only"):
        regional_upstream(service, region)


@pytest.mark.parametrize(
    ("service", "method", "path", "target", "expected"),
    [
        (GLUE, "POST", "/", "AWSGlue.UpdateTable", True),
        (GLUE, "POST", "/", "AWSGlue.GetTable", False),
        (GLUE, "POST", "/", "AWSGlue.CreateTable", False),
        (S3TABLES, "PUT", "/tables/a/ns/t/metadata-location", None, True),
        (S3TABLES, "GET", "/tables/a/ns/t/metadata-location", None, False),
        (S3TABLES, "PUT", "/tables/a/ns/t", None, False),
    ],
)
def test_the_commit_request_is_recognised(
    service: str, method: str, path: str, target: str | None, expected: bool
) -> None:
    headers = [("X-Amz-Target", target)] if target else []
    assert is_commit(service, Request(method, path, "", headers, b"")) is expected


def test_glue_refuses_another_database_and_every_delete(upstream: FakeUpstream) -> None:
    with gate_for(GLUE, upstream) as gate:
        refused = [
            glue_call(gate, "UpdateTable", glue_commit_body("example_silver")),
            glue_call(gate, "CreateTable", {"DatabaseName": "other", "TableInput": {"Name": "t"}}),
            glue_call(gate, "CreateDatabase", {"DatabaseInput": {"Name": "other"}}),
            glue_call(gate, "DeleteTable", {"DatabaseName": NAMESPACE, "Name": "t"}),
        ]
        assert refused == ["reply 403"] * 4
        assert glue_call(gate, "GetDatabases", {}) == "reply 200"
        assert len(gate.refusals()) == 4
        assert gate.commits() == []
    assert [seen.headers["x-amz-target"] for seen in upstream.seen] == ["AWSGlue.GetDatabases"]


def test_s3tables_refuses_another_namespace_bucket_and_every_delete(upstream: FakeUpstream) -> None:
    other_arn = "arn:aws:s3tables:us-east-1:000000000000:bucket/production"
    with gate_for(S3TABLES, upstream) as gate:
        assert s3tables_commit(gate, namespace="example_silver") == "reply 403"
        assert s3tables_commit(gate, arn=other_arn) == "reply 403"
        query = f"tableBucketARN={quote(ARN, safe='')}&namespace=other&name=t"
        assert call(gate, "GET", f"/get-table?{query}", {}, b"") == "reply 403"
        assert call(gate, "DELETE", s3tables_path(), {}, b"") == "reply 403"
        assert s3tables_read(gate) == "reply 200"
        assert len(gate.refusals()) == 4
    assert [seen.method for seen in upstream.seen] == ["GET"]


GLUE_REFUSED: list[tuple[str, str, bytes]] = [
    (
        "update-another-database",
        "UpdateDatabase",
        json.dumps({"Name": "prod", "DatabaseInput": {"Name": NAMESPACE}}).encode(),
    ),
    (
        "foreign-catalog-id",
        "CreateTable",
        json.dumps(
            {"CatalogId": "999999999999", "DatabaseName": NAMESPACE, "TableInput": {"Name": "t"}}
        ).encode(),
    ),
    (
        "unlisted-delete-column-statistics",
        "DeleteColumnStatisticsForTable",
        json.dumps({"DatabaseName": NAMESPACE, "TableName": "t", "ColumnName": "id"}).encode(),
    ),
    (
        "unlisted-delete-table-optimizer",
        "DeleteTableOptimizer",
        json.dumps({"DatabaseName": NAMESPACE, "TableName": "t", "Type": "compaction"}).encode(),
    ),
    ("unlisted-delete-connection", "DeleteConnection", b'{"ConnectionName": "prod-conn"}'),
    ("catalog-wide-search", "SearchTables", b'{"SearchText": "x"}'),
    (
        "duplicate-database-key",
        "GetTable",
        f'{{"DatabaseName":"prod","DatabaseName":"{NAMESPACE}","Name":"t"}}'.encode(),
    ),
    ("unparsable-body", "GetTable", b"not json"),
    (
        "create-table-in-another-database",
        "CreateTable",
        json.dumps({"DatabaseName": "other", "TableInput": {"Name": "t"}}).encode(),
    ),
]
GLUE_ALLOWED_CALLS: list[tuple[str, dict[str, Any]]] = [
    ("GetDatabases", {}),
    ("GetDatabase", {"Name": NAMESPACE}),
    ("GetTables", {"DatabaseName": NAMESPACE}),
    ("GetTable", {"DatabaseName": NAMESPACE, "Name": "testing_t"}),
    ("CreateTable", {"DatabaseName": NAMESPACE, "TableInput": {"Name": "testing_t"}}),
    ("UpdateTable", glue_commit_body()),
    ("GetDatabase", {"Name": "prod"}),
    ("GetTable", {"DatabaseName": "other", "Name": "t"}),
    ("GetTables", {"DatabaseName": "default"}),
]
QUOTED_ARN = quote(ARN, safe="")
S3TABLES_REFUSED: list[tuple[str, str, str, bytes]] = [
    (
        "create-another-namespace",
        "PUT",
        f"/namespaces/{QUOTED_ARN}",
        json.dumps({"namespace": ["prod"]}).encode(),
    ),
    (
        "rename-out-of-scratch",
        "PUT",
        f"/tables/{QUOTED_ARN}/{NAMESPACE}/testing_t/rename",
        json.dumps({"newNamespaceName": "prod", "newName": "t"}).encode(),
    ),
    ("list-tables-in-every-namespace", "GET", f"/tables/{QUOTED_ARN}", b""),
    (
        "bucket-wide-maintenance-write",
        "PUT",
        f"/buckets/{QUOTED_ARN}/maintenance/icebergUnreferencedFileRemoval",
        b'{"value":{"status":"disabled"}}',
    ),
    (
        "metadata-location-in-another-namespace",
        "GET",
        f"/tables/{QUOTED_ARN}/other/testing_t/metadata-location",
        b"",
    ),
    (
        "get-table-by-table-arn",
        "GET",
        f"/get-table?tableArn={quote(ARN + '/table/x', safe='')}",
        b"",
    ),
    (
        "get-table-repeats-the-namespace",
        "GET",
        f"/get-table?tableBucketARN={QUOTED_ARN}&namespace={NAMESPACE}&namespace=prod&name=t",
        b"",
    ),
]
S3TABLES_ALLOWED_CALLS: list[tuple[str, str, bytes]] = [
    ("GET", f"/get-table?tableBucketARN={QUOTED_ARN}&namespace={NAMESPACE}&name=testing_t", b""),
    ("GET", f"/tables/{QUOTED_ARN}/{NAMESPACE}/testing_t/metadata-location", b""),
    (
        "PUT",
        f"/tables/{QUOTED_ARN}/{NAMESPACE}/testing_t/metadata-location",
        b'{"metadataLocation":"s3://b/m.json","versionToken":"v"}',
    ),
    ("GET", f"/tables/{QUOTED_ARN}?namespace={NAMESPACE}", b""),
    ("PUT", f"/tables/{QUOTED_ARN}/{NAMESPACE}", b'{"name":"testing_t","format":"ICEBERG"}'),
    ("GET", f"/namespaces/{QUOTED_ARN}", b""),
    ("GET", f"/namespaces/{QUOTED_ARN}/{NAMESPACE}", b""),
    ("PUT", f"/namespaces/{QUOTED_ARN}", json.dumps({"namespace": [NAMESPACE]}).encode()),
]


@pytest.mark.parametrize(
    ("operation", "body"),
    [case[1:] for case in GLUE_REFUSED],
    ids=[case[0] for case in GLUE_REFUSED],
)
def test_glue_refuses_every_request_the_drills_do_not_send(
    operation: str, body: bytes, upstream: FakeUpstream
) -> None:
    headers = {"Content-Type": "application/x-amz-json-1.1", "X-Amz-Target": f"AWSGlue.{operation}"}
    with gate_for(GLUE, upstream) as gate:
        assert call(gate, "POST", "/", headers, body) == "reply 403"
        assert len(gate.refusals()) == 1
    assert upstream.seen == []


@pytest.mark.parametrize(
    ("operation", "body"), GLUE_ALLOWED_CALLS, ids=[case[0] for case in GLUE_ALLOWED_CALLS]
)
def test_glue_forwards_the_operations_the_drills_send(
    operation: str, body: dict[str, Any], upstream: FakeUpstream
) -> None:
    with gate_for(GLUE, upstream) as gate:
        assert glue_call(gate, operation, body) == "reply 200"
        assert gate.refusals() == []
    assert [seen.headers["x-amz-target"] for seen in upstream.seen] == [f"AWSGlue.{operation}"]


@pytest.mark.parametrize(
    ("method", "path", "body"),
    [case[1:] for case in S3TABLES_REFUSED],
    ids=[case[0] for case in S3TABLES_REFUSED],
)
def test_s3tables_refuses_every_request_the_drills_do_not_send(
    method: str, path: str, body: bytes, upstream: FakeUpstream
) -> None:
    with gate_for(S3TABLES, upstream) as gate:
        assert call(gate, method, path, {"Content-Type": "application/json"}, body) == "reply 403"
        assert len(gate.refusals()) == 1
    assert upstream.seen == []


@pytest.mark.parametrize(("method", "path", "body"), S3TABLES_ALLOWED_CALLS)
def test_s3tables_forwards_the_operations_the_drills_send(
    method: str, path: str, body: bytes, upstream: FakeUpstream
) -> None:
    with gate_for(S3TABLES, upstream) as gate:
        assert call(gate, method, path, {"Content-Type": "application/json"}, body) == "reply 200"
        assert gate.refusals() == []
    assert [seen.method for seen in upstream.seen] == [method]


@pytest.mark.parametrize("service", [GLUE, S3TABLES])
@pytest.mark.parametrize("script", list(SCRIPT_TABLE), ids=lambda script: script.value)
def test_each_script_answers_as_its_cell_says(
    service: str, script: Script, upstream: FakeUpstream
) -> None:
    answers, forwarded, upstream_commits = SCRIPT_TABLE[script]
    with gate_for(service, upstream) as gate:
        gate.arm(script)
        seen = [read(gate, service), commit(gate, service)]
        seen += [read(gate, service), commit(gate, service)]
        gate.stop_cutting()
        seen.append(read(gate, service))
        assert seen == answers
        assert [exchange.forwarded for exchange in gate.commits()] == forwarded
    assert len(upstream.commits(service)) == upstream_commits


@pytest.mark.parametrize("service", [GLUE, S3TABLES])
def test_the_throttled_resend_script_lands_then_lies_then_forwards(service: str) -> None:
    fake = FakeUpstream(stale_second_commit)
    try:
        with gate_for(service, fake) as gate:
            gate.arm(Script.LANDED_THROTTLED_RESEND)
            first = commit(gate, service)
            second = commit(gate, service)
            commits = gate.commits()
        expected_status = 400 if service == GLUE else 429
        assert first == f"reply {expected_status}"
        assert second == "reply 409"
        assert [exchange.forwarded for exchange in commits] == [True, True]
        assert commits[0].answer.endswith(("ThrottlingException", "TooManyRequestsException"))
        assert len(fake.commits(service)) == 2
    finally:
        fake.close()


@pytest.mark.parametrize("service", [GLUE, S3TABLES])
def test_the_hold_script_holds_unforwarded_then_resets(
    service: str, upstream: FakeUpstream
) -> None:
    outcome: list[str] = []
    with gate_for(service, upstream) as gate:
        gate.arm(Script.HOLD)
        writer = threading.Thread(target=lambda: outcome.append(commit(gate, service)))
        writer.start()
        assert gate.held.wait(10)
        assert upstream.commits(service) == []
        assert writer.is_alive()
        gate.stop_cutting()
        writer.join(10)
    assert outcome == ["reset"]
    assert upstream.commits(service) == []


@pytest.mark.parametrize("service", [GLUE, S3TABLES])
def test_the_gate_signs_again_for_its_upstream(service: str, upstream: FakeUpstream) -> None:
    with gate_for(service, upstream) as gate:
        assert commit(gate, service) == "reply 200"
        exchange = gate.commits()[0]
    forwarded = upstream.seen[0]
    host = f"127.0.0.1:{upstream.port}"
    assert forwarded.headers["host"] == host
    assert forwarded.headers["x-amz-security-token"] == TOKEN
    authorization = forwarded.headers["authorization"]
    assert authorization != CLIENT_SIGNATURE
    stamp = forwarded.headers["x-amz-date"]
    assert f"Credential=AKIDGATETEST/{stamp[:8]}/{REGION}/{service}/aws4_request" in authorization
    kept = [("Host", host), ("Content-Type", forwarded.headers["content-type"])]
    if service == GLUE:
        kept.append(("X-Amz-Target", forwarded.headers["x-amz-target"]))
    path, _, query = forwarded.path.partition("?")
    expected = sign(
        method=forwarded.method,
        path=path,
        query=query,
        headers=kept,
        body=forwarded.body,
        credentials=DUMMY,
        region=REGION,
        service=service,
        stamp=stamp,
        normalize=False,
    )
    assert authorization.endswith(f"Signature={expected.signature}")
    assert exchange.metadata_location is not None
    assert exchange.metadata_location.endswith(".metadata.json")
    assert "client-token-must-not-pass" not in str(forwarded.headers)


@pytest.mark.parametrize("service", [GLUE, S3TABLES])
def test_no_header_or_credential_is_logged(
    service: str,
    upstream: FakeUpstream,
    capsys: pytest.CaptureFixture[str],
    caplog: pytest.LogCaptureFixture,
) -> None:
    with gate_for(service, upstream) as gate:
        gate.arm(Script.LANDED_READS_OPEN)
        commit(gate, service)
        read(gate, service)
        records = repr(gate.exchanges()) + repr(gate) + repr(gate.refusals())
    captured = capsys.readouterr()
    for text in (records, captured.out, captured.err, caplog.text):
        for secret in (SECRET, TOKEN, "AKIDGATETEST", CLIENT_SIGNATURE, "Authorization"):
            assert secret not in text


def test_a_repark_session_reaches_glue_through_the_gate(
    upstream: FakeUpstream, monkeypatch: pytest.MonkeyPatch
) -> None:
    from repark import ReparkSession

    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "AKIDSESSIONDUMMY")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "session-dummy-secret")
    monkeypatch.setenv("AWS_REGION", REGION)
    monkeypatch.setenv("AWS_EC2_METADATA_DISABLED", "true")
    with gate_for(GLUE, upstream) as gate:
        prefix = "spark.sql.catalog.gated"
        session = (
            ReparkSession.builder.appName("commit-gate-m3")
            .config(prefix, "org.apache.iceberg.spark.SparkCatalog")
            .config(f"{prefix}.type", "glue")
            .config(f"{prefix}.warehouse", "s3://example-warehouse/")
            .config(f"{prefix}.uri", gate.url)
            .getOrCreate()
        )
        try:
            names = session.sql("SHOW NAMESPACES IN gated").to_arrow().column(0).to_pylist()
        finally:
            session.stop()
        operations = [exchange.operation for exchange in gate.exchanges()]
    assert names == [NAMESPACE]
    assert "GetDatabases" in operations
    assert set(operations) <= {"GetDatabases", "GetTables"}
    assert len(upstream.seen) == len(operations)
    for forwarded in upstream.seen:
        assert forwarded.headers["host"] == f"127.0.0.1:{upstream.port}"
        assert "Credential=AKIDGATETEST/" in forwarded.headers["authorization"]
        assert "AKIDSESSIONDUMMY" not in str(forwarded.headers)
