use std::collections::BTreeMap;
use std::sync::Arc;

use futures::TryStreamExt;
use repark_connect::postgres::{PgTypeKind, TypeMod};
use repark_connect::{
    ConnectError, PgIdent, PoolLimits, PostgresConnector, PostgresSettings, QualifiedRelation,
    QueryPool, ResolvedSource, ScanColumn, ScanOptions, ScanRequest, ScanSource, SettingsDoor,
    scan,
};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

fn message(kind: u8, body: &[u8]) -> Vec<u8> {
    let length = i32::try_from(body.len() + 4).expect("a short message");
    let mut framed = vec![kind];
    framed.extend_from_slice(&length.to_be_bytes());
    framed.extend_from_slice(body);
    framed
}

fn copy_stream(trailer: bool) -> Vec<u8> {
    let mut data = b"PGCOPY\n\xff\r\n\0".to_vec();
    data.extend_from_slice(&[0; 8]);
    data.extend_from_slice(&1_i16.to_be_bytes());
    data.extend_from_slice(&4_i32.to_be_bytes());
    data.extend_from_slice(&7_i32.to_be_bytes());
    if trailer {
        data.extend_from_slice(&(-1_i16).to_be_bytes());
    }
    data
}

async fn serve(mut socket: TcpStream, trailer: bool) {
    let length = socket.read_i32().await.expect("startup length");
    let mut startup = vec![0; usize::try_from(length - 4).expect("startup")];
    socket.read_exact(&mut startup).await.expect("startup");
    let mut key = 7_i32.to_be_bytes().to_vec();
    key.extend_from_slice(&11_i32.to_be_bytes());
    let mut out = message(b'R', &0_i32.to_be_bytes());
    out.extend(message(b'K', &key));
    out.extend(message(b'Z', b"I"));
    socket.write_all(&out).await.expect("ready");
    let mut queued = Vec::new();
    loop {
        let Ok(kind) = socket.read_u8().await else {
            return;
        };
        let length = socket.read_i32().await.expect("length");
        let mut body = vec![0; usize::try_from(length - 4).expect("body")];
        socket.read_exact(&mut body).await.expect("body");
        match kind {
            b'P' => queued.extend(message(b'1', &[])),
            b'D' => {
                queued.extend(message(b't', &0_i16.to_be_bytes()));
                queued.extend(message(b'n', &[]));
            }
            b'B' => queued.extend(message(b'2', &[])),
            b'E' => {
                queued.extend(message(b'H', &[1, 0, 1, 0, 1]));
                queued.extend(message(b'd', &copy_stream(trailer)));
                queued.extend(message(b'c', &[]));
                queued.extend(message(b'C', b"COPY 1\0"));
            }
            b'C' => queued.extend(message(b'3', &[])),
            b'Q' => {
                queued.extend(message(b'E', b"SERROR\0CXX000\0Mfake\0\0"));
                queued.extend(message(b'Z', b"I"));
            }
            b'S' => {
                queued.extend(message(b'Z', b"I"));
                socket.write_all(&queued).await.expect("responses");
                queued.clear();
            }
            b'X' => return,
            _ => {}
        }
        if kind == b'Q' {
            socket.write_all(&queued).await.expect("responses");
            queued.clear();
        }
    }
}

async fn read_from_fake(trailer: bool) -> (Result<Vec<Option<i32>>, ConnectError>, usize) {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let port = listener.local_addr().expect("address").port().to_string();
    let server = tokio::spawn(async move {
        let (socket, _) = listener.accept().await.expect("accept");
        serve(socket, trailer).await;
    });
    let props: BTreeMap<String, String> = [
        ("host", "127.0.0.1"),
        ("port", port.as_str()),
        ("user", "app"),
        ("sslmode", "disable"),
        ("read_timeout_ms", "2000"),
    ]
    .into_iter()
    .map(|(key, value)| (key.to_string(), value.to_string()))
    .collect();
    let settings =
        PostgresSettings::from_props(&props, SettingsDoor::ReparkToml).expect("settings");
    let connector = PostgresConnector::new(&settings).expect("connector");
    let pool = QueryPool::new(connector, PoolLimits::from_settings(&settings));
    let id = PgIdent::new("id").expect("identifier");
    let column = ScanColumn::resolve(id, "int4", PgTypeKind::Base, TypeMod::new(-1), true);
    let relation = QualifiedRelation::new(
        PgIdent::new("s").expect("schema"),
        PgIdent::new("t").expect("table"),
    );
    let resolved = Arc::new(ResolvedSource {
        source: ScanSource::Relation(relation),
        columns: vec![column.expect("int4")],
        server_version_num: 160_000,
        server_encoding: "UTF8".into(),
    });
    let options = ScanOptions::from_settings(&settings);
    let batches: Result<Vec<_>, _> = scan(Arc::clone(&pool), ScanRequest::new(resolved), options)
        .try_collect()
        .await;
    let values = batches.map(|batches| {
        batches
            .iter()
            .flat_map(|batch| {
                let column = batch.column(0);
                arrow::array::AsArray::as_primitive::<arrow::datatypes::Int32Type>(column)
                    .iter()
                    .collect::<Vec<_>>()
            })
            .collect()
    });
    server.abort();
    (values, pool.idle_count())
}

#[tokio::test]
async fn a_copy_stream_without_its_trailer_fails_the_scan() {
    assert_eq!(read_from_fake(true).await, (Ok(vec![Some(7)]), 0));
    assert_eq!(
        read_from_fake(false).await,
        (Err(ConnectError::Disconnected), 0)
    );
}
