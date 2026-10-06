use crate::object_store_s3::{S3A_ENDPOINT_CONFIG_KEY, resolve_endpoint_from_dump};
use crate::session::ReparkSessionBuilder;

const JDBC_URI_KEY: &str = "spark.repark.test.jdbc_uri";

#[test]
fn conf_dump_masks_url_userinfo_while_the_endpoint_resolves_raw() {
    let session = ReparkSessionBuilder::default()
        .config(
            JDBC_URI_KEY,
            "jdbc:postgresql://u:DumpPw1@db.example.com/sales",
        )
        .config(
            S3A_ENDPOINT_CONFIG_KEY,
            "http://minio:EndpointPw2@127.0.0.1:5599",
        )
        .build()
        .expect("session builds");
    let dumped = session.conf_dump();
    let rendered = format!("{dumped:?}");
    assert!(!rendered.contains("DumpPw1"), "{rendered}");
    assert!(!rendered.contains("EndpointPw2"), "{rendered}");
    let uri = dumped
        .iter()
        .find(|(key, _, _)| key == JDBC_URI_KEY)
        .expect("uri row");
    assert_eq!(uri.1, "jdbc:postgresql://u:***@db.example.com/sales");
    let endpoint = resolve_endpoint_from_dump(&session.conf_dump)
        .expect("endpoint resolves")
        .expect("endpoint configured");
    assert_eq!(
        endpoint.endpoint.as_deref(),
        Some("http://minio:EndpointPw2@127.0.0.1:5599")
    );
}
