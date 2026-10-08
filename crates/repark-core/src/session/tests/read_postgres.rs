use std::collections::BTreeMap;

use crate::{PostgresRead, PostgresTarget, ReparkSession};

fn read(properties: &[(&str, &str)]) -> PostgresRead {
    PostgresRead {
        url: "postgresql://203.0.113.1:5432/db".to_string(),
        target: PostgresTarget::Relation("public.t".to_string()),
        properties: properties
            .iter()
            .map(|(key, value)| ((*key).to_string(), (*value).to_string()))
            .collect::<BTreeMap<_, _>>(),
        partitioning: Vec::new(),
    }
}

#[tokio::test]
async fn a_dbtable_property_is_the_target_never_a_setting() {
    let session = ReparkSession::builder().build().expect("session");
    for key in ["dbtable", "DBTABLE"] {
        let error = session
            .read_postgres(read(&[(key, "public.t")]))
            .await
            .expect_err("a read without `user` refuses before any connection");
        let message = error.to_string();
        assert!(
            message.contains("database source `jdbc`"),
            "{key}: {message}"
        );
        assert!(message.contains("`user` is required"), "{key}: {message}");
        assert!(
            !message.to_lowercase().contains("dbtable"),
            "{key}: {message}"
        );
    }
}
