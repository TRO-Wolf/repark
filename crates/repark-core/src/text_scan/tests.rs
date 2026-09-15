use super::*;
use crate::text_schema::text_schema_with_partitions;
use arrow::array::Array;

fn test_session() -> crate::ReparkSession {
    crate::ReparkSession::builder().build().unwrap()
}

async fn text_rows(frame: &DataFrame) -> Vec<Option<String>> {
    let batches = frame.clone().collect().await.unwrap();
    let mut rows: Vec<Option<String>> = Vec::new();
    for batch in &batches {
        let column = batch.column(0);
        let values = column
            .as_any()
            .downcast_ref::<arrow::array::StringArray>()
            .unwrap();
        for row in 0..batch.num_rows() {
            rows.push(if values.is_null(row) {
                None
            } else {
                Some(values.value(row).to_string())
            });
        }
    }
    rows
}

async fn text_values(frame: &DataFrame) -> Vec<String> {
    let mut rows = text_rows(frame).await;
    rows.sort();
    rows.into_iter().flatten().collect()
}

#[tokio::test]
async fn text_split_drops_one_trailing_terminator() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("t.txt");
    std::fs::write(&path, "x\n\ny\r\nz").unwrap();
    let session = test_session();
    let frame = session
        .read_text(path.to_str().unwrap(), false, None, None, None)
        .await
        .unwrap();
    let rows = text_rows(&frame).await;
    let plain: Vec<&str> = rows
        .iter()
        .map(|row| row.as_deref().unwrap_or("<null>"))
        .collect();
    assert_eq!(plain, vec!["x", "", "y", "z"]);
}

#[tokio::test]
async fn text_custom_separator_splits_only_on_it() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("t.txt");
    std::fs::write(&path, "a;b;c").unwrap();
    let session = test_session();
    let frame = session
        .read_text(path.to_str().unwrap(), false, Some(";"), None, None)
        .await
        .unwrap();
    let rows = text_rows(&frame).await;
    let plain: Vec<&str> = rows.iter().map(|row| row.as_deref().unwrap()).collect();
    assert_eq!(plain, vec!["a", "b", "c"]);
}

#[tokio::test]
async fn text_wholetext_reads_one_row_per_file() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("t.txt");
    std::fs::write(&path, "a\nb\n").unwrap();
    let session = test_session();
    let frame = session
        .read_text(path.to_str().unwrap(), true, None, None, None)
        .await
        .unwrap();
    let rows = text_rows(&frame).await;
    assert_eq!(rows, vec![Some("a\nb\n".to_string())]);
}

#[tokio::test]
async fn text_lone_carriage_return_splits() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("t.txt");
    std::fs::write(&path, "a\rb\rc").unwrap();
    let session = test_session();
    let frame = session
        .read_text(path.to_str().unwrap(), false, None, None, None)
        .await
        .unwrap();
    assert_eq!(text_values(&frame).await, vec!["a", "b", "c"]);
}

#[tokio::test]
async fn text_crlf_split_across_read_chunks() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("t.txt");
    let mut bytes = vec![b'A'; TEXT_READ_CHUNK - 1];
    bytes.extend_from_slice(b"\r\nZ\n");
    std::fs::write(&path, &bytes).unwrap();
    let session = test_session();
    let frame = session
        .read_text(path.to_str().unwrap(), false, None, None, None)
        .await
        .unwrap();
    let rows = text_rows(&frame).await;
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0].as_deref().unwrap().len(), TEXT_READ_CHUNK - 1);
    assert_eq!(rows[1].as_deref(), Some("Z"));
}

#[tokio::test]
async fn text_two_trailing_terminators_keep_empty_row() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("t.txt");
    std::fs::write(&path, "x\n\n").unwrap();
    let session = test_session();
    let frame = session
        .read_text(path.to_str().unwrap(), false, None, None, None)
        .await
        .unwrap();
    assert_eq!(text_values(&frame).await, vec!["", "x"]);
}

#[tokio::test]
async fn text_empty_file_reads_no_rows() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("t.txt");
    std::fs::write(&path, "").unwrap();
    let session = test_session();
    let frame = session
        .read_text(path.to_str().unwrap(), false, None, None, None)
        .await
        .unwrap();
    assert!(text_rows(&frame).await.is_empty());
}

#[tokio::test]
async fn text_invalid_utf8_decodes_lossy() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("bad.txt");
    std::fs::write(&path, b"ok\n\xff\xfe\nmore\ncafe\xe2\x82").unwrap();
    let session = test_session();
    let frame = session
        .read_text(path.to_str().unwrap(), false, None, None, None)
        .await
        .unwrap();
    let rows = text_rows(&frame).await;
    let plain: Vec<&str> = rows.iter().map(|row| row.as_deref().unwrap()).collect();
    assert_eq!(plain, vec!["ok", "��", "more", "cafe�"]);
}

#[tokio::test]
async fn text_missing_path_reports_not_found() {
    let session = test_session();
    let missing = std::env::temp_dir().join("repark-no-such-text-path-1290");
    let Err(error) = session
        .read_text(missing.to_str().unwrap(), false, None, None, None)
        .await
    else {
        panic!("a missing path must refuse the text read");
    };
    assert_eq!(
        error.to_string(),
        format!(
            "[PATH_NOT_FOUND] Path does not exist: file:{}. SQLSTATE: 42K03",
            missing.to_str().unwrap()
        )
    );
}

#[tokio::test]
async fn text_empty_line_sep_refuses() {
    let session = test_session();
    let Err(error) = session
        .read_text("any.txt", false, Some(""), None, None)
        .await
    else {
        panic!("an empty lineSep must refuse the text read");
    };
    assert!(error.to_string().contains("lineSep"));
}

#[tokio::test]
async fn text_glob_star_matches_txt_only() {
    let dir = tempfile::tempdir().unwrap();
    for (name, body) in [
        ("list_1.txt", "one\n"),
        ("list_2.txt", "two\n"),
        ("other.log", "log\n"),
        ("foo[bar].txt", "brackets\n"),
        ("foob.txt", "b-class\n"),
    ] {
        std::fs::write(dir.path().join(name), body).unwrap();
    }
    let session = test_session();
    let pattern = dir.path().join("*.txt");
    let frame = session
        .read_text(pattern.to_str().unwrap(), false, None, None, None)
        .await
        .unwrap();
    assert_eq!(
        text_values(&frame).await,
        vec!["b-class", "brackets", "one", "two"]
    );
}

#[tokio::test]
async fn text_glob_question_matches_one_char() {
    let dir = tempfile::tempdir().unwrap();
    std::fs::write(dir.path().join("list_1.txt"), "one\n").unwrap();
    std::fs::write(dir.path().join("list_2.txt"), "two\n").unwrap();
    std::fs::write(dir.path().join("other.log"), "log\n").unwrap();
    let session = test_session();
    let pattern = dir.path().join("list_?.txt");
    let frame = session
        .read_text(pattern.to_str().unwrap(), false, None, None, None)
        .await
        .unwrap();
    assert_eq!(text_values(&frame).await, vec!["one", "two"]);
}

#[tokio::test]
async fn text_glob_brackets_are_a_class() {
    let dir = tempfile::tempdir().unwrap();
    std::fs::write(dir.path().join("foo[bar].txt"), "brackets\n").unwrap();
    std::fs::write(dir.path().join("foob.txt"), "b-class\n").unwrap();
    let session = test_session();
    let pattern = dir.path().join("foo[bar].txt");
    let frame = session
        .read_text(pattern.to_str().unwrap(), false, None, None, None)
        .await
        .unwrap();
    assert_eq!(text_values(&frame).await, vec!["b-class"]);
}

#[tokio::test]
async fn text_dir_descends_partition_dirs_only() {
    let dir = tempfile::tempdir().unwrap();
    std::fs::write(dir.path().join("top.txt"), "top\n").unwrap();
    let nested = dir.path().join("sub");
    std::fs::create_dir(&nested).unwrap();
    std::fs::write(nested.join("leaf.txt"), "leaf\n").unwrap();
    let keyed = dir.path().join("k=x");
    std::fs::create_dir(&keyed).unwrap();
    std::fs::write(keyed.join("part-00000.txt"), "hello\n").unwrap();
    let session = test_session();
    let frame = session
        .read_text(dir.path().to_str().unwrap(), false, None, None, None)
        .await
        .unwrap();
    assert_eq!(text_values(&frame).await, vec!["hello"]);
}

#[tokio::test]
async fn text_limit_stops_after_enough_rows() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("t.txt");
    std::fs::write(&path, "a\nb\nc\nd\ne\n").unwrap();
    let session = test_session();
    let frame = session
        .read_text(path.to_str().unwrap(), false, None, None, None)
        .await
        .unwrap();
    let limited = frame.limit(0, Some(2)).unwrap();
    let batches = limited.collect().await.unwrap();
    let count: usize = batches.iter().map(RecordBatch::num_rows).sum();
    assert_eq!(count, 2);
}

#[test]
fn text_limit_stops_appending_at_limit() {
    let mut stream = TextLineStream {
        files: Vec::new(),
        file_index: 0,
        current: None,
        reader: None,
        chunk: Vec::new(),
        carry: Vec::new(),
        value_builder: Some(StringBuilder::new()),
        part_builders: Vec::new(),
        part_slots: Vec::new(),
        part_current: Vec::new(),
        part_types: Vec::new(),
        part_plan: vec![None],
        partition_values: Arc::new(HashMap::new()),
        blank_rows: 0,
        wholetext: false,
        separator: None,
        schema: text_schema_with_partitions(&[]),
        limit: Some(10),
        emitted: 0,
        done: false,
        bytes_read: 0,
    };
    let line = vec![b'x'; 10 * 1024];
    for _ in 0..2000 {
        stream.carry.extend_from_slice(&line);
        stream.carry.push(b'\n');
    }
    stream.scan_carry(false);
    assert_eq!(
        stream.value_builder.as_ref().map(ArrayBuilder::len),
        Some(10)
    );
    let batch = stream.take_batch().unwrap();
    assert_eq!(batch.num_rows(), 10);
}

#[test]
fn text_grouping_caps_partitions() {
    let files: Vec<PathBuf> = (0..20)
        .map(|index| PathBuf::from(format!("{index}.txt")))
        .collect();
    let groups = group_text_files(&files);
    assert!(groups.len() <= TEXT_SCAN_PARTITIONS);
    let flat: Vec<PathBuf> = groups.into_iter().flatten().collect();
    assert_eq!(flat, files);
    assert_eq!(group_text_files(&[]), vec![Vec::<PathBuf>::new()]);
}

#[test]
fn text_expand_paths_missing_reports_not_found() {
    let error = expand_text_paths("/no/such/repark-text-path", None, "UTC").unwrap_err();
    assert!(error.to_string().starts_with("[PATH_NOT_FOUND]"));
    assert!(error.to_string().contains("SQLSTATE: 42K03"));
}
