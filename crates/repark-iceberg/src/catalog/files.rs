use bytes::Bytes;
use iceberg::io::FileIO;

#[allow(clippy::missing_errors_doc)]
pub async fn write_text_file(
    file_io: &FileIO,
    path: &str,
    contents: &str,
) -> Result<(), iceberg::Error> {
    file_io
        .write_new(path, Bytes::from(contents.to_string()))
        .await
}
