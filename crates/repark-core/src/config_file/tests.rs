use super::{ConfigFile, load, parse};

#[test]
fn load_without_a_file_is_the_empty_config() {
    assert_eq!(load().expect("empty config"), ConfigFile::default());
}

#[test]
fn an_empty_document_parses_to_the_empty_config() {
    assert_eq!(parse("").expect("empty document"), ConfigFile::default());
}

#[test]
fn an_unknown_top_level_key_refuses_as_a_config_error() {
    let error = parse("nonesuch = 1").expect_err("unknown key must refuse");
    assert!(error.to_string().contains("nonesuch"), "{error}");
}
