#[test]
fn the_append_module_still_exports_commit_append() {
    let restored = std::any::type_name_of_val(&repark_iceberg::write::append::commit_append);
    let flat = std::any::type_name_of_val(&repark_iceberg::write::commit_append);
    assert_eq!(restored, flat);
    assert!(restored.ends_with("::commit_append"), "{restored}");
}
