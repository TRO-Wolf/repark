use std::path::PathBuf;

use super::{parse_fixture, read_fixture};

#[test]
fn canonical_bytes_match_the_committed_golden() {
    let plan = parse_fixture("positive/crm_contacts.toml").expect("base");
    let golden = std::fs::read(
        PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("src/silver/fixtures/positive/crm_contacts.canonical.txt"),
    )
    .expect("canonical golden");
    assert_eq!(plan.canonical(), golden);
}

#[test]
fn permutation_whitespace_and_inline_spellings_share_canonical_bytes() {
    let base = parse_fixture("positive/crm_contacts.toml").expect("base");
    let permuted = parse_fixture("positive/crm_contacts_permuted_keys.toml").expect("permuted");
    let whitespace =
        parse_fixture("positive/crm_contacts_whitespace_comments.toml").expect("whitespace");
    let inline = parse_fixture("positive/crm_contacts_inline.toml").expect("inline");
    let canonical = base.canonical();
    assert_eq!(canonical, permuted.canonical());
    assert_eq!(canonical, whitespace.canonical());
    assert_eq!(canonical, inline.canonical());
    let with_comment = format!(
        "{}\n# ignored comment\n",
        read_fixture("positive/crm_contacts.toml")
    );
    let commented = crate::silver::SilverPlan::parse(&with_comment).expect("commented");
    assert_eq!(canonical, commented.canonical());
}

#[test]
fn scientific_threshold_matches_decimal_canonical_bytes() {
    let decimal = parse_fixture("positive/crm_contacts.toml").expect("decimal");
    let scientific =
        parse_fixture("positive/crm_contacts_threshold_scientific.toml").expect("scientific");
    assert_eq!(decimal.canonical(), scientific.canonical());
}

#[test]
fn a_semantic_field_change_changes_canonical_bytes() {
    let base = parse_fixture("positive/crm_contacts.toml").expect("base");
    let swapped = parse_fixture("positive/crm_contacts.toml")
        .expect("base text")
        .canonical();
    let mutated = read_fixture("positive/crm_contacts.toml").replace(
        "dataset_id = \"crm.contacts.silver\"",
        "dataset_id = \"crm.contacts.silver.v2\"",
    );
    let changed = crate::silver::SilverPlan::parse(&mutated).expect("mutated");
    assert_ne!(base.canonical(), changed.canonical());
    assert_eq!(base.canonical(), swapped);
    let trim_mutated =
        read_fixture("positive/crm_contacts.toml").replace("\"U+0020\"", "\"U+0009\"");
    let trim_changed = crate::silver::SilverPlan::parse(&trim_mutated).expect("trim mutated");
    assert_ne!(base.canonical(), trim_changed.canonical());
}

#[test]
fn explain_with_identity_matches_explain() {
    let plan = parse_fixture("positive/crm_contacts.toml").expect("base");
    let identity = plan.identity();
    assert_eq!(plan.explain(), plan.explain_with_identity(&identity));
}

#[test]
fn equivalent_spellings_explain_byte_identically() {
    let base = parse_fixture("positive/crm_contacts.toml").expect("base");
    let permuted = parse_fixture("positive/crm_contacts_permuted_keys.toml").expect("permuted");
    let whitespace =
        parse_fixture("positive/crm_contacts_whitespace_comments.toml").expect("whitespace");
    let inline = parse_fixture("positive/crm_contacts_inline.toml").expect("inline");
    let scientific =
        parse_fixture("positive/crm_contacts_threshold_scientific.toml").expect("scientific");
    let explained = base.explain();
    assert_eq!(explained, permuted.explain());
    assert_eq!(explained, whitespace.explain());
    assert_eq!(explained, inline.explain());
    assert_eq!(explained, scientific.explain());
    let golden = read_fixture("positive/crm_contacts.explain.txt");
    assert_eq!(explained, golden);
}
