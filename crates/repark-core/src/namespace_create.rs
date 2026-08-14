//! Namespace-create location guard (G-6 Q1).
//!
//! Shared by [`crate::ReparkSession::create_namespace`] and both SQL doors' `IF NOT EXISTS`
//! paths. Matching location and no-request-location are idempotent; a request location that
//! contradicts the stored one fails loud, naming both paths. This is a data-loss guard
//! (stale namespace, wrong warehouse path), not a Spark-parity surface.

use std::collections::HashMap;
use std::hash::BuildHasher;

use repark_iceberg::catalog::resolve_namespace_location;

/// Refuse a re-create whose explicit location contradicts the stored one.
///
/// - No explicit request location (`location` / `location_uri`) → `Ok(())` (idempotent adopt).
/// - Existing namespace has no stored location → `Ok(())` (nothing stored to contradict).
/// - Resolved locations match (trailing slashes stripped) → `Ok(())`.
/// - Both resolve and differ → `Err` with a message naming both paths and the namespace.
///
/// # Errors
/// Returns a message naming the namespace and both warehouse paths when the
/// request carries an explicit location that differs from the stored one.
pub fn refuse_contradictory_namespace_location<ExistingHasher, RequestedHasher>(
    namespace: &str,
    existing_properties: &HashMap<String, String, ExistingHasher>,
    requested_properties: &HashMap<String, String, RequestedHasher>,
) -> Result<(), String>
where
    ExistingHasher: BuildHasher,
    RequestedHasher: BuildHasher,
{
    let Some(requested) = resolve_namespace_location(requested_properties) else {
        return Ok(());
    };
    let Some(existing) = resolve_namespace_location(existing_properties) else {
        return Ok(());
    };
    if locations_equivalent(existing, requested) {
        return Ok(());
    }
    Err(namespace_location_conflict_message(
        namespace, existing, requested,
    ))
}

/// Fail-loud message naming the namespace and both warehouse paths.
fn namespace_location_conflict_message(
    namespace: &str,
    existing_location: &str,
    requested_location: &str,
) -> String {
    format!(
        "cannot create namespace `{namespace}`: requested location `{requested_location}` \
         contradicts existing location `{existing_location}`"
    )
}

/// Compare warehouse paths after stripping trailing slashes (S3 keys stay case-sensitive).
fn locations_equivalent(left: &str, right: &str) -> bool {
    normalize_location(left) == normalize_location(right)
}

fn normalize_location(location: &str) -> &str {
    location.trim_end_matches('/')
}

#[cfg(test)]
mod tests {
    use super::*;

    fn props(pairs: &[(&str, &str)]) -> HashMap<String, String> {
        pairs
            .iter()
            .map(|(key, value)| ((*key).to_string(), (*value).to_string()))
            .collect()
    }

    #[test]
    fn no_request_location_is_idempotent() {
        let existing = props(&[("location", "/warehouse/a")]);
        refuse_contradictory_namespace_location("ns", &existing, &HashMap::new())
            .expect("no request location must adopt");
    }

    #[test]
    fn matching_location_is_idempotent() {
        let existing = props(&[("location", "/warehouse/a")]);
        let requested = props(&[("location", "/warehouse/a")]);
        refuse_contradictory_namespace_location("ns", &existing, &requested)
            .expect("matching location must adopt");
    }

    #[test]
    fn trailing_slash_is_not_a_conflict() {
        let existing = props(&[("location", "/warehouse/a/")]);
        let requested = props(&[("location", "/warehouse/a")]);
        refuse_contradictory_namespace_location("ns", &existing, &requested)
            .expect("trailing-slash-only difference must not fail-loud");
    }

    #[test]
    fn location_uri_only_existing_matches_location_request() {
        let existing = props(&[("location_uri", "/warehouse/glue")]);
        let requested = props(&[("location", "/warehouse/glue")]);
        refuse_contradictory_namespace_location("ns", &existing, &requested)
            .expect("resolve_namespace_location must see both keys as the same path");
    }

    #[test]
    fn existing_without_location_adopts_a_requested_path() {
        let requested = props(&[("location", "/warehouse/new")]);
        refuse_contradictory_namespace_location("ns", &HashMap::new(), &requested)
            .expect("a location-less existing namespace has nothing to contradict");
    }

    #[test]
    fn conflicting_locations_name_both_paths() {
        let existing = props(&[("location", "/warehouse/a")]);
        let requested = props(&[("location", "/warehouse/b")]);
        let error = refuse_contradictory_namespace_location("silver", &existing, &requested)
            .expect_err("conflicting locations must fail loud");
        assert!(
            error.contains("/warehouse/a"),
            "must name the existing path: {error}"
        );
        assert!(
            error.contains("/warehouse/b"),
            "must name the requested path: {error}"
        );
        assert!(error.contains("silver"), "must name the namespace: {error}");
    }
}
