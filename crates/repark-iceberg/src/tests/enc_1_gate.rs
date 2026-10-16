use std::collections::BTreeSet;
use std::path::{Path, PathBuf};

const GUARD: &str = "repark-iceberg/src/catalog/encryption_guard.rs";
const BUILDERS: &str = "repark-iceberg/src/catalog/builders.rs";
const GUARDED_CATALOG: &str = "EncryptionGuardCatalog::install(Arc::new(catalog))";

const PRIMITIVES: &[(&str, &[&str])] = &[
    (
        "MemoryCatalogBuilder",
        &[BUILDERS, "repark-iceberg/src/catalog/cache_wiring.rs"],
    ),
    (
        "GlueCatalogBuilder",
        &[BUILDERS, "repark-iceberg/src/catalog/cache_wiring.rs"],
    ),
    (
        "S3TablesCatalogBuilder",
        &[BUILDERS, "repark-iceberg/src/catalog/cache_wiring.rs"],
    ),
    (
        "impl Catalog for",
        &[
            GUARD,
            "repark-iceberg/src/catalog/provider.rs",
            "repark-iceberg/src/write/sink_offsets/append_fence.rs",
        ],
    ),
    ("Table>::builder()", &[]),
    ("table::TableBuilder", &[]),
    ("table::Table as ", &[]),
    (
        "Table::builder",
        &[GUARD, "repark-iceberg/src/write/output_spec.rs"],
    ),
    ("StaticTable::", &["repark-core/src/iceberg_path.rs"]),
    ("StagedTableTransaction::begin_create", &[GUARD]),
    (
        "FileIOBuilder::new",
        &[GUARD, "repark-iceberg/src/catalog/location.rs"],
    ),
    ("FileIO::new_with_", &[]),
    ("FileIO::from_path", &[]),
    (
        "file_io_for_location(",
        &[
            "repark-core/src/iceberg_path.rs",
            "repark-sql/src/create_table.rs",
            "repark-spark/src/ctas.rs",
        ],
    ),
];

fn crates_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("the crate sits under crates/")
        .to_path_buf()
}

fn rust_sources(root: &Path) -> Vec<PathBuf> {
    let mut pending = vec![root.to_path_buf()];
    let mut found = Vec::new();
    while let Some(dir) = pending.pop() {
        let entries = std::fs::read_dir(&dir).expect("a readable source directory");
        for entry in entries {
            let path = entry.expect("a directory entry").path();
            let name = path
                .file_name()
                .and_then(|name| name.to_str())
                .unwrap_or_default()
                .to_string();
            if path.is_dir() {
                if !matches!(name.as_str(), "tests" | "benches" | "examples" | "target") {
                    pending.push(path);
                }
            } else if path.extension().is_some_and(|extension| extension == "rs") {
                found.push(path);
            }
        }
    }
    found.sort();
    found
}

fn module_item(line: &str) -> Option<(&str, bool)> {
    let rest = line.trim();
    let rest = rest
        .strip_prefix("pub(crate) ")
        .or_else(|| rest.strip_prefix("pub(super) "))
        .or_else(|| rest.strip_prefix("pub "))
        .unwrap_or(rest);
    let rest = rest.strip_prefix("mod ")?;
    if let Some(name) = rest.strip_suffix(';') {
        return Some((name.trim(), false));
    }
    rest.strip_suffix('{').map(|name| (name.trim(), true))
}

fn brace_delta(line: &str) -> i64 {
    let opened = i64::try_from(line.matches('{').count()).unwrap_or(i64::MAX);
    let closed = i64::try_from(line.matches('}').count()).unwrap_or(i64::MAX);
    opened - closed
}

struct Product {
    text: String,
    test_modules: Vec<PathBuf>,
    child_modules: Vec<PathBuf>,
}

fn module_files(
    directory: &Path,
    stem: &str,
    name: &str,
    declared_path: Option<String>,
) -> Vec<PathBuf> {
    let mut files = Vec::new();
    if let Some(declared) = declared_path {
        files.push(directory.join(declared));
    }
    for base in [directory.to_path_buf(), directory.join(stem)] {
        files.push(base.join(format!("{name}.rs")));
        files.push(base.join(name));
    }
    files
}

fn path_attribute(line: &str) -> Option<String> {
    line.trim()
        .strip_prefix("#[path = \"")
        .and_then(|rest| rest.strip_suffix("\"]"))
        .map(str::to_string)
}

fn product_of(path: &Path, source: &str) -> Product {
    let directory = path.parent().expect("a source file has a directory");
    let stem = path
        .file_stem()
        .and_then(|stem| stem.to_str())
        .unwrap_or_default();
    let mut text = String::new();
    let mut test_modules = Vec::new();
    let mut child_modules = Vec::new();
    let mut pending_path = None;
    let mut lines = source.lines();
    while let Some(line) = lines.next() {
        if line.trim_start().starts_with("//") {
            continue;
        }
        if line.trim() != "#[cfg(test)]" {
            if let Some((name, false)) = module_item(line) {
                child_modules.extend(module_files(directory, stem, name, pending_path.take()));
            }
            pending_path = path_attribute(line);
            text.push_str(line);
            text.push('\n');
            continue;
        }
        let mut declared_path = None;
        let mut item = lines.next().unwrap_or_default();
        while item.trim_start().starts_with("#[") {
            declared_path = path_attribute(item).or(declared_path);
            item = lines.next().unwrap_or_default();
        }
        match module_item(item) {
            Some((name, false)) => {
                test_modules.extend(module_files(directory, stem, name, declared_path));
            }
            Some((_, true)) => {
                let mut depth = brace_delta(item);
                while depth > 0 {
                    let Some(inner) = lines.next() else {
                        break;
                    };
                    depth += brace_delta(inner);
                }
            }
            None => {}
        }
    }
    Product {
        text,
        test_modules,
        child_modules,
    }
}

fn relative(root: &Path, path: &Path) -> String {
    path.strip_prefix(root)
        .expect("a path under crates/")
        .to_string_lossy()
        .replace('\\', "/")
}

fn product_sources() -> Vec<(String, String)> {
    let root = crates_root();
    let mut products = Vec::new();
    let mut test_modules = Vec::new();
    for path in rust_sources(&root) {
        let source = std::fs::read_to_string(&path).expect("a readable source file");
        let product = product_of(&path, &source);
        test_modules.extend(product.test_modules);
        products.push((path, product.text, product.child_modules));
    }
    let is_test =
        |path: &Path, modules: &[PathBuf]| modules.iter().any(|module| path.starts_with(module));
    loop {
        let before = test_modules.len();
        for (path, _, children) in &products {
            if is_test(path, &test_modules) {
                for child in children {
                    if !test_modules.contains(child) {
                        test_modules.push(child.clone());
                    }
                }
            }
        }
        if test_modules.len() == before {
            break;
        }
    }
    products
        .into_iter()
        .filter(|(path, _, _)| !is_test(path, &test_modules))
        .map(|(path, text, _)| (relative(&root, &path), text))
        .collect()
}

#[test]
fn every_catalog_table_handle_and_file_io_is_built_inside_the_guarded_wrappers() {
    let mut used = BTreeSet::new();
    let mut escapes = Vec::new();
    for (file, text) in product_sources() {
        for (primitive, allowed) in PRIMITIVES {
            if !text.contains(primitive) {
                continue;
            }
            if allowed.contains(&file.as_str()) {
                used.insert((*primitive, file.clone()));
            } else {
                escapes.push(format!("{file}: {primitive}"));
            }
        }
    }
    assert!(
        escapes.is_empty(),
        "ENC-1: built outside the guarded wrappers (see catalog/map.md):\n{}",
        escapes.join("\n")
    );
    let stale: Vec<String> = PRIMITIVES
        .iter()
        .flat_map(|(primitive, allowed)| allowed.iter().map(move |file| (*primitive, *file)))
        .filter(|(primitive, file)| !used.contains(&(*primitive, (*file).to_string())))
        .map(|(primitive, file)| format!("{file}: {primitive}"))
        .collect();
    assert!(
        stale.is_empty(),
        "ENC-1: allowance no longer used, remove the row:\n{}",
        stale.join("\n")
    );
}

const SWEEP_SITES: &[(&str, &str)] = &[
    ("repark-iceberg/src/write/append_commit.rs", "commit_append"),
    (
        "repark-iceberg/src/write/commit_target.rs",
        "commit_append_to",
    ),
    (
        "repark-iceberg/src/write/write_options.rs",
        "append_staged_with_options",
    ),
    (
        "repark-iceberg/src/write/write_options.rs",
        "commit_append_with_summary",
    ),
    (
        "repark-iceberg/src/write/write_options.rs",
        "commit_replace_write_with_summary",
    ),
    (
        "repark-iceberg/src/write/write_options.rs",
        "commit_overwrite_replace_all_with_summary",
    ),
    (
        "repark-iceberg/src/write/write_options.rs",
        "commit_overwrite_by_row_filter_with_summary",
    ),
    (
        "repark-iceberg/src/write/write_options.rs",
        "commit_replace_partitions_with_summary",
    ),
    (
        "repark-iceberg/src/write/overwrite_commit.rs",
        "commit_overwrite_replace_all_to",
    ),
    (
        "repark-iceberg/src/write/overwrite_commit.rs",
        "commit_replace_write",
    ),
    (
        "repark-iceberg/src/write/partition_overwrite.rs",
        "commit_overwrite_by_row_filter_to",
    ),
    (
        "repark-iceberg/src/write/partition_overwrite.rs",
        "commit_replace_partitions_to",
    ),
    (
        "repark-iceberg/src/write/overwrite_filter.rs",
        "commit_overwrite_by_filter_with_summary",
    ),
    (
        "repark-iceberg/src/write/merge/snapshot_commit.rs",
        "commit_overwrite_on_ref",
    ),
    (
        "repark-iceberg/src/write/merge/snapshot_commit.rs",
        "commit_row_delta_kind_on_ref",
    ),
];

const CENSUS_ROOTS: &[&str] = &[
    "repark-iceberg/src/",
    "repark-spark/src/",
    "repark-sql/src/",
];
const STAGING_CALLS: &[&str] = &[
    "add_data_files",
    "add_files",
    "add_file",
    "add_deletes",
    "add_delete_files",
];
const COMMIT_CALLS: &[&str] = &["commit", "publish_create_table", "publish_replace_table"];
const SWEEP_CALLS: &[&str] = &[
    "commit_with_keyed_sweep",
    "sweep_after_rejected_commit",
    "sweep_staged_files_on_keyed_refusal",
];
const EMPTY_STAGING: &[&str] = &["Vec::new()", "vec![]", "Vec::default()"];
const CENSUS_EXCEPTIONS: &[(&str, &str, &str)] = &[
    (
        "repark-spark/src/ctas.rs",
        "finish_ctas_staged_commit",
        "ENC-CTAS-REPLACE-1",
    ),
    (
        "repark-sql/src/create_table.rs",
        "execute_staged_create",
        "ENC-CTAS-REPLACE-1",
    ),
    (
        "repark-sql/src/create_table.rs",
        "publish_staged_with_summary",
        "ENC-CTAS-REPLACE-1",
    ),
];
const MAX_BINDING_HOPS: usize = 8;
const TRANSACTION_API_NAMES: &[&str] = &["apply"];

fn function_name(line: &str) -> Option<String> {
    let mut rest = line.trim();
    loop {
        let mut stripped = false;
        for prefix in [
            "pub(crate) ",
            "pub(super) ",
            "pub ",
            "async ",
            "unsafe ",
            "const ",
        ] {
            if let Some(after) = rest.strip_prefix(prefix) {
                rest = after;
                stripped = true;
                break;
            }
        }
        if !stripped {
            break;
        }
    }
    let rest = rest.strip_prefix("fn ")?;
    let end = rest
        .find(|character: char| character == '(' || character == '<' || character.is_whitespace())
        .unwrap_or(rest.len());
    if end == 0 {
        return None;
    }
    Some(rest[..end].to_string())
}

fn function_bodies(text: &str) -> Vec<(String, String)> {
    let mut functions = Vec::new();
    let mut current: Option<(String, i64, String, bool)> = None;
    let mut depth: i64 = 0;
    for raw in text.lines() {
        let line = raw.split("//").next().unwrap_or_default();
        if let Some(name) = function_name(line)
            && depth <= 1
        {
            if let Some((open, _, body, _)) = current.take() {
                functions.push((open, body));
            }
            current = Some((name, depth, String::new(), false));
        }
        if let Some((_, start, body, entered)) = current.as_mut() {
            body.push_str(line);
            body.push('\n');
            if depth > *start {
                *entered = true;
            }
        }
        depth += brace_delta(line);
        if let Some((_, start, _, entered)) = current.as_ref()
            && *entered
            && depth <= *start
            && let Some((open, _, body, _)) = current.take()
        {
            functions.push((open, body));
        }
    }
    if let Some((open, _, body, _)) = current.take() {
        functions.push((open, body));
    }
    functions
}

#[test]
fn every_catalog_builder_returns_through_the_encryption_guard() {
    let builders = product_sources()
        .into_iter()
        .find(|(file, _)| file == BUILDERS)
        .map(|(_, text)| text)
        .expect("the builders file is scanned");
    let loads = builders.matches(".load(").count();
    assert_eq!(loads, 3, "one load per catalog kind");
    assert_eq!(builders.matches(GUARDED_CATALOG).count(), loads);
    assert_eq!(builders.matches("Arc::new(catalog)").count(), loads);
}

#[test]
fn the_scan_skips_test_code_and_keeps_product_code() {
    let source = "use a::MemoryCatalogBuilder;\n// FileIO::from_path in a comment\n\
                  #[cfg(test)]\n#[path = \"x_pins.rs\"]\nmod pins;\n\
                  #[cfg(test)]\nmod tests {\n    fn t() { let _ = Table::builder(); }\n}\n\
                  fn after() { StaticTable::load(); }\n";
    let product = product_of(Path::new("crates/k/src/write/x.rs"), source);
    assert!(product.text.contains("MemoryCatalogBuilder"));
    assert!(product.text.contains("StaticTable::"));
    assert!(!product.text.contains("Table::builder()"));
    assert!(!product.text.contains("FileIO::from_path"));
    assert!(
        product
            .test_modules
            .contains(&PathBuf::from("crates/k/src/write/x_pins.rs"))
    );
    assert!(
        product
            .test_modules
            .contains(&PathBuf::from("crates/k/src/write/pins.rs"))
    );
    let nested = product_of(
        Path::new("crates/k/src/write/x_pins.rs"),
        "#[path = \"deep_pins.rs\"]\nmod deep;\nmod plain;\n",
    );
    assert!(
        nested
            .child_modules
            .contains(&PathBuf::from("crates/k/src/write/deep_pins.rs"))
    );
    assert!(
        nested
            .child_modules
            .contains(&PathBuf::from("crates/k/src/write/plain.rs"))
    );
}

struct CensusFunction {
    site: String,
    is_method: bool,
    takes_transaction: bool,
    returns_staged: bool,
    body: String,
}

fn is_identifier_char(character: char) -> bool {
    character.is_alphanumeric() || character == '_'
}

fn is_identifier(text: &str) -> bool {
    !text.is_empty()
        && text.chars().all(is_identifier_char)
        && !text.starts_with(|character: char| character.is_ascii_digit())
}

fn balanced_arguments(rest: &str) -> Option<String> {
    let mut depth = 1_i64;
    for (index, character) in rest.char_indices() {
        match character {
            '(' | '[' | '{' => depth += 1,
            ')' | ']' | '}' => {
                depth -= 1;
                if depth == 0 {
                    return Some(rest[..index].to_string());
                }
            }
            _ => {}
        }
    }
    None
}

fn call_arguments(body: &str, name: &str, method: bool) -> Vec<String> {
    let needle = format!("{name}(");
    let mut found = Vec::new();
    let mut from = 0;
    while let Some(offset) = body[from..].find(&needle) {
        let start = from + offset;
        from = start + needle.len();
        let before = body[..start].trim_end_matches([' ', '\t']);
        let previous = before.chars().next_back();
        if previous.is_some_and(is_identifier_char) {
            continue;
        }
        if before.ends_with("fn") {
            continue;
        }
        let dotted = before.ends_with('.') || before.ends_with(".\n");
        let dotted = dotted || body[..start].trim_end().ends_with('.');
        if dotted != method {
            continue;
        }
        if let Some(arguments) = balanced_arguments(&body[from..]) {
            found.push(arguments);
        }
    }
    found
}

fn split_arguments(arguments: &str) -> Vec<String> {
    let mut parts = Vec::new();
    let mut depth = 0_i64;
    let mut current = String::new();
    for character in arguments.chars() {
        match character {
            '(' | '[' | '{' | '<' => depth += 1,
            ')' | ']' | '}' | '>' => depth -= 1,
            ',' if depth == 0 => {
                parts.push(current.trim().to_string());
                current.clear();
                continue;
            }
            _ => {}
        }
        current.push(character);
    }
    if !current.trim().is_empty() {
        parts.push(current.trim().to_string());
    }
    parts
}

fn signature(body: &str) -> &str {
    body.find('{').map_or(body, |end| &body[..end])
}

fn census_functions() -> Vec<CensusFunction> {
    let mut functions = Vec::new();
    for (file, text) in product_sources() {
        if !CENSUS_ROOTS.iter().any(|root| file.starts_with(root)) {
            continue;
        }
        for (name, body) in function_bodies(&text) {
            let head = signature(&body);
            let returned = head.split_once("->").map_or("", |(_, returned)| returned);
            functions.push(CensusFunction {
                site: format!("{file}::{name}"),
                is_method: head.contains("self"),
                takes_transaction: head.contains(": Transaction"),
                returns_staged: returned.contains("Transaction") || returned.contains("Action"),
                body,
            });
        }
    }
    functions
}

fn name_of(function: &CensusFunction) -> &str {
    function
        .site
        .rsplit("::")
        .next()
        .unwrap_or(function.site.as_str())
}

fn stages_directly(function: &CensusFunction) -> bool {
    STAGING_CALLS.iter().any(|call| {
        call_arguments(&function.body, call, true)
            .iter()
            .any(|arguments| !EMPTY_STAGING.contains(&arguments.trim()))
    })
}

fn commits_directly(function: &CensusFunction) -> bool {
    COMMIT_CALLS.iter().any(|call| {
        call_arguments(&function.body, call, true)
            .iter()
            .any(|arguments| !arguments.trim().is_empty())
    })
}

fn calls(caller: &CensusFunction, callee: &CensusFunction) -> bool {
    !call_arguments(&caller.body, name_of(callee), callee.is_method).is_empty()
}

fn closure(
    functions: &[CensusFunction],
    seed: impl Fn(&CensusFunction) -> bool,
    step: impl Fn(&CensusFunction, &[bool]) -> bool,
) -> Vec<bool> {
    let mut members: Vec<bool> = functions.iter().map(&seed).collect();
    loop {
        let next: Vec<bool> = functions
            .iter()
            .zip(&members)
            .map(|(function, member)| *member || step(function, &members))
            .collect();
        if next == members {
            return members;
        }
        members = next;
    }
}

fn stage_then_commit_sites(functions: &[CensusFunction]) -> BTreeSet<String> {
    let direct_commit: Vec<bool> = functions.iter().map(commits_directly).collect();
    let direct_stage: Vec<bool> = functions.iter().map(stages_directly).collect();
    let calls_member = |function: &CensusFunction, members: &[bool]| {
        functions
            .iter()
            .zip(members)
            .any(|(callee, member)| *member && calls(function, callee))
    };
    let helpers = closure(
        functions,
        |function| {
            let index = functions
                .iter()
                .position(|candidate| std::ptr::eq(candidate, function))
                .unwrap_or_default();
            direct_commit[index] && function.takes_transaction && !direct_stage[index]
        },
        |function, members| {
            function.takes_transaction
                && !stages_directly(function)
                && calls_member(function, members)
        },
    );
    let commits: Vec<bool> = functions
        .iter()
        .enumerate()
        .map(|(index, function)| {
            !helpers[index] && (direct_commit[index] || calls_member(function, &helpers))
        })
        .collect();
    let committing: Vec<bool> = commits
        .iter()
        .zip(&helpers)
        .map(|(commit, helper)| *commit || *helper)
        .collect();
    let stagers = closure(functions, stages_directly, |function, members| {
        functions
            .iter()
            .zip(members)
            .zip(&committing)
            .any(|((callee, member), commit)| {
                *member
                    && !*commit
                    && callee.returns_staged
                    && !TRANSACTION_API_NAMES.contains(&name_of(callee))
                    && calls(function, callee)
            })
    });
    functions
        .iter()
        .enumerate()
        .filter(|(index, _)| commits[*index] && stagers[*index])
        .map(|(_, function)| function.site.clone())
        .collect()
}

fn staged_variables(body: &str) -> BTreeSet<String> {
    STAGING_CALLS
        .iter()
        .flat_map(|call| call_arguments(body, call, true))
        .map(|arguments| {
            arguments
                .trim()
                .trim_start_matches('&')
                .trim_end_matches(".clone()")
                .to_string()
        })
        .filter(|argument| is_identifier(argument))
        .collect()
}

fn binding(body: &str, variable: &str) -> Option<String> {
    for pattern in [
        format!("let {variable} = "),
        format!("let mut {variable} = "),
    ] {
        if let Some(start) = body.find(&pattern) {
            let rest = &body[start + pattern.len()..];
            return rest.find(';').map(|end| rest[..end].trim().to_string());
        }
    }
    None
}

fn staged_origin(body: &str, variable: &str) -> Option<String> {
    let mut current = variable.to_string();
    for _ in 0..MAX_BINDING_HOPS {
        let bound = binding(body, &current)?;
        if let Some(inner) = bound
            .strip_prefix("written_file_paths(&")
            .and_then(|rest| rest.strip_suffix(')'))
        {
            return Some(inner.trim().to_string());
        }
        if !is_identifier(&bound) {
            return None;
        }
        current = bound;
    }
    None
}

fn sweep_faults(function: &CensusFunction) -> Vec<String> {
    let sweeps: Vec<String> = SWEEP_CALLS
        .iter()
        .flat_map(|call| call_arguments(&function.body, call, false))
        .collect();
    if sweeps.is_empty() {
        return vec![format!("{}: no call to the shared sweep", function.site)];
    }
    let staged = staged_variables(&function.body);
    let mut faults = Vec::new();
    for arguments in sweeps {
        let parts = split_arguments(&arguments);
        let paths = parts.get(1).map(String::as_str).unwrap_or_default();
        let variable = paths.trim_start_matches('&').trim();
        if !is_identifier(variable) {
            faults.push(format!(
                "{}: sweeps `{paths}`, not the paths it staged",
                function.site
            ));
            continue;
        }
        match staged_origin(&function.body, variable) {
            Some(origin) if staged.contains(&origin) => {}
            origin => faults.push(format!(
                "{}: sweeps `{variable}` (from {origin:?}), not the paths of {staged:?}",
                function.site
            )),
        }
    }
    faults
}

#[test]
fn every_stage_then_commit_site_sweeps_staged_files_on_a_keyed_refusal() {
    let functions = census_functions();
    let found = stage_then_commit_sites(&functions);
    let listed: BTreeSet<String> = SWEEP_SITES
        .iter()
        .map(|(file, name)| format!("{file}::{name}"))
        .collect();
    let excepted: BTreeSet<String> = CENSUS_EXCEPTIONS
        .iter()
        .map(|(file, name, _)| format!("{file}::{name}"))
        .collect();
    let unlisted: Vec<&String> = found
        .iter()
        .filter(|site| !listed.contains(*site) && !excepted.contains(*site))
        .collect();
    assert!(
        unlisted.is_empty(),
        "ENC-1 residue item 1: a stage-then-commit site outside the census: {unlisted:?}"
    );
    let vanished: Vec<&String> = listed
        .union(&excepted)
        .filter(|site| !found.contains(*site))
        .collect();
    assert!(
        vanished.is_empty(),
        "ENC-1 residue item 1: a census row no longer commits staged files: {vanished:?}"
    );
    let faults: Vec<String> = functions
        .iter()
        .filter(|function| listed.contains(&function.site))
        .flat_map(sweep_faults)
        .collect();
    assert!(
        faults.is_empty(),
        "ENC-1 residue item 1: a census site does not sweep what it staged:\n{}",
        faults.join("\n")
    );
}

#[test]
fn the_census_finds_a_helper_split_site_and_an_emptied_sweep() {
    let helper_split = CensusFunction {
        site: "probe::split".to_string(),
        is_method: false,
        takes_transaction: false,
        returns_staged: false,
        body: "fn split(c: &C, t: &Table, files: Vec<DataFile>) -> R {\n    \
               let tx = staged(t, files)?;\n    tx.commit(c.as_ref()).await\n}\n"
            .to_string(),
    };
    let staging_helper = CensusFunction {
        site: "probe::staged".to_string(),
        is_method: false,
        takes_transaction: false,
        returns_staged: true,
        body: "fn staged(t: &Table, files: Vec<DataFile>) -> Result<Transaction> {\n    \
               let tx = Transaction::new(t);\n    tx.fast_append().add_data_files(files).apply(tx)\n}\n"
            .to_string(),
    };
    let found = stage_then_commit_sites(&[helper_split, staging_helper]);
    assert_eq!(found, BTreeSet::from(["probe::split".to_string()]));
    let emptied = CensusFunction {
        site: "probe::emptied".to_string(),
        is_method: false,
        takes_transaction: false,
        returns_staged: false,
        body: "fn emptied() {\n    let staged_paths = written_file_paths(&new_files);\n    \
               let tx = tx.fast_append().add_data_files(new_files);\n    \
               commit_with_keyed_sweep(table, &staged_paths[..0], outcome, &id).await\n}\n"
            .to_string(),
    };
    assert_eq!(sweep_faults(&emptied).len(), 1);
    let swept = CensusFunction {
        body: emptied.body.replace("&staged_paths[..0]", "&staged_paths"),
        ..emptied
    };
    assert!(sweep_faults(&swept).is_empty());
}

#[test]
fn the_catalog_provider_installs_the_encryption_guard_once() {
    let sources = product_sources();
    let text = |wanted: &str| {
        sources
            .iter()
            .find(|(file, _)| file == wanted)
            .map(|(_, text)| text.clone())
            .expect("the file is scanned")
    };
    let try_new = function_bodies(&text("repark-iceberg/src/catalog/provider.rs"))
        .into_iter()
        .find(|(name, _)| name == "try_new")
        .map(|(_, body)| body)
        .expect("ReparkCatalogProvider::try_new is scanned");
    assert!(
        try_new.contains("EncryptionGuardCatalog::install("),
        "ENC-1 residue item 5: ReparkCatalogProvider::try_new must install the guard"
    );
    assert!(
        !text("repark-core/src/session.rs").contains("EncryptionGuardCatalog::install"),
        "ENC-1 residue item 5: the guard is installed once, in ReparkCatalogProvider::try_new"
    );
}
