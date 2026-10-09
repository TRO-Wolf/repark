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
    (
        "Table::builder()",
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
