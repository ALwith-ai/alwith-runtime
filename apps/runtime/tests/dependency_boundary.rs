//! Runtime must build without concrete application capability implementations.
#[test]
fn every_path_dependency_stays_inside_the_runtime_repository() {
    let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../..").canonicalize().unwrap();
    for directory in ["apps", "crates"] {
        for entry in std::fs::read_dir(root.join(directory)).unwrap() {
            let manifest = entry.unwrap().path().join("Cargo.toml");
            if !manifest.is_file() {
                continue;
            }
            let text = std::fs::read_to_string(&manifest).unwrap();
            assert!(
                !text.contains("alwith-modules/") && !text.contains("alwith-api"),
                "{} depends on a consumer or concrete capability repository",
                manifest.display()
            );
            for line in text.lines() {
                if let Some((_, value)) = line.split_once("path = \"") {
                    let path = value.split('"').next().unwrap();
                    let resolved = manifest.parent().unwrap().join(path).canonicalize().unwrap();
                    assert!(
                        resolved.starts_with(&root),
                        "{} escapes Runtime: {}",
                        manifest.display(),
                        resolved.display()
                    );
                }
            }
        }
    }
}
