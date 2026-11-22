//! Keeps `k8s/components/internal-tls` (#343) in step with the base
//! manifests it patches. Setting `KOSHA_TLS_CERT_PATH` makes the listener
//! HTTPS-only, so a probe left on plain HTTP restarts the pod: the component
//! has to cover every probe of every Deployment, and every TLS setting the
//! base ConfigMap documents.

use std::collections::BTreeSet;
use std::path::{Path, PathBuf};

fn k8s() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../../k8s")
        .canonicalize()
        .expect("k8s directory")
}

fn read(path: &Path) -> String {
    std::fs::read_to_string(path).unwrap_or_else(|e| panic!("read {}: {e}", path.display()))
}

/// The lines of a YAML file that are not comments, trimmed.
fn code_lines(text: &str) -> Vec<&str> {
    text.lines()
        .map(str::trim)
        .filter(|line| !line.is_empty() && !line.starts_with('#'))
        .collect()
}

/// The key of a `KEY: value` line when the key is a TLS setting.
fn tls_key(line: &str) -> Option<&str> {
    let (key, _) = line.split_once(':')?;
    (key.starts_with("KOSHA_TLS_") || key.starts_with("KOSHA_INTERNAL_TLS")).then_some(key)
}

const PROBES: [&str; 3] = ["readinessProbe", "startupProbe", "livenessProbe"];

#[test]
fn probes_patch_covers_every_probe_of_every_deployment() {
    let k8s = k8s();
    let component = k8s.join("components/internal-tls");
    let kustomization = read(&component.join("kustomization.yaml"));
    assert!(code_lines(&kustomization).contains(&"kind: Component"));
    // The one JSON patch is aimed at all three Deployments by name.
    assert!(
        code_lines(&kustomization).contains(&"name: kosha(-query|-indexer)?"),
        "{kustomization}"
    );

    let patch = read(&component.join("probes-https-patch.yaml"));
    let patch = code_lines(&patch);
    for probe in PROBES {
        let path = format!("path: /spec/template/spec/containers/0/{probe}/httpGet/scheme");
        assert!(patch.contains(&path.as_str()), "no patch for {probe}");
    }
    assert_eq!(
        patch.iter().filter(|l| **l == "value: HTTPS").count(),
        PROBES.len()
    );

    for (file, name) in [
        ("deployment.yaml", "kosha"),
        ("deployment-query.yaml", "kosha-query"),
        ("deployment-indexer.yaml", "kosha-indexer"),
    ] {
        let text = read(&k8s.join("base").join(file));
        let lines = code_lines(&text);
        assert!(lines.contains(&format!("name: {name}").as_str()), "{file}");
        // The patch addresses container 0 and an existing `httpGet`: one
        // container, and exactly the three HTTP probes it names.
        let count = |wanted: &str| lines.iter().filter(|l| **l == wanted).count();
        assert_eq!(count("containers:"), 1, "{file}: one container list");
        assert_eq!(
            lines.iter().filter(|l| l.starts_with("image:")).count(),
            1,
            "{file}: the patch only reaches container 0"
        );
        for probe in PROBES {
            assert_eq!(count(&format!("{probe}:")), 1, "{file}: {probe}");
        }
        assert_eq!(count("httpGet:"), PROBES.len(), "{file}: HTTP probes");
        assert_eq!(count("scheme: HTTPS"), 0, "{file}: base stays on HTTP");
    }
}

#[test]
fn component_sets_every_tls_setting_the_base_configmap_documents() {
    let k8s = k8s();
    // The base ConfigMap only lists the TLS settings, commented out.
    let base = read(&k8s.join("base/configmap.yaml"));
    assert!(
        code_lines(&base).iter().all(|l| tls_key(l).is_none()),
        "base/configmap.yaml must leave TLS off"
    );
    let documented: BTreeSet<&str> = base
        .lines()
        .filter_map(|line| tls_key(line.trim().strip_prefix("# ")?))
        .collect();
    assert!(documented.contains("KOSHA_TLS_CERT_PATH"), "{documented:?}");

    let patch = read(&k8s.join("components/internal-tls/configmap-patch.yaml"));
    let set: BTreeSet<&str> = code_lines(&patch).into_iter().filter_map(tls_key).collect();
    assert_eq!(set, documented);

    // Query pods must forward writes to the now HTTPS-only ingest pod.
    let ingest = read(&k8s.join("components/internal-tls/ingest-host-patch.yaml"));
    assert!(
        code_lines(&ingest)
            .iter()
            .any(|l| l.starts_with("value: https://kosha-ingest-service.")),
        "{ingest}"
    );
}

#[test]
fn stage_and_prod_do_not_include_the_component() {
    for overlay in ["stage", "prod"] {
        let text = read(&k8s().join(overlay).join("kustomization.yaml"));
        assert!(
            !code_lines(&text).iter().any(|l| l.contains("components")),
            "{overlay} must not turn TLS on by default"
        );
    }
}
