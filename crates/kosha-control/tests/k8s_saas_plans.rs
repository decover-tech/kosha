//! The billing catalog shipped with the multi-tenant SaaS overlay (#359)
//! must load: `KOSHA_BILLING=on` stops every pod at startup otherwise.

use kosha_control::billing::Catalog;

#[test]
fn saas_overlay_billing_catalog_parses() {
    let path =
        std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../../k8s/saas/billing-plans.json");
    let text = std::fs::read_to_string(&path).expect("k8s/saas/billing-plans.json");
    let catalog = Catalog::parse(&text).expect("valid catalog");
    // No prices are committed until the pricing hypothesis is decided: no
    // default plan, so no tenant is billed or restricted by the catalog.
    assert!(catalog.default_plan.is_none());
    assert!(catalog.upgrade_plan.is_none());
}
