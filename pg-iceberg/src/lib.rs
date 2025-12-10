use crate::error::IcebergError;
use pg_tam::prelude::*;
use pgrx::prelude::*;
use std::sync::OnceLock;

mod access;
pub mod error;
pub mod hooks;

/// The access method name for Iceberg tables.
pub const ICEBERG_AM_NAME: &str = "iceberg";
use access::ddl::IcebergDdl;
use access::dml::IcebergModify;
use access::index::IcebergIndex;
use access::relation::IcebergRelation;
use access::scan::IcebergScan;

// crypto primitive provider initialization required by rustls > v0.22.
// It is not required by every FDW, but only call it when needed.
// ref: https://docs.rs/rustls/latest/rustls/index.html#cryptography-providers
static RUSTLS_CRYPTO_PROVIDER_LOCK: OnceLock<()> = OnceLock::new();

#[allow(dead_code)]
fn setup_rustls_default_crypto_provider() {
    RUSTLS_CRYPTO_PROVIDER_LOCK.get_or_init(|| {
        rustls::crypto::aws_lc_rs::default_provider()
            .install_default()
            .unwrap()
    });
}

pg_module_magic!();

extension_sql_file!("../sql/bootstrap.sql", bootstrap);
extension_sql_file!("../sql/finalize.sql", finalize);

#[pg_guard]
extern "C-unwind" fn _PG_init() {
    setup_rustls_default_crypto_provider();
    hooks::init_hooks();
}

// ============================================================================
//  Table Access Method Definition
// ============================================================================

#[pg_table_am(
    version = "0.1.0",
    author = "robertmu",
    website = "https://github.com/robertmu/pg-lakehouse"
)]
pub struct IcebergTableAm;

impl TableAccessMethod<IcebergError> for IcebergTableAm {
    type ScanState = IcebergScan;
    type RelationState = IcebergRelation;
    type IndexState = IcebergIndex;
    type DdlState = IcebergDdl;
    type ModifyState = IcebergModify;
}

#[cfg(test)]
pub mod pg_test {
    pub fn setup(_options: Vec<&str>) {
        // noop
    }

    pub fn postgresql_conf_options() -> Vec<&'static str> {
        vec![]
    }
}
