use pgrx::prelude::*;
use std::sync::OnceLock;

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

#[cfg(test)]
pub mod pg_test {
    pub fn setup(_options: Vec<&str>) {
        // noop
    }

    pub fn postgresql_conf_options() -> Vec<&'static str> {
        vec![]
    }
}
