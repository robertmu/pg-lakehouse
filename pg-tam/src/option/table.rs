//! TableOptions - Type-safe wrapper for table options.
//!
//! This module provides a high-level interface for extracting and persisting
//! custom options from `CREATE TABLE` statements with custom access methods.

use super::storage::{self, TamOptionDef};
use crate::catalog;
use crate::pg_wrapper::PgWrapper;
use pgrx::pg_sys;
use pgrx::pg_sys::panic::ErrorReport;
use pgrx::prelude::PgSqlErrorCode;
use pgrx::IntoDatum;
use thiserror::Error;

// ============================================================================
//  Error Type
// ============================================================================

/// Errors that can occur when handling table options.
#[derive(Error, Debug, Clone)]
pub enum TableOptionError {
    #[error("invalid table option: {0}")]
    InvalidOption(String),
    #[error("failed to persist table options: {0}")]
    PersistFailed(String),
}

impl From<TableOptionError> for ErrorReport {
    fn from(value: TableOptionError) -> Self {
        let error_message = format!("{value}");
        ErrorReport::new(
            PgSqlErrorCode::ERRCODE_INVALID_PARAMETER_VALUE,
            error_message,
            "",
        )
    }
}

// ============================================================================
//  TableOptions
// ============================================================================

/// Wrapper for custom table options extracted from `CREATE TABLE` statements.
#[derive(Debug, Clone)]
pub struct TableOptions {
    options: Vec<(String, Option<String>)>,
}

impl TableOptions {
    pub fn new(options: Vec<(String, Option<String>)>) -> Self {
        Self { options }
    }

    pub fn extract_from_stmt(
        stmt: &mut pg_sys::CreateStmt,
        valid_options: Option<&[TamOptionDef]>,
    ) -> Result<Option<Self>, TableOptionError> {
        let options_def = valid_options.unwrap();

        // SAFETY: We hold a mutable reference to the statement, so it is safe to modify via FFI.
        let opts = unsafe {
            storage::extract_and_remove_options(&mut stmt.options, options_def)
                .map_err(TableOptionError::InvalidOption)?
        };

        Ok((!opts.is_empty()).then(|| Self { options: opts }))
    }

    pub fn persist_to_catalog(
        &self,
        relid: pg_sys::Oid,
    ) -> Result<(), TableOptionError> {
        // If no options, nothing to persist
        if self.options.is_empty() {
            return Ok(());
        }

        // Get OID of lakehouse.table_options from Catalog cache
        let table_oid = catalog::get_table_options_oid()
            .map_err(|e| TableOptionError::PersistFailed(e.to_string()))?;

        unsafe {
            let rel = PgWrapper::table_open(table_oid, pg_sys::RowExclusiveLock as _)
                .map_err(|e| TableOptionError::PersistFailed(e.to_string()))?;

            let relid_datum = relid.into_datum().unwrap();

            let options_vec: Vec<String> = self
                .options
                .iter()
                .map(|(k, v)| {
                    let val = v.as_ref().map(|s| s.as_str()).unwrap_or("");
                    format!("{}={}", k, val)
                })
                .collect();

            let options_datum = options_vec.into_datum();

            let mut values =
                [relid_datum, options_datum.unwrap_or(pg_sys::Datum::from(0))];
            let mut nulls = [false, options_datum.is_none()];

            let tup_desc = (*rel).rd_att;
            let tuple = pg_sys::heap_form_tuple(
                tup_desc,
                values.as_mut_ptr(),
                nulls.as_mut_ptr(),
            );

            PgWrapper::catalog_tuple_insert(rel, tuple)
                .map_err(|e| TableOptionError::PersistFailed(e.to_string()))?;

            pg_sys::heap_freetuple(tuple);
            PgWrapper::table_close(rel, pg_sys::RowExclusiveLock as _)
                .map_err(|e| TableOptionError::PersistFailed(e.to_string()))?;
        }

        Ok(())
    }
}
