use crate::storage_options;
use pgrx::pg_sys;
use pgrx::pg_sys::panic::ErrorReport;
use pgrx::prelude::PgSqlErrorCode;
use thiserror::Error;

// ============================================================================
//  Error Type
// ============================================================================
#[derive(Error, Debug, Clone)]
pub enum TablespaceError {
    #[error("invalid tablespace option: {0}")]
    InvalidOption(String),
}

impl From<TablespaceError> for ErrorReport {
    fn from(value: TablespaceError) -> Self {
        let error_message = format!("{value}");
        ErrorReport::new(
            PgSqlErrorCode::ERRCODE_INVALID_PARAMETER_VALUE,
            error_message,
            "",
        )
    }
}

#[derive(Debug)]
pub struct TablespaceOptions {
    options: Vec<(String, Option<String>)>,
}

impl TablespaceOptions {
    pub fn extract_from_stmt<E>(
        stmt: &mut pg_sys::CreateTableSpaceStmt,
    ) -> Result<Option<Self>, E>
    where
        E: From<TablespaceError>,
    {
        // Call into the FFI layer (unsafe)
        // SAFETY: We hold a mutable reference to the statement, so it is safe to modify it via FFI.
        let opts = unsafe {
            storage_options::extract_and_remove_custom_options(stmt)
                .map_err(|msg| E::from(TablespaceError::InvalidOption(msg)))?
        };

        if opts.is_empty() {
            Ok(None)
        } else {
            Ok(Some(Self { options: opts }))
        }
    }

    pub fn persist_to_catalog(&self, spcoid: pg_sys::Oid) {
        storage_options::update_tablespace_options(spcoid, self.options.clone());
    }
}
