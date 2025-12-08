use crate::ICEBERG_AM_NAME;
use pg_tam::option::{OptionKind, StorageCategory, TamOptionDef};
use pg_tam::pg_wrapper::PgWrapper;
use pg_tam::prelude::*;
use pgrx::pg_sys;
use std::ffi::CStr;

// Define Iceberg-specific table options.
static ICEBERG_TABLE_OPTIONS: &[TamOptionDef] = &[
    TamOptionDef {
        name: "format-version",
        category: StorageCategory::Common,
        kind: OptionKind::Int {
            default: 2,
            min: Some(1),
            max: Some(2),
        },
        description: "Iceberg table format version (1 or 2)",
    },
    TamOptionDef {
        name: "write.parquet.compression-codec",
        category: StorageCategory::Common,
        kind: OptionKind::String {
            default: Some("zstd"),
        },
        description: "Parquet compression codec (snappy, zstd)",
    },
    TamOptionDef {
        name: "write.format.default",
        category: StorageCategory::Common,
        kind: OptionKind::Enum {
            default: "parquet",
            values: &["parquet", "avro", "orc"],
        },
        description: "Default file format (parquet, avro, orc)",
    },
];

struct IcebergTableHook;

/// Check if the CREATE TABLE statement uses the 'iceberg' access method.
fn is_iceberg_access_method(stmt: &pg_sys::CreateStmt) -> bool {
    unsafe {
        let am = stmt.accessMethod;
        if am.is_null() {
            return false;
        }
        CStr::from_ptr(am).to_string_lossy() == ICEBERG_AM_NAME
    }
}

impl UtilityHook for IcebergTableHook {
    fn on_pre(&self, context: &mut UtilityNode) -> Result<(), UtilityHookError> {
        let stmt = context
            .is_a_mut::<pg_sys::CreateStmt>(pg_sys::NodeTag::T_CreateStmt)
            .expect("Hook registered for T_CreateStmt");

        if !is_iceberg_access_method(stmt) {
            return Ok(());
        }

        // Extract and validate options (but don't persist yet, just validate)
        TableOptions::extract_from_stmt(stmt, Some(ICEBERG_TABLE_OPTIONS)).map_err(|e| {
            UtilityHookError::Internal(format!("table: option extraction failed - {}", e))
        })?;
        Ok(())
    }

    fn on_post(&self, context: &mut UtilityNode) -> Result<(), UtilityHookError> {
        let stmt = context
            .is_a_mut::<pg_sys::CreateStmt>(pg_sys::NodeTag::T_CreateStmt)
            .expect("Hook registered for T_CreateStmt");

        if !is_iceberg_access_method(stmt) {
            return Ok(());
        }

        if let Ok(Some(opts)) = TableOptions::extract_from_stmt(stmt, Some(ICEBERG_TABLE_OPTIONS)) {
            // Resolve the OID of the newly created table using RangeVarGetRelidExtended
            // This correctly handles search_path resolution when schema is not specified
            let oid = PgWrapper::range_var_get_relid(
                stmt.relation,
                pg_sys::NoLock as pg_sys::LOCKMODE,
                false, // fail if missing (shouldn't happen in on_post of CreateStmt)
            )
            .map_err(|e| {
                UtilityHookError::Internal(format!("table: failed to get table OID - {}", e))
            })?;

            opts.persist_to_catalog(oid).map_err(|e| {
                UtilityHookError::Internal(format!(
                    "table: failed to persist options to catalog - {}",
                    e
                ))
            })?;
        }
        Ok(())
    }
}

pub fn init_hook() {
    register_utility_hook(pg_sys::NodeTag::T_CreateStmt, Box::new(IcebergTableHook));
}
