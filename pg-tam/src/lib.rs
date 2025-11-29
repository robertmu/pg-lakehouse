//! pg-tam: A framework for building PostgreSQL Table Access Methods in Rust
//!
//! This library provides a safe, ergonomic API for implementing custom table access
//! methods for PostgreSQL using the pgrx framework.
//!
//! # Quick Start
//!
//! ```rust,no_run
//! use pg_tam::prelude::*;
//!
//! #[pg_table_am(
//!     version = "0.1.0",
//!     author = "Your Name",
//!     website = "https://github.com/your/repo"
//! )]
//! pub struct MyTableAm;
//!
//! impl TableAccessMethod<MyError> for MyTableAm {
//!     type ScanState = MyScan;
//!     type RelationState = MyRelation;
//!     type IndexState = MyIndex;
//!     type DdlState = MyDdl;
//!     type ModifyState = MyModify;
//! }
//! ```

/// Core trait definitions for table access methods
pub mod api;

/// Safe wrapper types for PostgreSQL FFI types
pub mod handles;

/// PostgreSQL data types (Cell, Row)
pub mod data;

/// Sequential and bitmap scan operations
pub mod scan;

/// Index fetch operations
pub mod index;

/// Tuple modification operations (insert, update, delete)
pub mod dml;

/// DDL operations (CREATE, TRUNCATE, etc.)
pub mod ddl;

/// Relation metadata and utility operations
pub mod relation;

/// Registration logic for Table Access Method routines
pub mod registry;

/// Storage options handling (low-level FFI)
pub mod storage_options;

/// Tablespace options (type-safe wrapper)
pub mod tablespace;

/// Generic ProcessUtility hook framework
pub mod utility_hook;

/// Helper functions and utilities
pub mod utils;

/// The prelude includes all necessary imports to make pg_tam work
pub mod prelude {
    pub use crate::api::*;
    pub use crate::data::*;
    pub use crate::handles::*;
    pub use crate::pg_table_am;
    pub use crate::registry::make_table_am_routine;
    pub use crate::tablespace::{TablespaceError, TablespaceOptions};
    pub use crate::utility_hook::{
        register_utility_hook, UtilityHook, UtilityHookError, UtilityNode,
    };
    pub use crate::utils::{
        create_async_runtime, log_debug1, report_error, report_info, report_notice,
        report_warning, CreateRuntimeError,
    };
    pub use tokio::runtime::Runtime;
}

use pgrx::prelude::*;
use pgrx::AllocatedByPostgres;

/// Internal memory context management
mod memory;

/// PgBox'ed `TableAmRoutine`, used in [`am_routine`](api::TableAccessMethod::am_routine)
pub type TableAmRoutine<A = AllocatedByPostgres> = PgBox<pg_sys::TableAmRoutine, A>;

/// Procedural macro for generating table access method boilerplate
pub use pg_tam_macros::pg_table_am;
