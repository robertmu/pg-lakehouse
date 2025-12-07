//! Storage and table options handling module
//!
//! This module provides types and utilities for handling options in:
//! - `CREATE TABLESPACE` statements
//! - `CREATE TABLE` statements (with custom access methods)
//!
mod storage;
mod table;
mod tablespace;
pub mod tablespace_cache;

// Re-export commonly used types
pub use storage::{
    extract_and_remove_options, OptionKind, StorageCategory, TamOptionDef,
};
pub use table::{TableOptionError, TableOptions};
pub use tablespace::{TablespaceError, TablespaceOptions};
