//! Storage and table options handling module
//!
//! This module provides types and utilities for handling options in:
//! - `CREATE TABLESPACE` statements
//! - `CREATE TABLE` statements (with custom access methods)
//!
pub mod am_cache;
mod storage;
mod table;
mod tablespace;
pub mod tablespace_cache;
pub mod utils;

// Re-export commonly used types
pub use am_cache::{AmCache, AmCacheable};
pub use storage::{
    extract_and_remove_options, OptionKind, StorageCategory, TamOptionDef,
};
pub use table::{TableOptionError, TableOptions};
pub use tablespace::{TablespaceError, TablespaceOptions};
pub use utils::{append_string, get_string_at_offset};
