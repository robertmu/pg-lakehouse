//! Iceberg table options cache for rd_amcache.
//!
//! This module provides a cached view of Iceberg table options that can be stored
//! in PostgreSQL's `rd_amcache` field for efficient access during table operations.
//!
//! # Memory Layout
//!
//! The cache uses a contiguous memory block allocated via `palloc`:
//!
//! ```text
//! +-------------------------------------------------------+
//! |  IcebergTableOptionCache (Fixed Size, #[repr(C)])     | <- rd_amcache points here
//! |-------------------------------------------------------|
//! |  format_version: i32                                  |
//! |  compression_offset: u32  (relative to struct start)  |
//! |  write_format_offset: u32                             |
//! +-------------------------------------------------------+
//! |  Variable Data Area (u8 bytes)                        |
//! |-------------------------------------------------------|
//! |  "zstd\0"                                             |
//! |  "parquet\0"                                          |
//! +-------------------------------------------------------+
//! ```

use pg_tam::option::{AmCacheable, TableOptions, append_string, get_string_at_offset};

/// Iceberg table options cached in rd_amcache.
///
/// This struct is stored directly in Postgres memory via palloc.
/// All fields must be POD types (no String, Vec, Box).
#[repr(C)]
#[derive(Clone, Copy)]
pub struct IcebergTableOptionCache {
    pub format_version: i32,
    compression_offset: u32,
    write_format_offset: u32,
}

// SAFETY: IcebergTableOptionCache is #[repr(C)] and contains only POD types.
// No heap allocations, no Drop implementations needed.
unsafe impl AmCacheable for IcebergTableOptionCache {
    fn from_options(opts: &TableOptions) -> (Self, Vec<u8>) {
        let header_size = std::mem::size_of::<Self>();
        let mut data = Vec::new();

        let format_version = opts.get_int("format-version").unwrap_or(2);
        let compression = opts
            .get_str("write.parquet.compression-codec")
            .unwrap_or_else(|| "zstd".to_string());
        let write_format = opts
            .get_str("write.format.default")
            .unwrap_or_else(|| "parquet".to_string());

        let compression_offset = append_string(&mut data, header_size, &compression);
        let write_format_offset = append_string(&mut data, header_size, &write_format);

        (
            Self {
                format_version,
                compression_offset,
                write_format_offset,
            },
            data,
        )
    }

    fn default_options() -> (Self, Vec<u8>) {
        let header_size = std::mem::size_of::<Self>();
        let mut data = Vec::new();

        let compression_offset = append_string(&mut data, header_size, "zstd");
        let write_format_offset = append_string(&mut data, header_size, "parquet");

        (
            Self {
                format_version: 2,
                compression_offset,
                write_format_offset,
            },
            data,
        )
    }
}

impl IcebergTableOptionCache {
    pub fn compression(&self) -> &str {
        unsafe { get_string_at_offset(self as *const _ as *const u8, self.compression_offset) }
    }

    pub fn write_format(&self) -> &str {
        unsafe { get_string_at_offset(self as *const _ as *const u8, self.write_format_offset) }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_options() {
        let (cache, data) = IcebergTableOptionCache::default_options();
        assert_eq!(cache.format_version, 2);
        // Verify offsets are non-zero (strings are stored)
        assert!(cache.compression_offset > 0);
        assert!(cache.write_format_offset > 0);
        // Verify data contains expected strings with null terminators
        assert!(data.len() > 0);
    }
}