use pgrx::pg_sys;
use pgrx::prelude::*;
use std::ffi::CStr;

// ============================================================================
//  Common Lakehouse Tablespace Option Definitions
// ============================================================================

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum StorageCategory {
    Common,
    S3,
    GCS,
    Azure,
    FileSystem,
}

#[derive(Debug, Clone)]
pub enum OptionKind {
    Bool {
        default: bool,
    },
    Int {
        default: i32,
        min: Option<i32>,
        max: Option<i32>,
    },
    String {
        default: Option<&'static str>,
    },
}

pub struct StorageOptionDef {
    pub name: &'static str, // Core name without prefix, e.g., "bucket", "region"
    pub category: StorageCategory,
    pub kind: OptionKind,
    pub description: &'static str,
}

// Define all standard options common to storage extensions.
static STANDARD_STORAGE_OPTIONS: &[StorageOptionDef] = &[
    // --- Common ---
    StorageOptionDef {
        name: "protocol",
        category: StorageCategory::Common,
        kind: OptionKind::String {
            default: Some("s3"),
        },
        description: "Storage protocol (s3, gcs, azure, fs)",
    },
    // --- S3 ---
    StorageOptionDef {
        name: "bucket",
        category: StorageCategory::S3,
        kind: OptionKind::String { default: None },
        description: "S3 Bucket name",
    },
    StorageOptionDef {
        name: "region",
        category: StorageCategory::S3,
        kind: OptionKind::String {
            default: Some("us-east-1"),
        },
        description: "AWS Region",
    },
    StorageOptionDef {
        name: "endpoint",
        category: StorageCategory::S3,
        kind: OptionKind::String { default: None },
        description: "Custom S3 Endpoint (for MinIO etc)",
    },
    StorageOptionDef {
        name: "allow_http",
        category: StorageCategory::S3,
        kind: OptionKind::Bool { default: false },
        description: "Allow HTTP connections (insecure)",
    },
    StorageOptionDef {
        name: "access_key_id",
        category: StorageCategory::S3,
        kind: OptionKind::String { default: None },
        description: "S3 Access Key ID",
    },
    StorageOptionDef {
        name: "secret_access_key",
        category: StorageCategory::S3,
        kind: OptionKind::String { default: None },
        description: "S3 Secret Access Key",
    },
    // --- FileSystem ---
    StorageOptionDef {
        name: "io_concurrency",
        category: StorageCategory::FileSystem,
        kind: OptionKind::Int {
            default: 4,
            min: Some(1),
            max: Some(64),
        },
        description: "IO Concurrency for local operations",
    },
];

// ============================================================================
//  Extraction Logic
// ============================================================================

/// Extract and remove custom storage options from CreateTableSpaceStmt.
///
/// This function iterates through the options provided in the `CREATE TABLESPACE` statement.
/// If an option matches one of the `STANDARD_STORAGE_OPTIONS`, it is validated, extracted,
/// and removed from the statement's options list.
///
/// # Arguments
/// * `stmt` - Pointer to the Postgres CreateTableSpaceStmt
///
/// # Returns
/// * `Ok(Vec<(String, Option<String>)>)` - A list of validated custom options.
/// * `Err(String)` - If validation fails for any option.
pub unsafe fn extract_and_remove_custom_options(
    stmt: *mut pg_sys::CreateTableSpaceStmt,
) -> Result<Vec<(String, Option<String>)>, String> {
    let mut custom_opts = Vec::new();
    let mut new_pg_opts: *mut pg_sys::List = std::ptr::null_mut();

    if (*stmt).options.is_null() {
        return Ok(custom_opts);
    }

    let cell = (*(*stmt).options).elements;
    let length = (*(*stmt).options).length;

    for i in 0..length {
        let def_elem_ptr = (*cell.add(i as usize)).ptr_value as *mut pg_sys::DefElem;
        let def_name_cstr = CStr::from_ptr((*def_elem_ptr).defname);
        let def_name = def_name_cstr.to_string_lossy().to_string();

        // Check if this option is one of our standard storage options
        if let Some(def) = STANDARD_STORAGE_OPTIONS
            .iter()
            .find(|opt| opt.name == def_name)
        {
            // 1. Extract raw value
            // Use Postgres internal helper `defGetString` which handles T_String, T_Integer, T_Float, and T_A_Const (PG15+)
            let raw_val = if (*def_elem_ptr).arg.is_null() {
                None
            } else {
                let val_ptr = pg_sys::defGetString(def_elem_ptr);
                if val_ptr.is_null() {
                    None
                } else {
                    Some(CStr::from_ptr(val_ptr).to_string_lossy().to_string())
                }
            };

            // 2. Check for duplicate options
            if custom_opts.iter().any(|(k, _)| k == &def_name) {
                return Err(format!(
                    "option '{}' specified more than once",
                    def_name
                ));
            }

            // 3. Validate and normalize value
            match validate_option_value(def, raw_val) {
                Ok(validated_val) => {
                    custom_opts.push((def_name, validated_val));
                }
                Err(e) => {
                    return Err(format!(
                        "Invalid value for option '{}': {}",
                        def_name, e
                    ));
                }
            }
        } else {
            // Not a known storage option, leave it for Postgres to handle.
            new_pg_opts =
                pg_sys::lappend(new_pg_opts, def_elem_ptr as *mut std::ffi::c_void);
        }
    }

    (*stmt).options = new_pg_opts;
    Ok(custom_opts)
}

fn validate_option_value(
    def: &StorageOptionDef,
    raw_val: Option<String>,
) -> Result<Option<String>, String> {
    match &def.kind {
        OptionKind::Bool { default: _ } => {
            // Postgres convention: omitting the value for a boolean option means "true"
            // regardless of the default value defined in our schema.
            let v = raw_val.unwrap_or_else(|| "true".to_string());
            let bool_res = parse_bool(&v)
                .ok_or_else(|| format!("invalid boolean value \"{}\"", v))?;
            Ok(Some(bool_res.to_string()))
        }
        OptionKind::Int {
            default: _,
            min,
            max,
        } => {
            // Int requires explicit value, standard "option" syntax without value is not supported for ints here
            let v = raw_val.ok_or("numeric option requires a value")?;
            let int_val = v
                .parse::<i32>()
                .map_err(|_| format!("invalid integer value \"{}\"", v))?;

            if let Some(min_val) = min {
                if int_val < *min_val {
                    return Err(format!(
                        "value {} is less than minimum {}",
                        int_val, min_val
                    ));
                }
            }
            if let Some(max_val) = max {
                if int_val > *max_val {
                    return Err(format!(
                        "value {} is greater than maximum {}",
                        int_val, max_val
                    ));
                }
            }
            Ok(Some(int_val.to_string()))
        }
        OptionKind::String { default } => {
            if let Some(val) = raw_val {
                Ok(Some(val))
            } else {
                // If user provided the key without value (arg is NULL), use default if present
                Ok(default.map(|s| s.to_string()))
            }
        }
    }
}

fn parse_bool(s: &str) -> Option<bool> {
    let s = s.to_lowercase();
    match s.as_str() {
        "true" | "t" | "yes" | "y" | "on" | "1" => Some(true),
        "false" | "f" | "no" | "n" | "off" | "0" => Some(false),
        _ => None,
    }
}

/// Update pg_tablespace catalog with extracted custom options.
///
/// This function is safe because it only uses PGRX's safe SPI interface
/// and works with pure Rust types (Oid is just a u32, and Vec is managed by Rust).
pub fn update_tablespace_options(
    spcoid: pg_sys::Oid,
    opts: Vec<(String, Option<String>)>,
) {
    let mut set_parts = Vec::new();
    for (k, v) in opts {
        if let Some(val) = v {
            set_parts.push(format!("{}={}", k, val));
        }
    }

    if set_parts.is_empty() {
        return;
    }

    // Escape for Postgres Array Literal:
    // 1. Backslashes must be escaped first (\ -> \\)
    // 2. Double quotes must be escaped (" -> \")
    let array_body = set_parts
        .iter()
        .map(|s| {
            let escaped = s.replace("\\", "\\\\").replace("\"", "\\\"");
            format!("\"{}\"", escaped)
        })
        .collect::<Vec<_>>()
        .join(",");

    let raw_array_literal = format!("{{{}}}", array_body);

    // Escape for SQL String Literal: ' -> ''
    let sql_literal = format!("'{}'", raw_array_literal.replace("'", "''"));

    let query_safe = format!(
        "UPDATE pg_catalog.pg_tablespace SET spcoptions = COALESCE(spcoptions, '{{}}'::text[]) || {}::text[] WHERE oid = {}",
        sql_literal, spcoid
    );

    if let Err(e) = pgrx::spi::Spi::run(&query_safe) {
        error!("Failed to update tablespace options: {}", e);
    }
}
