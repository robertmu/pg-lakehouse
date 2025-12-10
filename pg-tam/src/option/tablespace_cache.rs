use crate::option::storage;
use crate::pg_wrapper::{CacheRegisterSyscacheCallback, PgWrapper, PgWrapperError};
use pgrx::pg_sys;
use pgrx::prelude::*;
use std::cell::RefCell;
use std::collections::HashMap;
use std::rc::Rc;
use std::sync::Once;
use thiserror::Error;

// ============================================================================
//  Error Type
// ============================================================================

/// Errors that can occur when looking up tablespace options.
#[derive(Error, Debug)]
pub enum TablespaceCacheError {
    #[error("failed to lookup tablespace: {0}")]
    LookupFailed(#[from] PgWrapperError),
}

// ============================================================================
//  Protocol-specific Option Structs
// ============================================================================

/// S3-compatible storage options
#[derive(Debug, Clone)]
pub struct S3Options {
    pub bucket: Option<String>,
    pub region: String,
    pub endpoint: Option<String>,
    pub allow_http: bool,
    pub access_key_id: Option<String>,
    pub secret_access_key: Option<String>,
}

impl Default for S3Options {
    fn default() -> Self {
        Self {
            bucket: None,
            region: "us-east-1".to_string(),
            endpoint: None,
            allow_http: false,
            access_key_id: None,
            secret_access_key: None,
        }
    }
}

/// Google Cloud Storage options
#[derive(Debug, Clone)]
pub struct GcsOptions {
    pub bucket: Option<String>,
    pub project_id: Option<String>,
    // TODO: Add more GCS-specific options as needed
}

impl Default for GcsOptions {
    fn default() -> Self {
        Self {
            bucket: None,
            project_id: None,
        }
    }
}

/// Azure Blob Storage options
#[derive(Debug, Clone)]
pub struct AzureOptions {
    pub container: Option<String>,
    pub account_name: Option<String>,
    // TODO: Add more Azure-specific options as needed
}

impl Default for AzureOptions {
    fn default() -> Self {
        Self {
            container: None,
            account_name: None,
        }
    }
}

/// Local filesystem storage options
#[derive(Debug, Clone)]
pub struct FileSystemOptions {
    pub path: Option<String>,
    pub io_concurrency: i32,
}

impl Default for FileSystemOptions {
    fn default() -> Self {
        Self {
            path: None,
            io_concurrency: 4,
        }
    }
}

// ============================================================================
//  StorageOptions - Tagged Union (Rust enum)
// ============================================================================

/// Storage options as a tagged union, similar to C union but type-safe.
#[derive(Debug, Clone)]
pub enum StorageOptions {
    S3(S3Options),
    Gcs(GcsOptions),
    Azure(AzureOptions),
    FileSystem(FileSystemOptions),
}

impl Default for StorageOptions {
    fn default() -> Self {
        StorageOptions::S3(S3Options::default())
    }
}

// ============================================================================
//  CachedTableSpaceOpts
// ============================================================================

/// Structure to hold cached tablespace options.
#[derive(Debug, Clone)]
pub struct CachedTableSpaceOpts {
    pub storage: StorageOptions,
}

impl Default for CachedTableSpaceOpts {
    fn default() -> Self {
        Self {
            storage: StorageOptions::default(),
        }
    }
}

// ============================================================================
//  Cache Infrastructure
// ============================================================================
thread_local! {
    static TABLESPACE_CACHE: RefCell<HashMap<pg_sys::Oid, Option<Rc<CachedTableSpaceOpts>>>> =
        RefCell::new(HashMap::new());
}

static INIT: Once = Once::new();

/// Invalidate callback function called by Postgres
unsafe extern "C" fn invalidate_tablespace_cache_callback(
    _arg: pg_sys::Datum,
    _cacheid: std::os::raw::c_int,
    _hashvalue: u32,
) {
    // Clear the thread-local cache
    TABLESPACE_CACHE.with(|cache| {
        cache.borrow_mut().clear();
    });
}

/// Initialize the cache and register the callback
fn initialize_tablespace_cache() {
    INIT.call_once(|| unsafe {
        CacheRegisterSyscacheCallback(
            pg_sys::SysCacheIdentifier::TABLESPACEOID as i32,
            Some(invalidate_tablespace_cache_callback),
            0.into(),
        );
    });
}

// ============================================================================
//  Public API
// ============================================================================

/// Get tablespace options for a given OID.
///
/// Returns an `Rc` to avoid cloning the entire struct on each access.
/// Since PostgreSQL backend is single-threaded, we use `Rc` instead of `Arc`
/// to avoid atomic operation overhead.
///
/// # Errors
///
/// Returns an error if the tablespace lookup fails due to a Postgres error.
pub fn get_tablespace(
    spcid: pg_sys::Oid,
) -> Result<Option<Rc<CachedTableSpaceOpts>>, TablespaceCacheError> {
    initialize_tablespace_cache();

    let spcid = if spcid == pg_sys::InvalidOid {
        unsafe { pg_sys::MyDatabaseTableSpace }
    } else {
        spcid
    };

    TABLESPACE_CACHE.with(|cache| {
        // Fast path: check cache
        if let Some(entry) = cache.borrow().get(&spcid) {
            return Ok(entry.clone());
        }

        // Slow path: lookup and update cache
        let opts: Option<Rc<CachedTableSpaceOpts>> =
            lookup_tablespace_options(spcid)?.map(Rc::new);
        cache.borrow_mut().insert(spcid, opts.clone());
        Ok(opts)
    })
}

// ============================================================================
//  Internal Parsing
// ============================================================================

fn lookup_tablespace_options(
    spcid: pg_sys::Oid,
) -> Result<Option<CachedTableSpaceOpts>, TablespaceCacheError> {
    let cache_id = pg_sys::SysCacheIdentifier::TABLESPACEOID as i32;

    let tp = PgWrapper::search_sys_cache1(
        cache_id,
        spcid
            .into_datum()
            .expect("Failed to convert spcid Oid to Datum"),
    )?;

    let Some(tp) = tp else {
        return Ok(None);
    };

    let mut is_null = false;
    let datum = PgWrapper::sys_cache_get_attr(
        cache_id,
        tp,
        pg_sys::Anum_pg_tablespace_spcoptions as i16,
        &mut is_null,
    )?;

    let result = if is_null {
        None
    } else {
        // SAFETY: We must copy the data from the Datum (which points into Postgres memory)
        // into owned Rust Strings BEFORE we release the SysCache tuple.
        // Vec::from_datum performs this copy.
        // The datum is valid because we obtained it from SysCacheGetAttr on a valid tuple.
        let options_vec =
            unsafe { Vec::<String>::from_datum(datum, false) }.unwrap_or_default();
        if options_vec.is_empty() {
            None
        } else {
            Some(parse_options_to_cached(options_vec))
        }
    };

    // Release the syscache tuple. If this fails, PostgreSQL's resource owner
    // will clean it up at transaction end anyway.
    let _ = PgWrapper::release_sys_cache(tp);

    Ok(result)
}

/// Parse options vector and build CachedTableSpaceOpts with the appropriate StorageOptions variant
fn parse_options_to_cached(options_vec: Vec<String>) -> CachedTableSpaceOpts {
    // First pass: extract protocol to determine which variant to use
    let mut protocol = "s3".to_string();
    let mut opts_map: HashMap<&str, String> = HashMap::new();

    for opt_str in &options_vec {
        if let Some((key, value)) = opt_str.split_once('=') {
            if key == storage::OPT_PROTOCOL {
                protocol = value.to_string();
            }
            opts_map.insert(key, value.to_string());
        }
    }

    // Build the appropriate StorageOptions variant based on protocol
    let storage = match protocol.as_str() {
        "s3" => StorageOptions::S3(parse_s3_options(&opts_map)),
        "gcs" => StorageOptions::Gcs(parse_gcs_options(&opts_map)),
        "azure" => StorageOptions::Azure(parse_azure_options(&opts_map)),
        "fs" | "file" | "filesystem" => {
            StorageOptions::FileSystem(parse_fs_options(&opts_map))
        }
        unknown => panic!("unexpected storage protocol: {}", unknown),
    };

    CachedTableSpaceOpts { storage }
}

fn parse_s3_options(opts: &HashMap<&str, String>) -> S3Options {
    S3Options {
        bucket: opts.get(storage::OPT_BUCKET).cloned(),
        region: opts
            .get(storage::OPT_REGION)
            .cloned()
            .unwrap_or_else(|| "us-east-1".to_string()),
        endpoint: opts.get(storage::OPT_ENDPOINT).cloned(),
        allow_http: opts
            .get(storage::OPT_ALLOW_HTTP)
            .and_then(|v| storage::parse_bool(v))
            .unwrap_or(false),
        access_key_id: opts.get(storage::OPT_ACCESS_KEY_ID).cloned(),
        secret_access_key: opts.get(storage::OPT_SECRET_ACCESS_KEY).cloned(),
    }
}

fn parse_gcs_options(opts: &HashMap<&str, String>) -> GcsOptions {
    GcsOptions {
        bucket: opts.get(storage::OPT_BUCKET).cloned(),
        project_id: None, // TODO: Add constant when GCS options are defined
    }
}

fn parse_azure_options(_opts: &HashMap<&str, String>) -> AzureOptions {
    AzureOptions {
        container: None, // TODO: Add constant when Azure options are defined
        account_name: None,
    }
}

fn parse_fs_options(opts: &HashMap<&str, String>) -> FileSystemOptions {
    FileSystemOptions {
        path: None, // TODO: Add path option constant
        io_concurrency: opts
            .get(storage::OPT_IO_CONCURRENCY)
            .and_then(|v| v.parse::<i32>().ok())
            .unwrap_or(4),
    }
}
