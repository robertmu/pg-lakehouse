use pgrx::pg_sys;
use pgrx::PgTryBuilder;
use std::ffi::CStr;
use std::panic::AssertUnwindSafe;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum PgWrapperError {
    #[error("Invalid string (contains null byte): {0}")]
    NulError(#[from] std::ffi::NulError),

    #[error("Postgres error: {0}")]
    PostgresError(String),
}

pub struct PgWrapper;

impl PgWrapper {
    pub fn get_namespace_oid(
        nspname: &CStr,
        missing_ok: bool,
    ) -> Result<pg_sys::Oid, PgWrapperError> {
        let nspname_ptr = nspname.as_ptr();
        unsafe {
            PgTryBuilder::new(move || {
                Ok(pg_sys::get_namespace_oid(nspname_ptr, missing_ok))
            })
            .catch_others(|err| {
                Err(PgWrapperError::PostgresError(format!("{:?}", err)))
            })
            .execute()
        }
    }

    pub fn get_relname_relid(
        relname: &CStr,
        relnamespace: pg_sys::Oid,
    ) -> Result<pg_sys::Oid, PgWrapperError> {
        let relname_ptr = relname.as_ptr();
        unsafe {
            PgTryBuilder::new(move || {
                Ok(pg_sys::get_relname_relid(relname_ptr, relnamespace))
            })
            .catch_others(|err| {
                Err(PgWrapperError::PostgresError(format!("{:?}", err)))
            })
            .execute()
        }
    }

    pub fn range_var_get_relid(
        relation: *const pg_sys::RangeVar,
        lockmode: pg_sys::LOCKMODE,
        missing_ok: bool,
    ) -> Result<pg_sys::Oid, PgWrapperError> {
        let relation = AssertUnwindSafe(relation);
        // RVR_MISSING_OK = 1 << 0 = 1 (from namespace.h RVROption enum)
        const RVR_MISSING_OK: u32 = 1;
        let flags = if missing_ok { RVR_MISSING_OK } else { 0 };
        unsafe {
            PgTryBuilder::new(move || {
                Ok(pg_sys::RangeVarGetRelidExtended(
                    *relation,
                    lockmode,
                    flags,
                    None,
                    std::ptr::null_mut(),
                ))
            })
            .catch_others(|err| {
                Err(PgWrapperError::PostgresError(format!("{:?}", err)))
            })
            .execute()
        }
    }

    pub fn get_tablespace_oid(
        spcname: &CStr,
        missing_ok: bool,
    ) -> Result<pg_sys::Oid, PgWrapperError> {
        let spcname_ptr = spcname.as_ptr();
        unsafe {
            PgTryBuilder::new(move || {
                Ok(pg_sys::get_tablespace_oid(spcname_ptr, missing_ok))
            })
            .catch_others(|err| {
                Err(PgWrapperError::PostgresError(format!("{:?}", err)))
            })
            .execute()
        }
    }

    pub fn table_open(
        relation_id: pg_sys::Oid,
        lockmode: pg_sys::LOCKMODE,
    ) -> Result<pg_sys::Relation, PgWrapperError> {
        unsafe {
            PgTryBuilder::new(move || Ok(pg_sys::table_open(relation_id, lockmode)))
                .catch_others(|err| {
                    Err(PgWrapperError::PostgresError(format!("{:?}", err)))
                })
                .execute()
        }
    }

    pub fn table_close(
        relation: pg_sys::Relation,
        lockmode: pg_sys::LOCKMODE,
    ) -> Result<(), PgWrapperError> {
        let relation = AssertUnwindSafe(relation);
        unsafe {
            PgTryBuilder::new(move || {
                pg_sys::table_close(*relation, lockmode);
                Ok(())
            })
            .catch_others(|err| {
                Err(PgWrapperError::PostgresError(format!("{:?}", err)))
            })
            .execute()
        }
    }

    pub fn catalog_tuple_insert(
        relation: pg_sys::Relation,
        tuple: pg_sys::HeapTuple,
    ) -> Result<(), PgWrapperError> {
        let relation = AssertUnwindSafe(relation);
        let tuple = AssertUnwindSafe(tuple);
        unsafe {
            PgTryBuilder::new(move || {
                pg_sys::CatalogTupleInsert(*relation, *tuple);
                Ok(())
            })
            .catch_others(|err| {
                Err(PgWrapperError::PostgresError(format!("{:?}", err)))
            })
            .execute()
        }
    }

    pub fn catalog_tuple_update(
        relation: pg_sys::Relation,
        otid: *mut pg_sys::ItemPointerData,
        tuple: pg_sys::HeapTuple,
    ) -> Result<(), PgWrapperError> {
        let relation = AssertUnwindSafe(relation);
        let otid = AssertUnwindSafe(otid);
        let tuple = AssertUnwindSafe(tuple);
        unsafe {
            PgTryBuilder::new(move || {
                pg_sys::CatalogTupleUpdate(*relation, *otid, *tuple);
                Ok(())
            })
            .catch_others(|err| {
                Err(PgWrapperError::PostgresError(format!("{:?}", err)))
            })
            .execute()
        }
    }

    pub fn search_sys_cache_copy(
        cache_id: i32,
        key1: pg_sys::Datum,
        key2: pg_sys::Datum,
        key3: pg_sys::Datum,
        key4: pg_sys::Datum,
    ) -> Result<Option<pg_sys::HeapTuple>, PgWrapperError> {
        unsafe {
            PgTryBuilder::new(move || {
                let tuple =
                    pg_sys::SearchSysCacheCopy(cache_id, key1, key2, key3, key4);
                Ok(if tuple.is_null() { None } else { Some(tuple) })
            })
            .catch_others(|err| {
                Err(PgWrapperError::PostgresError(format!("{:?}", err)))
            })
            .execute()
        }
    }

    pub fn sys_cache_get_attr(
        cache_id: i32,
        tuple: pg_sys::HeapTuple,
        attribute_number: i16,
        is_null: *mut bool,
    ) -> Result<pg_sys::Datum, PgWrapperError> {
        let tuple = AssertUnwindSafe(tuple);
        let is_null = AssertUnwindSafe(is_null);
        unsafe {
            PgTryBuilder::new(move || {
                Ok(pg_sys::SysCacheGetAttr(
                    cache_id,
                    *tuple,
                    attribute_number,
                    *is_null,
                ))
            })
            .catch_others(|err| {
                Err(PgWrapperError::PostgresError(format!("{:?}", err)))
            })
            .execute()
        }
    }
}
