use crate::error::{IcebergError, IcebergResult};
use pg_tam::prelude::*;
use pgrx::pg_sys;

pub struct IcebergRelation;

impl AmRelation<IcebergError> for IcebergRelation {
    fn relation_estimate_size(
        _rel: &RelationHandle,
        _attr_widths: Option<&mut [i32]>,
    ) -> IcebergResult<(pg_sys::BlockNumber, f64, f64)> {
        Err(IcebergError::NotImplemented("relation_estimate_size"))
    }

    fn relation_size(
        _rel: &RelationHandle,
        _fork_number: pg_sys::ForkNumber::Type,
    ) -> IcebergResult<u64> {
        Err(IcebergError::NotImplemented("relation_size"))
    }
}
