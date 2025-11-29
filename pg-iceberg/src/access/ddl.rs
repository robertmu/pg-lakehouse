use crate::error::{IcebergError, IcebergResult};
use pg_tam::prelude::*;
use pgrx::pg_sys;

pub struct IcebergDdl;

impl AmDdl<IcebergError> for IcebergDdl {
    fn relation_set_new_filelocator(
        _rel: &RelationHandle,
        _newrlocator: &RelFileLocator,
        _persistence: u8,
    ) -> IcebergResult<(pg_sys::TransactionId, pg_sys::MultiXactId)> {
        Err(IcebergError::NotImplemented("relation_set_new_filelocator"))
    }

    fn relation_nontransactional_truncate(_rel: &RelationHandle) -> IcebergResult<()> {
        Err(IcebergError::NotImplemented(
            "relation_nontransactional_truncate",
        ))
    }

    fn relation_copy_data(
        _rel: &RelationHandle,
        _newrlocator: &RelFileLocator,
    ) -> IcebergResult<()> {
        Err(IcebergError::NotImplemented("relation_copy_data"))
    }

    fn relation_copy_for_cluster(
        _old_table: &RelationHandle,
        _new_table: &RelationHandle,
        _old_index: &RelationHandle,
        _use_sort: bool,
        _oldest_xmin: pg_sys::TransactionId,
    ) -> IcebergResult<(pg_sys::TransactionId, pg_sys::MultiXactId, f64, f64, f64)> {
        Err(IcebergError::NotImplemented("relation_copy_for_cluster"))
    }
}
