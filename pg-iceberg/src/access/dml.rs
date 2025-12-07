use crate::error::{IcebergError, IcebergResult};
use pg_tam::prelude::*;
use pgrx::pg_sys;

pub struct IcebergModify;

impl AmDml<IcebergError> for IcebergModify {
    fn new(_rel: pg_sys::Relation) -> IcebergResult<Self> {
        Ok(IcebergModify)
    }

    fn begin_modify(&mut self) -> IcebergResult<()> {
        Err(IcebergError::NotImplemented("begin_modify"))
    }

    fn end_modify(&mut self) -> IcebergResult<()> {
        Err(IcebergError::NotImplemented("end_modify"))
    }

    fn tuple_insert(
        &mut self,
        _row: &Row,
        _cid: pg_sys::CommandId,
        _options: i32,
        _bistate: Option<&mut BulkInsertStateHandle>,
    ) -> IcebergResult<()> {
        Err(IcebergError::NotImplemented("tuple_insert"))
    }

    fn multi_insert(
        &mut self,
        _rows: &[Row],
        _cid: pg_sys::CommandId,
        _options: i32,
        _bistate: Option<&mut BulkInsertStateHandle>,
    ) -> IcebergResult<()> {
        Err(IcebergError::NotImplemented("multi_insert"))
    }

    fn tuple_delete(
        &mut self,
        _tid: &ItemPointer,
        _cid: pg_sys::CommandId,
        _snapshot: &SnapshotHandle,
        _crosscheck: Option<&SnapshotHandle>,
        _wait: bool,
        _tmfd: &mut TM_FailureData,
        _changing_part: bool,
    ) -> IcebergResult<pg_sys::TM_Result::Type> {
        Err(IcebergError::NotImplemented("tuple_delete"))
    }

    fn tuple_update(
        &mut self,
        _otid: &ItemPointer,
        _row: &Row,
        _cid: pg_sys::CommandId,
        _snapshot: &SnapshotHandle,
        _crosscheck: Option<&SnapshotHandle>,
        _wait: bool,
        _tmfd: &mut TM_FailureData,
        _lockmode: &mut pg_sys::LockTupleMode::Type,
        _update_indexes: &mut pg_sys::TU_UpdateIndexes::Type,
    ) -> IcebergResult<pg_sys::TM_Result::Type> {
        Err(IcebergError::NotImplemented("tuple_update"))
    }

    fn tuple_lock(
        &mut self,
        _tid: &ItemPointer,
        _snapshot: &SnapshotHandle,
        _row: &mut Row,
        _cid: pg_sys::CommandId,
        _mode: pg_sys::LockTupleMode::Type,
        _wait_policy: pg_sys::LockWaitPolicy::Type,
        _flags: u8,
        _tmfd: &mut TM_FailureData,
    ) -> IcebergResult<pg_sys::TM_Result::Type> {
        Err(IcebergError::NotImplemented("tuple_lock"))
    }
}

