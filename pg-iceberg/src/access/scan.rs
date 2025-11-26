use pg_tam::prelude::*;
use crate::error::{IcebergError, IcebergResult};

pub struct IcebergScan;

impl AmScan<IcebergError> for IcebergScan {
    fn new(
        _rel: &RelationHandle,
        _snapshot: &SnapshotHandle,
        _key: Option<&ScanKeyHandle>,
        _pscan: Option<&ParallelTableScanDescHandle>,
        _flags: u32,
    ) -> IcebergResult<Self> {
        // Initialize scan state
        Ok(IcebergScan)
    }

    fn scan_begin(&mut self) -> IcebergResult<()> {
        Err(IcebergError::NotImplemented("scan_begin"))
    }

    fn scan_getnextslot(
        &mut self,
        _direction: ScanDirection,
        _row: &mut Row,
    ) -> IcebergResult<bool> {
        Err(IcebergError::NotImplemented("scan_getnextslot"))
    }

    fn scan_rescan(
        &mut self,
        _key: Option<&ScanKeyHandle>,
        _set_params: bool,
        _allow_strat: bool,
        _allow_sync: bool,
        _allow_pagemode: bool,
    ) -> IcebergResult<()> {
        Err(IcebergError::NotImplemented("scan_rescan"))
    }

    fn scan_end(&mut self) -> IcebergResult<()> {
        Err(IcebergError::NotImplemented("scan_end"))
    }

    fn scan_bitmap_next_block(
        &mut self,
        _tbmres: &TBMIterateResultHandle,
    ) -> IcebergResult<bool> {
        Err(IcebergError::NotImplemented("scan_bitmap_next_block"))
    }

    fn scan_bitmap_next_tuple(
        &mut self,
        _tbmres: &TBMIterateResultHandle,
        _row: &mut Row,
    ) -> IcebergResult<bool> {
        Err(IcebergError::NotImplemented("scan_bitmap_next_tuple"))
    }
}
