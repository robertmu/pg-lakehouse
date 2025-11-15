use crate::api::TableAccessMethod;
use crate::TableAmRoutine;
use pgrx::pg_sys;
use pgrx::pg_sys::panic::ErrorReport;
use pgrx::AllocatedByRust;

pub fn make_table_am_routine<E, T>() -> TableAmRoutine<AllocatedByRust>
where
    E: Into<ErrorReport>,
    T: TableAccessMethod<E>,
{
    unsafe {
        let mut am_routine = TableAmRoutine::<AllocatedByRust>::alloc_node(
            pg_sys::NodeTag::T_TableAmRoutine,
        );

        crate::scan::register::<E, T::ScanState>(&mut am_routine);
        crate::relation::register::<E, T::RelationState>(&mut am_routine);
        crate::index::register::<E, T::IndexState>(&mut am_routine);
        crate::dml::register::<E, T::ModifyState>(&mut am_routine);
        crate::ddl::register::<E, T::DdlState>(&mut am_routine);

        am_routine
    }
}
