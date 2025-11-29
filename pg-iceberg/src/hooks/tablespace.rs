use crate::error::IcebergError;
use pg_tam::prelude::*;
use pgrx::pg_sys;

struct IcebergTablespaceHook;

impl UtilityHook for IcebergTablespaceHook {
    fn on_pre(&self, context: &mut UtilityNode) -> Result<(), UtilityHookError> {
        let stmt = context
            .is_a_mut::<pg_sys::CreateTableSpaceStmt>(pg_sys::NodeTag::T_CreateTableSpaceStmt)
            .expect("Hook registered for T_CreateTableSpaceStmt but received different node type");

        if let Err(e) = TablespaceOptions::extract_from_stmt::<IcebergError>(stmt) {
            return Err(UtilityHookError::Internal(e.to_string()));
        }
        Ok(())
    }

    fn on_post(&self, context: &mut UtilityNode) {
        let stmt = context
            .is_a_mut::<pg_sys::CreateTableSpaceStmt>(pg_sys::NodeTag::T_CreateTableSpaceStmt)
            .expect("Hook registered for T_CreateTableSpaceStmt but received different node type");

        if let Ok(Some(opts)) = TablespaceOptions::extract_from_stmt::<IcebergError>(stmt) {
            let spcname_ptr = stmt.tablespacename;
            let oid = unsafe { pg_sys::get_tablespace_oid(spcname_ptr, false) };
            opts.persist_to_catalog(oid);
        }
    }
}

pub fn init_hook() {
    register_utility_hook(
        pg_sys::NodeTag::T_CreateTableSpaceStmt,
        Box::new(IcebergTablespaceHook),
    );
}
