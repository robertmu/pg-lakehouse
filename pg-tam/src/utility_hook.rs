use crate::utils::ReportableError;
use pgrx::pg_sys;
use pgrx::pg_sys::panic::ErrorReport;
use pgrx::prelude::*;
use std::marker::PhantomData;
use std::sync::{Arc, OnceLock, RwLock};
use thiserror::Error;

/// Error type for utility hooks
#[derive(Error, Debug)]
pub enum UtilityHookError {
    #[error("utility hook error: {0}")]
    Internal(String),
}

impl From<UtilityHookError> for ErrorReport {
    fn from(value: UtilityHookError) -> Self {
        let error_message = format!("{value}");
        ErrorReport::new(PgSqlErrorCode::ERRCODE_INTERNAL_ERROR, error_message, "")
    }
}

/// A safe wrapper around `pg_sys::Node`
pub struct UtilityNode<'a> {
    ptr: *mut pg_sys::Node,
    _marker: PhantomData<&'a mut pg_sys::Node>,
}

impl<'a> UtilityNode<'a> {
    pub unsafe fn new(ptr: *mut pg_sys::Node) -> Self {
        Self {
            ptr,
            _marker: PhantomData,
        }
    }

    pub fn is_a<T>(&self, tag: pg_sys::NodeTag) -> Option<&T> {
        unsafe {
            if (*self.ptr).type_ == tag {
                Some(&*(self.ptr as *const T))
            } else {
                None
            }
        }
    }

    pub fn is_a_mut<T>(&mut self, tag: pg_sys::NodeTag) -> Option<&mut T> {
        unsafe {
            if (*self.ptr).type_ == tag {
                Some(&mut *(self.ptr as *mut T))
            } else {
                None
            }
        }
    }
}

pub trait UtilityHook: Sync + Send {
    fn on_pre(&self, context: &mut UtilityNode) -> Result<(), UtilityHookError>;
    fn on_post(&self, context: &mut UtilityNode);
}

static REGISTRY: RwLock<Vec<(pg_sys::NodeTag, Arc<dyn UtilityHook>)>> =
    RwLock::new(Vec::new());
static PREV_PROCESS_UTILITY: OnceLock<pg_sys::ProcessUtility_hook_type> =
    OnceLock::new();

pub fn register_utility_hook(tag: pg_sys::NodeTag, hook: Box<dyn UtilityHook>) {
    let mut lock = REGISTRY.write().expect(
        "Lock poisoning should be impossible in a single-threaded Postgres backend",
    );
    lock.push((tag, Arc::from(hook)));

    PREV_PROCESS_UTILITY.get_or_init(|| unsafe {
        let prev = pg_sys::ProcessUtility_hook;
        pg_sys::ProcessUtility_hook = Some(process_utility_router);
        prev
    });
}

#[pg_guard]
unsafe extern "C-unwind" fn process_utility_router(
    pstmt: *mut pg_sys::PlannedStmt,
    query_string: *const std::os::raw::c_char,
    read_only_tree: bool,
    context: u32,
    params: *mut pg_sys::ParamListInfoData,
    query_env: *mut pg_sys::QueryEnvironment,
    dest: *mut pg_sys::DestReceiver,
    completion_tag: *mut pg_sys::QueryCompletion,
) {
    let target_node = (*pstmt).utilityStmt;
    let tag = (*target_node).type_;

    // Copy the original statement before on_pre might modify it.
    // copyObjectImpl allocates in CurrentMemoryContext, so PostgreSQL will manage its lifetime.
    let copied_node = pg_sys::copyObjectImpl(target_node as *const std::ffi::c_void)
        as *mut pg_sys::Node;

    let mut safe_node = UtilityNode::new(target_node);
    let mut safe_node_copy = UtilityNode::new(copied_node);

    {
        let registry = REGISTRY.read()
            .expect("Lock poisoning should be impossible in a single-threaded Postgres backend");
        for (reg_tag, hook) in registry.iter() {
            if *reg_tag == tag {
                hook.on_pre(&mut safe_node).report_unwrap();
            }
        }
    }

    match PREV_PROCESS_UTILITY.get() {
        Some(Some(prev)) => {
            prev(
                pstmt,
                query_string,
                read_only_tree,
                context,
                params,
                query_env,
                dest,
                completion_tag,
            );
        }
        _ => {
            pg_sys::standard_ProcessUtility(
                pstmt,
                query_string,
                read_only_tree,
                context,
                params,
                query_env,
                dest,
                completion_tag,
            );
        }
    }

    {
        let registry = REGISTRY.read()
            .expect("Lock poisoning should be impossible in a single-threaded Postgres backend");
        for (reg_tag, hook) in registry.iter() {
            if *reg_tag == tag {
                hook.on_post(&mut safe_node_copy);
            }
        }
    }
}
