//! Pending Delete mechanism for custom Table Access Methods.
//!
//! This module implements a mechanism similar to PostgreSQL's `PendingRelDelete`
//! for managing storage cleanup during transaction abort or commit.
//!
//! # Background
//!
//! PostgreSQL's heap tables use `PendingRelDelete` to handle two main scenarios:
//! 1. **Abort Cleanup (CREATE TABLE)**: If a transaction creates physical files but then
//!    fails, those files must be deleted during rollback.
//! 2. **Commit Cleanup (DROP TABLE)**: If a table is dropped, the physical files should
//!    only be deleted *after* the transaction successfully commits.
//!
//! For custom table access methods (like Iceberg), we provide the same mechanism
//! via the `PendingDelete` trait.
//!
//! # Architecture
//!
//! ```text
//! ┌──────────────────────────────────────────────────────────────┐
//! │                Transaction Lifecycle                          │
//! ├──────────────────────────────────────────────────────────────┤
//! │  Operation (e.g. CREATE or DROP)                              │
//! │      │                                                        │
//! │      ▼                                                        │
//! │  1. Perform action & register_pending_delete()                │
//! │      │  (mark at_commit=false for CREATE, true for DROP)      │
//! │      ▼                                                        │
//! ├──────┼────────────────────────────────────────────────────────┤
//! │      │                                                        │
//! │   COMMIT                         ABORT                        │
//! │      │                              │                         │
//! │      ▼                              ▼                         │
//! │  Execute items where            Execute items where           │
//! │  at_commit == true              at_commit == false            │
//! │                                                               │
//! │  (e.g. Delete dropped table)    (e.g. Cleanup failed create)  │
//! └──────────────────────────────────────────────────────────────┘
//! ```

use std::cell::RefCell;
use std::fmt::Debug;

use pgrx::pg_guard;
use pgrx::pg_sys;

/// A pending delete entry that can be executed when a transaction aborts or commits.
///
/// Implementations should clean up any storage resources that were created
/// during the transaction but need to be removed on failure (abort), or
/// resources that were marked for deletion but should only be physically
/// removed after successful completion (commit).
pub trait PendingDelete: Debug + Send {
    /// Execute the delete operation.
    ///
    /// This is called when the transaction reaches the specified state (commit or abort).
    /// Implementations should delete any storage files/directories that were registered.
    ///
    /// Errors during deletion are logged but do not prevent other pending
    /// deletes from being processed.
    fn execute(&self);

    /// Whether this delete should occur at commit time.
    ///
    /// - `true`: Execute on COMMIT (like DROP TABLE cleanup after commit)
    /// - `false`: Execute on ABORT (like CREATE TABLE cleanup on rollback)
    ///
    /// Default is `false` (execute on abort), which is the common case for
    /// cleaning up newly created storage on transaction failure.
    fn at_commit(&self) -> bool {
        false
    }
}

/// Internal entry wrapper to track transaction nesting level.
struct PendingDeleteEntry {
    inner: Box<dyn PendingDelete>,
    /// Transaction nesting level at the time of registration.
    nest_level: i32,
}

// Thread-local storage for pending delete entries.
//
// Using RefCell because PostgreSQL backends are single-threaded,
// and we need interior mutability for the global registry.
thread_local! {
    static PENDING_DELETES: RefCell<Vec<PendingDeleteEntry>> = const { RefCell::new(Vec::new()) };
}

// Track whether the XactCallback has been registered.
thread_local! {
    static XACT_CALLBACK_REGISTERED: RefCell<bool> = const { RefCell::new(false) };
}

/// Register a pending delete entry.
///
/// The entry will be processed at transaction end:
/// - If `entry.at_commit()` is false (default): executed on ABORT
/// - If `entry.at_commit()` is true: executed on COMMIT
///
/// This function captures the current transaction nesting level to correctly
/// handle subtransactions (SAVEPOINT/ROLLBACK TO).
pub fn register_pending_delete(entry: Box<dyn PendingDelete>) {
    let nest_level = unsafe { pg_sys::GetCurrentTransactionNestLevel() };
    PENDING_DELETES.with(|pending| {
        pending.borrow_mut().push(PendingDeleteEntry {
            inner: entry,
            nest_level,
        });
    });
}

/// Get the current number of pending delete entries.
///
/// Useful for debugging and testing.
pub fn pending_delete_size() -> usize {
    PENDING_DELETES.with(|pending| pending.borrow().len())
}

/// Initialize the transaction callbacks.
///
/// This should be called during extension initialization (`_PG_init`).
/// It registers:
/// 1. `XactCallback`: For top-level transaction Commit/Abort.
/// 2. `SubXactCallback`: For subtransaction (SAVEPOINT) Commit/Abort.
///
/// Safe to call multiple times - will only register once.
pub fn init_xact_callback() {
    XACT_CALLBACK_REGISTERED.with(|registered| {
        if *registered.borrow() {
            return;
        }

        unsafe {
            pg_sys::RegisterXactCallback(Some(xact_callback), std::ptr::null_mut());
            pg_sys::RegisterSubXactCallback(
                Some(subxact_callback),
                std::ptr::null_mut(),
            );
        }

        *registered.borrow_mut() = true;
    });
}

/// PostgreSQL transaction callback.
///
/// Handles top-level transaction execution. All pending entries are processed
/// and the list is cleared.
#[pg_guard]
unsafe extern "C-unwind" fn xact_callback(
    event: pg_sys::XactEvent::Type,
    _arg: *mut std::ffi::c_void,
) {
    // Import the event constants
    use pg_sys::XactEvent::*;

    // Determine if this is a commit or abort event
    let (is_commit, is_abort) = match event {
        XACT_EVENT_COMMIT | XACT_EVENT_PARALLEL_COMMIT => (true, false),
        XACT_EVENT_ABORT | XACT_EVENT_PARALLEL_ABORT => (false, true),
        // Pre-commit and pre-prepare events - don't process yet
        XACT_EVENT_PRE_COMMIT
        | XACT_EVENT_PRE_PREPARE
        | XACT_EVENT_PARALLEL_PRE_COMMIT => {
            return;
        }
        // Prepare for 2PC - don't process
        XACT_EVENT_PREPARE => return,
        // Unknown event - ignore
        _ => return,
    };

    let current_nest_level = unsafe { pg_sys::GetCurrentTransactionNestLevel() };

    // Take all pending entries (clears the list)
    let entries =
        PENDING_DELETES.with(|pending| std::mem::take(&mut *pending.borrow_mut()));

    // Process entries based on their at_commit flag
    // Defensive check: only process entries at current nest level or deeper
    // (At top-level commit/abort, all entries should have nest_level == 1)
    for entry in entries {
        // Skip entries from outer levels (defensive - shouldn't happen)
        if entry.nest_level < current_nest_level {
            continue;
        }

        let should_execute = if is_commit {
            entry.inner.at_commit()
        } else if is_abort {
            !entry.inner.at_commit()
        } else {
            false
        };

        if should_execute {
            // Execute the delete, logging any errors but continuing
            entry.inner.execute();
        }
    }
}

/// PostgreSQL subtransaction callback.
///
/// Handles subtransaction events (SAVEPOINT release/rollback).
#[pg_guard]
unsafe extern "C-unwind" fn subxact_callback(
    event: pg_sys::SubXactEvent::Type,
    _my_subid: pg_sys::SubTransactionId,
    _parent_subid: pg_sys::SubTransactionId,
    _arg: *mut std::ffi::c_void,
) {
    use pg_sys::SubXactEvent::*;

    let current_nest_level = unsafe { pg_sys::GetCurrentTransactionNestLevel() };

    PENDING_DELETES.with(|pending| {
        let mut pending = pending.borrow_mut();

        match event {
            // SUB_COMMIT: Successful RELEASE SAVEPOINT
            // "Promote" entries from this level to the parent level.
            SUBXACT_EVENT_COMMIT_SUB => {
                for entry in pending.iter_mut() {
                    if entry.nest_level >= current_nest_level {
                        entry.nest_level = current_nest_level - 1;
                    }
                }
            }
            // SUB_ABORT: ROLLBACK TO SAVEPOINT
            // 1. Execute "delete-on-abort" actions for items created in this subxact.
            // 2. Remove ALL items created in this subxact from the list.
            SUBXACT_EVENT_ABORT_SUB => {
                // We use retain to filter out the items belonging to this subtx logic
                // But we need to execute side-effects first.
                //
                // Note: The logic mirrors smgrDoPendingDeletes(false).
                // We iterate, if item belongs to this level (or deeper):
                //   if !at_commit (i.e. cleanup on abort), Execute it.
                //   ALWAYS remove it from the list.
                pending.retain(|entry| {
                    if entry.nest_level >= current_nest_level {
                        if !entry.inner.at_commit() {
                            entry.inner.execute();
                        }
                        // Return false to remove from list
                        false
                    } else {
                        // Keep parent items
                        true
                    }
                });
            }
            // Ignore other events (START_SUB, PRE_COMMIT_SUB)
            _ => {}
        }
    });
}
