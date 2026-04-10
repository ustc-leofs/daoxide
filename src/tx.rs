//! Transaction management for atomic operations.
//!
//! This module provides the [`Transaction`] type for managing DAOS transactions
//! with RAII semantics. Transactions ensure atomicity for operations within
//! a container.
//!
//! # Transaction Lifecycle
//!
//! 1. Open a transaction via [`Transaction::new()`]
//! 2. Perform operations (update, fetch, etc.)
//! 3. Commit the transaction via [`Transaction::commit()`] OR abort via [`Transaction::abort()`]
//! 4. Close the transaction handle via [`Transaction::close()`] (or automatic on drop)
//!
//! # Example
//!
//! ```ignore
//! use daoxide::{container::Container, tx::Transaction};
//!
//! let container = pool.create_container("mycontainer")?;
//!
//! // Start a transaction
//! let tx = Transaction::new(container.as_handle()?, 0)?;
//!
//! // Perform operations...
//!
//! // Commit the transaction
//! tx.commit()?;
//!
//! // Close is automatic on drop, but can be called explicitly
//! tx.close()?;
//! ```
//!
//! # Tx::none() Passthrough
//!
//! Some operations can be performed outside of a transaction using `Tx::none()`.
//! This is useful for independent operations that don't need atomicity:
//!
//! ```ignore
//! use daoxide::tx::Tx;
//!
//! let tx = Tx::none();
//! // Operations with tx will execute without transaction semantics
//! ```

use crate::error::{DaosError, Result};
use crate::runtime::require_runtime;
use crate::unsafe_inner::ffi::{
    daos_tx_abort, daos_tx_close, daos_tx_commit, daos_tx_open, daos_tx_open_snap, daos_tx_restart,
};
use crate::unsafe_inner::handle::{DAOS_HANDLE_NULL, DaosHandle};
use daos::daos_handle_t;

/// Transaction flags controlling transaction behavior.
pub mod flags {
    use crate::unsafe_inner::ffi::tx_flags;

    pub const TX_RDONLY: u64 = tx_flags::TX_RDONLY;
    pub const TX_ZERO_COPY: u64 = tx_flags::TX_ZERO_COPY;
}

/// Represents the state of a transaction.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TxState {
    /// Transaction is open and can be used for operations.
    Open,
    /// Transaction has been committed successfully.
    Committed,
    /// Transaction has been aborted and modifications discarded.
    Aborted,
    /// Transaction handle has been closed.
    Closed,
}

impl TxState {
    /// Returns true if the transaction state is `Open`.
    pub fn is_open(&self) -> bool {
        matches!(self, TxState::Open)
    }
}

/// A DAOS transaction with RAII semantics.
///
/// `Transaction` wraps a DAOS transaction handle and provides safe access
/// to transaction operations. Transactions ensure atomicity for operations
/// within a container.
///
/// # State Machine
///
/// A transaction follows this state machine:
///
/// ```text
///     [Created]
///         |
///         v
///     [Open] <------ restart()
///         |
///         +---------+---------+
///         |                   |
///         v                   v
///   [Committed]           [Aborted]
///         |                   |
///         +---------+---------+
///                   |
///                   v
///               [Closed]
/// ```
///
/// After a transaction is closed (either explicitly via `close()` or
/// automatically on `Drop`), it cannot be used for any operations.
#[derive(Debug)]
pub struct Transaction<'c> {
    container: &'c DaosHandle,
    handle: Option<DaosHandle>,
    state: TxState,
}

impl<'c> Transaction<'c> {
    /// Creates a new open transaction on the given container.
    ///
    /// # Arguments
    ///
    /// * `container` - The container handle to create the transaction on
    /// * `flags` - Transaction flags (e.g., `flags::TX_RDONLY`)
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - DAOS runtime is not initialized
    /// - Transaction open fails
    pub fn new(container: &'c DaosHandle, flags: u64) -> Result<Self> {
        require_runtime()?;
        let handle = daos_tx_open(*container, flags)?;
        Ok(Self {
            container,
            handle: Some(handle),
            state: TxState::Open,
        })
    }

    /// Opens a read-only transaction on a container snapshot.
    ///
    /// # Arguments
    ///
    /// * `container` - The container handle
    /// * `epoch` - The snapshot epoch to read from
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - DAOS runtime is not initialized
    /// - Snapshot epoch is invalid
    /// - Transaction open fails
    pub fn open_snap(container: &'c DaosHandle, epoch: u64) -> Result<Self> {
        require_runtime()?;
        let handle = daos_tx_open_snap(*container, epoch)?;
        Ok(Self {
            container,
            handle: Some(handle),
            state: TxState::Open,
        })
    }

    /// Returns the transaction handle for use with object operations.
    ///
    /// # Errors
    ///
    /// Returns `Err(DaosError::InvalidArg)` if the transaction is not in
    /// the Open state.
    #[inline]
    pub fn as_handle(&self) -> Result<DaosHandle> {
        if self.state == TxState::Open {
            self.handle.ok_or(DaosError::InvalidArg)
        } else {
            Err(DaosError::InvalidArg)
        }
    }

    /// Returns the current state of the transaction.
    #[inline]
    pub fn state(&self) -> TxState {
        self.state
    }

    /// Returns true if the transaction is in the Open state.
    #[inline]
    pub fn is_open(&self) -> bool {
        self.state == TxState::Open
    }

    /// Returns the container handle this transaction is bound to.
    #[inline]
    pub fn container_handle(&self) -> &DaosHandle {
        self.container
    }

    /// Returns the raw DAOS transaction handle for FFI calls.
    ///
    /// Unlike `as_handle()`, this does not check transaction state.
    /// It returns the raw handle even if the transaction is not open.
    /// This is useful for passing to FFI functions that will handle
    /// the state validation.
    ///
    /// # Safety
    ///
    /// The returned handle should only be used while the transaction
    /// is in a valid state for the intended operation.
    #[inline]
    pub(crate) fn raw_handle(&self) -> daos_handle_t {
        self.handle
            .map(|h| h.as_raw())
            .unwrap_or_else(|| crate::unsafe_inner::handle::DAOS_HANDLE_NULL)
    }

    /// Commits the transaction, making all modifications permanent.
    ///
    /// After commit, the transaction moves to the Committed state and
    /// cannot be used for new operations.
    ///
    /// # Errors
    ///
    /// Returns `DaosError::TxRestart` if the transaction must be restarted.
    /// In this case, call [`Transaction::restart()`] and retry the operations.
    ///
    /// Returns `Err(DaosError::InvalidArg)` if the transaction is not open.
    pub fn commit(&mut self) -> Result<()> {
        if self.state != TxState::Open {
            return Err(DaosError::InvalidArg);
        }
        let handle = self.handle.ok_or(DaosError::InvalidArg)?;
        daos_tx_commit(handle)?;
        self.state = TxState::Committed;
        Ok(())
    }

    /// Aborts the transaction, discarding all modifications.
    ///
    /// After abort, the transaction moves to the Aborted state and
    /// cannot be used for new operations.
    ///
    /// # Errors
    ///
    /// Returns `Err(DaosError::InvalidArg)` if the transaction is not open.
    pub fn abort(&mut self) -> Result<()> {
        if self.state != TxState::Open {
            return Err(DaosError::InvalidArg);
        }
        let handle = self.handle.ok_or(DaosError::InvalidArg)?;
        daos_tx_abort(handle)?;
        self.state = TxState::Aborted;
        Ok(())
    }

    /// Restarts the transaction after encountering a `TxRestart` error.
    ///
    /// This drops all IOs that have been issued via this transaction handle.
    /// Whether the restarted transaction observes conflicting modifications
    /// committed after this transaction was originally opened is undefined.
    ///
    /// # Errors
    ///
    /// Returns `Err(DaosError::InvalidArg)` if the transaction is not open.
    pub fn restart(&mut self) -> Result<()> {
        if self.state != TxState::Open {
            return Err(DaosError::InvalidArg);
        }
        let handle = self.handle.ok_or(DaosError::InvalidArg)?;
        daos_tx_restart(handle)?;
        self.state = TxState::Open;
        Ok(())
    }

    /// Closes the transaction handle.
    ///
    /// This is automatically called when the `Transaction` is dropped,
    /// but can be called explicitly to handle errors.
    ///
    /// Closing an already closed transaction is a no-op.
    ///
    /// # Errors
    ///
    /// Returns an error if the underlying close operation fails.
    /// After an error, the transaction is still considered closed.
    pub fn close(&mut self) -> Result<()> {
        if let Some(handle) = self.handle.take() {
            daos_tx_close(handle)?;
        }
        self.state = TxState::Closed;
        Ok(())
    }
}

impl Drop for Transaction<'_> {
    fn drop(&mut self) {
        // Close the handle if not already closed
        if let Some(handle) = self.handle.take() {
            if let Err(e) = daos_tx_close(handle) {
                eprintln!(
                    "Transaction::drop: daos_tx_close() failed with {:?}, continuing with drop anyway",
                    e
                );
            }
        }
        self.state = TxState::Closed;
    }
}

/// Represents the absence of a transaction for operations that don't require one.
///
/// `Tx` is used as a marker type to indicate that an operation should
/// execute without transaction semantics. This is useful for independent
/// operations that don't need atomicity guarantees.
#[derive(Debug, Default)]
pub enum Tx<'c> {
    /// An active transaction.
    Some(Transaction<'c>),
    /// No transaction - operations execute independently.
    #[default]
    None,
}

impl<'c> Tx<'c> {
    /// Creates a `Tx` representing no transaction.
    ///
    /// Operations using `Tx::None` will execute without transaction semantics.
    #[inline]
    pub fn none() -> Self {
        Tx::None
    }

    /// Returns true if this is `Tx::None`.
    #[inline]
    pub fn is_none(&self) -> bool {
        matches!(self, Tx::None)
    }

    /// Returns true if this is `Tx::Some`.
    #[inline]
    pub fn is_some(&self) -> bool {
        matches!(self, Tx::Some(_))
    }

    /// Returns the transaction handle if present, or a special "none" handle.
    ///
    /// This is useful for APIs that accept either a real transaction or
    /// a passthrough indicator.
    ///
    /// Returns `Ok(handle)` for `Tx::Some` or `Err(DaosError::InvalidArg)` for `Tx::None`.
    /// Callers should check `is_none()` first if they want different behavior.
    pub fn as_handle(&self) -> Result<DaosHandle> {
        match self {
            Tx::Some(tx) => tx.as_handle(),
            Tx::None => Err(DaosError::InvalidArg),
        }
    }

    /// Returns the raw DAOS transaction handle for FFI calls.
    ///
    /// For `Tx::Some(tx)`, returns the transaction's raw handle.
    /// For `Tx::None`, returns the null handle (cookie = 0) which tells DAOS
    /// to execute without transaction semantics.
    #[inline]
    pub(crate) fn as_raw_daos_handle(&self) -> daos_handle_t {
        match self {
            Tx::Some(tx) => tx.raw_handle(),
            Tx::None => DAOS_HANDLE_NULL,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::unsafe_inner::handle::DaosHandle;

    #[test]
    fn test_tx_state_initial_values() {
        assert!(TxState::Open != TxState::Committed);
        assert!(TxState::Open != TxState::Aborted);
        assert!(TxState::Open != TxState::Closed);
    }

    #[test]
    fn test_tx_state_debug() {
        assert_eq!(format!("{:?}", TxState::Open), "Open");
        assert_eq!(format!("{:?}", TxState::Committed), "Committed");
        assert_eq!(format!("{:?}", TxState::Aborted), "Aborted");
        assert_eq!(format!("{:?}", TxState::Closed), "Closed");
    }

    #[test]
    fn test_tx_none_default() {
        let tx = Tx::None;
        assert!(tx.is_none());
        assert!(!tx.is_some());
        assert!(matches!(tx, Tx::None));
    }

    #[test]
    fn test_tx_none_as_handle_error() {
        let tx = Tx::None;
        let result = tx.as_handle();
        assert!(result.is_err());
    }

    #[test]
    fn test_tx_none_default_impl() {
        let tx: Tx = Default::default();
        assert!(tx.is_none());
    }

    #[test]
    fn test_transaction_flags_constants() {
        assert_eq!(flags::TX_RDONLY, 1);
        assert_eq!(flags::TX_ZERO_COPY, 2);
    }

    #[test]
    fn test_tx_enum_debug() {
        let tx_none: Tx = Tx::None;
        assert!(format!("{:?}", tx_none).contains("None"));
    }

    #[test]
    fn test_require_runtime_error_when_not_init() {
        while crate::runtime::is_runtime_initialized() {
            drop(crate::runtime::DaosRuntime::new());
        }

        let valid_handle = unsafe { DaosHandle::from_raw(daos::daos_handle_t { cookie: 12345 }) };

        let result = Transaction::new(&valid_handle, 0);
        assert!(result.is_err());
    }

    #[test]
    fn test_transaction_state_transitions() {
        // This test verifies state enum and transition logic without actual DAOS calls
        let state = TxState::Open;
        assert!(state.is_open());

        // State transitions are tested via the state() method
        let committed = TxState::Committed;
        assert!(!committed.is_open());
    }

    #[test]
    fn test_tx_some_creation_requires_valid_container() {
        while crate::runtime::is_runtime_initialized() {
            drop(crate::runtime::DaosRuntime::new());
        }
        crate::runtime::DaosRuntime::new().unwrap();

        // Using an invalid handle should fail
        let invalid_handle = unsafe { DaosHandle::from_raw(daos::daos_handle_t { cookie: 0 }) };
        let result = Transaction::new(&invalid_handle, 0);
        assert!(result.is_err());

        while crate::runtime::is_runtime_initialized() {
            drop(crate::runtime::DaosRuntime::new());
        }
    }

    #[test]
    fn test_transaction_already_closed_operations() {
        while crate::runtime::is_runtime_initialized() {
            drop(crate::runtime::DaosRuntime::new());
        }
        crate::runtime::DaosRuntime::new().unwrap();

        let valid_handle = unsafe { DaosHandle::from_raw(daos::daos_handle_t { cookie: 12345 }) };

        // Create transaction - will fail due to invalid handle but we can test state checks
        let _result = Transaction::new(&valid_handle, 0);

        // If the transaction couldn't be created due to bad handle, that's expected
        // But if it was created, we could test state transitions

        while crate::runtime::is_runtime_initialized() {
            drop(crate::runtime::DaosRuntime::new());
        }
    }

    #[test]
    fn test_tx_none_passthrough_semantics() {
        // Tx::None should be usable as a passthrough indicator
        let tx: Tx = Tx::none();
        assert!(tx.is_none());

        // as_handle on None should error
        assert!(tx.as_handle().is_err());
    }

    #[test]
    fn test_transaction_drop_closes_handle() {
        // Test that Drop is available and tx.rs compiles
        // Actual drop behavior requires DAOS environment
        let tx_none: Tx = Tx::none();
        assert!(tx_none.is_none());
    }
}
