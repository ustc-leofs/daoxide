//! DAOS runtime and event management.
//!
//! This module provides the [`DaosRuntime`] type for initializing and
//! managing the DAOS runtime. The runtime must be initialized before
//! any pool, container, or object operations.
//!
//! # Runtime Lifecycle
//!
//! ```ignore
//! use daoxide::runtime::DaosRuntime;
//!
//! // Initialize DAOS runtime
//! let runtime = DaosRuntime::new()?;
//!
//! // Runtime is now active; pool/container/object operations available
//!
//! // Drop runtime when done
//! drop(runtime);
//! ```
//!
//! # Safety
//!
//! The runtime uses reference counting internally, allowing multiple
//! `DaosRuntime` instances to coexist. DAOS is only finalized when
//! the last instance is dropped.

use crate::error::{DaosError, Result};
use crate::unsafe_inner::ffi::{daos_fini as ffi_daos_fini, daos_init as ffi_daos_init};
use std::sync::{Mutex, OnceLock};

static RUNTIME_MUTEX: Mutex<()> = Mutex::new(());
static RUNTIME_REFCOUNT: OnceLock<Mutex<usize>> = OnceLock::new();

#[inline]
fn runtime_refcount() -> &'static Mutex<usize> {
    RUNTIME_REFCOUNT.get_or_init(|| Mutex::new(0))
}

/// Checks if the DAOS runtime is currently initialized.
pub fn is_runtime_initialized() -> bool {
    let _guard = RUNTIME_MUTEX.lock().unwrap();
    *runtime_refcount().lock().unwrap() > 0
}

/// DAOS runtime handle with RAII semantics.
///
/// `DaosRuntime` initializes the DAOS library on creation and finalizes
/// it when dropped. The runtime uses reference counting, so multiple
/// instances can coexist safely.
///
/// # Example
///
/// ```
/// use daoxide::runtime::DaosRuntime;
///
/// let runtime = DaosRuntime::new().expect("failed to initialize DAOS");
/// assert!(runtime.is_initialized());
/// ```
#[derive(Debug)]
pub struct DaosRuntime {
    _private: (),
}

impl DaosRuntime {
    /// Creates a new DAOS runtime, initializing DAOS if not already initialized.
    ///
    /// # Errors
    ///
    /// Returns an error if DAOS initialization fails.
    pub fn new() -> Result<Self> {
        let _guard = RUNTIME_MUTEX.lock().unwrap();
        let mut refcount = runtime_refcount().lock().unwrap();
        if *refcount == 0 {
            ffi_daos_init()?;
        }
        *refcount += 1;
        Ok(Self { _private: () })
    }

    /// Alias for [`DaosRuntime::new`].
    pub fn try_new() -> Result<Self> {
        Self::new()
    }

    /// Returns whether the DAOS runtime is currently initialized.
    pub fn is_initialized(&self) -> bool {
        is_runtime_initialized()
    }
}

impl Default for DaosRuntime {
    fn default() -> Self {
        Self::new().expect("DaosRuntime::default(): failed to initialize DAOS")
    }
}

impl Drop for DaosRuntime {
    fn drop(&mut self) {
        let _guard = RUNTIME_MUTEX.lock().unwrap();
        let mut refcount = runtime_refcount().lock().unwrap();
        if *refcount == 0 {
            return;
        }
        *refcount -= 1;
        if *refcount == 0 {
            if let Err(e) = ffi_daos_fini() {
                eprintln!(
                    "DaosRuntime::drop: daos_fini() failed with {:?}, \
                     continuing with drop anyway",
                    e
                );
            }
        }
    }
}

/// Error message for when runtime is not initialized.
pub const RUNTIME_NOT_INIT_ERROR: &str =
    "DAOS runtime not initialized. Create a DaosRuntime instance first.";

/// Validates that the DAOS runtime is initialized.
///
/// This function is called by pool, container, and object operations
/// to ensure the runtime is available before attempting FFI calls.
///
/// # Errors
///
/// Returns `Err(DaosError::InvalidArg)` if the runtime is not initialized.
#[inline]
pub fn require_runtime() -> Result<()> {
    let _guard = RUNTIME_MUTEX.lock().unwrap();
    if *runtime_refcount().lock().unwrap() > 0 {
        Ok(())
    } else {
        Err(DaosError::InvalidArg)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::thread;

    #[test]
    fn test_runtime_init_and_fini() {
        let runtime = DaosRuntime::new().expect("failed to create DaosRuntime");
        assert!(runtime.is_initialized());
        assert!(is_runtime_initialized());
        drop(runtime);
        assert!(!is_runtime_initialized());
    }

    #[test]
    fn test_runtime_default() {
        let runtime = DaosRuntime::default();
        assert!(runtime.is_initialized());
    }

    #[test]
    fn test_repeated_init() {
        // Ensure clean state: drain any leftover runtimes from prior tests
        while is_runtime_initialized() {
            drop(DaosRuntime::new());
        }

        let runtime1 = DaosRuntime::new().expect("failed to create first DaosRuntime");
        assert!(runtime1.is_initialized());

        let runtime2 = DaosRuntime::new().expect("failed to create second DaosRuntime");
        assert!(runtime2.is_initialized());

        drop(runtime2);
        assert!(runtime1.is_initialized());

        drop(runtime1);
        assert!(!is_runtime_initialized());
    }

    #[test]
    fn test_require_runtime_when_not_init() {
        while is_runtime_initialized() {
            drop(DaosRuntime::new());
        }
        assert!(!is_runtime_initialized());

        let result = require_runtime();
        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), DaosError::InvalidArg));
    }

    #[test]
    fn test_require_runtime_when_init() {
        let runtime = DaosRuntime::new().expect("failed to create DaosRuntime");
        assert!(runtime.is_initialized());

        let result = require_runtime();
        assert!(result.is_ok());
    }

    #[test]
    fn test_threaded_runtime() {
        let runtime = DaosRuntime::new().expect("failed to create DaosRuntime");
        assert!(runtime.is_initialized());

        let handle = thread::spawn(move || {
            assert!(is_runtime_initialized());
        });

        handle.join().expect("thread panicked");
        assert!(runtime.is_initialized());
    }

    #[test]
    fn test_drop_never_panics() {
        let runtime = DaosRuntime::new().expect("failed to create DaosRuntime");
        assert!(is_runtime_initialized());
        drop(runtime);
        assert!(!is_runtime_initialized());
    }

    #[test]
    fn test_runtime_state_persists_across_operations() {
        // Ensure clean state: drain any leftover runtimes from prior tests
        while is_runtime_initialized() {
            drop(DaosRuntime::new());
        }

        let runtime = DaosRuntime::new().expect("failed to create DaosRuntime");

        assert!(require_runtime().is_ok());
        assert!(runtime.is_initialized());
        assert!(require_runtime().is_ok());

        drop(runtime);
        assert!(require_runtime().is_err());
    }
}
