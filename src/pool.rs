//! Pool management and connectivity.
//!
//! This module provides the [`Pool`] type for connecting to DAOS pools
//! with RAII semantics. Pool handles are automatically disconnected
//! when the `Pool` value is dropped.
//!
//! # Example
//!
//! ```ignore
//! use daoxide::{pool::PoolBuilder, runtime::DaosRuntime};
//!
//! let runtime = DaosRuntime::new()?;
//! let pool = PoolBuilder::new()
//!     .label("mypool")
//!     .system("daos_server")
//!     .flags(0)
//!     .build()?;
//! // pool is automatically disconnected when dropped
//! ```

use crate::error::{DaosError, Result};
use crate::runtime::require_runtime;
use crate::unsafe_inner::ffi::{daos_pool_connect, daos_pool_disconnect};
use crate::unsafe_inner::handle::DaosHandle;

/// Connection flags for pool operations.
///
/// These flags control the access mode when connecting to a pool.
pub mod flags {
    /// No special flags.
    pub const POOL_CONNECT_NONE: u32 = 0;
    /// Read-only access mode.
    pub const POOL_CONNECT_READONLY: u32 = 1 << 0;
    /// Connect for query operations.
    pub const POOL_CONNECT_QUERY_ONLY: u32 = 1 << 1;
}

/// A connected DAOS pool with RAII semantics.
///
/// `Pool` wraps a DAOS pool handle and automatically disconnects
/// when the `Pool` value is dropped. This ensures that pool handles
/// are never leaked, even in error cases.
///
/// # Safety
///
/// Pool handles must only be created via [`PoolBuilder::build()`].
/// Users should not create `Pool` values directly.
#[derive(Debug)]
pub struct Pool {
    handle: Option<DaosHandle>,
}

impl Pool {
    /// Creates a new `PoolBuilder` for constructing a `Pool`.
    ///
    /// # Example
    ///
    /// ```ignore
    /// let pool = PoolBuilder::new()
    ///     .label("mypool")
    ///     .build()?;
    /// ```
    #[inline]
    pub fn builder() -> PoolBuilder {
        PoolBuilder::new()
    }

    /// Returns the raw pool handle for use with FFI operations.
    ///
    /// # Safety
    ///
    /// The returned handle is only valid while the `Pool` exists
    /// and has not been disconnected.
    #[allow(dead_code)]
    pub(crate) fn as_handle(&self) -> Option<DaosHandle> {
        self.handle
    }

    #[cfg(test)]
    pub(crate) fn from_handle(handle: DaosHandle) -> Self {
        Self {
            handle: Some(handle),
        }
    }

    /// Disconnects from the pool explicitly.
    ///
    /// This is called automatically when the `Pool` is dropped, but
    /// can be called explicitly to handle errors or return the result.
    pub fn disconnect(&mut self) -> Result<()> {
        if let Some(handle) = self.handle.take() {
            daos_pool_disconnect(handle)
        } else {
            Err(DaosError::InvalidArg)
        }
    }
}

/// Builder for configuring and connecting to a DAOS pool.
///
/// Use [`Pool::builder()`] to create a new `PoolBuilder`, then configure
/// it with the desired pool identifier and options before calling `build()`.
/// # Example
///
/// ```ignore
/// let pool = PoolBuilder::new()
///     .label("mypool")
///     .system("daos_server")
///     .flags(0)
///     .build()?;
/// ```
#[derive(Debug, Default)]
pub struct PoolBuilder {
    label: Option<String>,
    uuid: Option<String>,
    sys: Option<String>,
    flags: u32,
}

impl PoolBuilder {
    /// Creates a new `PoolBuilder` with default settings.
    #[inline]
    pub fn new() -> Self {
        Self::default()
    }

    /// Sets the pool label for connection.
    ///
    /// Pool labels are human-readable identifiers for pools.
    /// Either a label or a UUID must be provided, but not both.
    #[inline]
    pub fn label(mut self, label: impl Into<String>) -> Self {
        self.label = Some(label.into());
        self
    }

    /// Sets the pool UUID for connection.
    ///
    /// Either a label or a UUID must be provided, but not both.
    #[inline]
    pub fn uuid(mut self, uuid: impl Into<String>) -> Self {
        self.uuid = Some(uuid.into());
        self
    }

    /// Sets the DAOS system name for connection.
    ///
    /// If not specified, the default system is used.
    #[inline]
    pub fn system(mut self, sys: impl Into<String>) -> Self {
        self.sys = Some(sys.into());
        self
    }

    /// Sets the connection flags.
    ///
    /// Common flags are available in the [`flags`] module.
    #[inline]
    pub fn flags(mut self, flags: u32) -> Self {
        self.flags = flags;
        self
    }

    /// Validates the builder configuration.
    ///
    /// Returns `Err(DaosError::InvalidArg)` if:
    /// - Neither label nor UUID is specified
    /// - Both label and UUID are specified
    fn validate(&self) -> Result<()> {
        match (&self.label, &self.uuid) {
            (Some(_), Some(_)) => Err(DaosError::InvalidArg),
            (None, None) => Err(DaosError::InvalidArg),
            _ => Ok(()),
        }
    }

    /// Builds a connected `Pool` from this builder.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - DAOS runtime is not initialized
    /// - Neither label nor UUID is specified
    /// - Both label and UUID are specified
    /// - The pool connection itself fails
    pub fn build(&self) -> Result<Pool> {
        // Ensure runtime is initialized before attempting pool operations
        require_runtime()?;

        // Validate configuration
        self.validate()?;

        // Determine pool identifier (label or uuid)
        let pool_id = self.label.as_deref().or(self.uuid.as_deref());
        let pool_id = pool_id.ok_or(DaosError::InvalidArg)?;

        // System name as optional reference
        let sys = self.sys.as_deref();

        // Connect to the pool
        let handle = daos_pool_connect(pool_id, sys, self.flags)?;

        Ok(Pool {
            handle: Some(handle),
        })
    }
}

impl Drop for Pool {
    fn drop(&mut self) {
        if let Some(handle) = self.handle.take() {
            if let Err(e) = daos_pool_disconnect(handle) {
                eprintln!(
                    "Pool::drop: daos_pool_disconnect() failed with {:?}, continuing with drop anyway",
                    e
                );
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_pool_builder_default() {
        let builder = PoolBuilder::new();
        assert!(builder.label.is_none());
        assert!(builder.uuid.is_none());
        assert!(builder.sys.is_none());
        assert_eq!(builder.flags, 0);
    }

    #[test]
    fn test_pool_builder_with_label() {
        let builder = PoolBuilder::new().label("mypool");
        assert_eq!(builder.label.as_deref(), Some("mypool"));
    }

    #[test]
    fn test_pool_builder_with_uuid() {
        let builder = PoolBuilder::new().uuid("12345678-1234-1234-1234-123456789012");
        assert_eq!(
            builder.uuid.as_deref(),
            Some("12345678-1234-1234-1234-123456789012")
        );
    }

    #[test]
    fn test_pool_builder_with_system() {
        let builder = PoolBuilder::new().system("daos_server");
        assert_eq!(builder.sys.as_deref(), Some("daos_server"));
    }

    #[test]
    fn test_pool_builder_with_flags() {
        let builder = PoolBuilder::new().flags(42);
        assert_eq!(builder.flags, 42);
    }

    #[test]
    fn test_pool_builder_chaining() {
        let builder = PoolBuilder::new()
            .label("mypool")
            .system("daos_server")
            .flags(flags::POOL_CONNECT_READONLY);
        assert_eq!(builder.label.as_deref(), Some("mypool"));
        assert_eq!(builder.sys.as_deref(), Some("daos_server"));
        assert_eq!(builder.flags, flags::POOL_CONNECT_READONLY);
    }

    #[test]
    fn test_pool_builder_validate_neither_label_nor_uuid() {
        let builder = PoolBuilder::new().system("daos_server");
        let result = builder.validate();
        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), DaosError::InvalidArg));
    }

    #[test]
    fn test_pool_builder_validate_both_label_and_uuid() {
        let builder = PoolBuilder::new()
            .label("mypool")
            .uuid("12345678-1234-1234-1234-123456789012");
        let result = builder.validate();
        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), DaosError::InvalidArg));
    }

    #[test]
    fn test_pool_builder_validate_label_only() {
        let builder = PoolBuilder::new().label("mypool");
        assert!(builder.validate().is_ok());
    }

    #[test]
    fn test_pool_builder_validate_uuid_only() {
        let builder = PoolBuilder::new().uuid("12345678-1234-1234-1234-123456789012");
        assert!(builder.validate().is_ok());
    }

    #[test]
    fn test_pool_disconnect_without_connect() {
        let mut pool = Pool { handle: None };
        let result = pool.disconnect();
        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), DaosError::InvalidArg));
    }

    #[test]
    fn test_require_runtime_error_when_not_init() {
        // Clean up any existing runtime
        while crate::runtime::is_runtime_initialized() {
            drop(crate::runtime::DaosRuntime::new());
        }

        let builder = PoolBuilder::new().label("mypool");
        let result = builder.build();
        assert!(result.is_err());
    }

    #[test]
    fn test_pool_builder_flags_constants() {
        assert_eq!(flags::POOL_CONNECT_NONE, 0);
        assert_eq!(flags::POOL_CONNECT_READONLY, 1);
        assert_eq!(flags::POOL_CONNECT_QUERY_ONLY, 2);
    }
}
