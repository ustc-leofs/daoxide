//! Container lifecycle and metadata operations.
//!
//! This module provides the [`Container`] type for managing DAOS containers
//! with RAII semantics. Containers are tied to the lifetime of the [`Pool`]
//! they were created from or opened within.
//!
//! # Example
//!
//! ```ignore
//! use daoxide::{pool::PoolBuilder, container::{ContainerBuilder, ContainerOpen}, runtime::DaosRuntime};
//!
//! let runtime = DaosRuntime::new()?;
//! let pool = PoolBuilder::new()
//!     .label("mypool")
//!     .build()?;
//!
//! // Create a container
//! let container = ContainerBuilder::new()
//!     .label("mycontainer")
//!     .build(&pool)?;
//!
//! // Open an existing container
//! let container = pool.open_container("mycontainer", ContainerOpen::RW)?;
//!
//! // Query container info
//! let info = container.query()?;
//!
//! // Allocate OIDs
//! let oid = container.alloc_oids(1)?;
//! ```

use crate::error::{DaosError, Result};
use crate::pool::Pool;
use crate::runtime::require_runtime;
use crate::unsafe_inner::ffi::{
    daos_cont_close, daos_cont_create_with_label, daos_cont_open, daos_cont_query,
};
use crate::unsafe_inner::handle::DaosHandle;
use daos::daos_cont_info_t;

/// Open flags for container operations.
///
/// These flags control the access mode when opening a container.
pub mod flags {
    /// Read-only access mode.
    pub const CONT_OPEN_RO: u32 = 1 << 0;
    /// Read-write access mode.
    pub const CONT_OPEN_RW: u32 = 1 << 1;
    /// Exclusive access mode.
    pub const CONT_OPEN_EX: u32 = 1 << 2;
    /// Skip redundancy factor check.
    pub const CONT_OPEN_FORCE: u32 = 1 << 3;
}

/// Container open mode for explicit API selection.
///
/// Use this type to explicitly specify how to open a container
/// (by label or by UUID).
#[derive(Debug, Clone, Copy)]
pub enum ContainerOpen {
    /// Open by container label.
    ByLabel,
    /// Open by container UUID.
    ByUuid,
}

/// A connected DAOS container with RAII semantics.
///
/// `Container` wraps a DAOS container handle and is tied to the lifetime
/// of the [`Pool`] it was created from or opened within. This ensures
/// that containers cannot outlive their parent pool.
///
/// Container handles are automatically closed when the `Container` value
/// is dropped.
///
/// # Safety
///
/// Container handles must only be created via [`Pool::create_container()`]
/// or [`Pool::open_container()`]. Users should not create `Container`
/// values directly.
#[derive(Debug)]
pub struct Container<'p> {
    // pool reference is never read but enforces lifetime: Container cannot outlive Pool
    #[allow(dead_code)]
    pool: &'p Pool,
    handle: Option<DaosHandle>,
}

impl<'p> Container<'p> {
    fn new(pool: &'p Pool, handle: DaosHandle) -> Self {
        Self {
            pool,
            handle: Some(handle),
        }
    }

    /// Returns the raw container handle for use with FFI operations.
    ///
    /// # Safety
    ///
    /// The returned handle is only valid while the `Container` exists
    /// and has not been closed.
    #[inline]
    pub(crate) fn as_handle(&self) -> Result<DaosHandle> {
        self.handle.ok_or(DaosError::InvalidArg)
    }

    /// Closes the container explicitly.
    ///
    /// This is called automatically when the `Container` is dropped, but
    /// can be called explicitly to handle errors or return the result.
    pub fn close(&mut self) -> Result<()> {
        if let Some(handle) = self.handle.take() {
            daos_cont_close(handle)
        } else {
            Err(DaosError::InvalidArg)
        }
    }

    /// Queries container information.
    ///
    /// Returns metadata about the container including its UUID,
    /// state, and snapshot information.
    pub fn query(&self) -> Result<ContainerInfo> {
        let handle = self.as_handle()?;
        let info = daos_cont_query(handle)?;
        Ok(ContainerInfo::from_daos_cont_info_t(info))
    }

    /// Allocates object IDs in this container.
    ///
    /// Allocates `num_oids` unique object IDs and returns the starting OID.
    /// The IDs are guaranteed to be unique within this container.
    ///
    /// # Arguments
    ///
    /// * `num_oids` - Number of OIDs to allocate (typically 1)
    pub fn alloc_oids(&self, num_oids: u64) -> Result<u64> {
        let handle = self.as_handle()?;
        let mut oid: u64 = 0;
        let ret = unsafe {
            daos::daos_cont_alloc_oids(handle.as_raw(), num_oids, &mut oid, std::ptr::null_mut())
        };
        if ret == 0 {
            Ok(oid)
        } else {
            Err(crate::error::from_daos_errno(ret))
        }
    }
}

impl Drop for Container<'_> {
    fn drop(&mut self) {
        if let Some(handle) = self.handle.take() {
            if let Err(e) = daos_cont_close(handle) {
                eprintln!(
                    "Container::drop: daos_cont_close() failed with {:?}, continuing with drop anyway",
                    e
                );
            }
        }
    }
}

/// Container information returned by query operations.
#[derive(Debug, Clone)]
pub struct ContainerInfo {
    /// Container UUID.
    pub uuid: [u8; 16],
    /// Last snapshot epoch.
    pub snapshot_epoch: u64,
    /// Number of open handles.
    pub num_handles: u32,
    /// Number of snapshots.
    pub num_snapshots: u32,
}

/// Errors that can occur when creating a container.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CreateError {
    /// Invalid argument provided.
    InvalidArg,
    /// Permission denied.
    Permission,
    /// Resource is busy.
    Busy,
    /// Unknown error with code.
    Unknown(i32),
}

impl From<DaosError> for CreateError {
    fn from(err: DaosError) -> Self {
        match err {
            DaosError::InvalidArg => CreateError::InvalidArg,
            DaosError::Permission => CreateError::Permission,
            DaosError::Busy => CreateError::Busy,
            DaosError::Unknown(code) => CreateError::Unknown(code),
            _ => CreateError::InvalidArg,
        }
    }
}

/// Builder for creating a new container with a label.
///
/// Use [`Pool::create_container()`] to create a new `ContainerBuilder`.
#[derive(Debug, Default)]
pub struct ContainerBuilder {
    label: Option<String>,
    uuid: Option<String>,
}

impl ContainerBuilder {
    /// Creates a new `ContainerBuilder` with default settings.
    #[inline]
    pub fn new() -> Self {
        Self::default()
    }

    /// Sets the container label.
    ///
    /// Container labels are human-readable identifiers.
    /// Either a label or a UUID must be provided, but not both.
    #[inline]
    pub fn label(mut self, label: impl Into<String>) -> Self {
        self.label = Some(label.into());
        self
    }

    /// Sets the container UUID.
    ///
    /// Either a label or a UUID must be provided, but not both.
    #[inline]
    pub fn uuid(mut self, uuid: impl Into<String>) -> Self {
        self.uuid = Some(uuid.into());
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

    /// Builds a new connected `Container` from this builder.
    ///
    /// # Arguments
    ///
    /// * `pool` - The pool to create the container within
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - DAOS runtime is not initialized
    /// - Neither label nor UUID is specified
    /// - Both label and UUID are specified
    /// - The container creation itself fails
    pub fn build(self, pool: &Pool) -> Result<Container<'_>> {
        require_runtime()?;
        self.validate()?;

        let pool_handle = pool.as_handle().ok_or(DaosError::InvalidArg)?;

        // For create, we primarily use label (UUID is returned if provided)
        let label = self.label.as_deref();

        if let Some(label_str) = label {
            // Use create with label
            daos_cont_create_with_label(pool_handle, label_str, None)?;
            // Container created, now open it
            let handle = daos_cont_open(pool_handle, label_str, flags::CONT_OPEN_RW)?;
            Ok(Container::new(pool, handle))
        } else {
            // UUID-based creation requires different FFI not yet implemented
            Err(DaosError::InvalidArg)
        }
    }
}

impl ContainerInfo {
    fn from_daos_cont_info_t(info: daos_cont_info_t) -> Self {
        let uuid = unsafe {
            let mut uuid = [0u8; 16];
            std::ptr::copy_nonoverlapping(info.ci_uuid.as_ptr(), uuid.as_mut_ptr(), 16);
            uuid
        };
        Self {
            uuid,
            snapshot_epoch: info.ci_lsnapshot,
            num_handles: info.ci_nhandles,
            num_snapshots: info.ci_nsnapshots,
        }
    }
}

impl Pool {
    /// Creates a new container with the given label.
    ///
    /// This is a convenience method equivalent to:
    /// ```ignore
    /// ContainerBuilder::new().label(label).build(self)
    /// ```
    ///
    /// # Arguments
    ///
    /// * `label` - The label for the new container
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - DAOS runtime is not initialized
    /// - Container creation fails
    pub fn create_container(&self, label: &str) -> Result<Container<'_>> {
        require_runtime()?;
        let pool_handle = self.as_handle().ok_or(DaosError::InvalidArg)?;

        // Create the container with label
        daos_cont_create_with_label(pool_handle, label, None)?;

        // Open the container
        let handle = daos_cont_open(pool_handle, label, flags::CONT_OPEN_RW)?;

        Ok(Container::new(self, handle))
    }

    /// Opens an existing container by label or UUID.
    ///
    /// # Arguments
    ///
    /// * `identifier` - The container label or UUID string
    /// * `open_by` - Whether to open by label or by UUID (currently unused, for future extension)
    /// * `flags` - Open flags (e.g., `flags::CONT_OPEN_RO`, `flags::CONT_OPEN_RW`)
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - DAOS runtime is not initialized
    /// - Container does not exist
    /// - Container open fails
    pub fn open_container(
        &self,
        identifier: &str,
        _open_by: ContainerOpen,
        flags: u32,
    ) -> Result<Container<'_>> {
        require_runtime()?;
        let pool_handle = self.as_handle().ok_or(DaosError::InvalidArg)?;

        let handle = daos_cont_open(pool_handle, identifier, flags)?;

        Ok(Container::new(self, handle))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_container_builder_default() {
        let builder = ContainerBuilder::new();
        assert!(builder.label.is_none());
        assert!(builder.uuid.is_none());
    }

    #[test]
    fn test_container_builder_with_label() {
        let builder = ContainerBuilder::new().label("mycontainer");
        assert_eq!(builder.label.as_deref(), Some("mycontainer"));
    }

    #[test]
    fn test_container_builder_with_uuid() {
        let builder = ContainerBuilder::new().uuid("12345678-1234-1234-1234-123456789012");
        assert_eq!(
            builder.uuid.as_deref(),
            Some("12345678-1234-1234-1234-123456789012")
        );
    }

    #[test]
    fn test_container_builder_validate_neither() {
        let builder = ContainerBuilder::new();
        let result = builder.validate();
        assert!(result.is_err());
    }

    #[test]
    fn test_container_builder_validate_both() {
        let builder = ContainerBuilder::new()
            .label("mycontainer")
            .uuid("12345678-1234-1234-1234-123456789012");
        let result = builder.validate();
        assert!(result.is_err());
    }

    #[test]
    fn test_container_builder_validate_label_only() {
        let builder = ContainerBuilder::new().label("mycontainer");
        assert!(builder.validate().is_ok());
    }

    #[test]
    fn test_container_flags_constants() {
        assert_eq!(flags::CONT_OPEN_RO, 1);
        assert_eq!(flags::CONT_OPEN_RW, 2);
        assert_eq!(flags::CONT_OPEN_EX, 4);
        assert_eq!(flags::CONT_OPEN_FORCE, 8);
    }

    #[test]
    fn test_container_open_by_enum() {
        assert!(matches!(ContainerOpen::ByLabel, ContainerOpen::ByLabel));
        assert!(matches!(ContainerOpen::ByUuid, ContainerOpen::ByUuid));
    }

    #[test]
    fn test_require_runtime_error_when_not_init() {
        while crate::runtime::is_runtime_initialized() {
            drop(crate::runtime::DaosRuntime::new());
        }

        let pool = Pool::from_handle(unsafe {
            crate::unsafe_inner::handle::DaosHandle::from_raw(daos::daos_handle_t { cookie: 12345 })
        });

        let result = pool.create_container("mycontainer");
        assert!(result.is_err());
    }

    #[test]
    fn test_close_without_handle() {
        let pool = Pool::from_handle(unsafe {
            crate::unsafe_inner::handle::DaosHandle::from_raw(daos::daos_handle_t { cookie: 12345 })
        });
        let mut container = Container::new(&pool, unsafe {
            crate::unsafe_inner::handle::DaosHandle::from_raw(daos::daos_handle_t { cookie: 0 })
        });

        // First close should fail because handle is null
        // (Container::new doesn't validate handle)
        let result = container.close();
        // The FFI will reject the invalid handle
        assert!(result.is_err());
    }

    #[test]
    fn test_container_info_debug() {
        let info = ContainerInfo {
            uuid: [0x12, 0x34, 0x56, 0x78, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0],
            snapshot_epoch: 0,
            num_handles: 0,
            num_snapshots: 0,
        };
        let debug_str = format!("{:?}", info);
        assert!(debug_str.contains("ContainerInfo"));
    }
}
