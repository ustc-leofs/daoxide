//! Handle validity checking for DAOS handles.
//!
//! DAOS handles are opaque 64-bit cookies used to reference pools, containers,
//! and objects. This module provides validation before passing handles to FFI.

use crate::error::{DaosError, Result};
use daos::daos_handle_t;

/// Represents a validated DAOS handle with guaranteed validity.
///
/// This wrapper ensures that a handle has been checked for validity
/// before being used in FFI operations.
#[derive(Debug, Clone, Copy)]
pub struct DaosHandle {
    inner: daos_handle_t,
}

impl DaosHandle {
    /// Creates a new DaosHandle from a raw DAOS handle.
    ///
    /// SAFETY: The caller must ensure the handle was obtained from a
    /// successful DAOS call and has not been invalidated (e.g., by
    /// closing the associated pool/container/object).
    #[inline]
    pub unsafe fn from_raw(raw: daos_handle_t) -> Self {
        Self { inner: raw }
    }

    /// Returns the raw DAOS handle for FFI calls.
    #[inline]
    pub fn as_raw(&self) -> daos_handle_t {
        self.inner
    }

    /// Returns the handle's cookie value (for debugging/logging).
    #[inline]
    pub fn cookie(&self) -> u64 {
        self.inner.cookie
    }
}

/// Special handle value representing an invalid/null handle.
pub const DAOS_HANDLE_NULL: daos_handle_t = daos_handle_t { cookie: 0 };

/// Checks if a DAOS handle is valid (not the null handle).
///
/// A handle with cookie == 0 is considered invalid.
#[inline]
pub fn is_valid_handle(handle: daos_handle_t) -> bool {
    handle.cookie != 0
}

/// Validates a DAOS handle and returns an error if invalid.
///
/// Use this before passing any handle to FFI calls.
#[inline]
pub fn validate_handle(handle: daos_handle_t) -> Result<()> {
    if is_valid_handle(handle) {
        Ok(())
    } else {
        Err(DaosError::InvalidArg)
    }
}

/// Converts a raw handle to a validated DaosHandle wrapper.
///
/// Returns Ok(DaosHandle) if valid, Err(DaosError::InvalidArg) otherwise.
#[inline]
pub fn try_from_handle(handle: daos_handle_t) -> Result<DaosHandle> {
    validate_handle(handle)?;
    // SAFETY: We've validated that handle is non-null above
    Ok(unsafe { DaosHandle::from_raw(handle) })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_null_handle_is_invalid() {
        let null_handle = DAOS_HANDLE_NULL;
        assert!(!is_valid_handle(null_handle));
    }

    #[test]
    fn test_valid_handle_is_valid() {
        let valid_handle = daos_handle_t { cookie: 12345 };
        assert!(is_valid_handle(valid_handle));
    }

    #[test]
    fn test_validate_null_handle_returns_error() {
        let result = validate_handle(DAOS_HANDLE_NULL);
        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), DaosError::InvalidArg));
    }

    #[test]
    fn test_validate_valid_handle_ok() {
        let valid_handle = daos_handle_t { cookie: 12345 };
        let result = validate_handle(valid_handle);
        assert!(result.is_ok());
    }

    #[test]
    fn test_try_from_handle_success() {
        let handle = daos_handle_t { cookie: 999 };
        let result = try_from_handle(handle);
        assert!(result.is_ok());
        let wrapper = result.unwrap();
        assert_eq!(wrapper.cookie(), 999);
    }

    #[test]
    fn test_try_from_handle_failure() {
        let result = try_from_handle(DAOS_HANDLE_NULL);
        assert!(result.is_err());
    }
}
