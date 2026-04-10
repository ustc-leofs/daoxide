//! Typed error model for DAOS operations.
//!
//! This module provides a structured `DaosError` enum that maps raw DAOS
//! return values to meaningful Rust error variants, ensuring type-safe
//! error propagation across the public API.
//!
//! # Error Categories
//!
//! - `InvalidArg`: Invalid parameters passed to an operation
//! - `NotFound`: Requested entity does not exist
//! - `Permission`: Operation not permitted
//! - `Timeout`: Operation timed out
//! - `Busy`: Resource or device is busy
//! - `Unreachable`: Cannot reach the target node/service
//! - `Unsupported`: Operation not supported
//! - `Unknown`: Known DAOS code but not mapped to a specific category

use std::fmt;

/// Result type alias using `DaosError` as the error variant.
pub type Result<T> = std::result::Result<T, DaosError>;

/// Structured error types for DAOS operations.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DaosError {
    /// Invalid parameters provided to the operation.
    InvalidArg,
    /// The requested entity does not exist.
    NotFound,
    /// Operation lacks necessary permissions.
    Permission,
    /// Operation timed out before completing.
    Timeout,
    /// Resource or device is currently busy.
    Busy,
    /// Cannot reach the target node or service.
    Unreachable,
    /// This operation is not supported.
    Unsupported,
    /// Transaction must be restarted due to conflicts or other reasons.
    /// The caller should call `Transaction::restart()` and retry the operation.
    TxRestart,
    /// A known DAOS error code that doesn't map to a specific category.
    Unknown(i32),
    /// An internal error from daoxide itself.
    Internal(String),
}

impl fmt::Display for DaosError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            DaosError::InvalidArg => write!(f, "Invalid argument"),
            DaosError::NotFound => write!(f, "Entity not found"),
            DaosError::Permission => write!(f, "Permission denied"),
            DaosError::Timeout => write!(f, "Operation timed out"),
            DaosError::Busy => write!(f, "Resource is busy"),
            DaosError::Unreachable => write!(f, "Target is unreachable"),
            DaosError::Unsupported => write!(f, "Operation not supported"),
            DaosError::TxRestart => write!(f, "Transaction must be restarted"),
            DaosError::Unknown(code) => write!(f, "Unknown error: {}", code),
            DaosError::Internal(msg) => write!(f, "Internal error: {}", msg),
        }
    }
}

impl std::error::Error for DaosError {}

/// Errors from daos-rs FFI calls typically have this signature.
pub type RawDaosResult = i32;

/// Convert a raw DAOS error code to a `DaosError`.
///
/// This function maps known DAOS error codes to their corresponding
/// `DaosError` variants. Unknown error codes are preserved as
/// `DaosError::Unknown(code)` to allow debugging and future handling.
///
/// # Arguments
///
/// * `code` - The raw DAOS error code (e.g., `DER_INVAL`, `DER_NOTFOUND`)
///
/// # Examples
///
/// ```
/// use daoxide::error::{from_daos_errno, DaosError};
///
/// // Known error code
/// let err = from_daos_errno(1003); // DER_INVAL
/// assert!(matches!(err, DaosError::InvalidArg));
///
/// // Unknown error code
/// let err = from_daos_errno(9999);
/// assert!(matches!(err, DaosError::Unknown(9999)));
/// ```
/// DER_SUCCESS = 0
const DER_SUCCESS: i32 = 0;

#[inline]
pub fn from_daos_errno(code: i32) -> DaosError {
    if code == DER_SUCCESS {
        return DaosError::Internal("unexpected success code".to_string());
    }
    match code {
        1001 => DaosError::Permission,
        1002 => DaosError::InvalidArg,
        1003 => DaosError::InvalidArg,
        1005 => DaosError::NotFound,
        1006 => DaosError::Unreachable,
        1010 => DaosError::Unsupported,
        1011 => DaosError::Timeout,
        1012 => DaosError::Busy,
        2025 => DaosError::TxRestart,
        _ => DaosError::Unknown(code),
    }
}

impl From<i32> for DaosError {
    /// Convert a raw `i32` error code to `DaosError`.
    fn from(code: i32) -> Self {
        from_daos_errno(code)
    }
}

impl DaosError {
    /// Returns the raw DAOS error code if this is an `Unknown` variant.
    ///
    /// Returns `None` for non-Unknown variants.
    #[inline]
    pub const fn code(&self) -> Option<i32> {
        match self {
            DaosError::Unknown(code) => Some(*code),
            _ => None,
        }
    }
}

/// Helper trait for attaching context to errors without losing the original code.
///
/// This trait enables error propagation patterns where additional context
/// is added to errors while preserving the underlying error information.
pub trait ContextExt<T> {
    /// Attach additional context to an error, preserving the original error code.
    fn context(self, msg: impl Into<String>) -> Result<T>;
}

impl<T> ContextExt<T> for Result<T> {
    fn context(self, msg: impl Into<String>) -> Result<T> {
        self.map_err(|e| {
            // Preserve the original error code in Unknown variant
            if let Some(code) = e.code() {
                DaosError::Unknown(code)
            } else {
                DaosError::Internal(msg.into())
            }
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_known_error_mappings() {
        // GURT errors (1000-1999)
        assert!(matches!(from_daos_errno(1001), DaosError::Permission)); // DER_NO_PERM
        assert!(matches!(from_daos_errno(1002), DaosError::InvalidArg)); // DER_NO_HDL
        assert!(matches!(from_daos_errno(1003), DaosError::InvalidArg)); // DER_INVAL
        assert!(matches!(from_daos_errno(1005), DaosError::NotFound)); // DER_NONEXIST
        assert!(matches!(from_daos_errno(1006), DaosError::Unreachable)); // DER_UNREACH
        assert!(matches!(from_daos_errno(1010), DaosError::Unsupported)); // DER_NOSYS
        assert!(matches!(from_daos_errno(1011), DaosError::Timeout)); // DER_TIMEDOUT
        assert!(matches!(from_daos_errno(1012), DaosError::Busy)); // DER_BUSY
        assert!(matches!(from_daos_errno(2025), DaosError::TxRestart)); // DER_TX_RESTART
    }

    #[test]
    fn test_unknown_error_preserved() {
        // Unknown codes should be preserved exactly
        let err = from_daos_errno(9999);
        assert!(matches!(err, DaosError::Unknown(9999)));

        let err = from_daos_errno(-1);
        assert!(matches!(err, DaosError::Unknown(-1)));
    }

    #[test]
    fn test_error_display() {
        assert_eq!(format!("{}", DaosError::InvalidArg), "Invalid argument");
        assert_eq!(format!("{}", DaosError::NotFound), "Entity not found");
        assert_eq!(
            format!("{}", DaosError::Unknown(1234)),
            "Unknown error: 1234"
        );
    }

    #[test]
    fn test_error_code_extraction() {
        let err = DaosError::Unknown(5001);
        assert_eq!(err.code(), Some(5001));

        let err = DaosError::InvalidArg;
        assert_eq!(err.code(), None);
    }

    #[test]
    fn test_from_i32() {
        let err: DaosError = 1003i32.into();
        assert!(matches!(err, DaosError::InvalidArg));
    }
}
