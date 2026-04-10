//! Query capabilities for objects.
//!
//! This module provides types for querying keys and metadata from DAOS objects.

use crate::io::{AKey, DKey};
use daos::daos_recx_t;

/// Flags for query_key operations.
///
/// These flags specify which key or record to query.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct QueryKeyFlags(u64);

impl QueryKeyFlags {
    /// Query the maximum key/record.
    pub const GET_MAX: Self = Self(1 << 0);
    /// Query the minimum key/record.
    pub const GET_MIN: Self = Self(1 << 1);
    /// Return the distribution key.
    pub const GET_DKEY: Self = Self(1 << 2);
    /// Return the attribute key.
    pub const GET_AKEY: Self = Self(1 << 3);
    /// Return the record extent.
    pub const GET_RECX: Self = Self(1 << 4);

    /// Returns true if no flags are set.
    pub fn is_empty(self) -> bool {
        self.0 == 0
    }

    /// Returns true if this set contains the given flag.
    pub fn contains(self, other: Self) -> bool {
        (self.0 & other.0) != 0
    }

    /// Returns the raw flags value for FFI calls.
    pub fn as_raw(self) -> u64 {
        self.0
    }
}

impl std::ops::BitOr for QueryKeyFlags {
    type Output = Self;
    fn bitor(self, rhs: Self) -> Self {
        Self(self.0 | rhs.0)
    }
}

/// Result from a query_key operation.
#[derive(Debug, Clone)]
pub struct QueryKeyResult {
    /// The distribution key, if requested.
    pub dkey: Option<DKey>,
    /// The attribute key, if requested.
    pub akey: Option<AKey>,
    /// The record extent, if requested.
    pub recx: Option<daos_recx_t>,
}

/// Result from a query_max_epoch operation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct QueryEpochResult {
    /// The maximum epoch.
    pub epoch: u64,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_query_key_flags_constants() {
        assert_eq!(QueryKeyFlags::GET_MAX.0, 1 << 0);
        assert_eq!(QueryKeyFlags::GET_MIN.0, 1 << 1);
        assert_eq!(QueryKeyFlags::GET_DKEY.0, 1 << 2);
        assert_eq!(QueryKeyFlags::GET_AKEY.0, 1 << 3);
        assert_eq!(QueryKeyFlags::GET_RECX.0, 1 << 4);
    }

    #[test]
    fn test_query_key_flags_contains() {
        let flags = QueryKeyFlags::GET_DKEY | QueryKeyFlags::GET_AKEY;
        assert!(flags.contains(QueryKeyFlags::GET_DKEY));
        assert!(flags.contains(QueryKeyFlags::GET_AKEY));
        assert!(!flags.contains(QueryKeyFlags::GET_MAX));
    }

    #[test]
    fn test_query_key_flags_is_empty() {
        assert!(QueryKeyFlags::default().is_empty());
        assert!(!QueryKeyFlags::GET_MAX.is_empty());
    }
}
