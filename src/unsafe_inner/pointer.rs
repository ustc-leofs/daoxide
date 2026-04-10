//! Pointer conversion utilities with safety guarantees.
//!
//! Provides safe wrappers around raw pointer operations for FFI calls.

use crate::error::{DaosError, Result};
use std::ffi::{CStr, CString};
use std::ptr::NonNull;

/// Extension trait for NonNull pointers providing safe conversions.
pub trait NonNullExt<T> {
    /// Converts a potentially null raw pointer to a NonNull, checking for null.
    ///
    /// Returns Err(InvalidArg) if the pointer is null.
    fn check_null(ptr: *const T) -> Result<NonNull<T>>;

    /// Converts a potentially null mutable raw pointer to a NonNull, checking for null.
    ///
    /// Returns Err(InvalidArg) if the pointer is null.
    fn check_null_mut(ptr: *mut T) -> Result<NonNull<T>>;
}

impl<T> NonNullExt<T> for NonNull<T> {
    #[inline]
    fn check_null(ptr: *const T) -> Result<NonNull<T>> {
        if ptr.is_null() {
            Err(DaosError::InvalidArg)
        } else {
            // SAFETY: ptr is non-null, which is all NonNull needs
            Ok(unsafe { NonNull::new_unchecked(ptr as *mut T) })
        }
    }

    #[inline]
    fn check_null_mut(ptr: *mut T) -> Result<NonNull<T>> {
        if ptr.is_null() {
            Err(DaosError::InvalidArg)
        } else {
            // SAFETY: ptr is non-null, which is all NonNull needs
            Ok(unsafe { NonNull::new_unchecked(ptr) })
        }
    }
}

/// Validates that a pointer is non-null before FFI use.
///
/// Use this when an FFI function requires a non-null pointer but
/// you need to validate it first to return a proper error.
#[inline]
pub fn validate_non_null<T>(ptr: *const T) -> Result<NonNull<T>> {
    NonNull::check_null(ptr)
}

/// Validates that a mutable pointer is non-null before FFI use.
#[inline]
pub fn validate_non_null_mut<T>(ptr: *mut T) -> Result<NonNull<T>> {
    NonNull::check_null_mut(ptr)
}

/// Converts a C string pointer to a Rust &str, validating null and length.
///
/// Returns Err(InvalidArg) if ptr is null.
/// Returns Err(Unknown) if the string contains invalid UTF-8.
pub fn validate_c_str<'a>(ptr: *const std::os::raw::c_char) -> Result<&'a str> {
    if ptr.is_null() {
        return Err(DaosError::InvalidArg);
    }
    // SAFETY: We've checked ptr is non-null, and C strings are null-terminated
    let c_str = unsafe { CStr::from_ptr(ptr) };
    c_str
        .to_str()
        .map_err(|_| DaosError::Internal("Invalid UTF-8 in C string".to_string()))
}

/// Validates a mutable C string pointer, ensuring it's non-null.
pub fn validate_c_str_mut(ptr: *mut std::os::raw::c_char) -> Result<()> {
    if ptr.is_null() {
        Err(DaosError::InvalidArg)
    } else {
        Ok(())
    }
}

/// Converts a byte slice reference to a C char pointer for FFI.
///
/// Returns an owned `CString` suitable for passing to FFI functions that
/// expect a NUL-terminated string.
pub fn as_char_ptr(s: &str) -> Result<CString> {
    CString::new(s).map_err(|_| DaosError::InvalidArg)
}

/// Converts a byte slice reference to a const C char pointer for FFI.
pub fn as_const_char_ptr(s: &str) -> Result<CString> {
    as_char_ptr(s)
}

/// Converts a NonNull pointer to a mutable pointer.
#[inline]
pub fn as_mut_ptr<T>(ptr: NonNull<T>) -> *mut T {
    ptr.as_ptr()
}

/// Trait for safely converting types to raw pointers for FFI.
pub trait AsFFIPtr {
    /// The FFI pointer type this type converts to.
    type Target;

    /// Performs the conversion with safety checks.
    fn as_ffi_ptr(&self) -> Result<Self::Target>;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_check_null_rejects_null() {
        let null_ptr: *const i32 = std::ptr::null();
        let result = NonNull::<i32>::check_null(null_ptr);
        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), DaosError::InvalidArg));
    }

    #[test]
    fn test_check_null_accepts_valid() {
        let value = 42i32;
        let valid_ptr: *const i32 = &value;
        let result = NonNull::<i32>::check_null(valid_ptr);
        assert!(result.is_ok());
    }

    #[test]
    fn test_validate_non_null_mut() {
        let mut value = 42i32;
        let result = validate_non_null_mut(&mut value);
        assert!(result.is_ok());
    }

    #[test]
    fn test_validate_c_str_rejects_null() {
        let result = validate_c_str(std::ptr::null());
        assert!(result.is_err());
    }

    #[test]
    fn test_validate_c_str_valid() {
        let c_string = CString::new("hello").unwrap();
        let ptr = c_string.as_ptr();
        let result = validate_c_str(ptr);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), "hello");
    }

    #[test]
    fn test_as_char_ptr() {
        let s = "test string";
        let result = as_char_ptr(s);
        assert!(result.is_ok());
        assert_eq!(result.unwrap().as_bytes(), s.as_bytes());
    }

    #[test]
    fn test_as_char_ptr_rejects_nul() {
        // String with interior NUL character
        let s = "line1\0line2";
        let result = as_char_ptr(s);
        assert!(result.is_err()); // CString::new fails on interior nul
    }
}
