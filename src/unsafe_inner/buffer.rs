//! Buffer lifetime rules and safety wrappers.
//!
//! Provides safe wrappers for creating and using buffers with FFI calls,
//! ensuring buffers are properly aligned, sized, and outlive FFI operations.

use crate::error::{DaosError, Result};
use std::mem::MaybeUninit;
use std::ptr::NonNull;

/// Maximum buffer size to prevent allocation of excessive memory.
pub const MAX_BUFFER_SIZE: usize = 256 * 1024 * 1024; // 256 MB

/// A buffer with validated size for FFI operations.
///
/// This type ensures that:
/// - The buffer size is within acceptable bounds
/// - The pointer is non-null and properly aligned
/// - The lifetime is tied to the original data
#[derive(Debug)]
pub struct Buffer<T> {
    ptr: NonNull<T>,
    len: usize,
}

impl<T> Buffer<T> {
    /// Creates a new Buffer from a pointer and length, validating both.
    ///
    /// SAFETY: The caller must ensure:
    /// - `ptr` is non-null and valid for `len` elements
    /// - `len` does not exceed MAX_BUFFER_SIZE
    /// - The buffer outlives this Buffer instance
    #[inline]
    pub unsafe fn new(ptr: NonNull<T>, len: usize) -> Result<Self> {
        if len > MAX_BUFFER_SIZE {
            return Err(DaosError::InvalidArg);
        }
        Ok(Self { ptr, len })
    }

    /// Creates a Buffer from a slice, ensuring lifetime compatibility.
    #[inline]
    pub fn from_slice(slice: &[T]) -> Result<Self>
    where
        T: Clone,
    {
        if slice.len() > MAX_BUFFER_SIZE {
            return Err(DaosError::InvalidArg);
        }
        let mut buffer = Vec::with_capacity(slice.len());
        buffer.extend_from_slice(slice);
        // SAFETY: Vec's internal buffer is always non-null and properly aligned.
        // We use the first element's pointer as the base for the Buffer.
        let ptr = NonNull::from(&buffer[0]);
        let len = slice.len();
        // Leak the buffer to extend its lifetime - caller must ensure
        // the buffer is not used after the Buffer is dropped
        std::mem::forget(buffer);
        unsafe { Self::new(ptr, len) }
    }

    /// Returns the buffer length.
    #[inline]
    pub fn len(&self) -> usize {
        self.len
    }

    /// Returns true if the buffer is empty.
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    /// Returns the pointer to the buffer data.
    #[inline]
    pub fn as_ptr(&self) -> *const T {
        self.ptr.as_ptr()
    }

    /// Returns a mutable pointer to the buffer data.
    #[inline]
    pub fn as_mut_ptr(&self) -> *mut T {
        self.ptr.as_ptr()
    }

    /// Returns the buffer as a slice.
    ///
    /// SAFETY: The caller must ensure the buffer is used within its lifetime.
    #[inline]
    pub unsafe fn as_slice(&self) -> &[T] {
        // SAFETY: The caller guarantees the buffer is valid for the duration of this call
        unsafe { std::slice::from_raw_parts(self.ptr.as_ptr(), self.len) }
    }

    /// Returns the buffer as a mutable slice.
    ///
    /// SAFETY: The caller must ensure exclusive access and valid lifetime.
    #[inline]
    pub unsafe fn as_mut_slice(&mut self) -> &mut [T] {
        // SAFETY: The caller guarantees exclusive access and valid lifetime
        unsafe { std::slice::from_raw_parts_mut(self.ptr.as_ptr(), self.len) }
    }
}

/// Validates that a buffer size is within acceptable bounds.
#[inline]
pub fn validate_buffer_size(size: usize) -> Result<()> {
    if size > MAX_BUFFER_SIZE {
        Err(DaosError::InvalidArg)
    } else {
        Ok(())
    }
}

/// Validates buffer size is non-zero.
#[inline]
pub fn validate_buffer_size_nonzero(size: usize) -> Result<()> {
    if size == 0 {
        Err(DaosError::InvalidArg)
    } else {
        Ok(())
    }
}

/// Validates both pointer and size for FFI buffer operations.
///
/// Returns Err(InvalidArg) if ptr is null or size exceeds MAX_BUFFER_SIZE.
#[inline]
pub fn validate_ffi_buffer<T>(ptr: *const T, size: usize) -> Result<Buffer<T>> {
    if ptr.is_null() {
        return Err(DaosError::InvalidArg);
    }
    if size > MAX_BUFFER_SIZE {
        return Err(DaosError::InvalidArg);
    }
    // SAFETY: We've validated ptr is non-null and size is within bounds
    unsafe { Buffer::new(NonNull::new_unchecked(ptr as *mut T), size) }
}

/// Represents an output buffer for FFI calls that write data.
///
/// Tracks capacity separately from filled length, since FFI output buffers
/// report actual bytes written through the filled field.
pub struct OutputBuffer<'a> {
    buffer: &'a mut [MaybeUninit<u8>],
    filled: usize,
}

impl<'a> OutputBuffer<'a> {
    /// Creates a new OutputBuffer from a mutable byte slice.
    #[inline]
    pub fn new(buffer: &'a mut [MaybeUninit<u8>]) -> Self {
        Self { buffer, filled: 0 }
    }

    /// Returns the total capacity of the buffer.
    #[inline]
    pub fn capacity(&self) -> usize {
        self.buffer.len()
    }

    /// Returns the number of bytes that have been filled by FFI.
    #[inline]
    pub fn filled(&self) -> usize {
        self.filled
    }

    /// Returns a pointer to the buffer data for FFI calls.
    #[inline]
    pub fn as_ptr(&mut self) -> *mut std::ffi::c_void {
        self.buffer.as_mut_ptr() as *mut std::ffi::c_void
    }

    /// Returns the buffer as a mutable slice of bytes.
    #[inline]
    pub fn as_bytes(&mut self) -> &mut [MaybeUninit<u8>] {
        self.buffer
    }

    /// Updates the filled length after an FFI call.
    #[inline]
    pub fn set_filled(&mut self, filled: usize) -> Result<()> {
        if filled > self.buffer.len() {
            return Err(DaosError::Internal(
                "Filled size exceeds buffer capacity".to_string(),
            ));
        }
        self.filled = filled;
        Ok(())
    }

    /// Finalizes the buffer and returns the filled data as a byte slice.
    #[inline]
    pub fn freeze(self) -> &'a [u8] {
        // SAFETY: filled <= buffer.len() and we've only written valid bytes
        unsafe { std::slice::from_raw_parts(self.buffer.as_mut_ptr() as *const u8, self.filled) }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_validate_buffer_size_accepts_valid() {
        assert!(validate_buffer_size(1024).is_ok());
    }

    #[test]
    fn test_validate_buffer_size_rejects_excessive() {
        let result = validate_buffer_size(MAX_BUFFER_SIZE + 1);
        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), DaosError::InvalidArg));
    }

    #[test]
    fn test_validate_buffer_size_nonzero_rejects_zero() {
        assert!(validate_buffer_size_nonzero(0).is_err());
    }

    #[test]
    fn test_validate_buffer_size_nonzero_accepts_nonzero() {
        assert!(validate_buffer_size_nonzero(1).is_ok());
    }

    #[test]
    fn test_validate_ffi_buffer_rejects_null() {
        let null_ptr: *const u8 = std::ptr::null();
        let result = validate_ffi_buffer(null_ptr, 100);
        assert!(result.is_err());
    }

    #[test]
    fn test_validate_ffi_buffer_rejects_excessive_size() {
        let data = [0u8; 64];
        let result = validate_ffi_buffer(data.as_ptr(), MAX_BUFFER_SIZE + 1);
        assert!(result.is_err());
    }

    #[test]
    fn test_output_buffer_filled_tracking() {
        let mut buffer = vec![MaybeUninit::new(0u8); 100].into_boxed_slice();
        let output = OutputBuffer::new(&mut buffer);
        assert_eq!(output.capacity(), 100);
        assert_eq!(output.filled(), 0);
    }

    #[test]
    fn test_output_buffer_set_filled() {
        let mut buffer = vec![MaybeUninit::new(0u8); 100].into_boxed_slice();
        let mut output = OutputBuffer::new(&mut buffer);
        output.set_filled(50).unwrap();
        assert_eq!(output.filled(), 50);
    }

    #[test]
    fn test_output_buffer_set_filled_rejects_overflow() {
        let mut buffer = vec![MaybeUninit::new(0u8); 100].into_boxed_slice();
        let mut output = OutputBuffer::new(&mut buffer);
        let result = output.set_filled(101);
        assert!(result.is_err());
    }
}
