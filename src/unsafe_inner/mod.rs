//! # Unsafe Inner Layer - FFI Safety Boundaries
//!
//! This module provides the **only** location in the crate where `unsafe`
//! operations are permitted. All FFI calls to `daos-rs` are centralized here,
//! and every unsafe block is documented with a `SAFETY:` comment explaining
//! the invariants that must be maintained.
//!
//! ## Architecture
//!
//! ```text
//! unsafe_inner
//! ├── mod.rs    - Module root, re-exports public helpers
//! ├── ffi.rs    - Direct FFI wrappers calling daos-rs functions
//! ├── handle.rs - Handle validity checking and validation
//! ├── pointer.rs - Pointer-to-reference conversions with lifetime binding
//! └── buffer.rs  - Buffer creation, lifetime, and access rules
//! ```
//!
//! ## Safety Invariants
//!
//! All public functions in this module enforce these invariants:
//!
//! 1. **Handle validity**: DAOS handles must be checked before use
//! 2. **Pointer validity**: Null pointers must be validated before dereferencing
//! 3. **Buffer lifetime**: Buffers must outlive the FFI call that uses them
//! 4. **Size validity**: Buffer sizes must match what the FFI function expects
//!
//! ## Design Principles
//!
//! - **No unsafe in public API**: All unsafe code is contained in this module
//! - **Explicit contracts**: Every unsafe block has a SAFETY comment
//! - **Fail-fast on invalid input**: Invalid pointers/lengths return errors, not UB
//! - **Testable invariants**: Public wrapper functions can be tested

// Allow dead code in this foundation module - items will be used by public API modules
#![allow(dead_code, unused_imports)]

pub mod buffer;
pub mod ffi;
pub mod handle;
pub mod pointer;

// Re-export commonly used types for convenience within the crate
pub use buffer::Buffer;
pub use handle::{DaosHandle, validate_handle};
pub use pointer::{NonNullExt, as_const_char_ptr, as_mut_ptr};
