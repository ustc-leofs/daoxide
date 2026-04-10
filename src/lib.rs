//! # DAOXIDE - Idiomatic DAOS API Wrapper
//!
//! This crate provides a safe, idiomatic Rust interface to the DAOS (Distributed
//! Asynchronous Object Storage) API through the `daos-rs` bindings.
//!
//! ## Architecture
//!
//! The crate is organized around DAOS API domains:
//!
//! - [`pool`] - Pool management and connectivity
//! - [`container`] - Container lifecycle and metadata
//! - [`object`] - Object operations and key-value storage
//! - [`tx`] - Transaction management for atomic operations
//! - [`io`] - I/O handles and data movement
//! - [`query`] - Query capabilities and aggregation
//! - [`iter`] - Iterator patterns for batch operations
//! - [`oit`] - Object instance tracking
//! - [`runtime`] - Runtime and event management
//! - [`error`] - Error types and handling
//!
//! ## Public API Design
//!
//! The public API exposes only safe, high-level abstractions. Internal
//! implementation details are hidden using `pub(crate)` visibility.
//!
//! Low-level `daos-rs` symbols are never exposed directly in the public API.
//!
//! ## Module Hierarchy
//!
//! ```text
//! daoxide
//! ├── error      - Error types
//! ├── facade     - High-level ergonomic API
//! ├── runtime    - Runtime management
//! ├── pool       - Pool operations
//! ├── container  - Container operations
//! ├── object     - Object operations
//! ├── tx         - Transaction management
//! ├── io         - I/O operations
//! ├── query      - Query operations
//! ├── iter       - Iterator utilities
//! ├── oit        - Object instance tracking
//! └── prelude    - Common re-exports
//! ```

// Public API modules
pub mod container;
pub mod error;
pub mod facade;
pub mod io;
pub mod iter;
pub mod object;
pub mod oit;
pub mod pool;
pub mod query;
pub mod runtime;
pub mod tx;

// Prelude module for common imports
pub mod prelude;

#[cfg(feature = "async")]
pub mod r#async;

// Re-export error types for convenience
pub use error::{DaosError, Result};

// Implementation details - hidden from public API
// (daos-rs bindings will be imported internally as needed)

// Unsafe inner module containing all FFI boundaries
// This module is pub(crate) to keep unsafe code isolated from public API
pub(crate) mod unsafe_inner;
