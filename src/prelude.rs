//! Common re-exports for convenient importing.
//!
//! The prelude provides one-stop access to the most commonly used types,
//! reducing boilerplate for typical usage patterns.
//!
//! # What's Included
//!
//! - [`DaosError`] and [`Result`] - Error handling
//! - [`DaosClient`] and [`DaosClientBuilder`] - High-level facade
//! - [`ObjectType`], [`ObjectClass`], [`ObjectOpenMode`] - Object configuration
//! - [`ObjectId`] - Object identification
//! - [`DKey`], [`AKey`], [`IoBuffer`] - Key/value types
//! - [`Tx`] - Transaction marker
//!
//! # Example
//!
//! ```ignore
//! use daoxide::prelude::*;
//!
//! let client = DaosClient::builder()
//!     .pool_label("mypool")
//!     .container_label("mycontainer")
//!     .build()?;
//! ```

// Error handling
pub use crate::error::{DaosError, Result};

// High-level facade
pub use crate::facade::{DaosClient, DaosClientBuilder};

// Object types
pub use crate::object::{ObjectClass, ObjectClassHints, ObjectId, ObjectOpenMode, ObjectType};

// Key/value IO types
pub use crate::io::{AKey, DKey, IoBuffer};

// Transaction
pub use crate::tx::Tx;
