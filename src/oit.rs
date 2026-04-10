//! Object instance tracking.
//!
//! # Implementation Status
//!
//! This module is currently a stub. The OIT (Object Instance Tracking)
//! functionality requires `daos_oit_*` FFI functions which are not exported
//! by the current `daos-rs` bindings.
//!
//! Specifically, the following functions are needed but unavailable:
//! - `daos_oit_open` - Open OIT for a container
//! - `daos_oit_close` - Close OIT handle
//! - `daos_oit_list` - List OIDs in OIT
//! - `daos_oit_mark` - Mark an OID in OIT
//! - `daos_oit_list_unmarked` - List unmarked OIDs
//! - `daos_oit_list_filter` - List OIDs with filter callback
//!
//! # Workaround
//!
//! Until `daos-rs` adds OIT function bindings, use the object
//! enumeration API ([`crate::iter`]) to iterate objects instead.

pub struct Oit;
