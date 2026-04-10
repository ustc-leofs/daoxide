//! Example: Minimal DAOS client using the facade API
//!
//! This example demonstrates connecting to DAOS and performing basic
//! key-value operations using the high-level facade API.
//!
//! # Running
//!
//! ```ignore
//! # Note: Requires a running DAOS environment
//!
//! cargo run --example minimal_client --features daos-rs/...
//! ```

use daoxide::prelude::*;

fn main() -> daoxide::Result<()> {
    // Connect to DAOS pool and container
    let client = DaosClient::builder()
        .pool_label("mypool")
        .container_label("mycontainer")
        .build()?;

    // Allocate a new object
    let oid = client.alloc_oid(
        ObjectType::KvHashed,
        ObjectClass::UNKNOWN,
        ObjectClassHints::NONE,
    )?;

    // Store a value
    client.put(oid, b"dkey1", b"akey1", b"hello world")?;

    // Retrieve the value
    let mut buffer = vec![0u8; 1024];
    client.get(oid, b"dkey1", b"akey1", &mut buffer[..11])?;

    println!("Retrieved: {:?}", String::from_utf8_lossy(&buffer[..11]));

    // Delete the key
    client.delete(oid, Some(b"dkey1"), None, Tx::none())?;

    Ok(())
}
