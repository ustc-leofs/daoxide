//! Example: Lower-level pool and container operations
//!
//! Demonstrates using the mid-level API for pool and container management
//! without the facade.

#![allow(unused_variables)]

use daoxide::container::{ContainerOpen, flags::CONT_OPEN_RW};
use daoxide::pool::{PoolBuilder, flags::POOL_CONNECT_NONE};

fn main() -> daoxide::Result<()> {
    // Runtime is required before any DAOS operations
    // (stored to ensure proper drop order)
    let runtime = daoxide::runtime::DaosRuntime::new()?;

    // Connect to pool
    let pool = PoolBuilder::new()
        .label("mypool")
        .system("daos_server")
        .flags(POOL_CONNECT_NONE)
        .build()?;

    // Create a container
    let container = pool.create_container("testcontainer")?;

    // Query container info
    let info = container.query()?;
    println!("Container UUID: {:?}", info.uuid);

    // Allocate some OIDs
    let start_oid = container.alloc_oids(5)?;
    println!("Allocated OIDs starting from: {}", start_oid);

    // Open container again (multiple handles allowed)
    let container2 = pool.open_container("testcontainer", ContainerOpen::ByLabel, CONT_OPEN_RW)?;

    println!("Opened container2 successfully");
    println!("Runtime: {:?}", runtime);

    // container2 is closed when dropped
    // container is closed when dropped
    // pool is disconnected when dropped
    // runtime is finalized when dropped

    Ok(())
}
