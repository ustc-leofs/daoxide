//! Example: Object operations with key-value data
//!
//! Demonstrates opening objects and performing update/fetch operations.

#![allow(unused_variables)]

use daoxide::io::{IoBuffer, Iod, IodSingleBuilder, Sgl};
use daoxide::prelude::*;

fn main() -> daoxide::Result<()> {
    let client = DaosClient::builder()
        .pool_label("mypool")
        .container_label("mycontainer")
        .build()?;

    // Create object with facade
    let oid = client.alloc_oid(
        ObjectType::KvHashed,
        ObjectClass::UNKNOWN,
        ObjectClassHints::NONE,
    )?;

    // Open object for read/write
    let object = client
        .object_builder()
        .open(oid, ObjectOpenMode::ReadWrite)?;

    // Prepare key-value data
    let dkey = daoxide::io::DKey::new(b"my_dkey")?;
    let akey = daoxide::io::AKey::new(b"my_akey")?;
    let value = IoBuffer::from_slice(b"hello world");

    // Build IOD and SGL
    let iod = Iod::Single(
        IodSingleBuilder::new(akey.clone())
            .value_len(value.len())
            .build()?,
    );
    let sgl = Sgl::builder().push(value).build()?;

    // Perform update without transaction
    object.update(&Tx::none(), &dkey, &iod, &sgl)?;
    println!("Updated key-value pair");

    // Fetch back the value
    let mut fetch_bytes = vec![0u8; 1024];
    let fetch_buffer = IoBuffer::from_mut_slice(fetch_bytes.as_mut_slice());
    let mut fetch_sgl = Sgl::builder().push(fetch_buffer).build()?;
    let fetch_iod = Iod::Single(IodSingleBuilder::new(akey).value_len(1024).build()?);
    object.fetch(&Tx::none(), &dkey, &fetch_iod, &mut fetch_sgl)?;
    drop(fetch_sgl);
    println!("Fetched: {:?}", String::from_utf8_lossy(&fetch_bytes));

    // Transactional operations require the lower-level API
    // (see docs/MIGRATION.md for transaction examples)

    Ok(())
}
