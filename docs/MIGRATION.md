# Migration Guide: daos-rs to daoxide

This guide covers migrating from raw `daos-rs` bindings to the safe `daoxide` API.

## Overview

`daoxide` provides a safe, ergonomic Rust wrapper around `daos-rs`. Key improvements:

- **RAII handle management**: Pool, container, and object handles are automatically disconnected/closed on drop
- **Typed errors**: `DaosError` enum with meaningful variants instead of raw integer codes
- **Builder patterns**: Fluent APIs for configuration instead of complex initialization
- **Type safety**: Strong types for keys, object IDs, and flags prevent runtime errors
- **Transaction safety**: `Tx` marker type ensures proper transaction handling

## Quick Reference

| daos-rs Concept | daoxide Equivalent |
|----------------|--------------------|
| `daos_init()` / `daos_fini()` | `DaosRuntime` (RAII, auto-managed) |
| Raw `daos_handle_t` | `Pool`, `Container`, `Object` (RAII wrappers) |
| `daos_pool_connect()` | `PoolBuilder::build()` |
| `daos_cont_create()` / `daos_cont_open()` | `Pool::create_container()` / `Pool::open_container()` |
| `daos_obj_open()` | `Object::open()` |
| Manual handle cleanup | Automatic via `Drop` |
| Raw integer error codes | `DaosError` enum |

## Initialization and Lifecycle

### Before (daos-rs)

```c
// daos-rs style (C-like)
let ret = daos_init();
if ret != 0 {
    // handle error with raw code
}

let pool_handle = daos_pool_connect(
    "mypool".as_ptr(),
    std::ptr::null(),
    flags,
    &mut pool_coh,
    std::ptr::null_mut(),
);

if ret != 0 {
    // handle error with raw code
}

// ... use pool ...

daos_pool_disconnect(pool_coh, std::ptr::null_mut());
daos_fini();
```

### After (daoxide)

```rust
use daoxide::{DaosRuntime, PoolBuilder, Result};

fn main() -> Result<()> {
    // Runtime initializes DAOS automatically
    let runtime = DaosRuntime::new()?;
    
    // Pool connection with builder pattern
    let pool = PoolBuilder::new()
        .label("mypool")
        .build()?;
    
    // Pool is automatically disconnected when dropped
    // ...
    
    Ok(())
} // daos_fini() called automatically
```

## Pool Operations

### Before (daos-rs)

```c
let ret = daos_pool_connect(
    pool_label.c_str(),
    sys_name.c_str(),
    flags,
    &mut poh,
    std::ptr::null_mut(),
);
if ret != 0 { /* handle error */ }

// Query pool info requires separate call with raw handle
let ret = daos_pool_query(poh, std::ptr::null_mut(), /* ... */);
```

### After (daoxide)

```rust
use daoxide::{PoolBuilder, Result};

fn main() -> Result<()> {
    let pool = PoolBuilder::new()
        .label("mypool")
        .system("daos_server")  // optional
        .flags(0)
        .build()?;
    
    // Clean, typed API
    Ok(())
} // pool.disconnect() called automatically
```

## Container Operations

### Before (daos-rs)

```c
let ret = daos_cont_create_with_label(
    poh,
    "mycontainer".as_ptr(),
    std::ptr::null(),
    &mut coh,
    std::ptr::null_mut(),
);
// Or open existing:
let ret = daos_cont_open(poh, "mycontainer".as_ptr(), flags, &mut coh, std::ptr::null_mut());
```

### After (daoxide)

```rust
use daoxide::{Pool, Result};

fn main() -> Result<()> {
    let pool = PoolBuilder::new().label("mypool").build()?;
    
    // Create container
    let container = pool.create_container("mycontainer")?;
    
    // Or open existing
    let container = pool.open_container(
        "mycontainer",
        ContainerOpen::ByLabel,
        flags::CONT_OPEN_RW,
    )?;
    
    Ok(())
} // container.close() called automatically
```

## Object Operations

### Opening Objects

#### Before (daos-rs)

```c
let ret = daos_obj_open(
    coh,
    oid,
    mode,  // raw flags like DAOS_OO_RW
    &mut oh,
);
```

#### After (daoxide)

```rust
use daoxide::{Object, ObjectOpenMode, ObjectType, ObjectClass, ObjectClassHints};

let object = Object::open(coh, oid, ObjectOpenMode::ReadWrite)?;
```

### Key-Value Operations

#### Before (daos-rs)

```c
// Create dkey/akey structures manually
let dkey = daos_key_t {
    iov_buf: dkey_bytes.as_ptr() as *mut c_void,
    iov_buf_len: dkey_bytes.len(),
    iov_len: dkey_bytes.len(),
};

// Create iod
let iod = daos_iod_t {
    iod_name: akey,
    iod_type: DAOS_IOD_SINGLE,
    iod_size: value_len as u64,
    // ...
};

// Create sgl
let sgl = d_sg_list_t {
    sg_nr: 1,
    sg_iovs: &mut iov,
    // ...
};

// Update
let ret = daos_obj_update(oh, tx_handle, 0, &dkey, 1, &iod, &sgl, std::ptr::null_mut());
```

#### After (daoxide)

```rust
use daoxide::{
    io::{DKey, AKey, IoBuffer, Sgl, Iod, IodSingleBuilder},
    Object, Tx, Result,
};

let dkey = DKey::new(b"my_dkey")?;
let akey = AKey::new(b"my_akey")?;
let value = IoBuffer::from_vec(b"hello world".to_vec());

let iod = Iod::Single(IodSingleBuilder::new(akey)
    .value_len(value.len())
    .build()?);

let sgl = Sgl::builder()
    .push(value)
    .build()?;

// Single update call
object.update(&Tx::none(), &dkey, &iod, &sgl)?;
```

### Fetching Values

#### Before (daos-rs)

```c
let mut buffer = vec![0u8; max_size];
let mut sgl = d_sg_list_t {
    sg_nr: 1,
    sg_iovs: &mut iov,
    // ...
};

let ret = daos_obj_fetch(
    oh,
    tx_handle,
    0,
    &dkey,
    1,
    &iod,
    &mut sgl,
    std::ptr::null_mut(),
    std::ptr::null_mut(),
);
```

#### After (daoxide)

```rust
use daoxide::io::{DKey, AKey, IoBuffer, Sgl, Iod, IodSingleBuilder};

let mut buffer = IoBuffer::from_vec(vec![0u8; 1024]);
let dkey = DKey::new(b"my_dkey")?;
let akey = AKey::new(b"my_akey")?;

let mut sgl = Sgl::builder()
    .push(buffer)
    .build()?;

object.fetch(&Tx::none(), &dkey, &iod, &mut sgl)?;
```

## Transaction Operations

### Before (daos-rs)

```c
let mut tx_handle: daos_handle_t = daos_handle_t { cookie: 0 };
let ret = daos_tx_open(coh, &mut tx_handle, 0, std::ptr::null_mut());

// Perform operations within transaction
let ret = daos_obj_update(oh, tx_handle, 0, &dkey, 1, &iod, &sgl, std::ptr::null_mut());

// Commit
let ret = daos_tx_commit(tx_handle, std::ptr::null_mut());

// Or abort
let ret = daos_tx_abort(tx_handle, std::ptr::null_mut());

// Close
let ret = daos_tx_close(tx_handle, std::ptr::null_mut());
```

### After (daoxide)

```rust
use daoxide::{Container, Tx, Transaction, Result};

fn transactional_ops(container: &Container<'_>) -> Result<()> {
    // Begin transaction
    let tx = Transaction::new(container.as_handle()?, 0)?;
    
    // Perform operations
    // object.update(&tx, &dkey, &iod, &sgl)?;
    
    // Commit
    tx.commit()?;
    
    Ok(())
} // tx.close() called automatically on drop
```

### Transactionless Operations

```rust
// Use Tx::none() for operations without transaction semantics
object.update(&Tx::none(), &dkey, &iod, &sgl)?;
```

## Error Handling

### Before (daos-rs)

```c
let ret = daos_pool_connect(/* ... */);
if ret != 0 {
    if ret == -DER_NO_PERM {
        // handle permission error
    } else if ret == -DER_NONEXIST {
        // handle not found
    } else if ret == -DER_UNREACH {
        // handle unreachable
    }
    // ... many more cases
}
```

### After (daoxide)

```rust
use daoxide::{DaosError, Result, PoolBuilder};

fn main() -> Result<()> {
    let pool = match PoolBuilder::new().label("mypool").build() {
        Ok(p) => p,
        Err(DaosError::Permission) => {
            // handle permission error
            return Err(DaosError::Permission);
        }
        Err(DaosError::NotFound) => {
            // handle not found
            return Err(DaosError::NotFound);
        }
        Err(DaosError::Unreachable) => {
            // handle unreachable
            return Err(DaosError::Unreachable);
        }
        Err(e) => return Err(e),
    };
    
    Ok(())
}
```

### Error Enum Variants

| Variant | Meaning |
|---------|---------|
| `InvalidArg` | Invalid parameters passed to an operation |
| `NotFound` | Requested entity does not exist |
| `Permission` | Operation lacks necessary permissions |
| `Timeout` | Operation timed out before completing |
| `Busy` | Resource or device is currently busy |
| `Unreachable` | Cannot reach the target node or service |
| `Unsupported` | This operation is not supported |
| `TxRestart` | Transaction must be restarted |
| `Unknown(i32)` | Known DAOS code but not mapped to a specific category |

## Enumeration (Listing Keys)

### Before (daos-rs)

```c
let mut anchor = daos_anchor_t { /* zeroed */ };
let mut nr = batch_size;
let mut kds = vec![daos_key_desc_t::zeroed(); nr as usize];
let mut buffer = vec![0u8; buffer_size];

while !daos_anchor_is_eof(&anchor) {
    let ret = daos_obj_list_dkey(
        oh,
        std::ptr::null_mut(),
        &mut nr,
        kds.as_mut_ptr(),
        &mut sgl,
        &mut anchor,
        std::ptr::null_mut(),
    );
    if ret != 0 { /* handle error */ }
    
    // Process keys in buffer
    for i in 0..nr as usize {
        // extract key at kds[i].kd_key_len offset
    }
}
```

### After (daoxide)

```rust
use daoxide::iter::EnumConfig;

let config = EnumConfig {
    batch_size: 64,
    buffer_size: 4096,
};

let mut dkey_iter = object.enumerate_dkeys_with_config(config)?;

for dkey_result in dkey_iter {
    let dkey = dkey_result?;
    println!("Found dkey: {:?}", dkey.as_bytes());
}
```

## High-Level Facade API

For common use cases, the facade API reduces boilerplate significantly:

```rust
use daoxide::prelude::*;

fn main() -> Result<()> {
    // Connect with minimal setup
    let client = DaosClient::builder()
        .pool_label("mypool")
        .container_label("mycontainer")
        .build()?;
    
    // Allocate and use an object
    let oid = client.alloc_oid(
        ObjectType::KvHashed,
        ObjectClass::UNKNOWN,
        ObjectClassHints::NONE,
    )?;
    
    // Simple KV operations
    client.put(oid, b"dkey", b"akey", b"hello")?;
    let mut buffer = vec![0u8; 1024];
    client.get(oid, b"dkey", b"akey", &mut buffer)?;
    
    // Or use transactions
    client.put_tx(oid, Tx::none(), b"dkey", b"akey", b"atomic value")?;
    
    // Access lower-level APIs for advanced use
    let pool = client.pool();
    let container = client.container()?;
    let object = client.object_builder()
        .object_type(ObjectType::Array)
        .open(oid, ObjectOpenMode::ReadWrite)?;
    
    Ok(())
}
```

## OIT (Object Instance Tracking)

### Current Status

OIT functionality is **not yet implemented** in daoxide. The `daos_oit_*` FFI
functions are not exported by `daos-rs`.

**Workaround**: Use object enumeration via [`crate::iter`] to iterate objects.

### Planned API (when implemented)

```rust
// Future API (not yet available)
use daoxide::oit::Oit;

let oit = Oit::open(container, epoch)?;
for oid_result in oit.list()? {
    let oid = oid_result?;
    // process OID
}
```

## Feature Flags

| Feature | Description |
|---------|-------------|
| `default` | Sync API with tracing and serde |
| `async` | Async runtime support (Tokio) |
| `mock` | Mock testing utilities |
| `tracing` | Tracing instrumentation |
| `serde` | Serialization/deserialization |

## Known Limitations

1. **OIT Wrappers**: Blocked due to missing `daos_oit_*` functions in daos-rs
2. **Async Event Queue**: `daos_progress` is not exposed by daos-rs, limiting true async integration

## Summary

| Pattern | daos-rs | daoxide |
|---------|---------|---------|
| Initialization | Manual `init/fini` | `DaosRuntime::new()` RAII |
| Pool connect | Function call | `PoolBuilder::build()` |
| Handle cleanup | Manual `disconnect` | Automatic `Drop` |
| Error codes | Raw `i32` values | `DaosError` enum |
| Key construction | Manual `daos_key_t` | `DKey::new()`, `AKey::new()` |
| IOD creation | Manual `daos_iod_t` | `IodSingleBuilder`, `IodArrayBuilder` |
| Transaction | Manual `tx_open/commit/close` | `Transaction` with `commit()`/`abort()` |
| Enumeration | Manual anchor handling | Iterator APIs |
