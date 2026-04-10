# daoxide

> Caution: This repo was created using vibe coding

High-performance Rust library for accessing DAOS (Distributed Asynchronous Object Storage).

This crate provides a safe wrapper around [daos-rs](https://github.com/ustc-leofs/daos-rs) bindings, with an ergonomic Rust-style API.

## Overview

daoxide wraps the low-level DAOS C APIs into safe Rust interfaces:

- **RAII handle management**: Pool, Container, and Object handles are automatically released on `Drop`
- **Type safety**: strongly typed keys, object IDs, and flags to prevent runtime errors
- **Builder pattern**: fluent chained calls instead of complex initialization code
- **Transaction support**: marker-based `Tx` types ensure correct transaction handling
- **Unified error handling**: `DaosError` enum instead of raw integer error codes

## Feature Flags

| Feature | Enabled by Default | Description |
|---------|--------------------|-------------|
| `tracing` | Yes | Tracing observability support |
| `serde` | Yes | Serialization/deserialization support |
| `async` | No | Async runtime support (Tokio) |
| `mock` | No | Mock testing utilities |

```toml
[dependencies]
daoxide = { version = "0.1", features = ["async"] }
```

## Quick Start

### High-Level API (Facade)

Use `DaosClient::builder()` to connect to DAOS quickly:

```rust
use daoxide::prelude::*;

fn main() -> daoxide::Result<()> {
    let client = DaosClient::builder()
        .pool_label("mypool")
        .container_label("mycontainer")
        .build()?;

    let oid = client.alloc_oid(
        ObjectType::KvHashed,
        ObjectClass::UNKNOWN,
        ObjectClassHints::NONE,
    )?;

    client.put(oid, b"dkey1", b"akey1", b"hello world")?;

    let mut buffer = vec![0u8; 1024];
    client.get(oid, b"dkey1", b"akey1", &mut buffer[..11])?;

    println!("Retrieved: {:?}", String::from_utf8_lossy(&buffer[..11]));
    Ok(())
}
```

### Mid-Level API (Pool/Container/Object)

Use `PoolBuilder` and `Container` APIs directly:

```rust
use daoxide::pool::{PoolBuilder, flags::POOL_CONNECT_NONE};
use daoxide::container::{ContainerOpen, flags::CONT_OPEN_RW};

fn main() -> daoxide::Result<()> {
    let runtime = daoxide::runtime::DaosRuntime::new()?;

    let pool = PoolBuilder::new()
        .label("mypool")
        .system("daos_server")
        .flags(POOL_CONNECT_NONE)
        .build()?;

    let container = pool.create_container("testcontainer")?;
    let info = container.query()?;
    println!("Container UUID: {:?}", info.uuid);

    Ok(())
}
```

### Low-Level API (Object I/O)

Use `DKey`/`AKey`/`IoBuffer` for key-value operations:

```rust
use daoxide::io::{DKey, AKey, IoBuffer, Iod, IodSingleBuilder, Sgl};
use daoxide::prelude::*;

let object = client.object_builder()
    .open(oid, ObjectOpenMode::ReadWrite)?;

let dkey = DKey::new(b"my_dkey")?;
let akey = AKey::new(b"my_akey")?;
let value = IoBuffer::from_vec(b"hello world".to_vec());

let iod = Iod::Single(
    IodSingleBuilder::new(akey.clone())
        .value_len(value.len())
        .build()?,
);
let sgl = Sgl::builder().push(value).build()?;

object.update(&Tx::none(), &dkey, &iod, &sgl)?;
```

## Examples

| Example | File | Description |
|---------|------|-------------|
| Minimal client | `examples/minimal_client.rs` | Facade API demo |
| Object operations | `examples/object_io.rs` | Object read/write and key-value operations |
| Pool/container management | `examples/pool_container.rs` | Pool/Container lifecycle |

Run examples:

```bash
cargo run --example minimal_client
cargo run --example object_io
cargo run --example pool_container
```

## Documentation

| Document | Contents |
|----------|----------|
| [docs/MIGRATION.md](docs/MIGRATION.md) | Complete guide for migrating from daos-rs to daoxide |
| [RELEASE.md](RELEASE.md) | Release strategy, versioning, and quality gates |

## Quality Check Commands

```bash
# Code formatting check
cargo fmt --all -- --check

# Clippy check
cargo clippy --workspace --all-targets --all-features -- -D warnings

# Tests (must use --test-threads=1)
cargo test --workspace --all-features -- --test-threads=1

# Documentation build
cargo doc --workspace --all-features --no-deps

# MSRV verification
cargo +1.85 build --release
```

### Feature Matrix Validation

```bash
# No default features
cargo check --no-default-features

# Check each feature independently
cargo check --features mock
cargo check --features async

# All features
cargo check --all-features
```

## Known Limitations

### 1. OIT (Object Instance Tracking) is unavailable

`daos_oit_*` FFI functions are not exported by daos-rs, so `crate::oit` remains a stub.

**Workaround**: use object enumeration in the `crate::iter` module.

### 2. Async event queue is limited

`daos_progress` is not exposed in daos-rs. The `async` feature currently provides a `spawn_blocking` wrapper rather than native async progress.

### 3. Parallel testing caveat

`test_runtime_init_and_fini` may fail under parallel test execution because DAOS uses global state (`RUNTIME_REFCOUNT`).

**Solution**: run tests with `--test-threads=1`.

```bash
cargo test --workspace --all-features -- --test-threads=1
```

This is a pre-existing test infrastructure issue and does not affect library correctness.

## Public Module Architecture

```
daoxide
├── error      - Error types
├── facade     - High-level API (DaosClient)
├── runtime    - Runtime management
├── pool       - Pool operations
├── container  - Container operations
├── object     - Object operations
├── tx         - Transaction management
├── io         - I/O operations (DKey/AKey/IoBuffer)
├── query      - Query operations
├── iter       - Iterator utilities
├── oit        - Object instance tracking (currently unavailable)
└── prelude    - Common exports
```

## MSRV (Minimum Supported Rust Version)

- **MSRV**: Rust 1.85
- **Edition**: 2024

## License

GPL-3.0-only
