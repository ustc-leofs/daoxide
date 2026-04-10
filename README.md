# daoxide

High-performance Rust 库，用于访问 DAOS（Distributed Asynchronous Object Storage）。

本库是对 [daos-rs](https://github.com/ustc-leofs/daos-rs) 绑定的安全封装，提供符合 Rust 习惯的 Ergonomic API。

## 项目简介

daoxide 将 DAOS 的底层 C API 封装为安全的 Rust 接口：

- **RAII 句柄管理**：Pool、Container、Object 句柄在 Drop 时自动释放
- **类型安全**：强类型 Key、Object ID 和标志位，防止运行时错误
- **Builder 模式**：流畅的链式调用替代复杂的初始化代码
- **事务支持**：Tx 标记类型确保正确的事务处理
- **统一错误处理**：DaosError 枚举替代原始整数错误码

## 特性（Feature Flags）

| Feature | 默认启用 | 说明 |
|---------|----------|------|
| `tracing` | 是 |  Tracing 可观测性支持 |
| `serde` | 是 | 序列化/反序列化支持 |
| `async` | 否 | 异步运行时支持（Tokio） |
| `mock` | 否 |  Mock 测试工具 |

```toml
[dependencies]
daoxide = { version = "0.1", features = ["async"] }
```

## 快速开始

### 高层 API（Facade）

使用 `DaosClient::builder()` 快速连接 DAOS：

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

### 中层 API（Pool/Container/Object）

直接使用 PoolBuilder 和 Container API：

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

### 底层 API（Object I/O）

使用 DKey/AKey/IoBuffer 进行键值操作：

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

## 示例程序

| 示例 | 文件 | 说明 |
|------|------|------|
| 最小客户端 | `examples/minimal_client.rs` | Facade API 演示 |
| 对象操作 | `examples/object_io.rs` | 对象读写、键值操作 |
| 池容器管理 | `examples/pool_container.rs` | Pool/Container 生命周期 |

运行示例：

```bash
cargo run --example minimal_client
cargo run --example object_io
cargo run --example pool_container
```

## 文档导航

| 文档 | 内容 |
|------|------|
| [docs/MIGRATION.md](docs/MIGRATION.md) | 从 daos-rs 迁移到 daoxide 的完整指南 |
| [RELEASE.md](RELEASE.md) | 发布策略、版本管理、质量门禁 |

## 质量检查命令

```bash
# 代码格式检查
cargo fmt --all -- --check

# Clippy 检查
cargo clippy --workspace --all-targets --all-features -- -D warnings

# 测试（需使用 --test-threads=1）
cargo test --workspace --all-features -- --test-threads=1

# 文档构建
cargo doc --workspace --all-features --no-deps

# MSRV 验证
cargo +1.85 build --release
```

### Feature 矩阵验证

```bash
# 无默认特性
cargo check --no-default-features

# 各特性独立检查
cargo check --features mock
cargo check --features async

# 全特性
cargo check --all-features
```

## 已知限制

### 1. OIT（Object Instance Tracking）不可用

`daos_oit_*` FFI 函数未在 daos-rs 中导出，`crate::oit` 模块保持 stub 状态。

**临时方案**：使用 `crate::iter` 模块的对象枚举功能替代。

### 2. 异步事件队列受限

`daos_progress` 未在 daos-rs 中暴露，`async` feature 仅提供 `spawn_blocking` 封装，而非原生异步推进。

### 3. 并行测试注意事项

`test_runtime_init_and_fini` 在并行测试时可能失败，原因是 DAOS 底层使用全局状态（`RUNTIME_REFCOUNT`）。

**解决方法**：使用 `--test-threads=1` 运行测试。

```bash
cargo test --workspace --all-features -- --test-threads=1
```

这是预先存在的测试基础设施问题，不影响库本身的正确性。

## 公共模块架构

```
daoxide
├── error      - 错误类型
├── facade     - 高层 API（DaosClient）
├── runtime    - 运行时管理
├── pool       - 池操作
├── container  - 容器操作
├── object     - 对象操作
├── tx         - 事务管理
├── io         - I/O 操作（DKey/AKey/IoBuffer）
├── query      - 查询操作
├── iter       - 迭代器工具
├── oit        - 对象实例追踪（暂不可用）
└── prelude    - 常用类型导出
```

## MSRV（Minimum Supported Rust Version）

- **MSRV**: Rust 1.85
- **Edition**: 2024

## 许可

MIT OR Apache-2.0
