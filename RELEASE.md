# Release Policy

This document defines the release governance, stability guarantees, and compatibility commitments for `daoxide`.

---

## 1. Versioning Policy

### Semantic Versioning (Semver)

`daoxide` follows [Semantic Versioning 2.0.0](https://semver.org/):

- **MAJOR** version (`X.0.0`): Incompatible API changes
- **MINOR** version (`0.X.0`): New backwards-compatible functionality
- **PATCH** version (`0.0.X`): Backwards-compatible bug fixes

### Current Version

```
version = "0.1.0"
```

### Stability Guarantees

| Version Range | Stability | Notes |
|--------------|-----------|-------|
| `0.x` | **Unstable** | API may change in minor versions |

> **Warning**: `daoxide` is currently in early development (`0.x`). The public API does not yet carry stability guarantees. Users should pin to a specific version or use version ranges carefully.

---

## 2. Minimum Supported Rust Version (MSRV)

### Policy

- **MSRV**: Rust 1.85
- MSRV is defined in `Cargo.toml` via `rust-version = "1.85"`
- `clippy.toml` `msrv` value must stay in sync with `Cargo.toml`
- MSRV increases only on **major** version bumps
- MSRV changes require a **major** version bump and **must** be documented in the changelog

### Rationale

- Rust 1.85 is the current MSRV due to:
  - `daos-rs` dependency on FFI bindings requiring modern Rust
  - Edition 2024 features used in the crate
  - Clippy deny-warnings policy requiring recent lint rules

### Verification

```bash
cargo +1.85 build --release
```

### MSRV and CI

The CI matrix explicitly verifies builds against the MSRV using `+1.85` toolchain specification.

---

## 3. Breaking Change Policy

### What Constitutes a Breaking Change

A change is breaking if it causes any of the following:

1. **Removal or renaming** of any `pub` item (functions, types, traits, methods, constants)
2. **Signature changes** to existing `pub` functions or methods
3. **Semantic changes** that alter expected behavior
4. **Removal** of feature flags
5. **Constraint tightening** on trait bounds or generic parameters
6. **Error type changes** that alter `Error` trait implementations
7. **Changes to `#[derive]` macros** on public types that alter layout

### What Is NOT a Breaking Change

1. Adding new `pub` items (additive)
2. Adding new variants to `#[non_exhaustive]` enums
3. Adding new optional parameters with defaults
4. Implementation details (`pub(crate)`, `#[cfg(test)]`)
5. Documentation improvements
6. Bug fixes that correct documented behavior

### Breaking Change Signaling

Breaking changes **must** be:

1. Announced with `[breaking-change]` label in commit messages
2. Documented in `CHANGELOG.md` under the `[breaking]` section
3. Merged only during `major` version bumps

---

## 4. Deprecation Policy

### Deprecation Process

1. **Introduce deprecation** with `#[deprecated(since = "X.Y.Z", note = "...")]`
2. **Keep deprecated item** for at least **one minor version** before removal
3. **Document replacement** in deprecation message
4. **Add `#[allow(deprecated)]`** in examples and tests using deprecated items

### Example

```rust
#[deprecated(since = "0.2.0", note = "Use `new_api()` instead")]
pub fn old_api() -> Result<()> {
    // ... delegates to new_api
    new_api()
}

pub fn new_api() -> Result<()> {
    // ...
}
```

### Deprecation Timeline

| Deprecation introduced | Minimum removal |
|-----------------------|-----------------|
| `0.1.x` | `0.3.0` (2 minor versions) |
| `0.5.x` | `0.7.0` (2 minor versions) |
| `1.x` | `2.x` (1 major version) |

### Currently Deprecated Items

None.

---

## 5. Changelog Policy

### Changelog Location

`CHANGELOG.md` in the repository root.

### Format

Follows [Keep a Changelog](https://keepachangelog.com/en/1.0.0/) format:

```markdown
# Changelog

## [MAJOR.MINOR.PATCH] - YYYY-MM-DD

### Added
- New public API items

### Changed
- Changes to existing functionality

### Deprecated
- Items marked for future removal

### Removed
- Removed functionality (was previously deprecated)

### Fixed
- Bug fixes

### Security
- Security-related fixes
```

### Requirements

1. **Every release** must have a corresponding changelog entry
2. **Breaking changes** must be clearly marked under `[breaking]` subsection
3. **Contributors** should add entries for their changes
4. **Compare links** should be generated between releases using GitHub compare

### Unreleased Changes

Use `## [Unreleased]` header for changes not yet released:

```markdown
## [Unreleased]

### Added
- Feature X (PR #123)
```

---

## 6. Feature Compatibility Policy

### Feature Flag Matrix

| Feature | Default | Description | MSRV Impact |
|---------|---------|-------------|------------|
| `default` | Yes | Sync API with `tracing` and `serde` | None |
| `async` | No | Async runtime support (Tokio) | +Tokio MSRV |
| `mock` | No | Mock testing utilities | None |
| `tracing` | Yes* | Tracing instrumentation | None |
| `serde` | Yes* | Serialization/deserialization | +Serde MSRV |

\* Default feature; enabled via `default = ["tracing", "serde"]`

### Feature Stability

- **Stable features**: `default`, `tracing`, `serde` - considered stable
- **Unstable features**: `async`, `mock` - may have API changes

### Feature Transitions

| Transition | Notice Required |
|------------|-----------------|
| Add new optional feature | 1 minor version |
| Remove existing feature | 1 major version (breaking) |
| Promote unstable to stable | 1 minor version |

### Feature Dependencies

```toml
# Cargo.toml feature definition
[features]
default = ["tracing", "serde"]
mock = []
async = ["dep:tokio"]
tracing = ["dep:tracing"]
serde = ["dep:serde", "dep:serde_json"]
```

### Testing All Feature Combinations

```bash
# Test default features
cargo test --workspace

# Test all features
cargo test --workspace --all-features

# Test no default features
cargo test --workspace --no-default-features

# Test individual features
cargo test --features async
cargo test --features mock
```

---

## 7. API Stability Guarantees

### Stable vs Unstable API

| Category | Stability | Notes |
|----------|-----------|-------|
| `pub` items in `src/` | Stable | Follows semver |
| `pub(crate)` items | Unstable | Internal use only |
| `unsafe_inner` module | Unstable | FFI boundary isolation |

### Exposing Internal Bindings

- `daos-rs` git dependency is **never** re-exported
- All FFI calls go through `unsafe_inner` module
- Public API exposes only safe Rust abstractions

### No Stability Guarantees

> **Note**: During `0.x` development, even `pub` items do not carry hard stability guarantees. The team commits to maintaining a consistent API style and avoiding unnecessary churn, but breaking changes may occur with adequate notice.

---

## 8. Release Readiness Checklist

### Pre-Release Gates

- [ ] `cargo fmt --all -- --check` passes
- [ ] `cargo clippy --workspace --all-targets --all-features -- -D warnings` passes
- [ ] `cargo test --workspace --all-features` passes
- [ ] `cargo doc --workspace --all-features --no-deps` passes (no warnings)
- [ ] MSRV verified: `cargo +1.85 build --release` passes
- [ ] `CHANGELOG.md` updated with all changes since last release
- [ ] `RELEASE.md` reviewed if policy changes were made
- [ ] No `TODO` or `FIXME` left in public API documentation
- [ ] Examples compile and run (if applicable)

### Release Process

1. Create release branch: `release/vX.Y.Z`
2. Update version in `Cargo.toml`
3. Add changelog entry
4. Run full verification matrix
5. Tag and publish

---

## 9. Known Limitations (Release Blockers)

### External Dependency: `daos-rs`

- **Issue**: OIT (Object Instance Tracking) wrappers blocked because `daos_oit_*` FFI functions are not exported by `daos-rs`
- **Impact**: `crate::oit` module remains stubbed
- **Tracking**: See `docs/MIGRATION.md` Known Limitations section
- **Resolution**: Requires `daos-rs` to export OIT functions, or architecture change to allow direct FFI

### Async Event Queue

- **Issue**: `daos_progress` not exposed by `daos-rs`, limiting true async integration
- **Impact**: `async` feature provides `spawn_blocking` wrappers, not native async
- **Tracking**: See `docs/MIGRATION.md` Known Limitations section

---

## 10. Verification Commands

### Full Quality Gate Matrix

```bash
# Format check
cargo fmt --all -- --check

# Clippy with deny warnings
cargo clippy --workspace --all-targets --all-features -- -D warnings

# Tests with all features
cargo test --workspace --all-features

# Documentation build
cargo doc --workspace --all-features --no-deps

# MSRV verification
cargo +1.85 build --release
```

### Local CI Script

```bash
# Full CI run
./scripts/ci.sh

# Fast CI (skip doc build)
./scripts/ci.sh --fast

# Verbose output
./scripts/ci.sh --verbose
```

---

## 11. Compatibility Notes

### Rust Edition

- **Edition**: 2024
- All source code uses Rust 2024 edition idioms
- Edition upgrades require major version bump

### Platform Support

- **Tier 1**: Linux (x86_64) - full support
- **Tier 2**: Linux (aarch64) - expected to work
- **Tier 3**: Other platforms - may work, not tested in CI

### DAOS Compatibility

- **Tested against**: DAOS main branch (via `daos-rs`)
- **Minimum DAOS version**: Not explicitly tested; follows `daos-rs` requirements
