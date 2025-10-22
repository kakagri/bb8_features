# bb8-clickhouse

[![Documentation](https://docs.rs/bb8-clickhouse/badge.svg)](https://docs.rs/bb8-clickhouse/)
[![Crates.io](https://img.shields.io/crates/v/bb8-clickhouse.svg)](https://crates.io/crates/bb8-clickhouse)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](../LICENSE)

Full-featured async (tokio-based) connection pool for ClickHouse, built on [bb8](https://github.com/djc/bb8).

## Features

- **Async/Await**: Built on tokio for high-performance async operations
- **Connection Pooling**: Efficient connection reuse with configurable pool size
- **Health Checking**: Automatic connection validation before use
- **Lifecycle Management**: Support for connection timeouts, idle timeouts, and max lifetime
- **Statistics**: Comprehensive pool statistics for monitoring
- **Production Ready**: Thoroughly tested with unit, integration, and concurrency tests

## Installation

Add this to your `Cargo.toml`:

```toml
[dependencies]
bb8-clickhouse = "0.1"
```

### TLS/HTTPS Support

To connect to ClickHouse over HTTPS, enable one of the TLS features:

**Using rustls (recommended for most cases):**
```toml
[dependencies]
bb8-clickhouse = { version = "0.1", features = ["rustls-tls"] }
```

**Using native-tls (for system TLS, self-signed certificates):**
```toml
[dependencies]
bb8-clickhouse = { version = "0.1", features = ["native-tls"] }
```

**Available TLS features:**
- `rustls-tls` - rustls with default crypto (aws-lc) and webpki roots
- `rustls-tls-aws-lc` - rustls with aws-lc crypto provider
- `rustls-tls-ring` - rustls with ring crypto provider
- `rustls-tls-webpki-roots` - Use webpki certificate roots
- `rustls-tls-native-roots` - Use system certificate roots (for self-signed certificates)
- `native-tls` - System native TLS (OpenSSL on Linux, Secure Transport on macOS)

**Choosing a TLS feature:**
- For ClickHouse Cloud or public HTTPS: Use `rustls-tls`
- For self-signed certificates: Use `native-tls` or `rustls-tls-native-roots`
- For maximum compatibility: Use `native-tls`

## Quick Start

### HTTP Connection

```rust
use bb8::Pool;
use bb8_clickhouse::{ClickHouseConnectionManager, clickhouse::Row};
use serde::{Deserialize, Serialize};

#[derive(Row, Serialize, Deserialize)]
struct MyRow {
    id: u32,
    name: String,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Create connection manager
    let manager = ClickHouseConnectionManager::new("http://localhost:8123")
        .with_database("default")
        .with_user("default");

    // Build pool
    let pool = Pool::builder()
        .max_size(15)
        .build(manager)
        .await?;

    // Get a client from the pool
    let client = pool.get().await?;

    // Execute queries
    let rows = client
        .query("SELECT ?fields FROM some_table")
        .fetch_all::<MyRow>()
        .await?;

    println!("Retrieved {} rows", rows.len());
    Ok(())
}
```

### HTTPS Connection

When using HTTPS (requires TLS feature enabled):

```rust
use bb8::Pool;
use bb8_clickhouse::ClickHouseConnectionManager;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Create connection manager with HTTPS URL
    let manager = ClickHouseConnectionManager::new("https://your-instance.clickhouse.cloud:8443")
        .with_database("default")
        .with_user("your-user")
        .with_password("your-password");

    let pool = Pool::builder()
        .max_size(15)
        .build(manager)
        .await?;

    // Use the pool...
    Ok(())
}
```

## Configuration

The connection manager supports all ClickHouse client options:

```rust
let manager = ClickHouseConnectionManager::new("http://localhost:8123")
    .with_database("my_database")
    .with_user("user")
    .with_password("password")
    .with_compression(clickhouse::Compression::Lz4);
```

The pool can be configured with various options:

```rust
use std::time::Duration;

let pool = Pool::builder()
    .max_size(20)                                    // Maximum number of connections
    .min_idle(Some(5))                               // Minimum idle connections to maintain
    .connection_timeout(Duration::from_secs(30))     // Timeout for getting a connection
    .idle_timeout(Some(Duration::from_secs(600)))    // Close idle connections after 10 minutes
    .max_lifetime(Some(Duration::from_secs(1800)))   // Close connections after 30 minutes
    .test_on_check_out(true)                         // Validate connections before use
    .build(manager)
    .await?;
```

## Examples

See the [examples](examples/) directory for more:

- [basic.rs](examples/basic.rs) - Simple SELECT queries
- [concurrent.rs](examples/concurrent.rs) - Concurrent query execution
- [insert.rs](examples/insert.rs) - INSERT operations
- [advanced.rs](examples/advanced.rs) - All pool options and error handling
- [stress_test.rs](examples/stress_test.rs) - Performance testing
- [https_connection.rs](examples/https_connection.rs) - HTTPS/TLS connection example
- [thread_safety_demo.rs](examples/thread_safety_demo.rs) - Thread safety demonstration

Run an example:

```bash
# Start ClickHouse
docker run -p 8123:8123 clickhouse/clickhouse-server

# Run HTTP example
cargo run --example basic

# Run HTTPS example (requires TLS feature)
cargo run --example https_connection --features rustls-tls
```

## Testing

### Unit Tests

```bash
cargo test --lib
```

### Integration Tests

Integration tests require a running ClickHouse server:

```bash
# Start ClickHouse
docker run -p 8123:8123 clickhouse/clickhouse-server

# Run tests
cargo test --test integration
cargo test --test concurrency
cargo test --test lifecycle
cargo test --test failures
```

## Pool Statistics

The pool provides comprehensive statistics for monitoring:

```rust
let state = pool.state();
println!("Total connections: {}", state.connections);
println!("Idle connections: {}", state.idle_connections);

let stats = state.statistics;
println!("Connections created: {}", stats.connections_created);
println!("Direct gets: {}", stats.get_direct);
println!("Waited gets: {}", stats.get_waited);
println!("Timed out gets: {}", stats.get_timed_out);
```

## Thread Safety

**The implementation is fully thread safe** and can be safely shared across threads:

- ✅ Implements `Send + Sync` (compiler verified)
- ✅ Safe to clone and share via `Arc`
- ✅ Safe to use with multi-threaded tokio runtime
- ✅ Tested with 100+ concurrent operations

```rust
// Safe to share across threads
let pool = Arc::new(Pool::builder().build(manager).await?);
let pool_clone = pool.clone();
tokio::spawn(async move {
    let client = pool_clone.get().await?;
    // Use client safely
});
```

## Architecture

Unlike traditional database connections (like PostgreSQL), ClickHouse uses HTTP for communication. The `clickhouse::Client` already maintains an internal HTTP connection pool. This bb8 wrapper adds:

1. **Lifecycle management**: Control connection lifetime and reaping
2. **Connection limits**: Enforce maximum concurrent clients
3. **Health checking**: Verify clients can reach ClickHouse
4. **Statistics**: Track pool usage metrics
5. **Unified interface**: Consistent API across different backends
6. **Thread safety**: Safe concurrent access from multiple threads

## License

Licensed under the MIT license ([LICENSE](../LICENSE)).

## Contributing

Contributions are welcome! Please feel free to submit issues or pull requests.
