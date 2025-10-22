//! ClickHouse support for the `bb8` connection pool.
//!
//! # Features
//!
//! - **Async/Await**: Built on tokio for high-performance async operations
//! - **Connection Pooling**: Efficient connection reuse with configurable pool size
//! - **TLS/HTTPS Support**: Connect securely via `rustls-tls` or `native-tls` features
//! - **Thread Safe**: Safe to share across threads and tokio tasks
//! - **Health Checking**: Automatic connection validation
//! - **Statistics**: Comprehensive pool statistics for monitoring
//!
//! # TLS/HTTPS Support
//!
//! To use HTTPS connections, enable one of the TLS features in your `Cargo.toml`:
//!
//! ```toml
//! [dependencies]
//! bb8-clickhouse = { version = "0.1", features = ["rustls-tls"] }
//! ```
//!
//! Available TLS features: `rustls-tls`, `native-tls`, `rustls-tls-aws-lc`,
//! `rustls-tls-ring`, `rustls-tls-webpki-roots`, `rustls-tls-native-roots`
//!
//! # Example (HTTP)
//!
//! ```rust,no_run
//! use bb8_clickhouse::{
//!     bb8,
//!     clickhouse::{Client, Row},
//!     ClickHouseConnectionManager,
//! };
//! use serde::{Deserialize, Serialize};
//!
//! #[derive(Row, Serialize, Deserialize)]
//! struct MyRow {
//!     id: u32,
//!     name: String,
//! }
//!
//! #[tokio::main]
//! async fn main() -> Result<(), Box<dyn std::error::Error>> {
//!     let manager = ClickHouseConnectionManager::new("http://localhost:8123");
//!     let pool = bb8::Pool::builder()
//!         .max_size(15)
//!         .build(manager)
//!         .await?;
//!
//!     let client = pool.get().await?;
//!     let rows = client
//!         .query("SELECT ?fields FROM some_table")
//!         .fetch_all::<MyRow>()
//!         .await?;
//!
//!     println!("Retrieved {} rows", rows.len());
//!     Ok(())
//! }
//! ```
//!
//! # Example (HTTPS)
//!
//! ```rust,no_run
//! use bb8_clickhouse::{bb8, ClickHouseConnectionManager};
//!
//! #[tokio::main]
//! async fn main() -> Result<(), Box<dyn std::error::Error>> {
//!     let manager = ClickHouseConnectionManager::new("https://your-instance.clickhouse.cloud:8443")
//!         .with_user("your-user")
//!         .with_password("your-password");
//!
//!     let pool = bb8::Pool::builder().build(manager).await?;
//!     // Use the pool...
//!     Ok(())
//! }
//! ```
#![allow(clippy::needless_doctest_main)]
#![deny(missing_docs, missing_debug_implementations)]

pub use bb8;
pub use clickhouse;

use clickhouse::{Client, error::Error};
use std::fmt;

/// A `bb8::ManageConnection` for `clickhouse::Client`.
///
/// This connection manager creates and manages ClickHouse client instances.
/// Note that the ClickHouse client itself maintains an internal HTTP connection
/// pool, so this manager primarily provides lifecycle management, connection
/// limits, and health checking.
#[derive(Clone)]
pub struct ClickHouseConnectionManager {
    url: String,
    database: Option<String>,
    user: Option<String>,
    password: Option<String>,
    compression: Option<clickhouse::Compression>,
}

impl ClickHouseConnectionManager {
    /// Create a new `ClickHouseConnectionManager` with the specified URL.
    ///
    /// # Example
    ///
    /// ```rust
    /// use bb8_clickhouse::ClickHouseConnectionManager;
    ///
    /// let manager = ClickHouseConnectionManager::new("http://localhost:8123");
    /// ```
    pub fn new(url: impl Into<String>) -> Self {
        Self {
            url: url.into(),
            database: None,
            user: None,
            password: None,
            compression: None,
        }
    }

    /// Set the database name.
    ///
    /// # Example
    ///
    /// ```rust
    /// use bb8_clickhouse::ClickHouseConnectionManager;
    ///
    /// let manager = ClickHouseConnectionManager::new("http://localhost:8123")
    ///     .with_database("my_database");
    /// ```
    pub fn with_database(mut self, database: impl Into<String>) -> Self {
        self.database = Some(database.into());
        self
    }

    /// Set the user name for authentication.
    ///
    /// # Example
    ///
    /// ```rust
    /// use bb8_clickhouse::ClickHouseConnectionManager;
    ///
    /// let manager = ClickHouseConnectionManager::new("http://localhost:8123")
    ///     .with_user("default");
    /// ```
    pub fn with_user(mut self, user: impl Into<String>) -> Self {
        self.user = Some(user.into());
        self
    }

    /// Set the password for authentication.
    ///
    /// # Example
    ///
    /// ```rust
    /// use bb8_clickhouse::ClickHouseConnectionManager;
    ///
    /// let manager = ClickHouseConnectionManager::new("http://localhost:8123")
    ///     .with_password("secret");
    /// ```
    pub fn with_password(mut self, password: impl Into<String>) -> Self {
        self.password = Some(password.into());
        self
    }

    /// Set the compression mode.
    ///
    /// # Example
    ///
    /// ```rust
    /// use bb8_clickhouse::{ClickHouseConnectionManager, clickhouse::Compression};
    ///
    /// let manager = ClickHouseConnectionManager::new("http://localhost:8123")
    ///     .with_compression(Compression::None);
    /// ```
    pub fn with_compression(mut self, compression: clickhouse::Compression) -> Self {
        self.compression = Some(compression);
        self
    }

    /// Create a configured ClickHouse client from this manager's settings.
    fn create_client(&self) -> Client {
        let mut client = Client::default().with_url(&self.url);

        if let Some(ref database) = self.database {
            client = client.with_database(database);
        }

        if let Some(ref user) = self.user {
            client = client.with_user(user);
        }

        if let Some(ref password) = self.password {
            client = client.with_password(password);
        }

        if let Some(compression) = self.compression {
            client = client.with_compression(compression);
        }

        client
    }
}

impl bb8::ManageConnection for ClickHouseConnectionManager {
    type Connection = Client;
    type Error = Error;

    async fn connect(&self) -> Result<Self::Connection, Self::Error> {
        Ok(self.create_client())
    }

    async fn is_valid(&self, conn: &mut Self::Connection) -> Result<(), Self::Error> {
        // Execute a simple query to verify the connection is working
        conn.query("SELECT 1").execute().await
    }

    fn has_broken(&self, _conn: &mut Self::Connection) -> bool {
        // HTTP connections don't have a persistent broken state
        // Any connection issues will be detected during is_valid()
        false
    }
}

impl fmt::Debug for ClickHouseConnectionManager {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.debug_struct("ClickHouseConnectionManager")
            .field("url", &self.url)
            .field("database", &self.database)
            .field("user", &self.user)
            .field("password", &"<redacted>")
            .field("compression", &self.compression)
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_manager_creation() {
        let manager = ClickHouseConnectionManager::new("http://localhost:8123");
        assert_eq!(manager.url, "http://localhost:8123");
        assert!(manager.database.is_none());
        assert!(manager.user.is_none());
        assert!(manager.password.is_none());
    }

    #[test]
    fn test_manager_builder() {
        let manager = ClickHouseConnectionManager::new("http://localhost:8123")
            .with_database("test_db")
            .with_user("admin")
            .with_password("secret");

        assert_eq!(manager.url, "http://localhost:8123");
        assert_eq!(manager.database.as_deref(), Some("test_db"));
        assert_eq!(manager.user.as_deref(), Some("admin"));
        assert_eq!(manager.password.as_deref(), Some("secret"));
    }

    #[test]
    fn test_manager_debug() {
        let manager = ClickHouseConnectionManager::new("http://localhost:8123")
            .with_password("secret");

        let debug_str = format!("{:?}", manager);
        assert!(debug_str.contains("ClickHouseConnectionManager"));
        assert!(debug_str.contains("<redacted>"));
        assert!(!debug_str.contains("secret"));
    }
}
