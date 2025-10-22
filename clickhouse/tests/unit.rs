//! Unit tests for ClickHouseConnectionManager

use bb8::Pool;
use bb8_clickhouse::ClickHouseConnectionManager;

#[tokio::test]
async fn test_manager_creation() {
    let manager = ClickHouseConnectionManager::new("http://localhost:8123");
    let debug_str = format!("{:?}", manager);
    assert!(debug_str.contains("ClickHouseConnectionManager"));
    assert!(debug_str.contains("http://localhost:8123"));
}

#[tokio::test]
async fn test_manager_with_database() {
    let manager = ClickHouseConnectionManager::new("http://localhost:8123")
        .with_database("test_db");
    let debug_str = format!("{:?}", manager);
    assert!(debug_str.contains("test_db"));
}

#[tokio::test]
async fn test_manager_with_credentials() {
    let manager = ClickHouseConnectionManager::new("http://localhost:8123")
        .with_user("admin")
        .with_password("secret");

    let debug_str = format!("{:?}", manager);
    assert!(debug_str.contains("admin"));
    assert!(debug_str.contains("<redacted>"));
    assert!(!debug_str.contains("secret")); // Password should not be visible
}

#[tokio::test]
async fn test_manager_chaining() {
    let manager = ClickHouseConnectionManager::new("http://localhost:8123")
        .with_database("mydb")
        .with_user("myuser")
        .with_password("mypass");

    let debug_str = format!("{:?}", manager);
    assert!(debug_str.contains("mydb"));
    assert!(debug_str.contains("myuser"));
}

#[tokio::test]
async fn test_pool_builder() {
    let manager = ClickHouseConnectionManager::new("http://localhost:8123");

    // Pool builder should accept our manager
    let pool = Pool::builder()
        .max_size(5)
        .build_unchecked(manager);

    let state = pool.state();
    assert_eq!(state.connections, 0); // No connections yet with build_unchecked
}

#[tokio::test]
async fn test_manager_clone() {
    let manager1 = ClickHouseConnectionManager::new("http://localhost:8123")
        .with_database("test");

    let manager2 = manager1.clone();

    let debug1 = format!("{:?}", manager1);
    let debug2 = format!("{:?}", manager2);

    assert_eq!(debug1, debug2);
}
