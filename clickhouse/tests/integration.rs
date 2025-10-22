//! Integration tests requiring a running ClickHouse server
//!
//! To run these tests:
//! 1. Start ClickHouse: docker run -p 8123:8123 clickhouse/clickhouse-server
//! 2. Run: cargo test --test integration -- --test-threads=1

use bb8::Pool;
use bb8_clickhouse::{clickhouse::Row, ClickHouseConnectionManager};
use serde::{Deserialize, Serialize};

#[derive(Row, Serialize, Deserialize, Debug, PartialEq)]
struct TestRow {
    id: u32,
    name: String,
}

#[derive(Row, Serialize, Deserialize, Debug)]
struct NumberRow {
    number: u64,
}

// Helper to check if ClickHouse is available
async fn is_clickhouse_available() -> bool {
    let manager = ClickHouseConnectionManager::new("http://localhost:8123");
    match manager.connect().await {
        Ok(client) => {
            // Try a simple query
            client.query("SELECT 1").execute().await.is_ok()
        }
        Err(_) => false,
    }
}

#[tokio::test]
async fn test_connection_pool_basic() {
    if !is_clickhouse_available().await {
        eprintln!("ClickHouse not available, skipping test");
        return;
    }

    let manager = ClickHouseConnectionManager::new("http://localhost:8123");
    let pool = Pool::builder()
        .max_size(5)
        .build(manager)
        .await
        .expect("Failed to create pool");

    let state = pool.state();
    assert!(state.connections > 0);
}

#[tokio::test]
async fn test_simple_query() {
    if !is_clickhouse_available().await {
        eprintln!("ClickHouse not available, skipping test");
        return;
    }

    let manager = ClickHouseConnectionManager::new("http://localhost:8123");
    let pool = Pool::builder().build(manager).await.unwrap();

    let client = pool.get().await.unwrap();
    let rows = client
        .query("SELECT number FROM system.numbers LIMIT 5")
        .fetch_all::<NumberRow>()
        .await
        .unwrap();

    assert_eq!(rows.len(), 5);
    assert_eq!(rows[0].number, 0);
    assert_eq!(rows[4].number, 4);
}

#[tokio::test]
async fn test_connection_reuse() {
    if !is_clickhouse_available().await {
        eprintln!("ClickHouse not available, skipping test");
        return;
    }

    let manager = ClickHouseConnectionManager::new("http://localhost:8123");
    let pool = Pool::builder().max_size(1).build(manager).await.unwrap();

    // Get and return a client
    {
        let client = pool.get().await.unwrap();
        client.query("SELECT 1").execute().await.unwrap();
    }

    // Get another client - should reuse the same one
    {
        let client = pool.get().await.unwrap();
        client.query("SELECT 1").execute().await.unwrap();
    }

    let state = pool.state();
    assert_eq!(state.connections, 1);
    assert_eq!(state.statistics.connections_created, 1);
}

#[tokio::test]
async fn test_concurrent_queries() {
    if !is_clickhouse_available().await {
        eprintln!("ClickHouse not available, skipping test");
        return;
    }

    let manager = ClickHouseConnectionManager::new("http://localhost:8123");
    let pool = Pool::builder().max_size(5).build(manager).await.unwrap();

    let mut handles = vec![];
    for _ in 0..10 {
        let pool = pool.clone();
        handles.push(tokio::spawn(async move {
            let client = pool.get().await.unwrap();
            client
                .query("SELECT number FROM system.numbers LIMIT 10")
                .fetch_all::<NumberRow>()
                .await
                .unwrap()
        }));
    }

    for handle in handles {
        let rows = handle.await.unwrap();
        assert_eq!(rows.len(), 10);
    }
}

#[tokio::test]
async fn test_health_check() {
    if !is_clickhouse_available().await {
        eprintln!("ClickHouse not available, skipping test");
        return;
    }

    let manager = ClickHouseConnectionManager::new("http://localhost:8123");
    let pool = Pool::builder()
        .test_on_check_out(true)
        .build(manager)
        .await
        .unwrap();

    // Health check happens automatically on checkout
    let client = pool.get().await.unwrap();
    assert!(client.query("SELECT 1").execute().await.is_ok());
}

#[tokio::test]
async fn test_insert_and_select() {
    if !is_clickhouse_available().await {
        eprintln!("ClickHouse not available, skipping test");
        return;
    }

    let manager = ClickHouseConnectionManager::new("http://localhost:8123");
    let pool = Pool::builder().build(manager).await.unwrap();

    let client = pool.get().await.unwrap();

    // Create table
    client
        .query("CREATE TABLE IF NOT EXISTS test_pool_table (id UInt32, name String) ENGINE = Memory")
        .execute()
        .await
        .unwrap();

    // Insert data
    let mut insert = client.insert("test_pool_table").await.unwrap();
    insert
        .write(&TestRow {
            id: 1,
            name: "Alice".to_string(),
        })
        .await
        .unwrap();
    insert
        .write(&TestRow {
            id: 2,
            name: "Bob".to_string(),
        })
        .await
        .unwrap();
    insert.end().await.unwrap();

    // Select data
    let rows = client
        .query("SELECT ?fields FROM test_pool_table ORDER BY id")
        .fetch_all::<TestRow>()
        .await
        .unwrap();

    assert_eq!(rows.len(), 2);
    assert_eq!(
        rows[0],
        TestRow {
            id: 1,
            name: "Alice".to_string()
        }
    );
    assert_eq!(
        rows[1],
        TestRow {
            id: 2,
            name: "Bob".to_string()
        }
    );

    // Cleanup
    client
        .query("DROP TABLE test_pool_table")
        .execute()
        .await
        .unwrap();
}
