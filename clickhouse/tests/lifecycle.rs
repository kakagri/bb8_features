//! Connection lifecycle tests
//!
//! Tests for connection management features like max_lifetime, idle_timeout, and min_idle
//!
//! To run: cargo test --test lifecycle -- --test-threads=1

use bb8::Pool;
use bb8_clickhouse::ClickHouseConnectionManager;
use std::time::Duration;

async fn is_clickhouse_available() -> bool {
    let manager = ClickHouseConnectionManager::new("http://localhost:8123");
    match manager.connect().await {
        Ok(client) => client.query("SELECT 1").execute().await.is_ok(),
        Err(_) => false,
    }
}

#[tokio::test]
async fn test_min_idle_maintained() {
    if !is_clickhouse_available().await {
        eprintln!("ClickHouse not available, skipping test");
        return;
    }

    let manager = ClickHouseConnectionManager::new("http://localhost:8123");
    let pool = Pool::builder()
        .max_size(10)
        .min_idle(Some(3))
        .build(manager)
        .await
        .unwrap();

    // Wait a bit for the pool to stabilize
    tokio::time::sleep(Duration::from_millis(100)).await;

    let state = pool.state();
    assert!(
        state.idle_connections >= 3,
        "Expected at least 3 idle connections, got {}",
        state.idle_connections
    );
}

#[tokio::test]
async fn test_idle_timeout() {
    if !is_clickhouse_available().await {
        eprintln!("ClickHouse not available, skipping test");
        return;
    }

    let manager = ClickHouseConnectionManager::new("http://localhost:8123");
    let pool = Pool::builder()
        .max_size(5)
        .min_idle(Some(0))
        .idle_timeout(Some(Duration::from_secs(1)))
        .reaper_rate(Duration::from_millis(500))
        .build(manager)
        .await
        .unwrap();

    // Create some connections
    let clients: Vec<_> = (0..5)
        .map(|_| pool.get())
        .collect::<futures_util::stream::FuturesUnordered<_>>()
        .collect::<Vec<_>>()
        .await;

    let initial_count = pool.state().connections;
    assert_eq!(initial_count, 5);

    // Release all connections
    drop(clients);

    // Wait for idle timeout to trigger
    tokio::time::sleep(Duration::from_secs(3)).await;

    let final_state = pool.state();
    // Idle connections should have been reaped
    assert!(
        final_state.connections < initial_count,
        "Expected connections to be reaped, had {}, now have {}",
        initial_count,
        final_state.connections
    );
    assert!(final_state.statistics.connections_closed_idle_timeout > 0);
}

#[tokio::test]
async fn test_max_lifetime() {
    if !is_clickhouse_available().await {
        eprintln!("ClickHouse not available, skipping test");
        return;
    }

    let manager = ClickHouseConnectionManager::new("http://localhost:8123");
    let pool = Pool::builder()
        .max_size(2)
        .max_lifetime(Some(Duration::from_secs(1)))
        .reaper_rate(Duration::from_millis(500))
        .build(manager)
        .await
        .unwrap();

    let initial_created = pool.state().statistics.connections_created;

    // Get and release a connection
    {
        let client = pool.get().await.unwrap();
        let _ = client.query("SELECT 1").execute().await;
    }

    // Wait for max lifetime to expire
    tokio::time::sleep(Duration::from_secs(2)).await;

    // Get another connection - old one should have been reaped and new one created
    {
        let client = pool.get().await.unwrap();
        let _ = client.query("SELECT 1").execute().await;
    }

    let stats = pool.state().statistics;
    assert!(
        stats.connections_created > initial_created,
        "Expected new connection to be created after max lifetime"
    );
    assert!(stats.connections_closed_max_lifetime > 0);
}

#[tokio::test]
async fn test_connection_validation_on_checkout() {
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

    // Get a connection - should be validated
    let client = pool.get().await.unwrap();
    let _ = client.query("SELECT 1").execute().await;
    drop(client);

    // Get another connection - should validate the returned one
    let client = pool.get().await.unwrap();
    assert!(client.query("SELECT 1").execute().await.is_ok());
}

#[tokio::test]
async fn test_pool_state_tracking() {
    if !is_clickhouse_available().await {
        eprintln!("ClickHouse not available, skipping test");
        return;
    }

    let manager = ClickHouseConnectionManager::new("http://localhost:8123");
    let pool = Pool::builder()
        .max_size(5)
        .min_idle(Some(2))
        .build(manager)
        .await
        .unwrap();

    tokio::time::sleep(Duration::from_millis(100)).await;

    let initial_state = pool.state();
    println!("Initial state: {:?}", initial_state);

    // Get some connections
    let client1 = pool.get().await.unwrap();
    let client2 = pool.get().await.unwrap();

    let active_state = pool.state();
    assert_eq!(
        active_state.connections - active_state.idle_connections,
        2,
        "Should have 2 active connections"
    );

    drop(client1);
    drop(client2);

    tokio::time::sleep(Duration::from_millis(100)).await;

    let final_state = pool.state();
    assert_eq!(
        final_state.connections, final_state.idle_connections,
        "All connections should be idle"
    );
}

#[tokio::test]
async fn test_reaper_doesnt_close_below_min_idle() {
    if !is_clickhouse_available().await {
        eprintln!("ClickHouse not available, skipping test");
        return;
    }

    let manager = ClickHouseConnectionManager::new("http://localhost:8123");
    let pool = Pool::builder()
        .max_size(10)
        .min_idle(Some(5))
        .idle_timeout(Some(Duration::from_millis(500)))
        .reaper_rate(Duration::from_millis(250))
        .build(manager)
        .await
        .unwrap();

    // Wait for pool to stabilize with min_idle connections
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Create extra connections
    let clients: Vec<_> = (0..10)
        .map(|_| pool.get())
        .collect::<futures_util::stream::FuturesUnordered<_>>()
        .collect::<Vec<_>>()
        .await;

    drop(clients);

    // Wait for reaper to run
    tokio::time::sleep(Duration::from_secs(2)).await;

    let state = pool.state();
    // Should maintain at least min_idle connections
    assert!(
        state.idle_connections >= 5,
        "Expected at least 5 idle connections after reaping, got {}",
        state.idle_connections
    );
}
