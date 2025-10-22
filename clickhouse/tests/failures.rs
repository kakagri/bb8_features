//! Failure scenario tests
//!
//! Tests for error handling, network failures, and invalid connections
//!
//! To run: cargo test --test failures -- --test-threads=1

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
async fn test_invalid_url() {
    let manager = ClickHouseConnectionManager::new("http://invalid-host-that-does-not-exist:8123");

    let result = Pool::builder()
        .max_size(1)
        .connection_timeout(Duration::from_secs(2))
        .build(manager)
        .await;

    // Should fail to connect to invalid host within timeout
    assert!(result.is_err() || {
        // If build succeeds (build doesn't connect), get should fail
        if let Ok(pool) = result {
            pool.get().await.is_err()
        } else {
            true
        }
    });
}

#[tokio::test]
async fn test_connection_timeout() {
    let manager = ClickHouseConnectionManager::new("http://localhost:8123");
    let pool = Pool::builder()
        .max_size(1)
        .connection_timeout(Duration::from_millis(50))
        .build_unchecked(manager);

    // Acquire the only connection
    let _client = pool.get().await.unwrap();

    // Try to get another - should timeout
    let result = pool.get().await;
    assert!(matches!(result, Err(bb8::RunError::TimedOut)));

    let stats = pool.state().statistics;
    assert_eq!(stats.get_timed_out, 1);
}

#[tokio::test]
async fn test_pool_statistics_on_errors() {
    if !is_clickhouse_available().await {
        eprintln!("ClickHouse not available, skipping test");
        return;
    }

    let manager = ClickHouseConnectionManager::new("http://localhost:8123");
    let pool = Pool::builder()
        .max_size(2)
        .connection_timeout(Duration::from_millis(100))
        .build(manager)
        .await
        .unwrap();

    // Exhaust pool
    let _c1 = pool.get().await.unwrap();
    let _c2 = pool.get().await.unwrap();

    // This should timeout
    let result = pool.get().await;
    assert!(result.is_err());

    let stats = pool.state().statistics;
    assert_eq!(stats.get_timed_out, 1);
}

#[tokio::test]
async fn test_error_recovery() {
    if !is_clickhouse_available().await {
        eprintln!("ClickHouse not available, skipping test");
        return;
    }

    let manager = ClickHouseConnectionManager::new("http://localhost:8123");
    let pool = Pool::builder()
        .max_size(2)
        .connection_timeout(Duration::from_millis(100))
        .build(manager)
        .await
        .unwrap();

    // Cause a timeout
    let c1 = pool.get().await.unwrap();
    let c2 = pool.get().await.unwrap();
    let _ = pool.get().await; // This times out

    // Release connections
    drop(c1);
    drop(c2);

    // Should be able to get connections again
    let c3 = pool.get().await.unwrap();
    assert!(c3.query("SELECT 1").execute().await.is_ok());
}

#[tokio::test]
async fn test_invalid_query_doesnt_break_connection() {
    if !is_clickhouse_available().await {
        eprintln!("ClickHouse not available, skipping test");
        return;
    }

    let manager = ClickHouseConnectionManager::new("http://localhost:8123");
    let pool = Pool::builder().build(manager).await.unwrap();

    let client = pool.get().await.unwrap();

    // Execute an invalid query
    let result = client.query("INVALID SQL QUERY").execute().await;
    assert!(result.is_err());

    // Connection should still work for valid queries
    let result = client.query("SELECT 1").execute().await;
    assert!(result.is_ok());

    drop(client);

    // Pool should still work
    let client2 = pool.get().await.unwrap();
    assert!(client2.query("SELECT 1").execute().await.is_ok());
}

#[tokio::test]
async fn test_health_check_failure_detection() {
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

    // Get a connection and return it
    {
        let client = pool.get().await.unwrap();
        let _ = client.query("SELECT 1").execute().await;
    }

    // Note: With HTTP-based connections, we can't easily simulate a broken connection
    // that would fail health checks, since each request is independent.
    // This test mainly verifies that health checks don't cause issues.

    let client = pool.get().await.unwrap();
    assert!(client.query("SELECT 1").execute().await.is_ok());
}

#[tokio::test]
async fn test_multiple_concurrent_timeouts() {
    if !is_clickhouse_available().await {
        eprintln!("ClickHouse not available, skipping test");
        return;
    }

    let manager = ClickHouseConnectionManager::new("http://localhost:8123");
    let pool = Pool::builder()
        .max_size(1)
        .connection_timeout(Duration::from_millis(50))
        .build(manager)
        .await
        .unwrap();

    // Hold the only connection
    let _client = pool.get().await.unwrap();

    // Try to get multiple connections concurrently - all should timeout
    let mut handles = vec![];
    for _ in 0..5 {
        let pool = pool.clone();
        handles.push(tokio::spawn(async move { pool.get().await }));
    }

    let results = futures_util::future::join_all(handles).await;

    let timeout_count = results
        .iter()
        .filter(|r| {
            if let Ok(Err(bb8::RunError::TimedOut)) = r {
                true
            } else {
                false
            }
        })
        .count();

    assert!(timeout_count > 0, "Expected some timeouts");

    let stats = pool.state().statistics;
    assert!(stats.get_timed_out > 0);
}

#[tokio::test]
async fn test_statistics_tracking_comprehensive() {
    if !is_clickhouse_available().await {
        eprintln!("ClickHouse not available, skipping test");
        return;
    }

    let manager = ClickHouseConnectionManager::new("http://localhost:8123");
    let pool = Pool::builder()
        .max_size(2)
        .min_idle(Some(1))
        .test_on_check_out(true)
        .build(manager)
        .await
        .unwrap();

    let initial_stats = pool.state().statistics;
    println!("Initial stats: {:?}", initial_stats);

    // Perform some operations
    for _ in 0..5 {
        let client = pool.get().await.unwrap();
        let _ = client.query("SELECT 1").execute().await;
        drop(client);
    }

    let final_stats = pool.state().statistics;
    println!("Final stats: {:?}", final_stats);

    // Verify statistics were updated
    assert!(
        final_stats.connections_created >= initial_stats.connections_created,
        "Connections created should not decrease"
    );
    assert!(
        final_stats.get_direct + final_stats.get_waited
            >= initial_stats.get_direct + initial_stats.get_waited + 5,
        "Should have recorded at least 5 gets"
    );
}

#[tokio::test]
async fn test_build_failure_with_invalid_server() {
    let manager = ClickHouseConnectionManager::new("http://localhost:9999"); // Wrong port

    // build() tries to establish min_idle connections
    let result = Pool::builder()
        .min_idle(Some(1))
        .connection_timeout(Duration::from_secs(1))
        .build(manager)
        .await;

    // Should fail because it can't connect to establish min_idle
    // Or succeed if retry_connection handles it, but get() should fail
    match result {
        Err(_) => {
            // Expected - failed during initialization
        }
        Ok(pool) => {
            // If build succeeded, get should fail
            let get_result = pool.get().await;
            assert!(get_result.is_err(), "Expected get to fail with invalid server");
        }
    }
}
