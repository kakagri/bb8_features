//! Concurrency tests for the connection pool
//!
//! These tests verify thread safety and concurrent access patterns
//!
//! To run: cargo test --test concurrency -- --test-threads=1

use bb8::Pool;
use bb8_clickhouse::{clickhouse::Row, ClickHouseConnectionManager};
use futures_util::future::join_all;
use serde::{Deserialize, Serialize};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

#[derive(Row, Serialize, Deserialize, Debug)]
struct CountRow {
    count: u64,
}

async fn is_clickhouse_available() -> bool {
    let manager = ClickHouseConnectionManager::new("http://localhost:8123");
    match manager.connect().await {
        Ok(client) => client.query("SELECT 1").execute().await.is_ok(),
        Err(_) => false,
    }
}

#[tokio::test]
async fn test_many_concurrent_gets() {
    if !is_clickhouse_available().await {
        eprintln!("ClickHouse not available, skipping test");
        return;
    }

    let manager = ClickHouseConnectionManager::new("http://localhost:8123");
    let pool = Pool::builder()
        .max_size(10)
        .connection_timeout(Duration::from_secs(30))
        .build(manager)
        .await
        .unwrap();

    let num_tasks = 100;
    let mut handles = vec![];

    for i in 0..num_tasks {
        let pool = pool.clone();
        handles.push(tokio::spawn(async move {
            let client = pool.get().await?;
            let result = client
                .query("SELECT count() as count FROM system.numbers LIMIT 100")
                .fetch_one::<CountRow>()
                .await?;

            Ok::<_, Box<dyn std::error::Error + Send + Sync>>((i, result.count))
        }));
    }

    let results = join_all(handles).await;
    let successful = results.iter().filter(|r| r.is_ok()).count();

    assert_eq!(successful, num_tasks);

    // Verify pool state
    let state = pool.state();
    assert!(state.connections <= 10);
}

#[tokio::test]
async fn test_no_connection_leaks() {
    if !is_clickhouse_available().await {
        eprintln!("ClickHouse not available, skipping test");
        return;
    }

    let manager = ClickHouseConnectionManager::new("http://localhost:8123");
    let pool = Pool::builder().max_size(5).build(manager).await.unwrap();

    // Acquire and release connections many times
    for _ in 0..50 {
        let client = pool.get().await.unwrap();
        let _ = client.query("SELECT 1").execute().await;
        drop(client);
    }

    // Wait for pool to stabilize
    tokio::time::sleep(Duration::from_millis(100)).await;

    let state = pool.state();
    // All connections should be returned
    assert_eq!(state.connections, state.idle_connections);
}

#[tokio::test]
async fn test_statistics_accuracy() {
    if !is_clickhouse_available().await {
        eprintln!("ClickHouse not available, skipping test");
        return;
    }

    let manager = ClickHouseConnectionManager::new("http://localhost:8123");
    let pool = Pool::builder().max_size(2).build(manager).await.unwrap();

    let success_count = Arc::new(AtomicU64::new(0));

    let mut handles = vec![];
    for _ in 0..10 {
        let pool = pool.clone();
        let success_count = success_count.clone();

        handles.push(tokio::spawn(async move {
            let client = pool.get().await.unwrap();
            let _ = client.query("SELECT 1").execute().await;
            success_count.fetch_add(1, Ordering::Relaxed);
            drop(client);
        }));
    }

    join_all(handles).await;

    assert_eq!(success_count.load(Ordering::Relaxed), 10);

    let stats = pool.state().statistics;
    // Some gets should have waited since pool size is 2 but we had 10 concurrent requests
    assert!(stats.get_direct + stats.get_waited >= 10);
}

#[tokio::test]
async fn test_pool_exhaustion_recovery() {
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

    // Exhaust the pool
    let client1 = pool.get().await.unwrap();
    let client2 = pool.get().await.unwrap();

    // Try to get another (should timeout)
    let result = pool.get().await;
    assert!(result.is_err());

    // Release one connection
    drop(client1);

    // Should be able to get a connection now
    let client3 = pool.get().await.unwrap();
    assert!(client3.query("SELECT 1").execute().await.is_ok());

    drop(client2);
    drop(client3);

    // Pool should recover
    let state = pool.state();
    assert_eq!(state.connections, state.idle_connections);
}

#[tokio::test]
async fn test_concurrent_pool_clones() {
    if !is_clickhouse_available().await {
        eprintln!("ClickHouse not available, skipping test");
        return;
    }

    let manager = ClickHouseConnectionManager::new("http://localhost:8123");
    let pool = Pool::builder().max_size(5).build(manager).await.unwrap();

    let mut handles = vec![];

    // Spawn tasks that clone the pool
    for _ in 0..20 {
        let pool_clone = pool.clone();
        handles.push(tokio::spawn(async move {
            let client = pool_clone.get().await.unwrap();
            client.query("SELECT 1").execute().await.unwrap();
        }));
    }

    for handle in handles {
        handle.await.unwrap();
    }

    // All clones share the same pool
    let state = pool.state();
    assert!(state.connections <= 5);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_multi_threaded_access() {
    if !is_clickhouse_available().await {
        eprintln!("ClickHouse not available, skipping test");
        return;
    }

    let manager = ClickHouseConnectionManager::new("http://localhost:8123");
    let pool = Arc::new(Pool::builder().max_size(10).build(manager).await.unwrap());

    let num_threads = 4;
    let queries_per_thread = 25;
    let mut handles = vec![];

    for thread_id in 0..num_threads {
        let pool = pool.clone();
        handles.push(tokio::spawn(async move {
            for i in 0..queries_per_thread {
                let client = pool.get().await.unwrap();
                client
                    .query("SELECT number FROM system.numbers LIMIT 10")
                    .fetch_all::<CountRow>()
                    .await
                    .unwrap();

                if i % 10 == 0 {
                    println!("Thread {} completed {} queries", thread_id, i + 1);
                }
            }
            queries_per_thread
        }));
    }

    let results = join_all(handles).await;
    let total_queries: usize = results.into_iter().map(|r| r.unwrap()).sum();

    assert_eq!(total_queries, num_threads * queries_per_thread);
}
