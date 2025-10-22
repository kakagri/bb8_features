//! Explicit thread safety verification tests
//!
//! These tests verify that the connection manager and pool are truly thread safe

use bb8::Pool;
use bb8_clickhouse::ClickHouseConnectionManager;
use std::sync::Arc;
use std::thread;

/// Test that ClickHouseConnectionManager implements Send + Sync
#[test]
fn test_manager_is_send_sync() {
    fn assert_send<T: Send>() {}
    fn assert_sync<T: Sync>() {}

    assert_send::<ClickHouseConnectionManager>();
    assert_sync::<ClickHouseConnectionManager>();
}

/// Test that Pool<ClickHouseConnectionManager> implements Send + Sync
#[test]
fn test_pool_is_send_sync() {
    fn assert_send<T: Send>() {}
    fn assert_sync<T: Sync>() {}

    assert_send::<Pool<ClickHouseConnectionManager>>();
    assert_sync::<Pool<ClickHouseConnectionManager>>();
}

/// Test actual multi-threaded usage with std::thread (not just tokio tasks)
#[tokio::test]
async fn test_actual_multithreaded_usage() {
    let manager = ClickHouseConnectionManager::new("http://localhost:8123");
    let pool = Arc::new(
        Pool::builder()
            .max_size(10)
            .build_unchecked(manager),
    );

    let mut handles = vec![];

    // Spawn actual OS threads
    for i in 0..4 {
        let pool = pool.clone();
        let handle = thread::spawn(move || {
            let rt = tokio::runtime::Runtime::new().unwrap();
            rt.block_on(async move {
                for j in 0..5 {
                    let _client = pool.get().await.unwrap();
                    println!("Thread {} iteration {}", i, j);
                }
            });
        });
        handles.push(handle);
    }

    // Wait for all threads to complete
    for handle in handles {
        handle.join().unwrap();
    }

    println!("All threads completed successfully");
}

/// Test that the manager can be safely cloned across threads
#[test]
fn test_manager_clone_across_threads() {
    let manager = ClickHouseConnectionManager::new("http://localhost:8123")
        .with_database("test");

    let manager_clone = manager.clone();

    let handle = thread::spawn(move || {
        let _m = manager_clone;
        // If this compiles and runs, the manager is Send
    });

    handle.join().unwrap();

    // Original manager is still valid
    let _another_clone = manager.clone();
}

/// Test that Pool can be shared across threads using Arc
#[tokio::test]
async fn test_pool_in_arc_across_threads() {
    let manager = ClickHouseConnectionManager::new("http://localhost:8123");
    let pool = Arc::new(Pool::builder().max_size(5).build_unchecked(manager));

    let pool1 = pool.clone();
    let handle1 = tokio::spawn(async move {
        let _client = pool1.get().await.unwrap();
    });

    let pool2 = pool.clone();
    let handle2 = tokio::spawn(async move {
        let _client = pool2.get().await.unwrap();
    });

    handle1.await.unwrap();
    handle2.await.unwrap();

    // Original pool is still valid
    let _client = pool.get().await.unwrap();
}

/// Verify no data races with concurrent reads of manager state
#[test]
fn test_concurrent_manager_reads() {
    let manager = Arc::new(
        ClickHouseConnectionManager::new("http://localhost:8123")
            .with_database("testdb")
            .with_user("user"),
    );

    let mut handles = vec![];

    for _ in 0..10 {
        let m = manager.clone();
        let handle = thread::spawn(move || {
            // Read manager state (via Debug)
            let debug_str = format!("{:?}", m);
            assert!(debug_str.contains("testdb"));
            assert!(debug_str.contains("user"));
        });
        handles.push(handle);
    }

    for handle in handles {
        handle.join().unwrap();
    }
}

/// Test that connections can be safely moved between tasks
#[tokio::test]
async fn test_connection_move_between_tasks() {
    let manager = ClickHouseConnectionManager::new("http://localhost:8123");
    let pool = Pool::builder().max_size(5).build_unchecked(manager);

    // Get connection in one task
    let client = pool.get_owned().await.unwrap();

    // Move connection to another task
    let handle = tokio::spawn(async move {
        // Use the connection in a different task
        drop(client);
        "completed"
    });

    let result = handle.await.unwrap();
    assert_eq!(result, "completed");
}

/// Stress test: Many threads all trying to access the pool simultaneously
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn test_heavy_concurrent_access() {
    let manager = ClickHouseConnectionManager::new("http://localhost:8123");
    let pool = Arc::new(Pool::builder().max_size(20).build_unchecked(manager));

    let mut handles = vec![];

    // Spawn 100 tasks across 8 worker threads
    for i in 0..100 {
        let pool = pool.clone();
        handles.push(tokio::spawn(async move {
            let _client = pool.get().await.unwrap();
            // Simulate some work
            tokio::time::sleep(tokio::time::Duration::from_micros(100)).await;
            i
        }));
    }

    // Collect all results
    let results: Vec<_> = futures_util::future::join_all(handles)
        .await
        .into_iter()
        .map(|r| r.unwrap())
        .collect();

    assert_eq!(results.len(), 100);
}

/// Test that we can safely send Pool through channels
#[tokio::test]
async fn test_pool_through_channel() {
    let manager = ClickHouseConnectionManager::new("http://localhost:8123");
    let pool = Pool::builder().max_size(5).build_unchecked(manager);

    let (tx, rx) = tokio::sync::oneshot::channel();

    tokio::spawn(async move {
        tx.send(pool).unwrap();
    });

    let received_pool = rx.await.unwrap();
    let _client = received_pool.get().await.unwrap();
}
