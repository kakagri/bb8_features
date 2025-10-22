use bb8::Pool;
use bb8_clickhouse::{clickhouse::Row, ClickHouseConnectionManager};
use futures_util::stream::{FuturesUnordered, StreamExt};
use serde::{Deserialize, Serialize};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

// Stress test for the connection pool
//
// To run this example:
// 1. Start ClickHouse: docker run -p 8123:8123 clickhouse/clickhouse-server
// 2. Run: cargo run --example stress_test --release

#[derive(Row, Serialize, Deserialize, Debug)]
struct CountRow {
    count: u64,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let num_tasks = 1000;
    let pool_size = 20;

    println!("=== Connection Pool Stress Test ===");
    println!("Tasks: {}", num_tasks);
    println!("Pool size: {}", pool_size);
    println!();

    let manager = ClickHouseConnectionManager::new("http://localhost:8123");

    let pool = Pool::builder()
        .max_size(pool_size)
        .connection_timeout(Duration::from_secs(30))
        .build(manager)
        .await?;

    let success_count = Arc::new(AtomicU64::new(0));
    let error_count = Arc::new(AtomicU64::new(0));

    println!("Starting stress test...");
    let start = Instant::now();

    let futures: FuturesUnordered<_> = (0..num_tasks)
        .map(|i| {
            let pool = pool.clone();
            let success_count = success_count.clone();
            let error_count = error_count.clone();

            tokio::spawn(async move {
                match execute_query(&pool, i).await {
                    Ok(_) => {
                        success_count.fetch_add(1, Ordering::Relaxed);
                    }
                    Err(e) => {
                        eprintln!("Task {} failed: {}", i, e);
                        error_count.fetch_add(1, Ordering::Relaxed);
                    }
                }
            })
        })
        .collect();

    // Wait for all tasks to complete
    futures.collect::<Vec<_>>().await;

    let duration = start.elapsed();

    println!("\n=== Results ===");
    println!("Duration: {:?}", duration);
    println!("Successful queries: {}", success_count.load(Ordering::Relaxed));
    println!("Failed queries: {}", error_count.load(Ordering::Relaxed));
    println!(
        "Queries per second: {:.2}",
        num_tasks as f64 / duration.as_secs_f64()
    );

    println!("\n=== Pool Statistics ===");
    let state = pool.state();
    let stats = state.statistics;

    println!("Total connections: {}", state.connections);
    println!("Idle connections: {}", state.idle_connections);
    println!("Connections created: {}", stats.connections_created);
    println!("Gets (direct): {}", stats.get_direct);
    println!("Gets (waited): {}", stats.get_waited);
    println!("Gets (timed out): {}", stats.get_timed_out);
    println!("Average wait time: {:?}",
        if stats.get_waited > 0 {
            stats.get_wait_time / stats.get_waited as u32
        } else {
            Duration::from_secs(0)
        }
    );

    Ok(())
}

async fn execute_query(
    pool: &Pool<ClickHouseConnectionManager>,
    task_id: u32,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let client = pool.get().await?;

    // Execute a simple query
    let result = client
        .query("SELECT count() as count FROM system.numbers LIMIT 1000")
        .fetch_one::<CountRow>()
        .await?;

    // Simulate some processing
    if task_id % 100 == 0 {
        println!("Task {} completed: count = {}", task_id, result.count);
    }

    Ok(())
}
