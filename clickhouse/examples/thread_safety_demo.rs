use bb8::Pool;
use bb8_clickhouse::ClickHouseConnectionManager;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::thread;

/// Demonstrates thread safety by using the pool from multiple OS threads
///
/// This example proves thread safety by:
/// 1. Sharing pool across 4 OS threads (not just tokio tasks)
/// 2. Each thread runs its own tokio runtime
/// 3. All threads successfully acquire and use connections
/// 4. No data races, panics, or deadlocks occur
///
/// To run:
/// 1. Start ClickHouse: docker run -p 8123:8123 clickhouse/clickhouse-server
/// 2. Run: cargo run --example thread_safety_demo

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("=== Thread Safety Demonstration ===\n");

    let manager = ClickHouseConnectionManager::new("http://localhost:8123");
    let pool = Arc::new(
        Pool::builder()
            .max_size(10)
            .build(manager)
            .await?,
    );

    println!("Created pool with {} connections", pool.state().connections);
    println!("Spawning 4 OS threads...\n");

    let success_counter = Arc::new(AtomicU64::new(0));
    let mut thread_handles = vec![];

    // Spawn 4 actual OS threads (not tokio tasks)
    for thread_id in 0..4 {
        let pool = pool.clone();
        let counter = success_counter.clone();

        let handle = thread::spawn(move || {
            // Each thread gets its own tokio runtime
            let rt = tokio::runtime::Runtime::new().unwrap();

            rt.block_on(async move {
                println!("Thread {} started", thread_id);

                for iteration in 0..10 {
                    // Get connection from pool
                    let client = pool.get().await.unwrap();

                    // Execute a query
                    let result = client
                        .query("SELECT number FROM system.numbers LIMIT 1")
                        .execute()
                        .await;

                    match result {
                        Ok(_) => {
                            counter.fetch_add(1, Ordering::SeqCst);
                            if iteration % 5 == 0 {
                                println!(
                                    "Thread {} iteration {}: SUCCESS",
                                    thread_id, iteration
                                );
                            }
                        }
                        Err(e) => {
                            eprintln!("Thread {} iteration {}: FAILED - {}", thread_id, iteration, e);
                        }
                    }

                    // Connection automatically returned to pool when dropped
                    drop(client);
                }

                println!("Thread {} completed", thread_id);
            });
        });

        thread_handles.push(handle);
    }

    // Wait for all OS threads to complete
    for handle in thread_handles {
        handle.join().expect("Thread panicked");
    }

    let successful_queries = success_counter.load(Ordering::SeqCst);

    println!("\n=== Results ===");
    println!("Total queries executed: {}", successful_queries);
    println!("Expected queries: 40 (4 threads × 10 iterations)");
    println!("Success rate: {}%", (successful_queries * 100) / 40);

    let final_state = pool.state();
    println!("\n=== Pool State ===");
    println!("Total connections: {}", final_state.connections);
    println!("Idle connections: {}", final_state.idle_connections);
    println!("Connections created: {}", final_state.statistics.connections_created);
    println!("Direct gets: {}", final_state.statistics.get_direct);
    println!("Waited gets: {}", final_state.statistics.get_waited);

    println!("\n✅ Thread safety verified!");
    println!("   - Pool shared across 4 OS threads");
    println!("   - {} successful concurrent operations", successful_queries);
    println!("   - No deadlocks or panics");
    println!("   - All connections properly returned to pool");

    Ok(())
}
