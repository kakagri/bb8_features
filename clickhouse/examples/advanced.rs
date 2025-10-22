use bb8::Pool;
use bb8_clickhouse::{clickhouse::Row, ClickHouseConnectionManager};
use futures_util::StreamExt;
use serde::{Deserialize, Serialize};
use std::time::Duration;

// Advanced example showing all pool configuration options and error handling
//
// To run this example:
// 1. Start ClickHouse: docker run -p 8123:8123 clickhouse/clickhouse-server
// 2. Run: cargo run --example advanced

#[derive(Row, Serialize, Deserialize, Debug)]
struct NumberRow {
    number: u64,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let manager = ClickHouseConnectionManager::new("http://localhost:8123")
        .with_database("default")
        .with_user("default");

    println!("Manager: {:?}", manager);

    // Configure all pool options
    let pool = Pool::builder()
        .max_size(10) // Maximum number of clients in the pool
        .min_idle(Some(2)) // Maintain at least 2 idle clients
        .connection_timeout(Duration::from_secs(10)) // Timeout for getting a client
        .idle_timeout(Some(Duration::from_secs(300))) // Close idle clients after 5 minutes
        .max_lifetime(Some(Duration::from_secs(1800))) // Close clients after 30 minutes
        .test_on_check_out(true) // Validate clients before returning them
        .build(manager)
        .await?;

    println!("Pool initialized");
    print_pool_state(&pool);

    // Demonstrate error handling
    println!("\n--- Testing Query Execution ---");
    let client = pool.get().await?;

    match client
        .query("SELECT number FROM system.numbers LIMIT 5")
        .fetch_all::<NumberRow>()
        .await
    {
        Ok(rows) => {
            println!("Successfully retrieved {} rows", rows.len());
            for row in rows {
                println!("  number: {}", row.number);
            }
        }
        Err(e) => {
            eprintln!("Query failed: {}", e);
        }
    }

    drop(client);
    print_pool_state(&pool);

    // Demonstrate pool exhaustion handling
    println!("\n--- Testing Pool Exhaustion ---");
    let clients: Vec<_> = (0..10)
        .map(|_| pool.get())
        .collect::<futures_util::stream::FuturesUnordered<_>>()
        .collect::<Vec<_>>()
        .await;

    println!("Acquired {} clients", clients.len());
    print_pool_state(&pool);

    // Try to get another client (should wait or timeout)
    println!("Trying to get one more client (will timeout)...");
    match tokio::time::timeout(Duration::from_millis(100), pool.get()).await {
        Ok(Ok(_)) => println!("Unexpectedly got a client!"),
        Ok(Err(e)) => println!("Pool error: {}", e),
        Err(_) => println!("Timeout as expected"),
    }

    // Release all clients
    drop(clients);

    // Wait a bit for the pool to stabilize
    tokio::time::sleep(Duration::from_millis(100)).await;
    print_pool_state(&pool);

    println!("\n--- Final Statistics ---");
    print_statistics(&pool);

    Ok(())
}

fn print_pool_state(pool: &Pool<ClickHouseConnectionManager>) {
    let state = pool.state();
    println!(
        "Pool state: {} total, {} idle",
        state.connections, state.idle_connections
    );
}

fn print_statistics(pool: &Pool<ClickHouseConnectionManager>) {
    let stats = pool.state().statistics;
    println!("  Connections created: {}", stats.connections_created);
    println!("  Gets (direct): {}", stats.get_direct);
    println!("  Gets (waited): {}", stats.get_waited);
    println!("  Gets (timed out): {}", stats.get_timed_out);
    println!("  Wait time: {:?}", stats.get_wait_time);
    println!(
        "  Connections closed (broken): {}",
        stats.connections_closed_broken
    );
    println!(
        "  Connections closed (invalid): {}",
        stats.connections_closed_invalid
    );
    println!(
        "  Connections closed (max lifetime): {}",
        stats.connections_closed_max_lifetime
    );
    println!(
        "  Connections closed (idle timeout): {}",
        stats.connections_closed_idle_timeout
    );
}
