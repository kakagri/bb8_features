use bb8::Pool;
use bb8_clickhouse::{clickhouse::Row, ClickHouseConnectionManager};
use futures_util::future::join_all;
use serde::{Deserialize, Serialize};

// Example demonstrating concurrent query execution through the pool
//
// To run this example:
// 1. Start ClickHouse: docker run -p 8123:8123 clickhouse/clickhouse-server
// 2. Run: cargo run --example concurrent

#[derive(Row, Serialize, Deserialize, Debug)]
struct SumRow {
    sum: u64,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let manager = ClickHouseConnectionManager::new("http://localhost:8123");

    let pool = Pool::builder()
        .max_size(10)
        .connection_timeout(std::time::Duration::from_secs(5))
        .build(manager)
        .await?;

    println!("Spawning 50 concurrent tasks...");

    let mut handles = vec![];

    for i in 0..50 {
        let pool = pool.clone();
        handles.push(tokio::spawn(async move {
            let client = pool.get().await?;

            let result = client
                .query("SELECT sum(number) as sum FROM system.numbers LIMIT 1000")
                .fetch_one::<SumRow>()
                .await?;

            println!("Task {}: sum = {}", i, result.sum);

            Ok::<_, Box<dyn std::error::Error + Send + Sync>>(result.sum)
        }));
    }

    let results = join_all(handles).await;

    let successful = results.iter().filter(|r| r.is_ok()).count();
    println!("\nCompleted {} tasks successfully", successful);

    let stats = pool.state().statistics;
    println!("\nPool Statistics:");
    println!("  Connections created: {}", stats.connections_created);
    println!("  Gets (direct): {}", stats.get_direct);
    println!("  Gets (waited): {}", stats.get_waited);
    println!("  Gets (timed out): {}", stats.get_timed_out);
    println!("  Total wait time: {:?}", stats.get_wait_time);

    Ok(())
}
