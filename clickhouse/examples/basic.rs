use bb8::Pool;
use bb8_clickhouse::{clickhouse::Row, ClickHouseConnectionManager};
use serde::{Deserialize, Serialize};

// Basic example showing simple SELECT queries through a connection pool
//
// To run this example:
// 1. Start ClickHouse: docker run -p 8123:8123 clickhouse/clickhouse-server
// 2. Run: cargo run --example basic

#[derive(Row, Serialize, Deserialize, Debug)]
struct NumberRow {
    number: u64,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let manager = ClickHouseConnectionManager::new("http://localhost:8123");

    let pool = Pool::builder().max_size(5).build(manager).await?;

    println!("Pool created with {} connections", pool.state().connections);

    // Get a client from the pool
    let client = pool.get().await?;

    // Execute a simple query
    let rows = client
        .query("SELECT number FROM system.numbers LIMIT 10")
        .fetch_all::<NumberRow>()
        .await?;

    println!("Retrieved {} rows:", rows.len());
    for row in rows {
        println!("  number: {}", row.number);
    }

    // The client is automatically returned to the pool when dropped
    drop(client);

    println!("\nPool state: {:?}", pool.state());
    println!("Statistics: {:?}", pool.state().statistics);

    Ok(())
}
