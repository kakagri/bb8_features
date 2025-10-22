use bb8::Pool;
use bb8_clickhouse::{clickhouse::Row, ClickHouseConnectionManager};
use serde::{Deserialize, Serialize};

// Example demonstrating INSERT operations through the pool
//
// To run this example:
// 1. Start ClickHouse: docker run -p 8123:8123 clickhouse/clickhouse-server
// 2. Run: cargo run --example insert

#[derive(Row, Serialize, Deserialize, Debug)]
struct TestRow {
    id: u32,
    name: String,
    value: f64,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let manager = ClickHouseConnectionManager::new("http://localhost:8123");

    let pool = Pool::builder().max_size(5).build(manager).await?;

    // Get a client to create the table
    let client = pool.get().await?;

    // Create a test table
    println!("Creating test table...");
    client
        .query("DROP TABLE IF EXISTS test_table")
        .execute()
        .await?;

    client
        .query("CREATE TABLE test_table (id UInt32, name String, value Float64) ENGINE = Memory")
        .execute()
        .await?;

    // Insert data
    println!("Inserting data...");
    let mut insert = client.insert::<TestRow>("test_table").await?;

    for i in 0..10 {
        insert
            .write(&TestRow {
                id: i,
                name: format!("name_{}", i),
                value: i as f64 * 1.5,
            })
            .await?;
    }

    insert.end().await?;
    println!("Inserted 10 rows");

    // Retrieve the data
    println!("Retrieving data...");
    let rows = client
        .query("SELECT ?fields FROM test_table ORDER BY id")
        .fetch_all::<TestRow>()
        .await?;

    println!("Retrieved {} rows:", rows.len());
    for row in rows {
        println!("  {:?}", row);
    }

    // Cleanup
    client.query("DROP TABLE test_table").execute().await?;
    println!("\nTable dropped");

    Ok(())
}
