use bb8::Pool;
use bb8_clickhouse::{clickhouse::Row, ClickHouseConnectionManager};
use serde::{Deserialize, Serialize};

// Example demonstrating HTTPS/TLS connection to ClickHouse
//
// This example requires the rustls-tls or native-tls feature to be enabled.
//
// To run with rustls-tls:
//   cargo run --example https_connection --features rustls-tls
//
// To run with native-tls:
//   cargo run --example https_connection --features native-tls
//
// For ClickHouse Cloud or other HTTPS endpoints:
//   Set environment variables:
//     CLICKHOUSE_URL=https://your-instance.clickhouse.cloud:8443
//     CLICKHOUSE_USER=default
//     CLICKHOUSE_PASSWORD=your-password

#[derive(Row, Serialize, Deserialize, Debug)]
struct VersionRow {
    version: String,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("=== HTTPS Connection Example ===\n");

    // Get configuration from environment or use defaults
    let url = std::env::var("CLICKHOUSE_URL")
        .unwrap_or_else(|_| "https://localhost:8443".to_string());
    let user = std::env::var("CLICKHOUSE_USER")
        .unwrap_or_else(|_| "default".to_string());
    let password = std::env::var("CLICKHOUSE_PASSWORD").ok();

    println!("Connecting to: {}", url);
    println!("User: {}", user);
    println!();

    // Create connection manager with HTTPS URL
    let mut manager = ClickHouseConnectionManager::new(&url)
        .with_user(&user);

    if let Some(pass) = password {
        manager = manager.with_password(pass);
    }

    println!("Building connection pool...");
    let pool = Pool::builder()
        .max_size(5)
        .build(manager)
        .await?;

    println!("Pool created successfully!\n");

    // Test the connection
    println!("Testing connection...");
    let client = pool.get().await?;

    // Query ClickHouse version
    match client
        .query("SELECT version() as version")
        .fetch_one::<VersionRow>()
        .await
    {
        Ok(row) => {
            println!("✅ Successfully connected to ClickHouse!");
            println!("   Version: {}", row.version);
        }
        Err(e) => {
            eprintln!("❌ Failed to query ClickHouse: {}", e);
            eprintln!("\nTroubleshooting:");
            eprintln!("1. Ensure ClickHouse server is running with HTTPS enabled");
            eprintln!("2. Verify the URL, user, and password are correct");
            eprintln!("3. Check that the TLS feature is enabled (rustls-tls or native-tls)");
            eprintln!("4. For self-signed certificates, consider using native-tls or rustls-tls-native-roots");
            return Err(e.into());
        }
    }

    // Execute a simple query
    println!("\nExecuting test query...");
    client
        .query("SELECT number FROM system.numbers LIMIT 5")
        .execute()
        .await?;

    println!("✅ Query executed successfully");

    drop(client);

    // Show pool statistics
    let state = pool.state();
    println!("\n=== Pool Statistics ===");
    println!("Total connections: {}", state.connections);
    println!("Idle connections: {}", state.idle_connections);
    println!("Connections created: {}", state.statistics.connections_created);

    println!("\n✅ HTTPS connection test completed successfully!");

    Ok(())
}
