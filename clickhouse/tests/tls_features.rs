//! Tests to verify TLS features compile correctly
//!
//! These tests ensure that the TLS features are properly configured
//! and compile without errors.

use bb8_clickhouse::ClickHouseConnectionManager;

#[test]
fn test_https_url_accepted() {
    // Should accept HTTPS URLs (whether TLS is enabled or not)
    let manager = ClickHouseConnectionManager::new("https://localhost:8443");
    let debug_str = format!("{:?}", manager);
    assert!(debug_str.contains("https://localhost:8443"));
}

#[test]
fn test_https_with_credentials() {
    let manager = ClickHouseConnectionManager::new("https://example.clickhouse.cloud:8443")
        .with_database("mydb")
        .with_user("myuser")
        .with_password("mypass");

    let debug_str = format!("{:?}", manager);
    assert!(debug_str.contains("https://"));
    assert!(debug_str.contains("mydb"));
    assert!(debug_str.contains("myuser"));
    assert!(debug_str.contains("<redacted>"));
}

#[test]
fn test_manager_clone_with_https() {
    let manager1 = ClickHouseConnectionManager::new("https://localhost:8443")
        .with_database("test");

    let manager2 = manager1.clone();

    let debug1 = format!("{:?}", manager1);
    let debug2 = format!("{:?}", manager2);

    assert_eq!(debug1, debug2);
    assert!(debug1.contains("https://"));
}

/// Compile-time verification that TLS features are properly exposed
#[cfg(feature = "rustls-tls")]
#[test]
fn test_rustls_tls_feature_enabled() {
    // If this compiles, rustls-tls feature is working
    let _manager = ClickHouseConnectionManager::new("https://localhost:8443");
}

#[cfg(feature = "native-tls")]
#[test]
fn test_native_tls_feature_enabled() {
    // If this compiles, native-tls feature is working
    let _manager = ClickHouseConnectionManager::new("https://localhost:8443");
}

#[cfg(not(any(feature = "rustls-tls", feature = "native-tls")))]
#[test]
fn test_no_tls_feature_warning() {
    // This test documents that HTTPS URLs can be used without TLS features,
    // but will fail at runtime when connecting
    let _manager = ClickHouseConnectionManager::new("https://localhost:8443");
    println!("Warning: HTTPS URL specified but no TLS feature enabled");
    println!("Enable 'rustls-tls' or 'native-tls' feature to use HTTPS");
}
