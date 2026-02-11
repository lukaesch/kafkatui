//! Common trait definitions
//!
//! Defines traits for dependency injection and testing, including
//! FileStore and CredentialStore abstractions. These traits enable
//! mocking and testing of external dependencies.

use anyhow::Result;
use async_trait::async_trait;
use rdkafka::message::BorrowedMessage;
use rdkafka::metadata::Metadata;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

/// Trait for Kafka operations to enable testing with mocks
///
/// Defines the core operations needed for interacting with Kafka.
/// Can be implemented by mock objects for testing.
#[async_trait]
#[allow(dead_code)]
pub trait KafkaClient: Send + Sync {
    /// Fetch metadata for a topic
    async fn fetch_metadata(&self, topic: &str, timeout: Duration) -> Result<Arc<Metadata>>;

    /// Fetch watermarks for a partition
    async fn fetch_watermarks(
        &self,
        topic: &str,
        partition: i32,
        timeout: Duration,
    ) -> Result<(i64, i64)>;

    /// Receive a message
    async fn recv(&self) -> Result<BorrowedMessage<'_>>;

    /// Assign topic partition list
    fn assign(&self, topic: &str, partition: i32, offset: i64) -> Result<()>;
}

/// Trait for Schema Registry operations
///
/// Defines the interface for fetching Avro schemas. Can be implemented
/// by mock objects for testing schema-related functionality.
#[async_trait]
#[allow(dead_code)]
pub trait SchemaRegistry: Send + Sync {
    /// Get schema by ID
    async fn get_schema_by_id(&self, schema_id: i32) -> Result<String>;
}

/// Trait for file system operations
///
/// Abstracts file I/O operations for dependency injection and testing.
/// Production code uses `StdFileStore`, tests can use mock implementations.
#[allow(dead_code)]
pub trait FileStore: Send + Sync {
    /// Write content to a file
    fn write(&self, path: &Path, content: &str) -> Result<()>;

    /// Create directory
    fn create_dir_all(&self, path: &Path) -> Result<()>;
}

/// Trait for credential storage operations
///
/// Abstracts secure credential storage for dependency injection and testing.
/// Production code uses `KeyringCredentialStore`, tests can use mock implementations.
#[allow(dead_code)]
pub trait CredentialStore: Send + Sync {
    /// Save a password for a profile
    fn save_password(&self, profile_name: &str, password: &str) -> Result<()>;

    /// Get a password for a profile
    fn get_password(&self, profile_name: &str) -> Result<String>;

    /// Delete a password for a profile
    fn delete_password(&self, profile_name: &str) -> Result<()>;
}
