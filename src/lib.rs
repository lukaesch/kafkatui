//! KafkaTUI library
//!
//! This library provides the core functionality for KafkaTUI.
//! Modules are exposed for integration testing.

pub mod config;
pub mod connection;
pub mod schema_registry;
pub mod stores;
pub mod traits;
pub mod utils;

// Re-export commonly used types for testing
pub use config::{BrokerProfile, Config};
pub use connection::ConnectionManager;
pub use schema_registry::SchemaRegistryClient;
pub use stores::{KeyringCredentialStore, StdFileStore};
pub use traits::{CredentialStore, FileStore};
