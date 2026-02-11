//! Confluent Schema Registry client
//!
//! Handles communication with the Schema Registry to fetch and cache
//! Avro schemas for message deserialization. Implements the Confluent
//! wire format protocol for Avro messages.
//!
//! # Wire Format
//!
//! Confluent Avro messages use a specific wire format:
//! - Byte 0: Magic byte (0x00)
//! - Bytes 1-4: Schema ID (big-endian int32)
//! - Bytes 5+: Avro-encoded payload

use anyhow::Result;
use async_trait::async_trait;
use lru::LruCache;
use reqwest::{Client, StatusCode};
use serde::Deserialize;
use std::num::NonZeroUsize;
use std::sync::Arc;
use tokio::sync::Mutex;

use crate::traits::SchemaRegistry;

/// Schema Registry API response
///
/// Represents the JSON response from the Schema Registry when
/// fetching a schema by ID.
#[derive(Debug, Deserialize)]
pub struct SchemaResponse {
    /// The Avro schema as a JSON string
    pub schema: String,
    /// Schema ID (may be present in some responses)
    #[serde(skip_serializing_if = "Option::is_none")]
    #[allow(dead_code)]
    pub id: Option<i32>,
    /// Schema type (typically "AVRO")
    #[serde(rename = "schemaType")]
    #[allow(dead_code)]
    pub schema_type: Option<String>,
}

/// Client for Confluent Schema Registry
///
/// Provides schema fetching with LRU caching to minimize network requests.
/// Uses basic authentication for Schema Registry access.
pub struct SchemaRegistryClient {
    /// HTTP client for making requests
    client: Client,
    /// Base URL of the Schema Registry
    base_url: String,
    /// LRU cache mapping schema IDs to schema strings
    cache: Arc<Mutex<LruCache<i32, String>>>,
    /// Username for basic authentication
    username: String,
    /// Password for basic authentication
    password: String,
}

impl SchemaRegistryClient {
    /// Creates a new Schema Registry client
    ///
    /// # Arguments
    /// - `base_url` - Base URL of the Schema Registry (e.g., `<https://registry.example.com>`)
    /// - `username` - Username for basic authentication
    /// - `password` - Password for basic authentication
    ///
    /// # Returns
    /// A configured client with TLS verification enabled and a 100-entry cache
    ///
    /// # Errors
    /// Returns an error if the HTTP client cannot be built.
    pub fn new(base_url: String, username: &str, password: &str) -> Result<Self> {
        let client = Client::builder()
            .danger_accept_invalid_certs(false) // Explicitly require valid certificates
            .use_rustls_tls() // Use rustls for TLS
            .build()?;

        let cache = Arc::new(Mutex::new(LruCache::new(NonZeroUsize::new(100).unwrap())));

        Ok(Self {
            client,
            base_url,
            cache,
            username: username.to_string(),
            password: password.to_string(),
        })
    }

    /// Fetches an Avro schema by its ID
    ///
    /// Checks the cache first to avoid unnecessary network requests.
    /// If not cached, fetches from the Schema Registry and caches the result.
    ///
    /// # Arguments
    /// - `schema_id` - The schema ID to fetch
    ///
    /// # Returns
    /// The Avro schema as a JSON string
    ///
    /// # Errors
    /// Returns an error if:
    /// - The HTTP request fails
    /// - The Schema Registry returns a non-200 status
    /// - The response cannot be parsed
    pub async fn get_schema_by_id(&self, schema_id: i32) -> Result<String> {
        // Check cache first
        {
            let mut cache = self.cache.lock().await;
            if let Some(schema) = cache.get(&schema_id) {
                return Ok(schema.clone());
            }
        }

        // Fetch from registry
        let url = format!("{}/schemas/ids/{}", self.base_url, schema_id);
        let response = self
            .client
            .get(&url)
            .basic_auth(&self.username, Some(&self.password))
            .send()
            .await?;

        if response.status() != StatusCode::OK {
            return Err(anyhow::anyhow!(
                "Failed to fetch schema {}: HTTP {}",
                schema_id,
                response.status()
            ));
        }

        let response_text = response.text().await?;

        // Try to parse as SchemaResponse first
        match serde_json::from_str::<SchemaResponse>(&response_text) {
            Ok(schema_response) => {
                // Cache the schema
                {
                    let mut cache = self.cache.lock().await;
                    cache.put(schema_id, schema_response.schema.clone());
                }
                Ok(schema_response.schema)
            }
            Err(_) => {
                // Maybe it's just the raw schema string
                // Cache it
                {
                    let mut cache = self.cache.lock().await;
                    cache.put(schema_id, response_text.clone());
                }
                Ok(response_text)
            }
        }
    }
}

#[async_trait]
impl SchemaRegistry for SchemaRegistryClient {
    async fn get_schema_by_id(&self, schema_id: i32) -> Result<String> {
        self.get_schema_by_id(schema_id).await
    }
}

/// Checks if a message is in Confluent Avro wire format
///
/// Validates the magic byte and extracts the schema ID if present.
///
/// # Arguments
/// - `data` - The message payload to check
///
/// # Returns
/// - `Some(schema_id)` if the message is Confluent Avro format
/// - `None` if the message is not Avro or is too short
pub fn is_confluent_avro(data: &[u8]) -> Option<i32> {
    if data.len() < 5 {
        return None;
    }

    // Check magic byte
    if data[0] != 0x00 {
        return None;
    }

    // Extract schema ID (big-endian)
    let schema_id = i32::from_be_bytes([data[1], data[2], data[3], data[4]]);
    Some(schema_id)
}

/// Extracts Avro payload from Confluent wire format
///
/// Skips the 5-byte header (magic byte + schema ID) and returns
/// the remaining Avro-encoded data.
///
/// # Arguments
/// - `data` - The complete message including wire format header
///
/// # Returns
/// - `Some(payload)` containing the Avro data (bytes 5 onwards)
/// - `None` if the message is too short or invalid
pub fn extract_avro_payload(data: &[u8]) -> Option<&[u8]> {
    if data.len() < 5 {
        return None;
    }

    if data[0] != 0x00 {
        return None;
    }

    Some(&data[5..])
}
