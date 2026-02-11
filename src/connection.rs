//! Kafka connection pooling and management
//!
//! Maintains cached connections to different Kafka brokers and handles
//! connection lifecycle. Connections are pooled to avoid reconnecting
//! for frequently accessed brokers.
//!
//! # Security
//!
//! All connections use SASL_SSL with SCRAM-SHA-512 authentication and
//! TLS certificate verification enabled.

use anyhow::{Context, Result};
use rdkafka::{
    admin::AdminClient,
    client::DefaultClientContext,
    config::{ClientConfig, RDKafkaLogLevel},
    consumer::StreamConsumer,
};
use std::collections::{HashMap, VecDeque};
use std::sync::Arc;
use std::time::Duration;
use uuid::Uuid;

use crate::config::{self, BrokerProfile};

/// Manages Kafka connections and profiles
///
/// Provides connection pooling, profile management, and topic listing
/// functionality. Maintains a cache of active connections to avoid
/// reconnecting for frequently accessed brokers.
pub struct ConnectionManager {
    /// Map of profile name to profile configuration
    pub profiles: HashMap<String, BrokerProfile>,
    /// Cached Kafka consumer connections by profile name
    pub active_connections: HashMap<String, Arc<StreamConsumer>>,
    /// Currently selected profile name
    pub current_profile: Option<String>,
    /// Currently selected topic name
    pub current_topic: Option<String>,
    /// Recent topics accessed (profile, topic) pairs
    pub recent_topics: VecDeque<(String, String)>,
    /// List of available topics for the current broker
    pub available_topics: Vec<String>,
    /// Cached passwords from keyring to reduce OS keychain prompts
    /// Maps profile name to password
    password_cache: HashMap<String, String>,
}

impl Default for ConnectionManager {
    fn default() -> Self {
        Self::new()
    }
}

impl ConnectionManager {
    /// Creates a new connection manager with empty state
    pub fn new() -> Self {
        ConnectionManager {
            profiles: HashMap::new(),
            active_connections: HashMap::new(),
            current_profile: None,
            current_topic: None,
            recent_topics: VecDeque::with_capacity(10),
            available_topics: Vec::new(),
            password_cache: HashMap::new(),
        }
    }

    /// Connects to a broker using a named profile
    ///
    /// Returns a cached connection if one exists, otherwise creates a new
    /// connection and caches it. Retrieves the password from the system keyring.
    ///
    /// # Arguments
    /// - `profile_name` - Name of the profile to connect with
    ///
    /// # Returns
    /// An Arc-wrapped StreamConsumer for accessing the Kafka broker
    ///
    /// # Errors
    /// Returns an error if:
    /// - The profile doesn't exist
    /// - The password cannot be retrieved from the keyring
    /// - The connection to Kafka fails
    pub fn connect_to_profile(&mut self, profile_name: &str) -> Result<Arc<StreamConsumer>> {
        if let Some(consumer) = self.active_connections.get(profile_name) {
            return Ok(consumer.clone());
        }

        // Clone profile data to avoid borrow conflicts with get_password
        let (broker, user) = {
            let profile = self
                .profiles
                .get(profile_name)
                .with_context(|| format!("Profile '{}' not found", profile_name))?;
            (profile.broker.clone(), profile.user.clone())
        };

        let password = self
            .get_password(profile_name)
            .with_context(|| format!("Failed to get password for profile '{}'", profile_name))?;

        let consumer = create_consumer(&broker, &user, &password)?;

        let consumer = Arc::new(consumer);
        self.active_connections
            .insert(profile_name.to_string(), consumer.clone());
        self.current_profile = Some(profile_name.to_string());

        Ok(consumer)
    }

    /// Lists all topics available on a broker
    ///
    /// Creates a temporary admin client to fetch topic metadata. Internal
    /// Kafka topics (starting with "__") are filtered out. The result is
    /// cached in `available_topics`.
    ///
    /// # Arguments
    /// - `profile_name` - Name of the profile to list topics for
    ///
    /// # Returns
    /// A sorted vector of topic names
    ///
    /// # Errors
    /// Returns an error if:
    /// - The profile doesn't exist
    /// - The password cannot be retrieved
    /// - The admin client cannot be created
    /// - Metadata cannot be fetched
    pub async fn list_topics(&mut self, profile_name: &str) -> Result<Vec<String>> {
        // Clone profile data to avoid borrow conflicts with get_password
        let (broker, user) = {
            let profile = self
                .profiles
                .get(profile_name)
                .with_context(|| format!("Profile '{}' not found", profile_name))?;
            (profile.broker.clone(), profile.user.clone())
        };

        let password = self
            .get_password(profile_name)
            .with_context(|| format!("Failed to get password for profile '{}'", profile_name))?;

        let admin_client: AdminClient<DefaultClientContext> = ClientConfig::new()
            .set("bootstrap.servers", &broker)
            .set("security.protocol", "SASL_SSL")
            .set("sasl.mechanism", "SCRAM-SHA-512")
            .set("sasl.username", &user)
            .set("sasl.password", &password)
            .create()
            .context("Failed to create admin client")?;

        let metadata = admin_client
            .inner()
            .fetch_metadata(None, Duration::from_secs(5))
            .context("Failed to fetch metadata")?;

        let mut topics: Vec<String> = metadata
            .topics()
            .iter()
            .map(|topic| topic.name().to_string())
            .filter(|name| !name.starts_with("__"))
            .collect();

        topics.sort();
        self.available_topics = topics.clone();

        Ok(topics)
    }

    /// Adds a topic to the recent topics list
    ///
    /// Removes any existing entry for the same profile+topic combination
    /// and adds it to the front. Keeps only the 10 most recent topics.
    ///
    /// # Arguments
    /// - `profile` - Profile name used to access the topic
    /// - `topic` - Topic name that was accessed
    pub fn add_recent_topic(&mut self, profile: String, topic: String) {
        self.recent_topics
            .retain(|(p, t)| !(p == &profile && t == &topic));
        self.recent_topics.push_front((profile, topic));

        if self.recent_topics.len() > 10 {
            self.recent_topics.pop_back();
        }
    }

    /// Cleans up old connections to free resources
    ///
    /// Removes cached connections, keeping only the most recent ones.
    /// The current profile's connection is always preserved.
    ///
    /// # Arguments
    /// - `keep_count` - Number of connections to keep in the cache
    pub fn cleanup_old_connections(&mut self, keep_count: usize) {
        if self.active_connections.len() <= keep_count {
            return;
        }

        let current = self.current_profile.clone();
        let mut to_remove = Vec::new();

        for (name, _) in self.active_connections.iter() {
            if Some(name) != current.as_ref()
                && to_remove.len() < self.active_connections.len() - keep_count
            {
                to_remove.push(name.clone());
            }
        }

        for name in to_remove {
            self.active_connections.remove(&name);
        }
    }

    /// Retrieves a password from cache or keyring
    ///
    /// Checks the in-memory cache first. If not found, fetches from the system
    /// keyring and caches it to avoid repeated OS keychain prompts.
    ///
    /// # Arguments
    /// - `profile_name` - Name of the profile to get the password for
    ///
    /// # Returns
    /// The password as a String
    ///
    /// # Errors
    /// Returns an error if the password cannot be retrieved from the keyring
    ///
    /// # Security
    /// Passwords are cached in memory for the duration of the application.
    /// The cache is cleared when the ConnectionManager is dropped.
    pub fn get_password(&mut self, profile_name: &str) -> Result<String> {
        // Check cache first
        if let Some(password) = self.password_cache.get(profile_name) {
            return Ok(password.clone());
        }

        // Cache miss - fetch from keyring and cache it
        let password = config::get_password(profile_name)?;
        self.password_cache
            .insert(profile_name.to_string(), password.clone());
        Ok(password)
    }

    /// Seeds the password cache with a known password
    ///
    /// Use this to avoid redundant keychain prompts when a password has already
    /// been retrieved elsewhere (e.g., at startup). The password is stored in
    /// the in-memory cache so subsequent calls to `get_password()` won't trigger
    /// a keychain access.
    ///
    /// # Arguments
    /// - `profile_name` - Name of the profile to cache the password for
    /// - `password` - The password to cache
    pub fn seed_password(&mut self, profile_name: &str, password: String) {
        self.password_cache
            .insert(profile_name.to_string(), password);
    }

    /// Invalidates a cached password
    ///
    /// Removes a password from the cache, forcing the next access to fetch
    /// from the keyring. Call this after updating or deleting a profile's password.
    ///
    /// # Arguments
    /// - `profile_name` - Name of the profile to invalidate
    pub fn invalidate_password(&mut self, profile_name: &str) {
        self.password_cache.remove(profile_name);
    }

    /// Clears all cached passwords
    ///
    /// Removes all passwords from memory. Useful for security-conscious
    /// scenarios or when multiple profiles have been updated.
    #[allow(dead_code)]
    pub fn clear_password_cache(&mut self) {
        self.password_cache.clear();
    }
}

/// Creates a new Kafka consumer with SASL/SSL configuration
///
/// Configures a StreamConsumer with:
/// - SASL_SSL security protocol
/// - SCRAM-SHA-512 authentication
/// - TLS certificate verification enabled
/// - Auto-commit disabled
/// - Unique consumer group ID
///
/// # Arguments
/// - `broker` - Kafka broker address (e.g., "localhost:9092")
/// - `user` - SASL username
/// - `password` - SASL password
///
/// # Returns
/// A configured StreamConsumer ready for use
///
/// # Errors
/// Returns an error if the consumer configuration is invalid or
/// the connection to Kafka fails.
pub fn create_consumer(broker: &str, user: &str, password: &str) -> Result<StreamConsumer> {
    ClientConfig::new()
        .set("bootstrap.servers", broker)
        .set("security.protocol", "SASL_SSL")
        .set("sasl.mechanism", "SCRAM-SHA-512")
        .set("sasl.username", user)
        .set("sasl.password", password)
        .set("enable.ssl.certificate.verification", "true") // Enable TLS certificate validation
        .set("group.id", format!("kafkatui-{}", Uuid::new_v4()))
        .set("enable.partition.eof", "false")
        .set("session.timeout.ms", "6000")
        .set("enable.auto.commit", "false")
        .set("auto.offset.reset", "latest")
        .set_log_level(RDKafkaLogLevel::Error)
        .create()
        .context("Failed to create consumer")
}
