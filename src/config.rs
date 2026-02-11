//! Configuration management
//!
//! Handles loading and saving broker profiles and connection settings.
//! Passwords are stored securely in the system keyring, not in the config file.
//!
//! # Security
//!
//! This module uses the system keyring for secure password storage:
//! - macOS: Keychain
//! - Linux: Secret Service API / libsecret
//! - Windows: Credential Manager
//!
//! Configuration files are stored in platform-specific directories:
//! - macOS: `~/Library/Application Support/kafkatui/config.yaml`
//! - Linux: `~/.config/kafkatui/config.yaml`
//! - Windows: `%APPDATA%\kafkatui\config.yaml`

use anyhow::{Context, Result};
use directories::ProjectDirs;
use keyring::Entry;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fs;
use std::path::PathBuf;

/// A Kafka broker connection profile
///
/// Stores connection details for a Kafka broker, including authentication
/// credentials and optional Schema Registry URL. Passwords are stored
/// separately in the system keyring for security.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BrokerProfile {
    /// Unique profile name
    pub name: String,
    /// Kafka broker address (e.g., "localhost:9092")
    pub broker: String,
    /// SASL username for authentication
    pub user: String,
    /// Optional Confluent Schema Registry URL
    #[serde(skip_serializing_if = "Option::is_none")]
    pub schema_registry: Option<String>,
    /// User's favorite topics for quick access
    #[serde(default)]
    pub favorite_topics: Vec<String>,
}

/// Application configuration
///
/// Contains all broker profiles and recent connection history.
/// Persisted as YAML in the user's config directory.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Config {
    /// Map of profile name to profile configuration
    #[serde(default)]
    pub profiles: HashMap<String, BrokerProfile>,
    /// Recently accessed topics for quick reconnection
    #[serde(default)]
    pub recent_connections: Vec<RecentConnection>,
}

/// A record of a recent connection to a topic
///
/// Used to populate the "recent connections" list for quick access
/// to frequently used topics.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RecentConnection {
    /// Name of the profile used
    pub profile_name: String,
    /// Topic that was accessed
    pub topic: String,
    /// Unix timestamp in milliseconds
    pub timestamp: i64,
}

impl Config {
    /// Loads configuration from the default config file location
    ///
    /// # Returns
    /// - `Ok(Config)` with the loaded configuration, or an empty config if the file doesn't exist
    /// - `Err` if the file exists but cannot be read or parsed
    ///
    /// # Errors
    /// Returns an error if the config file exists but is malformed or unreadable.
    pub fn load() -> Result<Self> {
        let config_path = Self::config_path()?;

        if !config_path.exists() {
            return Ok(Config {
                profiles: HashMap::new(),
                recent_connections: Vec::new(),
            });
        }

        let contents = fs::read_to_string(&config_path)
            .with_context(|| format!("Failed to read config from {:?}", config_path))?;

        let config: Config =
            serde_yaml::from_str(&contents).with_context(|| "Failed to parse config file")?;

        Ok(config)
    }

    /// Saves the configuration to the default config file location
    ///
    /// Creates the config directory if it doesn't exist. The configuration
    /// is serialized as YAML.
    ///
    /// # Errors
    /// Returns an error if the directory cannot be created or the file cannot be written.
    pub fn save(&self) -> Result<()> {
        let config_path = Self::config_path()?;

        if let Some(parent) = config_path.parent() {
            fs::create_dir_all(parent)?;
        }

        let contents = serde_yaml::to_string(self)?;
        fs::write(&config_path, contents)
            .with_context(|| format!("Failed to write config to {:?}", config_path))?;

        Ok(())
    }

    /// Returns the platform-specific configuration file path
    ///
    /// Uses the `directories` crate to determine the appropriate location:
    /// - macOS: `~/Library/Application Support/kafkatui/config.yaml`
    /// - Linux: `~/.config/kafkatui/config.yaml`
    /// - Windows: `%APPDATA%\kafkatui\config.yaml`
    ///
    /// Falls back to `~/.config/kafkatui/config.yaml` if platform detection fails.
    ///
    /// # Errors
    /// Returns an error if the HOME environment variable is not set (fallback case only).
    pub fn config_path() -> Result<PathBuf> {
        if let Some(proj_dirs) = ProjectDirs::from("", "", "kafkatui") {
            let config_dir = proj_dirs.config_dir();
            Ok(config_dir.join("config.yaml"))
        } else {
            let home = std::env::var("HOME").context("HOME not set")?;
            Ok(PathBuf::from(home).join(".config/kafkatui/config.yaml"))
        }
    }

    /// Adds a connection to the recent connections list
    ///
    /// Removes any existing entry for the same profile+topic combination
    /// and adds it to the front. Keeps only the 10 most recent connections.
    ///
    /// # Arguments
    /// - `profile_name` - Name of the profile used for the connection
    /// - `topic` - Topic that was accessed
    pub fn add_recent_connection(&mut self, profile_name: String, topic: String) {
        let connection = RecentConnection {
            profile_name,
            topic,
            timestamp: chrono::Utc::now().timestamp_millis(),
        };

        self.recent_connections.retain(|c| {
            !(c.profile_name == connection.profile_name && c.topic == connection.topic)
        });

        self.recent_connections.insert(0, connection);

        if self.recent_connections.len() > 10 {
            self.recent_connections.truncate(10);
        }
    }
}

/// Saves a password to the system keyring
///
/// Stores the password securely using the platform's credential manager.
/// The service name is "kafkatui" and the account name is the profile name.
///
/// # Arguments
/// - `profile_name` - Name of the profile (used as the keyring entry identifier)
/// - `password` - Password to store securely
///
/// # Errors
/// Returns an error if the keyring entry cannot be created or the password cannot be saved.
pub fn save_password(profile_name: &str, password: &str) -> Result<()> {
    let entry = Entry::new("kafkatui", profile_name).with_context(|| {
        format!(
            "Failed to create keyring entry for profile '{}'",
            profile_name
        )
    })?;
    entry
        .set_password(password)
        .with_context(|| format!("Failed to save password for profile '{}'", profile_name))?;
    Ok(())
}

/// Retrieves a password from the system keyring
///
/// # Arguments
/// - `profile_name` - Name of the profile to retrieve the password for
///
/// # Returns
/// The stored password as a String
///
/// # Errors
/// Returns an error if the keyring entry doesn't exist or the password cannot be retrieved.
pub fn get_password(profile_name: &str) -> Result<String> {
    let entry = Entry::new("kafkatui", profile_name).with_context(|| {
        format!(
            "Failed to create keyring entry for profile '{}'",
            profile_name
        )
    })?;
    entry
        .get_password()
        .with_context(|| format!("Failed to retrieve password for profile '{}'", profile_name))
}

/// Deletes a password from the system keyring
///
/// # Arguments
/// - `profile_name` - Name of the profile to delete the password for
///
/// # Errors
/// Returns an error if the keyring entry doesn't exist or the password cannot be deleted.
pub fn delete_password(profile_name: &str) -> Result<()> {
    let entry = Entry::new("kafkatui", profile_name).with_context(|| {
        format!(
            "Failed to create keyring entry for profile '{}'",
            profile_name
        )
    })?;
    entry
        .delete_password()
        .with_context(|| format!("Failed to delete password for profile '{}'", profile_name))?;
    Ok(())
}
