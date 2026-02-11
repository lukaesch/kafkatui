//! KafkaTUI - Terminal User Interface for Apache Kafka
//!
//! A modern TUI for browsing Kafka topics with features including:
//! - Multi-broker/profile management
//! - Avro deserialization with Schema Registry
//! - Global search across partitions
//! - Secure credential storage in system keyring
//! - Message export to JSON
//!
//! # Architecture
//!
//! The application is organized into several modules:
//! - `config` - Configuration and profile management
//! - `connection` - Kafka connection pooling
//! - `schema_registry` - Avro schema fetching
//! - `ui` - Terminal UI rendering with ratatui
//! - `utils` - Formatting and validation utilities
//! - `stores` - Storage trait implementations
//! - `traits` - Common trait definitions

mod config;
mod connection;
mod schema_registry;
mod traits;
mod ui;
mod utils;

use crate::config::{BrokerProfile, Config};
use crate::connection::ConnectionManager;
use crate::schema_registry::SchemaRegistryClient;
use crate::ui::ui;
use anyhow::{Context, Result};
use chrono::Utc;
use clap::Parser;
use crossterm::{
    event::{self, poll, Event, KeyCode, KeyModifiers},
    execute,
    terminal::{disable_raw_mode, enable_raw_mode, EnterAlternateScreen, LeaveAlternateScreen},
};
use ratatui::{
    backend::{Backend, CrosstermBackend},
    widgets::ListState,
    Terminal,
};
use rdkafka::{
    config::{ClientConfig, RDKafkaLogLevel},
    consumer::{Consumer, StreamConsumer},
    message::{BorrowedMessage, Headers},
    Message, Offset, TopicPartitionList,
};
use serde_json::Value;
use std::{
    collections::{HashMap, VecDeque},
    env, io,
    sync::Arc,
    time::{Duration, Instant},
};
use tokio::{sync::mpsc, task::JoinHandle, time::timeout};
use utils::formatting::{avro_value_to_json, hex_dump, is_binary};
use utils::validation::sanitize_filename;
use uuid::Uuid;

// Configuration constants

/// Default number of messages to load per partition
const DEFAULT_MESSAGE_LOAD_COUNT: usize = 500;

/// Number of messages to search per partition in global search
const GLOBAL_SEARCH_MESSAGE_COUNT: usize = 100;

/// Maximum number of cached connections to maintain
const CONNECTION_POOL_SIZE: usize = 3;

/// Timeout for metadata fetch operations (seconds)
const METADATA_TIMEOUT_SECS: u64 = 5;

/// Kafka session timeout (milliseconds)
const SESSION_TIMEOUT_MS: &str = "6000";

/// Command-line arguments
#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    #[arg(short, long)]
    broker: Option<String>,

    #[arg(short, long)]
    user: Option<String>,

    #[arg(short, long)]
    password: Option<String>,

    #[arg(short, long)]
    topic: Option<String>,
}

/// Metadata for a Kafka message (without the full content)
///
/// Used for efficient list display. The full message content is loaded
/// on demand and cached separately.
#[derive(Clone)]
struct MessageMetadata {
    /// Partition this message belongs to
    partition: i32,
    /// Kafka offset number
    offset: i64,
    /// Message timestamp in milliseconds
    timestamp: i64,
    /// Message key (decoded if possible)
    key: Option<String>,
    /// Avro schema ID if the key is Avro-encoded
    key_avro_schema_id: Option<i32>,
    /// Raw key bytes for later processing
    raw_key: Option<Vec<u8>>,
    /// Size of the message value in bytes
    value_size: usize,
    /// Whether the value appears to be binary data
    is_binary: bool,
    /// Avro schema ID if the value is Avro-encoded
    avro_schema_id: Option<i32>,
    /// Message headers as key-value pairs
    headers: Vec<(String, String)>,
}

/// Full content of a Kafka message
///
/// Stored separately from metadata and loaded on demand for memory efficiency.
#[derive(Clone)]
struct MessageContent {
    /// Kafka offset (for cache lookup)
    #[allow(dead_code)]
    offset: i64,
    /// Decoded message value as a string
    value: String,
    /// Raw message bytes (for hex view)
    raw_bytes: Option<Vec<u8>>,
}

/// Information about a Kafka partition
///
/// Contains metadata about the partition and all loaded messages.
#[derive(Clone)]
struct PartitionInfo {
    /// Partition ID
    id: i32,
    /// High watermark (total message count)
    high_watermark: i64,
    /// Loaded message metadata
    messages: Vec<MessageMetadata>,
}

/// Current mode of the application UI
///
/// Determines which view is displayed and how input is handled.
enum AppMode {
    /// Selecting a broker profile
    BrokerSwitcher,
    /// Selecting a topic
    TopicSwitcher,
    /// Creating or editing a profile
    ProfileEditor,
    /// Confirming profile deletion
    ProfileDeleting,
    /// Testing a connection
    ConnectionTesting,
    /// Viewing list of partitions
    PartitionList,
    /// Viewing messages in a partition
    MessageList,
    /// Viewing detailed message content
    MessageDetail,
    /// Jumping to a specific offset
    OffsetJump,
    /// Searching within current partition
    Search,
    /// Loading data
    Loading,
    /// Analyzing message keys
    KeyAnalysis,
    /// Global search across all partitions
    GlobalSearch,
    /// Showing file saved confirmation
    FileSaved,
}

/// Message display format
///
/// Controls how message content is rendered in the detail view.
#[derive(Debug, Clone, PartialEq)]
enum DisplayFormat {
    /// Pretty-printed JSON
    Json,
    /// Hexadecimal dump with ASCII sidebar
    Hex,
    /// Raw bytes as lossy UTF-8 string
    Raw,
}

/// Global search strategy
///
/// Determines how thoroughly messages are searched.
#[derive(Debug, Clone)]
enum SearchStrategy {
    /// Search keys and cached message values only (fast)
    Quick,
    /// Load and search all message content (slow but thorough)
    Deep,
}

/// Updates from the background global search task
///
/// Sent via channel to update the UI with search progress and results.
enum SearchUpdate {
    /// A search result was found
    Result(GlobalSearchResult),
    /// Progress update
    Progress {
        /// Index of the partition being searched
        partition_idx: usize,
        /// Number of messages processed so far
        messages_processed: u64,
    },
    /// An error occurred during search
    Error(String),
    /// Search completed
    Complete,
}

/// A single result from global search
///
/// Contains information about where a match was found and context.
#[derive(Debug, Clone)]
struct GlobalSearchResult {
    /// Index into the app's partitions vector
    partition_idx: usize,
    /// Partition ID
    partition_id: i32,
    /// Index into the partition's messages vector
    message_idx: usize,
    /// Kafka offset
    #[allow(dead_code)]
    offset: i64,
    /// Message timestamp
    timestamp: i64,
    /// Message key
    key: Option<String>,
    /// Snippet of text showing the match
    match_context: String,
    /// Whether the match was in the key (vs value)
    match_in_key: bool,
}

/// State for global search across all partitions
///
/// Manages the search query, results, progress, and background task.
struct GlobalSearchState {
    /// Current search query
    query: String,
    /// Search strategy (Quick or Deep)
    strategy: SearchStrategy,
    /// Search results found so far
    results: Vec<GlobalSearchResult>,
    /// Number of partitions searched
    partitions_searched: usize,
    /// Total number of partitions to search
    total_partitions: usize,
    /// Whether a search is currently running
    is_searching: bool,
    /// Selected result index (for navigation)
    selected_result: usize,
    /// Whether user has requested cancellation
    cancel_requested: bool,
    /// Number of messages processed
    messages_processed: u64,
    /// Estimated total messages to process
    estimated_total: u64,
    /// Handle to the background search task
    search_handle: Option<JoinHandle<()>>,
    /// Channel for receiving search updates
    results_receiver: Option<mpsc::UnboundedReceiver<SearchUpdate>>,
}

/// Mode for profile editor (creating vs editing)
#[derive(Clone)]
enum ProfileEditMode {
    /// Creating a new profile
    Creating,
    /// Editing an existing profile
    Editing,
}

/// Fields in the profile editor form
#[derive(Debug, Clone, PartialEq)]
enum ProfileField {
    /// Profile name field
    Name,
    /// Broker address field
    Broker,
    /// Username field
    User,
    /// Password field
    Password,
    /// Schema Registry URL field
    SchemaRegistry,
}

/// Editor state for creating or modifying broker profiles
///
/// Manages the form state, validation, and user input for profile management.
#[derive(Clone)]
struct ProfileEditor {
    mode: ProfileEditMode,
    active_field: ProfileField,
    name: String,
    broker: String,
    user: String,
    password: String,
    schema_registry: String,
    use_env_var: bool,
    env_var_name: String,
    validation_errors: Vec<String>,
    original_name: Option<String>,
    show_password: bool,
}

impl ProfileEditor {
    /// Creates a new ProfileEditor for creating a new profile
    fn new_create() -> Self {
        ProfileEditor {
            mode: ProfileEditMode::Creating,
            active_field: ProfileField::Name,
            name: String::new(),
            broker: String::new(),
            user: String::new(),
            password: String::new(),
            schema_registry: String::new(),
            use_env_var: false,
            env_var_name: String::new(),
            validation_errors: Vec::new(),
            original_name: None,
            show_password: false,
        }
    }

    /// Creates a new ProfileEditor for editing an existing profile
    ///
    /// Accepts a pre-fetched password to avoid redundant keychain prompts.
    fn new_edit(profile: &BrokerProfile, password: String) -> Self {
        ProfileEditor {
            mode: ProfileEditMode::Editing,
            active_field: ProfileField::Name,
            name: profile.name.clone(),
            broker: profile.broker.clone(),
            user: profile.user.clone(),
            password,
            schema_registry: profile.schema_registry.clone().unwrap_or_default(),
            use_env_var: false,
            env_var_name: String::new(),
            validation_errors: Vec::new(),
            original_name: Some(profile.name.clone()),
            show_password: false,
        }
    }

    /// Move to the next input field
    fn next_field(&mut self) {
        self.active_field = match self.active_field {
            ProfileField::Name => ProfileField::Broker,
            ProfileField::Broker => ProfileField::User,
            ProfileField::User => ProfileField::Password,
            ProfileField::Password => ProfileField::SchemaRegistry,
            ProfileField::SchemaRegistry => ProfileField::Name,
        };
    }

    /// Moves to the previous input field in the form
    fn prev_field(&mut self) {
        self.active_field = match self.active_field {
            ProfileField::Name => ProfileField::SchemaRegistry,
            ProfileField::Broker => ProfileField::Name,
            ProfileField::User => ProfileField::Broker,
            ProfileField::Password => ProfileField::User,
            ProfileField::SchemaRegistry => ProfileField::Password,
        };
    }

    /// Returns a mutable reference to the currently active input field's value
    fn get_active_input(&mut self) -> &mut String {
        match self.active_field {
            ProfileField::Name => &mut self.name,
            ProfileField::Broker => &mut self.broker,
            ProfileField::User => &mut self.user,
            ProfileField::Password => {
                if self.use_env_var {
                    &mut self.env_var_name
                } else {
                    &mut self.password
                }
            }
            ProfileField::SchemaRegistry => &mut self.schema_registry,
        }
    }

    /// Validates all input fields and populates validation_errors
    /// Returns true if all fields are valid, false otherwise
    fn validate(&mut self) -> bool {
        self.validation_errors.clear();

        if self.name.is_empty() {
            self.validation_errors.push("Name is required".to_string());
        } else if !self
            .name
            .chars()
            .all(|c| c.is_alphanumeric() || c == '_' || c == '-')
        {
            self.validation_errors
                .push("Name can only contain letters, numbers, underscore and dash".to_string());
        }

        if self.broker.is_empty() {
            self.validation_errors
                .push("Broker address is required".to_string());
        } else if !self.broker.contains(':') {
            self.validation_errors
                .push("Broker must be in format host:port".to_string());
        }

        if self.user.is_empty() {
            self.validation_errors
                .push("Username is required".to_string());
        }

        if !self.use_env_var && self.password.is_empty() {
            self.validation_errors
                .push("Password is required (or use environment variable)".to_string());
        } else if self.use_env_var && self.env_var_name.is_empty() {
            self.validation_errors
                .push("Environment variable name is required".to_string());
        }

        if !self.schema_registry.is_empty() && !self.schema_registry.starts_with("http") {
            self.validation_errors
                .push("Schema registry must be a valid HTTP(S) URL".to_string());
        }

        self.validation_errors.is_empty()
    }

    /// Converts the editor state into a BrokerProfile for persistence
    /// Note: Password is saved separately to keyring, not in the profile
    fn to_profile(&self) -> BrokerProfile {
        BrokerProfile {
            name: self.name.clone(),
            broker: self.broker.clone(),
            user: self.user.clone(),
            schema_registry: if self.schema_registry.is_empty() {
                None
            } else {
                Some(self.schema_registry.clone())
            },
            favorite_topics: Vec::new(),
        }
    }

    /// Returns the password (either from password field or env var)
    fn get_password(&self) -> Result<String> {
        if self.use_env_var {
            env::var(&self.env_var_name)
                .with_context(|| format!("Environment variable '{}' not set", self.env_var_name))
        } else {
            Ok(self.password.clone())
        }
    }
}

#[derive(Debug, Clone)]
struct AppStats {
    total_messages: u64,
    messages_loaded: u64,
    bytes_read: u64,
    cache_hits: u64,
    cache_misses: u64,
    broker_address: String,
}

impl AppStats {
    fn new(broker_address: String) -> Self {
        Self {
            total_messages: 0,
            messages_loaded: 0,
            bytes_read: 0,
            cache_hits: 0,
            cache_misses: 0,
            broker_address,
        }
    }

    fn cache_hit_rate(&self) -> f64 {
        let total = self.cache_hits + self.cache_misses;
        if total == 0 {
            0.0
        } else {
            (self.cache_hits as f64 / total as f64) * 100.0
        }
    }
}

struct App {
    mode: AppMode,
    partitions: Vec<PartitionInfo>,
    selected_partition: usize,
    message_list_state: ListState,
    selected_message: Option<usize>,
    message_scroll: u16,
    jump_offset_input: String,
    search_query: String,
    search_results: Vec<(usize, usize)>,
    current_search_index: usize,
    topic_name: String,
    consumer: Arc<StreamConsumer>,
    empty_messages: Vec<MessageMetadata>,
    loading_message: String,
    debug_message: String,
    message_cache: HashMap<i64, MessageContent>,
    schema_registry: Option<Arc<SchemaRegistryClient>>,
    display_format: DisplayFormat,
    detail_scroll_offset: u16,
    error_log: VecDeque<(chrono::DateTime<Utc>, String)>,
    show_errors: bool,
    global_search: GlobalSearchState,
    stats: AppStats,
    connection_manager: ConnectionManager,
    broker_switcher_state: ListState,
    topic_switcher_state: ListState,
    topic_filter: String,
    profile_editor: Option<ProfileEditor>,
    profile_to_delete: Option<String>,
    test_result: Option<String>,
    saved_file_path: Option<String>,
}

/// Configuration for spawning a background search task
struct SearchTaskConfig {
    messages_to_search: Vec<(usize, i32, usize, i64, i64, Option<String>)>,
    query: String,
    strategy: SearchStrategy,
    cache_snapshot: HashMap<i64, String>,
    topic_name: String,
    broker: String,
    user: String,
    password: String,
    schema_registry: Option<Arc<SchemaRegistryClient>>,
    tx: mpsc::UnboundedSender<SearchUpdate>,
}

/// Spawns a background task to search through messages
fn spawn_search_task(config: SearchTaskConfig) -> JoinHandle<()> {
    let SearchTaskConfig {
        messages_to_search,
        query,
        strategy,
        cache_snapshot,
        topic_name,
        broker,
        user,
        password,
        schema_registry,
        tx,
    } = config;
    tokio::spawn(async move {
        let max_results = 200;
        let mut results_count = 0;

        for (idx, (partition_idx, partition_id, msg_idx, offset, timestamp, key)) in
            messages_to_search.into_iter().enumerate()
        {
            // Send progress update
            if tx
                .send(SearchUpdate::Progress {
                    partition_idx,
                    messages_processed: idx as u64 + 1,
                })
                .is_err()
            {
                break; // Channel closed, stop search
            }

            let mut found = false;
            let mut match_context = String::new();
            let mut match_in_key = false;

            // Search in key
            if let Some(ref key_str) = key {
                if key_str.to_lowercase().contains(&query) {
                    found = true;
                    match_in_key = true;
                    match_context = key_str.clone();
                }
            }

            // Search in content
            if !found {
                match strategy {
                    SearchStrategy::Quick => {
                        // Check cache
                        if let Some(content) = cache_snapshot.get(&offset) {
                            if content.to_lowercase().contains(&query) {
                                found = true;
                                match_context = content
                                    .chars()
                                    .take(100)
                                    .collect::<String>()
                                    .replace('\n', " ");
                            }
                        }
                    }
                    SearchStrategy::Deep => {
                        // Load content from Kafka
                        match load_message_content_static(
                            &topic_name,
                            partition_id,
                            offset,
                            &broker,
                            &user,
                            &password,
                            schema_registry.as_ref(),
                        )
                        .await
                        {
                            Ok(content) => {
                                if content.to_lowercase().contains(&query) {
                                    found = true;
                                    match_context = content
                                        .chars()
                                        .take(100)
                                        .collect::<String>()
                                        .replace('\n', " ");
                                }
                            }
                            Err(e) => {
                                let _ = tx.send(SearchUpdate::Error(format!(
                                    "Failed to load content: {}",
                                    e
                                )));
                            }
                        }
                    }
                }
            }

            if found {
                if let Err(_e) = tx.send(SearchUpdate::Result(GlobalSearchResult {
                    partition_idx,
                    partition_id,
                    message_idx: msg_idx,
                    offset,
                    timestamp,
                    key,
                    match_context,
                    match_in_key,
                })) {
                    break;
                }

                results_count += 1;
                if results_count >= max_results {
                    break;
                }
            }

            // Yield periodically to allow cancellation
            if idx % 10 == 0 {
                tokio::task::yield_now().await;
            }
        }

        let _ = tx.send(SearchUpdate::Complete);
    })
}

impl App {
    fn new(topic_name: String, consumer: StreamConsumer, broker_address: String) -> Self {
        let mut app = App {
            mode: AppMode::PartitionList,
            partitions: Vec::new(),
            selected_partition: 0,
            message_list_state: ListState::default(),
            selected_message: None,
            message_scroll: 0,
            jump_offset_input: String::new(),
            search_query: String::new(),
            search_results: Vec::new(),
            current_search_index: 0,
            topic_name,
            consumer: Arc::new(consumer),
            empty_messages: Vec::new(),
            loading_message: String::new(),
            debug_message: String::new(),
            message_cache: HashMap::new(),
            schema_registry: None,
            display_format: DisplayFormat::Json,
            detail_scroll_offset: 0,
            error_log: VecDeque::with_capacity(100),
            show_errors: false,
            global_search: GlobalSearchState {
                query: String::new(),
                strategy: SearchStrategy::Quick,
                results: Vec::new(),
                partitions_searched: 0,
                total_partitions: 0,
                is_searching: false,
                selected_result: 0,
                cancel_requested: false,
                messages_processed: 0,
                estimated_total: 0,
                search_handle: None,
                results_receiver: None,
            },
            stats: AppStats::new(broker_address),
            connection_manager: ConnectionManager::new(),
            broker_switcher_state: ListState::default(),
            topic_switcher_state: ListState::default(),
            topic_filter: String::new(),
            profile_editor: None,
            profile_to_delete: None,
            test_result: None,
            saved_file_path: None,
        };
        app.message_list_state.select(Some(0));
        app
    }

    fn with_schema_registry(mut self, client: Arc<SchemaRegistryClient>) -> Self {
        self.schema_registry = Some(client);
        self
    }

    fn log_error(&mut self, error: String) {
        // Keep only last 100 errors
        if self.error_log.len() >= 100 {
            self.error_log.pop_front();
        }
        self.error_log.push_back((Utc::now(), error));
    }

    async fn load_partition_info(&mut self) -> Result<()> {
        let metadata = self.consumer.fetch_metadata(
            Some(&self.topic_name),
            Duration::from_secs(METADATA_TIMEOUT_SECS * 2),
        )?;

        let topic = metadata
            .topics()
            .iter()
            .find(|t| t.name() == self.topic_name)
            .context("Topic not found")?;

        self.partitions.clear();
        self.stats.total_messages = 0; // Reset total

        for partition in topic.partitions() {
            let partition_id = partition.id();

            let (_low, high) = self.consumer.fetch_watermarks(
                &self.topic_name,
                partition_id,
                Duration::from_secs(METADATA_TIMEOUT_SECS),
            )?;

            self.stats.total_messages += high as u64;

            self.partitions.push(PartitionInfo {
                id: partition_id,
                high_watermark: high,
                messages: Vec::new(),
            });
        }

        Ok(())
    }

    async fn load_messages_at_offset(
        &mut self,
        partition_idx: usize,
        start_offset: i64,
        limit: usize,
    ) -> Result<()> {
        if partition_idx >= self.partitions.len() {
            return Ok(());
        }

        let partition = &self.partitions[partition_idx];
        let partition_id = partition.id;

        let mut tpl = TopicPartitionList::new();
        tpl.add_partition_offset(&self.topic_name, partition_id, Offset::Offset(start_offset))?;

        // Use connection manager's current profile credentials
        let profile_name = self
            .connection_manager
            .current_profile
            .as_ref()
            .context("No active profile selected")?
            .clone();

        // Clone profile data to avoid borrow conflicts with get_password
        let (broker, user) = {
            let profile = self
                .connection_manager
                .profiles
                .get(&profile_name)
                .context("Current profile not found")?;
            (profile.broker.clone(), profile.user.clone())
        };

        let password = self
            .connection_manager
            .get_password(&profile_name)
            .with_context(|| format!("Failed to get password for profile '{}'", profile_name))?;

        let consumer = create_kafka_consumer(&broker, &user, &password)?;

        consumer.assign(&tpl)?;

        let mut messages = Vec::new();
        let start = std::time::Instant::now();
        let mut consecutive_timeouts = 0;
        let mut has_received_any = false;

        while messages.len() < limit
            && start.elapsed() < Duration::from_secs(METADATA_TIMEOUT_SECS * 2)
        {
            match timeout(Duration::from_millis(100), consumer.recv()).await {
                Ok(Ok(msg)) => {
                    let metadata = self.parse_message_metadata(&msg)?;
                    let offset = metadata.offset;

                    if let Some(payload) = msg.payload() {
                        self.parse_and_cache_payload(payload, offset).await?;
                    }

                    messages.push(metadata);
                    consecutive_timeouts = 0;
                    has_received_any = true;
                }
                Ok(Err(e)) => {
                    let error_msg = format!("Message fetch error: {}", e);
                    self.log_error(error_msg.clone());
                    self.loading_message = error_msg;
                    // Break immediately on authorization errors
                    if e.to_string().contains("Authorization") || e.to_string().contains("ACL") {
                        break;
                    }
                    // Only break on other errors if we've already received some messages
                    if has_received_any {
                        break;
                    }
                }
                Err(_) => {
                    consecutive_timeouts += 1;
                    // Only apply early termination if we've already received messages
                    // Otherwise, keep trying for the full timeout period
                    if has_received_any && consecutive_timeouts >= 5 {
                        break;
                    }
                }
            }
        }

        // Reverse messages so newest is first
        messages.reverse();

        // Deserialize Avro keys if we have a schema registry
        self.deserialize_avro_keys(&mut messages).await?;

        // Update stats
        let new_messages_count = messages.len();
        self.stats.messages_loaded += new_messages_count as u64;

        // Update loading message with result
        if messages.is_empty() {
            self.loading_message = format!(
                "No messages loaded from partition {} (tried offset {})",
                partition_id, start_offset
            );
        } else {
            self.loading_message = format!(
                "Loaded {} messages from partition {}",
                new_messages_count, partition_id
            );
        }

        self.partitions[partition_idx].messages = messages;
        Ok(())
    }

    async fn load_messages(&mut self, partition_idx: usize, limit: usize) -> Result<()> {
        if partition_idx >= self.partitions.len() {
            return Ok(());
        }

        let partition = &self.partitions[partition_idx];
        let partition_id = partition.id;
        let high_watermark = partition.high_watermark;

        // Fetch the low watermark to ensure we don't read below it
        let (low_watermark, _) = self.consumer.fetch_watermarks(
            &self.topic_name,
            partition_id,
            Duration::from_secs(METADATA_TIMEOUT_SECS),
        )?;

        // Calculate offset to start from (get last 'limit' messages)
        let start_offset = if high_watermark > limit as i64 {
            (high_watermark - limit as i64).max(low_watermark)
        } else {
            low_watermark
        };

        self.loading_message = format!(
            "Loading partition {} from offset {} to {} (low: {}, high: {})",
            partition_id, start_offset, high_watermark, low_watermark, high_watermark
        );

        self.load_messages_at_offset(partition_idx, start_offset, limit)
            .await
    }

    async fn load_earlier_messages(&mut self, partition_idx: usize, limit: usize) -> Result<()> {
        if partition_idx >= self.partitions.len() {
            return Ok(());
        }

        let current_messages = &self.partitions[partition_idx].messages;
        // Since messages are reversed (newest first), the last message has the lowest offset
        if let Some(last_msg) = current_messages.last() {
            let partition_id = self.partitions[partition_idx].id;

            // Get the actual low watermark to check if we can load more
            let (low_watermark, _high_watermark) = self.consumer.fetch_watermarks(
                &self.topic_name,
                partition_id,
                Duration::from_secs(METADATA_TIMEOUT_SECS),
            )?;

            self.debug_message = format!(
                "DEBUG: Current oldest: {}, low_watermark: {}",
                last_msg.offset, low_watermark
            );

            // Check if we're already at the beginning
            if last_msg.offset <= low_watermark {
                self.loading_message = format!(
                    "At beginning: current oldest {} <= low watermark {}",
                    last_msg.offset, low_watermark
                );
                self.debug_message = format!(
                    "Stopped: offset {} <= low_watermark {}",
                    last_msg.offset, low_watermark
                );
                return Ok(());
            }

            // Start loading from before our oldest message
            let new_end = last_msg.offset - 1;
            let new_start = (new_end - limit as i64 + 1).max(low_watermark);

            self.loading_message = format!(
                "Loading older messages from offset {} to {} (current oldest: {})...",
                new_start, new_end, last_msg.offset
            );

            if new_start <= new_end {
                // Load earlier messages
                let mut earlier_messages = Vec::new();
                // Don't expect exact count due to possible gaps in offsets
                let max_to_load = limit;

                let mut tpl = TopicPartitionList::new();
                tpl.add_partition_offset(
                    &self.topic_name,
                    partition_id,
                    Offset::Offset(new_start),
                )?;

                // Use connection manager's current profile credentials
                let profile_name = self
                    .connection_manager
                    .current_profile
                    .as_ref()
                    .context("No active profile selected")?
                    .clone();

                // Clone profile data to avoid borrow conflicts with get_password
                let (broker, user) = {
                    let profile = self
                        .connection_manager
                        .profiles
                        .get(&profile_name)
                        .context("Current profile not found")?;
                    (profile.broker.clone(), profile.user.clone())
                };

                let password = self
                    .connection_manager
                    .get_password(&profile_name)
                    .with_context(|| {
                        format!("Failed to get password for profile '{}'", profile_name)
                    })?;

                let consumer = create_kafka_consumer(&broker, &user, &password)?;

                consumer.assign(&tpl)?;

                let start = std::time::Instant::now();
                let mut consecutive_timeouts = 0;
                let mut has_received_any = false;

                while earlier_messages.len() < max_to_load
                    && start.elapsed() < Duration::from_secs(METADATA_TIMEOUT_SECS)
                {
                    match timeout(Duration::from_millis(100), consumer.recv()).await {
                        Ok(Ok(msg)) => {
                            let parsed = self.parse_message_metadata(&msg)?;
                            // Load messages in our target range
                            if parsed.offset >= new_start && parsed.offset <= new_end {
                                let offset = parsed.offset;

                                // Cache the message content
                                if let Some(payload) = msg.payload() {
                                    self.parse_and_cache_payload(payload, offset).await?;
                                }

                                earlier_messages.push(parsed);
                                consecutive_timeouts = 0;
                                has_received_any = true;
                            }
                        }
                        Ok(Err(e)) => {
                            self.log_error(format!("Earlier message fetch error: {}", e));
                            if has_received_any {
                                break;
                            }
                        }
                        Err(_) => {
                            consecutive_timeouts += 1;
                            // Only terminate early if we've received messages
                            if has_received_any && consecutive_timeouts >= 5 {
                                break;
                            }
                        }
                    }
                }

                // Sort messages by offset descending (newest first) and append to existing ones
                earlier_messages.sort_by_key(|m| std::cmp::Reverse(m.offset));

                // Deserialize Avro keys if we have a schema registry
                self.deserialize_avro_keys(&mut earlier_messages).await?;

                if !earlier_messages.is_empty() {
                    let oldest_new = earlier_messages.iter().map(|m| m.offset).min().unwrap_or(0);
                    let newest_new = earlier_messages.iter().map(|m| m.offset).max().unwrap_or(0);
                    self.loading_message = format!(
                        "Loaded {} older messages (offsets {}-{})",
                        earlier_messages.len(),
                        oldest_new,
                        newest_new
                    );
                    self.debug_message = format!(
                        "Loaded {} msgs from {}-{}, elapsed: {:?}",
                        earlier_messages.len(),
                        oldest_new,
                        newest_new,
                        start.elapsed()
                    );
                    // Append older messages to the end (they're older than what we have)
                    self.partitions[partition_idx]
                        .messages
                        .extend(earlier_messages);
                } else {
                    self.loading_message = "No older messages found".to_string();
                    self.debug_message = format!(
                        "No messages in range {}-{}, elapsed: {:?}",
                        new_start,
                        new_end,
                        start.elapsed()
                    );
                }
            }
        } else {
            self.loading_message = "No messages loaded yet".to_string();
        }
        Ok(())
    }

    fn parse_message_metadata(&self, msg: &BorrowedMessage) -> Result<MessageMetadata> {
        let raw_key = msg.key().map(|k| k.to_vec());
        let key_avro_schema_id = raw_key
            .as_ref()
            .and_then(|k| crate::schema_registry::is_confluent_avro(k));

        let key = raw_key.as_ref().map(|k| {
            // Check if key is Avro encoded
            if let Some(schema_id) = crate::schema_registry::is_confluent_avro(k) {
                format!("[Avro Key - Schema ID: {}]", schema_id)
            } else {
                String::from_utf8_lossy(k).to_string()
            }
        });

        let (value_size, is_binary, avro_schema_id) = if let Some(payload) = msg.payload() {
            // Check for Confluent Avro wire format
            let schema_id = crate::schema_registry::is_confluent_avro(payload);

            if schema_id.is_some() {
                // It's Avro
                (payload.len(), true, schema_id)
            } else {
                // Check if it's binary by sampling first 100 bytes
                let sample_size = payload.len().min(100);
                let sample = &payload[..sample_size];
                (payload.len(), is_binary(sample), None)
            }
        } else {
            (0, false, None)
        };

        let mut headers = Vec::new();
        if let Some(hdrs) = msg.headers() {
            for i in 0..hdrs.count() {
                let hdr = hdrs.get(i);
                headers.push((
                    hdr.key.to_string(),
                    String::from_utf8_lossy(hdr.value.unwrap_or(b"")).to_string(),
                ));
            }
        }

        Ok(MessageMetadata {
            partition: msg.partition(),
            offset: msg.offset(),
            timestamp: msg.timestamp().to_millis().unwrap_or(0),
            key,
            key_avro_schema_id,
            raw_key,
            value_size,
            is_binary,
            avro_schema_id,
            headers,
        })
    }

    async fn load_message_content(
        &mut self,
        partition_id: i32,
        offset: i64,
    ) -> Result<MessageContent> {
        // Check cache first
        if let Some(content) = self.message_cache.get(&offset) {
            self.stats.cache_hits += 1;
            return Ok(content.clone());
        }

        self.stats.cache_misses += 1;

        // Load from Kafka
        let mut tpl = TopicPartitionList::new();
        tpl.add_partition_offset(&self.topic_name, partition_id, Offset::Offset(offset))?;

        // Use connection manager's current profile credentials
        let profile_name = self
            .connection_manager
            .current_profile
            .as_ref()
            .context("No active profile selected")?
            .clone();

        // Clone profile data to avoid borrow conflicts with get_password
        let (broker, user) = {
            let profile = self
                .connection_manager
                .profiles
                .get(&profile_name)
                .context("Current profile not found")?;
            (profile.broker.clone(), profile.user.clone())
        };

        let password = self
            .connection_manager
            .get_password(&profile_name)
            .with_context(|| format!("Failed to get password for profile '{}'", profile_name))?;

        let consumer = create_kafka_consumer(&broker, &user, &password)?;

        consumer.assign(&tpl)?;

        match timeout(Duration::from_secs(METADATA_TIMEOUT_SECS), consumer.recv()).await {
            Ok(Ok(msg)) => {
                let value = if let Some(payload) = msg.payload() {
                    // Check for Avro format
                    if let Some(schema_id) = crate::schema_registry::is_confluent_avro(payload) {
                        // Try to deserialize Avro
                        match self.deserialize_avro(payload, schema_id).await {
                            Ok(json_value) => {
                                format!(
                                    "[Avro - Schema ID: {}]\n{}",
                                    schema_id,
                                    serde_json::to_string_pretty(&json_value)
                                        .unwrap_or_else(|_| "Failed to format JSON".to_string())
                                )
                            }
                            Err(e) => {
                                // Fallback to hex dump if deserialization fails
                                format!(
                                    "[Avro - Schema ID: {} - Deserialization failed: {}]\n{}",
                                    schema_id,
                                    e,
                                    hex_dump(payload)
                                )
                            }
                        }
                    } else {
                        // Not Avro, check if it's binary
                        if is_binary(payload) {
                            format!(
                                "[Binary data - {} bytes]\n{}",
                                payload.len(),
                                hex_dump(payload)
                            )
                        } else {
                            String::from_utf8_lossy(payload).to_string()
                        }
                    }
                } else {
                    String::new()
                };

                let raw_bytes = msg.payload().map(|p| {
                    self.stats.bytes_read += p.len() as u64;
                    p.to_vec()
                });
                let content = MessageContent {
                    offset,
                    value,
                    raw_bytes,
                };

                // Cache it (limit cache size to 100 messages)
                if self.message_cache.len() >= 100 {
                    // Remove oldest entry
                    if let Some(&oldest_offset) = self.message_cache.keys().next() {
                        self.message_cache.remove(&oldest_offset);
                    }
                }
                self.message_cache.insert(offset, content.clone());

                Ok(content)
            }
            Ok(Err(e)) => Err(anyhow::anyhow!("Error loading message: {}", e)),
            Err(_) => Err(anyhow::anyhow!("Timeout loading message")),
        }
    }

    async fn deserialize_avro(&self, data: &[u8], schema_id: i32) -> Result<Value> {
        // Check if we have schema registry configured
        let registry = self
            .schema_registry
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("Schema Registry not configured"))?;

        // Extract the Avro payload (skip magic byte and schema ID)
        let avro_data = crate::schema_registry::extract_avro_payload(data)
            .ok_or_else(|| anyhow::anyhow!("Invalid Avro wire format"))?;

        // Fetch the schema
        let schema_json = registry
            .get_schema_by_id(schema_id)
            .await
            .map_err(|e| anyhow::anyhow!("Failed to fetch schema: {}", e))?;

        // Parse the schema
        let schema = apache_avro::Schema::parse_str(&schema_json)
            .map_err(|e| anyhow::anyhow!("Failed to parse schema: {}", e))?;

        // Deserialize the Avro data directly from raw bytes (no container file format)
        let value = apache_avro::from_avro_datum(&schema, &mut &avro_data[..], None)
            .map_err(|e| anyhow::anyhow!("Failed to decode Avro data: {}", e))?;

        // Convert Avro value to JSON
        let json_value = avro_value_to_json(&value);
        Ok(json_value)
    }

    fn save_message_to_file(&mut self, partition_idx: usize, message_idx: usize) -> Result<String> {
        // Get the message
        if partition_idx >= self.partitions.len() {
            return Err(anyhow::anyhow!("Invalid partition index"));
        }

        let partition = &self.partitions[partition_idx];
        if message_idx >= partition.messages.len() {
            return Err(anyhow::anyhow!("Invalid message index"));
        }

        let message = &partition.messages[message_idx];

        // Create export directory
        let export_dir = std::path::Path::new("./kafka-exports");
        std::fs::create_dir_all(export_dir)?;

        // Generate filename with sanitized topic name
        let timestamp_str = chrono::DateTime::<Utc>::from_timestamp(message.timestamp / 1000, 0)
            .map(|dt| dt.format("%Y%m%d_%H%M%S").to_string())
            .unwrap_or_else(|| "unknown".to_string());

        let filename = format!(
            "{}_{}_{}_{}.json",
            sanitize_filename(&self.topic_name),
            partition.id,
            message.offset,
            timestamp_str
        );

        let filepath = export_dir.join(&filename);

        // Validate the final path is still within export directory
        // We need to check the parent directory since the file doesn't exist yet
        let canonical_export = export_dir.canonicalize()?;
        if let Some(parent) = filepath.parent() {
            let canonical_parent = parent
                .canonicalize()
                .unwrap_or_else(|_| parent.to_path_buf());
            if !canonical_parent.starts_with(&canonical_export) {
                return Err(anyhow::anyhow!(
                    "Invalid export path: potential path traversal detected"
                ));
            }
        }

        // Get message value
        let value_json: serde_json::Value =
            if let Some(content) = self.message_cache.get(&message.offset) {
                // Try to parse as JSON
                serde_json::from_str(&content.value)
                    .unwrap_or_else(|_| serde_json::Value::String(content.value.clone()))
            } else {
                serde_json::Value::String("[Not loaded]".to_string())
            };

        // Create complete message structure
        let export_data = serde_json::json!({
            "topic": self.topic_name,
            "partition": partition.id,
            "offset": message.offset,
            "timestamp": chrono::DateTime::<Utc>::from_timestamp(message.timestamp / 1000, 0)
                .map(|dt| dt.to_rfc3339())
                .unwrap_or_else(|| "unknown".to_string()),
            "key": message.key,
            "headers": message.headers,
            "value": value_json
        });

        // Write to file
        let json_string = serde_json::to_string_pretty(&export_data)?;
        std::fs::write(&filepath, json_string)?;

        Ok(filename)
    }

    fn search_messages(&mut self, search_all_partitions: bool) {
        if self.search_query.is_empty() {
            self.search_results.clear();
            return;
        }

        self.search_results.clear();
        let query = self.search_query.to_lowercase();

        // Determine which partitions to search
        let partitions_to_search: Vec<usize> = if search_all_partitions {
            // Search all partitions that have loaded messages
            (0..self.partitions.len())
                .filter(|&idx| !self.partitions[idx].messages.is_empty())
                .collect()
        } else {
            // Search only current partition
            vec![self.selected_partition]
        };

        if partitions_to_search.is_empty() {
            return;
        }

        let mut search_count = 0;
        let max_search_results = 100; // Limit results for performance

        'search_loop: for &partition_idx in &partitions_to_search {
            let partition = &self.partitions[partition_idx];

            for (msg_idx, msg) in partition.messages.iter().enumerate() {
                let mut found = false;

                // Search in key
                if msg
                    .key
                    .as_ref()
                    .is_some_and(|k| k.to_lowercase().contains(&query))
                {
                    found = true;
                }

                // Search in cached value content if available
                if !found {
                    if let Some(content) = self.message_cache.get(&msg.offset) {
                        if content.value.to_lowercase().contains(&query) {
                            found = true;
                        }
                    }
                    // Note: We don't load uncached content - search only in loaded data
                }

                if found {
                    self.search_results.push((partition_idx, msg_idx));
                    search_count += 1;

                    // Stop after max results to keep it responsive
                    if search_count >= max_search_results {
                        break 'search_loop;
                    }
                }
            }
        }

        if !self.search_results.is_empty() {
            self.current_search_index = 0;
            self.jump_to_search_result(0);
        }
    }

    fn jump_to_search_result(&mut self, index: usize) {
        if let Some((partition_idx, msg_idx)) = self.search_results.get(index) {
            // Switch to the partition containing the search result
            if *partition_idx != self.selected_partition {
                self.selected_partition = *partition_idx;
            }
            self.selected_message = Some(*msg_idx);
            self.message_list_state.select(Some(*msg_idx));
        }
    }

    fn current_partition_messages(&self) -> &Vec<MessageMetadata> {
        self.partitions
            .get(self.selected_partition)
            .map(|p| &p.messages)
            .unwrap_or(&self.empty_messages)
    }

    /// Parses message payload and caches it, handling Avro deserialization if needed
    async fn parse_and_cache_payload(&mut self, payload: &[u8], offset: i64) -> Result<String> {
        let value = if let Some(schema_id) = crate::schema_registry::is_confluent_avro(payload) {
            // Try to deserialize Avro for caching
            if let Some(_registry) = &self.schema_registry {
                match self.deserialize_avro(payload, schema_id).await {
                    Ok(json_value) => serde_json::to_string(&json_value)
                        .unwrap_or_else(|_| format!("[Avro - Schema ID: {}]", schema_id)),
                    Err(e) => format!(
                        "[Avro - Schema ID: {} - Failed to deserialize: {}]",
                        schema_id, e
                    ),
                }
            } else {
                format!("[Avro - Schema ID: {} - No Schema Registry]", schema_id)
            }
        } else {
            String::from_utf8_lossy(payload).to_string()
        };

        let content = MessageContent {
            offset,
            value: value.clone(),
            raw_bytes: Some(payload.to_vec()),
        };

        // Update stats
        self.stats.bytes_read += payload.len() as u64;

        // Cache it (limit cache size to 1000 messages)
        if self.message_cache.len() >= 1000 {
            // Remove oldest entry
            if let Some(&oldest_offset) = self.message_cache.keys().next() {
                self.message_cache.remove(&oldest_offset);
            }
        }
        self.message_cache.insert(offset, content);

        Ok(value)
    }

    /// Deserializes Avro keys for a list of message metadata
    async fn deserialize_avro_keys(&mut self, messages: &mut [MessageMetadata]) -> Result<()> {
        if self.schema_registry.is_some() {
            for msg in messages.iter_mut() {
                if let (Some(schema_id), Some(raw_key)) = (msg.key_avro_schema_id, &msg.raw_key) {
                    match self.deserialize_avro(raw_key, schema_id).await {
                        Ok(json_value) => {
                            msg.key =
                                Some(serde_json::to_string(&json_value).unwrap_or_else(|_| {
                                    format!("[Avro Key - Schema ID: {}]", schema_id)
                                }));
                        }
                        Err(e) => {
                            msg.key = Some(format!(
                                "[Avro Key - Schema ID: {} - Error: {}]",
                                schema_id, e
                            ));
                        }
                    }
                }
            }
        }
        Ok(())
    }

    /// Prepares message list for global search by collecting message metadata
    fn prepare_search_messages(&self) -> Vec<(usize, i32, usize, i64, i64, Option<String>)> {
        let mut messages_to_search = Vec::new();
        for (partition_idx, partition) in self.partitions.iter().enumerate() {
            for (msg_idx, msg) in partition.messages.iter().enumerate() {
                messages_to_search.push((
                    partition_idx,
                    partition.id,
                    msg_idx,
                    msg.offset,
                    msg.timestamp,
                    msg.key.clone(),
                ));
            }
        }
        messages_to_search
    }

    /// Initializes global search state and resets counters
    fn init_search_state(&mut self) {
        self.global_search.is_searching = true;
        self.global_search.results.clear();
        self.global_search.selected_result = 0;
        self.global_search.partitions_searched = 0;
        self.global_search.total_partitions = self.partitions.len();
        self.global_search.cancel_requested = false;
        self.global_search.messages_processed = 0;
        self.global_search.estimated_total = 0;
    }

    /// Loads messages for empty partitions during Quick search
    async fn ensure_partitions_loaded(&mut self, strategy: &SearchStrategy) -> Result<()> {
        if matches!(strategy, SearchStrategy::Quick) {
            let partitions_to_load: Vec<usize> = (0..self.partitions.len())
                .filter(|&idx| self.partitions[idx].messages.is_empty())
                .collect();

            for partition_idx in partitions_to_load {
                self.load_messages(partition_idx, GLOBAL_SEARCH_MESSAGE_COUNT)
                    .await?;
            }
        }
        Ok(())
    }

    async fn start_global_search(&mut self) -> Result<()> {
        if self.global_search.query.is_empty() {
            return Ok(());
        }

        // Cancel any existing search
        if let Some(handle) = self.global_search.search_handle.take() {
            handle.abort();
        }

        self.init_search_state();

        let query = self.global_search.query.to_lowercase();
        let strategy = self.global_search.strategy.clone();

        // First, ensure partitions have some messages loaded for Quick search
        self.ensure_partitions_loaded(&strategy).await?;

        // Prepare data for background search
        let messages_to_search = self.prepare_search_messages();
        let total_messages = messages_to_search.len() as u64;
        self.global_search.estimated_total = total_messages;

        // Clone necessary data for the background task
        let cache_snapshot: HashMap<i64, String> = self
            .message_cache
            .iter()
            .map(|(k, v)| (*k, v.value.clone()))
            .collect();

        self.debug_message = format!(
            "Cache size: {}, Messages to search: {}",
            cache_snapshot.len(),
            total_messages
        );

        let topic_name = self.topic_name.clone();

        // Get credentials from connection manager's current profile
        let profile_name = self
            .connection_manager
            .current_profile
            .as_ref()
            .context("No active profile selected")?
            .clone();
        let profile = self
            .connection_manager
            .profiles
            .get(&profile_name)
            .context("Current profile not found")?;

        let broker = profile.broker.clone();
        let user = profile.user.clone();
        let password = self
            .connection_manager
            .get_password(&profile_name)
            .with_context(|| format!("Failed to get password for profile '{}'", profile_name))?;
        let schema_registry = self.schema_registry.clone();

        // Create channel for results
        let (tx, rx) = mpsc::unbounded_channel::<SearchUpdate>();

        // Spawn background search task
        let search_handle = spawn_search_task(SearchTaskConfig {
            messages_to_search,
            query,
            strategy,
            cache_snapshot,
            topic_name,
            broker,
            user,
            password,
            schema_registry,
            tx,
        });

        self.global_search.search_handle = Some(search_handle);
        self.global_search.results_receiver = Some(rx);

        // Give UI a chance to update before processing results
        tokio::time::sleep(Duration::from_millis(10)).await;

        Ok(())
    }

    fn cancel_search(&mut self) {
        self.global_search.cancel_requested = true;
        if let Some(handle) = self.global_search.search_handle.take() {
            handle.abort();
        }
        self.global_search.is_searching = false;
    }

    fn process_search_updates(&mut self) {
        // Take the receiver to avoid borrow issues
        if let Some(mut rx) = self.global_search.results_receiver.take() {
            let mut errors = Vec::new();
            let mut should_complete = false;

            // Process all pending updates
            while let Ok(update) = rx.try_recv() {
                match update {
                    SearchUpdate::Result(result) => {
                        self.global_search.results.push(result);
                        // Update debug message to show results are being received
                        self.debug_message =
                            format!("Results: {}", self.global_search.results.len());
                    }
                    SearchUpdate::Progress {
                        partition_idx,
                        messages_processed,
                    } => {
                        self.global_search.partitions_searched = partition_idx + 1;
                        self.global_search.messages_processed = messages_processed;
                    }
                    SearchUpdate::Error(err) => {
                        errors.push(err);
                    }
                    SearchUpdate::Complete => {
                        should_complete = true;
                        self.global_search.is_searching = false;
                    }
                }
            }

            // Put the receiver back if not complete
            if !should_complete {
                self.global_search.results_receiver = Some(rx);
            }

            // Log any errors
            for err in errors {
                self.log_error(err);
            }
        }
    }

    async fn analyze_keys(&mut self) -> Result<()> {
        self.loading_message = "Analyzing keys across all partitions...".to_string();
        self.mode = AppMode::Loading;

        // Load messages from all partitions
        let mut key_to_partitions: std::collections::HashMap<String, Vec<i32>> =
            std::collections::HashMap::new();
        let mut total_messages = 0;
        let mut messages_with_keys = 0;

        for (idx, partition) in self.partitions.clone().iter().enumerate() {
            // Load messages if not already loaded
            if partition.messages.is_empty() {
                self.load_messages(idx, DEFAULT_MESSAGE_LOAD_COUNT).await?;
            }

            // Analyze keys in this partition
            for msg in &self.partitions[idx].messages {
                total_messages += 1;
                if let Some(key) = &msg.key {
                    messages_with_keys += 1;
                    key_to_partitions
                        .entry(key.clone())
                        .or_default()
                        .push(partition.id);
                }
            }
        }

        // Analyze results
        let mut unique_keys = 0;
        let mut keys_in_multiple_partitions = 0;
        let mut partition_distribution = String::new();

        for (key, partitions) in &key_to_partitions {
            unique_keys += 1;
            let unique_partitions: std::collections::HashSet<_> = partitions.iter().collect();
            if unique_partitions.len() > 1 {
                keys_in_multiple_partitions += 1;
                partition_distribution.push_str(&format!(
                    "\nKey '{}' found in partitions: {:?}",
                    key.chars().take(20).collect::<String>(),
                    unique_partitions
                ));
            }
        }

        // Prepare analysis result
        self.loading_message = format!(
            "Key Analysis Results:\n\
            Total messages analyzed: {}\n\
            Messages with keys: {} ({}%)\n\
            Unique keys found: {}\n\
            Keys in multiple partitions: {}\n\
            \n\
            Partitioning Strategy: {}\n\
            {}",
            total_messages,
            messages_with_keys,
            if total_messages > 0 {
                (messages_with_keys * 100) / total_messages
            } else {
                0
            },
            unique_keys,
            keys_in_multiple_partitions,
            if keys_in_multiple_partitions == 0 && messages_with_keys > 0 {
                "Key-based (each key goes to exactly one partition)"
            } else if keys_in_multiple_partitions > 0 {
                "NOT key-based (same key found in multiple partitions)"
            } else if messages_with_keys == 0 {
                "No keys found - likely round-robin or custom partitioning"
            } else {
                "Inconclusive"
            },
            if keys_in_multiple_partitions > 0 && partition_distribution.len() < 500 {
                partition_distribution
            } else {
                String::new()
            }
        );

        self.mode = AppMode::KeyAnalysis;
        Ok(())
    }

    async fn switch_topic(&mut self, topic_name: String) -> Result<()> {
        self.loading_message = format!("Switching to topic '{}'...", topic_name);
        self.mode = AppMode::Loading;

        self.topic_name = topic_name.clone();
        self.partitions.clear();
        self.message_cache.clear();
        self.search_results.clear();
        self.selected_partition = 0;
        self.selected_message = None;
        self.message_list_state.select(Some(0));

        self.load_partition_info().await?;

        if let Some(profile) = self.connection_manager.current_profile.clone() {
            self.connection_manager
                .add_recent_topic(profile.clone(), topic_name.clone());

            // Also save to config for persistence
            if let Ok(mut config) = Config::load() {
                config.add_recent_connection(profile, topic_name);
                if let Err(e) = config.save() {
                    self.log_error(format!("Failed to save recent connection: {}", e));
                }
            }
        }

        self.mode = AppMode::PartitionList;
        Ok(())
    }

    async fn switch_profile(&mut self, profile_name: &str) -> Result<()> {
        self.loading_message = format!("Connecting to '{}'...", profile_name);
        self.mode = AppMode::Loading;

        let consumer = self.connection_manager.connect_to_profile(profile_name)?;
        self.consumer = consumer;

        // Clone profile data to avoid borrow conflicts
        let (broker, user, schema_registry_url) =
            if let Some(profile) = self.connection_manager.profiles.get(profile_name) {
                (
                    profile.broker.clone(),
                    profile.user.clone(),
                    profile.schema_registry.clone(),
                )
            } else {
                return Ok(());
            };

        self.stats = AppStats::new(broker.clone());

        // Update schema registry for the new profile
        self.schema_registry = if let Some(schema_registry_url) = schema_registry_url {
            let password = self
                .connection_manager
                .get_password(profile_name)
                .unwrap_or_default();
            match SchemaRegistryClient::new(schema_registry_url.clone(), &user, &password) {
                Ok(client) => Some(Arc::new(client)),
                Err(e) => {
                    self.log_error(format!("Failed to initialize schema registry: {}", e));
                    None
                }
            }
        } else {
            None
        };

        self.connection_manager.list_topics(profile_name).await?;

        if !self.connection_manager.available_topics.is_empty() {
            let first_topic = self.connection_manager.available_topics[0].clone();
            self.switch_topic(first_topic).await?;
        }

        self.connection_manager
            .cleanup_old_connections(CONNECTION_POOL_SIZE);

        Ok(())
    }

    fn get_filtered_topics(&self) -> Vec<String> {
        if self.topic_filter.is_empty() {
            self.connection_manager.available_topics.clone()
        } else {
            let filter = self.topic_filter.to_lowercase();
            self.connection_manager
                .available_topics
                .iter()
                .filter(|t| t.to_lowercase().contains(&filter))
                .cloned()
                .collect()
        }
    }

    async fn test_connection(&mut self, profile: &BrokerProfile) -> Result<String> {
        let start = Instant::now();

        // Get password from keyring
        let password = match self.connection_manager.get_password(&profile.name) {
            Ok(pwd) => pwd,
            Err(_) => {
                return Ok(format!(
                    "✗ Password not found in keyring for profile '{}'",
                    profile.name
                ))
            }
        };

        // Try to create consumer
        let consumer = match create_kafka_consumer(&profile.broker, &profile.user, &password) {
            Ok(c) => c,
            Err(e) => return Ok(format!("✗ Failed to create consumer: {}", e)),
        };

        // Try to fetch metadata
        match consumer.fetch_metadata(None, Duration::from_secs(METADATA_TIMEOUT_SECS)) {
            Ok(metadata) => {
                let topics: Vec<String> = metadata
                    .topics()
                    .iter()
                    .map(|t| t.name().to_string())
                    .filter(|name| !name.starts_with("__"))
                    .collect();

                let elapsed = start.elapsed();
                Ok(format!(
                    "✓ Connected successfully!\n\n{} topics accessible\nLatency: {}ms",
                    topics.len(),
                    elapsed.as_millis()
                ))
            }
            Err(e) => Ok(format!("✗ Connection failed: {}", e)),
        }
    }
}

/// Helper function to create a Kafka consumer with standard configuration
fn create_kafka_consumer(broker: &str, user: &str, password: &str) -> Result<StreamConsumer> {
    ClientConfig::new()
        .set("bootstrap.servers", broker)
        .set("security.protocol", "SASL_SSL")
        .set("sasl.mechanism", "SCRAM-SHA-512")
        .set("sasl.username", user)
        .set("sasl.password", password)
        .set("group.id", format!("kafkatui-{}", Uuid::new_v4()))
        .set("enable.auto.commit", "false")
        .set("session.timeout.ms", SESSION_TIMEOUT_MS)
        .set_log_level(RDKafkaLogLevel::Critical)
        .create()
        .context("Failed to create consumer")
}

// Static function for loading message content from background tasks
async fn load_message_content_static(
    topic_name: &str,
    partition_id: i32,
    offset: i64,
    broker: &str,
    user: &str,
    password: &str,
    schema_registry: Option<&Arc<SchemaRegistryClient>>,
) -> Result<String> {
    let mut tpl = TopicPartitionList::new();
    tpl.add_partition_offset(topic_name, partition_id, Offset::Offset(offset))?;

    let consumer = create_kafka_consumer(broker, user, password)?;

    consumer.assign(&tpl)?;

    match timeout(Duration::from_secs(METADATA_TIMEOUT_SECS), consumer.recv()).await {
        Ok(Ok(msg)) => {
            let value = if let Some(payload) = msg.payload() {
                // Check for Avro format
                if let Some(schema_id) = crate::schema_registry::is_confluent_avro(payload) {
                    if let Some(registry) = schema_registry {
                        // Try to deserialize Avro
                        match deserialize_avro_static(payload, schema_id, registry).await {
                            Ok(json_value) => {
                                format!(
                                    "[Avro - Schema ID: {}]\n{}",
                                    schema_id,
                                    serde_json::to_string_pretty(&json_value)
                                        .unwrap_or_else(|_| "Failed to format JSON".to_string())
                                )
                            }
                            Err(e) => {
                                format!("[Avro - Schema ID: {} - Deserialization Failed]\nError: {}\nRaw bytes (hex): {}", 
                                    schema_id,
                                    e,
                                    payload.iter().map(|b| format!("{:02x}", b)).collect::<String>()
                                )
                            }
                        }
                    } else {
                        format!(
                            "[Avro - Schema ID: {} - No Schema Registry]\nRaw bytes (hex): {}",
                            schema_id,
                            payload
                                .iter()
                                .map(|b| format!("{:02x}", b))
                                .collect::<String>()
                        )
                    }
                } else {
                    String::from_utf8_lossy(payload).to_string()
                }
            } else {
                String::new()
            };

            Ok(value)
        }
        Ok(Err(e)) => Err(anyhow::anyhow!("Kafka error: {}", e)),
        Err(_) => Err(anyhow::anyhow!("Timeout loading message")),
    }
}

async fn deserialize_avro_static(
    payload: &[u8],
    schema_id: i32,
    registry: &Arc<SchemaRegistryClient>,
) -> Result<Value> {
    // Skip magic byte and schema ID (5 bytes total)
    let avro_payload = &payload[5..];

    // Get schema from registry
    let schema_str = registry.get_schema_by_id(schema_id).await?;
    let schema = apache_avro::Schema::parse_str(&schema_str)?;

    // Create reader and deserialize
    let mut reader = apache_avro::Reader::with_schema(&schema, avro_payload)?;

    if let Some(value) = reader.next() {
        match value {
            Ok(avro_value) => {
                return Ok(avro_value_to_json(&avro_value));
            }
            Err(e) => return Err(anyhow::anyhow!("Failed to read Avro value: {}", e)),
        }
    }

    Err(anyhow::anyhow!("No Avro value found in payload"))
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();

    // Check if we have any way to connect
    let no_args_or_env = args.broker.is_none()
        && args.user.is_none()
        && args.password.is_none()
        && args.topic.is_none()
        && env::var("KAFKA_BROKER").is_err()
        && env::var("KAFKA_USER").is_err()
        && env::var("KAFKA_PASSWORD").is_err();

    // Try to restore last session if no args provided
    let (broker, user, password, topic, restored_profile_name) = if no_args_or_env {
        // Try to load from last session, but if that fails, we'll start in profile picker mode
        if let Ok(config) = Config::load() {
            if !config.profiles.is_empty() && config.recent_connections.is_empty() {
                // Has profiles but no recent connection - will start in profile picker mode
                (
                    "".to_string(),
                    "".to_string(),
                    "".to_string(),
                    "".to_string(),
                    None,
                )
            } else if let Some(recent) = config.recent_connections.first() {
                if let Some(profile) = config.profiles.get(&recent.profile_name) {
                    // Try to get password from keyring
                    if let Ok(password) = config::get_password(&recent.profile_name) {
                        (
                            profile.broker.clone(),
                            profile.user.clone(),
                            password,
                            recent.topic.clone(),
                            Some(recent.profile_name.clone()),
                        )
                    } else {
                        // Password not in keyring - will start in profile picker mode
                        (
                            "".to_string(),
                            "".to_string(),
                            "".to_string(),
                            "".to_string(),
                            None,
                        )
                    }
                } else {
                    // Profile not found - will start in profile picker mode
                    (
                        "".to_string(),
                        "".to_string(),
                        "".to_string(),
                        "".to_string(),
                        None,
                    )
                }
            } else {
                // No recent connections - will start in profile picker mode
                (
                    "".to_string(),
                    "".to_string(),
                    "".to_string(),
                    "".to_string(),
                    None,
                )
            }
        } else {
            // No config - will start in profile picker mode
            (
                "".to_string(),
                "".to_string(),
                "".to_string(),
                "".to_string(),
                None,
            )
        }
    } else {
        // Use provided args or env vars
        let broker = args
            .broker
            .or_else(|| env::var("KAFKA_BROKER").ok())
            .context("Broker not provided. Use -b or KAFKA_BROKER env var")?;

        let user = args
            .user
            .or_else(|| env::var("KAFKA_USER").ok())
            .context("User not provided. Use -u or KAFKA_USER env var")?;

        let password = args
            .password
            .or_else(|| env::var("KAFKA_PASSWORD").ok())
            .context("Password not provided. Use -p or KAFKA_PASSWORD env var")?;

        let consumer = create_kafka_consumer(&broker, &user, &password)?;

        let topic = if let Some(t) = args.topic {
            t
        } else {
            let metadata =
                consumer.fetch_metadata(None, Duration::from_secs(METADATA_TIMEOUT_SECS * 2))?;
            let topics: Vec<String> = metadata
                .topics()
                .iter()
                .map(|t| t.name().to_string())
                .filter(|name| !name.starts_with("__"))
                .collect();

            if topics.is_empty() {
                anyhow::bail!("No accessible topics found");
            } else if topics.len() == 1 {
                topics[0].clone()
            } else {
                anyhow::bail!(
                    "Multiple topics found: {:?}. Please specify one with -t",
                    topics
                );
            }
        };

        (broker, user, password, topic, None)
    };

    // Check if we should start in profile picker mode (no credentials)
    let start_in_picker_mode = broker.is_empty();

    // Determine if we're in setup mode and find a valid profile in one pass
    // This avoids redundant keychain accesses
    let profile_with_password = if start_in_picker_mode {
        if let Ok(config) = Config::load() {
            config.profiles.iter().find_map(|(name, profile)| {
                config::get_password(name)
                    .ok()
                    .map(|password| (name.clone(), profile.clone(), password))
            })
        } else {
            None
        }
    } else {
        None
    };

    let setup_mode = start_in_picker_mode && profile_with_password.is_none();

    // Track profile+password from picker mode to seed the cache later
    let mut picker_cache_seed: Option<(String, String)> = None;

    let (consumer, initial_topic, initial_broker) = if !start_in_picker_mode {
        let consumer = create_kafka_consumer(&broker, &user, &password)?;
        (consumer, topic.clone(), broker.clone())
    } else if setup_mode {
        // Setup mode - create minimal dummy consumer, will connect after profile creation
        // This consumer won't be used until user creates and saves a profile
        let dummy_consumer = ClientConfig::new()
            .set("bootstrap.servers", "localhost:9092")
            .set("group.id", "setup-dummy")
            .set_log_level(RDKafkaLogLevel::Emerg) // Suppress all connection logs
            .create::<StreamConsumer>()?;
        (dummy_consumer, String::new(), String::new())
    } else {
        // Profile picker mode - use the profile+password we already found
        let (picker_profile_name, profile, pwd) =
            profile_with_password.context("No profiles with valid keychain passwords found")?;

        let consumer = create_kafka_consumer(&profile.broker, &profile.user, &pwd)?;

        let metadata = consumer.fetch_metadata(None, Duration::from_secs(5))?;
        let topics: Vec<String> = metadata
            .topics()
            .iter()
            .map(|t| t.name().to_string())
            .filter(|name| !name.starts_with("__"))
            .collect();

        let first_topic = topics
            .first()
            .context("No topics found for profile")?
            .clone();

        // Save for cache seeding after ConnectionManager is created
        picker_cache_seed = Some((picker_profile_name, pwd));

        (consumer, first_topic, profile.broker.clone())
    };

    enable_raw_mode()?;
    let mut stdout = io::stdout();
    execute!(stdout, EnterAlternateScreen)?;
    let backend = CrosstermBackend::new(stdout);
    let mut terminal = Terminal::new(backend)?;
    terminal.clear()?;

    // Initialize Schema Registry if URL is provided (skip if in picker mode)
    let schema_registry = if !start_in_picker_mode {
        if let Ok(url) = env::var("KAFKA_SCHEMA_REGISTRY_URL") {
            match SchemaRegistryClient::new(url, &user, &password) {
                Ok(client) => Some(Arc::new(client)),
                Err(_) => None,
            }
        } else {
            None
        }
    } else {
        None
    };

    let mut app = App::new(initial_topic, consumer, initial_broker);
    if let Some(registry) = schema_registry {
        app = app.with_schema_registry(registry);
    }

    // Load config and populate connection manager
    if let Ok(config) = Config::load() {
        for (name, mut profile) in config.profiles {
            profile.name = name.clone();
            app.connection_manager.profiles.insert(name, profile);
        }
    }

    // Seed the password cache with passwords already retrieved at startup
    if let Some(ref profile_name) = restored_profile_name {
        if !password.is_empty() {
            app.connection_manager
                .seed_password(profile_name, password.clone());
        }
    }
    if let Some((ref profile_name, ref pwd)) = picker_cache_seed {
        app.connection_manager
            .seed_password(profile_name, pwd.clone());
    }

    // Set the current profile based on whether we restored from last session
    if setup_mode {
        // Setup mode - open profile editor immediately to create first profile
        app.mode = AppMode::ProfileEditor;
        app.profile_editor = Some(ProfileEditor::new_create());
    } else if !start_in_picker_mode {
        let current_profile_name = if let Some(profile_name) = restored_profile_name {
            // We restored from a saved profile, use that
            // Initialize schema registry for restored profile
            // Clone profile data to avoid borrow conflict with get_password
            let sr_info = app
                .connection_manager
                .profiles
                .get(&profile_name)
                .and_then(|profile| {
                    profile
                        .schema_registry
                        .as_ref()
                        .map(|url| (url.clone(), profile.user.clone()))
                });
            if let Some((schema_registry_url, user)) = sr_info {
                if let Ok(pwd) = app.connection_manager.get_password(&profile_name) {
                    match SchemaRegistryClient::new(schema_registry_url, &user, &pwd) {
                        Ok(client) => {
                            app.schema_registry = Some(Arc::new(client));
                        }
                        Err(e) => {
                            app.log_error(format!("Failed to initialize schema registry: {}", e));
                        }
                    }
                }
            }
            profile_name
        } else {
            // We connected via args/env vars, create or use "current" profile
            let current_name = "current".to_string();
            if !app.connection_manager.profiles.contains_key(&current_name) {
                let current_profile = BrokerProfile {
                    name: current_name.clone(),
                    broker: broker.clone(),
                    user: user.clone(),
                    schema_registry: env::var("KAFKA_SCHEMA_REGISTRY_URL").ok(),
                    favorite_topics: vec![topic.clone()],
                };
                // Save password to keyring for the "current" profile
                if config::save_password(&current_name, &password).is_ok() {
                    // Seed cache with the password we just saved
                    app.connection_manager
                        .seed_password(&current_name, password.clone());
                }
                app.connection_manager
                    .profiles
                    .insert(current_name.clone(), current_profile);
            }
            current_name
        };

        app.connection_manager.current_profile = Some(current_profile_name);
        app.connection_manager.current_topic = Some(topic.clone());
    } else {
        // Start in profile picker mode
        app.mode = AppMode::BrokerSwitcher;
    }

    // List available topics for current connection (skip in setup mode)
    if !setup_mode {
        if let Ok(metadata) = app
            .consumer
            .fetch_metadata(None, Duration::from_secs(METADATA_TIMEOUT_SECS))
        {
            let topics: Vec<String> = metadata
                .topics()
                .iter()
                .map(|t| t.name().to_string())
                .filter(|name| !name.starts_with("__"))
                .collect();
            app.connection_manager.available_topics = topics;
        }

        app.load_partition_info().await?;
    }

    let res = run_app(&mut terminal, &mut app).await;

    disable_raw_mode()?;
    execute!(terminal.backend_mut(), LeaveAlternateScreen)?;
    terminal.show_cursor()?;

    res
}

/// Handles key events for BrokerSwitcher mode
async fn handle_broker_switcher_input(app: &mut App, key_code: KeyCode) -> Result<bool> {
    match key_code {
        KeyCode::Esc => {
            app.mode = AppMode::PartitionList;
        }
        KeyCode::Up => {
            let profiles_count = app.connection_manager.profiles.len();
            if profiles_count > 0 {
                let current = app.broker_switcher_state.selected().unwrap_or(0);
                let new_index = if current == 0 {
                    profiles_count - 1
                } else {
                    current - 1
                };
                app.broker_switcher_state.select(Some(new_index));
            }
        }
        KeyCode::Down => {
            let profiles_count = app.connection_manager.profiles.len();
            if profiles_count > 0 {
                let current = app.broker_switcher_state.selected().unwrap_or(0);
                let new_index = (current + 1) % profiles_count;
                app.broker_switcher_state.select(Some(new_index));
            }
        }
        KeyCode::Enter => {
            if let Some(selected) = app.broker_switcher_state.selected() {
                let profile_names: Vec<String> =
                    app.connection_manager.profiles.keys().cloned().collect();
                if let Some(profile_name) = profile_names.get(selected) {
                    // Clear cached connection to force fresh connection with latest profile settings
                    app.connection_manager
                        .active_connections
                        .remove(profile_name);
                    app.switch_profile(profile_name).await?;
                    app.mode = AppMode::PartitionList;
                }
            }
        }
        KeyCode::Char('n') => {
            app.profile_editor = Some(ProfileEditor::new_create());
            app.mode = AppMode::ProfileEditor;
        }
        KeyCode::Char('e') => {
            if let Some(selected) = app.broker_switcher_state.selected() {
                let profile_names: Vec<String> =
                    app.connection_manager.profiles.keys().cloned().collect();
                if let Some(profile_name) = profile_names.get(selected) {
                    // Clone profile and fetch password to avoid borrow conflicts
                    let profile = app.connection_manager.profiles.get(profile_name).cloned();
                    if let Some(profile) = profile {
                        let password = app
                            .connection_manager
                            .get_password(profile_name)
                            .unwrap_or_default();
                        app.profile_editor = Some(ProfileEditor::new_edit(&profile, password));
                        app.mode = AppMode::ProfileEditor;
                    }
                }
            }
        }
        KeyCode::Char('d') => {
            if let Some(selected) = app.broker_switcher_state.selected() {
                let profile_names: Vec<String> =
                    app.connection_manager.profiles.keys().cloned().collect();
                if let Some(profile_name) = profile_names.get(selected) {
                    app.profile_to_delete = Some(profile_name.clone());
                    app.mode = AppMode::ProfileDeleting;
                }
            }
        }
        KeyCode::Char('t') => {
            if let Some(selected) = app.broker_switcher_state.selected() {
                let profile_names: Vec<String> =
                    app.connection_manager.profiles.keys().cloned().collect();
                if let Some(profile_name) = profile_names.get(selected) {
                    if let Some(profile) =
                        app.connection_manager.profiles.get(profile_name).cloned()
                    {
                        app.test_result = None;
                        app.mode = AppMode::ConnectionTesting;
                        // Run the test
                        match app.test_connection(&profile).await {
                            Ok(result) => app.test_result = Some(result),
                            Err(e) => app.test_result = Some(format!("✗ Test error: {}", e)),
                        }
                    }
                }
            }
        }
        _ => {}
    }
    Ok(false)
}

/// Saves a profile from the editor and handles setup mode if applicable
async fn save_profile_from_editor(app: &mut App, editor_clone: ProfileEditor) -> Result<()> {
    let profile = editor_clone.to_profile();
    let name = profile.name.clone();
    let original_name = editor_clone.original_name.clone();

    // Detect if this is setup mode (first profile creation)
    let is_setup_mode = app.connection_manager.profiles.is_empty() && original_name.is_none();

    // Save password to keyring
    let password_result = editor_clone.get_password();
    if let Ok(password) = password_result {
        if let Err(e) = config::save_password(&name, &password) {
            let error_msg = format!("Failed to save password to keyring: {}", e);
            app.log_error(error_msg);
        } else {
            // Seed cache with the password we just saved to avoid a redundant keychain prompt
            app.connection_manager
                .seed_password(&name, password.clone());
        }
    }

    // If editing existing profile
    if let Some(ref orig_name) = original_name {
        // Remove old profile from profiles map
        app.connection_manager.profiles.remove(orig_name);

        // Remove cached connection (important: forces reconnect with new credentials/broker)
        app.connection_manager.active_connections.remove(orig_name);

        // Always invalidate old name's password cache
        app.connection_manager.invalidate_password(orig_name);

        // If name changed, delete old password from keyring
        if orig_name != &name {
            let _ = config::delete_password(orig_name);
        }
    }

    // Always remove cached connection for the current name to force fresh connection
    app.connection_manager.active_connections.remove(&name);

    // Add the new/updated profile
    app.connection_manager
        .profiles
        .insert(name.clone(), profile.clone());

    // Save to config file
    if let Ok(mut config) = Config::load() {
        config.profiles = app.connection_manager.profiles.clone();
        if let Err(e) = config.save() {
            app.log_error(format!("Failed to save profile: {}", e));
        }
    }

    // If setup mode, auto-connect to the new profile
    if is_setup_mode {
        app.profile_editor = None;
        app.mode = AppMode::ConnectionTesting;

        // Try to switch to the new profile
        match app.switch_profile(&name).await {
            Ok(()) => {
                // Connection successful! Load topics
                match app.connection_manager.list_topics(&name).await {
                    Ok(_topics) => {
                        // Successfully connected and got topics
                        app.mode = AppMode::TopicSwitcher;
                        app.test_result = Some(format!("✓ Connected to {}!", name));
                    }
                    Err(e) => {
                        // Connected but failed to list topics
                        app.test_result =
                            Some(format!("✗ Connected but failed to list topics: {}", e));
                        app.mode = AppMode::BrokerSwitcher;
                    }
                }
            }
            Err(e) => {
                // Connection failed - go back to editor
                app.profile_editor = Some(editor_clone);
                app.mode = AppMode::ProfileEditor;
                app.log_error(format!(
                    "Connection failed: {}. Please check your credentials.",
                    e
                ));
            }
        }
    } else {
        // Not setup mode - just go back to broker switcher
        app.profile_editor = None;
        app.mode = AppMode::BrokerSwitcher;
    }

    Ok(())
}

/// Handles key events for ProfileEditor mode
async fn handle_profile_editor_input(
    app: &mut App,
    key_code: KeyCode,
    key_modifiers: KeyModifiers,
) -> Result<bool> {
    if let Some(editor) = &mut app.profile_editor {
        match key_code {
            KeyCode::Esc => {
                app.profile_editor = None;
                app.mode = AppMode::BrokerSwitcher;
            }
            KeyCode::Tab => {
                editor.next_field();
            }
            KeyCode::BackTab => {
                editor.prev_field();
            }
            KeyCode::Enter => {
                // Clone editor to avoid borrow issues
                let mut editor_clone = editor.clone();

                if editor_clone.validate() {
                    save_profile_from_editor(app, editor_clone).await?;
                } else {
                    // Validation failed - update editor with validation errors
                    *editor = editor_clone;
                }
            }
            KeyCode::Backspace => {
                editor.get_active_input().pop();
            }
            KeyCode::Char(c) => {
                if key_modifiers.contains(KeyModifiers::CONTROL) {
                    match c {
                        'e' | 'E' => {
                            if editor.active_field == ProfileField::Password {
                                editor.use_env_var = !editor.use_env_var;
                            }
                        }
                        's' | 'S' => {
                            if editor.active_field == ProfileField::Password && !editor.use_env_var
                            {
                                editor.show_password = !editor.show_password;
                            }
                        }
                        't' | 'T' => {
                            let test_profile = editor.to_profile();
                            app.test_result = None;
                            app.mode = AppMode::ConnectionTesting;

                            // Temporarily save password to keyring for testing
                            let test_result = if let Ok(password) = editor.get_password() {
                                if let Err(e) = config::save_password(&test_profile.name, &password)
                                {
                                    format!("✗ Failed to save password for test: {}", e)
                                } else {
                                    // Invalidate cache to ensure fresh test
                                    app.connection_manager
                                        .invalidate_password(&test_profile.name);

                                    match app.test_connection(&test_profile).await {
                                        Ok(result) => result,
                                        Err(e) => format!("✗ Test error: {}", e),
                                    }
                                }
                            } else {
                                "✗ Failed to get password".to_string()
                            };
                            app.test_result = Some(test_result);
                        }
                        _ => {}
                    }
                } else {
                    editor.get_active_input().push(c);
                }
            }
            _ => {}
        }
    }
    Ok(false)
}

/// Handles key events for TopicSwitcher mode
async fn handle_topic_switcher_input(app: &mut App, key_code: KeyCode) -> Result<bool> {
    match key_code {
        KeyCode::Esc => {
            app.mode = AppMode::PartitionList;
        }
        KeyCode::Up => {
            let topics = app.get_filtered_topics();
            if !topics.is_empty() {
                let current = app.topic_switcher_state.selected().unwrap_or(0);
                let new_index = if current == 0 {
                    topics.len() - 1
                } else {
                    current - 1
                };
                app.topic_switcher_state.select(Some(new_index));
            }
        }
        KeyCode::Down => {
            let topics = app.get_filtered_topics();
            if !topics.is_empty() {
                let current = app.topic_switcher_state.selected().unwrap_or(0);
                let new_index = (current + 1) % topics.len();
                app.topic_switcher_state.select(Some(new_index));
            }
        }
        KeyCode::Enter => {
            let topics = app.get_filtered_topics();
            if let Some(selected) = app.topic_switcher_state.selected() {
                if let Some(topic) = topics.get(selected) {
                    app.switch_topic(topic.clone()).await?;
                    app.mode = AppMode::PartitionList;
                }
            }
        }
        KeyCode::Backspace => {
            app.topic_filter.pop();
            app.topic_switcher_state.select(Some(0));
        }
        KeyCode::Char(c) => {
            app.topic_filter.push(c);
            app.topic_switcher_state.select(Some(0));
        }
        _ => {}
    }
    Ok(false)
}

/// Handles key events for PartitionList mode
async fn handle_partition_list_input(app: &mut App, key_code: KeyCode) -> Result<bool> {
    match key_code {
        KeyCode::Char('q') => return Ok(true),
        KeyCode::Down => {
            if app.selected_partition < app.partitions.len().saturating_sub(1) {
                app.selected_partition += 1;
            }
        }
        KeyCode::Up => {
            if app.selected_partition > 0 {
                app.selected_partition -= 1;
            }
        }
        KeyCode::Enter => {
            app.load_messages(app.selected_partition, DEFAULT_MESSAGE_LOAD_COUNT * 2)
                .await?;
            app.mode = AppMode::MessageList;
            app.message_list_state.select(Some(0)); // Start at top (newest)
        }
        KeyCode::Char('k') => {
            // Analyze keys across all partitions
            app.analyze_keys().await?;
        }
        KeyCode::Char('e') => {
            app.show_errors = !app.show_errors;
        }
        KeyCode::Char('/') => {
            // Enter global search mode
            app.global_search.query.clear();
            app.global_search.results.clear();
            app.global_search.strategy = SearchStrategy::Quick;
            app.global_search.is_searching = false;
            app.mode = AppMode::GlobalSearch;

            // Load some messages for Quick search if partitions are empty
            let empty_partitions: Vec<usize> = (0..app.partitions.len())
                .filter(|&idx| app.partitions[idx].messages.is_empty())
                .collect();

            if !empty_partitions.is_empty() {
                app.loading_message = format!(
                    "Loading messages for {} partitions...",
                    empty_partitions.len()
                );
                app.mode = AppMode::Loading;

                // Load messages for each empty partition
                for idx in empty_partitions {
                    app.load_messages(idx, GLOBAL_SEARCH_MESSAGE_COUNT).await?;
                }

                app.mode = AppMode::GlobalSearch;
            }
        }
        _ => {}
    }
    Ok(false)
}

/// Handles key events for MessageList mode
async fn handle_message_list_input<B: Backend>(
    app: &mut App,
    key_code: KeyCode,
    terminal: &mut Terminal<B>,
) -> Result<bool> {
    match key_code {
        KeyCode::Char('q') => return Ok(true),
        KeyCode::Char('b') => {
            app.mode = AppMode::PartitionList;
            // Clear search results when going back
            app.search_results.clear();
            app.search_query.clear();
        }
        KeyCode::Down => {
            let messages_len = app.current_partition_messages().len();
            if let Some(selected) = app.message_list_state.selected() {
                if selected < messages_len.saturating_sub(1) {
                    app.message_list_state.select(Some(selected + 1));
                }
            }
        }
        KeyCode::Up => {
            if let Some(selected) = app.message_list_state.selected() {
                if selected > 0 {
                    app.message_list_state.select(Some(selected - 1));
                }
            }
        }
        KeyCode::Tab => {
            if let Some(selected) = app.message_list_state.selected() {
                let metadata_info = app
                    .current_partition_messages()
                    .get(selected)
                    .map(|m| (m.partition, m.offset));

                if let Some((partition, offset)) = metadata_info {
                    // Load content if not already cached
                    if !app.message_cache.contains_key(&offset) {
                        app.loading_message = "Loading message content...".to_string();
                        app.mode = AppMode::Loading;
                        terminal.draw(|f| ui(f, app))?;

                        match app.load_message_content(partition, offset).await {
                            Ok(_) => {
                                app.selected_message = Some(selected);
                                app.message_scroll = 0;
                                app.mode = AppMode::MessageDetail;
                            }
                            Err(e) => {
                                app.log_error(format!("Failed to load content: {}", e));
                                app.mode = AppMode::MessageList;
                            }
                        }
                    } else {
                        app.selected_message = Some(selected);
                        app.message_scroll = 0;
                        app.mode = AppMode::MessageDetail;
                    }
                }
            }
        }
        KeyCode::Char('j') => {
            app.jump_offset_input.clear();
            app.mode = AppMode::OffsetJump;
        }
        KeyCode::Char('/') => {
            app.search_query.clear();
            app.mode = AppMode::Search;
        }
        KeyCode::Char('e') => {
            app.show_errors = !app.show_errors;
        }
        KeyCode::Char('n') if !app.search_results.is_empty() => {
            app.current_search_index = (app.current_search_index + 1) % app.search_results.len();
            app.jump_to_search_result(app.current_search_index);
        }
        KeyCode::Char('N') if !app.search_results.is_empty() => {
            app.current_search_index = if app.current_search_index == 0 {
                app.search_results.len() - 1
            } else {
                app.current_search_index - 1
            };
            app.jump_to_search_result(app.current_search_index);
        }
        KeyCode::PageUp => {
            // PageUp = scroll up in the list (move selection up)
            if let Some(selected) = app.message_list_state.selected() {
                if selected > 10 {
                    app.message_list_state
                        .select(Some(selected.saturating_sub(10)));
                } else {
                    app.message_list_state.select(Some(0));
                }
            }
        }
        KeyCode::PageDown => {
            // PageDown = scroll down in the list (move selection down)
            let messages_len = app.current_partition_messages().len();
            if let Some(selected) = app.message_list_state.selected() {
                let new_pos = selected + 10;
                if new_pos < messages_len {
                    app.message_list_state.select(Some(new_pos));

                    // Load more messages when we're within 50 messages of the bottom
                    if new_pos >= messages_len.saturating_sub(50) {
                        app.loading_message = "Loading older messages...".to_string();
                        app.mode = AppMode::Loading;
                        terminal.draw(|f| ui(f, app))?;

                        app.load_earlier_messages(app.selected_partition, 500)
                            .await?;
                        app.mode = AppMode::MessageList;
                    }
                } else {
                    app.message_list_state
                        .select(Some(messages_len.saturating_sub(1)));

                    // Also try to load more when reaching the absolute bottom
                    app.loading_message = "Loading older messages...".to_string();
                    app.mode = AppMode::Loading;
                    terminal.draw(|f| ui(f, app))?;

                    app.load_earlier_messages(app.selected_partition, 500)
                        .await?;
                    app.mode = AppMode::MessageList;
                }
            }
        }
        _ => {}
    }
    Ok(false)
}

/// Handles key events for MessageDetail mode
fn handle_message_detail_input(app: &mut App, key_code: KeyCode) -> Result<bool> {
    match key_code {
        KeyCode::Tab | KeyCode::Char('b') => {
            app.mode = AppMode::MessageList;
            app.detail_scroll_offset = 0;
        }
        KeyCode::Down => {
            app.detail_scroll_offset = app.detail_scroll_offset.saturating_add(1);
        }
        KeyCode::Up => {
            app.detail_scroll_offset = app.detail_scroll_offset.saturating_sub(1);
        }
        KeyCode::PageDown => {
            app.detail_scroll_offset = app.detail_scroll_offset.saturating_add(10);
        }
        KeyCode::PageUp => {
            app.detail_scroll_offset = app.detail_scroll_offset.saturating_sub(10);
        }
        KeyCode::Char('f') => {
            // Toggle format
            app.display_format = match app.display_format {
                DisplayFormat::Json => DisplayFormat::Hex,
                DisplayFormat::Hex => DisplayFormat::Raw,
                DisplayFormat::Raw => DisplayFormat::Json,
            };
        }
        KeyCode::Char('s') => {
            // Save message to file
            if let Some(msg_idx) = app.selected_message {
                match app.save_message_to_file(app.selected_partition, msg_idx) {
                    Ok(filename) => {
                        app.saved_file_path = Some(format!("./kafka-exports/{}", filename));
                        app.mode = AppMode::FileSaved;
                    }
                    Err(e) => {
                        app.log_error(format!("Failed to save message: {}", e));
                        app.loading_message = format!("Failed to save: {}", e);
                    }
                }
            }
        }
        _ => {}
    }
    Ok(false)
}

/// Handles key events for OffsetJump mode
fn handle_offset_jump_input(app: &mut App, key_code: KeyCode) -> Result<bool> {
    match key_code {
        KeyCode::Esc => {
            app.mode = AppMode::MessageList;
        }
        KeyCode::Enter => {
            if let Ok(target_offset) = app.jump_offset_input.parse::<i64>() {
                // Find the message with this offset
                let messages = app.current_partition_messages();
                if let Some(idx) = messages.iter().position(|m| m.offset == target_offset) {
                    app.message_list_state.select(Some(idx));
                    app.selected_message = Some(idx);
                } else {
                    // Find the closest message
                    let closest_idx = messages
                        .iter()
                        .enumerate()
                        .min_by_key(|(_, m)| (m.offset - target_offset).abs())
                        .map(|(idx, _)| idx)
                        .unwrap_or(0);
                    app.message_list_state.select(Some(closest_idx));
                    app.selected_message = Some(closest_idx);
                }
            }
            app.jump_offset_input.clear();
            app.mode = AppMode::MessageList;
        }
        KeyCode::Backspace => {
            app.jump_offset_input.pop();
        }
        KeyCode::Char(c) if c.is_numeric() => {
            app.jump_offset_input.push(c);
        }
        _ => {}
    }
    Ok(false)
}

/// Handles key events for Search mode
fn handle_search_input(app: &mut App, key_code: KeyCode) -> Result<bool> {
    match key_code {
        KeyCode::Esc => {
            app.mode = AppMode::MessageList;
        }
        KeyCode::Enter => {
            // Default to searching current partition only
            app.search_messages(false);
            app.mode = AppMode::MessageList;
        }
        KeyCode::Backspace => {
            app.search_query.pop();
        }
        KeyCode::Char(c) => {
            app.search_query.push(c);
        }
        _ => {}
    }
    Ok(false)
}

/// Handles key events for GlobalSearch mode
async fn handle_global_search_input(app: &mut App, key_code: KeyCode) -> Result<bool> {
    match key_code {
        KeyCode::Esc => {
            if app.global_search.is_searching {
                // Cancel the search
                app.cancel_search();
            } else {
                // Go back to partition list
                app.mode = AppMode::PartitionList;
            }
        }
        KeyCode::Enter => {
            if !app.global_search.query.is_empty() && !app.global_search.is_searching {
                app.start_global_search().await?;
            }
        }
        KeyCode::Tab => {
            // Toggle between Quick and Deep search
            app.global_search.strategy = match app.global_search.strategy {
                SearchStrategy::Quick => SearchStrategy::Deep,
                SearchStrategy::Deep => SearchStrategy::Quick,
            };
        }
        KeyCode::Up => {
            if app.global_search.selected_result > 0 {
                app.global_search.selected_result -= 1;
            }
        }
        KeyCode::Down => {
            if app.global_search.selected_result < app.global_search.results.len().saturating_sub(1)
            {
                app.global_search.selected_result += 1;
            }
        }
        KeyCode::Char('\n') | KeyCode::Char(' ') => {
            // Jump to selected result in context
            if let Some(result) = app
                .global_search
                .results
                .get(app.global_search.selected_result)
            {
                app.selected_partition = result.partition_idx;
                app.mode = AppMode::MessageList;
                app.message_list_state.select(Some(result.message_idx));
            }
        }
        KeyCode::Backspace => {
            app.global_search.query.pop();
        }
        KeyCode::Char(c) => {
            app.global_search.query.push(c);
        }
        _ => {}
    }
    Ok(false)
}

/// Handles key events for ProfileDeleting mode
fn handle_profile_deleting_input(app: &mut App, key_code: KeyCode) -> Result<bool> {
    match key_code {
        KeyCode::Char('y') | KeyCode::Char('Y') => {
            if let Some(profile_name) = app.profile_to_delete.clone() {
                app.connection_manager.profiles.remove(&profile_name);

                // Delete password from keyring
                if let Err(e) = config::delete_password(&profile_name) {
                    app.log_error(format!("Failed to delete password from keyring: {}", e));
                }

                // Invalidate password cache
                app.connection_manager.invalidate_password(&profile_name);

                // Remove cached connection
                app.connection_manager
                    .active_connections
                    .remove(&profile_name);

                // Save to config file
                if let Ok(mut config) = Config::load() {
                    config.profiles = app.connection_manager.profiles.clone();
                    if let Err(e) = config.save() {
                        app.log_error(format!("Failed to save config after deletion: {}", e));
                    }
                }

                app.profile_to_delete = None;
                app.mode = AppMode::BrokerSwitcher;
            }
        }
        KeyCode::Char('n') | KeyCode::Char('N') | KeyCode::Esc => {
            app.profile_to_delete = None;
            app.mode = AppMode::BrokerSwitcher;
        }
        _ => {}
    }
    Ok(false)
}

/// Handles key events for ConnectionTesting mode
fn handle_connection_testing_input(app: &mut App, key_code: KeyCode) -> Result<bool> {
    match key_code {
        KeyCode::Esc | KeyCode::Enter => {
            app.test_result = None;
            // Return to the appropriate mode
            if app.profile_editor.is_some() {
                app.mode = AppMode::ProfileEditor;
            } else {
                app.mode = AppMode::BrokerSwitcher;
            }
        }
        _ => {}
    }
    Ok(false)
}

/// Handles key events for FileSaved mode
fn handle_file_saved_input(app: &mut App, key_code: KeyCode) -> Result<bool> {
    match key_code {
        KeyCode::Esc | KeyCode::Enter | KeyCode::Char(' ') => {
            app.saved_file_path = None;
            app.mode = AppMode::MessageDetail;
        }
        _ => {}
    }
    Ok(false)
}

async fn run_app<B: Backend>(terminal: &mut Terminal<B>, app: &mut App) -> Result<()> {
    // Clear the terminal once at start
    terminal.clear()?;

    loop {
        // Process any pending search updates
        app.process_search_updates();

        terminal.draw(|f| ui(f, app))?;

        // Poll for events with a short timeout to allow search updates
        if poll(Duration::from_millis(50))? {
            if let Event::Key(key) = event::read()? {
                // Global keyboard shortcuts (work from any mode except when in switchers)
                if !matches!(app.mode, AppMode::BrokerSwitcher | AppMode::TopicSwitcher)
                    && key.modifiers.contains(KeyModifiers::CONTROL)
                {
                    match key.code {
                        KeyCode::Char('b') => {
                            // Initialize broker switcher
                            app.broker_switcher_state = ListState::default();
                            app.broker_switcher_state.select(Some(0));
                            app.mode = AppMode::BrokerSwitcher;
                            continue;
                        }
                        KeyCode::Char('t') => {
                            // Initialize topic switcher
                            app.topic_filter.clear();
                            app.topic_switcher_state = ListState::default();
                            app.topic_switcher_state.select(Some(0));
                            app.mode = AppMode::TopicSwitcher;
                            continue;
                        }
                        _ => {}
                    }
                }

                let should_quit = match app.mode {
                    AppMode::BrokerSwitcher => handle_broker_switcher_input(app, key.code).await?,
                    AppMode::ProfileEditor => {
                        handle_profile_editor_input(app, key.code, key.modifiers).await?
                    }
                    AppMode::ProfileDeleting => handle_profile_deleting_input(app, key.code)?,
                    AppMode::ConnectionTesting => handle_connection_testing_input(app, key.code)?,
                    AppMode::TopicSwitcher => handle_topic_switcher_input(app, key.code).await?,
                    AppMode::PartitionList => handle_partition_list_input(app, key.code).await?,
                    AppMode::MessageList => {
                        handle_message_list_input(app, key.code, terminal).await?
                    }
                    AppMode::MessageDetail => handle_message_detail_input(app, key.code)?,
                    AppMode::OffsetJump => handle_offset_jump_input(app, key.code)?,
                    AppMode::Search => handle_search_input(app, key.code)?,
                    AppMode::Loading => {
                        // No key handling during loading
                        false
                    }
                    AppMode::KeyAnalysis => {
                        // Any key returns to partition list
                        app.mode = AppMode::PartitionList;
                        false
                    }
                    AppMode::GlobalSearch => handle_global_search_input(app, key.code).await?,
                    AppMode::FileSaved => handle_file_saved_input(app, key.code)?,
                };

                if should_quit {
                    return Ok(());
                }
            }
        }
    }
}
