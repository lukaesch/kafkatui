# KafkaTUI

A modern terminal user interface for Apache Kafka with secure credential management, Avro support, and multi-broker operations.

## Features

- **Multi-Broker Support**: Switch between different Kafka environments (production, staging, dev)
- **Topic Switching**: Quickly switch between topics with filtering
- **Interactive Navigation**: Browse partitions and messages with keyboard controls
- **Lazy Loading**: Efficient memory usage - loads message content only when needed
- **Message Metadata**: Shows offset, timestamp, key, and size without loading full content
- **Message Details**: View full message content with JSON pretty-printing (loaded on demand)
- **Avro Support**: Automatic deserialization of Avro messages with Schema Registry integration
- **Binary/Avro Detection**: Automatic detection and hex dump display for binary messages
- **Search**: Find messages by key (content search loads messages on demand)
- **Offset Jump**: Navigate directly to specific message offsets
- **Key Analysis**: Analyze message keys to determine partitioning strategy
- **SASL Authentication**: Supports SCRAM-SHA-512 authentication
- **Mouse Selection**: Text can be selected with mouse for copying
- **Configuration Profiles**: Save and reuse connection configurations
- **Secure Credentials**: Passwords stored in system keyring (macOS Keychain, Linux Secret Service, Windows Credential Manager)
- **Message Export**: Save messages to JSON files with path traversal protection

## Installation

### From Source

```bash
git clone https://github.com/lukaesch/kafkatui.git
cd kafkatui
cargo build --release
./target/release/kafkatui
```

### From Crates.io

```bash
cargo install kafkatui
```

## Usage

### First Run (Recommended)

Simply run the application without arguments to enter setup mode:
```bash
kafkatui
```

This will open the profile editor where you can create your first connection profile. Passwords are securely stored in your system's keyring.

### With Configuration File

You can also pre-configure profiles in `~/.config/kafkatui/config.yaml`:
```yaml
profiles:
  production:
    broker: "prod-kafka:9092"
    user: "prod-user"
    # Password will be stored in system keyring after first connection
    # You can use environment variables during setup: ${PROD_KAFKA_PASSWORD}
    schema_registry: "https://prod-registry:8081"
    favorite_topics:
      - important-topic
```

**Note**: Passwords are NOT stored in the config file. After creating a profile in the TUI, passwords are securely stored in:
- **macOS**: Keychain
- **Linux**: Secret Service API (gnome-keyring, kwallet)
- **Windows**: Credential Manager

### With Environment Variables
```bash
export KAFKA_BROKER="broker:9092"
export KAFKA_USER="your-user"
export KAFKA_PASSWORD="your-password"
export KAFKA_SCHEMA_REGISTRY_URL="https://schema-registry:8081"  # Optional for Avro support
cargo run
```

### With Command Line Arguments
```bash
cargo run -- -b broker:9092 -u user -p password -t topic-name
```

## Key Bindings

### Global
- `Ctrl+B`: Open broker/profile switcher
- `Ctrl+T`: Open topic switcher

### Partition View
- `↑/↓`: Navigate partitions
- `Enter`: Select partition and load messages
- `k`: Analyze keys (check partitioning strategy)
- `/`: Global search across partitions
- `q`: Quit

### Message List
- `↑/↓`: Navigate messages
- `PageUp/PageDown`: Navigate by 10 messages
- `Tab`: View message details (loads content on demand)
- `j`: Jump to offset
- `/`: Search messages (searches in keys)
- `n/N`: Next/Previous search result
- `b`: Back to partitions
- `q`: Quit

### Message Detail
- `↑/↓`: Scroll content
- `f`: Toggle format (JSON/Hex/Raw)
- `s`: Save message to file
- `Tab` or `b`: Back to message list

### Search/Jump Dialogs
- `Enter`: Confirm
- `Esc`: Cancel

### Message Export
- Messages are saved to `./kafka-exports/` directory
- Filename format: `{topic}_{partition}_{offset}_{timestamp}.json`
- Includes metadata (partition, offset, timestamp, headers)

### Broker Switcher (`Ctrl+B`)
- `↑/↓`: Navigate profiles
- `Enter`: Connect to selected profile
- `n`: Create new profile
- `e`: Edit selected profile
- `d`: Delete selected profile
- `t`: Test connection
- `Esc`: Cancel

### Profile Editor
- `Tab/Shift+Tab`: Navigate between fields
- `Ctrl+E`: Toggle environment variable mode for password
- `Ctrl+S`: Show/hide password (when not using env var)
- `Ctrl+T`: Test connection with current settings
- `Enter`: Save profile
- `Esc`: Cancel

## Security

- **TLS/SSL**: Enabled by default with certificate validation
- **SASL**: Supports SCRAM-SHA-512 authentication
- **Credentials**: Stored securely in system keyring, never in plaintext
- **Path Traversal Protection**: File exports are sanitized and validated

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md) for development guidelines.

## License

Licensed under the [MIT License](LICENSE-MIT).