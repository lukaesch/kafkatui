# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [1.0.0] - 2025-01-XX

### Added
- Initial v1.0 release
- Terminal user interface for Apache Kafka
- Multi-broker/profile management with configuration persistence
- SASL/SSL authentication (SCRAM-SHA-512)
- Secure credential storage using system keyring (macOS Keychain, Linux Secret Service, Windows Credential Manager)
- Avro message deserialization with Schema Registry integration
- Binary message detection with hex dump display
- Message export to JSON files with path traversal protection
- Global search across all partitions
- Key analysis to determine partitioning strategy
- Lazy loading for efficient memory usage
- Offset jumping for direct navigation
- Profile editor with connection testing
- Favorite topics per profile
- Error logging with toggle view
- Mouse support for text selection

### Security
- TLS certificate validation enabled
- Credentials stored in system keyring (not plaintext)
- Path sanitization for file exports
- No hardcoded credentials

### Changed
- Refactored codebase into modular structure (app/, ui/, services/)
- Extracted pure functions to utils/ module with unit tests
- Improved error handling and user feedback
- Connection cache invalidation for profile updates
- Validation error display in profile editor

### Fixed
- Connection caching bug when editing profiles
- Validation errors not displaying in profile editor
- Schema registry initialization on session restore
- File export path validation for non-existent files

## [0.1.0] - 2025-01-XX

### Added
- Initial development release
- Basic Kafka browsing functionality
- Profile management
- Message viewing

[1.0.0]: https://github.com/lukaesch/kafkatui/releases/tag/v1.0.0
[0.1.0]: https://github.com/lukaesch/kafkatui/releases/tag/v0.1.0
