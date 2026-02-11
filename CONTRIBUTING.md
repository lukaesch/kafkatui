# Contributing to KafkaTUI

Thank you for your interest in contributing to KafkaTUI! This document provides guidelines and instructions for contributing.

## Code of Conduct

Be respectful, inclusive, and professional in all interactions.

## Getting Started

### Prerequisites

- Rust 1.70+
- Cargo
- A Kafka cluster for testing (can use Docker)

### Building from Source

```bash
git clone https://github.com/lukaesch/kafkatui.git
cd kafkatui
cargo build
```

### Running Tests

```bash
cargo test
cargo clippy -- -D warnings
cargo fmt --check
```

## How to Contribute

### Reporting Bugs

Open an issue with:
- Clear description of the problem
- Steps to reproduce
- Expected vs actual behavior
- Environment details (OS, Rust version, Kafka version)

### Suggesting Features

Open an issue describing:
- Use case and motivation
- Proposed solution or API
- Alternatives considered

### Pull Requests

1. **Fork the repository**
2. **Create a feature branch**: `git checkout -b feature/my-feature`
3. **Make your changes**:
   - Write clear commit messages
   - Add tests for new functionality
   - Update documentation as needed
   - Run `cargo fmt` and `cargo clippy`
4. **Push to your fork**: `git push origin feature/my-feature`
5. **Open a Pull Request**

#### PR Guidelines

- Keep PRs focused on a single change
- Write descriptive PR titles and descriptions
- Link related issues
- Ensure all tests pass
- Update CHANGELOG.md

## Development Guidelines

### Code Style

- Follow Rust standard naming conventions
- Use `cargo fmt` for formatting
- Address all `cargo clippy` warnings
- Keep functions under 100 lines
- Document public APIs with `///` comments
- Add module-level docs with `//!`

### Testing

- Add unit tests for pure functions
- Add integration tests for complex workflows
- Aim for meaningful test coverage
- Test edge cases and error paths

### Architecture

```
src/
  app/          # Application state and logic
  ui/           # TUI rendering
  services/     # Business logic (Kafka, search, export)
  utils/        # Pure utility functions
  config.rs     # Configuration management
  connection.rs # Connection pooling
```

### Commit Messages

Use conventional commits format:

```
feat: add message export to CSV format
fix: resolve connection cache invalidation bug
docs: update README with keyring setup
refactor: extract UI rendering to ui module
test: add integration tests for search
```

## Security

Report security vulnerabilities privately via [GitHub Security Advisories](https://github.com/lukaesch/kafkatui/security/advisories/new).

Do not open public issues for security problems.

## Questions?

Open a discussion or issue for questions about contributing.

## License

By contributing, you agree that your contributions will be licensed under the MIT License.
