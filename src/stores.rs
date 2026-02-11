//! Storage implementations for files and credentials
//!
//! Provides trait-based abstractions for file I/O and secure credential
//! storage in the system keyring. These implementations are used throughout
//! the application for configuration and password management.

use crate::traits::{CredentialStore, FileStore};
use anyhow::{Context, Result};
use std::fs;
use std::path::Path;

/// Standard filesystem implementation
///
/// Implements the FileStore trait using the standard library's `std::fs` module.
pub struct StdFileStore;

impl FileStore for StdFileStore {
    fn write(&self, path: &Path, content: &str) -> Result<()> {
        fs::write(path, content).with_context(|| format!("Failed to write to {:?}", path))
    }

    fn create_dir_all(&self, path: &Path) -> Result<()> {
        fs::create_dir_all(path).with_context(|| format!("Failed to create directory {:?}", path))
    }
}

/// Keyring-based credential store implementation
///
/// Uses the system's secure credential storage:
/// - macOS: Keychain
/// - Linux: Secret Service API (libsecret)
/// - Windows: Credential Manager
pub struct KeyringCredentialStore;

impl CredentialStore for KeyringCredentialStore {
    fn save_password(&self, profile_name: &str, password: &str) -> Result<()> {
        let entry = keyring::Entry::new("kafkatui", profile_name).with_context(|| {
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

    fn get_password(&self, profile_name: &str) -> Result<String> {
        let entry = keyring::Entry::new("kafkatui", profile_name).with_context(|| {
            format!(
                "Failed to create keyring entry for profile '{}'",
                profile_name
            )
        })?;
        entry
            .get_password()
            .with_context(|| format!("Failed to retrieve password for profile '{}'", profile_name))
    }

    fn delete_password(&self, profile_name: &str) -> Result<()> {
        let entry = keyring::Entry::new("kafkatui", profile_name).with_context(|| {
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
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_std_file_store_creates_directory() {
        let store = StdFileStore;
        let temp_dir = std::env::temp_dir().join("kafkatui_test");

        let result = store.create_dir_all(&temp_dir);
        assert!(result.is_ok());
        assert!(temp_dir.exists());

        // Cleanup
        let _ = std::fs::remove_dir_all(&temp_dir);
    }

    #[test]
    fn test_std_file_store_writes_file() {
        let store = StdFileStore;
        let temp_file = std::env::temp_dir().join("kafkatui_test_file.txt");

        let result = store.write(&temp_file, "test content");
        assert!(result.is_ok());

        let content = std::fs::read_to_string(&temp_file).unwrap();
        assert_eq!(content, "test content");

        // Cleanup
        let _ = std::fs::remove_file(&temp_file);
    }
}
