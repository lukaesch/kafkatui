//! Path validation and sanitization
//!
//! Provides security functions to prevent path traversal attacks
//! when exporting messages to files. All user-provided filenames
//! are sanitized before use.

use anyhow::Result;
use std::path::{Path, PathBuf};

/// Sanitizes a string to be safe for use as a filename
///
/// Replaces dangerous characters (slashes, colons, wildcards, etc.)
/// with underscores to prevent path traversal and invalid filenames.
///
/// # Arguments
/// - `input` - The unsanitized filename
///
/// # Returns
/// A safe filename with dangerous characters replaced
///
/// # Security
/// This function prevents path traversal attacks by removing characters
/// that could navigate the directory structure.
pub fn sanitize_filename(input: &str) -> String {
    input
        .chars()
        .map(|c| match c {
            '/' | '\\' | ':' | '*' | '?' | '"' | '<' | '>' | '|' | '\0' => '_',
            c if c.is_control() => '_',
            c => c,
        })
        .collect()
}

/// Validates that a file path is within a given directory
///
/// Ensures that a file path, when resolved, is contained within the
/// specified directory. This prevents path traversal attacks.
///
/// # Arguments
/// - `file_path` - The path to validate
/// - `allowed_dir` - The directory that must contain the file
///
/// # Returns
/// `Ok(())` if the path is safe, `Err` if potential traversal detected
///
/// # Security
/// This function prevents "../../../etc/passwd" style attacks.
#[allow(dead_code)]
pub fn validate_path_within_directory(file_path: &Path, allowed_dir: &Path) -> Result<()> {
    let canonical_dir = allowed_dir.canonicalize()?;

    if let Some(parent) = file_path.parent() {
        let canonical_parent = parent
            .canonicalize()
            .unwrap_or_else(|_| parent.to_path_buf());

        if !canonical_parent.starts_with(&canonical_dir) {
            return Err(anyhow::anyhow!(
                "Invalid path: potential path traversal detected"
            ));
        }
    }

    Ok(())
}

/// Builds a safe file path within a directory
///
/// Combines a directory path with a sanitized filename.
///
/// # Arguments
/// - `dir` - The base directory
/// - `filename` - The filename to sanitize and append
///
/// # Returns
/// A safe path within the directory
#[allow(dead_code)]
pub fn build_safe_filepath(dir: &Path, filename: &str) -> PathBuf {
    dir.join(sanitize_filename(filename))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_sanitize_filename_removes_slashes() {
        assert_eq!(sanitize_filename("../etc/passwd"), ".._etc_passwd");
    }

    #[test]
    fn test_sanitize_filename_removes_special_chars() {
        assert_eq!(sanitize_filename("file:name*?.txt"), "file_name__.txt");
    }

    #[test]
    fn test_sanitize_filename_preserves_valid_chars() {
        assert_eq!(
            sanitize_filename("valid-file_name.txt"),
            "valid-file_name.txt"
        );
    }

    #[test]
    fn test_sanitize_filename_removes_null_byte() {
        assert_eq!(sanitize_filename("file\0name"), "file_name");
    }

    #[test]
    fn test_build_safe_filepath() {
        let dir = Path::new("/tmp/exports");
        let path = build_safe_filepath(dir, "../../../etc/passwd");
        assert_eq!(path.to_str().unwrap(), "/tmp/exports/.._.._.._etc_passwd");
    }
}
