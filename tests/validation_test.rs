//! Integration tests for path validation and sanitization
//!
//! These tests verify that the security functions properly prevent
//! path traversal attacks when exporting messages to files.

use kafkatui::utils::validation::{build_safe_filepath, sanitize_filename};
use std::path::Path;

#[test]
fn test_sanitize_filename_removes_dangerous_characters() {
    let dangerous_inputs = vec![
        ("../../etc/passwd", ".._.._etc_passwd"),
        ("C:\\Windows\\System32", "C__Windows_System32"),
        ("file<>name", "file__name"),
        ("test|pipe", "test_pipe"),
        ("quote\"test", "quote_test"),
        ("null\0byte", "null_byte"),
    ];

    for (input, expected) in dangerous_inputs {
        let result = sanitize_filename(input);
        assert_eq!(result, expected, "Failed to sanitize: {}", input);
    }
}

#[test]
fn test_sanitize_filename_preserves_safe_characters() {
    let safe_inputs = vec![
        "normal-filename.txt",
        "file_with_underscores.json",
        "file.with.dots.txt",
        "file-with-dashes-123.log",
    ];

    for input in safe_inputs {
        let result = sanitize_filename(input);
        assert_eq!(result, input, "Should not modify safe filename: {}", input);
    }
}

#[test]
fn test_build_safe_filepath_sanitizes_filename() {
    let dir = Path::new("/tmp/exports");
    let dangerous_filename = "../../../etc/passwd";

    let result = build_safe_filepath(dir, dangerous_filename);

    // Should sanitize the dangerous path components
    assert_eq!(
        result.to_str().unwrap(),
        "/tmp/exports/.._.._.._etc_passwd",
        "Should sanitize path traversal attempts"
    );
}

#[test]
fn test_build_safe_filepath_combines_dir_and_filename() {
    let dir = Path::new("/var/kafka-exports");
    let filename = "topic_partition_offset.json";

    let result = build_safe_filepath(dir, filename);

    assert_eq!(
        result.to_str().unwrap(),
        "/var/kafka-exports/topic_partition_offset.json"
    );
}

#[test]
fn test_build_safe_filepath_handles_special_characters() {
    let dir = Path::new("/tmp");
    let filename = "file:with*special?chars.txt";

    let result = build_safe_filepath(dir, filename);

    // Special characters should be replaced with underscores
    assert_eq!(result.to_str().unwrap(), "/tmp/file_with_special_chars.txt");
}

#[test]
fn test_sanitize_filename_handles_unicode() {
    let unicode_input = "文件名.txt";
    let result = sanitize_filename(unicode_input);
    // Should preserve valid unicode characters
    assert!(result.contains("txt"), "Should preserve safe ASCII");
}

#[test]
fn test_sanitize_filename_handles_empty_string() {
    let result = sanitize_filename("");
    assert_eq!(result, "", "Empty string should remain empty");
}

#[test]
fn test_sanitize_filename_handles_only_dangerous_chars() {
    let result = sanitize_filename("/\\/\\");
    assert_eq!(result, "____", "Should replace all dangerous characters");
}
