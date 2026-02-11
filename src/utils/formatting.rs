//! Message formatting utilities
//!
//! Functions for formatting Kafka messages, detecting binary content,
//! generating hex dumps, and converting Avro to JSON. These utilities
//! help present message data in human-readable formats.

/// Generates a hex dump of binary data with extracted readable strings
///
/// Creates a formatted hex dump similar to the `hexdump` command-line tool.
/// Attempts to extract readable strings from the binary data for easier analysis.
///
/// # Arguments
/// - `data` - The binary data to format
///
/// # Returns
/// A multi-line string containing:
/// - Extracted readable strings (if any)
/// - Hex dump with ASCII representation
pub fn hex_dump(data: &[u8]) -> String {
    let mut result = String::new();

    // First, try to extract readable strings from the binary data
    let readable_strings = extract_readable_strings(data);
    if !readable_strings.is_empty() {
        result.push_str("Extracted readable fields:\n");
        for s in &readable_strings {
            result.push_str(&format!("  - {}\n", s));
        }
        result.push_str("\nHex dump:\n");
    }

    for (i, chunk) in data.chunks(16).enumerate() {
        // Offset
        result.push_str(&format!("{:08x}  ", i * 16));

        // Hex bytes
        for byte in chunk {
            result.push_str(&format!("{:02x} ", byte));
        }

        // Padding
        for _ in chunk.len()..16 {
            result.push_str("   ");
        }

        result.push_str(" |");

        // ASCII representation
        for byte in chunk {
            if byte.is_ascii_graphic() || *byte == b' ' {
                result.push(*byte as char);
            } else {
                result.push('.');
            }
        }

        result.push('|');
        result.push('\n');
    }

    result
}

/// Extracts readable strings from binary data
///
/// Scans binary data for sequences of printable ASCII characters
/// and extracts them as strings. Useful for finding embedded text
/// in binary formats like Avro.
///
/// # Arguments
/// - `data` - The binary data to scan
///
/// # Returns
/// A vector of unique strings (minimum 4 characters, containing at least one letter)
pub fn extract_readable_strings(data: &[u8]) -> Vec<String> {
    let mut strings = Vec::new();
    let mut current = String::new();
    let min_length = 4;

    for &byte in data {
        if byte.is_ascii_graphic() || byte == b' ' {
            current.push(byte as char);
        } else {
            if current.len() >= min_length {
                // Check if it looks like a meaningful string (not just random chars)
                if current.chars().any(|c| c.is_alphabetic()) {
                    strings.push(current.trim().to_string());
                }
            }
            current.clear();
        }
    }

    if current.len() >= min_length && current.chars().any(|c| c.is_alphabetic()) {
        strings.push(current.trim().to_string());
    }

    // Remove duplicates and sort
    strings.sort();
    strings.dedup();
    strings
}

/// Converts Apache Avro value to serde_json Value
///
/// Recursively converts all Avro types to their JSON equivalents.
/// Handles complex types including records, arrays, maps, unions, and enums.
///
/// # Arguments
/// - `value` - The Avro value to convert
///
/// # Returns
/// A serde_json::Value representing the Avro data
pub fn avro_value_to_json(value: &apache_avro::types::Value) -> serde_json::Value {
    use apache_avro::types::Value as AvroValue;

    match value {
        AvroValue::Null => serde_json::Value::Null,
        AvroValue::Boolean(b) => serde_json::Value::Bool(*b),
        AvroValue::Int(i) => serde_json::Value::Number((*i).into()),
        AvroValue::Long(l) => serde_json::Value::Number((*l).into()),
        AvroValue::Float(f) => serde_json::Value::Number(
            serde_json::Number::from_f64(*f as f64).unwrap_or_else(|| serde_json::Number::from(0)),
        ),
        AvroValue::Double(d) => serde_json::Value::Number(
            serde_json::Number::from_f64(*d).unwrap_or_else(|| serde_json::Number::from(0)),
        ),
        AvroValue::Bytes(bytes) | AvroValue::Fixed(_, bytes) => {
            // Try to convert to string, otherwise base64
            match String::from_utf8(bytes.clone()) {
                Ok(s) => serde_json::Value::String(s),
                Err(_) => {
                    // Convert to hex string
                    let hex_string = bytes
                        .iter()
                        .map(|b| format!("{:02x}", b))
                        .collect::<String>();
                    serde_json::Value::String(format!("0x{}", hex_string))
                }
            }
        }
        AvroValue::String(s) => serde_json::Value::String(s.clone()),
        AvroValue::Array(arr) => {
            serde_json::Value::Array(arr.iter().map(avro_value_to_json).collect())
        }
        AvroValue::Map(map) => {
            let mut json_map = serde_json::Map::new();
            for (k, v) in map {
                json_map.insert(k.clone(), avro_value_to_json(v));
            }
            serde_json::Value::Object(json_map)
        }
        AvroValue::Union(_, boxed) => avro_value_to_json(boxed),
        AvroValue::Record(fields) => {
            let mut json_map = serde_json::Map::new();
            for (name, value) in fields {
                json_map.insert(name.clone(), avro_value_to_json(value));
            }
            serde_json::Value::Object(json_map)
        }
        AvroValue::Enum(_, symbol) => serde_json::Value::String(symbol.clone()),
        AvroValue::Duration(duration) => {
            let mut map = serde_json::Map::new();
            map.insert(
                "months".to_string(),
                serde_json::Value::Number(u32::from(duration.months()).into()),
            );
            map.insert(
                "days".to_string(),
                serde_json::Value::Number(u32::from(duration.days()).into()),
            );
            map.insert(
                "millis".to_string(),
                serde_json::Value::Number(u32::from(duration.millis()).into()),
            );
            serde_json::Value::Object(map)
        }
        AvroValue::Decimal(decimal) => {
            // Convert to string representation
            serde_json::Value::String(format!("{:?}", decimal))
        }
        AvroValue::Uuid(uuid) => serde_json::Value::String(uuid.to_string()),
        AvroValue::Date(date) => serde_json::Value::Number((*date).into()),
        AvroValue::TimeMillis(time) => serde_json::Value::Number((*time).into()),
        AvroValue::TimeMicros(time) => serde_json::Value::Number((*time).into()),
        AvroValue::TimestampMillis(ts) => serde_json::Value::Number((*ts).into()),
        AvroValue::TimestampMicros(ts) => serde_json::Value::Number((*ts).into()),
        AvroValue::LocalTimestampMillis(ts) => serde_json::Value::Number((*ts).into()),
        AvroValue::LocalTimestampMicros(ts) => serde_json::Value::Number((*ts).into()),
    }
}

/// Detects if data appears to be binary (non-printable characters)
///
/// Uses a heuristic: if more than 30% of characters are non-printable,
/// considers the data to be binary.
///
/// # Arguments
/// - `data` - The data to check
///
/// # Returns
/// `true` if the data appears to be binary, `false` if it looks like text
pub fn is_binary(data: &[u8]) -> bool {
    let text = String::from_utf8_lossy(data);
    let non_printable_count = text
        .chars()
        .filter(|c| {
            !c.is_ascii_graphic() && !c.is_whitespace() && *c != '\t' && *c != '\n' && *c != '\r'
        })
        .count();
    let ratio = non_printable_count as f32 / text.len().max(1) as f32;
    ratio > 0.3
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_extract_readable_strings() {
        let data = b"Hello\x00World\x00Test\x00";
        let strings = extract_readable_strings(data);
        assert_eq!(strings, vec!["Hello", "Test", "World"]);
    }

    #[test]
    fn test_extract_readable_strings_ignores_short() {
        let data = b"Hi\x00World\x00";
        let strings = extract_readable_strings(data);
        assert_eq!(strings, vec!["World"]);
    }

    #[test]
    fn test_is_binary_detects_binary_data() {
        let binary_data = vec![0x00, 0x01, 0x02, 0x03, 0xFF, 0xFE];
        assert!(is_binary(&binary_data));
    }

    #[test]
    fn test_is_binary_detects_text() {
        let text_data = b"This is plain text";
        assert!(!is_binary(text_data));
    }

    #[test]
    fn test_hex_dump_format() {
        let data = b"Hello, World!";
        let dump = hex_dump(data);
        assert!(dump.contains("48 65 6c 6c 6f")); // "Hello" in hex
        assert!(dump.contains("Hello, World!"));
    }
}
