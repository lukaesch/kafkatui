//! Message list and detail view rendering
//!
//! Handles the display of Kafka messages in both list and detail views.
//! Supports multiple display formats (JSON, Hex, Raw) and shows message
//! metadata including timestamps, keys, headers, and Avro schema IDs.

use crate::{utils::formatting::hex_dump, App, DisplayFormat};
use chrono::{TimeZone, Utc};
use ratatui::{
    layout::Rect,
    style::{Color, Modifier, Style},
    text::{Line, Span},
    widgets::{Block, Borders, List, ListItem, Paragraph, Wrap},
    Frame,
};
use serde_json::Value;

/// Renders the message list view
///
/// Displays messages from the current partition with metadata:
/// - Offset number
/// - Timestamp
/// - Message key
/// - Size and type (text/binary/Avro)
///
/// Search results are highlighted in yellow, with the current result in cyan.
///
/// # Arguments
/// - `f` - The Frame to render into
/// - `app` - The application state
/// - `area` - The screen area to render in
pub(crate) fn render_message_list(f: &mut Frame, app: &App, area: Rect) {
    let messages = app.current_partition_messages();

    let items: Vec<ListItem> = messages
        .iter()
        .enumerate()
        .map(|(i, msg)| {
            let is_search_result = app
                .search_results
                .iter()
                .any(|(part_idx, msg_idx)| *part_idx == app.selected_partition && *msg_idx == i);
            let is_current_search = is_search_result
                && app
                    .search_results
                    .get(app.current_search_index)
                    .is_some_and(|(part_idx, msg_idx)| {
                        *part_idx == app.selected_partition && *msg_idx == i
                    });

            let style = if is_current_search {
                Style::default()
                    .bg(Color::Cyan)
                    .fg(Color::Black)
                    .add_modifier(Modifier::BOLD)
            } else if is_search_result {
                Style::default().bg(Color::Yellow).fg(Color::Black)
            } else {
                Style::default()
            };

            let timestamp = Utc
                .timestamp_millis_opt(msg.timestamp)
                .single()
                .map(|dt| dt.format("%Y-%m-%d %H:%M:%S").to_string())
                .unwrap_or_else(|| "Unknown".to_string());

            // Format size and type for display
            let size_str = if msg.value_size < 1024 {
                format!("{}B", msg.value_size)
            } else if msg.value_size < 1024 * 1024 {
                format!("{:.1}KB", msg.value_size as f64 / 1024.0)
            } else {
                format!("{:.1}MB", msg.value_size as f64 / 1024.0 / 1024.0)
            };

            let type_display = if let Some(schema_id) = msg.avro_schema_id {
                format!("Avro:{}", schema_id)
            } else if msg.is_binary {
                "binary".to_string()
            } else {
                "text".to_string()
            };

            let key_display = msg.key.as_deref().unwrap_or("null");

            ListItem::new(Line::from(vec![
                Span::styled(
                    format!("[{}] ", msg.offset),
                    Style::default().fg(Color::Cyan),
                ),
                Span::styled(timestamp, Style::default().fg(Color::Gray)),
                Span::raw(" | "),
                Span::styled(
                    format!("Key: {} ", key_display),
                    Style::default().fg(Color::Green),
                ),
                Span::raw("| "),
                Span::styled(
                    format!("[{} {}]", size_str, type_display),
                    Style::default().fg(Color::Magenta),
                ),
            ]))
            .style(style)
        })
        .collect();

    let partition_info = app.partitions.get(app.selected_partition);
    let title = if let Some(partition) = partition_info {
        let messages = &partition.messages;
        let base_title = if messages.is_empty() {
            format!("Messages - Partition {} (No messages loaded)", partition.id)
        } else {
            let first_offset = messages.first().unwrap().offset;
            let last_offset = messages.last().unwrap().offset;
            let total_in_partition = partition.high_watermark;
            format!(
                "Messages - Partition {} (Showing {} messages: {} to {} of total {})",
                partition.id,
                messages.len(),
                first_offset,
                last_offset,
                total_in_partition
            )
        };

        // Add search info if searching
        if !app.search_results.is_empty() {
            let results_in_partition = app
                .search_results
                .iter()
                .filter(|(p, _)| *p == app.selected_partition)
                .count();
            format!(
                "{} | Search: {} result{} in this partition",
                base_title,
                results_in_partition,
                if results_in_partition == 1 { "" } else { "s" }
            )
        } else {
            base_title
        }
    } else {
        "Messages".to_string()
    };

    let list = List::new(items)
        .block(Block::default().borders(Borders::ALL).title(title))
        .highlight_style(Style::default().add_modifier(Modifier::BOLD))
        .highlight_symbol(">> ");

    let mut state = app.message_list_state.clone();
    f.render_stateful_widget(list, area, &mut state);
}

/// Renders the message detail view
///
/// Shows the full content of a selected message with all metadata.
/// Supports three display formats:
/// - JSON: Pretty-printed JSON with syntax
/// - Hex: Hexadecimal dump with ASCII sidebar
/// - Raw: Raw bytes as lossy UTF-8 string
///
/// # Arguments
/// - `f` - The Frame to render into
/// - `app` - The application state
/// - `area` - The screen area to render in
pub(crate) fn render_message_detail(f: &mut Frame, app: &App, area: Rect) {
    if let Some(msg_idx) = app.selected_message {
        if let Some(metadata) = app.current_partition_messages().get(msg_idx) {
            // Get content from cache or show loading message
            let content = app.message_cache.get(&metadata.offset);
            let mut text = vec![
                Line::from(vec![
                    Span::styled("Offset: ", Style::default().add_modifier(Modifier::BOLD)),
                    Span::raw(metadata.offset.to_string()),
                ]),
                Line::from(vec![
                    Span::styled("Partition: ", Style::default().add_modifier(Modifier::BOLD)),
                    Span::raw(metadata.partition.to_string()),
                ]),
                Line::from(vec![
                    Span::styled("Timestamp: ", Style::default().add_modifier(Modifier::BOLD)),
                    Span::raw(
                        Utc.timestamp_millis_opt(metadata.timestamp)
                            .single()
                            .map(|dt| dt.format("%Y-%m-%d %H:%M:%S").to_string())
                            .unwrap_or_else(|| "Unknown".to_string()),
                    ),
                ]),
                Line::from(vec![
                    Span::styled("Size: ", Style::default().add_modifier(Modifier::BOLD)),
                    Span::raw(format!("{} bytes", metadata.value_size)),
                ]),
            ];

            if let Some(key) = &metadata.key {
                text.push(Line::from(vec![
                    Span::styled("Key: ", Style::default().add_modifier(Modifier::BOLD)),
                    Span::raw(key),
                ]));
            }

            if !metadata.headers.is_empty() {
                text.push(Line::from(Span::styled(
                    "Headers:",
                    Style::default().add_modifier(Modifier::BOLD),
                )));
                for (k, v) in &metadata.headers {
                    text.push(Line::from(format!("  {}: {}", k, v)));
                }
            }

            text.push(Line::from(""));

            // Add format indicator
            let format_text = match app.display_format {
                DisplayFormat::Json => "JSON",
                DisplayFormat::Hex => "Hex",
                DisplayFormat::Raw => "Raw",
            };
            text.push(Line::from(vec![
                Span::styled("Value [", Style::default().add_modifier(Modifier::BOLD)),
                Span::styled(
                    format_text,
                    Style::default()
                        .fg(Color::Cyan)
                        .add_modifier(Modifier::BOLD),
                ),
                Span::styled("]:", Style::default().add_modifier(Modifier::BOLD)),
            ]));

            // Show content if loaded, otherwise show loading message
            if let Some(content) = content {
                match app.display_format {
                    DisplayFormat::Json => {
                        // Try to parse as JSON for pretty printing
                        if content.value.starts_with("[")
                            && content.value.contains("Deserialization failed")
                        {
                            // Show the error message and extracted fields
                            for line in content.value.lines() {
                                text.push(Line::from(Span::styled(
                                    line.to_string(),
                                    Style::default().fg(Color::Yellow),
                                )));
                            }
                        } else if let Ok(json) = serde_json::from_str::<Value>(&content.value) {
                            if let Ok(pretty) = serde_json::to_string_pretty(&json) {
                                for line in pretty.lines() {
                                    text.push(Line::from(line.to_string()));
                                }
                            } else {
                                text.push(Line::from(content.value.clone()));
                            }
                        } else {
                            // Not JSON, show as raw text
                            for line in content.value.lines() {
                                text.push(Line::from(line.to_string()));
                            }
                        }
                    }
                    DisplayFormat::Hex => {
                        // Show hex dump
                        if let Some(raw_bytes) = &content.raw_bytes {
                            text.push(Line::from(""));
                            text.push(Line::from(Span::styled(
                                "Hex dump:",
                                Style::default().add_modifier(Modifier::BOLD),
                            )));
                            for line in hex_dump(raw_bytes).lines() {
                                text.push(Line::from(Span::styled(
                                    line.to_string(),
                                    Style::default().fg(Color::Cyan),
                                )));
                            }
                        } else {
                            text.push(Line::from(Span::styled(
                                "Hex view not available",
                                Style::default().fg(Color::DarkGray),
                            )));
                        }
                    }
                    DisplayFormat::Raw => {
                        // Show raw bytes as string (lossy conversion)
                        if let Some(raw_bytes) = &content.raw_bytes {
                            let raw_string = String::from_utf8_lossy(raw_bytes);
                            for line in raw_string.lines() {
                                text.push(Line::from(line.to_string()));
                            }
                        } else {
                            for line in content.value.lines() {
                                text.push(Line::from(line.to_string()));
                            }
                        }
                    }
                }
            } else {
                // Content not loaded yet
                text.push(Line::from(Span::styled(
                    "Content not loaded. Press Tab to load message content...",
                    Style::default().fg(Color::Yellow),
                )));
            }

            let paragraph = Paragraph::new(text)
                .block(
                    Block::default()
                        .borders(Borders::ALL)
                        .title(format!("Message Detail - Format: {}", format_text)),
                )
                .scroll((app.detail_scroll_offset, 0))
                .wrap(Wrap { trim: false });

            f.render_widget(paragraph, area);
        }
    }
}
