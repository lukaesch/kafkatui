//! Global search interface rendering
//!
//! Displays the global search UI that searches across all partitions.
//! Supports two search strategies: Quick (keys + cached) and Deep (full content).

use crate::{App, SearchStrategy};
use chrono::{TimeZone, Utc};
use ratatui::{
    layout::{Alignment, Constraint, Direction, Layout, Rect},
    style::{Color, Modifier, Style},
    text::{Line, Span},
    widgets::{Block, Borders, List, ListItem, Paragraph},
    Frame,
};

/// Renders the global search view
///
/// Shows:
/// - Search query input field
/// - Search strategy selector (Quick/Deep)
/// - Progress indicator during search
/// - Results list with match context
///
/// Results show the partition, timestamp, key, and a snippet of matching text.
///
/// # Arguments
/// - `f` - The Frame to render into
/// - `app` - The application state
/// - `area` - The screen area to render in
pub(crate) fn render_global_search(f: &mut Frame, app: &App, area: Rect) {
    let chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(5), // Search input area
            Constraint::Min(0),    // Results area
        ])
        .split(area);

    // Search input area
    let strategy_indicator = match app.global_search.strategy {
        SearchStrategy::Quick => "Quick (keys + cached)",
        SearchStrategy::Deep => "Deep (keys + content)",
    };

    let search_status = if app.global_search.is_searching {
        format!(
            "Searching... ({}/{})",
            app.global_search.partitions_searched, app.global_search.total_partitions
        )
    } else if !app.global_search.results.is_empty() {
        format!("Found {} results", app.global_search.results.len())
    } else if !app.global_search.query.is_empty() {
        "No results found".to_string()
    } else {
        "Enter search query...".to_string()
    };

    let input_lines = vec![
        Line::from(vec![
            Span::styled("Global Search | Strategy: ", Style::default()),
            Span::styled(strategy_indicator, Style::default().fg(Color::Cyan)),
            Span::styled(" (Tab to toggle)", Style::default().fg(Color::DarkGray)),
        ]),
        Line::from(""),
        Line::from(Span::styled(
            &app.global_search.query,
            Style::default()
                .fg(Color::Yellow)
                .add_modifier(Modifier::BOLD),
        )),
        Line::from(Span::styled(
            search_status,
            Style::default().fg(Color::Gray),
        )),
    ];

    let input_block = Paragraph::new(input_lines).block(
        Block::default()
            .borders(Borders::ALL)
            .title("Search Across All Partitions"),
    );
    f.render_widget(input_block, chunks[0]);

    // Results area
    if !app.global_search.results.is_empty() {
        let results: Vec<ListItem> = app
            .global_search
            .results
            .iter()
            .enumerate()
            .map(|(idx, result)| {
                let timestamp = Utc
                    .timestamp_millis_opt(result.timestamp)
                    .single()
                    .map(|dt| dt.format("%H:%M:%S").to_string())
                    .unwrap_or_else(|| "Unknown".to_string());

                let key_display = result.key.as_deref().unwrap_or("null");
                let match_indicator = if result.match_in_key { "KEY" } else { "VAL" };

                let style = if idx == app.global_search.selected_result {
                    Style::default()
                        .bg(Color::Blue)
                        .fg(Color::White)
                        .add_modifier(Modifier::BOLD)
                } else {
                    Style::default()
                };

                ListItem::new(Line::from(vec![
                    Span::styled(
                        format!("[P{}] ", result.partition_id),
                        Style::default().fg(Color::Green),
                    ),
                    Span::styled(
                        format!("{} ", timestamp),
                        Style::default().fg(Color::DarkGray),
                    ),
                    Span::styled(
                        format!("[{}] ", match_indicator),
                        Style::default().fg(Color::Magenta),
                    ),
                    Span::styled(
                        format!("Key: {} | ", key_display),
                        Style::default().fg(Color::Yellow),
                    ),
                    Span::styled(&result.match_context, Style::default()),
                ]))
                .style(style)
            })
            .collect();

        let results_list = List::new(results).block(
            Block::default()
                .borders(Borders::ALL)
                .title("Search Results"),
        );
        f.render_widget(results_list, chunks[1]);
    } else {
        let empty_text = if app.global_search.query.is_empty() {
            "Type your search query and press Enter to search across all partitions"
        } else {
            "No matches found. Try different keywords or switch to Deep search with Tab."
        };

        let empty_paragraph = Paragraph::new(empty_text)
            .style(Style::default().fg(Color::DarkGray))
            .alignment(Alignment::Center)
            .block(
                Block::default()
                    .borders(Borders::ALL)
                    .title("Search Results"),
            );
        f.render_widget(empty_paragraph, chunks[1]);
    }
}
