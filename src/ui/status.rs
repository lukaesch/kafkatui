//! Status bar and error display rendering
//!
//! Handles the rendering of the status bar (showing keyboard shortcuts)
//! and the error log panel.

use crate::{App, AppMode};
use ratatui::{
    layout::{Alignment, Rect},
    style::{Color, Modifier, Style},
    text::{Line, Span},
    widgets::{Block, Borders, List, ListItem, Paragraph},
    Frame,
};

/// Renders the status bar with context-appropriate keyboard shortcuts
///
/// Displays different keyboard shortcuts based on the current application mode.
/// Helps users discover available actions in each context.
///
/// # Arguments
/// - `f` - The Frame to render into
/// - `app` - The application state
/// - `area` - The screen area to render in
pub(crate) fn render_status_bar(f: &mut Frame, app: &App, area: Rect) {
    // Footer shows only keyboard shortcuts based on current mode
    let shortcuts_text = match app.mode {
        AppMode::BrokerSwitcher => "[↑↓] Navigate  [Enter] Connect  [Esc] Cancel".to_string(),
        AppMode::TopicSwitcher => "[↑↓] Navigate  [Enter] Select  [/] Filter  [Esc] Cancel".to_string(),
        AppMode::ProfileEditor => "[Tab] Next field  [Enter] Save  [Esc] Cancel".to_string(),
        AppMode::ProfileDeleting => "[Y] Confirm  [N/Esc] Cancel".to_string(),
        AppMode::ConnectionTesting => "Press any key to continue".to_string(),
        AppMode::PartitionList => "[↑↓] Nav  [Enter] View  [/] Search  [k] Keys  [Ctrl+B] Broker  [Ctrl+T] Topic  [q] Quit".to_string(),
        AppMode::MessageList => {
            if !app.search_results.is_empty() {
                let current = app.current_search_index + 1;
                let total = app.search_results.len();
                format!("Search \"{}\" ({}/{})  [n/N] Next/Prev  [Tab] View  [/] New  [b] Back",
                    app.search_query, current, total)
            } else {
                "[↑↓] Nav  [Tab] Detail  [j] Jump  [/] Search  [l] Load more  [b] Back  [q] Quit".to_string()
            }
        },
        AppMode::MessageDetail => "[↑↓] Scroll  [f] Format  [s] Save  [Tab/b] Back".to_string(),
        AppMode::OffsetJump => "[Enter] Jump  [Esc] Cancel".to_string(),
        AppMode::Search => "[Enter] Search  [Esc] Cancel".to_string(),
        AppMode::Loading => "Loading...".to_string(),
        AppMode::KeyAnalysis => "Press any key to return".to_string(),
        AppMode::GlobalSearch => {
            if app.global_search.is_searching {
                let percent = if app.global_search.estimated_total > 0 {
                    (app.global_search.messages_processed as f64 / app.global_search.estimated_total as f64 * 100.0) as u8
                } else {
                    0
                };
                format!("Searching: {}% complete  [Esc] Cancel", percent)
            } else {
                "[↑↓] Nav  [Enter] Search  [Space] Jump  [Tab] Mode  [Esc] Back".to_string()
            }
        }
        AppMode::FileSaved => "[Enter/Esc] Close".to_string(),
    };

    let status_bar = Paragraph::new(shortcuts_text)
        .style(Style::default().fg(Color::White))
        .alignment(Alignment::Center)
        .block(
            Block::default()
                .borders(Borders::ALL)
                .border_style(Style::default().fg(Color::Gray)),
        );

    f.render_widget(status_bar, area);
}

/// Renders the error log panel
///
/// Displays the most recent errors (up to 20) in reverse chronological order.
/// Each error shows a timestamp and error message.
///
/// # Arguments
/// - `f` - The Frame to render into
/// - `app` - The application state
/// - `area` - The screen area to render in
pub(crate) fn render_error_panel(f: &mut Frame, app: &App, area: Rect) {
    let error_count = app.error_log.len();
    let title = format!("Errors ({}) - Press 'e' to hide", error_count);

    let errors: Vec<ListItem> = app
        .error_log
        .iter()
        .rev() // Show newest first
        .take(20) // Limit display to last 20 errors
        .map(|(timestamp, error)| {
            let time_str = timestamp.format("%H:%M:%S").to_string();
            ListItem::new(Line::from(vec![
                Span::styled(
                    format!("[{}] ", time_str),
                    Style::default().fg(Color::DarkGray),
                ),
                Span::styled(error, Style::default().fg(Color::Red)),
            ]))
        })
        .collect();

    let list = List::new(errors).block(
        Block::default()
            .borders(Borders::ALL)
            .title(title)
            .border_style(Style::default().fg(Color::Red)),
    );

    f.render_widget(list, area);
}

/// Renders the loading screen
///
/// Shows a loading indicator with a status message.
///
/// # Arguments
/// - `f` - The Frame to render into
/// - `app` - The application state containing the loading message
/// - `area` - The screen area to render in
pub(crate) fn render_loading(f: &mut Frame, app: &App, area: Rect) {
    let loading_text = vec![
        Line::from(""),
        Line::from(Span::styled(
            "Loading Messages...",
            Style::default()
                .fg(Color::Cyan)
                .add_modifier(Modifier::BOLD),
        )),
        Line::from(""),
        Line::from(app.loading_message.as_str()),
    ];

    let paragraph = Paragraph::new(loading_text)
        .alignment(Alignment::Center)
        .block(Block::default().borders(Borders::ALL).title("Loading"));

    f.render_widget(paragraph, area);
}
