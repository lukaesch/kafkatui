//! Input dialogs and modals
//!
//! Provides various dialog boxes for user input including offset jump,
//! search, and file save confirmation.

use crate::ui::helpers::centered_rect;
use crate::App;
use ratatui::{
    layout::Alignment,
    style::{Color, Modifier, Style},
    text::{Line, Span},
    widgets::{Block, Borders, Clear, Paragraph},
    Frame,
};

/// Renders the offset jump dialog
///
/// Displays a modal for jumping to a specific Kafka offset.
/// Shows the current offset range for context.
///
/// # Arguments
/// - `f` - The Frame to render into
/// - `app` - The application state containing the input
pub(crate) fn render_offset_jump_dialog(f: &mut Frame, app: &App) {
    let area = centered_rect(50, 14, f.area());
    f.render_widget(Clear, area);

    let messages = app.current_partition_messages();
    let offset_range = if !messages.is_empty() {
        let first = messages.first().unwrap().offset;
        let last = messages.last().unwrap().offset;
        format!(" (Range: {} - {})", first, last)
    } else {
        String::new()
    };

    let mut text = vec![
        Line::from(Span::styled(
            format!("Current offset range{}", offset_range),
            Style::default().fg(Color::Cyan),
        )),
        Line::from(""),
        Line::from("Enter Kafka offset to jump to:"),
        Line::from(""),
        Line::from(Span::styled(
            &app.jump_offset_input,
            Style::default()
                .fg(Color::Yellow)
                .add_modifier(Modifier::BOLD),
        )),
    ];

    if !app.jump_offset_input.is_empty() {
        text.push(Line::from(""));
        text.push(Line::from(Span::styled(
            "Press Enter to jump, Esc to cancel",
            Style::default().fg(Color::Gray),
        )));
    }

    let paragraph = Paragraph::new(text).alignment(Alignment::Center).block(
        Block::default()
            .borders(Borders::ALL)
            .title("Jump to Offset"),
    );

    f.render_widget(paragraph, area);
}

/// Renders the search dialog
///
/// Displays a modal for entering a search query to search within
/// the current partition's loaded messages.
///
/// # Arguments
/// - `f` - The Frame to render into
/// - `app` - The application state containing the search query
pub(crate) fn render_search_dialog(f: &mut Frame, app: &App) {
    let area = centered_rect(60, 12, f.area());
    f.render_widget(Clear, area);

    let mut text = vec![
        Line::from("Search in loaded messages (keys and cached values):"),
        Line::from(Span::styled(
            "Note: Only searches in current partition's loaded messages",
            Style::default().fg(Color::DarkGray),
        )),
        Line::from(""),
        Line::from(Span::styled(
            &app.search_query,
            Style::default()
                .fg(Color::Yellow)
                .add_modifier(Modifier::BOLD),
        )),
    ];

    if !app.search_query.is_empty() {
        text.push(Line::from(""));
        text.push(Line::from(Span::styled(
            "Press Enter to search current partition, Esc to cancel",
            Style::default().fg(Color::Gray),
        )));
    }

    let paragraph = Paragraph::new(text)
        .alignment(Alignment::Center)
        .block(Block::default().borders(Borders::ALL).title("Search"));

    f.render_widget(paragraph, area);
}

/// Renders the file saved confirmation modal
///
/// Shows a success message after a message has been exported to a file.
/// Displays the file path where the message was saved.
///
/// # Arguments
/// - `f` - The Frame to render into
/// - `app` - The application state containing the saved file path
pub(crate) fn render_file_saved_modal(f: &mut Frame, app: &App) {
    let area = centered_rect(60, 15, f.area());
    f.render_widget(Clear, area);

    let mut text = vec![
        Line::from(Span::styled(
            "✓ Message saved successfully!",
            Style::default()
                .fg(Color::Green)
                .add_modifier(Modifier::BOLD),
        )),
        Line::from(""),
    ];

    if let Some(filepath) = &app.saved_file_path {
        text.push(Line::from("File location:"));
        text.push(Line::from(Span::styled(
            filepath,
            Style::default()
                .fg(Color::Cyan)
                .add_modifier(Modifier::BOLD),
        )));
    }

    text.push(Line::from(""));
    text.push(Line::from(Span::styled(
        "Press Enter or Esc to close",
        Style::default().fg(Color::Gray),
    )));

    let paragraph = Paragraph::new(text).alignment(Alignment::Center).block(
        Block::default()
            .borders(Borders::ALL)
            .title("Export Complete")
            .border_style(Style::default().fg(Color::Green)),
    );

    f.render_widget(paragraph, area);
}
