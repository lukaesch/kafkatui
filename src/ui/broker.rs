//! Broker and topic switcher views
//!
//! Provides UI for switching between broker profiles and topics.
//! Displays available options and allows quick navigation.

use crate::ui::helpers::centered_rect;
use crate::App;
use ratatui::{
    layout::{Alignment, Constraint, Direction, Layout},
    style::{Color, Modifier, Style},
    text::{Line, Span},
    widgets::{Block, Borders, Clear, List, ListItem, Paragraph},
    Frame,
};

/// Renders the broker profile switcher dialog
///
/// Shows a modal dialog with all configured broker profiles.
/// Highlights the current profile and provides options to create,
/// edit, delete, or test profiles.
///
/// # Arguments
/// - `f` - The Frame to render into
/// - `app` - The application state (mutable for list state)
pub(crate) fn render_broker_switcher(f: &mut Frame, app: &mut App) {
    let area = centered_rect(60, 70, f.area());
    f.render_widget(Clear, area);

    // Split into list and help text
    let chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([Constraint::Min(0), Constraint::Length(3)])
        .split(area);

    let mut profiles: Vec<ListItem> = app
        .connection_manager
        .profiles
        .iter()
        .map(|(name, profile)| {
            let is_current = app.connection_manager.current_profile.as_ref() == Some(name);
            let style = if is_current {
                Style::default()
                    .fg(Color::Green)
                    .add_modifier(Modifier::BOLD)
            } else {
                Style::default()
            };

            let indicator = if is_current { "> " } else { "  " };
            ListItem::new(Line::from(vec![
                Span::raw(indicator),
                Span::styled(format!("[{}] ", name), style),
                Span::raw(&profile.broker),
            ]))
        })
        .collect();

    // Add separator and options
    profiles.push(ListItem::new(Line::from(
        "───────────────────────────────────────",
    )));
    profiles.push(ListItem::new(Line::from(vec![
        Span::styled("[n]", Style::default().fg(Color::Yellow)),
        Span::raw(" New profile"),
    ])));

    let list = List::new(profiles)
        .block(
            Block::default()
                .title("Switch Broker Profile")
                .borders(Borders::ALL)
                .border_style(Style::default().fg(Color::Cyan)),
        )
        .highlight_style(Style::default().bg(Color::DarkGray))
        .highlight_symbol(">> ");

    f.render_stateful_widget(list, chunks[0], &mut app.broker_switcher_state);

    let help = Paragraph::new(
        "↑/↓: Navigate | Enter: Connect | n: New | e: Edit | d: Delete | t: Test | ESC: Cancel",
    )
    .style(Style::default().fg(Color::Gray))
    .alignment(Alignment::Center)
    .block(Block::default().borders(Borders::ALL));
    f.render_widget(help, chunks[1]);
}

/// Renders the topic switcher dialog
///
/// Shows a modal dialog with all available topics for the current broker.
/// Includes a filter input field for quick topic search. Highlights the
/// current topic.
///
/// # Arguments
/// - `f` - The Frame to render into
/// - `app` - The application state (mutable for list state)
pub(crate) fn render_topic_switcher(f: &mut Frame, app: &mut App) {
    let area = centered_rect(70, 70, f.area());
    f.render_widget(Clear, area);

    let chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([Constraint::Length(3), Constraint::Min(0)])
        .split(area);

    let input = Paragraph::new(app.topic_filter.as_str()).block(
        Block::default()
            .title("Filter Topics (Type to search, Enter to select, ESC to cancel)")
            .borders(Borders::ALL)
            .border_style(Style::default().fg(Color::Yellow)),
    );
    f.render_widget(input, chunks[0]);

    let filtered_topics = app.get_filtered_topics();
    let topics: Vec<ListItem> = filtered_topics
        .iter()
        .map(|topic| {
            let is_current = &app.topic_name == topic;
            let style = if is_current {
                Style::default()
                    .fg(Color::Green)
                    .add_modifier(Modifier::BOLD)
            } else {
                Style::default()
            };

            let indicator = if is_current { "> " } else { "  " };
            ListItem::new(Line::from(vec![
                Span::raw(indicator),
                Span::styled(topic, style),
            ]))
        })
        .collect();

    let list = List::new(topics)
        .block(
            Block::default()
                .title(format!("Topics ({} matched)", filtered_topics.len()))
                .borders(Borders::ALL),
        )
        .highlight_style(Style::default().bg(Color::DarkGray))
        .highlight_symbol(">> ");

    f.render_stateful_widget(list, chunks[1], &mut app.topic_switcher_state);
}
