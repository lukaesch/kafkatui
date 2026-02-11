//! Partition list view rendering
//!
//! Displays the list of Kafka topic partitions with metadata like
//! message count and offset ranges.

use crate::App;
use ratatui::{
    layout::Rect,
    style::{Color, Modifier, Style},
    text::{Line, Span},
    widgets::{Block, Borders, List, ListItem},
    Frame,
};

/// Renders the partition list view
///
/// Shows all partitions for the current topic with their high watermarks
/// (total message count). The selected partition is highlighted.
///
/// # Arguments
/// - `f` - The Frame to render into
/// - `app` - The application state
/// - `area` - The screen area to render in
pub(crate) fn render_partition_list(f: &mut Frame, app: &App, area: Rect) {
    let items: Vec<ListItem> = app
        .partitions
        .iter()
        .enumerate()
        .map(|(i, p)| {
            let style = if i == app.selected_partition {
                Style::default()
                    .fg(Color::Yellow)
                    .add_modifier(Modifier::BOLD)
            } else {
                Style::default()
            };

            ListItem::new(Line::from(vec![
                Span::raw(format!("Partition {}: ", p.id)),
                Span::styled(
                    format!("{} messages", p.high_watermark),
                    Style::default().fg(Color::Green),
                ),
            ]))
            .style(style)
        })
        .collect();

    let list = List::new(items).block(Block::default().borders(Borders::ALL).title("Partitions"));

    f.render_widget(list, area);
}
